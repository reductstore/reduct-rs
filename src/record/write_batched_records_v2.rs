// Copyright 2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::Record;
use async_stream::stream;
use futures_util::StreamExt;
use reduct_base::batch::v2::{encode_entry_name, make_batched_header_name};
use reduct_base::error::{ErrorCode, ReductError};
use reqwest::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

/// Builder for writing multiple records across entries in a single request.
pub struct WriteRecordBatchBuilder {
    bucket: String,
    records: VecDeque<Record>,
    client: Arc<HttpClient>,
    last_access: SystemTime,
}

type FailedRecordMap = BTreeMap<(String, u64), ReductError>;

impl WriteRecordBatchBuilder {
    pub(crate) fn new(bucket: String, client: Arc<HttpClient>) -> Self {
        Self {
            bucket,
            records: VecDeque::new(),
            client,
            last_access: SystemTime::now(),
        }
    }

    /// Add a record to the batch.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to add to the batch.
    ///
    /// # Returns
    ///
    /// Returns the builder for chaining.
    pub fn add_record(mut self, record: Record) -> Self {
        self.records.push_back(record);
        self.last_access = SystemTime::now();
        self
    }

    /// Add record to the batch without chaining.
    ///
    /// # Arguments
    ///
    /// * `record` - The record to append to the batch.
    pub fn append_record(&mut self, record: Record) {
        self.records.push_back(record);
        self.last_access = SystemTime::now();
    }

    /// Add records to the batch.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to add to the batch.
    ///
    /// # Returns
    ///
    /// Returns the builder for chaining.
    pub fn add_records(mut self, records: Vec<Record>) -> Self {
        self.records.extend(records);
        self.last_access = SystemTime::now();
        self
    }

    /// Add records to the batch without chaining.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append to the batch.
    ///
    pub fn append_records(&mut self, records: Vec<Record>) {
        self.records.extend(records);
        self.last_access = SystemTime::now();
    }

    /// Build the request and send it to the server.
    ///
    /// # Returns
    ///
    /// Returns a map of (entry, timestamp) to errors for any records that failed to write.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If the request was not successful.
    pub async fn send(mut self) -> Result<FailedRecordMap, ReductError> {
        if let Some(version) = self.client.get_api_version().await {
            if version.1 < 18 {
                return Err(ReductError::new(
                    ErrorCode::InvalidRequest,
                    "Multi-entry batch writes are not supported in API versions below v1.18",
                ));
            }
        }

        if self.records.is_empty() {
            return Err(ReductError::new(
                ErrorCode::InvalidRequest,
                "Batch must contain at least one record",
            ));
        }

        let mut entries = Vec::new();
        let mut entry_index = HashMap::new();
        for record in &self.records {
            if record.entry().is_empty() {
                return Err(ReductError::new(
                    ErrorCode::InvalidRequest,
                    "Record entry name is required for multi-entry batch writes",
                ));
            }

            if !entry_index.contains_key(record.entry()) {
                let index = entries.len();
                entries.push(record.entry().to_string());
                entry_index.insert(record.entry().to_string(), index);
            }
        }

        let mut records: Vec<Record> = self.records.drain(..).collect();
        let start_ts = records
            .iter()
            .map(|record| record.timestamp_us())
            .min()
            .unwrap();

        records.sort_by(|left, right| {
            let left_idx = entry_index.get(left.entry()).unwrap();
            let right_idx = entry_index.get(right.entry()).unwrap();
            left_idx
                .cmp(right_idx)
                .then_with(|| left.timestamp_us().cmp(&right.timestamp_us()))
        });

        let content_length: usize = records.iter().map(|r| r.content_length()).sum();

        let mut request = self
            .client
            .request(Method::POST, &format!("/io/{}/write", self.bucket))
            .header(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            )
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&content_length.to_string()).unwrap(),
            )
            .header(
                "x-reduct-start-ts",
                HeaderValue::from_str(&start_ts.to_string()).unwrap(),
            )
            .header(
                "x-reduct-entries",
                HeaderValue::from_str(&encode_entries(&entries)).unwrap(),
            );

        for record in &records {
            let idx = *entry_index.get(record.entry()).unwrap();
            let delta = record.timestamp_us() - start_ts;
            let value = make_record_header_value(record);
            request = request.header(
                make_batched_header_name(idx, delta),
                HeaderValue::from_str(&value).unwrap(),
            );
        }

        let client = Arc::clone(&self.client);
        let stream = stream! {
            for record in records {
                let mut stream = record.stream_bytes();
                while let Some(bytes) = stream.next().await {
                    yield bytes;
                }
            }
        };

        let response = client
            .send_request(request.body(Body::wrap_stream(stream)))
            .await?;

        let mut failed_records = FailedRecordMap::new();
        response
            .headers()
            .iter()
            .filter(|(key, _)| key.as_str().starts_with("x-reduct-error-"))
            .for_each(|(key, value)| {
                if let Some((entry_idx, delta)) = parse_error_key(key.as_str()) {
                    if let Some(entry) = entries.get(entry_idx) {
                        if let Some((status, message)) = value.to_str().unwrap().split_once(',') {
                            if let Ok(status) = status.parse::<i16>() {
                                if let Ok(code) = ErrorCode::try_from(status) {
                                    failed_records.insert(
                                        (entry.to_string(), start_ts + delta),
                                        ReductError::new(code, message),
                                    );
                                }
                            }
                        }
                    }
                }
            });

        Ok(failed_records)
    }

    /// Get the size of the batch in bytes.
    pub fn size(&self) -> usize {
        self.records.iter().map(|r| r.content_length()).sum()
    }

    /// Get the number of records in the batch.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Get the last time a record was added to the batch.
    ///
    /// Can be used for sending the batch after a certain period of time.
    pub fn last_access(&self) -> SystemTime {
        self.last_access
    }

    /// Clear the batch of records.
    pub fn clear(&mut self) {
        self.records.clear();
    }
}

fn encode_entries(entries: &[String]) -> String {
    entries
        .iter()
        .map(|entry| encode_entry_name(entry))
        .collect::<Vec<_>>()
        .join(",")
}

fn make_record_header_value(record: &Record) -> String {
    let content_type = if record.content_type().is_empty() {
        "application/octet-stream"
    } else {
        record.content_type()
    };

    let labels = record.labels();
    if labels.is_empty() {
        format!("{},{}", record.content_length(), content_type)
    } else {
        format!(
            "{},{},{}",
            record.content_length(),
            content_type,
            format_label_delta(labels)
        )
    }
}

fn format_label_delta(labels: &crate::Labels) -> String {
    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    pairs
        .into_iter()
        .map(|(key, value)| {
            if value.contains(',') {
                format!("{}=\"{}\"", key, value)
            } else {
                format!("{}={}", key, value)
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_error_key(key: &str) -> Option<(usize, u64)> {
    let suffix = key.strip_prefix("x-reduct-error-")?;
    let (entry_idx, delta) = suffix.rsplit_once('-')?;
    Some((entry_idx.parse().ok()?, delta.parse().ok()?))
}
