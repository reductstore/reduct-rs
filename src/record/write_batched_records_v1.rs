// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::{Record, RecordBuilder};
use async_stream::stream;
use futures_util::StreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};
use std::collections::{BTreeMap, VecDeque};

use reduct_base::error::{ErrorCode, ReductError};
use std::sync::Arc;
use std::time::SystemTime;

pub(crate) enum WriteBatchType {
    Write,
    Update,
    Remove,
}

/// Builder for writing or updating multiple records in a single request.
pub struct WriteBatchBuilder {
    bucket: String,
    entry: String,
    batch_type: WriteBatchType,
    records: VecDeque<Record>,
    sync: bool,
    client: Arc<HttpClient>,
    last_access: SystemTime,
}

type FailedRecordMap = BTreeMap<u64, ReductError>;

impl WriteBatchBuilder {
    pub(crate) fn new(
        bucket: String,
        entry: String,
        client: Arc<HttpClient>,
        batch_type: WriteBatchType,
    ) -> Self {
        Self {
            bucket,
            entry,
            batch_type,
            records: VecDeque::new(),
            sync: false,
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

    /// Set whether the write must be synchronized with durable storage.
    ///
    /// This setting only applies to write batches.
    pub fn sync(mut self, sync: bool) -> Self {
        self.sync = sync;
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

    /// Add an empty record to the batch with the given timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The UNIX timestamp in microseconds of the record.
    ///
    /// # Returns
    ///
    /// Returns the builder for chaining.
    pub fn add_timestamp_us(mut self, timestamp: u64) -> Self {
        self.records
            .push_back(RecordBuilder::new().timestamp_us(timestamp).build());
        self.last_access = SystemTime::now();
        self
    }

    /// Add an empty record to the batch with the given timestamp without chaining.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The UNIX timestamp in microseconds of the record.
    ///
    pub fn append_timestamp_us(&mut self, timestamp: u64) {
        self.records
            .push_back(RecordBuilder::new().timestamp_us(timestamp).build());
        self.last_access = SystemTime::now();
    }

    /// Add an empty record to the batch with the given timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// Returns the builder for chaining.
    pub fn add_timestamp(mut self, timestamp: SystemTime) -> Self {
        self.records
            .push_back(RecordBuilder::new().timestamp(timestamp).build());
        self.last_access = SystemTime::now();
        self
    }

    /// Add an empty record to the batch with the given timestamp without chaining.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp of the record.
    ///
    pub fn append_timestamp(&mut self, timestamp: SystemTime) {
        self.records
            .push_back(RecordBuilder::new().timestamp(timestamp).build());
        self.last_access = SystemTime::now();
    }

    /// Add a vector of empty records to the batch with the given timestamps.
    ///
    /// # Arguments
    ///
    /// * `timestamps` - The UNIX timestamps in microseconds of the records.
    ///
    /// # Returns
    ///
    /// Returns the builder for chaining.
    ///
    pub fn add_timestamps_us(mut self, timestamps: Vec<u64>) -> Self {
        self.records.extend(
            timestamps
                .into_iter()
                .map(|t| RecordBuilder::new().timestamp_us(t).build()),
        );
        self.last_access = SystemTime::now();
        self
    }

    /// Add a vector of empty records to the batch with the given timestamps without chaining.
    ///
    /// # Arguments
    ///
    /// * `timestamps` - The UNIX timestamps in microseconds of the records.
    ///
    pub fn append_timestamps_us(&mut self, timestamps: Vec<u64>) {
        self.records.extend(
            timestamps
                .into_iter()
                .map(|t| RecordBuilder::new().timestamp_us(t).build()),
        );
        self.last_access = SystemTime::now();
    }

    /// Add an empty record to the batch with the given timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// Returns the builder for chaining.
    pub fn add_timestamps(mut self, timestamps: Vec<SystemTime>) -> Self {
        self.records.extend(
            timestamps
                .into_iter()
                .map(|t| RecordBuilder::new().timestamp(t).build()),
        );
        self.last_access = SystemTime::now();
        self
    }

    /// Add an empty record to the batch with the given timestamp without chaining.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp of the record.
    ///
    pub fn append_timestamps(&mut self, timestamps: Vec<SystemTime>) {
        self.records.extend(
            timestamps
                .into_iter()
                .map(|t| RecordBuilder::new().timestamp(t).build()),
        );
        self.last_access = SystemTime::now();
    }

    /// Build the request and send it to the server.
    ///
    /// # Returns
    ///
    /// Returns a map of timestamps to errors for any records that failed to write.
    ///
    /// # Errors
    ///
    /// * `ReductError` - If the request was not successful.
    pub async fn send(mut self) -> Result<FailedRecordMap, ReductError> {
        let method = match self.batch_type {
            WriteBatchType::Write => Method::POST,
            WriteBatchType::Update => Method::PATCH,
            WriteBatchType::Remove => Method::DELETE,
        };

        let request = self
            .client
            .request(method, &format!("/b/{}/{}/batch", self.bucket, self.entry));

        let content_length: usize = self.records.iter().map(|r| r.content_length()).sum();

        let mut request = request.header(
            CONTENT_TYPE,
            HeaderValue::from_str("application/octet-stream").unwrap(),
        );

        request = match self.batch_type {
            WriteBatchType::Update => {
                request.header(CONTENT_LENGTH, HeaderValue::from_str("0").unwrap())
            }

            WriteBatchType::Write => request.header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&content_length.to_string()).unwrap(),
            ),

            WriteBatchType::Remove => {
                request.header(CONTENT_LENGTH, HeaderValue::from_str("0").unwrap())
            }
        };

        if should_sync_write(self.sync, &self.batch_type) {
            request = request.header("x-reduct-sync-write", "true");
        }

        for record in &self.records {
            let mut header_values = Vec::new();
            match self.batch_type {
                WriteBatchType::Update => {
                    header_values.push("0".to_string());
                    header_values.push("".to_string());
                }
                WriteBatchType::Write => {
                    header_values.push(record.content_length().to_string());
                    header_values.push(record.content_type().to_string());
                }
                WriteBatchType::Remove => {
                    header_values.push("0".to_string());
                    header_values.push("".to_string());
                }
            }

            if !record.labels().is_empty() {
                for (key, value) in record.labels() {
                    if value.contains(',') {
                        header_values.push(format!("{}=\"{}\"", key, value));
                    } else {
                        header_values.push(format!("{}={}", key, value));
                    }
                }
            }

            request = request.header(
                &format!("x-reduct-time-{}", record.timestamp_us()),
                HeaderValue::from_str(&header_values.join(",").to_string()).unwrap(),
            );
        }

        let client = Arc::clone(&self.client);

        let response = match self.batch_type {
            WriteBatchType::Update => client.send_request(request).await?,

            WriteBatchType::Write => {
                let stream = stream! {
                 while let Some(record) = self.records.pop_front() {
                     let mut stream = record.stream_bytes();
                     while let Some(bytes) = stream.next().await {
                         yield bytes;
                     }
                    }
                };

                client
                    .send_request(request.body(Body::wrap_stream(stream)))
                    .await?
            }

            WriteBatchType::Remove => client.send_request(request).await?,
        };

        let mut failed_records = FailedRecordMap::new();
        response
            .headers()
            .iter()
            .filter(|(key, _)| key.as_str().starts_with("x-reduct-error"))
            .for_each(|(key, value)| {
                let record_ts = key
                    .as_str()
                    .trim_start_matches("x-reduct-error-")
                    .parse::<u64>()
                    .unwrap();
                let (status, message) = value.to_str().unwrap().split_once(',').unwrap();
                failed_records.insert(
                    record_ts,
                    ReductError::new(
                        ErrorCode::try_from(status.parse::<i16>().unwrap()).unwrap(),
                        message,
                    ),
                );
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

fn should_sync_write(sync: bool, batch_type: &WriteBatchType) -> bool {
    sync && matches!(batch_type, WriteBatchType::Write)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_client::request_capture_server;
    use bytes::Bytes;
    use std::time::Duration;

    async fn capture_request(batch_type: WriteBatchType, sync: Option<bool>) -> String {
        let (url, receiver, server) = request_capture_server();
        let client = Arc::new(HttpClient::new(&url, "token", reqwest::Client::new()).unwrap());
        let batch = WriteBatchBuilder::new("bucket".into(), "entry".into(), client, batch_type);
        let batch = match sync {
            Some(sync) => batch.sync(sync),
            None => batch,
        };

        batch
            .add_record(
                RecordBuilder::new()
                    .timestamp_us(1)
                    .data(Bytes::from("data"))
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let request = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        server.join().unwrap();
        request
    }

    #[tokio::test]
    async fn sync_write_header_is_opt_in() {
        assert!(capture_request(WriteBatchType::Write, Some(true))
            .await
            .contains("x-reduct-sync-write: true\r\n"));
        assert!(!capture_request(WriteBatchType::Write, None)
            .await
            .contains("x-reduct-sync-write:"));
        assert!(!capture_request(WriteBatchType::Write, Some(false))
            .await
            .contains("x-reduct-sync-write:"));
    }

    #[tokio::test]
    async fn sync_write_header_is_not_sent_for_update_or_remove() {
        assert!(!capture_request(WriteBatchType::Update, Some(true))
            .await
            .contains("x-reduct-sync-write:"));
        assert!(!capture_request(WriteBatchType::Remove, Some(true))
            .await
            .contains("x-reduct-sync-write:"));
    }
}
