// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::Record;
use async_stream::stream;
use futures_util::StreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};
use std::collections::{BTreeMap, VecDeque};

use reduct_base::error::{ErrorCode, IntEnum, ReductError};
use std::sync::Arc;

pub(crate) enum WriteBatchType {
    Write,
    Update,
}

/// Builder for writing or updating multiple records in a single request.
pub struct WriteBatchBuilder {
    bucket: String,
    entry: String,
    batch_type: WriteBatchType,
    records: VecDeque<Record>,
    client: Arc<HttpClient>,
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
            client,
        }
    }

    /// Add a record to the batch.
    pub fn add_record(mut self, record: Record) -> Self {
        self.records.push_back(record);
        self
    }

    /// Add records to the batch.
    pub fn add_records(mut self, records: Vec<Record>) -> Self {
        self.records.extend(records);
        self
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
        };

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
                        ErrorCode::from_int(status.parse().unwrap()).unwrap(),
                        message,
                    ),
                );
            });

        Ok(failed_records)
    }
}
