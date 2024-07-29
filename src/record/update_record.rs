// Copyright 2023-2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::record::{from_system_time, Labels};

use reqwest::header::CONTENT_LENGTH;
use reqwest::Method;

use reduct_base::error::{ErrorCode, ReductError};
use std::sync::Arc;
use std::time::SystemTime;

/// Builder for a write record request.
pub struct UpdateRecordBuilder {
    bucket: String,
    entry: String,
    timestamp: Option<u64>,
    labels: Labels,
    client: Arc<HttpClient>,
}

impl UpdateRecordBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            timestamp: None,
            labels: Labels::new(),
            bucket,
            entry,
            client,
        }
    }

    /// Set the timestamp of the record to write.
    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(from_system_time(timestamp));
        self
    }

    /// Set the timestamp of the record to write as a unix timestamp in microseconds.
    pub fn timestamp_us(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the labels of the record to update.
    ///
    /// if the label already exists, it will be updated.
    /// if the label does not exist, it will be added.
    /// if the value is empty, the label will be removed.
    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    /// Add a label to the record to update.
    pub fn update_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    /// Add a label to the record to remove.
    pub fn remove_label(mut self, key: &str) -> Self {
        self.labels.insert(key.to_string(), "".to_string());
        self
    }

    /// Send the write record request.
    pub async fn send(self) -> Result<(), ReductError> {
        let timestamp = match self.timestamp {
            Some(ts) => ts,
            None => {
                return Err(ReductError::new(
                    ErrorCode::UnprocessableEntity,
                    "timestamp is required",
                ))
            }
        };

        let mut request = self.client.request(
            Method::PATCH,
            &format!("/b/{}/{}?ts={}", self.bucket, self.entry, timestamp),
        );

        for (key, value) in self.labels {
            request = request.header(&format!("x-reduct-label-{}", key), value);
        }

        request = request.header(CONTENT_LENGTH, 0);
        self.client.send_request(request).await?;
        Ok(())
    }
}
