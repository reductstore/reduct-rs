// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::record::from_system_time;
use http::Method;
use reduct_base::error::ReductError;
use std::sync::Arc;

/// Builder for a remove record request.
pub struct RemoveRecordBuilder {
    bucket: String,
    entry: String,
    timestamp: Option<u64>,
    client: Arc<HttpClient>,
}

impl RemoveRecordBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            bucket,
            entry,
            timestamp: None,
            client,
        }
    }

    /// Set the timestamp of the record to remove as a unix timestamp in microseconds.
    pub fn timestamp_us(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set the timestamp of the record to remove.
    pub fn timestamp(mut self, timestamp: std::time::SystemTime) -> Self {
        self.timestamp = Some(from_system_time(timestamp));
        self
    }

    /// Send the remove record request.
    ///
    /// # Returns
    ///
    /// Returns an error if the record could not be removed.
    ///
    /// # Panics
    ///
    /// Panics if the timestamp is not set.
    pub async fn send(self) -> Result<(), ReductError> {
        let request = self.client.request(
            Method::DELETE,
            &format!(
                "/b/{}/{}?ts={}",
                self.bucket,
                self.entry,
                self.timestamp.expect("timestamp is required")
            ),
        );
        self.client.send_request(request).await?;
        Ok(())
    }
}
