// Copyright 2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::Bucket;
use chrono::{DateTime, Utc};
use http::Method;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use std::sync::Arc;

pub struct CreateQueryLinkBuilder {
    entries: Vec<String>,
    request: QueryLinkCreateRequest,
    legacy_index: u64,
    index_explicit: bool,
    file_name: Option<String>,
    http_client: Arc<HttpClient>,
}

impl CreateQueryLinkBuilder {
    pub(crate) fn new(bucket: String, entries: Vec<String>, http_client: Arc<HttpClient>) -> Self {
        let entry = entries.first().cloned().unwrap_or_default();
        Self {
            entries,
            request: QueryLinkCreateRequest {
                bucket,
                entry,
                expire_at: Utc::now() + chrono::Duration::hours(24),
                ..Default::default()
            },
            legacy_index: 0,
            index_explicit: false,
            file_name: None,
            http_client,
        }
    }

    /// Set the expiration time for the query link.
    pub fn expire_at(mut self, expire_at: DateTime<Utc>) -> Self {
        self.request.expire_at = expire_at;
        self
    }

    /// Set the record index for the query link.
    pub fn index(mut self, index: u64) -> Self {
        self.legacy_index = index;
        self.index_explicit = true;
        self
    }

    /// Set the exact record identity for the query link.
    pub fn record(mut self, entry: &str, timestamp: u64) -> Self {
        self.request.record_entry = Some(entry.to_string());
        self.request.record_timestamp = Some(timestamp);
        self
    }

    /// Set the query to share.
    pub fn query(mut self, query: QueryEntry) -> Self {
        self.request.query = query;
        self
    }

    /// Set the file name for the query link.
    pub fn file_name(mut self, file_name: &str) -> Self {
        self.file_name = Some(file_name.to_string());
        self
    }

    /// Set the base URL for the query link.
    pub fn base_url(mut self, base_url: &str) -> Self {
        self.request.base_url = Some(base_url.to_string());
        self
    }

    /// Send the create query link request.
    pub async fn send(self) -> Result<String, ReductError> {
        let version = self.ensure_api_version().await?;
        let mut request = self.request.clone();

        if self.entries.len() > 1 {
            if version.1 < 18 {
                return Err(ReductError::new(
                    ErrorCode::InvalidRequest,
                    "Multi-entry query links are not supported in API versions below v1.18",
                ));
            }

            request.query.entries = Some(self.entries.clone());
        }

        if request.record_entry.is_some() ^ request.record_timestamp.is_some() {
            return Err(ReductError::new(
                ErrorCode::InvalidRequest,
                "Both record_entry and record_timestamp must be provided",
            ));
        }

        let has_record_identity = request.record_entry.is_some();
        let legacy_index = if self.index_explicit {
            Some(self.legacy_index)
        } else if has_record_identity {
            None
        } else {
            Some(0)
        };

        if version.1 >= 19 && !has_record_identity {
            return Err(ReductError::new(
                ErrorCode::InvalidRequest,
                "record entry and timestamp must be provided for ReductStore API v1.19+; use .record(entry, timestamp)",
            ));
        }

        let default_selector = legacy_index.or(request.record_timestamp).unwrap_or(0);
        let default_name = if self.entries.len() > 1 {
            request.bucket.clone()
        } else {
            request.entry.clone()
        };
        let file_name = self
            .file_name
            .unwrap_or(format!("{}_{}.bin", default_name, default_selector));
        let mut payload = serde_json::to_value(&request).map_err(|e| {
            ReductError::new(
                ErrorCode::Unknown,
                &format!("Failed to serialize query link request: {}", e),
            )
        })?;
        if let Some(index) = legacy_index {
            payload["index"] = serde_json::Value::from(index);
        }
        let response: QueryLinkCreateResponse = self
            .http_client
            .send_and_receive_json(
                Method::POST,
                &format!("/links/{}", file_name),
                Some(payload),
            )
            .await?;
        Ok(response.link)
    }

    async fn ensure_api_version(&self) -> Result<(u32, u32), ReductError> {
        if let Some(version) = self.http_client.get_api_version().await {
            return Ok(version);
        }

        let request = self.http_client.request(Method::HEAD, "/alive");
        self.http_client.send_request(request).await?;
        self.http_client.get_api_version().await.ok_or_else(|| {
            ReductError::new(
                ErrorCode::Unknown,
                "Failed to determine ReductStore API version",
            )
        })
    }
}

impl Bucket {
    /// Create a query link for sharing.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry or entries to create the query link for.
    ///
    /// # Returns
    ///
    /// Returns a builder for creating a query link.
    pub fn create_query_link<In: super::read::IntoEntryList>(
        &self,
        entry: In,
    ) -> CreateQueryLinkBuilder {
        CreateQueryLinkBuilder::new(
            self.name.clone(),
            entry.into_entry_list(),
            self.http_client.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::tests::bucket;
    use crate::Bucket;
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::entry_api::QueryEntry;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_link_creation(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
            .record("entry-1", 1000)
            .expire_at(chrono::Utc::now() + chrono::Duration::hours(1))
            .send()
            .await
            .unwrap();

        let body = reqwest::get(&link).await.unwrap().text().await.unwrap();
        assert_eq!(body, "Hey entry-1!");
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_with_query(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
            .record("entry-1", 1000)
            .query(QueryEntry {
                start: Some(0),
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        let body = reqwest::get(&link).await.unwrap().text().await.unwrap();
        assert_eq!(body, "Hey entry-1!");
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_multi_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link(&["entry-1", "entry-2"])
            .record("entry-1", 1000)
            .query(QueryEntry {
                start: Some(0),
                ..Default::default()
            })
            .send()
            .await
            .unwrap();

        assert!(link.contains("links/test-bucket-1_"));
        let body = reqwest::get(&link).await.unwrap().text().await.unwrap();
        assert_eq!(body, "Hey entry-1!");
    }

    #[rstest]
    #[cfg(feature = "test-api-119")]
    #[tokio::test]
    async fn test_link_creation_with_explicit_record_identity(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;

        let link = bucket
            .create_query_link("entry-2")
            .record("entry-2", 3000)
            .send()
            .await
            .unwrap();
        let body = reqwest::get(&link).await.unwrap().text().await.unwrap();
        assert_eq!(body, "0");
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_expired(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
            .record("entry-1", 1000)
            .expire_at(chrono::Utc::now() - chrono::Duration::hours(1))
            .send()
            .await
            .unwrap();
        let response = reqwest::get(&link).await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_file_name(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
            .record("entry-1", 1000)
            .file_name("my-link.bin")
            .send()
            .await
            .unwrap();
        assert!(link.contains("links/my-link.bin?"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_requires_record_identity_for_api_19(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let error = bucket
            .create_query_link("entry-1")
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.status(), ErrorCode::InvalidRequest);
        assert_eq!(
            error.message(),
            "record entry and timestamp must be provided for ReductStore API v1.19+; use .record(entry, timestamp)"
        );
    }
}
