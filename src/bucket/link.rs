// Copyright 2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::Bucket;
use chrono::{DateTime, Utc};
use http::Method;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use std::sync::Arc;

pub struct CreateQueryLinkBuilder {
    entries: Vec<String>,
    request: QueryLinkCreateRequest,
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
        self.request.index = Some(index);
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
        let mut request = self.request;
        if self.entries.len() > 1 {
            if let Some(version) = self.http_client.get_api_version().await {
                if version.1 < 18 {
                    return Err(ReductError::new(
                        reduct_base::error::ErrorCode::InvalidRequest,
                        "Multi-entry query links are not supported in API versions below v1.18",
                    ));
                }
            }

            request.query.entries = Some(self.entries.clone());
        }

        let default_name = if self.entries.len() > 1 {
            request.bucket.clone()
        } else {
            request.entry.clone()
        };
        let file_name = self.file_name.unwrap_or(format!(
            "{}_{}.bin",
            default_name,
            request.index.unwrap_or(0)
        ));
        let response: QueryLinkCreateResponse = self
            .http_client
            .send_and_receive_json(
                Method::POST,
                &format!("/links/{}", file_name),
                Some(request),
            )
            .await?;
        Ok(response.link)
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
    use reduct_base::msg::entry_api::QueryEntry;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_link_creation(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
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
    #[tokio::test]
    async fn test_link_creation_with_index(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
            .index(1)
            .send()
            .await
            .unwrap();
        let body = reqwest::get(&link).await.unwrap().text().await.unwrap();
        assert_eq!(body, r#"{"detail": "Record number out of range"}"#)
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_expired(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let link = bucket
            .create_query_link("entry-1")
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
            .file_name("my-link.bin")
            .send()
            .await
            .unwrap();
        assert!(link.contains("links/my-link.bin?"));
    }
}
