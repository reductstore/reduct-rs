// Copyright 2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::Bucket;
use chrono::{DateTime, Utc};
use http::Method;
use reduct_base::batch::sort_headers_by_time;
use reduct_base::batch::v2::parse_batched_headers;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo, QueryType};
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
struct QueryLinkCreateRequestCompat {
    bucket: String,
    entry: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_entry: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<u64>,
    query: QueryEntry,
    #[serde(serialize_with = "chrono::serde::ts_seconds::serialize")]
    expire_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_url: Option<String>,
}

pub struct CreateQueryLinkBuilder {
    entries: Vec<String>,
    request: QueryLinkCreateRequest,
    selected_index: Option<u64>,
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
            selected_index: None,
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
        self.selected_index = Some(index);
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
        let mut request = self.request.clone();
        let selected_index = self.selected_index;

        if self.entries.len() > 1 {
            if let Some(version) = self.http_client.get_api_version().await {
                if version.1 < 18 {
                    return Err(ReductError::new(
                        ErrorCode::InvalidRequest,
                        "Multi-entry query links are not supported in API versions below v1.18",
                    ));
                }
            }

            request.query.entries = Some(self.entries.clone());
        }

        let record_identity = match selected_index {
            Some(index) => Some(self.resolve_record_identity(&request, index).await?),
            None => None,
        };

        let payload = QueryLinkCreateRequestCompat {
            bucket: request.bucket.clone(),
            entry: request.entry.clone(),
            record_entry: record_identity.as_ref().map(|(entry, _)| entry.clone()),
            record_timestamp: record_identity.map(|(_, timestamp)| timestamp),
            index: selected_index,
            query: request.query.clone(),
            expire_at: request.expire_at,
            base_url: request.base_url.clone(),
        };

        let default_name = if self.entries.len() > 1 {
            payload.bucket.clone()
        } else {
            payload.entry.clone()
        };
        let file_name = self.file_name.unwrap_or(format!(
            "{}_{}.bin",
            default_name,
            payload.index.unwrap_or(0)
        ));

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

    async fn resolve_record_identity(
        &self,
        request: &QueryLinkCreateRequest,
        index: u64,
    ) -> Result<(String, u64), ReductError> {
        let mut query = request.query.clone();
        query.query_type = QueryType::Query;
        query.only_metadata = Some(true);

        let max_records = index.checked_add(1).ok_or_else(|| {
            ReductError::new(ErrorCode::InvalidRequest, "Record index is too large")
        })?;
        query.limit = Some(query.limit.unwrap_or(max_records).min(max_records));

        if self.entries.len() == 1 {
            self.resolve_record_identity_v1(request, query, index).await
        } else {
            self.resolve_record_identity_v2(request, query, index).await
        }
    }

    async fn resolve_record_identity_v1(
        &self,
        request: &QueryLinkCreateRequest,
        query: QueryEntry,
        index: u64,
    ) -> Result<(String, u64), ReductError> {
        let entry = request.entry.clone();
        let query_info: QueryInfo = self
            .http_client
            .send_and_receive_json(
                Method::POST,
                &format!("/b/{}/{}/q", request.bucket, entry),
                Some(query),
            )
            .await?;

        let mut remaining = index;
        loop {
            let response = self
                .http_client
                .send_request(self.http_client.request(
                    Method::HEAD,
                    &format!("/b/{}/{}/batch?q={}", request.bucket, entry, query_info.id),
                ))
                .await?;

            if response.status() == reqwest::StatusCode::NO_CONTENT {
                break;
            }

            let is_last = response
                .headers()
                .get("x-reduct-last")
                .and_then(|value| value.to_str().ok())
                == Some("true");

            for (timestamp, _) in sort_headers_by_time(response.headers())? {
                if remaining == 0 {
                    return Ok((entry, timestamp));
                }
                remaining -= 1;
            }

            if is_last {
                break;
            }
        }

        Err(ReductError::new(
            ErrorCode::UnprocessableEntity,
            "Record number out of range",
        ))
    }

    async fn resolve_record_identity_v2(
        &self,
        request: &QueryLinkCreateRequest,
        mut query: QueryEntry,
        index: u64,
    ) -> Result<(String, u64), ReductError> {
        query.entries = Some(self.entries.clone());
        let query_info: QueryInfo = self
            .http_client
            .send_and_receive_json(
                Method::POST,
                &format!("/io/{}/q", request.bucket),
                Some(query),
            )
            .await?;

        let mut remaining = index;
        loop {
            let response = self
                .http_client
                .send_request(
                    self.http_client
                        .request(Method::HEAD, &format!("/io/{}/read", request.bucket))
                        .header("x-reduct-query-id", query_info.id.to_string()),
                )
                .await?;

            if response.status() == reqwest::StatusCode::NO_CONTENT {
                break;
            }

            let is_last = response
                .headers()
                .get("x-reduct-last")
                .and_then(|value| value.to_str().ok())
                == Some("true");

            for record in parse_batched_headers(response.headers())? {
                if remaining == 0 {
                    return Ok((record.entry, record.timestamp));
                }
                remaining -= 1;
            }

            if is_last {
                break;
            }
        }

        Err(ReductError::new(
            ErrorCode::UnprocessableEntity,
            "Record number out of range",
        ))
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
            .index(0)
            .send()
            .await
            .unwrap();
        let body = reqwest::get(&link).await.unwrap().text().await.unwrap();
        assert_eq!(body, "Hey entry-1!");
    }

    #[rstest]
    #[tokio::test]
    async fn test_link_creation_with_index_out_of_range(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let err = bucket
            .create_query_link("entry-1")
            .index(1)
            .send()
            .await
            .unwrap_err();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert_eq!(err.message(), "Record number out of range");
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
