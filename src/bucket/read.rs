// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Bucket, QueryBuilder, ReadRecordBuilder};
use std::sync::Arc;

impl Bucket {
    /// Create a record to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a record builder.
    pub fn read_record(&self, entry: &str) -> ReadRecordBuilder {
        ReadRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a record to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a record builder.
    ///
    /// ```no_run
    /// use reduct_rs::{ReductClient, ReductError};
    /// use std::time::SystemTime;
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///    let client = ReductClient::builder()
    ///         .url("https://play.reduct.store")
    ///         .api_token("reductstore")
    ///         .build();
    ///     let bucket = client.get_bucket("datasets").await?;
    ///     let query = bucket.query("cats").limit(10).send().await?;
    ///     tokio::pin!(query);
    ///     while let Some(record) = query.next().await {
    ///         let record = record?;
    ///         let content_ = record.bytes().await?;
    ///     }
    ///     Ok(())
    /// }
    ///  ```
    pub fn query(&self, entry: &str) -> QueryBuilder {
        QueryBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::tests::bucket;
    use crate::Bucket;
    use bytes::Bytes;
    use chrono::Duration;
    use futures::pin_mut;
    use futures_util::StreamExt;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[tokio::test]
    async fn test_read_record(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let record = bucket
            .read_record("entry-1")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.timestamp_us(), 1000);
        assert_eq!(record.content_length(), 12);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("bucket"), Some(&"1".to_string()));
        assert_eq!(record.labels().get("entry"), Some(&"1".to_string()));
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hey entry-1!"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_record_as_stream(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let record = bucket
            .read_record("entry-1")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        let mut stream = record.stream_bytes();
        assert_eq!(
            stream.next().await.unwrap(),
            Ok(Bytes::from("Hey entry-1!"))
        );
        assert_eq!(stream.next().await, None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_head_record(#[future] bucket: Bucket) {
        let record = bucket
            .await
            .read_record("entry-1")
            .timestamp_us(1000)
            .head_only(true)
            .send()
            .await
            .unwrap();

        assert_eq!(record.timestamp_us(), 1000);
        assert_eq!(record.content_length(), 12);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("bucket"), Some(&"1".to_string()));
        assert_eq!(record.labels().get("entry"), Some(&"1".to_string()));
    }

    #[rstest]
    #[case(true, 10)]
    #[case(false, 100)]
    #[case(false, 10_000)]
    #[case(false, 20_000_000)]
    #[tokio::test]
    async fn test_query(#[future] bucket: Bucket, #[case] head_only: bool, #[case] size: usize) {
        let bucket: Bucket = bucket.await;
        let mut bodies: Vec<Vec<u8>> = Vec::new();
        for i in 0..20usize {
            let mut content = Vec::with_capacity(size);
            for _j in 0..size {
                content.push(i as u8);
            }
            bodies.push(content);

            bucket
                .write_record("entry-3")
                .timestamp_us(i as u64)
                .data(Bytes::from(bodies[i].clone()))
                .send()
                .await
                .unwrap();
        }

        let query = bucket
            .query("entry-3")
            .ttl(Duration::minutes(1).to_std().unwrap())
            .head_only(head_only)
            .send()
            .await
            .unwrap();
        pin_mut!(query);

        for i in 0..20usize {
            let record = query.next().await.unwrap().unwrap();
            assert_eq!(record.timestamp_us(), i as u64);
            assert_eq!(record.content_length(), size);
            assert_eq!(record.content_type(), "application/octet-stream");

            if !head_only {
                assert_eq!(
                    record.bytes().await.unwrap(),
                    Bytes::from(bodies[i].clone())
                );
            }
        }

        assert!(query.next().await.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_each_second(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket.query("entry-2").each_s(0.002).send().await.unwrap();

        pin_mut!(query);
        let rec = query.next().await.unwrap().unwrap();
        assert_eq!(rec.timestamp_us(), 2000);
        let rec = query.next().await.unwrap().unwrap();
        assert_eq!(rec.timestamp_us(), 4000);
        assert!(query.next().await.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_query_each_minute(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket.query("entry-2").each_n(2).send().await.unwrap();

        pin_mut!(query);
        let rec = query.next().await.unwrap().unwrap();
        assert_eq!(rec.timestamp_us(), 2000);
        let rec = query.next().await.unwrap().unwrap();
        assert_eq!(rec.timestamp_us(), 4000);
        assert!(query.next().await.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_limit_query(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket.query("entry-1").limit(1).send().await.unwrap();

        pin_mut!(query);
        let _ = query.next().await.unwrap().unwrap();
        assert!(query.next().await.is_none());
    }

    #[rstest]
    #[tokio::test]
    #[cfg_attr(not(feature = "test-api-113"), ignore)]
    async fn test_query_when(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket
            .query("entry-1")
            .when(json!({
                "&entry": { "$eq": 1}
            }))
            .send()
            .await;

        let query = query.unwrap();
        pin_mut!(query);

        let rec = query.next().await.unwrap().unwrap();
        assert_eq!(rec.timestamp_us(), 1000);

        assert!(query.next().await.is_none());
    }

    #[rstest]
    #[tokio::test]
    #[cfg_attr(not(feature = "test-api-113"), ignore)]
    async fn test_query_when_strict(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket
            .query("entry-1")
            .when(json!({
                "&NOT_EXIST": { "$eq": 1}
            }))
            .send()
            .await;

        let query = query.unwrap();
        pin_mut!(query);
        assert!(query.next().await.is_none());

        let query = bucket
            .query("entry-1")
            .when(json!({
                "&NOT_EXIST": { "$eq": 1}
            }))
            .strict(true)
            .send()
            .await;

        let query = query.unwrap();
        pin_mut!(query);
        assert!(query.next().await.unwrap().is_err());
    }
}
