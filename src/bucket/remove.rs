// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::query::RemoveQueryBuilder;
use crate::record::write_batched_records_v1::WriteBatchType;
use crate::{Bucket, RemoveRecordBuilder, WriteBatchBuilder, WriteRecordBatchBuilder};
use http::Method;
use reduct_base::error::ReductError;

impl Bucket {
    /// Remove an entry from the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove.
    ///
    /// # Returns
    ///
    /// Returns an error if the entry could not be removed.
    pub async fn remove_entry(&self, entry: &str) -> Result<(), ReductError> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/b/{}/{}", self.name, entry));
        self.http_client.send_request(request).await?;
        Ok(())
    }

    /// Remove a record from an entry.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove.
    ///
    /// # Returns
    ///
    /// Returns an error if the record could not be removed.
    pub fn remove_record(&self, entry: &str) -> RemoveRecordBuilder {
        RemoveRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            self.http_client.clone(),
        )
    }

    /// Remove records in a batch.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove.
    ///
    /// # Returns
    ///
    /// Returns a write batch builder.
    ///
    /// # Example
    ///
    /// ```no_run
    ///
    /// use reduct_rs::{ReductClient, ReductError};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///     let client = ReductClient::builder()
    ///         .url("https://play.reduct.store")
    ///         .api_token("reductstore")
    ///         .build();
    ///     let bucket = client.get_bucket("datasets").await?;
    ///     let batch = bucket.remove_batch("cats");
    ///     let errors = batch.add_timestamp_us(1000).add_timestamp_us(5000).send().await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn remove_batch(&self, entry: &str) -> WriteBatchBuilder {
        WriteBatchBuilder::new(
            self.name.clone(),
            entry.to_string(),
            self.http_client.clone(),
            WriteBatchType::Remove,
        )
    }

    /// Remove records across multiple entries in the bucket.
    ///
    /// You should specify entry names in each record when using this method.
    ///
    /// # Returns
    ///
    /// Returns a batch builder.
    pub fn remove_record_batch(&self) -> WriteRecordBatchBuilder {
        WriteRecordBatchBuilder::new(
            self.name.clone(),
            self.http_client.clone(),
            WriteBatchType::Remove,
        )
    }

    /// Remove records in a query.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to remove.
    ///
    /// # Returns
    ///
    /// Returns a remove query builder.
    ///
    /// # Example
    ///
    /// ```no_run
    ///
    /// use reduct_rs::{ReductClient, ReductError};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///     let client = ReductClient::builder()
    ///         .url("https://play.reduct.store")
    ///         .api_token("reductstore")
    ///         .build();
    ///
    ///     let bucket = client.get_bucket("datasets").await?;
    ///     let query = bucket.remove_query("cats");
    ///     let removed_records = query.start_us(1000).stop_us(5000).send().await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn remove_query<In: super::read::IntoEntryList>(&self, entry: In) -> RemoveQueryBuilder {
        RemoveQueryBuilder::new(
            self.name.clone(),
            entry.into_entry_list(),
            self.http_client.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::tests::bucket;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[tokio::test]
    async fn remove_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket.remove_entry("entry-1").await.unwrap();
        let error = bucket.read_record("entry-1").send().await.err().unwrap();
        // After deletion, entry may be DELETING (Conflict) or gone (NotFound)
        assert!(
            error.status == ErrorCode::NotFound || error.status == ErrorCode::Conflict,
            "Expected NotFound or Conflict, got {:?}",
            error.status
        );
    }

    #[rstest]
    #[tokio::test]
    async fn remove_record(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .remove_record("entry-1")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        assert_eq!(
            bucket
                .read_record("entry-1")
                .timestamp_us(1000)
                .send()
                .await
                .err()
                .unwrap()
                .status,
            ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn remove_batch(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;

        let batch = bucket.remove_batch("entry-1");
        let errors = batch
            .add_timestamp_us(1000)
            .add_timestamp_us(5000)
            .send()
            .await
            .unwrap();

        assert_eq!(
            bucket
                .read_record("entry-1")
                .send()
                .await
                .err()
                .unwrap()
                .status,
            ErrorCode::NotFound
        );

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[&5000].status, ErrorCode::NotFound);
    }

    #[rstest]
    #[tokio::test]
    #[cfg(feature = "test-api-118")]
    async fn remove_batch_multi_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("entry-3")
            .timestamp_us(1000)
            .data(Bytes::from("A"))
            .send()
            .await
            .unwrap();
        bucket
            .write_record("entry-4")
            .timestamp_us(1000)
            .data(Bytes::from("B"))
            .send()
            .await
            .unwrap();

        let batch = bucket.remove_record_batch();
        let errors = batch
            .add_record(
                crate::RecordBuilder::new()
                    .entry("entry-3")
                    .timestamp_us(1000)
                    .build(),
            )
            .add_record(
                crate::RecordBuilder::new()
                    .entry("entry-4")
                    .timestamp_us(1000)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        assert!(errors.is_empty());
        for entry in ["entry-3", "entry-4"] {
            assert_eq!(
                bucket
                    .read_record(entry)
                    .timestamp_us(1000)
                    .send()
                    .await
                    .err()
                    .unwrap()
                    .status,
                ErrorCode::NotFound
            );
        }
    }

    #[rstest]
    #[tokio::test]
    #[cfg(feature = "test-api-118")]
    async fn remove_batch_multi_entry_with_error(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("entry-3")
            .timestamp_us(1000)
            .data(Bytes::from("A"))
            .send()
            .await
            .unwrap();

        let batch = bucket.remove_record_batch();
        let errors = batch
            .add_record(
                crate::RecordBuilder::new()
                    .entry("entry-3")
                    .timestamp_us(1000)
                    .build(),
            )
            .add_record(
                crate::RecordBuilder::new()
                    .entry("entry-4")
                    .timestamp_us(2000)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors.get(&(String::from("entry-4"), 2000)).unwrap().status,
            ErrorCode::NotFound
        );
        assert_eq!(
            bucket
                .read_record("entry-3")
                .timestamp_us(1000)
                .send()
                .await
                .err()
                .unwrap()
                .status,
            ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn remove_query(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;

        let query = bucket.remove_query("entry-1");
        let removed_records = query.start_us(1000).stop_us(5000).send().await.unwrap();

        assert_eq!(
            bucket
                .read_record("entry-1")
                .timestamp_us(1000)
                .send()
                .await
                .err()
                .unwrap()
                .status,
            ErrorCode::NotFound
        );

        assert_eq!(removed_records, 1);
    }

    #[rstest]
    #[tokio::test]
    #[cfg(feature = "test-api-118")]
    async fn remove_query_multi_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;

        let removed_records = bucket
            .remove_query(&["entry-1", "entry-2"])
            .start_us(1000)
            .stop_us(4000)
            .send()
            .await
            .unwrap();

        assert_eq!(removed_records, 3);

        for (entry, timestamp) in [("entry-1", 1000), ("entry-2", 2000), ("entry-2", 3000)] {
            assert_eq!(
                bucket
                    .read_record(entry)
                    .timestamp_us(timestamp)
                    .send()
                    .await
                    .err()
                    .unwrap()
                    .status,
                ErrorCode::NotFound
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_query_when(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket
            .remove_query("entry-1")
            .when(json!({
                "&entry": { "$eq": 1}
            }))
            .send()
            .await;

        let removed_records = query.unwrap();
        assert_eq!(removed_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_query_when_strict(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let query = bucket
            .remove_query("entry-1")
            .when(json!({
                "&NOT_EXIST": { "$eq": 1}
            }))
            .send()
            .await;

        let removed_records = query.unwrap();
        assert_eq!(removed_records, 0);

        let query = bucket
            .remove_query("entry-1")
            .when(json!({
                "&NOT_EXIST": { "$eq": 1}
            }))
            .strict(true)
            .send()
            .await;

        assert!(query.is_err());
    }
}
