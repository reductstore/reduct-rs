// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::update_record::UpdateRecordBuilder;
use crate::record::write_batched_records_v1::WriteBatchType;
use crate::{Bucket, WriteBatchBuilder};
use std::sync::Arc;

impl Bucket {
    /// Update labels of a record in an entry.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to update.
    ///
    /// # Returns
    ///
    /// Returns a builder to update the record and send the request.
    pub fn update_record(&self, entry: &str) -> UpdateRecordBuilder {
        UpdateRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a batch to update records in the bucket.
    ///
    /// You should use RecordBuilder to create the records to update.
    /// Add labels to the record to update. Labels with an empty value will be removed.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to update.
    ///
    /// # Returns
    ///
    /// Returns a batch builder.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tokio;
    /// use reduct_rs::{ReductClient, ReductError};
    /// use reduct_rs::RecordBuilder;
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///     let client = ReductClient::builder()
    ///        .url("https://play.reduct.store")
    ///        .api_token("reductstore")
    ///        .build();
    ///     let bucket = client.get_bucket("datasets").await?;
    ///     let batch = bucket.update_batch("cats");
    ///     let record1 = RecordBuilder::new()
    ///         .timestamp_us(1000)
    ///         .add_label("test".to_string(), "2".to_string())
    ///         .add_label("x".to_string(), "".to_string()) // Remove label x
    ///         .build();
    ///
    ///     batch.add_record(record1).send().await?;
    ///     Ok(())
    /// }
    pub fn update_batch(&self, entry: &str) -> WriteBatchBuilder {
        WriteBatchBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
            WriteBatchType::Update,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::tests::bucket;
    use crate::{Bucket, RecordBuilder};
    use bytes::Bytes;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_update_record(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .data(Bytes::from("Hey"))
            .add_label("test", "1")
            .add_label("x", "y")
            .send()
            .await
            .unwrap();

        bucket
            .update_record("test")
            .timestamp_us(1000)
            .update_label("test", "2")
            .remove_label("x")
            .send()
            .await
            .unwrap();

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.labels().get("test"), Some(&"2".to_string()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_record_batched(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .data(Bytes::from("Hey"))
            .add_label("test", "1")
            .add_label("x", "y")
            .send()
            .await
            .unwrap();

        let batch = bucket.update_batch("test");
        let record1 = RecordBuilder::new()
            .timestamp_us(1000)
            .add_label("test".to_string(), "2".to_string())
            .add_label("x".to_string(), "".to_string())
            .build();
        let record2 = RecordBuilder::new()
            .timestamp_us(10000)
            .add_label("test".to_string(), "3".to_string())
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();

        assert_eq!(error_map.len(), 1);
        assert_eq!(error_map.get(&10000).unwrap().status, ErrorCode::NotFound);

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.labels().get("test"), Some(&"2".to_string()));
        assert_eq!(record.labels().get("x"), None);
    }
}
