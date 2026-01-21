// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::write_batched_records_v1::WriteBatchType;
use crate::{Bucket, WriteBatchBuilder, WriteRecordBatchBuilder, WriteRecordBuilder};
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use reduct_rs::{ReductClient, ReductError};
    /// use std::time::SystemTime;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///    let client = ReductClient::builder()
    ///         .url("https://play.reduct.store")
    ///         .api_token("reductstore")
    ///         .build();
    ///     let bucket = client.get_bucket("demo").await?;
    ///     let record = bucket.write_record("entry-1").timestamp_us(1000).data("Some data").send().await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn write_record(&self, entry: &str) -> WriteRecordBuilder {
        WriteRecordBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
        )
    }

    /// Create a batch to write to the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to write to.
    ///
    /// # Returns
    ///
    /// Returns a batch builder.
    pub fn write_batch(&self, entry: &str) -> WriteBatchBuilder {
        WriteBatchBuilder::new(
            self.name.clone(),
            entry.to_string(),
            Arc::clone(&self.http_client),
            WriteBatchType::Write,
        )
    }

    /// Create a batch to write records to multiple entries in the bucket.
    ///
    /// You should specify entry names in each record when using this method.
    ///
    /// # Returns
    ///
    ///  Returns a batch builder.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use reduct_rs::{ReductClient, ReductError, RecordBuilder};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), ReductError> {
    ///    let client = ReductClient::builder()
    ///        .url("https://play.reduct.store")
    ///       .api_token("reductstore")
    ///       .build();
    ///
    ///   let bucket = client.get_bucket("demo").await?;
    ///   let batch = bucket.write_record_batch();
    ///
    ///   let record1 = RecordBuilder::new()
    ///       .entry("entry-1")
    ///       .timestamp_us(1000)
    ///       .data("Data for entry 1")
    ///       .build()
    ///
    ///  let record2 = RecordBuilder::new()
    ///      .entry("entry-2")
    ///      .timestamp_us(1000)
    ///      .data("Data for entry 2")
    ///      .build();
    ///
    ///  batch.add_record(record1).add_record(record2).send().await?;
    ///  Ok(())
    /// }
    ///
    pub fn write_record_batch(&self) -> WriteRecordBatchBuilder {
        WriteRecordBatchBuilder::new(
            self.name.clone(),
            Arc::clone(&self.http_client),
            WriteBatchType::Write,
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
    async fn test_bucket_write_record_data(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .data(Bytes::from("Hey"))
            .send()
            .await
            .unwrap();

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hey"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_write_record_stream(#[future] bucket: Bucket) {
        let chunks: Vec<crate::client::Result<_>> = vec![Ok("hello"), Ok(" "), Ok("world")];

        let stream = futures_util::stream::iter(chunks);

        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .content_length(11)
            .stream(Box::pin(stream))
            .send()
            .await
            .unwrap();

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("hello world"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_write(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let batch = bucket.write_batch("test");

        let record1 = RecordBuilder::new()
            .timestamp_us(1000)
            .data(Bytes::from("Hey,"))
            .add_label("test", "1")
            .content_type("text/plain")
            .build();

        let stream = futures_util::stream::iter(vec![Ok(Bytes::from("World"))]);

        let record2 = RecordBuilder::new()
            .timestamp_us(2000)
            .stream(Box::pin(stream))
            .add_label("test", "2")
            .content_type("text/plain")
            .content_length(5)
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();
        assert!(error_map.is_empty());

        let record = bucket
            .read_record("test")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.content_length(), 4);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("test"), Some(&"1".to_string()));
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hey,"));

        let record = bucket
            .read_record("test")
            .timestamp_us(2000)
            .send()
            .await
            .unwrap();

        assert_eq!(record.content_length(), 5);
        assert_eq!(record.content_type(), "text/plain");
        assert_eq!(record.labels().get("test"), Some(&"2".to_string()));
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("World"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_write_multi_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let batch = bucket.write_record_batch();

        let record1 = RecordBuilder::new()
            .entry("entry-3")
            .timestamp_us(1000)
            .data(Bytes::from("Hello"))
            .build();

        let record2 = RecordBuilder::new()
            .entry("entry-4")
            .timestamp_us(1000)
            .data(Bytes::from("World"))
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();
        assert!(error_map.is_empty());

        let record = bucket
            .read_record("entry-3")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("Hello"));

        let record = bucket
            .read_record("entry-4")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("World"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_write_multi_entry_with_error(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("entry-3")
            .timestamp_us(1000)
            .data(Bytes::from("exists"))
            .send()
            .await
            .unwrap();

        let batch = bucket.write_record_batch();
        let record1 = RecordBuilder::new()
            .entry("entry-3")
            .timestamp_us(1000)
            .data(Bytes::from("conflict"))
            .build();
        let record2 = RecordBuilder::new()
            .entry("entry-4")
            .timestamp_us(1000)
            .data(Bytes::from("ok"))
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();

        assert_eq!(error_map.len(), 1);
        let error = error_map.get(&(String::from("entry-3"), 1000)).unwrap();
        assert_eq!(error.status, ErrorCode::Conflict);
        assert_eq!(error.message, "A record with timestamp 1000 already exists");

        let record = bucket
            .read_record("entry-4")
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();
        assert_eq!(record.bytes().await.unwrap(), Bytes::from("ok"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_write_with_error(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket
            .write_record("test")
            .timestamp_us(1000)
            .data(Bytes::from("xxx"))
            .send()
            .await
            .unwrap();

        let batch = bucket.write_batch("test");
        let record1 = RecordBuilder::new()
            .timestamp_us(1000)
            .data(Bytes::from("Hey,"))
            .build();
        let record2 = RecordBuilder::new()
            .timestamp_us(2000)
            .data(Bytes::from("World"))
            .build();

        let error_map = batch
            .add_record(record1)
            .add_record(record2)
            .send()
            .await
            .unwrap();

        assert_eq!(error_map.len(), 1);
        assert_eq!(error_map.get(&1000).unwrap().status, ErrorCode::Conflict);
        assert_eq!(
            error_map.get(&1000).unwrap().message,
            "A record with timestamp 1000 already exists"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_batch_helper_methods(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        let mut batch = bucket.write_batch("test");

        batch.append_record(
            RecordBuilder::new()
                .timestamp_us(1000)
                .data(Bytes::from("Hey,"))
                .build(),
        );
        batch.append_record(
            RecordBuilder::new()
                .timestamp_us(2000)
                .data(Bytes::from("World"))
                .build(),
        );

        assert_eq!(batch.record_count(), 2);
        assert_eq!(batch.size(), 9);

        batch.clear();
        assert_eq!(batch.record_count(), 0);
        assert_eq!(batch.size(), 0);
    }
}
