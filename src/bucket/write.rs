// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::write_batched_records::WriteBatchType;
use crate::{Bucket, WriteBatchBuilder, WriteRecordBuilder};
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
}
