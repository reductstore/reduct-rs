// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::write_batched_records::WriteBatchType;
use crate::{Bucket, WriteBatchBuilder};
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
    /// * `timestamp` - The timestamp of the record to remove.
    ///
    /// # Returns
    ///
    /// Returns an error if the record could not be removed.
    pub async fn remove_record(&self, entry: &str, timestamp: u64) -> Result<(), ReductError> {
        let request = self.http_client.request(
            Method::DELETE,
            &format!("/b/{}/{}?ts={}", self.name, entry, timestamp),
        );
        self.http_client.send_request(request).await?;
        Ok(())
    }

    pub fn remove_batch(&self, entry: &str) -> WriteBatchBuilder {
        WriteBatchBuilder::new(
            self.name.clone(),
            entry.to_string(),
            self.http_client.clone(),
            WriteBatchType::Remove,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::tests::bucket;
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn remove_entry(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket.remove_entry("entry-1").await.unwrap();
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
    }

    #[rstest]
    #[tokio::test]
    async fn remove_record(#[future] bucket: Bucket) {
        let bucket: Bucket = bucket.await;
        bucket.remove_record("entry-1", 1000).await.unwrap();
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
}
