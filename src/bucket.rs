// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod read;
mod remove;
mod update;
mod write;

use std::sync::Arc;

use reqwest::Method;

use reduct_base::error::ErrorCode;
use reduct_base::msg::bucket_api::{BucketInfo, BucketSettings, FullBucketInfo, QuotaType};
use reduct_base::msg::entry_api::{EntryInfo, RenameEntry};

use crate::client::Result;
use crate::http_client::HttpClient;

/// A bucket to store data in.
pub struct Bucket {
    pub(crate) name: String,
    pub(crate) http_client: Arc<HttpClient>,
}

pub struct BucketBuilder {
    name: String,
    exist_ok: bool,
    settings: BucketSettings,
    http_client: Arc<HttpClient>,
}

impl BucketBuilder {
    pub(crate) fn new(name: String, http_client: Arc<HttpClient>) -> Self {
        Self {
            name,
            exist_ok: false,
            settings: BucketSettings::default(),
            http_client,
        }
    }

    /// Don't fail if the bucket already exists.
    pub fn exist_ok(mut self, exist_ok: bool) -> Self {
        self.exist_ok = exist_ok;
        self
    }

    /// Set the quota type.
    pub fn quota_type(mut self, quota_type: QuotaType) -> Self {
        self.settings.quota_type = Some(quota_type);
        self
    }

    /// Set the quota size.
    pub fn quota_size(mut self, quota_size: u64) -> Self {
        self.settings.quota_size = Some(quota_size);
        self
    }

    /// Set the max block size.
    pub fn max_block_size(mut self, max_block_size: u64) -> Self {
        self.settings.max_block_size = Some(max_block_size);
        self
    }

    /// Set the max block records.
    pub fn max_block_records(mut self, max_block_records: u64) -> Self {
        self.settings.max_block_records = Some(max_block_records);
        self
    }

    /// Set and overwrite the settings of the bucket.
    pub fn settings(mut self, settings: BucketSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Create the bucket.
    pub async fn send(self) -> Result<Bucket> {
        let result = self
            .http_client
            .send_json(Method::POST, &format!("/b/{}", self.name), self.settings)
            .await;
        match result {
            Ok(_) => {}
            Err(e) => {
                if !(self.exist_ok && e.status() == ErrorCode::Conflict) {
                    return Err(e);
                }
            }
        }

        Ok(Bucket {
            name: self.name.clone(),
            http_client: Arc::clone(&self.http_client),
        })
    }
}

impl Bucket {
    /// Name of the bucket.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// URL of the server.
    pub fn server_url(&self) -> &str {
        &self.http_client.url()
    }

    /// Remove the bucket.
    ///
    /// # Returns
    ///
    /// Returns an error if the bucket could not be removed.
    pub async fn remove(&self) -> Result<()> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/b/{}", self.name));
        self.http_client.send_request(request).await?;
        Ok(())
    }

    /// Get the settings of the bucket.
    ///
    /// # Returns
    ///
    /// Return settings of the bucket
    pub async fn settings(&self) -> Result<BucketSettings> {
        Ok(self.full_info().await?.settings)
    }

    /// Set the settings of the bucket.
    ///
    /// # Arguments
    ///
    /// * `settings` - The new settings of the bucket.
    ///
    /// # Returns
    ///
    ///  Returns an error if the bucket could not be found.
    pub async fn set_settings(&self, settings: BucketSettings) -> Result<()> {
        self.http_client
            .send_json::<BucketSettings>(Method::PUT, &format!("/b/{}", self.name), settings)
            .await
    }

    /// Get full information about the bucket (stats, settings, entries).
    pub async fn full_info(&self) -> Result<FullBucketInfo> {
        self.http_client
            .send_and_receive_json::<(), FullBucketInfo>(
                Method::GET,
                &format!("/b/{}", self.name),
                None,
            )
            .await
    }

    /// Get bucket stats.
    pub async fn info(&self) -> Result<BucketInfo> {
        Ok(self.full_info().await?.info)
    }

    /// Get bucket entries.
    pub async fn entries(&self) -> Result<Vec<EntryInfo>> {
        Ok(self.full_info().await?.entries)
    }

    /// Rename an entry in the bucket.
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry to rename.
    /// * `new_name` - The new name of the entry.
    ///
    /// # Returns
    ///
    /// Returns an error if the entry could not be renamed.
    pub async fn rename_entry(&self, entry: &str, new_name: &str) -> Result<()> {
        self.http_client
            .send_json(
                Method::PUT,
                &format!("/b/{}/{}/rename", self.name, entry),
                RenameEntry {
                    new_name: new_name.to_string(),
                },
            )
            .await
    }

    /// Rename the bucket.
    ///
    /// # Arguments
    ///
    /// * `new_name` - The new name of the bucket.
    ///
    /// # Returns
    ///
    /// Returns an error if the bucket could not be renamed.
    pub async fn rename(&mut self, new_name: &str) -> Result<()> {
        self.http_client
            .send_json(
                Method::PUT,
                &format!("/b/{}/rename", self.name),
                RenameEntry {
                    new_name: new_name.to_string(),
                },
            )
            .await?;
        self.name = new_name.to_string();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use reduct_base::error::ErrorCode;

    use crate::client::tests::{bucket_settings, client};
    use crate::client::ReductClient;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn test_bucket_full_info(#[future] bucket: Bucket) {
        let bucket = bucket.await;
        let FullBucketInfo {
            info,
            settings,
            entries,
        } = bucket.full_info().await.unwrap();
        assert_eq!(info, bucket.info().await.unwrap());
        assert_eq!(settings, bucket.settings().await.unwrap());
        assert_eq!(entries, bucket.entries().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_settings(#[future] bucket: Bucket, bucket_settings: BucketSettings) {
        let bucket = bucket.await;
        let settings = bucket.settings().await.unwrap();
        assert_eq!(settings, bucket_settings);

        let new_settings = BucketSettings {
            quota_size: Some(100),
            ..BucketSettings::default()
        };

        bucket.set_settings(new_settings.clone()).await.unwrap();
        assert_eq!(
            bucket.settings().await.unwrap(),
            BucketSettings {
                quota_size: new_settings.quota_size,
                ..bucket_settings
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_remove(#[future] bucket: Bucket) {
        let bucket = bucket.await;
        bucket.remove().await.unwrap();

        assert_eq!(
            bucket.info().await.err().unwrap().status,
            ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_rename_entry(#[future] bucket: Bucket) {
        let bucket = bucket.await;
        bucket.rename_entry("entry-1", "new-entry-1").await.unwrap();
        let entries = bucket.entries().await.unwrap();
        assert!(entries.iter().any(|entry| entry.name == "new-entry-1"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_bucket_rename(#[future] bucket: Bucket) {
        let mut bucket = bucket.await;
        bucket.rename("new-bucket").await.unwrap();
        assert_eq!(bucket.name(), "new-bucket");
        assert!(bucket.info().await.is_ok());
    }

    #[fixture]
    pub async fn bucket(#[future] client: ReductClient) -> Bucket {
        client.await.get_bucket("test-bucket-1").await.unwrap()
    }
}
