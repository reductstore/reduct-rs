// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Bucket, RecordBuilder};
use futures_util::{pin_mut, StreamExt};
use reduct_base::error::{ErrorCode, ReductError};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::SystemTime;

impl Bucket {
    /// Write entry attachments.
    ///
    /// Attachments are stored in a hidden metadata entry `<entry>/$meta` as JSON payloads
    /// with the `key` label containing the attachment name.
    pub async fn write_attachments(
        &self,
        entry: &str,
        attachments: HashMap<String, Value>,
    ) -> Result<(), ReductError> {
        let meta_entry = format!("{entry}/$meta");
        let mut batch = self.write_record_batch();
        if attachments.is_empty() {
            return Ok(());
        }

        let mut timestamp_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("SystemTime must be after UNIX_EPOCH")
            .as_micros() as u64;

        for (key, content) in attachments {
            let payload = serde_json::to_vec(&content).map_err(|err| {
                ReductError::new(
                    ErrorCode::UnprocessableEntity,
                    &format!("failed to serialize attachment '{key}': {err}"),
                )
            })?;
            batch = batch.add_record(
                RecordBuilder::new()
                    .entry(meta_entry.clone())
                    .timestamp_us(timestamp_us)
                    .data(payload)
                    .content_type("application/json")
                    .add_label("key".to_string(), key)
                    .build(),
            );
            timestamp_us += 1;
        }

        batch.send().await?;
        Ok(())
    }

    /// Read entry attachments.
    ///
    /// Returns a map where keys are attachment names and values are JSON payloads.
    pub async fn read_attachments(
        &self,
        entry: &str,
    ) -> Result<HashMap<String, Value>, ReductError> {
        let mut attachments = HashMap::new();
        let query = self.query(format!("{entry}/$meta")).send().await?;
        pin_mut!(query);

        while let Some(record) = query.next().await {
            let record = record?;
            let key = record.labels().get("key").cloned();
            if let Some(key) = key {
                let content = record.bytes().await?;
                let metadata = serde_json::from_slice(&content).map_err(|err| {
                    ReductError::new(
                        ErrorCode::UnprocessableEntity,
                        &format!("failed to decode attachment '{key}': {err}"),
                    )
                })?;
                attachments.insert(key.clone(), metadata);
            }
        }

        Ok(attachments)
    }

    /// Remove entry attachments.
    ///
    /// If `attachment_keys` is `None`, all attachments are removed.
    pub async fn remove_attachments(
        &self,
        entry: &str,
        attachment_keys: Option<Vec<String>>,
    ) -> Result<(), ReductError> {
        let meta_entry = format!("{entry}/$meta");
        let mut batch = self.update_record_batch();

        let query = if let Some(keys) = attachment_keys {
            let mut when = vec![json!("&key")];
            when.extend(keys.into_iter().map(Value::from));
            self.query(meta_entry.clone())
                .when(json!({"$in": when}))
                .send()
                .await?
        } else {
            self.query(meta_entry.clone()).send().await?
        };

        pin_mut!(query);
        while let Some(record) = query.next().await {
            let record = record?;
            let mut labels = record.labels().clone();
            labels.insert("remove".to_string(), "true".to_string());

            batch = batch.add_record(
                RecordBuilder::new()
                    .entry(record.entry())
                    .timestamp_us(record.timestamp_us())
                    .labels(labels)
                    .build(),
            );
        }

        if batch.record_count() == 0 {
            return Ok(());
        }

        batch.send().await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "test-api-119"))]
mod tests {
    use crate::bucket::tests::bucket;
    use crate::Bucket;
    use rstest::{fixture, rstest};
    use serde_json::json;
    use std::collections::HashMap;

    const ENTRY: &str = "entry-1";

    #[fixture]
    fn complex_attachments() -> HashMap<String, serde_json::Value> {
        HashMap::from([
            (
                "meta-1".to_string(),
                json!({"enabled": true, "values": [1, 2, 3]}),
            ),
            ("meta-2".to_string(), json!({"name": "test"})),
        ])
    }

    #[fixture]
    fn removable_attachments() -> HashMap<String, serde_json::Value> {
        HashMap::from([
            ("meta-1".to_string(), json!({"value": 1})),
            ("meta-2".to_string(), json!({"value": 2})),
        ])
    }

    #[fixture]
    fn selected_after_remove() -> HashMap<String, serde_json::Value> {
        HashMap::from([("meta-2".to_string(), json!({"value": 2}))])
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_read_attachments(
        #[future] bucket: Bucket,
        complex_attachments: HashMap<String, serde_json::Value>,
    ) {
        let bucket = bucket.await;
        bucket
            .write_attachments(ENTRY, complex_attachments.clone())
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, complex_attachments);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_selected_attachments(
        #[future] bucket: Bucket,
        removable_attachments: HashMap<String, serde_json::Value>,
        selected_after_remove: HashMap<String, serde_json::Value>,
    ) {
        let bucket = bucket.await;
        bucket
            .write_attachments(ENTRY, removable_attachments)
            .await
            .unwrap();

        bucket
            .remove_attachments(ENTRY, Some(vec!["meta-1".to_string()]))
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, selected_after_remove);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_all_attachments(
        #[future] bucket: Bucket,
        removable_attachments: HashMap<String, serde_json::Value>,
    ) {
        let bucket = bucket.await;
        bucket
            .write_attachments(ENTRY, removable_attachments)
            .await
            .unwrap();

        bucket.remove_attachments(ENTRY, None).await.unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, HashMap::new());
    }
}
