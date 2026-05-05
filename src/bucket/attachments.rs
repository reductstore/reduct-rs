// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Bucket, RecordBuilder};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures_util::{pin_mut, StreamExt};
use reduct_base::error::{ErrorCode, ReductError};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::SystemTime;

fn is_json_content_type(content_type: &str) -> bool {
    let ct = content_type.split(';').next().unwrap_or("").trim();
    ct.eq_ignore_ascii_case("application/json")
        || ct.eq_ignore_ascii_case("text/json")
        || ct.to_ascii_lowercase().ends_with("+json")
}

impl Bucket {
    /// Write entry attachments.
    ///
    /// Attachments are stored in a hidden metadata entry `<entry>/$meta` as JSON payloads
    /// with the `key` label containing the attachment name.
    pub async fn write_attachments(
        &self,
        entry: &str,
        attachments: HashMap<String, Value>,
        content_type: Option<&str>,
    ) -> Result<(), ReductError> {
        let meta_entry = format!("{entry}/$meta");
        let mut batch = self.write_record_batch();
        if attachments.is_empty() {
            return Ok(());
        }

        let ct = content_type.unwrap_or("application/json");
        let is_json = is_json_content_type(ct);

        let mut timestamp_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("SystemTime must be after UNIX_EPOCH")
            .as_micros() as u64;

        for (key, content) in attachments {
            let payload = if is_json {
                serde_json::to_vec(&content).map_err(|err| {
                    ReductError::new(
                        ErrorCode::UnprocessableEntity,
                        &format!("failed to serialize attachment '{key}': {err}"),
                    )
                })?
            } else {
                let b64 = content.as_str().ok_or_else(|| {
                    ReductError::new(
                        ErrorCode::UnprocessableEntity,
                        &format!("non-JSON attachment '{key}' must be a base64-encoded string"),
                    )
                })?;
                BASE64_STANDARD.decode(b64).map_err(|err| {
                    ReductError::new(
                        ErrorCode::UnprocessableEntity,
                        &format!("failed to decode base64 for attachment '{key}': {err}"),
                    )
                })?
            };
            batch = batch.add_record(
                RecordBuilder::new()
                    .entry(meta_entry.clone())
                    .timestamp_us(timestamp_us)
                    .data(payload)
                    .content_type(ct)
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
                let ct = record.content_type().to_string();
                let content = record.bytes().await?;
                let value = if is_json_content_type(&ct) {
                    serde_json::from_slice(&content).map_err(|err| {
                        ReductError::new(
                            ErrorCode::UnprocessableEntity,
                            &format!("failed to decode attachment '{key}': {err}"),
                        )
                    })?
                } else {
                    Value::String(BASE64_STANDARD.encode(&content))
                };
                attachments.insert(key.clone(), value);
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
            let mut when = vec![json!({"&key": {"$cast": "string"}})];
            // Escape "$"-prefixed keys so the ReductStore query engine treats
            // them as literal values instead of operators such as $$system.
            let escaped_keys = keys
                .into_iter()
                .map(|key| {
                    if key.starts_with('$') {
                        format!("${}", key)
                    } else {
                        key
                    }
                })
                .collect::<Vec<_>>();
            when.extend(escaped_keys.into_iter().map(Value::from));
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
            ("$system".to_string(), json!({"value": "test"})),
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
            .write_attachments(ENTRY, complex_attachments.clone(), None)
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
            .write_attachments(ENTRY, removable_attachments, None)
            .await
            .unwrap();

        bucket
            .remove_attachments(
                ENTRY,
                Some(vec!["meta-1".to_string(), "$system".to_string()]),
            )
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, selected_after_remove);
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_system_attachment_only(
        #[future] bucket: Bucket,
        removable_attachments: HashMap<String, serde_json::Value>,
    ) {
        let bucket = bucket.await;
        bucket
            .write_attachments(ENTRY, removable_attachments, None)
            .await
            .unwrap();

        bucket
            .remove_attachments(ENTRY, Some(vec!["$system".to_string()]))
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert!(!attachments.contains_key("$system"));
        assert!(attachments.contains_key("meta-1"));
        assert!(attachments.contains_key("meta-2"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_attachments_with_numeric_keys(#[future] bucket: Bucket) {
        let bucket = bucket.await;
        bucket
            .write_attachments(
                ENTRY,
                HashMap::from([
                    (
                        "1".to_string(),
                        json!({"enabled": true, "values": [1, 2, 3]}),
                    ),
                    ("2.5".to_string(), json!({"name": "test"})),
                ]),
                None,
            )
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(
            attachments,
            HashMap::from([
                (
                    "1".to_string(),
                    json!({"enabled": true, "values": [1, 2, 3]})
                ),
                ("2.5".to_string(), json!({"name": "test"})),
            ])
        );

        bucket
            .remove_attachments(ENTRY, Some(vec!["1".to_string(), "2.5".to_string()]))
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, HashMap::new());
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_all_attachments(
        #[future] bucket: Bucket,
        removable_attachments: HashMap<String, serde_json::Value>,
    ) {
        let bucket = bucket.await;
        bucket
            .write_attachments(ENTRY, removable_attachments, None)
            .await
            .unwrap();

        bucket.remove_attachments(ENTRY, None).await.unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, HashMap::new());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_read_non_json_attachment(#[future] bucket: Bucket) {
        use super::BASE64_STANDARD;
        use base64::Engine;

        let bucket = bucket.await;
        let raw_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let encoded = BASE64_STANDARD.encode(&raw_bytes);

        bucket
            .write_attachments(
                ENTRY,
                HashMap::from([(
                    "binary-data".to_string(),
                    serde_json::Value::String(encoded.clone()),
                )]),
                Some("application/octet-stream"),
            )
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(
            attachments.get("binary-data"),
            Some(&serde_json::Value::String(encoded))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_attachments_default_json(
        #[future] bucket: Bucket,
        complex_attachments: HashMap<String, serde_json::Value>,
    ) {
        let bucket = bucket.await;
        bucket
            .write_attachments(ENTRY, complex_attachments.clone(), None)
            .await
            .unwrap();

        let attachments = bucket.read_attachments(ENTRY).await.unwrap();
        assert_eq!(attachments, complex_attachments);
    }
}
