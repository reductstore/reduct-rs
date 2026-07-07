// Copyright 2024-2025 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use crate::http_client::HttpClient;
use crate::Labels;

use reduct_base::msg::replication_api::{ReplicationMode, ReplicationSettings};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

/// Replication builder.
pub struct ReplicationBuilder {
    name: String,
    settings: ReplicationSettings,
    http_client: Arc<HttpClient>,
}

impl ReplicationBuilder {
    /// Create a new replication builder.
    pub(super) fn new(name: String, http_client: Arc<HttpClient>) -> Self {
        let mut settings = ReplicationSettings::default();
        // Keep compatibility with older ReductStore versions that expect an empty token field.
        settings.dst_token = Some("".to_string());
        Self {
            name,
            settings,
            http_client,
        }
    }

    /// Set the source bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Source bucket. Required and must exist.
    pub fn src_bucket(mut self, bucket: &str) -> Self {
        self.settings.src_bucket = bucket.to_string();
        self
    }

    /// Set the destination bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Destination bucket. Required and must exist.
    pub fn dst_bucket(mut self, bucket: &str) -> Self {
        self.settings.dst_bucket = bucket.to_string();
        self
    }

    /// Set the destination host.
    ///
    /// # Arguments
    ///
    /// * `host` - Destination host. Required.
    pub fn dst_host(mut self, host: &str) -> Self {
        self.settings.dst_host = host.to_string();
        self
    }

    /// Set the destination token.
    ///
    /// # Arguments
    ///
    /// * `token` - Destination token.
    pub fn dst_token(mut self, token: &str) -> Self {
        self.settings.dst_token = Some(token.to_string());
        self
    }

    /// Set the replication entries.
    ///
    /// # Arguments
    /// * `entries` - Replication entries. If empty, all entries will be replicated. Wildcards are supported.
    pub fn entries(mut self, entries: Vec<String>) -> Self {
        self.settings.entries = entries;
        self
    }

    /// Set the destination entry prefix.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Prefix to add to destination entry names.
    pub fn dst_prefix(mut self, prefix: &str) -> Self {
        self.settings.dst_prefix = prefix.to_string();
        self
    }

    /// Set the replication conditional query.
    ///
    /// # Arguments
    ///
    /// * `when` - Conditional query.
    pub fn when(mut self, when: serde_json::Value) -> Self {
        self.settings.when = Some(when);
        self
    }

    /// Set replication mode.
    ///
    /// * `mode` - Enabled, Paused, or Disabled.
    pub fn mode(mut self, mode: ReplicationMode) -> Self {
        self.settings.mode = mode;
        self
    }

    /// Override all the replication settings.
    ///
    /// # Arguments
    ///
    /// * `settings` - Replication settings.
    pub fn set_settings(mut self, settings: ReplicationSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Send request to create a new replication.
    pub async fn send(self) -> Result<()> {
        self.http_client
            .send_json(
                Method::POST,
                &format!("/replications/{}", self.name),
                ReplicationSettingsRequest::from(&self.settings),
            )
            .await
    }
}

#[derive(Serialize)]
pub(crate) struct ReplicationSettingsRequest<'a> {
    src_bucket: &'a str,
    dst_bucket: &'a str,
    dst_host: &'a str,
    dst_token: &'a Option<String>,
    entries: &'a Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dst_prefix: Option<&'a str>,
    include: &'a Labels,
    exclude: &'a Labels,
    each_n: &'a Option<u64>,
    when: &'a Option<Value>,
    mode: ReplicationMode,
}

impl<'a> From<&'a ReplicationSettings> for ReplicationSettingsRequest<'a> {
    fn from(settings: &'a ReplicationSettings) -> Self {
        Self {
            src_bucket: settings.src_bucket.as_str(),
            dst_bucket: settings.dst_bucket.as_str(),
            dst_host: settings.dst_host.as_str(),
            dst_token: &settings.dst_token,
            entries: &settings.entries,
            dst_prefix: if settings.dst_prefix.is_empty() {
                None
            } else {
                Some(settings.dst_prefix.as_str())
            },
            include: &settings.include,
            exclude: &settings.exclude,
            each_n: &settings.each_n,
            when: &settings.when,
            mode: settings.mode,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_replication_settings_request_omits_empty_dst_prefix() {
        let settings = ReplicationSettings::default();

        let value = serde_json::to_value(ReplicationSettingsRequest::from(&settings)).unwrap();

        assert_eq!(value.get("dst_prefix"), None);
    }

    #[test]
    fn test_replication_settings_request_serializes_dst_prefix() {
        let settings = ReplicationSettings {
            src_bucket: "edge".to_string(),
            dst_bucket: "fleet".to_string(),
            dst_host: "https://central.example".to_string(),
            dst_prefix: "robot-1".to_string(),
            ..Default::default()
        };

        let value = serde_json::to_value(ReplicationSettingsRequest::from(&settings)).unwrap();

        assert_eq!(value["dst_prefix"], json!("robot-1"));
    }
}
