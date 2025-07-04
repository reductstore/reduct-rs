// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use crate::http_client::HttpClient;

use reduct_base::msg::replication_api::ReplicationSettings;
use reduct_base::Labels;
use reqwest::Method;
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
        Self {
            name,
            settings: ReplicationSettings {
                src_bucket: "".to_string(),
                dst_bucket: "".to_string(),
                dst_host: "".to_string(),
                dst_token: "".to_string(),
                entries: vec![],
                include: Default::default(),
                exclude: Default::default(),
                each_s: None,
                each_n: None,
                when: None,
            },
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
        self.settings.dst_token = token.to_string();
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

    /// Set the replication include.
    ///
    /// # Arguments
    ///
    /// * `include` - Replication include. If empty, all labels will be replicated.
    ///
    #[deprecated(
        since = "1.14.0",
        note = "Use the `when` method instead. It will be removed in v1.18.0."
    )]
    pub fn include(mut self, include: Labels) -> Self {
        self.settings.include = include;
        self
    }

    /// Set the replication exclude.
    ///
    /// # Arguments
    ///
    /// * `exclude` - Replication exclude. If empty, no labels will be excluded.
    ///        If a few labels are specified, records must have none of them to be replicated.
    #[deprecated(
        since = "1.14.0",
        note = "Use the `when` method instead. It will be removed in v1.18.0."
    )]
    pub fn exclude(mut self, exclude: Labels) -> Self {
        self.settings.exclude = exclude;
        self
    }

    /// Set the replication each_s setting.
    ///
    /// Replicate a record every S seconds if set.
    ///
    /// # Arguments
    ///
    /// * `each_s` - Replicate a record every S seconds.
    #[deprecated(
        since = "1.15.0",
        note = "Use `$each_t` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn each_s(mut self, each_s: f64) -> Self {
        self.settings.each_s = Some(each_s);
        self
    }

    /// Set the replication each_n setting.
    ///
    /// Replicate every Nth record if set.
    ///
    /// # Arguments
    ///
    /// * `each_n` - Replicate every Nth record.
    #[deprecated(
        since = "1.15.0",
        note = "Use `$each_n` operator in `when` condition. It will be removed in v1.18.0."
    )]
    pub fn each_n(mut self, each_n: u64) -> Self {
        self.settings.each_n = Some(each_n);
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
                self.settings,
            )
            .await
    }
}
