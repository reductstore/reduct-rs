// Copyright 2023-2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::client::Result;
use crate::http_client::HttpClient;
use reduct_base::msg::lifecycle_api::{LifecycleMode, LifecycleSettings, LifecycleType};
use reqwest::Method;
use serde_json::Value;
use std::sync::Arc;

/// Lifecycle builder.
pub struct LifecycleBuilder {
    name: String,
    settings: LifecycleSettings,
    http_client: Arc<HttpClient>,
}

impl LifecycleBuilder {
    /// Create a new lifecycle builder.
    pub(super) fn new(name: String, http_client: Arc<HttpClient>) -> Self {
        Self {
            name,
            settings: LifecycleSettings::default(),
            http_client,
        }
    }

    /// Set lifecycle policy type.
    ///
    /// # Arguments
    ///
    /// * `lifecycle_type` - Lifecycle type.
    pub fn lifecycle_type(mut self, lifecycle_type: LifecycleType) -> Self {
        self.settings.lifecycle_type = lifecycle_type;
        self
    }

    /// Set the bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.settings.bucket = bucket.to_string();
        self
    }

    /// Set the lifecycle entries.
    ///
    /// # Arguments
    /// * `entries` - Lifecycle entries. If empty, all removable entries are matched. Prefix wildcards are supported.
    pub fn entries(mut self, entries: Vec<String>) -> Self {
        self.settings.entries = entries;
        self
    }

    /// Set the maximum record age.
    ///
    /// # Arguments
    ///
    /// * `max_age` - Maximum age, e.g. "30d", "24h", or "3600s".
    pub fn max_age(mut self, max_age: &str) -> Self {
        self.settings.max_age = max_age.to_string();
        self
    }

    /// Set the interval between lifecycle runs.
    ///
    /// # Arguments
    ///
    /// * `interval` - Interval, e.g. "10m", "1h", or "3600s".
    pub fn interval(mut self, interval: &str) -> Self {
        self.settings.interval = interval.to_string();
        self
    }

    /// Set the lifecycle conditional query.
    ///
    /// # Arguments
    ///
    /// * `when` - Conditional query.
    pub fn when(mut self, when: Value) -> Self {
        self.settings.when = Some(when);
        self
    }

    /// Set lifecycle mode.
    ///
    /// * `mode` - Enabled, Disabled, or DryRun.
    pub fn mode(mut self, mode: LifecycleMode) -> Self {
        self.settings.mode = mode;
        self
    }

    /// Override all the lifecycle settings.
    ///
    /// # Arguments
    ///
    /// * `settings` - Lifecycle settings.
    pub fn set_settings(mut self, settings: LifecycleSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Send request to create a new lifecycle policy.
    pub async fn send(self) -> Result<()> {
        self.http_client
            .send_json(
                Method::POST,
                &format!("/lifecycles/{}", self.name),
                self.settings,
            )
            .await
    }
}
