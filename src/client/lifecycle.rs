use super::{ReductClient, Result};
use crate::http_client::HttpClient;
use reduct_base::msg::lifecycle_api::{
    FullLifecycleInfo, LifecycleInfo, LifecycleList, LifecycleMode, LifecycleModePayload,
    LifecycleSettings, LifecycleType,
};
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

    /// Set the record age threshold.
    ///
    /// # Arguments
    ///
    /// * `older_than` - Age threshold, e.g. "30d", "24h", or "3600s".
    pub fn older_than(mut self, older_than: &str) -> Self {
        self.settings.older_than = older_than.to_string();
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

impl ReductClient {
    /// Get list of lifecycle policies
    ///
    /// # Returns
    ///
    /// The list of lifecycle policies or an error
    pub async fn list_lifecycles(&self) -> Result<Vec<LifecycleInfo>> {
        let list = self
            .http_client
            .send_and_receive_json::<(), LifecycleList>(Method::GET, "/lifecycles", None)
            .await?;
        Ok(list.lifecycles)
    }

    /// Get full lifecycle policy info
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lifecycle policy
    ///
    /// # Returns
    ///
    /// The lifecycle policy info or an error
    pub async fn get_lifecycle(&self, name: &str) -> Result<FullLifecycleInfo> {
        let info = self
            .http_client
            .send_and_receive_json::<(), FullLifecycleInfo>(
                Method::GET,
                &format!("/lifecycles/{}", name),
                None,
            )
            .await?;
        Ok(info)
    }

    /// Create a lifecycle policy
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lifecycle policy
    ///
    /// # Returns
    ///
    /// a lifecycle builder to set the lifecycle settings
    ///
    /// # Example
    ///
    /// ```no_run
    /// use reduct_rs::{condition, LifecycleMode, LifecycleType, ReductClient};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = ReductClient::builder()
    ///         .url("http://127.0.0.1:8383")
    ///         .api_token("my-api-token")
    ///         .build();
    ///
    ///     client
    ///         .create_lifecycle("test-lifecycle")
    ///         .lifecycle_type(LifecycleType::Delete)
    ///         .bucket("test-bucket-1")
    ///         .entries(vec!["entry-*".to_string()])
    ///         .older_than("1h")
    ///         .interval("10m")
    ///         .when(condition!({"$eq": ["&label", 1]}))
    ///         .mode(LifecycleMode::DryRun)
    ///         .send()
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub fn create_lifecycle(&self, name: &str) -> LifecycleBuilder {
        LifecycleBuilder::new(name.to_string(), Arc::clone(&self.http_client))
    }

    /// Update a lifecycle policy
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lifecycle policy
    /// * `settings` - The lifecycle policy settings
    ///
    /// # Returns
    ///
    /// Ok if the lifecycle policy was updated, otherwise an error
    pub async fn update_lifecycle(&self, name: &str, settings: LifecycleSettings) -> Result<()> {
        self.http_client
            .send_json(Method::PUT, &format!("/lifecycles/{}", name), settings)
            .await
    }

    /// Update lifecycle mode without changing other settings.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lifecycle policy
    /// * `mode` - New lifecycle mode
    pub async fn set_lifecycle_mode(&self, name: &str, mode: LifecycleMode) -> Result<()> {
        self.http_client
            .send_json(
                Method::PATCH,
                &format!("/lifecycles/{}/mode", name),
                LifecycleModePayload { mode },
            )
            .await
    }

    /// Delete a lifecycle policy
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lifecycle policy
    ///
    /// # Returns
    ///
    /// Ok if the lifecycle policy was deleted, otherwise an error
    pub async fn delete_lifecycle(&self, name: &str) -> Result<()> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/lifecycles/{}", name));
        self.http_client.send_request(request).await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "test-api-120"))]
mod tests {
    use super::*;
    use crate::client::tests::client;
    use crate::condition;
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_list_lifecycles(#[future] client: ReductClient) {
        let lifecycles = client.await.list_lifecycles().await.unwrap();
        assert!(lifecycles.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_lifecycle(#[future] client: ReductClient, settings: LifecycleSettings) {
        let client = client.await;
        client
            .create_lifecycle("test-lifecycle")
            .lifecycle_type(settings.lifecycle_type)
            .bucket(settings.bucket.as_str())
            .entries(settings.entries.clone())
            .older_than(settings.older_than.as_str())
            .interval(settings.interval.as_str())
            .when(settings.when.unwrap())
            .mode(settings.mode)
            .send()
            .await
            .unwrap();
        let lifecycles = client.list_lifecycles().await.unwrap();
        assert_eq!(lifecycles.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_lifecycle(#[future] client: ReductClient, settings: LifecycleSettings) {
        let client = client.await;
        client
            .create_lifecycle("test-lifecycle")
            .set_settings(settings.clone())
            .send()
            .await
            .unwrap();

        let lifecycle = client.get_lifecycle("test-lifecycle").await.unwrap();

        assert_eq!(lifecycle.info.name, "test-lifecycle");
        assert_eq!(lifecycle.info.mode, settings.mode);
        assert_eq!(lifecycle.settings, settings);
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_lifecycle(
        #[future] client: ReductClient,
        mut settings: LifecycleSettings,
    ) {
        let client = client.await;
        client
            .create_lifecycle("test-lifecycle")
            .set_settings(settings.clone())
            .send()
            .await
            .unwrap();

        settings.older_than = "2h".to_string();
        settings.mode = LifecycleMode::Disabled;

        client
            .update_lifecycle("test-lifecycle", settings.clone())
            .await
            .unwrap();

        let lifecycle = client.get_lifecycle("test-lifecycle").await.unwrap();
        assert_eq!(lifecycle.settings, settings);
    }

    #[rstest]
    #[tokio::test]
    async fn test_set_lifecycle_mode(#[future] client: ReductClient, settings: LifecycleSettings) {
        let client = client.await;
        client
            .create_lifecycle("test-lifecycle")
            .set_settings(settings)
            .send()
            .await
            .unwrap();

        client
            .set_lifecycle_mode("test-lifecycle", LifecycleMode::Disabled)
            .await
            .unwrap();

        let lifecycle = client.get_lifecycle("test-lifecycle").await.unwrap();
        assert_eq!(lifecycle.info.mode, LifecycleMode::Disabled);
        assert_eq!(lifecycle.settings.mode, LifecycleMode::Disabled);
    }

    #[rstest]
    #[tokio::test]
    async fn test_set_lifecycle_mode_dry_run(
        #[future] client: ReductClient,
        settings: LifecycleSettings,
    ) {
        let client = client.await;
        client
            .create_lifecycle("test-lifecycle")
            .set_settings(settings)
            .send()
            .await
            .unwrap();

        client
            .set_lifecycle_mode("test-lifecycle", LifecycleMode::DryRun)
            .await
            .unwrap();

        let lifecycle = client.get_lifecycle("test-lifecycle").await.unwrap();
        assert_eq!(lifecycle.info.mode, LifecycleMode::DryRun);
        assert_eq!(lifecycle.settings.mode, LifecycleMode::DryRun);
    }

    #[rstest]
    #[tokio::test]
    async fn test_delete_lifecycle(#[future] client: ReductClient, settings: LifecycleSettings) {
        let client = client.await;
        client
            .create_lifecycle("test-lifecycle")
            .set_settings(settings)
            .send()
            .await
            .unwrap();

        client.delete_lifecycle("test-lifecycle").await.unwrap();

        let lifecycles = client.list_lifecycles().await.unwrap();
        assert!(lifecycles.is_empty());
    }

    #[fixture]
    fn settings() -> LifecycleSettings {
        LifecycleSettings {
            lifecycle_type: LifecycleType::Delete,
            bucket: "test-bucket-1".to_string(),
            entries: vec![],
            older_than: "1h".to_string(),
            interval: "10m".to_string(),
            when: Some(condition!({"$eq": ["&label", 1]})),
            mode: LifecycleMode::Enabled,
        }
    }
}
