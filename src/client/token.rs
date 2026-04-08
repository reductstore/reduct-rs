use super::{ReductClient, Result};
use crate::http_client::HttpClient;
use chrono::{DateTime, Utc};
use reduct_base::msg::token_api::{
    Permissions, Token, TokenCreateRequest, TokenCreateResponse, TokenList,
};
use reqwest::Method;
use std::sync::Arc;
use std::time::Duration;

/// Builder for creating an access token.
pub struct CreateTokenBuilder {
    name: String,
    request: TokenCreateRequest,
    http_client: Arc<HttpClient>,
}

impl CreateTokenBuilder {
    pub(super) fn new(
        name: String,
        permissions: Permissions,
        http_client: Arc<HttpClient>,
    ) -> Self {
        Self {
            name,
            request: TokenCreateRequest {
                permissions,
                ..Default::default()
            },
            http_client,
        }
    }

    /// Set the absolute expiration time for the token.
    pub fn expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.request.expires_at = Some(expires_at);
        self
    }

    /// Set the inactivity timeout for the token.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.request.ttl = Some(ttl.as_secs());
        self
    }

    /// Restrict the token to the provided client IP addresses.
    pub fn ip_allowlist(mut self, ip_allowlist: Vec<String>) -> Self {
        self.request.ip_allowlist = ip_allowlist;
        self
    }

    /// Send the token creation request.
    pub async fn send(self) -> Result<TokenCreateResponse> {
        self.http_client
            .send_and_receive_json::<TokenCreateRequest, TokenCreateResponse>(
                Method::POST,
                &format!("/tokens/{}", self.name),
                Some(self.request),
            )
            .await
    }
}

impl ReductClient {
    /// Get the token with permissions for the current user.
    ///
    /// # Returns
    ///
    /// The token or HttpError
    pub async fn me(&self) -> Result<Token> {
        self.http_client
            .send_and_receive_json::<(), Token>(Method::GET, "/me", None)
            .await
    }

    /// Get extended token info for the current user.
    pub async fn me_info(&self) -> Result<Token> {
        self.http_client
            .send_and_receive_json::<(), Token>(Method::GET, "/me", None)
            .await
    }

    /// Create an access token.
    pub async fn create_token(&self, name: &str, permissions: Permissions) -> Result<String> {
        let token = self.create_token_builder(name, permissions).send().await?;
        Ok(token.value)
    }

    /// Create an access token with optional settings such as expiration, inactivity TTL,
    /// and IP allowlist.
    pub fn create_token_builder(&self, name: &str, permissions: Permissions) -> CreateTokenBuilder {
        CreateTokenBuilder::new(name.to_string(), permissions, Arc::clone(&self.http_client))
    }

    /// Rotate an access token value and revoke the old one.
    pub async fn rotate_token(&self, name: &str) -> Result<TokenCreateResponse> {
        self.http_client
            .send_and_receive_json::<(), TokenCreateResponse>(
                Method::POST,
                &format!("/tokens/{}/rotate", name),
                None,
            )
            .await
    }

    /// Get an access token
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the token
    ///
    /// # Returns
    ///
    /// The token or an error
    pub async fn get_token(&self, name: &str) -> Result<Token> {
        self.http_client
            .send_and_receive_json::<(), Token>(Method::GET, &format!("/tokens/{}", name), None)
            .await
    }

    /// Delete an access token
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the token
    ///
    /// # Returns
    ///
    /// Ok if the token was deleted, otherwise an error
    pub async fn delete_token(&self, name: &str) -> Result<()> {
        let request = self
            .http_client
            .request(Method::DELETE, &format!("/tokens/{}", name));
        self.http_client.send_request(request).await?;
        Ok(())
    }

    /// List all access tokens
    ///
    /// # Returns
    ///
    /// The list of tokens or an error
    pub async fn list_tokens(&self) -> Result<Vec<Token>> {
        let list = self
            .http_client
            .send_and_receive_json::<(), TokenList>(Method::GET, "/tokens", None)
            .await?;
        Ok(list.tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::tests::client;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_me(#[future] client: ReductClient) {
        let token = client.await.me().await.unwrap();
        assert_eq!(token.name, "init-token");
        assert!(token.permissions.unwrap().full_access);
    }

    #[rstest]
    #[tokio::test]
    async fn test_me_info(#[future] client: ReductClient) {
        let token = client.await.me_info().await.unwrap();
        assert_eq!(token.name, "init-token");
        assert!(token.permissions.unwrap().full_access);
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_token(#[future] client: ReductClient) {
        let token_value = client
            .await
            .create_token(
                "test-token",
                Permissions {
                    full_access: false,
                    read: vec!["test-bucket".to_string()],
                    write: vec!["test-bucket".to_string()],
                },
            )
            .await
            .unwrap();

        assert!(token_value.starts_with("test-token"));
    }

    #[cfg(feature = "test-api-119")]
    #[rstest]
    #[tokio::test]
    async fn test_create_token_builder(#[future] client: ReductClient) {
        let token = client
            .await
            .create_token_builder(
                "test-token-options",
                Permissions {
                    full_access: false,
                    read: vec!["test-bucket".to_string()],
                    write: vec!["test-bucket".to_string()],
                },
            )
            .ttl(Duration::from_secs(3600))
            .ip_allowlist(vec!["127.0.0.1".to_string()])
            .send()
            .await
            .unwrap();

        assert!(token.value.starts_with("test-token-options"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token(#[future] client: ReductClient) {
        let token = client.await.get_token("init-token").await.unwrap();
        assert_eq!(token.name, "init-token");
        assert!(token.is_provisioned);

        let permissions = token.permissions.unwrap();
        assert!(permissions.full_access);
        assert!(permissions.read.is_empty());
        assert!(permissions.write.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_tokens(#[future] client: ReductClient) {
        let tokens = client.await.list_tokens().await.unwrap();
        assert!(!tokens.is_empty());
    }

    #[cfg(feature = "test-api-119")]
    #[rstest]
    #[tokio::test]
    async fn test_rotate_token(#[future] client: ReductClient) {
        let client = client.await;
        client
            .create_token("test-token-rotate", Permissions::default())
            .await
            .unwrap();
        let rotated = client.rotate_token("test-token-rotate").await.unwrap();
        assert!(rotated.value.starts_with("test-token-rotate"));
        client.delete_token("test-token-rotate").await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn delete_token(#[future] client: ReductClient) {
        let client = client.await;
        client
            .create_token("test-token", Permissions::default())
            .await
            .unwrap();
        client.delete_token("test-token").await.unwrap();
    }
}
