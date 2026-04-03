use chrono::{DateTime, Utc};
use reduct_base::msg::token_api::Permissions;
use serde::{Deserialize, Serialize};

/// Extended token create request payload (API v2).
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct TokenCreateOptions {
    pub permissions: Permissions,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ttl: Option<u64>,
    #[serde(default)]
    pub ip_allowlist: Vec<String>,
}

/// Extended token view with token API fields available in newer ReductStore versions.
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct TokenInfo {
    pub name: String,
    #[serde(default)]
    pub value: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub permissions: Option<Permissions>,
    #[serde(default)]
    pub is_provisioned: bool,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ttl: Option<u64>,
    #[serde(default)]
    pub last_access: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ip_allowlist: Vec<String>,
    #[serde(default)]
    pub is_expired: bool,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct TokenInfoList {
    pub tokens: Vec<TokenInfo>,
}
