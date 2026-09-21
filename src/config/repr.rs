//! serde 反序列化中间表示：把 `*_ms` 字段转换为 [`Duration`]。
//!
//! [`NatsConfig`] 的手写 `Deserialize` 让 TOML 只承载非敏感字段：
//! `password` / `token` / `nkey_seed` 只能经环境变量或 Builder 注入。

use std::time::Duration;

use serde::Deserialize;

use super::{NatsConfig, TlsPolicy, SCHEMA_VERSION};
use crate::error::{NatsError, NatsResult};

/// serde 反序列化中间表示：把 `*_ms` 字段转换为 [`Duration`]。
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NatsConfigRepr {
    schema_version: u32,
    url: String,
    user: Option<String>,
    name: String,
    tls: bool,
    tls_policy: Option<TlsPolicy>,
    jetstream: bool,
    connect_timeout_ms: u64,
    operation_timeout_ms: u64,
    subscription_capacity: usize,
    client_capacity: usize,
    max_reconnects: usize,
    reconnect_max_delay_ms: u64,
    ignore_discovered_servers: bool,
    tls_ca_file: Option<String>,
    tls_cert_file: Option<String>,
    tls_key_file: Option<String>,
}

impl Default for NatsConfigRepr {
    fn default() -> Self {
        let base = NatsConfig::default();
        Self {
            schema_version: SCHEMA_VERSION,
            url: base.url,
            user: base.user,
            name: base.name,
            tls: base.tls,
            tls_policy: base.tls_policy,
            jetstream: base.jetstream,
            connect_timeout_ms: duration_to_millis(base.connect_timeout),
            operation_timeout_ms: duration_to_millis(base.operation_timeout),
            subscription_capacity: base.subscription_capacity,
            client_capacity: base.client_capacity,
            max_reconnects: base.max_reconnects,
            reconnect_max_delay_ms: duration_to_millis(base.reconnect_max_delay),
            ignore_discovered_servers: base.ignore_discovered_servers,
            tls_ca_file: base.tls_ca_file,
            tls_cert_file: base.tls_cert_file,
            tls_key_file: base.tls_key_file,
        }
    }
}

impl NatsConfigRepr {
    fn into_config(self) -> NatsResult<NatsConfig> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(NatsError::config(format!(
                "schema_version 不支持: {}（期望 {SCHEMA_VERSION}）",
                self.schema_version
            )));
        }
        Ok(NatsConfig {
            url: self.url,
            user: self.user,
            // 敏感字段只能经环境变量或 Builder 注入，不参与反序列化
            password: None,
            token: None,
            nkey_seed: None,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            operation_timeout: Duration::from_millis(self.operation_timeout_ms),
            name: self.name,
            tls: self.tls,
            tls_policy: self.tls_policy,
            jetstream: self.jetstream,
            subscription_capacity: self.subscription_capacity,
            client_capacity: self.client_capacity,
            max_reconnects: self.max_reconnects,
            reconnect_max_delay: Duration::from_millis(self.reconnect_max_delay_ms),
            ignore_discovered_servers: self.ignore_discovered_servers,
            tls_ca_file: self.tls_ca_file,
            tls_cert_file: self.tls_cert_file,
            tls_key_file: self.tls_key_file,
        })
    }
}

impl<'de> Deserialize<'de> for NatsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        NatsConfigRepr::deserialize(deserializer)?
            .into_config()
            .map_err(serde::de::Error::custom)
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}
