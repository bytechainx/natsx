//! 环境变量加载层：`from_env`、`apply_env_overrides` 与其读取 / 解析辅助。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。搬走的项**全是私有**的
//! （`from_env` 是 `pub`，其余只在层内互调），故**无需任何可见性调整**。

use std::time::Duration;

use crate::error::{NatsError, NatsResult};

use super::{NatsConfig, TlsPolicy, ENV_LEGACY_PREFIX, ENV_PREFIX};

impl NatsConfig {
    /// 从环境变量加载：以 [`NatsConfig::default`] 为基线，再应用 `FOUNDATIONX_NATSX_*`
    /// （兼容 `FOUNDATIONX_NATS_*`）覆盖，最后 [`NatsConfig::validate`]。
    ///
    /// # Errors
    ///
    /// 环境变量取值非法或最终配置不满足 [`NatsConfig::validate`] 时返回错误。
    pub fn from_env() -> NatsResult<Self> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// 环境变量覆盖（规范前缀优先，兼容历史前缀）。
    fn apply_env_overrides(&mut self) -> NatsResult<()> {
        if let Some((_, value)) = lookup_env("URL") {
            self.url = value;
        } else if let Some((_, value)) = lookup_env("SERVERS") {
            if let Some(first) = value.split(',').next() {
                self.url = first.trim().to_string();
            }
        }
        if let Some((_, value)) = lookup_env("USER") {
            self.user = Some(value);
        } else if let Some((_, value)) = lookup_env("USERNAME") {
            self.user = Some(value);
        }
        if let Some((_, value)) = lookup_env("PASSWORD") {
            self.password = Some(value);
        }
        if let Some((_, value)) = lookup_env("TOKEN") {
            self.token = Some(value);
        }
        if let Some((_, value)) = lookup_env("NKEY_SEED") {
            self.nkey_seed = Some(value);
        }
        if let Some((_, value)) = lookup_env("NAME") {
            self.name = value;
        }
        if let Some((key, value)) = lookup_env("TLS") {
            self.tls = parse_bool(&value, &key)?;
        }
        if let Some((_, value)) = lookup_env("TLS_POLICY") {
            self.tls_policy = Some(TlsPolicy::parse(&value)?);
        }
        if let Some((key, value)) = lookup_env("JETSTREAM") {
            self.jetstream = parse_bool(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("IGNORE_DISCOVERED_SERVERS") {
            self.ignore_discovered_servers = parse_bool(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("CONNECT_TIMEOUT_MS") {
            self.connect_timeout = parse_millis(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("OPERATION_TIMEOUT_MS") {
            self.operation_timeout = parse_millis(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("SLOW_CONSUMER_TIMEOUT_MS") {
            self.slow_consumer_timeout = Some(parse_millis(&value, &key)?);
        }
        if let Some((key, value)) = lookup_env("RECONNECT_MAX_DELAY_MS") {
            self.reconnect_max_delay = parse_millis(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("SUBSCRIPTION_CAPACITY") {
            self.subscription_capacity = parse_usize(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("CLIENT_CAPACITY") {
            self.client_capacity = parse_usize(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("MAX_RECONNECTS") {
            self.max_reconnects = parse_usize(&value, &key)?;
        }
        if let Some((_, value)) = lookup_env("TLS_CA_FILE") {
            self.tls_ca_file = Some(value);
        }
        if let Some((_, value)) = lookup_env("TLS_CERT_FILE") {
            self.tls_cert_file = Some(value);
        }
        if let Some((_, value)) = lookup_env("TLS_KEY_FILE") {
            self.tls_key_file = Some(value);
        }
        Ok(())
    }
}

/// 读取环境变量：规范前缀优先，其次兼容前缀；空白值视为未设置。
fn lookup_env(suffix: &str) -> Option<(String, String)> {
    for key in [
        format!("{ENV_PREFIX}{suffix}"),
        format!("{ENV_LEGACY_PREFIX}{suffix}"),
    ] {
        if let Ok(value) = std::env::var(&key) {
            if !value.trim().is_empty() {
                return Some((key, value));
            }
        }
    }
    None
}

fn parse_bool(value: &str, name: &str) -> NatsResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(NatsError::config(format!("{name} 非法（布尔值）: {other}"))),
    }
}

fn parse_usize(value: &str, name: &str) -> NatsResult<usize> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|error| NatsError::config(format!("{name} 非法（无符号整数）: {error}")))
}

fn parse_millis(value: &str, name: &str) -> NatsResult<Duration> {
    value
        .trim()
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|error| NatsError::config(format!("{name} 非法（毫秒整数）: {error}")))
}
