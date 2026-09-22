//! NATS 客户端配置：TLS 策略、环境变量加载、TOML 解析与 Builder。
//!
//! # 加载优先级
//!
//! 显式字段 / TOML（[`NatsConfig::from_toml`]）< 环境变量（[`NatsConfig::from_env`] 覆盖）。
//!
//! # 环境变量
//!
//! 规范前缀为 `FOUNDATIONX_NATSX_*`，并兼容历史前缀 `FOUNDATIONX_NATS_*`
//! （前者优先）。所有 `ENV_*` 常量在下方公开，便于部署侧核对。
//!
//! # TLS 默认策略
//!
//! 1. 显式 [`TlsPolicy`] 优先；
//! 2. 否则 `tls == true` → [`TlsPolicy::Require`]；
//! 3. 否则按 URL host 自动判定：
//!    - loopback（`127.0.0.1` / `localhost` / `::1`）→ [`TlsPolicy::Prefer`]（允许明文）
//!    - 非 loopback → [`TlsPolicy::Require`]
//!
//! # 敏感字段
//!
//! `password` / `token` / `nkey_seed` 不进入 [`std::fmt::Debug`] 输出（统一渲染为 `***`），
//! URL 中的内嵌 userinfo 也会被脱敏；`validate()` 另外禁止 URL 内嵌凭据。

use std::fmt;
use std::time::Duration;

mod builder;
mod envvars;
mod repr;
mod tls;
mod tomlfile;
mod validate;

pub use builder::NatsConfigBuilder;
pub use tls::{url_is_loopback, TlsPolicy};

/// 配置 schema 版本；TOML 中 `schema_version` 必须等于该值。
pub const SCHEMA_VERSION: u32 = 1;

/// 默认 URL（无认证；生产凭据必须经环境注入）。
pub const DEFAULT_URL: &str = "nats://127.0.0.1:4222";

/// 默认客户端名。
pub const DEFAULT_CLIENT_NAME: &str = "natsx";

/// 规范环境变量前缀。
pub const ENV_PREFIX: &str = "FOUNDATIONX_NATSX_";

/// 兼容的历史环境变量前缀（优先级低于 [`ENV_PREFIX`]）。
pub const ENV_LEGACY_PREFIX: &str = "FOUNDATIONX_NATS_";

/// `FOUNDATIONX_NATSX_URL`：服务器地址。
pub const ENV_URL: &str = "FOUNDATIONX_NATSX_URL";
/// `FOUNDATIONX_NATSX_SERVERS`：逗号分隔的服务器列表（取首个作为主地址）。
pub const ENV_SERVERS: &str = "FOUNDATIONX_NATSX_SERVERS";
/// `FOUNDATIONX_NATSX_USER`：用户名。
pub const ENV_USER: &str = "FOUNDATIONX_NATSX_USER";
/// `FOUNDATIONX_NATSX_USERNAME`：用户名（别名）。
pub const ENV_USERNAME: &str = "FOUNDATIONX_NATSX_USERNAME";
/// `FOUNDATIONX_NATSX_PASSWORD`：密码（敏感）。
pub const ENV_PASSWORD: &str = "FOUNDATIONX_NATSX_PASSWORD";
/// `FOUNDATIONX_NATSX_TOKEN`：认证 token（敏感）。
pub const ENV_TOKEN: &str = "FOUNDATIONX_NATSX_TOKEN";
/// `FOUNDATIONX_NATSX_NKEY_SEED`：NKey seed（敏感）。
pub const ENV_NKEY_SEED: &str = "FOUNDATIONX_NATSX_NKEY_SEED";
/// `FOUNDATIONX_NATSX_NAME`：客户端名。
pub const ENV_NAME: &str = "FOUNDATIONX_NATSX_NAME";
/// `FOUNDATIONX_NATSX_TLS`：遗留 TLS 布尔开关。
pub const ENV_TLS: &str = "FOUNDATIONX_NATSX_TLS";
/// `FOUNDATIONX_NATSX_TLS_POLICY`：TLS 策略（disable / prefer / require）。
pub const ENV_TLS_POLICY: &str = "FOUNDATIONX_NATSX_TLS_POLICY";
/// `FOUNDATIONX_NATSX_TLS_CA_FILE`：CA bundle（PEM）。
pub const ENV_TLS_CA_FILE: &str = "FOUNDATIONX_NATSX_TLS_CA_FILE";
/// `FOUNDATIONX_NATSX_TLS_CERT_FILE`：mTLS 客户端证书（PEM）。
pub const ENV_TLS_CERT_FILE: &str = "FOUNDATIONX_NATSX_TLS_CERT_FILE";
/// `FOUNDATIONX_NATSX_TLS_KEY_FILE`：mTLS 客户端私钥（PEM）。
pub const ENV_TLS_KEY_FILE: &str = "FOUNDATIONX_NATSX_TLS_KEY_FILE";
/// `FOUNDATIONX_NATSX_JETSTREAM`：是否期望使用 JetStream。
pub const ENV_JETSTREAM: &str = "FOUNDATIONX_NATSX_JETSTREAM";
/// `FOUNDATIONX_NATSX_CONNECT_TIMEOUT_MS`：连接超时（毫秒）。
pub const ENV_CONNECT_TIMEOUT_MS: &str = "FOUNDATIONX_NATSX_CONNECT_TIMEOUT_MS";
/// `FOUNDATIONX_NATSX_OPERATION_TIMEOUT_MS`：操作超时（毫秒）。
pub const ENV_OPERATION_TIMEOUT_MS: &str = "FOUNDATIONX_NATSX_OPERATION_TIMEOUT_MS";
/// `FOUNDATIONX_NATSX_SUBSCRIPTION_CAPACITY`：单订阅缓冲上限。
pub const ENV_SUBSCRIPTION_CAPACITY: &str = "FOUNDATIONX_NATSX_SUBSCRIPTION_CAPACITY";
/// `FOUNDATIONX_NATSX_CLIENT_CAPACITY`：驱动命令队列容量。
pub const ENV_CLIENT_CAPACITY: &str = "FOUNDATIONX_NATSX_CLIENT_CAPACITY";
/// `FOUNDATIONX_NATSX_MAX_RECONNECTS`：最大重连次数。
pub const ENV_MAX_RECONNECTS: &str = "FOUNDATIONX_NATSX_MAX_RECONNECTS";
/// `FOUNDATIONX_NATSX_RECONNECT_MAX_DELAY_MS`：单次重连退避上限（毫秒）。
pub const ENV_RECONNECT_MAX_DELAY_MS: &str = "FOUNDATIONX_NATSX_RECONNECT_MAX_DELAY_MS";
/// `FOUNDATIONX_NATSX_IGNORE_DISCOVERED_SERVERS`：是否忽略服务端发现的地址。
pub const ENV_IGNORE_DISCOVERED_SERVERS: &str = "FOUNDATIONX_NATSX_IGNORE_DISCOVERED_SERVERS";

/// NATS 客户端配置。
///
/// 可直接反序列化 TOML（`schema_version` + 扁平字段），
/// 但**敏感字段**（`password` / `token` / `nkey_seed`）不允许出现在 TOML 中，
/// 只能经环境变量或 [`NatsConfigBuilder`] 注入。
#[derive(Clone)]
pub struct NatsConfig {
    /// 服务器 URL。
    pub url: String,
    /// 用户名。
    pub user: Option<String>,
    /// 密码（敏感，不进入 `Debug`）。
    password: Option<String>,
    /// 认证 token（敏感，不进入 `Debug`）。
    token: Option<String>,
    /// NKey seed（敏感，不进入 `Debug`）。
    nkey_seed: Option<String>,
    /// 连接超时。
    pub connect_timeout: Duration,
    /// Core NATS / JetStream 操作截止时间。
    pub operation_timeout: Duration,
    /// 客户端名。
    pub name: String,
    /// 遗留 TLS 布尔开关（`true` 等价于 [`TlsPolicy::Require`]）。
    pub tls: bool,
    /// 显式 TLS 策略；`None` 时按 host 自动解析。
    pub tls_policy: Option<TlsPolicy>,
    /// 是否期望使用 JetStream（文档/校验标志，不影响 Core NATS 连接）。
    pub jetstream: bool,
    /// 每订阅缓冲上限。
    pub subscription_capacity: usize,
    /// 驱动命令发送队列容量。
    pub client_capacity: usize,
    /// 连续重连最大尝试次数（必须为有限正数）。
    pub max_reconnects: usize,
    /// 单次重连退避上限。
    pub reconnect_max_delay: Duration,
    /// 是否忽略服务端发现的地址，仅重连显式 URL。
    pub ignore_discovered_servers: bool,
    /// CA bundle 文件路径（PEM）。
    pub tls_ca_file: Option<String>,
    /// mTLS 客户端证书路径（PEM）。
    pub tls_cert_file: Option<String>,
    /// mTLS 客户端私钥路径（PEM）。
    pub tls_key_file: Option<String>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_string(),
            // 无默认账号：避免把草稿/过期凭据写进库；由 FOUNDATIONX_NATSX_* 注入
            user: None,
            password: None,
            token: None,
            nkey_seed: None,
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(5),
            name: DEFAULT_CLIENT_NAME.to_string(),
            tls: false,
            tls_policy: None,
            jetstream: false,
            subscription_capacity: 256,
            client_capacity: 256,
            max_reconnects: 60,
            reconnect_max_delay: Duration::from_secs(5),
            ignore_discovered_servers: false,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
        }
    }
}

impl fmt::Debug for NatsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsConfig")
            .field("url", &redact_url(&self.url))
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("nkey_seed", &self.nkey_seed.as_ref().map(|_| "***"))
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("name", &self.name)
            .field("tls", &self.tls)
            .field("tls_policy", &self.tls_policy)
            .field("jetstream", &self.jetstream)
            .field("subscription_capacity", &self.subscription_capacity)
            .field("client_capacity", &self.client_capacity)
            .field("max_reconnects", &self.max_reconnects)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("ignore_discovered_servers", &self.ignore_discovered_servers)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("tls_cert_file", &self.tls_cert_file)
            .field("tls_key_file", &self.tls_key_file)
            .finish()
    }
}

fn redact_url(raw: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(raw) else {
        return "<invalid-url>".to_string();
    };
    if !parsed.username().is_empty() {
        let _ = parsed.set_username("***");
    }
    if parsed.password().is_some() {
        let _ = parsed.set_password(Some("***"));
    }
    parsed.to_string()
}

impl NatsConfig {
    /// 创建 Builder（以 [`NatsConfig::default`] 为基线）。
    #[must_use]
    pub fn builder() -> NatsConfigBuilder {
        NatsConfigBuilder::new()
    }

    /// 密码（敏感）。
    #[must_use]
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// 认证 token（敏感）。
    #[must_use]
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// NKey seed（敏感）。
    #[must_use]
    pub fn nkey_seed(&self) -> Option<&str> {
        self.nkey_seed.as_deref()
    }

    /// 用户名 + 密码（两者同时存在时返回）。
    #[must_use]
    pub fn user_password(&self) -> Option<(&str, &str)> {
        match (self.user.as_deref(), self.password.as_deref()) {
            (Some(user), Some(password)) => Some((user, password)),
            _ => None,
        }
    }

    /// 生效的 TLS 策略（显式 > `tls` 布尔 > host 自动判定）。
    #[must_use]
    pub fn effective_tls_policy(&self) -> TlsPolicy {
        if let Some(policy) = self.tls_policy {
            return policy;
        }
        if self.tls {
            return TlsPolicy::Require;
        }
        if url_is_loopback(&self.url) {
            TlsPolicy::Prefer
        } else {
            TlsPolicy::Require
        }
    }

    /// URL 是否声明了 TLS scheme（`tls://` / `nats+tls://` / `wss://`）。
    #[must_use]
    pub fn url_implies_tls(&self) -> bool {
        let lower = self.url.trim().to_ascii_lowercase();
        lower.starts_with("tls://")
            || lower.starts_with("nats+tls://")
            || lower.starts_with("wss://")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 环境变量是进程级共享状态，读写 env 的用例必须串行执行。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 退出作用域（含 panic）时自动清理本次设置的环境变量。
    struct EnvScope {
        keys: Vec<String>,
    }

    impl EnvScope {
        fn new() -> Self {
            Self { keys: Vec::new() }
        }

        fn set(&mut self, key: &str, value: &str) {
            std::env::set_var(key, value);
            self.keys.push(key.to_string());
        }
    }

    impl Drop for EnvScope {
        fn drop(&mut self) {
            for key in &self.keys {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn defaults_are_loopback_prefer() {
        let config = NatsConfig::default();
        assert_eq!(config.url, DEFAULT_URL);
        assert!(config.user.is_none());
        assert!(config.password().is_none());
        assert!(config.token().is_none());
        assert!(config.nkey_seed().is_none());
        assert!(!config.tls);
        assert!(config.tls_policy.is_none());
        assert!(!config.jetstream);
        assert_eq!(config.effective_tls_policy(), TlsPolicy::Prefer);
        config.validate().expect("默认配置必须有效");
    }

    #[test]
    fn from_toml_parses_flat_fields() {
        let text = r#"
schema_version = 1
url = "nats://127.0.0.1:4223"
name = "writer"
jetstream = true
connect_timeout_ms = 8000
subscription_capacity = 128
"#;
        let config = NatsConfig::from_toml(text).expect("TOML 解析");
        assert_eq!(config.url, "nats://127.0.0.1:4223");
        assert_eq!(config.name, "writer");
        assert!(config.jetstream);
        assert_eq!(config.connect_timeout, Duration::from_millis(8000));
        assert_eq!(config.subscription_capacity, 128);
        assert!(config.password().is_none());
    }

    #[test]
    fn from_toml_rejects_secrets_schema_and_unknown_fields() {
        assert!(NatsConfig::from_toml("schema_version = 1\npassword = \"x\"\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 1\ntoken = \"x\"\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 1\nnkey_seed = \"x\"\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 99\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 1\nsink_id = \"x\"\n").is_err());
    }

    #[test]
    fn direct_serde_deserialization_uses_defaults() {
        let config: NatsConfig =
            toml::from_str("url = \"nats://127.0.0.1:4222\"").expect("反序列化");
        assert_eq!(config.name, DEFAULT_CLIENT_NAME);
        assert_eq!(config.operation_timeout, Duration::from_secs(5));
    }

    #[test]
    fn debug_redacts_all_sensitive_material() {
        let config = NatsConfig {
            url: "nats://embedded-user:embedded-secret@localhost:4222".into(),
            user: Some("embedded-user".into()),
            password: Some("super-secret-pass".into()),
            token: Some("super-secret-token".into()),
            nkey_seed: Some("SUACBSEED".into()),
            ..NatsConfig::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        for secret in [
            "super-secret-pass",
            "super-secret-token",
            "SUACBSEED",
            "embedded-secret",
        ] {
            assert!(!debug.contains(secret), "Debug 泄露了 {secret}");
        }
    }

    #[test]
    fn tls_policy_defaults_follow_host() {
        assert_eq!(
            NatsConfig::default().effective_tls_policy(),
            TlsPolicy::Prefer
        );

        let remote = NatsConfig {
            url: "nats://broker.example.com:4222".into(),
            ..NatsConfig::default()
        };
        assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);
        assert!(remote.validate().is_ok());
        assert!(!remote.url_implies_tls());

        let remote_disable = NatsConfig {
            url: "nats://broker.example.com:4222".into(),
            tls_policy: Some(TlsPolicy::Disable),
            ..NatsConfig::default()
        };
        assert!(remote_disable.validate().is_err());

        let remote_prefer = NatsConfig {
            url: "nats://broker.example.com:4222".into(),
            tls_policy: Some(TlsPolicy::Prefer),
            ..NatsConfig::default()
        };
        assert!(remote_prefer.validate().is_err());

        let tls_bool = NatsConfig {
            url: "nats://10.0.0.9:4222".into(),
            tls: true,
            ..NatsConfig::default()
        };
        assert_eq!(tls_bool.effective_tls_policy(), TlsPolicy::Require);
        assert!(tls_bool.validate().is_ok());
    }

    #[test]
    fn url_is_loopback_matrix() {
        assert!(url_is_loopback("nats://127.0.0.1:4222"));
        assert!(url_is_loopback("nats://localhost:4222"));
        assert!(url_is_loopback("nats://[::1]:4222"));
        assert!(url_is_loopback("tls://LOCALHOST:4222"));
        assert!(!url_is_loopback("nats://10.0.0.5:4222"));
        assert!(!url_is_loopback("tls://nats.prod.internal:4222"));
        assert!(!url_is_loopback("nats://0.0.0.0:4222"));
    }

    #[test]
    fn tls_policy_parse_aliases() {
        assert_eq!(
            TlsPolicy::parse("require").expect("require"),
            TlsPolicy::Require
        );
        assert_eq!(
            TlsPolicy::parse("REQUIRED").expect("REQUIRED"),
            TlsPolicy::Require
        );
        assert_eq!(TlsPolicy::parse("off").expect("off"), TlsPolicy::Disable);
        assert_eq!(TlsPolicy::parse(" auto ").expect("auto"), TlsPolicy::Prefer);
        assert!(TlsPolicy::parse("weird").is_err());
        assert!(TlsPolicy::Require.require_tls());
        assert!(!TlsPolicy::Prefer.require_tls());
        assert_eq!(TlsPolicy::Disable.to_string(), "disable");
    }

    #[test]
    fn validate_rejects_partial_auth_and_unbounded_resources() {
        let cases = [
            NatsConfig {
                url: "  ".into(),
                ..NatsConfig::default()
            },
            NatsConfig {
                url: "nats://user:pass@127.0.0.1:4222".into(),
                ..NatsConfig::default()
            },
            NatsConfig {
                user: Some("u".into()),
                ..NatsConfig::default()
            },
            NatsConfig {
                connect_timeout: Duration::ZERO,
                ..NatsConfig::default()
            },
            NatsConfig {
                operation_timeout: Duration::ZERO,
                ..NatsConfig::default()
            },
            NatsConfig {
                reconnect_max_delay: Duration::ZERO,
                ..NatsConfig::default()
            },
            NatsConfig {
                subscription_capacity: 0,
                ..NatsConfig::default()
            },
            NatsConfig {
                client_capacity: 0,
                ..NatsConfig::default()
            },
            NatsConfig {
                max_reconnects: 0,
                ..NatsConfig::default()
            },
        ];
        for config in cases {
            assert!(config.validate().is_err(), "必须拒绝非法配置: {config:?}");
        }
    }

    #[test]
    fn validate_rejects_conflicting_auth_material() {
        let both = NatsConfig {
            user: Some("u".into()),
            password: Some("p".into()),
            token: Some("t".into()),
            ..NatsConfig::default()
        };
        assert!(both.validate().is_err());

        let nkey_and_user = NatsConfig {
            user: Some("u".into()),
            password: Some("p".into()),
            nkey_seed: Some("SUACB".into()),
            ..NatsConfig::default()
        };
        assert!(nkey_and_user.validate().is_err());

        let token_only = NatsConfig {
            token: Some("t".into()),
            ..NatsConfig::default()
        };
        assert!(token_only.validate().is_ok());

        let nkey_only = NatsConfig {
            nkey_seed: Some("SUACB".into()),
            ..NatsConfig::default()
        };
        assert!(nkey_only.validate().is_ok());
    }

    #[test]
    fn validate_checks_tls_material_paths() {
        let cert_only = NatsConfig {
            tls_cert_file: Some("/nonexistent/cert.pem".into()),
            ..NatsConfig::default()
        };
        assert!(cert_only.validate().is_err());

        let key_only = NatsConfig {
            tls_key_file: Some("/nonexistent/key.pem".into()),
            ..NatsConfig::default()
        };
        assert!(key_only.validate().is_err());

        let ca_missing = NatsConfig {
            tls_ca_file: Some("/nonexistent/ca.pem".into()),
            ..NatsConfig::default()
        };
        assert!(ca_missing.validate().is_err());
    }

    #[test]
    fn builder_applies_overrides_and_validates() {
        let config = NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .name("worker")
            .credentials("user", "pass")
            .jetstream(true)
            .operation_timeout(Duration::from_secs(3))
            .max_reconnects(7)
            .build()
            .expect("Builder 构建");
        assert_eq!(config.name, "worker");
        assert_eq!(config.operation_timeout, Duration::from_secs(3));
        assert_eq!(config.max_reconnects, 7);
        assert_eq!(config.user_password(), Some(("user", "pass")));

        // 远程地址 + 显式 Prefer 必须被拒绝
        assert!(NatsConfig::builder()
            .url("nats://broker.example.com:4222")
            .tls_policy(TlsPolicy::Prefer)
            .build()
            .is_err());
        // 远程地址未显式指定策略时自动 Require，构建通过
        let remote = NatsConfig::builder()
            .url("nats://broker.example.com:4222")
            .build()
            .expect("远程地址默认 Require");
        assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);
        // 零超时同样被拒绝
        assert!(NatsConfig::builder()
            .operation_timeout(Duration::ZERO)
            .build()
            .is_err());
    }

    #[test]
    fn from_env_reads_canonical_prefix_and_validates() {
        let _guard = env_guard();
        let mut scope = EnvScope::new();
        for key in [ENV_URL, "FOUNDATIONX_NATS_URL", ENV_CONNECT_TIMEOUT_MS] {
            std::env::remove_var(key);
        }
        scope.set(ENV_URL, "nats://127.0.0.1:14222");
        scope.set(ENV_CONNECT_TIMEOUT_MS, "1500");
        let config = NatsConfig::from_env().expect("from_env");
        assert_eq!(config.url, "nats://127.0.0.1:14222");
        assert_eq!(config.connect_timeout, Duration::from_millis(1500));
    }

    #[test]
    fn from_env_prefers_canonical_over_legacy_prefix() {
        let _guard = env_guard();
        let mut scope = EnvScope::new();
        scope.set(ENV_URL, "nats://127.0.0.1:14223");
        scope.set("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:14224");
        let config = NatsConfig::from_env().expect("from_env");
        assert_eq!(config.url, "nats://127.0.0.1:14223");

        std::env::remove_var(ENV_URL);
        let legacy = NatsConfig::from_env().expect("from_env legacy");
        assert_eq!(legacy.url, "nats://127.0.0.1:14224");
    }

    #[test]
    fn from_env_rejects_invalid_values() {
        let _guard = env_guard();
        let mut scope = EnvScope::new();
        scope.set(ENV_TLS_POLICY, "weird");
        assert!(NatsConfig::from_env().is_err(), "非法 TLS 策略必须被拒绝");

        // 远程地址 + disable：env 覆盖后同样 fail-closed
        scope.set(ENV_TLS_POLICY, "disable");
        scope.set(ENV_URL, "nats://broker.example.com:4222");
        assert!(NatsConfig::from_env().is_err(), "远程明文必须被拒绝");

        // 远程地址 + require：合法，且生效策略为 Require
        scope.set(ENV_TLS_POLICY, "require");
        let remote = NatsConfig::from_env().expect("远程 + require 合法");
        assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);

        // 非法数值同样被拒绝
        scope.set(ENV_MAX_RECONNECTS, "not-a-number");
        assert!(NatsConfig::from_env().is_err());
    }
}
