//! `config` 模块单元测试：默认值、TOML/环境变量加载、TLS 策略与 Builder 校验。
//!
//! 由本模块的 `#[cfg(test)] mod tests;` 引入，仅在测试构建中编译。

#[cfg(test)]
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
    let config: NatsConfig = toml::from_str("url = \"nats://127.0.0.1:4222\"").expect("反序列化");
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
