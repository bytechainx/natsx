#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 配置校验、环境变量加载、TLS 策略选择与敏感字段脱敏。

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use natsx::{
    url_is_loopback, NatsConfig, TlsPolicy, ENV_CONNECT_TIMEOUT_MS, ENV_LEGACY_PREFIX,
    ENV_MAX_RECONNECTS, ENV_NAME, ENV_PASSWORD, ENV_PREFIX, ENV_SERVERS, ENV_TLS_POLICY, ENV_TOKEN,
    ENV_URL, ENV_USER,
};

/// 环境变量是进程级共享状态，写入 env 的用例必须串行执行。
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_guard() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 规范前缀与兼容前缀下的全部后缀。
///
/// 用例必须对**整个** `FOUNDATIONX_NATSX_*` / `FOUNDATIONX_NATS_*` 空间保持封闭：
/// 本机联调常把 live 凭据 `source` 进 shell（`run-release-gate.sh --live` 即如此），
/// 此时进程环境里已存在 `URL` 等键。`from_env` 的 `URL` 优先于 `SERVERS`，
/// 只清理本用例显式设置的少数键会残留外部注入，导致断言随调用环境漂移。
const ENV_SUFFIXES: [&str; 21] = [
    "URL",
    "SERVERS",
    "USER",
    "USERNAME",
    "PASSWORD",
    "TOKEN",
    "NKEY_SEED",
    "NAME",
    "TLS",
    "TLS_POLICY",
    "TLS_CA_FILE",
    "TLS_CERT_FILE",
    "TLS_KEY_FILE",
    "JETSTREAM",
    "CONNECT_TIMEOUT_MS",
    "OPERATION_TIMEOUT_MS",
    "SUBSCRIPTION_CAPACITY",
    "CLIENT_CAPACITY",
    "MAX_RECONNECTS",
    "RECONNECT_MAX_DELAY_MS",
    "IGNORE_DISCOVERED_SERVERS",
];

/// 环境隔离夹具：构造时快照并清空全部 `FOUNDATIONX_NATS(X)_*` 键，
/// 退出作用域（含 panic）时按快照恢复，使每条用例只看到自己显式设置的内容。
struct EnvScope {
    restore: Vec<(String, Option<String>)>,
}

impl EnvScope {
    fn new() -> Self {
        let mut restore = Vec::new();
        for suffix in ENV_SUFFIXES {
            for prefix in [ENV_PREFIX, ENV_LEGACY_PREFIX] {
                let key = format!("{prefix}{suffix}");
                restore.push((key.clone(), std::env::var(&key).ok()));
                std::env::remove_var(&key);
            }
        }
        Self { restore }
    }

    fn set(&mut self, key: &str, value: &str) {
        std::env::set_var(key, value);
    }

    fn unset(&mut self, key: &str) {
        std::env::remove_var(key);
    }
}

impl Drop for EnvScope {
    fn drop(&mut self) {
        for (key, value) in &self.restore {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[test]
fn defaults_are_valid_and_loopback_friendly() {
    let config = NatsConfig::default();
    assert_eq!(config.url, "nats://127.0.0.1:4222");
    assert_eq!(config.name, "natsx");
    assert!(config.user.is_none());
    assert!(config.password().is_none());
    assert!(config.token().is_none());
    assert!(config.nkey_seed().is_none());
    assert!(!config.tls);
    assert!(config.tls_policy.is_none());
    assert!(!config.jetstream);
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
    assert_eq!(config.operation_timeout, Duration::from_secs(5));
    assert_eq!(config.max_reconnects, 60);
    assert_eq!(config.effective_tls_policy(), TlsPolicy::Prefer);
    config.validate().expect("默认配置必须有效");
}

#[test]
fn tls_policy_is_derived_from_host() {
    // loopback：允许明文，默认 Prefer
    let loopback = NatsConfig::default();
    assert!(url_is_loopback(&loopback.url));
    assert_eq!(loopback.effective_tls_policy(), TlsPolicy::Prefer);
    assert!(!loopback.effective_tls_policy().require_tls());
    assert!(loopback.validate().is_ok());

    // loopback 显式 Disable 也合法（仅用于本地明文联调）
    let loopback_disable = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .tls_policy(TlsPolicy::Disable)
        .build()
        .expect("loopback + disable 合法");
    assert_eq!(loopback_disable.effective_tls_policy(), TlsPolicy::Disable);

    // 非 loopback：自动 Require
    let remote = NatsConfig::builder()
        .url("nats://broker.example.com:4222")
        .build()
        .expect("远程配置");
    assert!(!url_is_loopback(&remote.url));
    assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);
    assert!(remote.effective_tls_policy().require_tls());
    assert!(remote.validate().is_ok());
    assert!(!remote.url_implies_tls());

    // 非 loopback + Prefer / Disable：必须 fail-closed
    for policy in [TlsPolicy::Prefer, TlsPolicy::Disable] {
        let error = NatsConfig::builder()
            .url("nats://broker.example.com:4222")
            .tls_policy(policy)
            .build()
            .expect_err("远程明文必须被拒绝");
        assert!(
            error.to_string().contains("require"),
            "错误信息应说明必须 require: {error}"
        );
    }

    // 遗留布尔开关 tls = true 等价 Require，并让远程地址合法
    let remote_bool = NatsConfig::builder()
        .url("nats://10.0.0.9:4222")
        .tls(true)
        .build()
        .expect("远程 + tls=true 合法");
    assert_eq!(remote_bool.effective_tls_policy(), TlsPolicy::Require);

    // TLS scheme 可被识别并推导出 Require
    let tls_scheme = NatsConfig::builder()
        .url("tls://nats.prod.internal:4222")
        .build()
        .expect("tls:// 远程地址合法");
    assert!(tls_scheme.url_implies_tls());
    assert_eq!(tls_scheme.effective_tls_policy(), TlsPolicy::Require);
}

#[test]
fn validate_rejects_invalid_inputs() {
    // URL 为空
    assert!(NatsConfig::builder().url("   ").build().is_err());
    // URL 内嵌凭据
    assert!(NatsConfig::builder()
        .url("nats://user:secret@127.0.0.1:4222")
        .build()
        .is_err());
    // 只给 user 不给 password
    assert!(NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .credentials("user", "")
        .build()
        .is_err());
    // 零超时 / 零容量 / 零重连次数
    assert!(NatsConfig::builder()
        .connect_timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(NatsConfig::builder()
        .operation_timeout(Duration::ZERO)
        .build()
        .is_err());
    assert!(NatsConfig::builder()
        .reconnect_max_delay(Duration::ZERO)
        .build()
        .is_err());
    assert!(NatsConfig::builder()
        .subscription_capacity(0)
        .build()
        .is_err());
    assert!(NatsConfig::builder().client_capacity(0).build().is_err());
    assert!(NatsConfig::builder().max_reconnects(0).build().is_err());
    // TLS 材料路径不存在
    assert!(NatsConfig::builder()
        .tls_ca_file("/nonexistent/ca.pem")
        .build()
        .is_err());
    assert!(NatsConfig::builder()
        .tls_client_identity("/nonexistent/cert.pem", "/nonexistent/key.pem")
        .build()
        .is_err());
    // 认证材料互斥
    assert!(NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .credentials("user", "pass")
        .token("token")
        .build()
        .is_err());
    assert!(NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .credentials("user", "pass")
        .nkey_seed("SUACB")
        .build()
        .is_err());
    // token / NKey seed 单独使用合法
    assert!(NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .token("token")
        .build()
        .is_ok());
    assert!(NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .nkey_seed("SUACB")
        .build()
        .is_ok());
}

#[test]
fn debug_output_redacts_secrets() {
    let credentialed = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .name("writer")
        .credentials("app-user", "super-secret-pass")
        .build()
        .expect("user/password 配置");
    let debug = format!("{credentialed:?}");
    assert!(debug.contains("***"), "Debug 必须包含脱敏占位符: {debug}");
    assert!(
        !debug.contains("super-secret-pass"),
        "Debug 泄露了 password"
    );
    assert!(debug.contains("writer"), "非敏感字段应保留");
    assert!(debug.contains("NatsConfig"));
    // 访问器仍可编程读取
    assert_eq!(credentialed.password(), Some("super-secret-pass"));
    assert_eq!(
        credentialed.user_password(),
        Some(("app-user", "super-secret-pass"))
    );

    let tokenized = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .token("super-secret-token")
        .build()
        .expect("token 配置");
    let debug = format!("{tokenized:?}");
    assert!(
        !debug.contains("super-secret-token"),
        "Debug 泄露了 token: {debug}"
    );
    assert_eq!(tokenized.token(), Some("super-secret-token"));

    let seeded = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .nkey_seed("SUACBSEEDMATERIAL")
        .build()
        .expect("nkey 配置");
    let debug = format!("{seeded:?}");
    assert!(
        !debug.contains("SUACBSEEDMATERIAL"),
        "Debug 泄露了 NKey seed: {debug}"
    );
    assert_eq!(seeded.nkey_seed(), Some("SUACBSEEDMATERIAL"));

    // URL 内嵌 userinfo 同样脱敏（该配置本身非法，但 Debug 不依赖合法性）
    let embedded: NatsConfig =
        toml::from_str("url = \"nats://embedded-user:embedded-secret@127.0.0.1:4222\"")
            .expect("反序列化");
    let debug = format!("{embedded:?}");
    assert!(
        !debug.contains("embedded-secret"),
        "Debug 泄露了 URL 内嵌密码: {debug}"
    );
    assert!(
        !debug.contains("embedded-user"),
        "Debug 泄露了 URL 内嵌用户名: {debug}"
    );
    assert!(embedded.validate().is_err(), "内嵌 userinfo 的配置必须非法");
}

#[test]
fn from_toml_parses_documented_fields_and_rejects_secrets() {
    let text = r#"
schema_version = 1
url = "nats://127.0.0.1:4222"
name = "reader"
tls_policy = "prefer"
jetstream = true
connect_timeout_ms = 2500
operation_timeout_ms = 3000
subscription_capacity = 64
client_capacity = 32
max_reconnects = 10
reconnect_max_delay_ms = 1000
ignore_discovered_servers = true
"#;
    let config = NatsConfig::from_toml(text).expect("TOML 解析");
    assert_eq!(config.name, "reader");
    assert_eq!(config.connect_timeout, Duration::from_millis(2500));
    assert_eq!(config.operation_timeout, Duration::from_millis(3000));
    assert_eq!(config.subscription_capacity, 64);
    assert_eq!(config.client_capacity, 32);
    assert_eq!(config.max_reconnects, 10);
    assert_eq!(config.reconnect_max_delay, Duration::from_millis(1000));
    assert!(config.jetstream);
    assert!(config.ignore_discovered_servers);
    assert_eq!(config.tls_policy, Some(TlsPolicy::Prefer));
    config.validate().expect("TOML 配置有效");

    // 敏感字段禁止落盘
    for secret in [
        "password = \"x\"",
        "token = \"x\"",
        "nkey_seed = \"x\"",
        "jwt = \"x\"",
    ] {
        let text = format!("schema_version = 1\n{secret}\n");
        assert!(NatsConfig::from_toml(&text).is_err(), "{secret} 必须被拒绝");
    }
    // schema_version 与未知字段
    assert!(NatsConfig::from_toml("schema_version = 2\n").is_err());
    assert!(NatsConfig::from_toml("schema_version = 1\nunknown_key = 1\n").is_err());
    // 非法 TLS 策略字符串
    assert!(NatsConfig::from_toml("schema_version = 1\ntls_policy = \"weird\"\n").is_err());
}

#[test]
fn from_env_reads_values_and_prefers_canonical_prefix() {
    let _guard = env_guard();
    let mut scope = EnvScope::new();

    scope.set(ENV_URL, "nats://127.0.0.1:14300");
    scope.set(ENV_NAME, "env-reader");
    scope.set(ENV_CONNECT_TIMEOUT_MS, "1234");
    scope.set(ENV_MAX_RECONNECTS, "3");
    // user/password 必须成对提供，否则配置层直接拒绝
    scope.set(ENV_USER, "env-user");
    scope.set(ENV_PASSWORD, "env-secret");

    let config = NatsConfig::from_env().expect("from_env");
    assert_eq!(config.url, "nats://127.0.0.1:14300");
    assert_eq!(config.name, "env-reader");
    assert_eq!(config.connect_timeout, Duration::from_millis(1234));
    assert_eq!(config.max_reconnects, 3);
    assert_eq!(config.user_password(), Some(("env-user", "env-secret")));
    assert_eq!(config.password(), Some("env-secret"));
    assert!(!format!("{config:?}").contains("env-secret"));

    // 规范前缀优先于兼容前缀
    scope.set("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:14301");
    assert_eq!(
        NatsConfig::from_env().expect("from_env").url,
        "nats://127.0.0.1:14300"
    );
    scope.unset(ENV_URL);
    assert_eq!(
        NatsConfig::from_env().expect("from_env legacy").url,
        "nats://127.0.0.1:14301"
    );
    scope.unset("FOUNDATIONX_NATS_URL");

    // 认证 token 也可经环境注入，且不泄露到 Debug（token 与 user/password 互斥）
    scope.unset(ENV_USER);
    scope.unset(ENV_PASSWORD);
    scope.set(ENV_TOKEN, "env-token");
    let tokenized = NatsConfig::from_env().expect("from_env token");
    assert_eq!(tokenized.token(), Some("env-token"));
    assert!(!format!("{tokenized:?}").contains("env-token"));
}

#[test]
fn from_env_handles_servers_list_and_invalid_values() {
    let _guard = env_guard();
    let mut scope = EnvScope::new();

    scope.set(
        ENV_SERVERS,
        "nats://127.0.0.1:14310, nats://127.0.0.1:14311",
    );
    assert_eq!(
        NatsConfig::from_env().expect("from_env servers").url,
        "nats://127.0.0.1:14310"
    );
    scope.unset(ENV_SERVERS);

    scope.set(ENV_TLS_POLICY, "not-a-policy");
    assert!(NatsConfig::from_env().is_err(), "非法 TLS 策略必须被拒绝");

    scope.set(ENV_TLS_POLICY, "disable");
    scope.set(ENV_URL, "nats://broker.example.com:4222");
    assert!(NatsConfig::from_env().is_err(), "远程明文必须被拒绝");

    scope.set(ENV_TLS_POLICY, "require");
    let remote = NatsConfig::from_env().expect("远程 + require 合法");
    assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);

    scope.set(ENV_MAX_RECONNECTS, "abc");
    assert!(NatsConfig::from_env().is_err(), "非法数值必须被拒绝");
}
