#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 不可达地址的失败路径：`connect()` 必须返回 `Err` 而不是挂起或 panic。
//!
//! 使用 `nats://127.0.0.1:1`（特权端口且无监听）保证连接必然被拒绝，
//! 不依赖任何真实 NATS 服务。

use std::time::{Duration, Instant};

use natsx::{
    JetStream, NatsConfig, NatsError, NatsPool, ENV_CONNECT_TIMEOUT_MS, ENV_LEGACY_PREFIX,
    ENV_PREFIX, ENV_URL,
};

/// 与 `config_env` 同口径：live 注入不得污染默认 `from_env` 断言。
const ENV_SUFFIXES: [&str; 22] = [
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
    "SLOW_CONSUMER_TIMEOUT_MS",
    "SUBSCRIPTION_CAPACITY",
    "CLIENT_CAPACITY",
    "MAX_RECONNECTS",
    "RECONNECT_MAX_DELAY_MS",
    "IGNORE_DISCOVERED_SERVERS",
];

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

fn unreachable_config() -> NatsConfig {
    NatsConfig::builder()
        .url("nats://127.0.0.1:1")
        .name("natsx-test")
        .connect_timeout(Duration::from_millis(300))
        .operation_timeout(Duration::from_millis(300))
        .build()
        .expect("本地拒绝端口配置合法")
}

#[tokio::test]
async fn connect_to_unreachable_address_returns_error() {
    let started = Instant::now();
    let result = NatsPool::connect(unreachable_config()).await;
    let error = result.expect_err("127.0.0.1:1 必然拒绝连接");

    assert!(
        matches!(error, NatsError::Connection(_) | NatsError::Timeout(_)),
        "应归类为连接类错误，实际: {error:?}"
    );
    assert!(error.is_retryable(), "连接被拒属于可重试的瞬时错误");
    assert!(!error.to_string().is_empty());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "connect 必须受 connect_timeout 约束，实际耗时 {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn connect_to_unreachable_address_is_bounded_by_outer_timeout() {
    // 外层硬超时兜底：即使内部逻辑退化也不允许无限挂起
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        NatsPool::connect(unreachable_config()),
    )
    .await
    .expect("connect 必须在硬超时前返回");
    assert!(result.is_err(), "不可达地址必须返回 Err");
}

#[tokio::test]
async fn unconnected_pool_fails_closed_on_data_plane() {
    let pool = NatsPool::new(unreachable_config()).expect("同步构造只校验配置");
    assert!(!pool.is_connected());
    assert!(pool.client().is_none());

    let publish_error = pool
        .publish("orders.created", "payload")
        .await
        .expect_err("未连接不可发布");
    assert!(matches!(publish_error, NatsError::Connection(_)));

    let subscribe_error = pool
        .subscribe("orders.created")
        .await
        .expect_err("未连接不可订阅");
    assert!(matches!(subscribe_error, NatsError::Connection(_)));

    let ping_error = pool.ping().await.expect_err("未连接不可 ping");
    assert!(matches!(ping_error, NatsError::Connection(_)));

    let jetstream_error = JetStream::from_pool(&pool).expect_err("未连接不可构造 JetStream");
    assert!(matches!(jetstream_error, NatsError::Connection(_)));

    // 健康检查是诊断入口：失败不返回 Err，但状态必须是 not connected
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(!health.connected);
    assert!(!health.jetstream);
    assert!(
        health.detail.contains("尚未连接"),
        "detail = {}",
        health.detail
    );

    let stats = pool.stats();
    assert_eq!(stats.published, 0);
    assert_eq!(stats.publish_failed, 0);
    assert!(!stats.closed);

    // new() 也必须拒绝非法配置（此处经反序列化绕过 from_toml 校验）
    let invalid: NatsConfig = toml::from_str("url = \"   \"").expect("反序列化");
    assert!(NatsPool::new(invalid).is_err());
}

#[tokio::test]
async fn connect_from_env_with_unreachable_url_returns_error() {
    let mut scope = EnvScope::new();
    scope.set(ENV_URL, "nats://127.0.0.1:1");
    scope.set(ENV_CONNECT_TIMEOUT_MS, "300");
    let result = NatsPool::connect_from_env().await;
    let error = result.expect_err("不可达地址必须返回 Err");
    assert!(error.is_retryable(), "实际: {error:?}");

    std::env::remove_var(ENV_URL);
    std::env::remove_var(ENV_CONNECT_TIMEOUT_MS);
    let config = NatsConfig::from_env().expect("隔离后默认 env 配置有效");
    assert_eq!(config.url, "nats://127.0.0.1:4222");
}
