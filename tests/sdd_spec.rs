#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的章节条款转成可执行断言。
//!
//! // SPEC-MAP: S-1 | 1. 定位 | assert_positioning
//! // SPEC-MAP: S-2 | 2. 配置治理 | assert_config_governance
//! // SPEC-MAP: S-3 | 3. TLS 策略（fail-closed） | assert_tls_policy_fail_closed
//! // SPEC-MAP: S-4 | 4. 连接与消费 | assert_connection_and_consumption
//! // SPEC-MAP: S-5 | 5. 验收 | assert_acceptance

use std::time::Duration;

use natsx::{
    url_is_loopback, validate_consumer_name, validate_operation_timeout, validate_publish_subject,
    validate_stream_name, validate_subject, JetStreamConsumerConfig, NatsConfig, NatsError,
    NatsHealth, NatsPool, NatsPoolStats, PullConsumerConfig, StreamConfig, TlsPolicy, DEFAULT_URL,
    ENV_CLIENT_CAPACITY, ENV_CONNECT_TIMEOUT_MS, ENV_JETSTREAM, ENV_MAX_RECONNECTS, ENV_NAME,
    ENV_NKEY_SEED, ENV_OPERATION_TIMEOUT_MS, ENV_PASSWORD, ENV_PREFIX, ENV_RECONNECT_MAX_DELAY_MS,
    ENV_SERVERS, ENV_SLOW_CONSUMER_TIMEOUT_MS, ENV_SUBSCRIPTION_CAPACITY, ENV_TLS, ENV_TLS_CA_FILE,
    ENV_TLS_CERT_FILE, ENV_TLS_KEY_FILE, ENV_TLS_POLICY, ENV_TOKEN, ENV_URL, ENV_USER,
    ENV_USERNAME, SCHEMA_VERSION,
};

fn assert_send_sync<T: Send + Sync>() {}
fn assert_type<T>() {}

/// S-1：定位——Core NATS 发布/订阅 + JetStream 持久消费 + TLS 策略 + 连接池 + 结构化健康；
/// 零内部依赖，且不实现服务端 / K-V·Object Store 高层封装。
#[test]
fn assert_positioning() {
    // 连接池：同步构造不联网，可达性由 ping / health_check 承担。
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置可构造");
    assert!(!pool.is_connected(), "new 不得建立连接");
    assert!(pool.client().is_none(), "未连接不得暴露客户端句柄");

    // Core NATS 面：subject 校验是纯函数，订阅允许通配符、发布不允许。
    assert!(validate_subject("orders.*").is_ok());
    assert!(validate_publish_subject("orders.*").is_err());

    // JetStream 面：拉取消费的配置类型齐备（不自动 ack）。
    assert!(validate_stream_name("ORDERS").is_ok());
    assert!(validate_consumer_name("worker").is_ok());
    let stream = StreamConfig::new("ORDERS", "orders.>");
    assert_eq!(stream.name, "ORDERS");
    assert_eq!(PullConsumerConfig::durable("worker").durable_name, "worker");
    assert!(JetStreamConsumerConfig::ephemeral().durable_name.is_none());
    assert_type::<natsx::JetStream>();

    // 能力缺失以 Unsupported 表达。
    assert!(matches!(
        NatsError::unsupported("K/V 高层封装不在范围内"),
        NatsError::Unsupported(_)
    ));
}

/// S-2：配置治理——schema 版本、统一 env 前缀、TOML 扁平字段、敏感字段禁入、
/// Debug 脱敏、有界队列与必备超时。
#[test]
fn assert_config_governance() {
    assert_eq!(SCHEMA_VERSION, 1);
    for name in [
        ENV_URL,
        ENV_SERVERS,
        ENV_USER,
        ENV_USERNAME,
        ENV_PASSWORD,
        ENV_TOKEN,
        ENV_NKEY_SEED,
        ENV_NAME,
        ENV_TLS,
        ENV_TLS_POLICY,
        ENV_TLS_CA_FILE,
        ENV_TLS_CERT_FILE,
        ENV_TLS_KEY_FILE,
        ENV_JETSTREAM,
        ENV_CONNECT_TIMEOUT_MS,
        ENV_OPERATION_TIMEOUT_MS,
        ENV_SLOW_CONSUMER_TIMEOUT_MS,
        ENV_SUBSCRIPTION_CAPACITY,
        ENV_CLIENT_CAPACITY,
        ENV_MAX_RECONNECTS,
        ENV_RECONNECT_MAX_DELAY_MS,
    ] {
        assert!(name.starts_with(ENV_PREFIX), "{name} 未使用规范前缀");
    }

    // TOML 扁平字段。
    let from_toml = NatsConfig::from_toml(
        "schema_version = 1\nurl = \"nats://127.0.0.1:4223\"\nname = \"writer\"\njetstream = true\nsubscription_capacity = 128\n",
    )
    .expect("合法 TOML");
    assert_eq!(from_toml.name, "writer");
    assert!(from_toml.jetstream);
    assert_eq!(from_toml.subscription_capacity, 128);
    assert!(from_toml.password().is_none(), "TOML 不得注入凭据");

    // 敏感字段与未知字段禁入 TOML。
    for key in ["password", "token", "nkey_seed", "jwt"] {
        assert!(
            NatsConfig::from_toml(&format!("schema_version = 1\n{key} = \"x\"\n")).is_err(),
            "{key} 必须被拒绝"
        );
    }
    assert!(NatsConfig::from_toml("schema_version = 1\nunknown = 1\n").is_err());

    // Debug 脱敏：密码与 URL 内嵌 userinfo。
    let mut with_secrets = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .credentials("sdd-user", "sdd-password-value")
        .build()
        .expect("回环凭据合法");
    with_secrets.url = "nats://embedded-user:embedded-password@127.0.0.1:4222".into();
    let text = format!("{with_secrets:?}");
    assert!(
        !text.contains("sdd-password-value"),
        "Debug 泄露密码: {text}"
    );
    assert!(
        !text.contains("embedded-user"),
        "Debug 泄露 userinfo: {text}"
    );
    assert!(text.contains("***"), "应出现脱敏占位: {text}");

    // 有界队列与必备超时都是可配置项，且非法值 fail-fast。
    let config = NatsConfig::default();
    assert!(config.subscription_capacity > 0 && config.client_capacity > 0);
    assert!(validate_operation_timeout(config.operation_timeout).is_ok());
    assert!(validate_operation_timeout(Duration::ZERO).is_err());
}

/// S-3：TLS 策略——loopback 默认 `Prefer`、非 loopback 默认 `Require`、
/// 「非 loopback + 非 Require」被拒绝、显式策略优先于布尔开关。
#[test]
fn assert_tls_policy_fail_closed() {
    assert_eq!(TlsPolicy::default(), TlsPolicy::Prefer);
    assert!(url_is_loopback(DEFAULT_URL));
    assert!(NatsConfig::default().effective_tls_policy() == TlsPolicy::Prefer);

    let mut remote = NatsConfig::default();
    remote.url = "nats://nats.example.com:4222".into();
    assert_eq!(
        remote.effective_tls_policy(),
        TlsPolicy::Require,
        "非 loopback 默认必须 Require"
    );

    remote.tls_policy = Some(TlsPolicy::Disable);
    assert!(
        remote.validate().is_err(),
        "非 loopback + disable 必须 fail-closed"
    );

    // 显式策略优先于 tls 布尔开关。
    let mut explicit = NatsConfig::default();
    explicit.url = "nats://nats.example.com:4222".into();
    explicit.tls = false;
    explicit.tls_policy = Some(TlsPolicy::Require);
    assert_eq!(explicit.effective_tls_policy(), TlsPolicy::Require);
    explicit.validate().expect("非 loopback + Require 合法");

    // tls 布尔为真等价于 Require。
    let mut boolean = NatsConfig::default();
    boolean.url = "nats://nats.example.com:4222".into();
    boolean.tls = true;
    assert_eq!(boolean.effective_tls_policy(), TlsPolicy::Require);
    assert!(TlsPolicy::Require.require_tls());
    assert!(!TlsPolicy::Prefer.require_tls());

    // 自定义 CA 必须可访问；策略字符串解析不区分大小写。
    assert!(TlsPolicy::parse("REQUIRE").is_ok());
    assert!(TlsPolicy::parse("off").expect("别名") == TlsPolicy::Disable);
    assert!(TlsPolicy::parse("nonsense").is_err());
    assert!(
        NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .tls_ca_file("/nonexistent/natsx-sdd-ca.pem")
            .build()
            .is_err(),
        "CA 文件不存在必须被拒绝"
    );
}

/// S-4：连接与消费——池可克隆且 `Send + Sync`、未连接 fail-closed、
/// `close` 立即关闭而 `drain` 优雅排空、校验先于协议层。
#[tokio::test]
async fn assert_connection_and_consumption() {
    assert_send_sync::<NatsPool>();
    assert_send_sync::<NatsConfig>();

    let pool = NatsPool::new(NatsConfig::default()).expect("构造");
    let cloned = pool.clone();
    assert!(!cloned.is_connected());

    // 未连接：数据面全部 fail-closed，且不 panic。
    assert!(pool.publish("orders.created", "x").await.is_err());
    assert!(pool.subscribe("orders.created").await.is_err());
    assert!(pool.ping().await.is_err());
    assert!(pool.flush().await.is_err());
    assert!(pool
        .request("orders.get", "x", Duration::from_secs(1))
        .await
        .is_err());

    // close 立即关闭；drain 拒绝零 deadline、接受正 deadline。
    pool.close().await.expect("未连接池 close 成功");
    assert!(pool.stats().closed, "close 后必须标记 closed");
    assert!(pool.publish("orders.created", "x").await.is_err());
    assert!(
        pool.drain(Duration::ZERO).await.is_err(),
        "零 deadline 必须被拒绝"
    );
    assert!(pool.drain(Duration::from_millis(50)).await.is_ok());

    // subject / stream / consumer 名称校验在协议层之前生效。
    assert!(validate_subject("has space").is_err());
    assert!(validate_stream_name("bad.name").is_err());
    assert!(validate_consumer_name("bad*name").is_err());

    // 健康与统计是结构化数据。
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(!health.connected);
    assert_eq!(health.rtt_ms, 0.0);
    assert!(health.detail.contains("已关闭"), "detail={}", health.detail);
    let stats = pool.stats();
    assert!(stats.closed);
    assert_eq!(NatsPoolStats::default().published, 0);
    let _ = NatsHealth {
        connected: false,
        server: String::new(),
        rtt_ms: 0.0,
        jetstream: false,
        detail: String::new(),
    };
}

/// S-5：验收——默认 `cargo test` 离线；分层锚点见 `标准.md` §5。本函数只证离线可达常量与纯函数。
#[test]
fn assert_acceptance() {
    // 公开 API 面：常量与纯函数可达。
    assert_eq!(DEFAULT_URL, "nats://127.0.0.1:4222");
    assert_eq!(SCHEMA_VERSION, 1);
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_NATSX_");
    assert!(url_is_loopback("nats://localhost:4222"));
    assert!(validate_publish_subject("orders.created").is_ok());
    assert!(validate_operation_timeout(Duration::from_secs(1)).is_ok());

    // 校验纯函数在发出请求前拒绝非法名称（不依赖任何服务）。
    assert!(validate_subject("").is_err());
    assert!(validate_stream_name("bad>name").is_err());

    // 错误分类稳定，便于日志与指标打标。
    assert_eq!(NatsError::config("x").kind_name(), "config");
    assert_eq!(NatsError::timeout("x").kind_name(), "timeout");
    assert!(NatsError::timeout("x").is_retryable());

    // 验收命令在仓库根执行，当前目录必须可取得。
    assert!(std::env::current_dir().is_ok());
}
