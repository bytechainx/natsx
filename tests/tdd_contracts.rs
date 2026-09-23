#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）：逐公开入口先红后绿。
//!
//! 入口集合 = `specs/features/002-public-api-compliance-and-test-tiers/contracts/public-api-contract.md`
//! 的 natsx 节。每个入口在变异副本上观测红、在原树观测绿；实际执行的变异与红摘要见 PR 描述。
//!
//! // TDD-PROBE: NatsConfig::from_env | 变异：lookup_env 不再让规范前缀优先于兼容前缀 | 红=from_env_prefers_canonical_prefix | 绿=from_env_prefers_canonical_prefix
//! // TDD-PROBE: NatsConfig::from_toml | 变异：reject_secret_keys 不再拒绝 password | 红=from_toml_parses_and_rejects_secrets | 绿=from_toml_parses_and_rejects_secrets
//! // TDD-PROBE: NatsConfig::validate | 变异：去掉「非 loopback 必须 Require」这条 guard | 红=validate_fail_closed_matrix | 绿=validate_fail_closed_matrix
//! // TDD-PROBE: NatsPool::connect | 变异：connect 不执行 validate | 红=connect_refused_is_retryable_and_bounded | 绿=connect_refused_is_retryable_and_bounded
//! // TDD-PROBE: NatsPool::publish | 变异：publish 不再校验发布 subject | 红=publish_validates_subject_and_connection | 绿=publish_validates_subject_and_connection
//! // TDD-PROBE: NatsPool::subscribe | 变异：subscribe 不再校验 subject | 红=subscribe_validates_subject_and_connection | 绿=subscribe_validates_subject_and_connection
//! // TDD-PROBE: NatsPool::request | 变异：request 接受零 deadline | 红=request_validates_deadline_and_connection | 绿=request_validates_deadline_and_connection
//! // TDD-PROBE: NatsPool::ping | 变异：未连接池的 ping 返回 Ok(Duration::ZERO) | 红=ping_requires_connection | 绿=ping_requires_connection
//! // TDD-PROBE: NatsPool::health_check | 变异：未连接时 health_check 报 Err 而非结构化 false | 红=health_check_is_structured_when_disconnected | 绿=health_check_is_structured_when_disconnected
//! // TDD-PROBE: NatsError::is_retryable | 变异：把 Timeout 从可重试集合移除（重试分类反转） | 红=is_retryable_classification_is_conservative | 绿=is_retryable_classification_is_conservative

use std::time::Duration;

use natsx::{NatsConfig, NatsError, NatsPool, ENV_LEGACY_PREFIX, ENV_PREFIX, ENV_URL, ENV_USER};

/// 本用例会读写的环境变量后缀（规范前缀 + 兼容前缀两份）。
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

/// 清理全部 `FOUNDATIONX_NATSX_*` 与 `FOUNDATIONX_NATS_*` 键，避免外部注入污染断言。
fn clear_env() {
    for suffix in ENV_SUFFIXES {
        for prefix in [ENV_PREFIX, ENV_LEGACY_PREFIX] {
            std::env::remove_var(format!("{prefix}{suffix}"));
        }
    }
}

/// 入口 `NatsConfig::from_env`：规范前缀优先于兼容前缀，且凭据在 Debug 中脱敏。
#[test]
fn from_env_prefers_canonical_prefix() {
    clear_env();
    let legacy_url = format!("{ENV_LEGACY_PREFIX}URL");
    let legacy_user = format!("{ENV_LEGACY_PREFIX}USER");
    let legacy_password = format!("{ENV_LEGACY_PREFIX}PASSWORD");
    std::env::set_var(&legacy_url, "nats://127.0.0.1:4223");
    std::env::set_var(&legacy_user, "legacy-user");
    std::env::set_var(&legacy_password, "legacy-secret");
    std::env::set_var(ENV_URL, "nats://127.0.0.1:4224");
    std::env::set_var(ENV_USER, "canonical-user");

    let preferred = NatsConfig::from_env().expect("合法环境变量");
    assert_eq!(preferred.url, "nats://127.0.0.1:4224", "规范前缀必须优先");
    assert_eq!(preferred.user.as_deref(), Some("canonical-user"));
    assert_eq!(
        preferred.password(),
        Some("legacy-secret"),
        "兼容前缀仍生效"
    );
    assert!(
        !format!("{preferred:?}").contains("legacy-secret"),
        "密码不得出现在 Debug 输出"
    );

    std::env::remove_var(ENV_URL);
    std::env::remove_var(ENV_USER);
    let legacy_only = NatsConfig::from_env().expect("仅兼容前缀仍应可加载");
    assert_eq!(legacy_only.url, "nats://127.0.0.1:4223");

    clear_env();
}

/// 入口 `NatsConfig::from_toml`：解析扁平字段，拒绝敏感字段、未知字段与错误 schema 版本。
#[test]
fn from_toml_parses_and_rejects_secrets() {
    let config = NatsConfig::from_toml(
        "schema_version = 1\nurl = \"nats://127.0.0.1:4223\"\nname = \"tdd\"\nconnect_timeout_ms = 8000\n",
    )
    .expect("合法 TOML");
    assert_eq!(config.url, "nats://127.0.0.1:4223");
    assert_eq!(config.name, "tdd");
    assert_eq!(config.connect_timeout, Duration::from_millis(8000));

    for key in ["password", "token", "nkey_seed", "jwt"] {
        let text = format!("schema_version = 1\n{key} = \"tdd-secret-value\"\n");
        let error = NatsConfig::from_toml(&text).expect_err("敏感字段必须被拒绝");
        assert!(matches!(error, NatsError::Config(_)), "{key}: {error}");
        assert!(
            !error.to_string().contains("tdd-secret-value"),
            "{key} 错误回显了凭据"
        );
    }

    assert!(
        NatsConfig::from_toml("schema_version = 1\nunknown_key = 1\n").is_err(),
        "未知字段必须被拒绝"
    );
    assert!(
        NatsConfig::from_toml("schema_version = 2\n").is_err(),
        "schema_version 必须等于 SCHEMA_VERSION"
    );
}

/// 入口 `NatsConfig::validate`：fail-closed 矩阵（凭据配对、互斥、超时、容量、重连）。
#[test]
fn validate_fail_closed_matrix() {
    NatsConfig::default().validate().expect("默认配置合法");

    let mut empty_url = NatsConfig::default();
    empty_url.url = "  ".into();
    assert!(empty_url.validate().is_err(), "空 url 必须被拒绝");

    let mut userinfo = NatsConfig::default();
    userinfo.url = "nats://user:pass@127.0.0.1:4222".into();
    assert!(userinfo.validate().is_err(), "URL 禁内嵌 userinfo");

    // 只给 user 不给 password：必须成对。
    let mut half_credentials = NatsConfig::default();
    half_credentials.user = Some("only-user".into());
    assert!(
        half_credentials.validate().is_err(),
        "user/password 必须成对"
    );

    // token 与 user/password 互斥（只能经 builder 注入 token）。
    let error = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .token("tdd-token")
        .credentials("tdd-user", "tdd-pass")
        .build()
        .expect_err("token 与 user/password 互斥");
    assert!(matches!(error, NatsError::Config(_)), "{error}");

    // 空 token 字符串同样必须被拒绝。
    assert!(
        NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .token("   ")
            .build()
            .is_err(),
        "空白 token 必须被拒绝"
    );

    let mut zero_timeout = NatsConfig::default();
    zero_timeout.operation_timeout = Duration::ZERO;
    assert!(zero_timeout.validate().is_err(), "零超时必须被拒绝");

    let mut zero_capacity = NatsConfig::default();
    zero_capacity.subscription_capacity = 0;
    assert!(zero_capacity.validate().is_err(), "零容量必须被拒绝");

    let mut zero_reconnects = NatsConfig::default();
    zero_reconnects.max_reconnects = 0;
    assert!(
        zero_reconnects.validate().is_err(),
        "max_reconnects 必须为有限正数"
    );
}

/// 入口 `NatsPool::connect`：拒绝连接在内部截止时间内失败，且归类可重试。
#[tokio::test]
async fn connect_refused_is_retryable_and_bounded() {
    let config = NatsConfig::builder()
        .url("nats://127.0.0.1:1")
        .connect_timeout(Duration::from_millis(300))
        .operation_timeout(Duration::from_millis(300))
        .build()
        .expect("回环配置合法");

    let error = tokio::time::timeout(Duration::from_secs(10), NatsPool::connect(config))
        .await
        .expect("connect 必须受内部截止时间约束")
        .expect_err("端口 1 必然拒绝连接");
    assert!(error.is_retryable(), "连接失败应可重试: {error}");
}

/// 入口 `NatsPool::publish`：先校验 subject，再要求已连接。
#[tokio::test]
async fn publish_validates_subject_and_connection() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置合法");

    let error = pool
        .publish("orders.*", "payload")
        .await
        .expect_err("发布 subject 不允许通配符");
    assert!(matches!(error, NatsError::Config(_)), "{error}");

    let error = pool
        .publish("orders.created", "payload")
        .await
        .expect_err("未连接的池不得发布成功");
    assert!(matches!(error, NatsError::Connection(_)), "{error}");
}

/// 入口 `NatsPool::subscribe`：订阅允许通配符，但仍要求已连接。
#[tokio::test]
async fn subscribe_validates_subject_and_connection() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置合法");

    let error = pool
        .subscribe("has space")
        .await
        .expect_err("含空白的 subject 必须被拒绝");
    assert!(matches!(error, NatsError::Config(_)), "{error}");

    let error = pool
        .subscribe("orders.*")
        .await
        .expect_err("订阅通配符合法但未连接必须失败");
    assert!(matches!(error, NatsError::Connection(_)), "{error}");
}

/// 入口 `NatsPool::request`：零 deadline 与未连接都被拒绝。
#[tokio::test]
async fn request_validates_deadline_and_connection() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置合法");

    let error = pool
        .request("orders.get", "x", Duration::ZERO)
        .await
        .expect_err("零 deadline 必须被拒绝");
    assert!(matches!(error, NatsError::Config(_)), "{error}");

    let error = pool
        .request("orders.get", "x", Duration::from_secs(1))
        .await
        .expect_err("未连接的池不得请求成功");
    assert!(matches!(error, NatsError::Connection(_)), "{error}");
}

/// 入口 `NatsPool::ping`：未连接必须报错，不得伪造零耗时成功。
#[tokio::test]
async fn ping_requires_connection() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置合法");
    let error = pool.ping().await.expect_err("未连接的池 ping 必须失败");
    assert!(matches!(error, NatsError::Connection(_)), "{error}");
}

/// 入口 `NatsPool::health_check`：未连接时是**结构化**结果（`Ok` + connected=false），不是错误。
#[tokio::test]
async fn health_check_is_structured_when_disconnected() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置合法");
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(!health.connected, "未连接不得标记 connected");
    assert_eq!(health.rtt_ms, 0.0, "未连接时不得伪报 RTT");
    assert!(
        health.server.is_empty(),
        "服务端信息不可用时必须为空串，不得编造地址: {:?}",
        health.server
    );
    assert!(!health.jetstream, "服务端信息未知时不得声称支持 JetStream");
    assert!(
        health.detail.contains("尚未连接"),
        "detail 应说明未连接: {}",
        health.detail
    );
}

/// 入口 `NatsError::is_retryable`：仅 Connection / Timeout / Io 可重试，其余一律不可。
#[test]
fn is_retryable_classification_is_conservative() {
    for error in [
        NatsError::connection("x"),
        NatsError::timeout("x"),
        NatsError::Io(std::io::Error::other("x")),
    ] {
        assert!(error.is_retryable(), "{} 应可重试", error.kind_name());
    }
    for error in [
        NatsError::config("x"),
        NatsError::backend("x"),
        NatsError::serialization("x"),
        NatsError::unsupported("x"),
    ] {
        assert!(!error.is_retryable(), "{} 不应重试", error.kind_name());
    }
}
