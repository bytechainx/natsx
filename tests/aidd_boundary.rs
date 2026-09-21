#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: TOML 承载 password/token/nkey_seed/jwt 四种敏感字段 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 敏感字段禁入 TOML 且错误不得回显 | 结论=保留
//! // AIDD: URL 内嵌 userinfo | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 URL 内嵌 userinfo 同脱敏 / §3 fail-closed | 结论=保留
//! // AIDD: token 与 user/password 同时提供、token 为空串 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 认证材料互斥与 fail-fast | 结论=保留
//! // AIDD: 非 loopback 配 Disable/Prefer | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 拒绝「非 loopback + 非 Require」 | 结论=保留
//! // AIDD: 零容量 / 零重连 / 零超时 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 有界队列与必备超时 | 结论=保留
//! // AIDD: subject 空白/制表/换行与超长 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 subject 校验先于协议层 | 结论=保留
//! // AIDD: stream/consumer 名含点号或通配符 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 名称校验与服务端约束对齐 | 结论=保留
//! // AIDD: mTLS 只给 cert 或只给 key、CA 路径不存在 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 TLS 材料必须成对且可访问 | 结论=保留
//! // AIDD: 未连接池的全部数据面入口 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 未连接/已关闭 fail-closed 不 panic | 结论=保留
//! // AIDD: TOML 错误回显承载凭据的源码行 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 错误消息不得回显敏感值 | 结论=保留

use std::time::Duration;

use natsx::{
    validate_consumer_name, validate_publish_subject, validate_stream_name, validate_subject,
    NatsConfig, NatsError, NatsPool, TlsPolicy,
};

/// 边界：TOML 的四种敏感字段都必须被拒绝，且错误消息不得回显值。
#[test]
fn toml_sensitive_fields_all_rejected_without_echo() {
    for key in ["password", "token", "nkey_seed", "jwt"] {
        let text = format!("schema_version = 1\n{key} = \"aidd-secret-value\"\n");
        let error = NatsConfig::from_toml(&text).expect_err("敏感字段必须被拒绝");
        assert!(matches!(error, NatsError::Config(_)), "{key}: {error}");
        assert!(
            !error.to_string().contains("aidd-secret-value"),
            "{key} 错误回显了凭据: {error}"
        );
    }
}

/// 边界：URL 内嵌 userinfo 必须被拒绝，且 `Debug` 与错误消息都不泄露该片段。
#[test]
fn url_userinfo_rejected_and_redacted() {
    let raw = "nats://aidd-embedded-user:aidd-embedded-password@127.0.0.1:4222";
    let mut config = NatsConfig::default();
    config.url = raw.into();
    assert!(config.validate().is_err(), "URL 内嵌 userinfo 必须被拒绝");

    let text = format!("{config:?}");
    assert!(
        !text.contains("aidd-embedded-user"),
        "Debug 泄露用户名: {text}"
    );
    assert!(
        !text.contains("aidd-embedded-password"),
        "Debug 泄露密码: {text}"
    );
    assert!(text.contains("***"), "应出现脱敏占位: {text}");

    let message = config.validate().expect_err("重复校验仍应失败").to_string();
    assert!(
        !message.contains("aidd-embedded-password"),
        "错误消息泄露密码: {message}"
    );
}

/// 边界：token 与 user/password 互斥；空白 token 视为非法而非「未设置」。
#[test]
fn token_mutual_exclusion_and_empty_token() {
    let error = NatsConfig::builder()
        .url("nats://127.0.0.1:4222")
        .token("aidd-token")
        .credentials("aidd-user", "aidd-pass")
        .build()
        .expect_err("token 与 user/password 互斥");
    assert!(matches!(error, NatsError::Config(_)), "{error}");

    assert!(
        NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .token("  ")
            .build()
            .is_err(),
        "空白 token 必须被拒绝"
    );

    assert!(
        NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .nkey_seed("seed-value")
            .credentials("aidd-user", "aidd-pass")
            .build()
            .is_err(),
        "NKey seed 与 user/password 互斥"
    );
}

/// 边界：非 loopback 只接受 `Require`；显式策略优先于 `tls` 布尔开关。
#[test]
fn non_loopback_requires_tls_policy() {
    let mut disable = NatsConfig::default();
    disable.url = "nats://aidd.example.com:4222".into();
    disable.tls_policy = Some(TlsPolicy::Disable);
    assert!(
        disable.validate().is_err(),
        "非 loopback + Disable 必须失败"
    );

    let mut prefer = NatsConfig::default();
    prefer.url = "nats://aidd.example.com:4222".into();
    prefer.tls_policy = Some(TlsPolicy::Prefer);
    assert!(prefer.validate().is_err(), "非 loopback + Prefer 必须失败");

    let mut explicit = NatsConfig::default();
    explicit.url = "nats://aidd.example.com:4222".into();
    explicit.tls = false;
    explicit.tls_policy = Some(TlsPolicy::Require);
    assert_eq!(explicit.effective_tls_policy(), TlsPolicy::Require);
    explicit.validate().expect("非 loopback + Require 合法");

    // 显式策略优先于 tls 布尔：tls=true 也不能绕过显式 Disable 的判定。
    let mut boolean_bypass = NatsConfig::default();
    boolean_bypass.url = "nats://aidd.example.com:4222".into();
    boolean_bypass.tls = true;
    boolean_bypass.tls_policy = Some(TlsPolicy::Disable);
    assert_eq!(
        boolean_bypass.effective_tls_policy(),
        TlsPolicy::Disable,
        "显式策略必须优先"
    );
    assert!(boolean_bypass.validate().is_err());
}

/// 边界：零容量 / 零重连 / 零超时必须 fail-fast。
#[test]
fn zero_capacities_and_timeouts_rejected() {
    let mut zero_subscription = NatsConfig::default();
    zero_subscription.subscription_capacity = 0;
    assert!(zero_subscription.validate().is_err());

    let mut zero_client = NatsConfig::default();
    zero_client.client_capacity = 0;
    assert!(zero_client.validate().is_err());

    let mut zero_reconnects = NatsConfig::default();
    zero_reconnects.max_reconnects = 0;
    assert!(zero_reconnects.validate().is_err());

    let mut zero_connect_timeout = NatsConfig::default();
    zero_connect_timeout.connect_timeout = Duration::ZERO;
    assert!(zero_connect_timeout.validate().is_err());

    let mut zero_backoff = NatsConfig::default();
    zero_backoff.reconnect_max_delay = Duration::ZERO;
    assert!(zero_backoff.validate().is_err());
}

/// 边界：subject 的空白/控制字符与超长输入；发布禁通配符、订阅允许。
#[test]
fn subject_boundaries() {
    for invalid in ["", "   ", "has space", "tab\there", "nl\nhere", "cr\rhere"] {
        assert!(validate_subject(invalid).is_err(), "{invalid:?} 必须被拒绝");
    }
    assert!(validate_subject("orders.*").is_ok(), "订阅允许单层通配符");
    assert!(validate_subject("orders.>").is_ok(), "订阅允许多层通配符");
    assert!(validate_publish_subject("orders.*").is_err());
    assert!(validate_publish_subject("orders.>").is_err());
    assert!(validate_publish_subject("").is_err());

    // 校验层不设长度上限（长度由服务端 max_payload 兜底），故超长 subject 仍被判合法。
    let long_subject = format!("orders.{}", "a".repeat(10_000));
    assert!(
        validate_subject(&long_subject).is_ok(),
        "校验层不应自行发明长度上限"
    );
}

/// 边界：stream / consumer 名禁空白、点号与通配符（与 async-nats 服务端约束对齐）。
#[test]
fn stream_and_consumer_name_boundaries() {
    for invalid in [
        "",
        "bad.name",
        "bad*name",
        "bad>name",
        "has space",
        "\tbad",
        "bad\n",
    ] {
        assert!(
            validate_stream_name(invalid).is_err(),
            "stream 名 {invalid:?} 必须被拒绝"
        );
        let error = validate_consumer_name(invalid).expect_err("consumer 名同样非法");
        assert!(
            error.to_string().contains("consumer 名非法"),
            "错误消息应指明对象: {error}"
        );
    }
    assert!(validate_stream_name("EVENTS").is_ok());
    assert!(validate_consumer_name("worker_1").is_ok());
}

/// 边界：mTLS 材料必须成对且路径可访问；CA 不存在必须被拒绝。
#[test]
fn tls_material_must_be_complete() {
    let mut cert_only = NatsConfig::default();
    cert_only.tls_cert_file = Some("/tmp/natsx-aidd-cert.pem".into());
    assert!(cert_only.validate().is_err(), "只有 cert 必须被拒绝");

    let mut key_only = NatsConfig::default();
    key_only.tls_key_file = Some("/tmp/natsx-aidd-key.pem".into());
    assert!(key_only.validate().is_err(), "只有 key 必须被拒绝");

    let mut pair_missing = NatsConfig::default();
    pair_missing.tls_cert_file = Some("/tmp/natsx-aidd-cert.pem".into());
    pair_missing.tls_key_file = Some("/tmp/natsx-aidd-key.pem".into());
    assert!(pair_missing.validate().is_err(), "路径不可访问时必须被拒绝");

    assert!(
        NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .tls_ca_file("/nonexistent/natsx-aidd-ca.pem")
            .build()
            .is_err(),
        "CA 路径不存在必须被拒绝"
    );
}

/// 边界：TOML 的语法/语义错误**不得**把承载凭据的源码行回显进公开错误消息。
///
/// 回归保护：`toml` 的错误 `Display` 会连原始源码行一起渲染。此前 `from_toml` 直接
/// 插值 `{error}`，于是「凭据行本身写得不对」时（引号未闭合、或该行触发未知字段错误），
/// 凭据片段会被原样写进 `NatsError::Serialization` —— 而错误消息通常会进日志与打点，
/// 等于把凭据泄漏到可观测面。标准.md §2 要求敏感字段既不能经 TOML 进入、也不能被回显。
#[test]
fn toml_error_never_echoes_credential_value() {
    let cases = [
        // 语法错误：未闭合引号，出错行恰是凭据行（原实现回显整行）。
        "schema_version = 1\npassword = \"aidd-secret-probe\n",
        // 语义错误：未知字段所在行携带看起来像凭据的值（原实现回显该行）。
        "schema_version = 1\nendpoint = \"svc:aidd-secret-probe@host\"\n",
    ];
    for text in cases {
        let error = NatsConfig::from_toml(text).expect_err("非法 TOML 必须失败");
        let message = error.to_string();
        assert!(
            !message.contains("aidd-secret-probe"),
            "错误消息回显了凭据片段: {message}"
        );
        assert!(
            !message.contains("2 |") && !message.contains("^"),
            "错误消息仍带源码片段: {message}"
        );
    }

    // 定位信息必须保留（否则等于牺牲可诊断性换安全）。
    let message = NatsConfig::from_toml(cases[0])
        .expect_err("非法 TOML 必须失败")
        .to_string();
    assert!(message.contains("第 2 行"), "应保留行号定位信息: {message}");
}

/// 边界：未连接池的全部数据面入口都必须 fail-closed，且不得 panic。
#[tokio::test]
async fn disconnected_pool_fails_closed_without_panic() {
    let pool = NatsPool::new(NatsConfig::default()).expect("构造");
    assert!(!pool.is_connected());

    assert!(matches!(
        pool.publish("orders.created", "x").await,
        Err(NatsError::Connection(_))
    ));
    assert!(pool.subscribe("orders.created").await.is_err());
    assert!(pool.ping().await.is_err());
    assert!(pool.flush().await.is_err());
    assert!(pool
        .request("orders.get", "x", Duration::from_secs(1))
        .await
        .is_err());
    assert!(pool
        .publish_json("orders.created", &serde_json::json!({"k": 1}))
        .await
        .is_err());

    // 健康面不 panic、给出结构化 false。
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(!health.connected);
    assert!(health.server.is_empty(), "不得编造服务端地址");
}
