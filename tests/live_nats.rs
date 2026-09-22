#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服（natsx）：覆盖全部公开接口，需先 `source /home/zone/workspace/sre/secrets/env/natsx.env`。
//!
//! 全部用例 `#[ignore]`，默认不跑（CI 行为不变）。显式运行：
//!
//! ```bash
//! set -a; source /home/zone/workspace/sre/secrets/env/natsx.env; set +a
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_nats -- --ignored --test-threads=1
//! ```
//!
//! 凭据只从环境变量读取，绝不硬编码；subject / stream / consumer 名唯一化
//! （pid + 原子计数 + 纳秒时间戳），Core NATS 无历史回放，故每条用例先订阅、
//! flush 确保 SUB 到达服务端，再发布。JetStream 用例自建唯一名字的 stream，
//! 收尾（含失败路径）尽力删除，绝不动服务端他人资源。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use natsx::{
    url_is_loopback, validate_consumer_name, validate_operation_timeout, validate_publish_subject,
    validate_stream_name, validate_subject, JetStream, JetStreamConsumerConfig,
    JetStreamDeliveryMetadata, NatsConfig, NatsConfigBuilder, NatsError, NatsHealth, NatsPool,
    NatsPoolStats, NatsResult, PullConsumerConfig, StreamConfig, TlsPolicy, DEFAULT_CLIENT_NAME,
    DEFAULT_URL, ENV_LEGACY_PREFIX, ENV_PASSWORD, ENV_PREFIX, ENV_URL, ENV_USER, SCHEMA_VERSION,
};

/// 进程内唯一的 subject：`natsx.live.<用途>.<pid>.<纳秒>`。
fn unique_subject(use_case: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("natsx.live.{use_case}.{}.{}", std::process::id(), nanos)
}

/// 进程内唯一的 stream / consumer 名（禁止 `.` `*` `>`，用连字符拼接）。
fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间应晚于 UNIX_EPOCH")
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "natsx-live-{prefix}-{}-{counter}-{nanos}",
        std::process::id()
    )
}

/// 读取必填环境变量（凭据只从环境注入，绝不硬编码）。
fn env_or_fail(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} 必须已注入（source natsx.env）"))
}

/// 建连 → 结构化 health → ping RTT → 唯一 subject 发布/订阅往返 → close 收尾。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_pubsub_health_and_close() {
    let config = NatsConfig::from_env().expect("FOUNDATIONX_NATSX_* 必须已注入");
    let pool = NatsPool::connect(config).await.expect("建连必须成功");
    assert!(pool.is_connected(), "建连后应处于已连接态");

    // 结构化探活：connected 为真，RTT 有界且 detail 可读。
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(health.connected, "应已连接: {}", health.detail);
    assert!(!health.detail.is_empty(), "detail 应给出可读说明");
    assert!(
        health.rtt_ms > 0.0 && health.rtt_ms < 5_000.0,
        "RTT 应在合理区间内: {}ms",
        health.rtt_ms
    );
    assert!(
        !health.server.is_empty(),
        "已连接时服务端标识不应为空串: {health:?}"
    );
    assert!(health.jetstream, "目标服务端声明支持 JetStream: {health:?}");

    // `NatsPool::ping` 返回 flush 往返耗时（Duration）。
    let rtt = pool.ping().await.expect("ping 必须成功");
    assert!(
        rtt < Duration::from_secs(5),
        "RTT 应远小于操作超时: {rtt:?}"
    );

    // 唯一 subject 的 Core NATS 往返。
    let subject = unique_subject("pubsub");
    let mut subscription = pool.subscribe(&subject).await.expect("订阅必须成功");
    pool.flush().await.expect("flush 以确保 SUB 已到达服务端");
    let payload = b"natsx-live-roundtrip".to_vec();
    pool.publish(&subject, payload.clone())
        .await
        .expect("发布必须成功（返回前已 flush）");
    let message = tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await
        .expect("接收不得超时")
        .expect("应收到自己发布的那条消息");
    assert_eq!(message.subject, subject, "subject 必须一致");
    assert_eq!(
        message.payload.as_ref(),
        payload.as_slice(),
        "往返载荷必须逐字节一致"
    );
    drop(subscription);

    let stats = pool.stats();
    assert!(stats.published >= 1, "成功发布应计入统计");
    assert!(stats.connected >= 1, "初始连接事件应计入统计");
    assert!(!stats.closed, "close 之前不应标记 closed");

    // close() 收尾（E5）：关闭后数据面必须 fail-closed。
    pool.close().await.expect("close 必须成功");
    assert!(pool.stats().closed, "close 后必须标记 closed");
    assert!(
        pool.publish("natsx.live.after_close", "x").await.is_err(),
        "关闭后不得再发布成功"
    );
}

/// 建连 → 唯一 subject 的 request-reply 往返 → close 收尾。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_request_reply() {
    let pool = NatsPool::connect_from_env().await.expect("建连必须成功");
    let subject = unique_subject("request");

    // 应答方：订阅并回写到 reply subject（request 场景下 reply 必须非空）。
    let mut responder = pool.subscribe(&subject).await.expect("订阅必须成功");
    pool.flush().await.expect("flush 以确保 SUB 已到达服务端");
    let responder_pool = pool.clone();
    let task = tokio::spawn(async move {
        if let Some(message) = responder.next().await {
            assert!(message.reply.is_some(), "request 场景下 reply 必须非空");
            if let Some(reply) = message.reply {
                responder_pool
                    .publish(&reply, message.payload.clone())
                    .await
                    .expect("应答发布必须成功");
            }
        }
    });

    let response = pool
        .request(&subject, b"ping".to_vec(), Duration::from_secs(5))
        .await
        .expect("request 必须收到应答");
    assert_eq!(response.payload.as_ref(), b"ping", "应答载荷必须与请求一致");
    task.await.expect("应答任务不得 panic");

    pool.close().await.expect("close 必须成功");
    assert!(pool.stats().closed, "close 后必须标记 closed");
}

/// from_env / Builder 全字段构造 / 访问器 / Debug 脱敏（真凭据不落日志）→ 真连。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_config_builder_surface_and_debug_redaction() {
    let url = env_or_fail(ENV_URL);
    let user = env_or_fail(ENV_USER);
    let password = env_or_fail(ENV_PASSWORD);

    // from_env 的 Debug 必须脱敏真实密码。
    let config = NatsConfig::from_env().expect("from_env 必须成功");
    let debug = format!("{config:?}");
    assert!(
        !debug.contains(&password),
        "NatsConfig Debug 不得泄露密码明文"
    );
    assert!(debug.contains("***"), "敏感字段应渲染为 ***");
    assert_eq!(config.password(), Some(password.as_str()));
    assert!(
        config.user_password().is_some(),
        "env 注入的账号密码应成对可用"
    );
    assert!(config.token().is_none());
    assert!(config.nkey_seed().is_none());
    assert!(url_is_loopback(&url), "live 目标应为 loopback");
    assert_eq!(
        config.effective_tls_policy(),
        TlsPolicy::Prefer,
        "loopback 默认 Prefer"
    );
    assert!(!config.url_implies_tls(), "nats:// 不隐含 TLS");
    assert_eq!(
        config.effective_slow_consumer_timeout(),
        config.operation_timeout,
        "未显式配置时回退 operation_timeout"
    );

    // Builder 全字段构造 → 真连。
    let built = NatsConfigBuilder::new()
        .url(url.as_str())
        .credentials(user.as_str(), password.as_str())
        .name("natsx-live-builder")
        .connect_timeout(Duration::from_secs(5))
        .operation_timeout(Duration::from_secs(5))
        .jetstream(true)
        .subscription_capacity(64)
        .client_capacity(64)
        .max_reconnects(3)
        .reconnect_max_delay(Duration::from_secs(2))
        .ignore_discovered_servers(false)
        .build()
        .expect("Builder 构建必须成功");
    let pool = NatsPool::connect(built).await.expect("建连必须成功");
    assert!(pool.is_connected());
    assert!(pool.client().is_some(), "连接后 client 逃生口必须可用");
    assert_eq!(pool.config().name, "natsx-live-builder");
    assert_eq!(pool.config().url, url);
    assert!(pool.stats().connected >= 1, "初始连接事件必须计数");
    pool.ping().await.expect("ping 必须成功");

    // NatsPool Debug：脱敏配置 + 连接状态 + 统计快照。
    let pool_debug = format!("{pool:?}");
    assert!(pool_debug.contains("NatsPool"));
    assert!(pool_debug.contains("natsx-live-builder"));
    assert!(
        !pool_debug.contains(&password),
        "NatsPool Debug 不得泄露密码明文"
    );

    // from_config：以已有配置为基线重建。
    let rebased = NatsConfigBuilder::from_config(pool.config().clone())
        .build()
        .expect("from_config 重建必须成功");
    assert_eq!(rebased.url, url);

    pool.close().await.expect("close 必须成功");
    assert!(pool.stats().closed);
}

/// publish_with_headers（headers 往返）/ publish_json（载荷即 JSON 序列化结果）/
/// NatsSubscription 作为 Stream 消费 / drain 优雅关停。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_headers_json_stream_and_drain() {
    let pool = NatsPool::connect_from_env().await.expect("建连必须成功");
    let subject = unique_subject("headers");

    let mut subscription = pool.subscribe(&subject).await.expect("订阅必须成功");
    pool.flush().await.expect("flush 确保 SUB 到达服务端");

    // publish_with_headers：headers 必须随消息往返。
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("X-Natsx-Live", "headers-roundtrip");
    pool.publish_with_headers(&subject, headers, b"with-headers".to_vec())
        .await
        .expect("带 headers 发布必须成功");

    // publish_json：载荷必须是给定值的 JSON 序列化结果。
    let value = serde_json::json!({ "kind": "live", "seq": 1 });
    pool.publish_json(&subject, &value)
        .await
        .expect("JSON 发布必须成功");

    // NatsSubscription 作为 Stream 消费（显式走 futures_core::Stream 实现）。
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::StreamExt::next(&mut subscription),
    )
    .await
    .expect("第一条消息不得超时")
    .expect("应收到带 headers 的消息");
    assert_eq!(first.subject, subject);
    assert_eq!(first.payload.as_ref(), b"with-headers");
    let header_value = first
        .headers
        .as_ref()
        .and_then(|map| map.get("X-Natsx-Live"))
        .map(|value| value.as_str().to_string());
    assert_eq!(
        header_value.as_deref(),
        Some("headers-roundtrip"),
        "headers 必须随消息往返"
    );

    // 内置 next() 消费第二条：载荷为 JSON 序列化结果，seq 单调递增。
    let second = tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await
        .expect("第二条消息不得超时")
        .expect("应收到 JSON 消息");
    let expected_json = serde_json::to_vec(&value).expect("期望载荷必须可计算");
    assert_eq!(second.payload.as_ref(), expected_json.as_slice());
    assert!(second.seq > first.seq, "会话内 seq 应单调递增");

    drop(subscription);

    // drain（E5 优雅关停）：关停后数据面 fail-closed。
    pool.drain(Duration::from_secs(5))
        .await
        .expect("drain 必须成功");
    assert!(pool.stats().closed, "drain 后必须标记 closed");
    assert!(
        pool.publish(&subject, "x").await.is_err(),
        "drain 后不得再发布成功"
    );
}

/// 无人订阅的 request：无响应者错误必须映射为 Backend（不可重试）。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_request_no_responders_maps_backend() {
    let pool = NatsPool::connect_from_env().await.expect("建连必须成功");
    let subject = unique_subject("nobody");
    let error = pool
        .request(&subject, b"ping".to_vec(), Duration::from_secs(5))
        .await
        .expect_err("无人订阅的 request 必须失败");
    assert!(
        matches!(error, NatsError::Backend(_)),
        "无响应者应映射为 Backend: {error:?}"
    );
    assert!(!error.is_retryable(), "无响应者不属于可重试瞬时错误");
    pool.close().await.expect("close 必须成功");
}

/// 慢消费者计数：单条缓冲 + 500ms 判定超时，下游不接收时必须计入 slow_consumers。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_slow_consumer_accounting() {
    let url = env_or_fail(ENV_URL);
    let user = env_or_fail(ENV_USER);
    let password = env_or_fail(ENV_PASSWORD);
    let config = NatsConfigBuilder::new()
        .url(url.as_str())
        .credentials(user.as_str(), password.as_str())
        .subscription_capacity(1)
        .slow_consumer_timeout(Duration::from_millis(500))
        .build()
        .expect("Builder 构建必须成功");
    assert_eq!(
        config.effective_slow_consumer_timeout(),
        Duration::from_millis(500),
        "显式配置的慢消费者超时必须生效"
    );
    let pool = NatsPool::connect(config).await.expect("建连必须成功");
    let subject = unique_subject("slow");
    let subscription = pool.subscribe(&subject).await.expect("订阅必须成功");
    pool.flush().await.expect("flush 确保 SUB 到达服务端");

    // 灌满缓冲（容量 1）且不消费：转发任务等待下游超时即判慢。
    for index in 0..3 {
        pool.publish(&subject, format!("slow-{index}"))
            .await
            .expect("发布必须成功");
    }
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let stats = pool.stats();
    assert!(
        stats.slow_consumers >= 1,
        "不消费的下游必须被判慢并计数: {stats:?}"
    );
    drop(subscription);
    pool.close().await.expect("close 必须成功");
}

/// 对明文端口强制 Require TLS：连接必须失败（fail-closed）。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_tls_require_on_plaintext_fails_closed() {
    let url = env_or_fail(ENV_URL);
    let user = env_or_fail(ENV_USER);
    let password = env_or_fail(ENV_PASSWORD);
    let config = NatsConfigBuilder::new()
        .url(url.as_str())
        .credentials(user.as_str(), password.as_str())
        .tls_policy(TlsPolicy::Require)
        .build()
        .expect("Builder 构建必须成功");
    let error = NatsPool::connect(config)
        .await
        .expect_err("对明文端口强制 TLS 必须失败");
    // 实测分类：rustls 把「明文响应当 TLS 记录」报为 IO 错误（corrupt message），
    // 经 map_connect_error 归入 Connection（按文档口径可重试）。E2E 关注的
    // fail-closed 性质是「连接必须失败」，分类不在此断言。
    assert!(
        matches!(error, NatsError::Connection(_) | NatsError::Config(_)),
        "握手失败应落入连接/配置类: {error:?}"
    );
}

/// 错误凭据（无效 token）：必须被服务端拒绝且归类为不可重试（Config 类）。
#[tokio::test]
#[ignore = "需要真实 NATS 服务与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_invalid_token_fails_closed() {
    let url = env_or_fail(ENV_URL);
    let config = NatsConfigBuilder::new()
        .url(url.as_str())
        .token("natsx-live-invalid-token")
        .build()
        .expect("Builder 构建必须成功");
    let error = NatsPool::connect(config)
        .await
        .expect_err("错误凭据必须被服务端拒绝");
    assert!(
        !error.is_retryable(),
        "认证失败属于 Config 类（不可重试）: {error:?}"
    );
}

/// JetStream stream 生命周期：from_pool / with_operation_timeout / create（重复失败）/
/// get_or_create（幂等）/ get_stream / publish / publish_json / purge / from_client / delete。
#[tokio::test]
#[ignore = "需要真实 NATS 服务、JetStream 与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_jetstream_stream_lifecycle() {
    let pool = NatsPool::connect_from_env().await.expect("建连必须成功");
    let js = JetStream::from_pool(&pool).expect("已连接池必须能构造 JetStream");
    let stream = unique_name("stream");
    let subject = unique_subject("jsstream");

    let outcome = jetstream_stream_scenario(&pool, &js, &stream, &subject).await;
    // 无论场景成败都尽力清理自建 stream，避免污染共享服务端。
    let _ = js.delete_stream(&stream).await;
    outcome.expect("JetStream stream 生命周期场景必须成功");
    pool.close().await.expect("close 必须成功");
}

async fn jetstream_stream_scenario(
    pool: &NatsPool,
    js: &JetStream,
    stream: &str,
    subject: &str,
) -> NatsResult<()> {
    // with_operation_timeout 覆盖截止时间；零值 fail-closed。
    let js = js
        .clone()
        .with_operation_timeout(Duration::from_secs(3))
        .expect("覆盖操作超时必须成功");
    assert_eq!(js.operation_timeout(), Duration::from_secs(3));
    let untouched = JetStream::from_pool(pool).expect("重新构造 JetStream");
    assert!(untouched.with_operation_timeout(Duration::ZERO).is_err());
    let _ = js.context(); // 底层上下文可获取（高级逃生口）

    // create_stream：唯一名创建成功。
    js.create_stream(StreamConfig::new(stream, subject)).await?;

    // get_or_create_stream 幂等。
    js.get_or_create_stream(StreamConfig::new(stream, subject))
        .await?;

    // get_stream：结构与计数如实反映服务端状态。
    let info = js.get_stream(stream).await?;
    assert_eq!(info.name, stream);
    assert_eq!(info.subjects, vec![subject.to_string()]);
    assert_eq!(info.messages, 0);
    assert_eq!(info.consumers, 0);

    // publish（等 ack）与 publish_json。
    js.publish(subject, b"js-live-1".as_slice()).await?;
    js.publish(subject, b"js-live-2".as_slice()).await?;
    let value = serde_json::json!({ "kind": "js-live", "seq": 3 });
    js.publish_json(subject, &value).await?;
    let info = js.get_stream(stream).await?;
    assert_eq!(info.messages, 3, "三条持久化消息必须可见");
    assert!(info.bytes > 0);

    // 重复 create_stream：底层走 JetStream 的 STREAM.CREATE（upsert 语义），
    // 相同配置重复创建被服务端接受且**不清空**已有消息。
    // 注意：`JetStream::create_stream` 的文档注释写「已存在则失败」，与实测不符（已登记）。
    js.create_stream(StreamConfig::new(stream, subject)).await?;
    let info = js.get_stream(stream).await?;
    assert_eq!(info.messages, 3, "重复创建（upsert）不得清空已有消息");

    // purge：清空消息但保留 stream。
    js.purge_stream(stream).await?;
    let info = js.get_stream(stream).await?;
    assert_eq!(info.messages, 0, "purge 后消息数必须归零");

    // from_client 逃生口：默认操作超时 5s，可独立发布。
    let client = pool.client().expect("连接后 client 必须可用");
    let js_escape = JetStream::from_client(client);
    assert_eq!(js_escape.operation_timeout(), Duration::from_secs(5));
    js_escape
        .publish(subject, b"from-client".as_slice())
        .await?;
    assert_eq!(js.get_stream(stream).await?.messages, 1);

    // delete_stream：删除后查询必须失败。
    js.delete_stream(stream).await?;
    assert!(
        matches!(js.get_stream(stream).await, Err(NatsError::Backend(_))),
        "删除后的 stream 查询必须报 Backend 错误"
    );
    Ok(())
}

/// JetStream 消费确认语义：create_pull_consumer / get_pull_consumer / consumer /
/// info / pending / next_timeout（空拉与零值）/ next_batch / double_ack /
/// progress + nak（重投且 delivery_attempts 递增）/ ack / term（不再重投）。
#[tokio::test]
#[ignore = "需要真实 NATS 服务、JetStream 与 FOUNDATIONX_NATSX_* 环境变量"]
async fn live_nats_jetstream_consumer_delivery_semantics() {
    let pool = NatsPool::connect_from_env().await.expect("建连必须成功");
    let js = JetStream::from_pool(&pool).expect("构造 JetStream");
    let stream = unique_name("cons");
    let subject = unique_subject("jscons");
    js.get_or_create_stream(StreamConfig::new(&stream, &subject))
        .await
        .expect("建 stream 必须成功");

    let outcome = jetstream_consumer_scenario(&js, &stream, &subject).await;
    // 无论场景成败都尽力清理自建 stream。
    let _ = js.delete_stream(&stream).await;
    outcome.expect("JetStream 消费确认语义场景必须成功");
    pool.close().await.expect("close 必须成功");
}

async fn jetstream_consumer_scenario(
    js: &JetStream,
    stream: &str,
    subject: &str,
) -> NatsResult<()> {
    // legacy pull consumer：创建后可经 get_pull_consumer 取回（底层逃生口）。
    let legacy_name = unique_name("legacy");
    js.create_pull_consumer(
        stream,
        PullConsumerConfig::durable(&legacy_name).filter(subject),
    )
    .await?;
    let raw = js.get_pull_consumer(stream, &legacy_name).await?;
    assert_eq!(raw.cached_info().name, legacy_name);
    assert_eq!(raw.cached_info().stream_name, stream);

    // 显式确认 consumer：短 ack_wait 便于观察 nak 重投。
    let worker_name = unique_name("worker");
    let mut config = JetStreamConsumerConfig::durable(&worker_name).filter(subject);
    config.ack_wait = Duration::from_secs(5);
    let consumer = js.consumer(stream, config).await?;

    // 空拉：fetch expiry 正常结束返回 None。
    assert!(consumer
        .next_timeout(Duration::from_millis(300))
        .await?
        .is_none());

    // 三条消息入库。
    js.publish(subject, b"msg-1".as_slice()).await?;
    js.publish(subject, b"msg-2".as_slice()).await?;
    js.publish(subject, b"msg-3".as_slice()).await?;

    // info / pending：待投递数如实。
    let info = consumer.info().await?;
    assert_eq!(info.stream_name, stream);
    assert_eq!(info.name, worker_name);
    assert_eq!(consumer.pending().await?, 3);

    // next_batch：一次拉全三条（batch expiry 1s 后结束）。
    let deliveries = consumer.next_batch(Duration::from_secs(1), 10).await?;
    assert_eq!(deliveries.len(), 3);
    let mut sequences = Vec::new();
    for (index, delivery) in deliveries.iter().enumerate() {
        assert_eq!(delivery.subject(), subject);
        assert_eq!(
            delivery.payload().as_ref(),
            format!("msg-{}", index + 1).as_bytes()
        );
        let metadata = delivery.metadata();
        assert_eq!(metadata.stream, stream);
        assert_eq!(metadata.consumer, worker_name);
        assert_eq!(metadata.delivery_attempts, 1);
        sequences.push(metadata.stream_sequence);
    }
    assert_eq!(sequences, vec![1, 2, 3], "stream_sequence 必须单调");

    let mut deliveries = deliveries.into_iter();
    let first = deliveries.next().expect("第一条投递");
    let second = deliveries.next().expect("第二条投递");
    let third = deliveries.next().expect("第三条投递");

    // double_ack：双确认。
    first.double_ack().await?;

    // progress + nak(None)：立即重投，delivery_attempts 递增后 ack 终结。
    second.progress().await?;
    second.nak(None).await?;
    let redelivered = consumer
        .next_timeout(Duration::from_secs(4))
        .await?
        .expect("nak 后必须重投");
    assert_eq!(redelivered.metadata().stream_sequence, 2);
    assert_eq!(redelivered.metadata().delivery_attempts, 2);
    assert_eq!(redelivered.payload().as_ref(), b"msg-2".as_slice());
    redelivered.ack().await?;

    // term：终止该消息后续重投。
    third.term().await?;
    assert!(
        consumer
            .next_timeout(Duration::from_secs(1))
            .await?
            .is_none(),
        "term 后不得再收到重投"
    );

    // 全部终结后空拉仍为 None；零超时 fail-closed。
    assert!(consumer
        .next_timeout(Duration::from_millis(300))
        .await?
        .is_none());
    assert!(consumer.next_timeout(Duration::ZERO).await.is_err());

    // 消息全部确认后待投递数归零。
    assert_eq!(consumer.pending().await?, 0);
    Ok(())
}

/// 离线纯面顺带断言：常量、校验函数、TLS 策略解析、错误分类面、from_toml、
/// JetStream 配置构造器与值类型（这些路径不触网；详细单测见 src/ 与 tests/ 各文件）。
#[test]
#[ignore = "随 live 套件一起执行"]
fn live_nats_pure_surface_assertions() {
    // 常量
    assert_eq!(SCHEMA_VERSION, 1);
    assert_eq!(DEFAULT_URL, "nats://127.0.0.1:4222");
    assert_eq!(DEFAULT_CLIENT_NAME, "natsx");
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_NATSX_");
    assert_eq!(ENV_LEGACY_PREFIX, "FOUNDATIONX_NATS_");
    assert_eq!(ENV_URL, "FOUNDATIONX_NATSX_URL");
    assert_eq!(ENV_USER, "FOUNDATIONX_NATSX_USER");
    assert_eq!(ENV_PASSWORD, "FOUNDATIONX_NATSX_PASSWORD");

    // 校验函数（数据面前的 fail-closed 关卡）
    assert!(validate_subject("a.b").is_ok());
    assert!(validate_subject("a.*").is_ok());
    assert!(validate_subject("has space").is_err());
    assert!(validate_publish_subject("a.b").is_ok());
    assert!(validate_publish_subject("a.*").is_err());
    assert!(validate_publish_subject("a.>").is_err());
    assert!(validate_stream_name("EVENTS").is_ok());
    assert!(validate_stream_name("bad.name").is_err());
    assert!(validate_consumer_name("worker").is_ok());
    assert!(validate_consumer_name("bad.name").is_err());
    assert!(validate_operation_timeout(Duration::from_millis(1)).is_ok());
    assert!(validate_operation_timeout(Duration::ZERO).is_err());

    // TLS 策略解析与展示
    assert_eq!(
        TlsPolicy::parse("require").expect("require"),
        TlsPolicy::Require
    );
    assert_eq!(TlsPolicy::parse("off").expect("off"), TlsPolicy::Disable);
    assert_eq!(TlsPolicy::parse("auto").expect("auto"), TlsPolicy::Prefer);
    assert!(TlsPolicy::parse("weird").is_err());
    assert!(TlsPolicy::Require.require_tls());
    assert!(!TlsPolicy::Prefer.require_tls());
    assert_eq!(TlsPolicy::default(), TlsPolicy::Prefer);
    assert_eq!(TlsPolicy::Disable.to_string(), "disable");
    assert!(url_is_loopback("nats://127.0.0.1:4222"));
    assert!(!url_is_loopback("nats://10.0.0.5:4222"));

    // 错误分类面
    assert_eq!(NatsError::config("x").kind_name(), "config");
    assert_eq!(NatsError::connection("x").kind_name(), "connection");
    assert_eq!(NatsError::backend("x").kind_name(), "backend");
    assert_eq!(NatsError::serialization("x").kind_name(), "serialization");
    assert_eq!(NatsError::timeout("x").kind_name(), "timeout");
    assert_eq!(NatsError::unsupported("x").kind_name(), "unsupported");
    assert!(!NatsError::config("x").is_retryable());
    assert!(NatsError::connection("x").is_retryable());
    assert!(NatsError::timeout("x").is_retryable());
    let io_error: NatsError = std::io::Error::other("x").into();
    assert!(io_error.is_retryable());
    assert!(NatsError::config("url 不能为空")
        .to_string()
        .contains("配置无效"));

    // from_toml（离线解析 + 敏感字段拒绝）
    let config =
        NatsConfig::from_toml("schema_version = 1\nname = \"live-toml\"\n").expect("TOML 解析");
    assert_eq!(config.name, "live-toml");
    assert!(
        NatsConfig::from_toml("schema_version = 1\npassword = \"x\"\n").is_err(),
        "TOML 禁止敏感字段"
    );

    // JetStream 配置构造器
    let stream = StreamConfig::new("S", "s.>");
    assert_eq!(stream.max_messages, 10_000);
    let multi = StreamConfig::with_subjects("E", ["a", "b"], 5);
    assert_eq!(multi.subjects.len(), 2);
    let pull = PullConsumerConfig::durable("w").filter("s.a");
    assert_eq!(pull.filter_subject.as_deref(), Some("s.a"));
    let mut durable = JetStreamConsumerConfig::durable("d");
    assert_eq!(durable.max_deliver, 5);
    durable.validate().expect("默认 durable 配置必须有效");
    durable.ack_wait = Duration::ZERO;
    assert!(durable.validate().is_err(), "零 ack_wait 必须被拒绝");
    let ephemeral = JetStreamConsumerConfig::ephemeral();
    assert!(ephemeral.durable_name.is_none());
    ephemeral.validate().expect("ephemeral 配置必须有效");

    // 值类型可构造
    assert!(!NatsPoolStats::default().closed);
    let health = NatsHealth {
        connected: true,
        server: "s".into(),
        rtt_ms: 1.0,
        jetstream: true,
        detail: "ok".into(),
    };
    assert!(health.connected);
    let metadata = JetStreamDeliveryMetadata {
        stream: "S".into(),
        consumer: "w".into(),
        stream_sequence: 1,
        consumer_sequence: 1,
        delivery_attempts: 1,
        pending: 0,
    };
    assert_eq!(metadata.stream, "S");
}
