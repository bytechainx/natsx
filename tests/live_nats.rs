#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服（natsx）：需先 `source /home/workspace/sre/secrets/env/natsx.env`。
//!
//! 全部用例 `#[ignore]`，默认不跑（CI 行为不变）。显式运行：
//!
//! ```bash
//! set -a; source /home/workspace/sre/secrets/env/natsx.env; set +a
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_nats -- --ignored --test-threads=1
//! ```
//!
//! 凭据只从环境变量读取，绝不硬编码；subject 唯一化（pid + 纳秒时间戳），
//! Core NATS 无历史回放，故每条用例先订阅、flush 确保 SUB 到达服务端，再发布。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use natsx::{NatsConfig, NatsPool};

/// 进程内唯一的 subject：`natsx.live.<用途>.<pid>.<纳秒>`。
fn unique_subject(use_case: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时间应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("natsx.live.{use_case}.{}.{}", std::process::id(), nanos)
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

    // 应答方：订阅并回写到 reply subject。
    let mut responder = pool.subscribe(&subject).await.expect("订阅必须成功");
    pool.flush().await.expect("flush 以确保 SUB 已到达服务端");
    let responder_pool = pool.clone();
    let task = tokio::spawn(async move {
        if let Some(message) = responder.next().await {
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
