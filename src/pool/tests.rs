//! NatsPool 单元测试：离线 fail-closed 行为、错误分类、订阅流与关停语义。
//!
//! 由本模块的 `#[cfg(test)] mod tests;` 引入，仅在测试构建中编译。

#[cfg(test)]
use super::*;
use crate::config::NatsConfig;

#[test]
fn new_validates_without_connecting() {
    let pool = NatsPool::new(NatsConfig::default()).expect("默认配置可构造");
    assert!(!pool.is_connected());
    assert!(pool.client().is_none());
    assert_eq!(pool.stats(), NatsPoolStats::default());
    assert!(format!("{pool:?}").contains("NatsPool"));

    // 绕过 from_toml/Builder 校验的配置必须被 new() 再次拒绝（fail-closed）
    let invalid: NatsConfig = toml::from_str("url = \"\"").expect("反序列化");
    assert!(NatsPool::new(invalid).is_err());
}

#[tokio::test]
async fn disconnected_pool_fails_closed_without_panic() {
    let pool = NatsPool::new(NatsConfig::default()).expect("构造");
    assert!(pool.publish("subject", "payload").await.is_err());
    assert!(pool.subscribe("subject").await.is_err());
    assert!(pool.ping().await.is_err());
    assert!(pool.flush().await.is_err());
    assert!(pool.close().await.is_ok());

    // 关闭后依然 fail-closed，且状态可观测
    assert!(pool.publish("subject", "payload").await.is_err());
    assert!(pool.stats().closed);
    let health = pool.health_check().await.expect("健康检查恒为 Ok");
    assert!(!health.connected);
}

#[tokio::test]
async fn publish_rejects_invalid_subject_before_io() {
    let pool = NatsPool::new(NatsConfig::default()).expect("构造");
    // 先命中 subject 校验，错误分类为 Config 而非 Connection
    let error = pool
        .publish("bad subject", "x")
        .await
        .expect_err("非法 subject");
    assert!(matches!(error, NatsError::Config(_)));
    let error = pool
        .publish("orders.*", "x")
        .await
        .expect_err("通配符不可发布");
    assert!(matches!(error, NatsError::Config(_)));
    let error = pool
        .request("", "x", Duration::from_secs(1))
        .await
        .expect_err("空 subject");
    assert!(matches!(error, NatsError::Config(_)));
    let error = pool
        .request("orders.get", "x", Duration::ZERO)
        .await
        .expect_err("零 deadline");
    assert!(matches!(error, NatsError::Config(_)));
}

#[tokio::test]
async fn drain_rejects_zero_deadline() {
    let pool = NatsPool::new(NatsConfig::default()).expect("构造");
    assert!(pool.drain(Duration::ZERO).await.is_err());
    assert!(pool.drain(Duration::from_millis(50)).await.is_ok());
}

#[tokio::test]
async fn connect_refused_maps_to_retryable_error() {
    let config = NatsConfig::builder()
        .url("nats://127.0.0.1:1")
        .connect_timeout(Duration::from_millis(300))
        .build()
        .expect("构造");
    let started = Instant::now();
    let error = NatsPool::connect(config)
        .await
        .expect_err("端口 1 必然拒绝连接");
    assert!(
        error.is_retryable(),
        "连接被拒应归类为可重试错误，实际: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "必须受内部截止时间约束"
    );
}

#[tokio::test]
async fn dropping_subscription_aborts_forwarder_task() {
    let (_tx, rx) = mpsc::channel(1);
    let task = tokio::spawn(std::future::pending::<()>());
    let subscription = NatsSubscription {
        rx,
        task: Some(SubscriptionTask(task.abort_handle())),
    };
    drop(subscription);
    let error = task.await.expect_err("订阅 drop 必须取消转发任务");
    assert!(error.is_cancelled());
}

#[tokio::test]
async fn subscription_stream_forwards_then_ends() {
    let (tx, rx) = mpsc::channel(2);
    let mut subscription = NatsSubscription { rx, task: None };
    tx.send(NatsMessage {
        subject: "orders.created".into(),
        payload: Bytes::from_static(b"a"),
        reply: None,
        seq: 7,
        headers: None,
    })
    .await
    .expect("发送");
    let received = subscription.next().await.expect("接收");
    assert_eq!(received.seq, 7);
    assert_eq!(received.subject, "orders.created");
    drop(tx);
    assert!(subscription.next().await.is_none());
}

#[tokio::test]
async fn subscription_works_as_stream() {
    let (tx, rx) = mpsc::channel(1);
    let subscription = NatsSubscription { rx, task: None };
    tx.send(NatsMessage {
        subject: "s".into(),
        payload: Bytes::from_static(b"b"),
        reply: Some("reply".into()),
        seq: 1,
        headers: None,
    })
    .await
    .expect("发送");
    drop(tx);
    let collected: Vec<NatsMessage> = subscription.collect().await;
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].reply.as_deref(), Some("reply"));
}

#[tokio::test]
async fn joining_subscription_task_reports_panic() {
    let task: JoinHandle<()> = tokio::spawn(async { panic!("注入订阅转发任务 panic") });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let error = join_subscription_tasks(vec![task], true)
        .await
        .expect_err("任务 panic 必须作为关停错误上报");
    assert!(matches!(error, NatsError::Connection(_)));
}

/// `render_server` 在服务端信息缺失时必须返回空串，**不得编造地址**。
///
/// 回归保护：此前实现走 `try_server_info().unwrap_or_default()`，而
/// `ServerInfo::default()` 的 host 为空、port 为 0，渲染结果正是下面锁定的
/// `":0"`——一个看起来像 host:port、实际无意义的串，会被 readiness 面板当成
/// 真实服务端地址。因此调用方绝不能把 `None` 折叠成 `default()`。
#[test]
fn render_server_never_fabricates_address() {
    assert_eq!(
        render_server(None),
        "",
        "服务端信息缺失时必须为空串，不得编造地址"
    );

    // 锁定「伪造串」长什么样：它正是旧实现会输出的值。
    assert_eq!(render_server(Some(&ServerInfo::default())), ":0");

    let named = ServerInfo {
        server_name: "nats-1".into(),
        host: "10.0.0.1".into(),
        port: 4222,
        ..ServerInfo::default()
    };
    assert_eq!(render_server(Some(&named)), "nats-1");

    // `server_name` 为空时回落到 `host:port`。
    let unnamed = ServerInfo {
        host: "10.0.0.1".into(),
        port: 4222,
        ..ServerInfo::default()
    };
    assert_eq!(render_server(Some(&unnamed)), "10.0.0.1:4222");
}

#[test]
fn health_and_stats_types_are_constructible() {
    let health = NatsHealth {
        connected: false,
        server: "nats-1".into(),
        rtt_ms: 1.5,
        jetstream: true,
        detail: "offline".into(),
    };
    assert!(!health.connected);
    assert!(health.detail.contains("offline"));
    assert!(health.jetstream);

    let stats = NatsPoolStats {
        published: 1,
        publish_failed: 2,
        closed: false,
        connected: 1,
        disconnected: 3,
        slow_consumers: 4,
    };
    assert_eq!(stats.published, 1);
    assert_eq!(stats.slow_consumers, 4);
    assert_eq!(NatsPoolStats::default().published, 0);
    assert!(!NatsPoolStats::default().closed);
}
