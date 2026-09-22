//! `NatsPool`：共享 `async-nats` 客户端句柄，提供 Core NATS 发布/订阅/健康/生命周期原语。
//!
//! # 生命周期
//!
//! - [`NatsPool::new`]：**同步仅校验**配置，不建立网络连接（便于配置预热与测试）；
//! - [`NatsPool::connect`]：异步建连，成功后数据面操作可用；
//! - [`NatsPool::close`] / [`NatsPool::drain`]：关停（拒绝新请求 + 结束订阅转发任务 + flush）。
//!
//! # 语义
//!
//! - `publish` 在返回前执行一次 `flush`，调用方看到 `Ok` 时消息已离开客户端缓冲；
//! - 未连接（仅 `new`）或已关闭的池，所有数据面操作返回 [`NatsError::Connection`]；
//! - 订阅是实时流，**无历史回放**；需要回放请使用 JetStream。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::{Client, ServerInfo};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};

use crate::config::NatsConfig;
use crate::error::{NatsError, NatsResult};

mod connection;
mod pubsub;

/// 连接池统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NatsPoolStats {
    /// 成功 publish 次数。
    pub published: u64,
    /// publish 失败次数。
    pub publish_failed: u64,
    /// 是否已关闭。
    pub closed: bool,
    /// 建连事件数（含首次连接与重连成功）。
    pub connected: u64,
    /// 断线事件数。
    pub disconnected: u64,
    /// 慢消费者事件数（驱动报告或转发缓冲写满）。
    pub slow_consumers: u64,
}

/// 结构化健康结果。
#[derive(Debug, Clone, PartialEq)]
pub struct NatsHealth {
    /// 是否已连接且 `flush` 成功。
    pub connected: bool,
    /// 服务端标识：优先 `server_name`，为空时回落 `host:port`。
    ///
    /// **未连接、或服务端信息不可用时为空串**——不会编造地址。
    pub server: String,
    /// 最近一次 `flush` 往返耗时（毫秒）；未连接时为 0。
    pub rtt_ms: f64,
    /// 服务端是否声明支持 JetStream。
    ///
    /// 服务端信息不可用时为 `false`，表示**未知**而非已证否。
    pub jetstream: bool,
    /// 诊断说明。
    pub detail: String,
}

/// 渲染展示用的服务端标识：优先 `server_name`，为空时回落 `host:port`。
///
/// 传入 `None`（服务端信息获取失败）时返回**空串**。这里刻意不编造任何地址：
/// `ServerInfo::default()` 的 `host` 为空、`port` 为 `0`，格式化会得到 `":0"`
/// 这种看起来像地址、实际无意义的串，会被 readiness 面板当成真实的服务端。
fn render_server(info: Option<&ServerInfo>) -> String {
    match info {
        Some(info) if info.server_name.is_empty() => format!("{}:{}", info.host, info.port),
        Some(info) => info.server_name.clone(),
        None => String::new(),
    }
}

/// 收到的 Core NATS 消息。
#[derive(Debug, Clone)]
pub struct NatsMessage {
    /// subject。
    pub subject: String,
    /// 载荷。
    pub payload: Bytes,
    /// reply subject（request-reply 场景下由订阅方回填）。
    pub reply: Option<String>,
    /// 会话内单调序号（跨重连不可用于去重）。
    pub seq: u64,
    /// 消息 headers。
    pub headers: Option<async_nats::HeaderMap>,
}

/// 订阅句柄：后台任务把消息推入通道，`Drop` 时取消该任务。
///
/// 既可直接 `await` 内置的 [`NatsSubscription::next`]，
/// 也可当作 [`futures_core::Stream`] 交给 `futures_util::StreamExt` 组合。
pub struct NatsSubscription {
    rx: mpsc::Receiver<NatsMessage>,
    task: Option<SubscriptionTask>,
}

impl std::fmt::Debug for NatsSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSubscription")
            .field("buffered", &self.rx.len())
            .field("has_forwarder", &self.task.is_some())
            .finish()
    }
}

struct SubscriptionTask(AbortHandle);

impl Drop for SubscriptionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl NatsSubscription {
    /// 取下一条消息；通道关闭（池关闭或转发任务结束）时返回 `None`。
    pub async fn next(&mut self) -> Option<NatsMessage> {
        self.rx.recv().await
    }
}

impl futures_core::Stream for NatsSubscription {
    type Item = NatsMessage;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Core NATS 连接池（可克隆，内部共享同一个连接句柄）。
#[derive(Clone)]
pub struct NatsPool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for NatsPool {
    /// 输出已脱敏的配置、连接状态与统计快照。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsPool")
            .field("config", &self.inner.config)
            .field("connected", &self.is_connected())
            .field("stats", &self.stats())
            .finish()
    }
}

struct PoolInner {
    config: NatsConfig,
    client: Option<Client>,
    published: AtomicU64,
    publish_failed: AtomicU64,
    closed: AtomicBool,
    sub_seq: AtomicU64,
    connected: Arc<AtomicU64>,
    disconnected: Arc<AtomicU64>,
    slow_consumers: Arc<AtomicU64>,
    subscription_tasks: Mutex<Vec<JoinHandle<()>>>,
}
impl NatsPool {
    /// 配置。
    #[must_use]
    pub fn config(&self) -> &NatsConfig {
        &self.inner.config
    }

    /// 底层 `async-nats` 客户端句柄（高级逃生口）；未连接时为 `None`。
    #[must_use]
    pub fn client(&self) -> Option<Client> {
        self.inner.client.clone()
    }

    /// 是否已建立连接且未关闭。
    #[must_use]
    pub fn is_connected(&self) -> bool {
        !self.inner.closed.load(Ordering::Relaxed) && self.inner.client.is_some()
    }

    /// flush 客户端缓冲并返回往返耗时（健康探测的底层原语）。
    ///
    /// # Errors
    ///
    /// 池未连接/已关闭、flush 超时或连接中断时返回错误。
    pub async fn ping(&self) -> NatsResult<Duration> {
        let client = self.ready_client()?.clone();
        let started = Instant::now();
        tokio::time::timeout(self.inner.config.operation_timeout, client.flush())
            .await
            .map_err(|_| {
                NatsError::timeout(format!(
                    "ping/flush 超时（{}ms）",
                    self.inner.config.operation_timeout.as_millis()
                ))
            })?
            .map_err(|error| NatsError::connection(format!("ping/flush 失败: {error}")))?;
        Ok(started.elapsed())
    }

    /// flush 客户端缓冲（不返回耗时）。
    ///
    /// # Errors
    ///
    /// 同 [`NatsPool::ping`]。
    pub async fn flush(&self) -> NatsResult<()> {
        self.ping().await.map(|_| ())
    }

    /// 结构化健康检查：返回连接状态、服务端信息、往返耗时与 JetStream 可用性。
    ///
    /// 该方法是**诊断**入口，探测失败不返回 `Err`：结果中 `connected == false`
    /// 并在 `detail` 中给出失败原因；仅当配置本身使探测无法进行时才可能返回错误
    /// （当前实现恒为 `Ok`）。
    ///
    /// 服务端信息（`server` / `jetstream`）获取失败时**如实标注为未知**：
    /// `server` 为空串、`detail` 附带说明，而不是回落到 `ServerInfo::default()`
    /// 那条会渲染出 `":0"` 的路径。
    ///
    /// # Errors
    ///
    /// 保留 `Result` 形态以便未来扩展；当前实现不返回错误。
    pub async fn health_check(&self) -> NatsResult<NatsHealth> {
        let Some(client) = self.inner.client.as_ref().filter(|_| self.is_connected()) else {
            return Ok(NatsHealth {
                connected: false,
                server: String::new(),
                rtt_ms: 0.0,
                jetstream: false,
                detail: if self.inner.closed.load(Ordering::Relaxed) {
                    "连接池已关闭".to_string()
                } else {
                    "连接池尚未连接（请先调用 NatsPool::connect）".to_string()
                },
            });
        };
        // 获取失败时保持 `None`：`ServerInfo::default()` 的 host 为空、port 为 0，
        // 直接格式化会得到一个看似地址实为 `":0"` 的串——宁可如实标注为未知。
        let info = client.try_server_info();
        let server = render_server(info.as_ref());
        let jetstream = info.as_ref().is_some_and(|info| info.jetstream);
        let info_note = if info.is_some() {
            ""
        } else {
            "；服务端信息不可用（server 与 jetstream 未知）"
        };
        match self.ping().await {
            Ok(rtt) => Ok(NatsHealth {
                connected: true,
                server,
                rtt_ms: rtt.as_secs_f64() * 1_000.0,
                jetstream,
                detail: format!("flush ok{info_note}"),
            }),
            Err(error) => Ok(NatsHealth {
                connected: false,
                server,
                rtt_ms: 0.0,
                jetstream,
                detail: format!("{error}{info_note}"),
            }),
        }
    }

    /// 统计快照。
    #[must_use]
    pub fn stats(&self) -> NatsPoolStats {
        NatsPoolStats {
            published: self.inner.published.load(Ordering::Relaxed),
            publish_failed: self.inner.publish_failed.load(Ordering::Relaxed),
            closed: self.inner.closed.load(Ordering::Relaxed),
            connected: self.inner.connected.load(Ordering::Relaxed),
            disconnected: self.inner.disconnected.load(Ordering::Relaxed),
            slow_consumers: self.inner.slow_consumers.load(Ordering::Relaxed),
        }
    }

    /// 取回可用的客户端句柄；池未连接或已关闭时返回错误。
    fn ready_client(&self) -> NatsResult<&Client> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(NatsError::connection("连接池已关闭"));
        }
        self.inner
            .client
            .as_ref()
            .ok_or_else(|| NatsError::connection("连接池尚未连接（请先调用 NatsPool::connect）"))
    }

    fn register_task(&self, task: JoinHandle<()>) -> NatsResult<()> {
        let mut tasks = self
            .inner
            .subscription_tasks
            .lock()
            .map_err(|_| NatsError::connection("订阅任务注册表锁已中毒"))?;
        if self.inner.closed.load(Ordering::Relaxed) {
            task.abort();
            return Err(NatsError::connection("连接池已关闭"));
        }
        tasks.push(task);
        Ok(())
    }

    fn take_tasks(&self) -> NatsResult<Vec<JoinHandle<()>>> {
        let mut tasks = self
            .inner
            .subscription_tasks
            .lock()
            .map_err(|_| NatsError::connection("订阅任务注册表锁已中毒"))?;
        Ok(tasks.drain(..).collect())
    }
}

/// 离线可测路径：未连接池的 fail-closed 行为、错误分类与订阅流语义。
#[cfg(test)]
mod tests {
    use super::*;
    // 数据面与关停的私有辅助/依赖下沉到子模块后，测试按需显式引入（写在测试模块内，
    // 避免在非测试构建里产生 unused import）。
    use super::connection::join_subscription_tasks;
    use crate::config::NatsConfig;
    use futures_util::StreamExt;

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
}
