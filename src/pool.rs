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
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};

use crate::config::NatsConfig;
use crate::error::{map_connect_error, NatsError, NatsResult};
use crate::validation::{validate_publish_subject, validate_subject};

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
    /// 同步构造：只做 [`NatsConfig::validate`]，**不**建立网络连接。
    ///
    /// 返回的池处于“未连接”状态，数据面操作会返回 [`NatsError::Connection`]；
    /// 需要立即可用请改用 [`NatsPool::connect`]。
    ///
    /// # Errors
    ///
    /// 配置不合法时返回 [`NatsError::Config`]。
    pub fn new(config: NatsConfig) -> NatsResult<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(PoolInner::new(config, None)),
        })
    }

    /// 按配置异步建立连接。
    ///
    /// 连接选项来源：
    /// - TLS：`require_tls` 按 [`NatsConfig::effective_tls_policy`] 设置，
    ///   自定义 CA / mTLS 经 `tls_client_config` 或 `add_client_certificate` 传入；
    /// - 认证：NKey seed > token > user/password（互斥关系已由校验收紧）；
    /// - 重连：`max_reconnects` + 指数退避（上限 `reconnect_max_delay`）。
    ///
    /// # Errors
    ///
    /// 配置非法、建连失败、建连或首次 `flush` 超时时返回错误。
    pub async fn connect(config: NatsConfig) -> NatsResult<Self> {
        config.validate()?;
        let policy = config.effective_tls_policy();
        let reconnect_max_delay = config.reconnect_max_delay;
        let connect_timeout = config.connect_timeout;
        let operation_timeout = config.operation_timeout;

        let connected = Arc::new(AtomicU64::new(0));
        let disconnected = Arc::new(AtomicU64::new(0));
        let slow_consumers = Arc::new(AtomicU64::new(0));
        let event_connected = Arc::clone(&connected);
        let event_disconnected = Arc::clone(&disconnected);
        let event_slow_consumers = Arc::clone(&slow_consumers);

        let mut options = async_nats::ConnectOptions::new()
            .name(config.name.clone())
            .connection_timeout(connect_timeout)
            .request_timeout(Some(operation_timeout))
            .subscription_capacity(config.subscription_capacity)
            .client_capacity(config.client_capacity)
            .max_reconnects(Some(config.max_reconnects))
            .reconnect_delay_callback(move |attempt| {
                let exponent = u32::try_from(attempt.min(16)).unwrap_or(16);
                let factor = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
                Duration::from_millis(100)
                    .saturating_mul(factor)
                    .min(reconnect_max_delay)
            })
            .event_callback(move |event| {
                let connected = Arc::clone(&event_connected);
                let disconnected = Arc::clone(&event_disconnected);
                let slow_consumers = Arc::clone(&event_slow_consumers);
                async move {
                    match event {
                        async_nats::Event::Connected => {
                            connected.fetch_add(1, Ordering::Relaxed);
                        }
                        async_nats::Event::Disconnected => {
                            disconnected.fetch_add(1, Ordering::Relaxed);
                        }
                        async_nats::Event::SlowConsumer(_) => {
                            slow_consumers.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
            });
        options = config.apply_tls(options)?;
        if config.ignore_discovered_servers {
            options = options.ignore_discovered_servers().retain_servers_order();
        }
        if let Some(seed) = config.nkey_seed() {
            options = options.nkey(seed.to_string());
        } else if let Some(token) = config.token() {
            options = options.token(token.to_string());
        } else if let Some((user, password)) = config.user_password() {
            options = options.user_and_password(user.to_string(), password.to_string());
        }

        tracing::debug!(
            url = %config.url,
            tls_policy = %policy,
            jetstream = config.jetstream,
            "natsx 建立连接"
        );
        let client = tokio::time::timeout(connect_timeout, options.connect(config.url.as_str()))
            .await
            .map_err(|_| {
                NatsError::timeout(format!(
                    "连接 NATS 超时（{}ms）",
                    connect_timeout.as_millis()
                ))
            })?
            .map_err(|error| map_connect_error(&error))?;

        tokio::time::timeout(operation_timeout, client.flush())
            .await
            .map_err(|_| {
                NatsError::timeout(format!(
                    "连接后 flush 超时（{}ms）",
                    operation_timeout.as_millis()
                ))
            })?
            .map_err(|error| NatsError::connection(format!("连接后 flush 失败: {error}")))?;

        Ok(Self {
            inner: Arc::new(PoolInner::new_with_counters(
                config,
                Some(client),
                connected,
                disconnected,
                slow_consumers,
            )),
        })
    }

    /// 从环境变量读取配置并连接（见 [`NatsConfig::from_env`]）。
    ///
    /// # Errors
    ///
    /// 环境变量非法或建连失败时返回错误。
    pub async fn connect_from_env() -> NatsResult<Self> {
        Self::connect(NatsConfig::from_env()?).await
    }

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

    /// 发布消息（Fire-and-Forget + flush，**非**持久化）。
    ///
    /// # Errors
    ///
    /// subject 非法、池未连接/已关闭、发布或 flush 超时、驱动返回错误时返回错误。
    pub async fn publish(&self, subject: &str, payload: impl Into<Bytes>) -> NatsResult<()> {
        self.publish_inner(subject, payload.into(), None).await
    }

    /// 带 headers 发布消息。
    ///
    /// # Errors
    ///
    /// 同 [`NatsPool::publish`]。
    pub async fn publish_with_headers(
        &self,
        subject: &str,
        headers: async_nats::HeaderMap,
        payload: impl Into<Bytes>,
    ) -> NatsResult<()> {
        self.publish_inner(subject, payload.into(), Some(headers))
            .await
    }

    /// 以 JSON 序列化后发布消息。
    ///
    /// # Errors
    ///
    /// 序列化失败返回 [`NatsError::Serialization`]；其余同 [`NatsPool::publish`]。
    pub async fn publish_json<T: serde::Serialize + ?Sized>(
        &self,
        subject: &str,
        value: &T,
    ) -> NatsResult<()> {
        let payload = serde_json::to_vec(value)
            .map_err(|error| NatsError::serialization(format!("JSON 序列化失败: {error}")))?;
        self.publish(subject, payload).await
    }

    async fn publish_inner(
        &self,
        subject: &str,
        payload: Bytes,
        headers: Option<async_nats::HeaderMap>,
    ) -> NatsResult<()> {
        validate_publish_subject(subject)?;
        let client = self.ready_client()?.clone();
        let timeout = self.inner.config.operation_timeout;

        let publish = async {
            match headers {
                Some(headers) => {
                    client
                        .publish_with_headers(subject.to_string(), headers, payload)
                        .await
                }
                None => client.publish(subject.to_string(), payload).await,
            }
        };
        if let Err(error) = tokio::time::timeout(timeout, publish)
            .await
            .map_err(|_| NatsError::timeout(format!("publish 超时（{}ms）", timeout.as_millis())))?
        {
            self.inner.publish_failed.fetch_add(1, Ordering::Relaxed);
            return Err(map_publish_error(&error));
        }

        if let Err(error) = tokio::time::timeout(timeout, client.flush())
            .await
            .map_err(|_| {
                NatsError::timeout(format!("publish flush 超时（{}ms）", timeout.as_millis()))
            })?
        {
            self.inner.publish_failed.fetch_add(1, Ordering::Relaxed);
            return Err(NatsError::connection(format!(
                "publish 后 flush 失败: {error}"
            )));
        }
        self.inner.published.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 订阅 subject（实时流；无历史回放）。
    ///
    /// # Errors
    ///
    /// subject 非法、池未连接/已关闭、订阅或内部任务注册失败时返回错误。
    pub async fn subscribe(&self, subject: &str) -> NatsResult<NatsSubscription> {
        validate_subject(subject)?;
        let client = self.ready_client()?.clone();
        let timeout = self.inner.config.operation_timeout;

        let mut subscriber = tokio::time::timeout(timeout, client.subscribe(subject.to_string()))
            .await
            .map_err(|_| {
                NatsError::timeout(format!("subscribe 超时（{}ms）", timeout.as_millis()))
            })?
            .map_err(|error| NatsError::backend(format!("subscribe 被驱动拒绝: {error}")))?;

        let (tx, rx) = mpsc::channel(self.inner.config.subscription_capacity);
        let seq_base = self.inner.sub_seq.fetch_add(1, Ordering::Relaxed) << 32;
        let counter = Arc::new(AtomicU64::new(0));
        let slow_consumers = Arc::clone(&self.inner.slow_consumers);

        let task = tokio::spawn(async move {
            while let Some(message) = subscriber.next().await {
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                let out = NatsMessage {
                    subject: message.subject.to_string(),
                    payload: message.payload,
                    reply: message.reply.map(|reply| reply.to_string()),
                    seq: seq_base | seq,
                    headers: message.headers,
                };
                match tokio::time::timeout(timeout, tx.send(out)).await {
                    Ok(Ok(())) => {}
                    // 接收端已丢弃：正常结束转发任务
                    Ok(Err(_)) => break,
                    // 下游消费过慢：计一次慢消费者并结束，避免无界堆积
                    Err(_) => {
                        slow_consumers.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        });

        let abort_handle = task.abort_handle();
        self.register_task(task)?;
        Ok(NatsSubscription {
            rx,
            task: Some(SubscriptionTask(abort_handle)),
        })
    }

    /// 发送 Core NATS request 并等待至多一条 reply（请求-应答模式）。
    ///
    /// # Errors
    ///
    /// subject 非法、`deadline` 为零、池未连接/已关闭、等待超时或无响应时返回错误。
    pub async fn request(
        &self,
        subject: &str,
        payload: impl Into<Bytes>,
        deadline: Duration,
    ) -> NatsResult<NatsMessage> {
        validate_subject(subject)?;
        if deadline.is_zero() {
            return Err(NatsError::config("request deadline 必须大于零"));
        }
        let client = self.ready_client()?.clone();
        let response = tokio::time::timeout(
            deadline,
            client.request(subject.to_string(), payload.into()),
        )
        .await
        .map_err(|_| NatsError::timeout(format!("request 超时（{}ms）", deadline.as_millis())))?
        .map_err(|error| map_request_error(&error))?;
        Ok(NatsMessage {
            subject: response.subject.to_string(),
            payload: response.payload,
            reply: None,
            seq: self.inner.sub_seq.fetch_add(1, Ordering::Relaxed) << 32,
            headers: response.headers,
        })
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

    /// 关停：拒绝新请求，取消并等待订阅转发任务，最后 flush 连接缓冲。
    ///
    /// # Errors
    ///
    /// 等待订阅任务或 flush 超时、任务异常退出时返回错误。
    pub async fn close(&self) -> NatsResult<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        let handles = self.take_tasks()?;
        tokio::time::timeout(
            self.inner.config.operation_timeout,
            join_subscription_tasks(handles, true),
        )
        .await
        .map_err(|_| NatsError::timeout("close 等待订阅任务退出超时"))??;

        let Some(client) = self.inner.client.clone() else {
            return Ok(());
        };
        tokio::time::timeout(self.inner.config.operation_timeout, client.flush())
            .await
            .map_err(|_| NatsError::timeout("close flush 超时"))?
            .map_err(|error| NatsError::connection(format!("close flush 失败: {error}")))
    }

    /// 优雅关停：拒绝新请求 → flush 缓冲 → 结束订阅转发任务 → 最终 flush。
    ///
    /// `deadline` 是整个流程的总截止时间，各阶段共享剩余预算。
    ///
    /// # Errors
    ///
    /// `deadline` 为零、阶段超时或 flush 失败时返回错误。
    pub async fn drain(&self, deadline: Duration) -> NatsResult<()> {
        if deadline.is_zero() {
            return Err(NatsError::config("drain deadline 必须大于零"));
        }
        self.inner.closed.store(true, Ordering::SeqCst);
        let started = Instant::now();
        let Some(client) = self.inner.client.clone() else {
            return Ok(());
        };

        let remaining = deadline.saturating_sub(started.elapsed());
        tokio::time::timeout(remaining, client.flush())
            .await
            .map_err(|_| NatsError::timeout("drain 首阶段 flush 超时"))?
            .map_err(|error| NatsError::connection(format!("drain flush 失败: {error}")))?;

        let handles = self.take_tasks()?;
        let remaining = deadline.saturating_sub(started.elapsed());
        tokio::time::timeout(remaining, join_subscription_tasks(handles, true))
            .await
            .map_err(|_| NatsError::timeout("drain 等待订阅任务退出超时"))??;

        let remaining = deadline.saturating_sub(started.elapsed());
        tokio::time::timeout(remaining, client.flush())
            .await
            .map_err(|_| NatsError::timeout("drain 最终 flush 超时"))?
            .map_err(|error| NatsError::connection(format!("drain 最终 flush 失败: {error}")))
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

impl PoolInner {
    fn new(config: NatsConfig, client: Option<Client>) -> Self {
        Self::new_with_counters(
            config,
            client,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn new_with_counters(
        config: NatsConfig,
        client: Option<Client>,
        connected: Arc<AtomicU64>,
        disconnected: Arc<AtomicU64>,
        slow_consumers: Arc<AtomicU64>,
    ) -> Self {
        Self {
            config,
            client,
            published: AtomicU64::new(0),
            publish_failed: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            sub_seq: AtomicU64::new(0),
            connected,
            disconnected,
            slow_consumers,
            subscription_tasks: Mutex::new(Vec::new()),
        }
    }
}

/// 把 `async-nats` 的发布错误映射为 [`NatsError`]。
fn map_publish_error(error: &async_nats::client::PublishError) -> NatsError {
    use async_nats::PublishErrorKind as Kind;

    match error.kind() {
        Kind::InvalidSubject | Kind::MaxPayloadExceeded => {
            NatsError::config(format!("publish 请求被本地拒绝: {error}"))
        }
        Kind::Send => NatsError::connection(format!("publish 发送失败: {error}")),
    }
}

/// 把 `async-nats` 的 request-reply 错误映射为 [`NatsError`]。
fn map_request_error(error: &async_nats::RequestError) -> NatsError {
    use async_nats::RequestErrorKind as Kind;

    match error.kind() {
        Kind::NoResponders => NatsError::backend(format!("request 无响应者: {error}")),
        Kind::TimedOut => NatsError::timeout(format!("request 等待应答超时: {error}")),
        Kind::InvalidSubject | Kind::MaxPayloadExceeded => {
            NatsError::config(format!("request 请求被本地拒绝: {error}"))
        }
        Kind::Other => NatsError::connection(format!("request 失败: {error}")),
    }
}

async fn join_subscription_tasks(tasks: Vec<JoinHandle<()>>, abort: bool) -> NatsResult<()> {
    if abort {
        for task in &tasks {
            task.abort();
        }
    }
    for task in tasks {
        match task.await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                return Err(NatsError::connection(format!(
                    "订阅转发任务异常退出: {error}"
                )));
            }
        }
    }
    Ok(())
}

/// 离线可测路径：未连接池的 fail-closed 行为、错误分类与订阅流语义。
#[cfg(test)]
mod tests;
