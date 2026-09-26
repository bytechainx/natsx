//! `NatsPool` 的连接建立与关停。
//!
//! 从 `src/pool.rs` 下沉而来：建连（`new` / `connect` / `connect_from_env`）、
//! 关停（`close` / `drain`）、它们共用的 `PoolInner` 构造，以及订阅转发任务的汇合。
//! `NatsPool` / `PoolInner` 的定义与门面仍在 `src/pool.rs`；本模块是它的子模块，
//! 故可直接读写二者的私有字段（可见性方向单向：子可见父，父不可见子）。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_nats::Client;
use tokio::task::JoinHandle;

use crate::config::NatsConfig;
use crate::error::{map_connect_error, NatsError, NatsResult};

use super::{NatsPool, PoolInner};

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
    ///   自定义 CA 经 `add_root_certificates`；仅 mTLS 经 `add_client_certificate`；
    /// - 认证：NKey seed > token > user/password（互斥关系已由校验收紧）；
    /// - 重连：`max_reconnects` + 指数退避（±25% 抖动，上限 `reconnect_max_delay`）。
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
            .reconnect_delay_callback(move |attempt| reconnect_delay(attempt, reconnect_max_delay))
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

pub(super) async fn join_subscription_tasks(
    tasks: Vec<JoinHandle<()>>,
    abort: bool,
) -> NatsResult<()> {
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

/// 计算第 `attempt` 次重连的退避时长：指数退避 + ±25% 抖动，封顶 `max_delay`。
///
/// 纯指数退避是确定性序列——大规模部署（数百客户端 + 服务端重启）下所有实例
/// 会在同一时刻重连，形成同步风暴（thundering herd）。加入 ±25% 抖动把重连
/// 时刻打散到一个窗口内；抖动幅度与 postgresx `PgRetryConfig` 保持一致。
fn reconnect_delay(attempt: usize, max_delay: Duration) -> Duration {
    let exponent = u32::try_from(attempt.min(16)).unwrap_or(16);
    let factor = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
    let base = Duration::from_millis(100).saturating_mul(factor);
    // 抖动系数 0.75 ~ 1.25（±25%）；封顶在抖动之后，保证不超过 max_delay
    let jitter_factor = 750 + (jitter_seed() % 501) as u32;
    base.mul_f64(f64::from(jitter_factor) / 1000.0)
        .min(max_delay)
}

/// 每次调用返回一个新的伪随机种子（时间纳秒 + 单调计数器混合，无需额外依赖）。
fn jitter_seed() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    let mut state = nanos ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn reconnect_delay_stays_within_jitter_band() {
        // attempt=3 → 基准 800ms，±25% 抖动后应落在 [600ms, 1000ms]
        let max = Duration::from_secs(5);
        for _ in 0..64 {
            let delay = reconnect_delay(3, max);
            assert!(
                delay >= Duration::from_millis(600) && delay <= Duration::from_millis(1000),
                "退避 {delay:?} 超出 ±25% 抖动区间"
            );
        }
    }

    #[test]
    fn reconnect_delay_is_jittered_not_deterministic() {
        // 32 次采样必须出现至少两个不同值，证明抖动生效（计数器保证种子互异）
        let max = Duration::from_secs(30);
        let samples: HashSet<Duration> = (0..32).map(|_| reconnect_delay(2, max)).collect();
        assert!(samples.len() > 1, "32 次采样全部相同，抖动未生效");
    }

    #[test]
    fn reconnect_delay_never_exceeds_cap() {
        let max = Duration::from_secs(1);
        for attempt in 0..20 {
            let delay = reconnect_delay(attempt, max);
            assert!(
                delay <= max,
                "attempt={attempt} 退避 {delay:?} 超出上限 {max:?}"
            );
        }
    }
}
