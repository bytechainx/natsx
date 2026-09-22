//! `NatsPool` 的数据面：发布 / 订阅 / 请求-应答。
//!
//! 从 `src/pool.rs` 下沉而来：`publish*` 族、`subscribe`（含转发任务与慢消费者计数）、
//! `request`，以及两条驱动错误映射。`NatsPool` 的定义与门面仍在 `src/pool.rs`；
//! 本模块是它的子模块，故可直接调用门面上的私有辅助（`ready_client` / `register_task`）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::error::{NatsError, NatsResult};
use crate::validation::{validate_publish_subject, validate_subject};

use super::{NatsMessage, NatsPool, NatsSubscription, SubscriptionTask};

impl NatsPool {
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
