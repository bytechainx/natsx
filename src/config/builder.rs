//! [`NatsConfigBuilder`]：链式覆盖 [`NatsConfig`] 字段，构建时执行完整校验。
//!
//! [`NatsConfigBuilder::build`] 调用 [`NatsConfig::validate`]，因此经 Builder 构造的
//! 配置与 [`NatsConfig::from_env`] / [`NatsConfig::from_toml`] 同源同规。

use std::time::Duration;

use super::{NatsConfig, TlsPolicy};
use crate::error::NatsResult;

/// 配置 Builder：链式覆盖字段后 [`NatsConfigBuilder::build`] 会执行完整校验。
#[derive(Debug, Clone)]
pub struct NatsConfigBuilder {
    config: NatsConfig,
}

impl Default for NatsConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl NatsConfigBuilder {
    /// 以 [`NatsConfig::default`] 为基线创建 Builder。
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: NatsConfig::default(),
        }
    }

    /// 以已有配置为基线创建 Builder。
    #[must_use]
    pub fn from_config(config: NatsConfig) -> Self {
        Self { config }
    }

    /// 设置服务器 URL。
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.config.url = url.into();
        self
    }

    /// 设置用户名与密码。
    #[must_use]
    pub fn credentials(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.user = Some(user.into());
        self.config.password = Some(password.into());
        self
    }

    /// 设置认证 token。
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = Some(token.into());
        self
    }

    /// 设置 NKey seed。
    #[must_use]
    pub fn nkey_seed(mut self, seed: impl Into<String>) -> Self {
        self.config.nkey_seed = Some(seed.into());
        self
    }

    /// 设置客户端名。
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.config.name = name.into();
        self
    }

    /// 设置连接超时。
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// 设置操作超时。
    #[must_use]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.config.operation_timeout = timeout;
        self
    }

    /// 设置慢消费者判定超时（订阅转发任务等待下游接收单条消息的上限）。
    ///
    /// 与 [`NatsConfigBuilder::operation_timeout`]（服务端操作截止时间）语义不同；
    /// 未设置时回退 `operation_timeout`。零值会被 `validate` 拒绝。
    #[must_use]
    pub fn slow_consumer_timeout(mut self, timeout: Duration) -> Self {
        self.config.slow_consumer_timeout = Some(timeout);
        self
    }

    /// 设置 TLS 策略。
    #[must_use]
    pub fn tls_policy(mut self, policy: TlsPolicy) -> Self {
        self.config.tls_policy = Some(policy);
        self
    }

    /// 设置遗留 TLS 布尔开关。
    #[must_use]
    pub fn tls(mut self, enabled: bool) -> Self {
        self.config.tls = enabled;
        self
    }

    /// 设置 JetStream 期望标志。
    #[must_use]
    pub fn jetstream(mut self, enabled: bool) -> Self {
        self.config.jetstream = enabled;
        self
    }

    /// 设置订阅缓冲上限。
    #[must_use]
    pub fn subscription_capacity(mut self, capacity: usize) -> Self {
        self.config.subscription_capacity = capacity;
        self
    }

    /// 设置驱动命令队列容量。
    #[must_use]
    pub fn client_capacity(mut self, capacity: usize) -> Self {
        self.config.client_capacity = capacity;
        self
    }

    /// 设置最大重连次数。
    #[must_use]
    pub fn max_reconnects(mut self, max_reconnects: usize) -> Self {
        self.config.max_reconnects = max_reconnects;
        self
    }

    /// 设置单次重连退避上限。
    #[must_use]
    pub fn reconnect_max_delay(mut self, delay: Duration) -> Self {
        self.config.reconnect_max_delay = delay;
        self
    }

    /// 设置是否忽略服务端发现的地址。
    #[must_use]
    pub fn ignore_discovered_servers(mut self, ignore: bool) -> Self {
        self.config.ignore_discovered_servers = ignore;
        self
    }

    /// 设置 CA bundle 路径。
    #[must_use]
    pub fn tls_ca_file(mut self, path: impl Into<String>) -> Self {
        self.config.tls_ca_file = Some(path.into());
        self
    }

    /// 设置 mTLS 证书与私钥路径。
    #[must_use]
    pub fn tls_client_identity(
        mut self,
        cert_file: impl Into<String>,
        key_file: impl Into<String>,
    ) -> Self {
        self.config.tls_cert_file = Some(cert_file.into());
        self.config.tls_key_file = Some(key_file.into());
        self
    }

    /// 完成构建并执行 [`NatsConfig::validate`]。
    ///
    /// # Errors
    ///
    /// 配置不合法时返回 [`NatsError::Config`]。
    pub fn build(self) -> NatsResult<NatsConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}
