//! natsx 错误类型与 `async-nats` 错误映射。
//!
//! 设计要点：
//!
//! - 统一为 7 个稳定分类，调用方无需感知 `async-nats` 内部错误枚举；
//! - [`NatsError::is_retryable`] 只对**可安全重试的瞬时错误**返回 `true`，
//!   认证/授权/协议/配置类错误一律返回 `false`（重试无意义，需要修正输入或凭据）。

use std::fmt;

/// crate 专用结果别名。
pub type NatsResult<T> = Result<T, NatsError>;

/// natsx 错误类型。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NatsError {
    /// 配置非法（含认证材料非法、URL 非法、TLS 策略与地址不匹配等）。
    #[error("配置无效: {0}")]
    Config(String),
    /// 连接建立/维护失败（DNS、IO、重连耗尽等）。
    #[error("连接失败: {0}")]
    Connection(String),
    /// 远端返回业务/协议错误（broker 拒绝、ack 失败、JetStream 元数据缺失等）。
    #[error("远端返回错误: {0}")]
    Backend(String),
    /// 序列化或解析失败。
    #[error("序列化失败: {0}")]
    Serialization(String),
    /// 网络或 I/O 失败。
    #[error("I/O 失败: {0}")]
    Io(#[from] std::io::Error),
    /// 操作超时。
    #[error("操作超时: {0}")]
    Timeout(String),
    /// 当前能力不支持。
    #[error("不支持的操作: {0}")]
    Unsupported(String),
}

impl NatsError {
    /// 构造配置错误。
    #[must_use]
    pub fn config(message: impl fmt::Display) -> Self {
        Self::Config(message.to_string())
    }

    /// 构造连接错误。
    #[must_use]
    pub fn connection(message: impl fmt::Display) -> Self {
        Self::Connection(message.to_string())
    }

    /// 构造远端错误。
    #[must_use]
    pub fn backend(message: impl fmt::Display) -> Self {
        Self::Backend(message.to_string())
    }

    /// 构造序列化错误。
    #[must_use]
    pub fn serialization(message: impl fmt::Display) -> Self {
        Self::Serialization(message.to_string())
    }

    /// 构造超时错误。
    #[must_use]
    pub fn timeout(message: impl fmt::Display) -> Self {
        Self::Timeout(message.to_string())
    }

    /// 构造“不支持”错误。
    #[must_use]
    pub fn unsupported(message: impl fmt::Display) -> Self {
        Self::Unsupported(message.to_string())
    }

    /// 是否属于可安全重试的瞬时错误。
    ///
    /// - `Connection` / `Timeout` / `Io`：网络抖动、对端暂时不可达、超时与 IO 中断，
    ///   保持相同语义重试可能成功 → `true`；
    /// - `Config`：认证失败、授权被拒、URL 非法等，重试必然重复失败 → `false`；
    /// - `Backend`：broker 协议/业务拒绝，需修正请求或数据 → `false`；
    /// - `Serialization` / `Unsupported`：确定性失败 → `false`。
    ///
    /// 该判定与源工程的错误分类语义保持一致：只有瞬时类错误进入自动重试通道，
    /// 认证与协议类错误必须显式修正，不允许被重试策略掩盖。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Connection(_) | Self::Timeout(_) | Self::Io(_))
    }

    /// 分类名称，便于日志与指标打标。
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Connection(_) => "connection",
            Self::Backend(_) => "backend",
            Self::Serialization(_) => "serialization",
            Self::Io(_) => "io",
            Self::Timeout(_) => "timeout",
            Self::Unsupported(_) => "unsupported",
        }
    }
}

/// 把 `async-nats` 的连接错误映射为 [`NatsError`]。
///
/// 认证 / 授权 / TLS 材料问题归类为 [`NatsError::Config`]（不可重试），
/// DNS / IO / 重连耗尽归类为 [`NatsError::Connection`]（可重试），
/// 连接超时归类为 [`NatsError::Timeout`]（可重试）。
pub(crate) fn map_connect_error(error: &async_nats::ConnectError) -> NatsError {
    use async_nats::ConnectErrorKind as Kind;

    let detail = error.to_string();
    match error.kind() {
        Kind::TimedOut => NatsError::timeout(format!("连接 NATS 超时: {detail}")),
        Kind::Authentication | Kind::AuthorizationViolation | Kind::Tls | Kind::ServerParse => {
            NatsError::config(format!("连接 NATS 被拒绝（认证或 TLS 配置问题）: {detail}"))
        }
        Kind::Dns | Kind::Io | Kind::MaxReconnects => {
            NatsError::connection(format!("连接 NATS 失败: {detail}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification_is_conservative() {
        assert!(NatsError::connection("x").is_retryable());
        assert!(NatsError::timeout("x").is_retryable());
        assert!(NatsError::Io(std::io::Error::other("x")).is_retryable());

        assert!(!NatsError::config("x").is_retryable());
        assert!(!NatsError::backend("x").is_retryable());
        assert!(!NatsError::serialization("x").is_retryable());
        assert!(!NatsError::unsupported("x").is_retryable());
    }

    #[test]
    fn kind_names_are_stable() {
        assert_eq!(NatsError::config("x").kind_name(), "config");
        assert_eq!(NatsError::connection("x").kind_name(), "connection");
        assert_eq!(NatsError::backend("x").kind_name(), "backend");
        assert_eq!(NatsError::serialization("x").kind_name(), "serialization");
        assert_eq!(NatsError::Io(std::io::Error::other("x")).kind_name(), "io");
        assert_eq!(NatsError::timeout("x").kind_name(), "timeout");
        assert_eq!(NatsError::unsupported("x").kind_name(), "unsupported");
    }

    #[test]
    fn display_contains_message() {
        let text = NatsError::config("url 不能为空").to_string();
        assert!(text.contains("配置无效"));
        assert!(text.contains("url 不能为空"));
    }

    #[test]
    fn io_error_converts_via_from() {
        let error: NatsError = std::io::Error::new(std::io::ErrorKind::NotFound, "missing").into();
        assert!(matches!(error, NatsError::Io(_)));
    }
}
