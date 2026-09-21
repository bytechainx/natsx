//! NATS 客户端配置：TLS 策略、环境变量加载、TOML 解析与 Builder。
//!
//! # 加载优先级
//!
//! 显式字段 / TOML（[`NatsConfig::from_toml`]）< 环境变量（[`NatsConfig::from_env`] 覆盖）。
//!
//! # 环境变量
//!
//! 规范前缀为 `FOUNDATIONX_NATSX_*`，并兼容历史前缀 `FOUNDATIONX_NATS_*`
//! （前者优先）。所有 `ENV_*` 常量在下方公开，便于部署侧核对。
//!
//! # TLS 默认策略
//!
//! 1. 显式 [`TlsPolicy`] 优先；
//! 2. 否则 `tls == true` → [`TlsPolicy::Require`]；
//! 3. 否则按 URL host 自动判定：
//!    - loopback（`127.0.0.1` / `localhost` / `::1`）→ [`TlsPolicy::Prefer`]（允许明文）
//!    - 非 loopback → [`TlsPolicy::Require`]
//!
//! # 敏感字段
//!
//! `password` / `token` / `nkey_seed` 不进入 [`std::fmt::Debug`] 输出（统一渲染为 `***`），
//! URL 中的内嵌 userinfo 也会被脱敏；`validate()` 另外禁止 URL 内嵌凭据。

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_nats::rustls::pki_types::pem::PemObject;
use async_nats::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use async_nats::rustls::{ClientConfig, RootCertStore};
use serde::Deserialize;

use crate::error::{NatsError, NatsResult};

/// 配置 schema 版本；TOML 中 `schema_version` 必须等于该值。
pub const SCHEMA_VERSION: u32 = 1;

/// 默认 URL（无认证；生产凭据必须经环境注入）。
pub const DEFAULT_URL: &str = "nats://127.0.0.1:4222";

/// 默认客户端名。
pub const DEFAULT_CLIENT_NAME: &str = "natsx";

/// 规范环境变量前缀。
pub const ENV_PREFIX: &str = "FOUNDATIONX_NATSX_";

/// 兼容的历史环境变量前缀（优先级低于 [`ENV_PREFIX`]）。
pub const ENV_LEGACY_PREFIX: &str = "FOUNDATIONX_NATS_";

/// `FOUNDATIONX_NATSX_URL`：服务器地址。
pub const ENV_URL: &str = "FOUNDATIONX_NATSX_URL";
/// `FOUNDATIONX_NATSX_SERVERS`：逗号分隔的服务器列表（取首个作为主地址）。
pub const ENV_SERVERS: &str = "FOUNDATIONX_NATSX_SERVERS";
/// `FOUNDATIONX_NATSX_USER`：用户名。
pub const ENV_USER: &str = "FOUNDATIONX_NATSX_USER";
/// `FOUNDATIONX_NATSX_USERNAME`：用户名（别名）。
pub const ENV_USERNAME: &str = "FOUNDATIONX_NATSX_USERNAME";
/// `FOUNDATIONX_NATSX_PASSWORD`：密码（敏感）。
pub const ENV_PASSWORD: &str = "FOUNDATIONX_NATSX_PASSWORD";
/// `FOUNDATIONX_NATSX_TOKEN`：认证 token（敏感）。
pub const ENV_TOKEN: &str = "FOUNDATIONX_NATSX_TOKEN";
/// `FOUNDATIONX_NATSX_NKEY_SEED`：NKey seed（敏感）。
pub const ENV_NKEY_SEED: &str = "FOUNDATIONX_NATSX_NKEY_SEED";
/// `FOUNDATIONX_NATSX_NAME`：客户端名。
pub const ENV_NAME: &str = "FOUNDATIONX_NATSX_NAME";
/// `FOUNDATIONX_NATSX_TLS`：遗留 TLS 布尔开关。
pub const ENV_TLS: &str = "FOUNDATIONX_NATSX_TLS";
/// `FOUNDATIONX_NATSX_TLS_POLICY`：TLS 策略（disable / prefer / require）。
pub const ENV_TLS_POLICY: &str = "FOUNDATIONX_NATSX_TLS_POLICY";
/// `FOUNDATIONX_NATSX_TLS_CA_FILE`：CA bundle（PEM）。
pub const ENV_TLS_CA_FILE: &str = "FOUNDATIONX_NATSX_TLS_CA_FILE";
/// `FOUNDATIONX_NATSX_TLS_CERT_FILE`：mTLS 客户端证书（PEM）。
pub const ENV_TLS_CERT_FILE: &str = "FOUNDATIONX_NATSX_TLS_CERT_FILE";
/// `FOUNDATIONX_NATSX_TLS_KEY_FILE`：mTLS 客户端私钥（PEM）。
pub const ENV_TLS_KEY_FILE: &str = "FOUNDATIONX_NATSX_TLS_KEY_FILE";
/// `FOUNDATIONX_NATSX_JETSTREAM`：是否期望使用 JetStream。
pub const ENV_JETSTREAM: &str = "FOUNDATIONX_NATSX_JETSTREAM";
/// `FOUNDATIONX_NATSX_CONNECT_TIMEOUT_MS`：连接超时（毫秒）。
pub const ENV_CONNECT_TIMEOUT_MS: &str = "FOUNDATIONX_NATSX_CONNECT_TIMEOUT_MS";
/// `FOUNDATIONX_NATSX_OPERATION_TIMEOUT_MS`：操作超时（毫秒）。
pub const ENV_OPERATION_TIMEOUT_MS: &str = "FOUNDATIONX_NATSX_OPERATION_TIMEOUT_MS";
/// `FOUNDATIONX_NATSX_SUBSCRIPTION_CAPACITY`：单订阅缓冲上限。
pub const ENV_SUBSCRIPTION_CAPACITY: &str = "FOUNDATIONX_NATSX_SUBSCRIPTION_CAPACITY";
/// `FOUNDATIONX_NATSX_CLIENT_CAPACITY`：驱动命令队列容量。
pub const ENV_CLIENT_CAPACITY: &str = "FOUNDATIONX_NATSX_CLIENT_CAPACITY";
/// `FOUNDATIONX_NATSX_MAX_RECONNECTS`：最大重连次数。
pub const ENV_MAX_RECONNECTS: &str = "FOUNDATIONX_NATSX_MAX_RECONNECTS";
/// `FOUNDATIONX_NATSX_RECONNECT_MAX_DELAY_MS`：单次重连退避上限（毫秒）。
pub const ENV_RECONNECT_MAX_DELAY_MS: &str = "FOUNDATIONX_NATSX_RECONNECT_MAX_DELAY_MS";
/// `FOUNDATIONX_NATSX_IGNORE_DISCOVERED_SERVERS`：是否忽略服务端发现的地址。
pub const ENV_IGNORE_DISCOVERED_SERVERS: &str = "FOUNDATIONX_NATSX_IGNORE_DISCOVERED_SERVERS";

/// TLS 策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsPolicy {
    /// 不主动要求 TLS（允许明文）；仅建议用于 loopback。
    Disable,
    /// 优先 TLS、允许明文；仅建议用于 loopback。
    #[default]
    Prefer,
    /// 必须 TLS；连接层设置 `require_tls(true)`，握手失败即连接失败。
    Require,
}

impl TlsPolicy {
    /// 解析策略字符串（大小写不敏感）。
    ///
    /// # Errors
    ///
    /// 字符串不属于 `disable|prefer|require` 及其别名时返回 [`NatsError::Config`]。
    pub fn parse(raw: &str) -> NatsResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disable" | "disabled" | "off" | "false" | "0" | "none" => Ok(Self::Disable),
            "prefer" | "optional" | "auto" => Ok(Self::Prefer),
            "require" | "required" | "on" | "true" | "1" | "mandatory" => Ok(Self::Require),
            other => Err(NatsError::config(format!(
                "未知 TLS 策略 {other:?}（期望 disable|prefer|require）"
            ))),
        }
    }

    /// 是否在连接选项上设置 `require_tls(true)`。
    #[must_use]
    pub fn require_tls(self) -> bool {
        matches!(self, Self::Require)
    }
}

impl fmt::Display for TlsPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disable => write!(f, "disable"),
            Self::Prefer => write!(f, "prefer"),
            Self::Require => write!(f, "require"),
        }
    }
}

impl<'de> Deserialize<'de> for TlsPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// 判断 NATS URL 是否指向 loopback（`127.0.0.1` / `localhost` / `::1`）。
///
/// 支持 `scheme://host:port`、`[::1]:port`、带用户信息与多地址逗号分隔等写法。
#[must_use]
pub fn url_is_loopback(url: &str) -> bool {
    matches!(
        extract_host(url).to_ascii_lowercase().as_str(),
        "127.0.0.1" | "localhost" | "::1" | "[::1]"
    )
}

fn extract_host(url: &str) -> String {
    let trimmed = url.trim();
    let without_scheme = if let Some(index) = trimmed.find("://") {
        &trimmed[index + 3..]
    } else {
        trimmed
    };
    // 多地址（逗号分隔）只看第一个
    let single = without_scheme.split(',').next().unwrap_or(without_scheme);
    // 去掉 userinfo@
    let after_user = single.rsplit('@').next().unwrap_or(single);
    // [ipv6]:port
    if let Some(rest) = after_user.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return format!("[{}]", &rest[..end]);
        }
    }
    let host_port = after_user.split('/').next().unwrap_or(after_user);
    host_port
        .split(':')
        .next()
        .unwrap_or(host_port)
        .trim()
        .to_string()
}

/// NATS 客户端配置。
///
/// 可直接反序列化 TOML（`schema_version` + 扁平字段），
/// 但**敏感字段**（`password` / `token` / `nkey_seed`）不允许出现在 TOML 中，
/// 只能经环境变量或 [`NatsConfigBuilder`] 注入。
#[derive(Clone)]
pub struct NatsConfig {
    /// 服务器 URL。
    pub url: String,
    /// 用户名。
    pub user: Option<String>,
    /// 密码（敏感，不进入 `Debug`）。
    password: Option<String>,
    /// 认证 token（敏感，不进入 `Debug`）。
    token: Option<String>,
    /// NKey seed（敏感，不进入 `Debug`）。
    nkey_seed: Option<String>,
    /// 连接超时。
    pub connect_timeout: Duration,
    /// Core NATS / JetStream 操作截止时间。
    pub operation_timeout: Duration,
    /// 客户端名。
    pub name: String,
    /// 遗留 TLS 布尔开关（`true` 等价于 [`TlsPolicy::Require`]）。
    pub tls: bool,
    /// 显式 TLS 策略；`None` 时按 host 自动解析。
    pub tls_policy: Option<TlsPolicy>,
    /// 是否期望使用 JetStream（文档/校验标志，不影响 Core NATS 连接）。
    pub jetstream: bool,
    /// 每订阅缓冲上限。
    pub subscription_capacity: usize,
    /// 驱动命令发送队列容量。
    pub client_capacity: usize,
    /// 连续重连最大尝试次数（必须为有限正数）。
    pub max_reconnects: usize,
    /// 单次重连退避上限。
    pub reconnect_max_delay: Duration,
    /// 是否忽略服务端发现的地址，仅重连显式 URL。
    pub ignore_discovered_servers: bool,
    /// CA bundle 文件路径（PEM）。
    pub tls_ca_file: Option<String>,
    /// mTLS 客户端证书路径（PEM）。
    pub tls_cert_file: Option<String>,
    /// mTLS 客户端私钥路径（PEM）。
    pub tls_key_file: Option<String>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_string(),
            // 无默认账号：避免把草稿/过期凭据写进库；由 FOUNDATIONX_NATSX_* 注入
            user: None,
            password: None,
            token: None,
            nkey_seed: None,
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(5),
            name: DEFAULT_CLIENT_NAME.to_string(),
            tls: false,
            tls_policy: None,
            jetstream: false,
            subscription_capacity: 256,
            client_capacity: 256,
            max_reconnects: 60,
            reconnect_max_delay: Duration::from_secs(5),
            ignore_discovered_servers: false,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
        }
    }
}

impl fmt::Debug for NatsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsConfig")
            .field("url", &redact_url(&self.url))
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("nkey_seed", &self.nkey_seed.as_ref().map(|_| "***"))
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("name", &self.name)
            .field("tls", &self.tls)
            .field("tls_policy", &self.tls_policy)
            .field("jetstream", &self.jetstream)
            .field("subscription_capacity", &self.subscription_capacity)
            .field("client_capacity", &self.client_capacity)
            .field("max_reconnects", &self.max_reconnects)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("ignore_discovered_servers", &self.ignore_discovered_servers)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("tls_cert_file", &self.tls_cert_file)
            .field("tls_key_file", &self.tls_key_file)
            .finish()
    }
}

fn redact_url(raw: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(raw) else {
        return "<invalid-url>".to_string();
    };
    if !parsed.username().is_empty() {
        let _ = parsed.set_username("***");
    }
    if parsed.password().is_some() {
        let _ = parsed.set_password(Some("***"));
    }
    parsed.to_string()
}

impl NatsConfig {
    /// 创建 Builder（以 [`NatsConfig::default`] 为基线）。
    #[must_use]
    pub fn builder() -> NatsConfigBuilder {
        NatsConfigBuilder::new()
    }

    /// 密码（敏感）。
    #[must_use]
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// 认证 token（敏感）。
    #[must_use]
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// NKey seed（敏感）。
    #[must_use]
    pub fn nkey_seed(&self) -> Option<&str> {
        self.nkey_seed.as_deref()
    }

    /// 用户名 + 密码（两者同时存在时返回）。
    #[must_use]
    pub fn user_password(&self) -> Option<(&str, &str)> {
        match (self.user.as_deref(), self.password.as_deref()) {
            (Some(user), Some(password)) => Some((user, password)),
            _ => None,
        }
    }

    /// 从环境变量加载：以 [`NatsConfig::default`] 为基线，再应用 `FOUNDATIONX_NATSX_*`
    /// （兼容 `FOUNDATIONX_NATS_*`）覆盖，最后 [`NatsConfig::validate`]。
    ///
    /// # Errors
    ///
    /// 环境变量取值非法或最终配置不满足 [`NatsConfig::validate`] 时返回错误。
    pub fn from_env() -> NatsResult<Self> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// 从 TOML 字符串解析配置。
    ///
    /// 期望根结构为 `schema_version = 1` 加扁平字段；
    /// `password` / `token` / `nkey_seed` / `jwt` 等敏感字段**禁止**出现在 TOML 中。
    ///
    /// # Errors
    ///
    /// TOML 非法、schema_version 不匹配、出现敏感字段或校验失败时返回错误。
    pub fn from_toml(text: &str) -> NatsResult<Self> {
        reject_secret_keys(text)?;
        let config: Self = toml::from_str(text)
            .map_err(|error| NatsError::serialization(format!("TOML 解析失败: {error}")))?;
        config.validate()?;
        Ok(config)
    }

    /// 生效的 TLS 策略（显式 > `tls` 布尔 > host 自动判定）。
    #[must_use]
    pub fn effective_tls_policy(&self) -> TlsPolicy {
        if let Some(policy) = self.tls_policy {
            return policy;
        }
        if self.tls {
            return TlsPolicy::Require;
        }
        if url_is_loopback(&self.url) {
            TlsPolicy::Prefer
        } else {
            TlsPolicy::Require
        }
    }

    /// URL 是否声明了 TLS scheme（`tls://` / `nats+tls://` / `wss://`）。
    #[must_use]
    pub fn url_implies_tls(&self) -> bool {
        let lower = self.url.trim().to_ascii_lowercase();
        lower.starts_with("tls://")
            || lower.starts_with("nats+tls://")
            || lower.starts_with("wss://")
    }

    /// 校验配置合法性（fail-closed，不发网络请求）。
    ///
    /// 规则：
    /// - URL 非空、可被 `url` 解析、且不得内嵌 userinfo；
    /// - `user` / `password` 必须同时提供或同时缺省；
    /// - `token` 与 `user/password`、NKey seed 互斥；NKey seed 与 `user/password` 互斥；
    /// - 非 loopback 地址必须使用 [`TlsPolicy::Require`]；
    /// - `tls_cert_file` / `tls_key_file` 必须成对出现且文件存在；
    /// - `tls_ca_file` 存在时必须可访问；
    /// - 超时、容量、最大重连数必须为正。
    ///
    /// # Errors
    ///
    /// 任一条不满足时返回 [`NatsError::Config`]。
    pub fn validate(&self) -> NatsResult<()> {
        if self.url.trim().is_empty() {
            return Err(NatsError::config("url 不能为空"));
        }
        let parsed = url::Url::parse(self.url.trim())
            .map_err(|error| NatsError::config(format!("URL 非法: {error}")))?;
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(NatsError::config(
                "URL 禁止内嵌 userinfo；请使用独立 user/password 字段",
            ));
        }

        match (&self.user, &self.password) {
            (Some(user), Some(password)) if !user.is_empty() && !password.is_empty() => {}
            (None, None) => {}
            _ => {
                return Err(NatsError::config("user/password 必须同时提供或同时缺省"));
            }
        }
        if self
            .token
            .as_ref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(NatsError::config("token 不能为空字符串"));
        }
        let has_user_password = self.user.is_some() || self.password.is_some();
        if self.token.is_some() && (has_user_password || self.nkey_seed.is_some()) {
            return Err(NatsError::config(
                "token 与 user/password、NKey seed 互斥，只能二选一",
            ));
        }
        if self
            .nkey_seed
            .as_ref()
            .is_some_and(|seed| seed.trim().is_empty())
        {
            return Err(NatsError::config("NKey seed 不能为空字符串"));
        }
        if self.nkey_seed.is_some() && has_user_password {
            return Err(NatsError::config(
                "NKey seed 与 user/password 互斥，只能二选一",
            ));
        }

        let policy = self.effective_tls_policy();
        if !url_is_loopback(&self.url) && policy != TlsPolicy::Require {
            return Err(NatsError::config(format!(
                "远程服务必须使用 require TLS 策略（当前为 {policy}）"
            )));
        }

        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.reconnect_max_delay.is_zero()
        {
            return Err(NatsError::config(
                "connect/operation/reconnect 超时必须大于零",
            ));
        }
        if self.subscription_capacity == 0 || self.client_capacity == 0 {
            return Err(NatsError::config("subscription/client capacity 必须大于零"));
        }
        if self.max_reconnects == 0 {
            return Err(NatsError::config("max_reconnects 必须为有限正数"));
        }

        match (&self.tls_cert_file, &self.tls_key_file) {
            (Some(cert), Some(key)) => {
                if !(Path::new(cert).is_file() && Path::new(key).is_file()) {
                    return Err(NatsError::config("TLS cert/key 文件路径不存在或不可访问"));
                }
                if policy == TlsPolicy::Disable {
                    return Err(NatsError::config("TLS 证书已配置，但 TLS 策略为 disable"));
                }
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(NatsError::config("TLS cert 文件需要 key 文件同时提供"));
            }
            (None, Some(_)) => {
                return Err(NatsError::config("TLS key 文件需要 cert 文件同时提供"));
            }
        }
        if let Some(ca) = &self.tls_ca_file {
            if !Path::new(ca).is_file() {
                return Err(NatsError::config("TLS CA 文件路径不存在或不可访问"));
            }
        }
        Ok(())
    }

    /// 把 TLS 策略与证书材料落到 `async-nats` 连接选项上。
    ///
    /// - 始终设置 `require_tls(policy.require_tls())`；
    /// - 配置了自定义 CA 时，用 `tls_client_config` 传入自建 `rustls::ClientConfig`
    ///   （根证书集合 = 该 CA bundle，不再叠加系统根证书）；
    /// - 仅配置 mTLS 证书时，使用 `add_client_certificate`（保留系统根证书）。
    pub(crate) fn apply_tls(
        &self,
        options: async_nats::ConnectOptions,
    ) -> NatsResult<async_nats::ConnectOptions> {
        let policy = self.effective_tls_policy();
        let options = options.require_tls(policy.require_tls());
        match (&self.tls_ca_file, &self.tls_cert_file, &self.tls_key_file) {
            (Some(ca), cert, key) => {
                let identity = match (cert, key) {
                    (Some(cert), Some(key)) => Some((cert.as_str(), key.as_str())),
                    _ => None,
                };
                let config = build_tls_client_config(ca, identity)?;
                Ok(options.tls_client_config(config))
            }
            (None, Some(cert), Some(key)) => {
                Ok(options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key)))
            }
            _ => Ok(options),
        }
    }

    /// 环境变量覆盖（规范前缀优先，兼容历史前缀）。
    fn apply_env_overrides(&mut self) -> NatsResult<()> {
        if let Some((_, value)) = lookup_env("URL") {
            self.url = value;
        } else if let Some((_, value)) = lookup_env("SERVERS") {
            if let Some(first) = value.split(',').next() {
                self.url = first.trim().to_string();
            }
        }
        if let Some((_, value)) = lookup_env("USER") {
            self.user = Some(value);
        } else if let Some((_, value)) = lookup_env("USERNAME") {
            self.user = Some(value);
        }
        if let Some((_, value)) = lookup_env("PASSWORD") {
            self.password = Some(value);
        }
        if let Some((_, value)) = lookup_env("TOKEN") {
            self.token = Some(value);
        }
        if let Some((_, value)) = lookup_env("NKEY_SEED") {
            self.nkey_seed = Some(value);
        }
        if let Some((_, value)) = lookup_env("NAME") {
            self.name = value;
        }
        if let Some((key, value)) = lookup_env("TLS") {
            self.tls = parse_bool(&value, &key)?;
        }
        if let Some((_, value)) = lookup_env("TLS_POLICY") {
            self.tls_policy = Some(TlsPolicy::parse(&value)?);
        }
        if let Some((key, value)) = lookup_env("JETSTREAM") {
            self.jetstream = parse_bool(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("IGNORE_DISCOVERED_SERVERS") {
            self.ignore_discovered_servers = parse_bool(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("CONNECT_TIMEOUT_MS") {
            self.connect_timeout = parse_millis(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("OPERATION_TIMEOUT_MS") {
            self.operation_timeout = parse_millis(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("RECONNECT_MAX_DELAY_MS") {
            self.reconnect_max_delay = parse_millis(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("SUBSCRIPTION_CAPACITY") {
            self.subscription_capacity = parse_usize(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("CLIENT_CAPACITY") {
            self.client_capacity = parse_usize(&value, &key)?;
        }
        if let Some((key, value)) = lookup_env("MAX_RECONNECTS") {
            self.max_reconnects = parse_usize(&value, &key)?;
        }
        if let Some((_, value)) = lookup_env("TLS_CA_FILE") {
            self.tls_ca_file = Some(value);
        }
        if let Some((_, value)) = lookup_env("TLS_CERT_FILE") {
            self.tls_cert_file = Some(value);
        }
        if let Some((_, value)) = lookup_env("TLS_KEY_FILE") {
            self.tls_key_file = Some(value);
        }
        Ok(())
    }
}

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

/// serde 反序列化中间表示：把 `*_ms` 字段转换为 [`Duration`]。
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NatsConfigRepr {
    schema_version: u32,
    url: String,
    user: Option<String>,
    name: String,
    tls: bool,
    tls_policy: Option<TlsPolicy>,
    jetstream: bool,
    connect_timeout_ms: u64,
    operation_timeout_ms: u64,
    subscription_capacity: usize,
    client_capacity: usize,
    max_reconnects: usize,
    reconnect_max_delay_ms: u64,
    ignore_discovered_servers: bool,
    tls_ca_file: Option<String>,
    tls_cert_file: Option<String>,
    tls_key_file: Option<String>,
}

impl Default for NatsConfigRepr {
    fn default() -> Self {
        let base = NatsConfig::default();
        Self {
            schema_version: SCHEMA_VERSION,
            url: base.url,
            user: base.user,
            name: base.name,
            tls: base.tls,
            tls_policy: base.tls_policy,
            jetstream: base.jetstream,
            connect_timeout_ms: duration_to_millis(base.connect_timeout),
            operation_timeout_ms: duration_to_millis(base.operation_timeout),
            subscription_capacity: base.subscription_capacity,
            client_capacity: base.client_capacity,
            max_reconnects: base.max_reconnects,
            reconnect_max_delay_ms: duration_to_millis(base.reconnect_max_delay),
            ignore_discovered_servers: base.ignore_discovered_servers,
            tls_ca_file: base.tls_ca_file,
            tls_cert_file: base.tls_cert_file,
            tls_key_file: base.tls_key_file,
        }
    }
}

impl NatsConfigRepr {
    fn into_config(self) -> NatsResult<NatsConfig> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(NatsError::config(format!(
                "schema_version 不支持: {}（期望 {SCHEMA_VERSION}）",
                self.schema_version
            )));
        }
        Ok(NatsConfig {
            url: self.url,
            user: self.user,
            // 敏感字段只能经环境变量或 Builder 注入，不参与反序列化
            password: None,
            token: None,
            nkey_seed: None,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            operation_timeout: Duration::from_millis(self.operation_timeout_ms),
            name: self.name,
            tls: self.tls,
            tls_policy: self.tls_policy,
            jetstream: self.jetstream,
            subscription_capacity: self.subscription_capacity,
            client_capacity: self.client_capacity,
            max_reconnects: self.max_reconnects,
            reconnect_max_delay: Duration::from_millis(self.reconnect_max_delay_ms),
            ignore_discovered_servers: self.ignore_discovered_servers,
            tls_ca_file: self.tls_ca_file,
            tls_cert_file: self.tls_cert_file,
            tls_key_file: self.tls_key_file,
        })
    }
}

impl<'de> Deserialize<'de> for NatsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        NatsConfigRepr::deserialize(deserializer)?
            .into_config()
            .map_err(serde::de::Error::custom)
    }
}

/// 特征化敏感字段：TOML 中出现即拒绝，避免凭据落盘。
fn reject_secret_keys(text: &str) -> NatsResult<()> {
    let value: toml::Value = toml::from_str(text)
        .map_err(|error| NatsError::serialization(format!("TOML 解析失败: {error}")))?;
    let table = value
        .as_table()
        .ok_or_else(|| NatsError::config("TOML 根必须为表（key = value 结构）"))?;
    for key in ["password", "token", "nkey_seed", "jwt"] {
        if table.contains_key(key) {
            return Err(NatsError::config(format!(
                "TOML 禁止字段 {key}：敏感凭据必须经环境变量或 Builder 注入"
            )));
        }
    }
    Ok(())
}

/// 读取环境变量：规范前缀优先，其次兼容前缀；空白值视为未设置。
fn lookup_env(suffix: &str) -> Option<(String, String)> {
    for key in [
        format!("{ENV_PREFIX}{suffix}"),
        format!("{ENV_LEGACY_PREFIX}{suffix}"),
    ] {
        if let Ok(value) = std::env::var(&key) {
            if !value.trim().is_empty() {
                return Some((key, value));
            }
        }
    }
    None
}

fn parse_bool(value: &str, name: &str) -> NatsResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(NatsError::config(format!("{name} 非法（布尔值）: {other}"))),
    }
}

fn parse_usize(value: &str, name: &str) -> NatsResult<usize> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|error| NatsError::config(format!("{name} 非法（无符号整数）: {error}")))
}

fn parse_millis(value: &str, name: &str) -> NatsResult<Duration> {
    value
        .trim()
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|error| NatsError::config(format!("{name} 非法（毫秒整数）: {error}")))
}

fn duration_to_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}

/// 用自定义 CA（可选 mTLS 身份）构造 rustls 客户端配置。
fn build_tls_client_config(
    ca_file: &str,
    identity: Option<(&str, &str)>,
) -> NatsResult<ClientConfig> {
    let ca_pem = std::fs::read(ca_file)
        .map_err(|error| NatsError::config(format!("读取 TLS CA 文件失败 {ca_file}: {error}")))?;
    let certificates = CertificateDer::pem_slice_iter(&ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| NatsError::config(format!("解析 TLS CA 文件失败 {ca_file}: {error}")))?;
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(certificates);
    if added == 0 {
        return Err(NatsError::config(format!(
            "TLS CA 文件未包含可用证书: {ca_file}"
        )));
    }
    if ignored > 0 {
        tracing::debug!(ca_file, ignored, "TLS CA 文件中存在无法解析的证书条目");
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    match identity {
        Some((cert_file, key_file)) => {
            let cert_pem = std::fs::read(cert_file).map_err(|error| {
                NatsError::config(format!("读取 TLS 客户端证书失败 {cert_file}: {error}"))
            })?;
            let key_pem = std::fs::read(key_file).map_err(|error| {
                NatsError::config(format!("读取 TLS 客户端私钥失败 {key_file}: {error}"))
            })?;
            let chain = CertificateDer::pem_slice_iter(&cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    NatsError::config(format!("解析 TLS 客户端证书失败 {cert_file}: {error}"))
                })?;
            let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|error| {
                NatsError::config(format!("解析 TLS 客户端私钥失败 {key_file}: {error}"))
            })?;
            let config = builder.with_client_auth_cert(chain, key).map_err(|error| {
                NatsError::config(format!("TLS 客户端证书与私钥不匹配: {error}"))
            })?;
            Ok(config)
        }
        None => Ok(builder.with_no_client_auth()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 环境变量是进程级共享状态，读写 env 的用例必须串行执行。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 退出作用域（含 panic）时自动清理本次设置的环境变量。
    struct EnvScope {
        keys: Vec<String>,
    }

    impl EnvScope {
        fn new() -> Self {
            Self { keys: Vec::new() }
        }

        fn set(&mut self, key: &str, value: &str) {
            std::env::set_var(key, value);
            self.keys.push(key.to_string());
        }
    }

    impl Drop for EnvScope {
        fn drop(&mut self) {
            for key in &self.keys {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn defaults_are_loopback_prefer() {
        let config = NatsConfig::default();
        assert_eq!(config.url, DEFAULT_URL);
        assert!(config.user.is_none());
        assert!(config.password().is_none());
        assert!(config.token().is_none());
        assert!(config.nkey_seed().is_none());
        assert!(!config.tls);
        assert!(config.tls_policy.is_none());
        assert!(!config.jetstream);
        assert_eq!(config.effective_tls_policy(), TlsPolicy::Prefer);
        config.validate().expect("默认配置必须有效");
    }

    #[test]
    fn from_toml_parses_flat_fields() {
        let text = r#"
schema_version = 1
url = "nats://127.0.0.1:4223"
name = "writer"
jetstream = true
connect_timeout_ms = 8000
subscription_capacity = 128
"#;
        let config = NatsConfig::from_toml(text).expect("TOML 解析");
        assert_eq!(config.url, "nats://127.0.0.1:4223");
        assert_eq!(config.name, "writer");
        assert!(config.jetstream);
        assert_eq!(config.connect_timeout, Duration::from_millis(8000));
        assert_eq!(config.subscription_capacity, 128);
        assert!(config.password().is_none());
    }

    #[test]
    fn from_toml_rejects_secrets_schema_and_unknown_fields() {
        assert!(NatsConfig::from_toml("schema_version = 1\npassword = \"x\"\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 1\ntoken = \"x\"\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 1\nnkey_seed = \"x\"\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 99\n").is_err());
        assert!(NatsConfig::from_toml("schema_version = 1\nsink_id = \"x\"\n").is_err());
    }

    #[test]
    fn direct_serde_deserialization_uses_defaults() {
        let config: NatsConfig =
            toml::from_str("url = \"nats://127.0.0.1:4222\"").expect("反序列化");
        assert_eq!(config.name, DEFAULT_CLIENT_NAME);
        assert_eq!(config.operation_timeout, Duration::from_secs(5));
    }

    #[test]
    fn debug_redacts_all_sensitive_material() {
        let config = NatsConfig {
            url: "nats://embedded-user:embedded-secret@localhost:4222".into(),
            user: Some("embedded-user".into()),
            password: Some("super-secret-pass".into()),
            token: Some("super-secret-token".into()),
            nkey_seed: Some("SUACBSEED".into()),
            ..NatsConfig::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        for secret in [
            "super-secret-pass",
            "super-secret-token",
            "SUACBSEED",
            "embedded-secret",
        ] {
            assert!(!debug.contains(secret), "Debug 泄露了 {secret}");
        }
    }

    #[test]
    fn tls_policy_defaults_follow_host() {
        assert_eq!(
            NatsConfig::default().effective_tls_policy(),
            TlsPolicy::Prefer
        );

        let remote = NatsConfig {
            url: "nats://broker.example.com:4222".into(),
            ..NatsConfig::default()
        };
        assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);
        assert!(remote.validate().is_ok());
        assert!(!remote.url_implies_tls());

        let remote_disable = NatsConfig {
            url: "nats://broker.example.com:4222".into(),
            tls_policy: Some(TlsPolicy::Disable),
            ..NatsConfig::default()
        };
        assert!(remote_disable.validate().is_err());

        let remote_prefer = NatsConfig {
            url: "nats://broker.example.com:4222".into(),
            tls_policy: Some(TlsPolicy::Prefer),
            ..NatsConfig::default()
        };
        assert!(remote_prefer.validate().is_err());

        let tls_bool = NatsConfig {
            url: "nats://10.0.0.9:4222".into(),
            tls: true,
            ..NatsConfig::default()
        };
        assert_eq!(tls_bool.effective_tls_policy(), TlsPolicy::Require);
        assert!(tls_bool.validate().is_ok());
    }

    #[test]
    fn url_is_loopback_matrix() {
        assert!(url_is_loopback("nats://127.0.0.1:4222"));
        assert!(url_is_loopback("nats://localhost:4222"));
        assert!(url_is_loopback("nats://[::1]:4222"));
        assert!(url_is_loopback("tls://LOCALHOST:4222"));
        assert!(!url_is_loopback("nats://10.0.0.5:4222"));
        assert!(!url_is_loopback("tls://nats.prod.internal:4222"));
        assert!(!url_is_loopback("nats://0.0.0.0:4222"));
    }

    #[test]
    fn tls_policy_parse_aliases() {
        assert_eq!(
            TlsPolicy::parse("require").expect("require"),
            TlsPolicy::Require
        );
        assert_eq!(
            TlsPolicy::parse("REQUIRED").expect("REQUIRED"),
            TlsPolicy::Require
        );
        assert_eq!(TlsPolicy::parse("off").expect("off"), TlsPolicy::Disable);
        assert_eq!(TlsPolicy::parse(" auto ").expect("auto"), TlsPolicy::Prefer);
        assert!(TlsPolicy::parse("weird").is_err());
        assert!(TlsPolicy::Require.require_tls());
        assert!(!TlsPolicy::Prefer.require_tls());
        assert_eq!(TlsPolicy::Disable.to_string(), "disable");
    }

    #[test]
    fn validate_rejects_partial_auth_and_unbounded_resources() {
        let cases = [
            NatsConfig {
                url: "  ".into(),
                ..NatsConfig::default()
            },
            NatsConfig {
                url: "nats://user:pass@127.0.0.1:4222".into(),
                ..NatsConfig::default()
            },
            NatsConfig {
                user: Some("u".into()),
                ..NatsConfig::default()
            },
            NatsConfig {
                connect_timeout: Duration::ZERO,
                ..NatsConfig::default()
            },
            NatsConfig {
                operation_timeout: Duration::ZERO,
                ..NatsConfig::default()
            },
            NatsConfig {
                reconnect_max_delay: Duration::ZERO,
                ..NatsConfig::default()
            },
            NatsConfig {
                subscription_capacity: 0,
                ..NatsConfig::default()
            },
            NatsConfig {
                client_capacity: 0,
                ..NatsConfig::default()
            },
            NatsConfig {
                max_reconnects: 0,
                ..NatsConfig::default()
            },
        ];
        for config in cases {
            assert!(config.validate().is_err(), "必须拒绝非法配置: {config:?}");
        }
    }

    #[test]
    fn validate_rejects_conflicting_auth_material() {
        let both = NatsConfig {
            user: Some("u".into()),
            password: Some("p".into()),
            token: Some("t".into()),
            ..NatsConfig::default()
        };
        assert!(both.validate().is_err());

        let nkey_and_user = NatsConfig {
            user: Some("u".into()),
            password: Some("p".into()),
            nkey_seed: Some("SUACB".into()),
            ..NatsConfig::default()
        };
        assert!(nkey_and_user.validate().is_err());

        let token_only = NatsConfig {
            token: Some("t".into()),
            ..NatsConfig::default()
        };
        assert!(token_only.validate().is_ok());

        let nkey_only = NatsConfig {
            nkey_seed: Some("SUACB".into()),
            ..NatsConfig::default()
        };
        assert!(nkey_only.validate().is_ok());
    }

    #[test]
    fn validate_checks_tls_material_paths() {
        let cert_only = NatsConfig {
            tls_cert_file: Some("/nonexistent/cert.pem".into()),
            ..NatsConfig::default()
        };
        assert!(cert_only.validate().is_err());

        let key_only = NatsConfig {
            tls_key_file: Some("/nonexistent/key.pem".into()),
            ..NatsConfig::default()
        };
        assert!(key_only.validate().is_err());

        let ca_missing = NatsConfig {
            tls_ca_file: Some("/nonexistent/ca.pem".into()),
            ..NatsConfig::default()
        };
        assert!(ca_missing.validate().is_err());
    }

    #[test]
    fn builder_applies_overrides_and_validates() {
        let config = NatsConfig::builder()
            .url("nats://127.0.0.1:4222")
            .name("worker")
            .credentials("user", "pass")
            .jetstream(true)
            .operation_timeout(Duration::from_secs(3))
            .max_reconnects(7)
            .build()
            .expect("Builder 构建");
        assert_eq!(config.name, "worker");
        assert_eq!(config.operation_timeout, Duration::from_secs(3));
        assert_eq!(config.max_reconnects, 7);
        assert_eq!(config.user_password(), Some(("user", "pass")));

        // 远程地址 + 显式 Prefer 必须被拒绝
        assert!(NatsConfig::builder()
            .url("nats://broker.example.com:4222")
            .tls_policy(TlsPolicy::Prefer)
            .build()
            .is_err());
        // 远程地址未显式指定策略时自动 Require，构建通过
        let remote = NatsConfig::builder()
            .url("nats://broker.example.com:4222")
            .build()
            .expect("远程地址默认 Require");
        assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);
        // 零超时同样被拒绝
        assert!(NatsConfig::builder()
            .operation_timeout(Duration::ZERO)
            .build()
            .is_err());
    }

    #[test]
    fn from_env_reads_canonical_prefix_and_validates() {
        let _guard = env_guard();
        let mut scope = EnvScope::new();
        for key in [ENV_URL, "FOUNDATIONX_NATS_URL", ENV_CONNECT_TIMEOUT_MS] {
            std::env::remove_var(key);
        }
        scope.set(ENV_URL, "nats://127.0.0.1:14222");
        scope.set(ENV_CONNECT_TIMEOUT_MS, "1500");
        let config = NatsConfig::from_env().expect("from_env");
        assert_eq!(config.url, "nats://127.0.0.1:14222");
        assert_eq!(config.connect_timeout, Duration::from_millis(1500));
    }

    #[test]
    fn from_env_prefers_canonical_over_legacy_prefix() {
        let _guard = env_guard();
        let mut scope = EnvScope::new();
        scope.set(ENV_URL, "nats://127.0.0.1:14223");
        scope.set("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:14224");
        let config = NatsConfig::from_env().expect("from_env");
        assert_eq!(config.url, "nats://127.0.0.1:14223");

        std::env::remove_var(ENV_URL);
        let legacy = NatsConfig::from_env().expect("from_env legacy");
        assert_eq!(legacy.url, "nats://127.0.0.1:14224");
    }

    #[test]
    fn from_env_rejects_invalid_values() {
        let _guard = env_guard();
        let mut scope = EnvScope::new();
        scope.set(ENV_TLS_POLICY, "weird");
        assert!(NatsConfig::from_env().is_err(), "非法 TLS 策略必须被拒绝");

        // 远程地址 + disable：env 覆盖后同样 fail-closed
        scope.set(ENV_TLS_POLICY, "disable");
        scope.set(ENV_URL, "nats://broker.example.com:4222");
        assert!(NatsConfig::from_env().is_err(), "远程明文必须被拒绝");

        // 远程地址 + require：合法，且生效策略为 Require
        scope.set(ENV_TLS_POLICY, "require");
        let remote = NatsConfig::from_env().expect("远程 + require 合法");
        assert_eq!(remote.effective_tls_policy(), TlsPolicy::Require);

        // 非法数值同样被拒绝
        scope.set(ENV_MAX_RECONNECTS, "not-a-number");
        assert!(NatsConfig::from_env().is_err());
    }
}
