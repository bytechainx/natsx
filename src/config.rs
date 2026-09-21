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
use std::path::Path;
use std::time::Duration;

use crate::error::{NatsError, NatsResult};

mod builder;
mod repr;
mod tls;

pub use builder::NatsConfigBuilder;
pub use tls::{url_is_loopback, TlsPolicy};

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

#[cfg(test)]
mod tests;
