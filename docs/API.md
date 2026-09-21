# natsx 公开 API

**版本 / 角色**：`natsx 0.1.0` · NATS 适配器（Core NATS 发布/订阅 + JetStream 持久消费 + TLS 策略 + 连接池化 + 健康报告）

## 公开消费面

| 入口 | 说明 |
| --- | --- |
| `NatsConfig` / `NatsConfigBuilder` | 配置：`from_env` / `from_toml` / `validate` / `builder`；`SCHEMA_VERSION = 1` |
| `TlsPolicy` / `url_is_loopback` | TLS 策略（`Prefer` / `Require` 等）与 loopback 地址判定 |
| `NatsPool` | `connect` / `new` / `publish` / `publish_with_headers` / `subscribe` / `request` / `ping` / `flush` / `health_check` / `stats` / `close` / `drain`；可克隆 |
| `NatsSubscription` / `NatsMessage` | 订阅流（`next()` 或 `Stream`）与消息 |
| `NatsHealth` / `NatsPoolStats` | 结构化健康与统计 |
| `JetStream` | `publish` / `publish_json` / stream 管理 / `consumer` |
| `JetStreamConsumer` / `JetStreamDelivery` / `JetStreamDeliveryMetadata` | 有限拉取消费与 `ack` / `nak` / `progress` / `term` |
| `JetStreamConsumerConfig` / `PullConsumerConfig` / `StreamConfig` / `StreamInfo` | JetStream 配置与流信息 |
| `NatsError` / `NatsResult` | 统一错误与结果别名 |
| `validation` 纯函数 | `validate_subject` / `validate_publish_subject` / `validate_stream_name` / `validate_consumer_name` / `validate_operation_timeout` |
| `config::ENV_*` 常量 | 环境变量键（前缀 `FOUNDATIONX_NATSX_*`，兼容 `FOUNDATIONX_NATS_*`，前者优先） |

## 最小用法

```rust,no_run
use natsx::{NatsConfig, NatsPool, NatsResult};

# async fn run() -> NatsResult<()> {
let pool = NatsPool::connect(NatsConfig::default()).await?;
let mut subscription = pool.subscribe("demo.subject").await?;
pool.publish("demo.subject", "hello nats").await?;
if let Some(message) = subscription.next().await {
    println!("收到 {} 字节", message.payload.len());
}
println!("rtt = {:?}", pool.ping().await?);
pool.drain(std::time::Duration::from_secs(5)).await?;
# Ok(())
# }
```

## TLS 策略

- loopback（`127.0.0.1` / `localhost` / `::1`）默认 `TlsPolicy::Prefer`：允许明文，服务端要求时才升级 TLS；
- 非 loopback 默认 `TlsPolicy::Require`：`ConnectOptions::require_tls(true)`，握手失败即连接失败；
- 显式 `NatsConfig::tls_policy` 优先，其次 `tls` 布尔开关；
- `validate()` 拒绝「非 loopback + 非 Require」组合（fail-closed）；
- 自定义 CA 经 `tls_client_config` 注入；仅配置 mTLS 证书时使用 `add_client_certificate`（保留系统根证书）。

## 安全约定

- `password` / `token` / `nkey_seed` 不进入 `Debug`（渲染为 `***`），URL 内嵌 userinfo 同样脱敏；
- 敏感字段禁止出现在 TOML，只能经环境变量或 `NatsConfigBuilder` 注入；
- 生产代码不含 `unwrap` / `expect` / `panic`，错误一律经 `NatsError` 返回。

## 能力边界

本 crate 提供 NATS 客户端适配（Core + JetStream 拉取消费）；**不提供**NATS 服务端、K/V·Object Store 高层封装、请求路由框架或消息总线语义（topic 层级治理由调用方负责）。所有公开类型 `Send + Sync`。
