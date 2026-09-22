# natsx Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

NATS 适配器：Core NATS 发布/订阅 + JetStream 持久消费，带 TLS 策略、连接池化与结构化健康报告。

## 技术栈

- Rust edition 2021, rust-version 1.88
- 关键依赖: `async-nats` 0.50（jetstream / nkeys / ring TLS 后端）、`tokio`、`thiserror`、`serde`、`serde_json`、`toml`、`tracing`、`url`、`bytes`、`futures-*`
- 零内部耦合，不依赖 kernel/contracts 等私有 crate

## 代码结构

```text
src/
├── lib.rs        # 入口：模块声明 + 受控 re-export + API 面测试
├── config.rs     # NatsConfig / NatsConfigBuilder / TlsPolicy / ENV_* 常量（pub mod config）
├── error.rs      # NatsError / NatsResult（pub mod error）
├── jetstream.rs  # JetStream / JetStreamConsumer / JetStreamDelivery / StreamConfig 等
├── pool.rs       # NatsPool 门面：公开类型定义 + 内联测试 +
│                 # config/client/is_connected/ping/flush/health_check/stats/ready_client/register_task/take_tasks
├── pool/
│   ├── connection.rs # 建连与关停：new / connect / connect_from_env / close / drain（含 PoolInner 构造）
│   └── pubsub.rs     # 数据面：publish / publish_with_headers / publish_json / subscribe / request
└── validation.rs # 纯函数校验：subject / stream / consumer / operation_timeout（pub mod validation）

tests/            # api_surface.rs · config_env.rs · connect_failure.rs · pure_functions.rs
benches/          # hot_path.rs（harness = false，离线基准）
docs/             # API.md · 标准.md
```

## 设计约定（改代码前必读）

- **TLS fail-closed**：loopback 默认 `TlsPolicy::Prefer`，非 loopback 默认 `Require`；`validate()` 拒绝「非 loopback + 非 Require」组合。不要放宽该策略。
- **秘密脱敏**：`password` / `token` / `nkey_seed` 不进入 `Debug`（渲染为 `***`），URL 内嵌 userinfo 同样脱敏；敏感字段禁止出现在 TOML，只能经 env 或 builder 注入。
- **环境变量前缀**：规范 `FOUNDATIONX_NATSX_*`，兼容 `FOUNDATIONX_NATS_*`（前者优先）；新增 env 键必须同时定义 `ENV_*` 常量并写文档。
- **有界容量**：订阅与客户端队列有界（`subscription_capacity` / `client_capacity`），禁止无界缓冲。
- **纯函数可离线测**：subject/stream/consumer 校验与配置解析全部离线可测；连接行为测试用连接失败路径（`tests/connect_failure.rs`），不依赖真实 NATS 服务。

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误类型：thiserror 枚举 + `#[non_exhaustive]` + `pub type NatsResult<T>`
- 配置：`NatsConfig` 结构体 + `builder()`/`from_env()`/`from_toml()` + `validate()` + fail-fast
- 禁止裸 `unwrap()`（库代码）/ 无注释 `expect()`；测试模块经 `#![cfg_attr(test, allow(...))]` 放宽
- 异步代码使用 tokio，禁止在 async 中做阻塞 I/O；连接与操作有超时（`connect_timeout` / `operation_timeout`）
- `#![forbid(unsafe_code)]`、`#![deny(missing_docs)]` 已开启：新增 pub 项必须带中文 `///` 文档
- 公开 API 面变化需同步 `src/lib.rs` 的 `public_api_surface` 测试与 `tests/api_surface.rs`

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

基准（离线，不需要 NATS 服务，可选）：

```bash
cargo bench --bench hot_path             # 完整 50_000 次迭代
cargo bench --bench hot_path -- --quick  # 快速 1_000 次迭代
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`
