# CONTRIBUTING.md — 贡献指南（natsx）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` /
  `contracts` 等），全部依赖来自 crates.io 公开包。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` /
  `refactor:`），描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty
```

基准（离线，不需要 NATS 服务，可选）：

```bash
cargo bench --bench hot_path -- --quick
```

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/natsx#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  natsx = { git = "https://github.com/bytechainx/natsx" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- 错误类型：`thiserror` 枚举 + `#[non_exhaustive]` + `pub type NatsResult<T>` 别名。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` /
  `panic`）。集成测试目标（`tests/*.rs`、`benches/hot_path.rs`）经 `#![allow(...)]` 豁免。
- 所有 `pub` 项必须有中文 `///` 文档（`missing_docs` 已 `deny`）。
- 集成测试**必须离线运行**，不触碰真实网络。
- MSRV 为 `1.88`，edition 2021；升级下界必须同步 `rust-version` 与 CI。
- TLS 策略是 fail-closed 的：loopback 默认 `Prefer`、非 loopback 默认 `Require`，
  `validate()` 拒绝「非 loopback + 非 Require」组合；**不要放宽该策略**。
- 订阅与客户端命令队列保持有界（`subscription_capacity` / `client_capacity`），
  禁止引入无界缓冲。
- 环境变量前缀规范为 `FOUNDATIONX_NATSX_*`，兼容 `FOUNDATIONX_NATS_*`（前者优先）；
  新增 env 键必须同时定义 `ENV_*` 常量并写文档。
- 公开 API 面变化需同步 `src/lib.rs` 的 `public_api_surface` 测试与
  `tests/api_surface.rs`。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --all-targets` 通过
- [ ] `cargo package --no-verify --allow-dirty` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 公开 API 面变化已同步 `public_api_surface` 测试与 `tests/api_surface.rs`
