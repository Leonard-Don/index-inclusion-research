# Pre-Analysis Plan (PAP) — index-inclusion-research

本文档冻结当前 7 条 CMA 假说裁决与方法论参数，作为下一轮迭代（数据扩展 / 代码改动 / 论文定稿）的预注册基线。
任何偏离本计划的修改必须在 §7 决策日志中签字记录。

- **冻结日期**：2026-05-03
- **冻结提交**：参见 `git log -1 --format=%H` 与 §7 表格的 commit 列
- **基线快照**：[`snapshots/pre-registration-2026-05-03.csv`](../snapshots/pre-registration-2026-05-03.csv)
- **比对命令**：`index-inclusion-verdict-summary --compare-with snapshots/pre-registration-2026-05-03.csv`

> 本 PAP 公开后，本仓库的 7 假说裁决从 **post-hoc 解释** 升级为 **confirmatory 验证**。
> 在 §7 之前，仍需在 `docs/limitations.md` §6 与 `docs/paper_outline_verdicts.md` 顶部标注 post-hoc。
> §7 一旦签字，即可在论文与 README 中按 confirmatory 表述。

## 1. 决策阈值（冻结）

| 项 | 取值 | 适用范围 |
|---|---|---|
| 主阈值 `p` | **0.10** | H1 / H4 / H5 等 p-gated 假说的"部分支持"边界 |
| 内阈值 `p_inner` | **0.05** | p-gated 假说的"支持"边界（= 主阈值 / 2）|
| 多重检验校正 | Bonferroni 与 Benjamini-Hochberg q-value | 输出层附加（不替代主阈值）|
| 主表纳入规则 | `evidence_tier = core` | core = H1, H5, H7；supplementary = H2, H3, H4, H6 走附录 |

CLI 引用：`index-inclusion-cma --threshold 0.10`（默认值即冻结值，重跑无需显式传参）。

## 2. 假说与判据（冻结）

下列 7 条假说文本、metric 与判据于本日冻结。任何措辞或判据修改必须在 §7 记录。

### H1 · 信息泄露与预运行 — `evidence_tier=core`

- **样本**：`results/real_tables/cma_ar_path.csv`、`cma_gap_summary.csv`，n=436（CN+US）
- **统计量**：CN-US pre-runup 差异的 block-bootstrap p-value（按 `announce_date` 聚类）
- **判据**：bootstrap p < 0.05 → 支持；0.05 ≤ p < 0.10 → 部分支持；p ≥ 0.10 → 证据不足
- **冻结裁决**：证据不足（bootstrap p = 0.875，CI95=[-3.25%, 4.70%]）

### H2 · 被动基金 AUM 差异 — `evidence_tier=supplementary`

- **样本**：Federal Reserve Z.1 `BOGZ1FL564090005A` 年度数据，n=12（2014-2025）
- **统计量**：US AUM 首尾比 vs US effective rolling CAR 方向一致性
- **判据**：方向一致 + 二者增量同号 → 支持；其余 → 证据不足
- **冻结裁决**：证据不足（US AUM ratio = 13.481；rolling CAR 方向不稳定）

### H3 · 散户 vs 机构结构 — `evidence_tier=supplementary`

- **样本**：`cma_mechanism_panel.csv`（4 个 CN/US × announce/effective 象限），n=4
- **统计量**：dual-channel 命中率（turnover 通道 + volume 通道双 p<0.10 的象限占比）
- **判据**：命中率 ≥ 0.75 → 支持；0.50 ≤ 命中率 < 0.75 → 部分支持；< 0.50 → 证据不足
- **冻结裁决**：支持（双通道命中率 = 0.750）

### H4 · 卖空约束 — `evidence_tier=supplementary`

- **样本**：`cma_gap_event_level.csv` 控制 gap_length_days，n=436
- **统计量**：CN-US gap_drift 差异在事件级回归下的 p-value（HC3 标准误）
- **判据**：regression p < 0.05 → 支持；0.05 ≤ p < 0.10 → 部分支持；p ≥ 0.10 → 证据不足
- **冻结裁决**：证据不足（regression p = 0.537，CN coef = 0.61%）

### H5 · 涨跌停限制 — `evidence_tier=core`

- **样本**：CN 事件级 `cma_h5_limit_predictive_regression.csv`，n=936
- **统计量**：limit_coef 在 announce-day CAR 回归中的 p-value
- **判据**：limit_coef p < 0.05 → 支持；0.05 ≤ p < 0.10 → 部分支持；p ≥ 0.10 → 证据不足
- **冻结裁决**：支持（limit_coef p = 0.008，limit_coef = 0.1549，R²=0.011）

### H6 · 指数权重可预测性 — `evidence_tier=supplementary`

- **样本**：`cma_h6_weight_explanation.csv` + `cma_heterogeneity_size.csv`，匹配后 n=67
- **统计量**：heavy-light spread（announce_jump 中位数）+ OLS / sector-FE / quantile 三维稳健性
- **判据**：spread > 0 且至少两条稳健性回归 p < 0.10 → 支持；其余 → 证据不足
- **冻结裁决**：证据不足（heavy-light spread = -0.019；OLS coef = -0.0061，p = 0.001 但方向反向）

### H7 · 行业结构差异 — `evidence_tier=core`

- **样本**：`cma_heterogeneity_sector.csv`（US 8 个行业），n=187
- **统计量**：US 行业间 asymmetry_index 的 spread（max - min）
- **判据**：US sector spread ≥ 4.0 + spread 跨越 0 → 支持；其余 → 证据不足
- **冻结裁决**：支持（US sector spread = 5.95，Materials +3.90 vs Real Estate -2.05）

## 3. 样本边界（冻结）

| 维度 | 取值 |
|---|---|
| 时间窗口 | 2010-01-01 至 2025-12-31 |
| 市场 | CN（CSI300）+ US（S&P500）|
| 事件清单 | `data/processed/events.csv`，893 条（CN 274 + US 619）|
| 事件研究窗口 | `[-20, +20]` 交易日，CAR 主窗 `[-1, +1]` |
| 匹配口径 | sector × 同期市值 quintile，covariate balance 门禁 \|SMD\|<0.25 |
| HS300 RDD L3 | 6 批次 / 159 行（2023-05 → 2025-11）|
| HS300 RDD 主表用法 | **illustrative / preliminary** — 不进主表 |
| 被动 AUM | Federal Reserve Z.1 年度数据（n=12）|

数据契约固定为 `events.csv` / `prices.csv` / `benchmarks.csv` 的现有 schema（README §"数据输入契约"）。
任何新增 / 修改字段都必须在 §7 记录并对 schema 加版本标签。

## 4. 主表 / 附录划分（冻结）

- **主表（论文正文）**：H1 · H5 · H7（`evidence_tier = core`）
- **附录（supplementary）**：H2 · H3 · H4 · H6
- **HS300 RDD**：作为附录 / 方法论补充章节，不进主表（直到 L3 ≥ 10 年）
- **核心问题**：announce-day CAR 显著正向（CN +1.75%, US +1.47%）这一主结论用事件研究直接给出，
  CMA 7 假说回答的是 CN/US 不对称的来源，**不是**"是否上涨"本身——
  这一区分必须在论文 §引言 与 §结论 中显式写出。

## 5. 数据扩展 / 代码改动的合规边界

允许在不需要 §7 签字的前提下做的事：

- 跑 `make rebuild` 复现现有产出
- 跑 `index-inclusion-cma`（默认阈值）刷新 verdicts
- 收集 / 添加新数据，**前提是**新数据落在 §3 已冻结的 schema 与样本边界内
- 改 dashboard / docs / 测试，不影响 verdict 计算逻辑

需要在 §7 签字才能做的事：

- 修改任意假说的样本筛选、统计量定义或判据阈值
- 增加 / 删除假说
- 改 §3 的样本边界（包括延长时间窗口）
- 改 §1 的主阈值或 evidence_tier 划分
- 切换 HS300 RDD 进主表

## 6. 报告口径

发表 / 汇报中引用本项目结论时，必须同时引用本 PAP：

> 本研究 7 条机制假说的预注册基线见 `docs/pre_registration.md`（冻结日 2026-05-03）。
> 详细数据与方法限制见 `docs/limitations.md`。

裁决文本、metric 与判据应直接引用本文档对应小节，不得只引用 `cma_hypothesis_verdicts.csv`
（CSV 文本可能在 verdict 重跑时被复盖）。

## 7. 决策日志（任何偏离本 PAP 的改动必须签字）

| 日期 | 改动类型 | 改动摘要 | 触达条目 | 签字 | commit |
|---|---|---|---|---|---|
| 2026-05-03 | 冻结基线 | 创建 PAP 与 snapshots/pre-registration-2026-05-03.csv；7 假说阈值与判据按当日 verdicts.csv 锁定 | §1-§4 全部 | _待 leo 签字_ | _待填_ |

签字后请把"_待 leo 签字_"替换为日期 + 签字人名（建议格式 `2026-05-03 leo`），并在 commit 列填入冻结提交的 short SHA。
此后任何修改新增一行，**不得删除已有行**。

---

参考：
- [docs/limitations.md](limitations.md) §6 — 多重检验
- [docs/verdict_iteration.md](verdict_iteration.md) — verdict-diff 工作流
- [docs/paper_outline_verdicts.md](paper_outline_verdicts.md) — 论文级裁决叙述
- [docs/hs300_rdd_l3_collection_audit.md](hs300_rdd_l3_collection_audit.md) — RDD L3 覆盖审计
