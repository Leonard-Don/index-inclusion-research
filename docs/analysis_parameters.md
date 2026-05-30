# 分析参数记录 — index-inclusion-research

> **本文档不是预分析计划 (pre-analysis plan)。** 本项目 7 条 CMA 假说 (H1–H7) 是
> **post-hoc、探索性**的：它们是在观察到 announce-vs-effective、CN-vs-US 的不对称结果
> **之后**形成的。下列裁决阈值（`p<0.10`、内阈值 `0.05`）是在已知结果的情况下选定的
> **分析参数**，**没有事前承诺**。本文档的作用是把这些分析参数、样本边界与
> 各假说判据集中记录下来，便于透明复核与读者评估——它**不构成**任何形式的
> 预注册、confirmatory 验证或"基线锁定"。

本文档记录当前 7 条 CMA 假说裁决所用的方法论参数与样本边界。配套的
`snapshots/pre-registration-*.csv` 是**裁决基线快照**（用于跨时间观察 verdict 稳定性的
工具，详见 §6），不是 pre-analysis plan。

- **参数记录日期**：2026-05-29（最近一次更新；见 §7 变更日志）
- **基线快照**：[`snapshots/pre-registration-2026-05-29.csv`](../snapshots/pre-registration-2026-05-29.csv)
  （CSV 文件名为历史命名，保留以兼容现有 `--vs-pap` / `--compare-with` 工具；语义见 §6）
- **比对命令**：`index-inclusion-verdict-summary --compare-with snapshots/pre-registration-2026-05-29.csv`

## 1. 决策阈值（分析参数）

下列阈值是在已知结果的情况下选定的分析参数，记录于此以便透明复核。

| 项 | 取值 | 适用范围 |
|---|---|---|
| 主阈值 `p` | **0.10** | H1 / H4 / H5 等 p-gated 假说的"部分支持"边界 |
| 内阈值 `p_inner` | **0.05** | p-gated 假说的"支持"边界（= 主阈值 / 2）|
| 多重检验校正 | Bonferroni 与 Benjamini-Hochberg q-value | 输出层附加（不替代主阈值）|
| 主表纳入规则 | `evidence_tier = core` | core = H1, H5, H7；supplementary = H2, H3, H4, H6 走附录 |

CLI 引用：`index-inclusion-cma --threshold 0.10`（默认值即上述取值，重跑无需显式传参）。

## 2. 假说与判据

下列 7 条假说文本、metric 与判据是当前裁决所用的口径。它们是 post-hoc 形成的；
任何措辞或判据修改请在 §7 变更日志中记录。

### H1 · 信息泄露与预运行 — `evidence_tier=core`

- **样本**：`results/real_tables/cma_ar_path.csv`、`cma_gap_summary.csv`，n=436（CN+US）
- **统计量**：CN-US pre-runup 差异的 block-bootstrap p-value（按 `announce_date` 聚类）
- **判据**：bootstrap p < 0.05 → 支持；0.05 ≤ p < 0.10 → 部分支持；p ≥ 0.10 → 证据不足
- **当前裁决**：证据不足（bootstrap p = 0.875，CI95=[-3.25%, 4.70%]）

### H2 · 被动基金 AUM 差异 — `evidence_tier=supplementary`

- **样本**：Federal Reserve Z.1 `BOGZ1FL564090005A` 年度数据，n=12（2014-2025）
- **统计量**：US AUM 首尾比 vs US effective rolling CAR 方向一致性
- **判据**：方向一致 + 二者增量同号 → 支持；其余 → 证据不足
- **当前裁决**：证据不足（US AUM ratio = 13.481；rolling CAR 方向不稳定）
- **证据边界**：当前 manifest 只确认 US AUM 序列；CN 可比被动 AUM 缺失时必须保持 supplementary/warn 表述。

### H3 · 散户 vs 机构结构 — `evidence_tier=supplementary`

- **样本**：`cma_mechanism_panel.csv`（4 个 CN/US × announce/effective 象限），n=4
- **统计量**：dual-channel 命中率（turnover 通道 + volume 通道双 p<0.10 的象限占比）
- **判据**：命中率 ≥ 0.75 → 支持；0.50 ≤ 命中率 < 0.75 → 部分支持；< 0.50 → 证据不足
- **当前裁决**：支持（双通道命中率 = 0.750）

### H4 · 卖空约束 — `evidence_tier=supplementary`

- **样本**：`cma_gap_event_level.csv` 控制 gap_length_days，n=436
- **统计量**：CN-US gap_drift 差异在事件级回归下的 p-value（HC3 标准误）
- **判据**：regression p < 0.05 → 支持；0.05 ≤ p < 0.10 → 部分支持；p ≥ 0.10 → 证据不足
- **当前裁决**：证据不足（regression p = 0.537，CN coef = 0.61%）

### H5 · 涨跌停限制 — `evidence_tier=core`

- **样本**：CN 事件级 `cma_h5_limit_predictive_regression.csv`，n=936
- **统计量**：limit_coef 在 announce-day CAR 回归中的 p-value
- **判据**：limit_coef p < 0.05 → 支持；0.05 ≤ p < 0.10 → 部分支持；p ≥ 0.10 → 证据不足
- **当前裁决**：支持（limit_coef p = 0.008，limit_coef = 0.1549，R²=0.011）

### H6 · 指数权重可预测性 — `evidence_tier=supplementary`

- **样本**：`cma_h6_weight_explanation.csv` + `cma_heterogeneity_size.csv`，匹配后 n=67
- **统计量**：heavy-light spread（announce_jump 中位数）+ OLS / sector-FE / quantile 三维稳健性
- **判据**：spread > 0 且至少两条稳健性回归 p < 0.10 → 支持；其余 → 证据不足
- **当前裁决**：证据不足（heavy-light spread = -0.019；OLS coef = -0.0061，p = 0.001 但方向反向）

### H7 · 行业结构差异 — `evidence_tier=core`

- **样本**：`cma_heterogeneity_sector.csv`（US 8 个行业），n=187
- **统计量**：US 行业间 asymmetry_index 的 spread（max - min）
- **判据**：US sector spread ≥ 4.0 + spread 跨越 0 → 支持；其余 → 证据不足
- **当前裁决**：支持（US sector spread = 5.95，Materials +3.90 vs Real Estate -2.05）
- **补充稳健性**：`cma_h7_sector_interaction.csv` 可作为 sector×phase/treatment 回归补强；
  不改变上述判据、verdict、confidence、key metric。

## 3. 样本边界

| 维度 | 取值 |
|---|---|
| 时间窗口 | 2010-01-01 至 2025-12-31 |
| 市场 | CN（CSI300）+ US（S&P500）|
| 事件清单 | `data/processed/events.csv`，893 条（CN 274 + US 619）|
| 事件研究窗口 | `[-20, +20]` 交易日，CAR 主窗 `[-1, +1]` |
| 匹配口径 | sector × 同期市值 quintile，covariate balance 门禁 \|SMD\|<0.25 |
| HS300 RDD L3 | 11 批次 / 356 行（2020-11 → 2025-11）|
| HS300 RDD 主表用法 | **illustrative / preliminary** — 不进主表（直至 L3 ≥10 年 ≈ 20 批次）|
| 被动 AUM | Federal Reserve Z.1 年度数据（n=12）|

数据契约固定为 `events.csv` / `prices.csv` / `benchmarks.csv` 的现有 schema（README §"数据输入契约"）。
任何新增 / 修改字段都请在 §7 记录并对 schema 加版本标签。

## 4. 主表 / 附录划分

- **主表（论文正文）**：H1 · H5 · H7（`evidence_tier = core`）
- **附录（supplementary）**：H2 · H3 · H4 · H6
- **HS300 RDD**：作为附录 / 方法论补充章节，不进主表（直到 L3 ≥ 10 年）
- **核心问题**：announce-day CAR 显著正向（CN +2.07%, US +1.87%）这一主结论用事件研究直接给出，
  CMA 7 假说回答的是 CN/US 不对称的来源，**不是**"是否上涨"本身——
  这一区分必须在论文 §引言 与 §结论 中显式写出。

## 5. 数据扩展 / 代码改动的边界

下列改动属于例行复现，可直接进行：

- 跑 `make rebuild` 复现现有产出
- 跑 `index-inclusion-cma`（默认阈值）刷新 verdicts
- 收集 / 添加新数据，前提是新数据落在 §3 的 schema 与样本边界内
- 改 dashboard / docs / 测试，不影响 verdict 计算逻辑

下列改动会改变本文档记录的分析参数或假说集合，请在 §7 变更日志中记录原因与影响：

- 修改任意假说的样本筛选、统计量定义或判据阈值
- 增加 / 删除假说
- 改 §3 的样本边界（包括延长时间窗口）
- 改 §1 的主阈值或 evidence_tier 划分
- 切换 HS300 RDD 进主表

> 说明：本文档不是 pre-analysis plan，上述"记录"机制只是为了让分析参数的变更**可追溯**，
> 不会把 post-hoc 裁决转化为 confirmatory 结论。

## 6. 裁决基线快照与报告口径

`snapshots/pre-registration-*.csv` 是**裁决基线快照**——把某一时点的 7 条 verdict 冻结成
CSV，配合 `index-inclusion-verdict-summary --vs-pap` / `--compare-with` 即可观察
**verdict 在不同时间点是否稳定**（verdict stability over time）。这是一个研究迭代追踪工具，
**不是 pre-registration**：快照在结果已知之后创建，不构成事前承诺。CSV 文件名与
CLI 标志名（`--vs-pap`）保留历史命名仅为兼容，不改变其语义。

发表 / 汇报中引用本项目结论时，请同时说明 H1–H7 是 post-hoc、探索性裁决：

> 本研究 7 条机制假说为 post-hoc、探索性形成（详见 `docs/analysis_parameters.md` 与
> `docs/limitations.md` §6），不是预注册裁决。
> 详细数据与方法限制见 `docs/limitations.md`。

裁决文本、metric 与判据应直接引用本文档对应小节，不要只引用 `cma_hypothesis_verdicts.csv`
（CSV 文本可能在 verdict 重跑时被覆盖）。

## 7. 变更日志

下表记录分析参数与假说集合的变更历史，便于读者追溯每条裁决口径的来由。

| 日期 | 改动类型 | 改动摘要 | 触达条目 | 记录人 | commit |
|---|---|---|---|---|---|
| 2026-05-03 | 首次记录 | 创建本文档与 snapshots/pre-registration-2026-05-03.csv（裁决基线快照）；7 假说阈值与判据按当日 verdicts.csv 记录 | §1-§4 全部 | leo | fad5211 |
| 2026-05-03 | 样本边界扩展 | HS300 RDD L3 从 6 批次扩到 11 批次（新增 csi300-2020-11、2021-05、2021-11、2022-05、2022-11，共 197 行）。来源：中证官方 Excel 公告附件原本有 `备选名单` sheet（含 `排序` 列），原 parser 漏读；修正后无需手工 archive 检索。仍 < 20 批次 / 10 年门槛，主表用法不变。verdicts diff vs 基线快照：0 changed。 | §3 仅 RDD 行；§4 不变 | leo | 006440a |
| 2026-05-06 | 证据补强 | 新增 H7 sector×phase/treatment 交互回归表；H2 manifest 明确 US-only / CN AUM 缺失边界；不改变 verdict/confidence/key metric。 | H2/H7 证据说明、dashboard 明细 | leo | 本提交 |
| 2026-05-16 | H2 数据扩展 + 数据驱动 tier 升级 | 引入 `data/raw/cn_passive_aum_proxy.csv`(CSI300+CSI500 跟踪 ETF 年终 TNA 聚合,通过 akshare `fund_etf_scale_sse` + `fund_scale_daily_szse` + `fund_etf_fund_info_em`(close 兜底) 抓取);H2 verdict 函数升级为双市场比较;`_core.EVIDENCE_TIER_PROMOTION_FLOOR["H2"]=15` 在合并 n(US rolling 12 + CN rolling 5 = 17)越线后将 H2 evidence_tier 由 supplementary 升级为 core;副作用:H2 verdict 由"证据不足/低"变为"部分支持/中"——CN 端 AUM 上升伴随 effective CAR 下降(0.59%→0.42%),US 端 effective CAR 没有持续衰减(0.04%→0.05%);verdict 文本变更是数据驱动而非人工硬改。proxy 局限见 docs/limitations.md §3。snapshots/pre-registration-2026-05-03.csv 基线不变,§4 主表纳入规则改写为 baseline {H1/H5/H7} 或 proxy-promoted {H1/H2/H5/H7}。 | §1 主表纳入规则; §2 H2 metric/judgment 文本; §4 主表/附录划分 | leo | 本提交 |
| 2026-05-29 | Tushare CN 行情口径刷新 + 新裁决快照 | 使用 qmt 项目中的 Tushare token 将 A 股日线、CSI300 基准、`daily_basic.total_mv` 与 `turnover_rate` 接入 `index-inclusion-download-real-data --cn-price-source tushare` 并重跑真实证据流水线；相对 2026-05-16 快照，H2 由"部分支持/中"翻为"证据不足/低"，H5 由"支持/高"翻为"证据不足/中"，H1/H3/H4 弱化但 verdict 未变，H6/H7 verdict 未变。创建 `snapshots/pre-registration-2026-05-29.csv` 作为新的裁决稳定性快照；旧快照保留，可用 `index-inclusion-verdict-summary --compare-with snapshots/pre-registration-2026-05-16.csv` 复核本次翻转。 | 数据源说明; CMA verdicts; 裁决基线快照 | leo | 本提交 |

此后任何分析参数变更新增一行，不要删除已有行。

---

参考：
- [docs/limitations.md](limitations.md) §6 — 多重检验与 post-hoc 披露
- [docs/verdict_iteration.md](verdict_iteration.md) — verdict-diff 工作流
- [docs/paper_outline_verdicts.md](paper_outline_verdicts.md) — 论文级裁决叙述
- [docs/hs300_rdd_l3_collection_audit.md](hs300_rdd_l3_collection_audit.md) — RDD L3 覆盖审计
