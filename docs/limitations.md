# 数据与方法限制

本文档集中记录项目的关键数据近似与方法约束，论文写作和读者评估时请同时引用此页。

## 1. 价格与市值数据

- **价格 / 收益**：Yahoo Finance（yfinance）日频 OHLCV；按当前交易日复权。
- **市值（mkt_cap）**：用 Yahoo `sharesOutstanding`（当前值）× 历史价格近似得到。
  **不等价于交易所历史自由流通市值**，仅适合机制分析与课程汇报。
- **换手率（turnover）**：volume / shares_outstanding 近似，没有过滤大宗 / 协议交易。
- **基准（benchmark_ret）**：CN 用 CSI300 指数收益，US 用 S&P 500 指数收益（`benchmarks.csv`）。

## 2. 事件清单

- **CN 事件**：中证指数公司官方调整公告 PDF + 公开新闻补充转录（`source` 列记录来源）。
- **US 事件**：维基百科 S&P 500 成分股表 + S&P Dow Jones 官方脚注。
- **未覆盖**：增发 / 分拆事件、内部技术性调整（如 ticker 变更）。
- **样本规模**：893 个真实事件（274 CN + 619 US，2010-2025）。

## 3. 被动 AUM（H2 假说）

- **来源**：Federal Reserve Z.1 系列（`BOGZ1FL564090005A`，US ETF Total Financial Assets）。
- **样本**：仅 12 个年度观测（2010-2025），结构性 underpower，不能支持时间序列层面的强结论。
- **缺口**：CN passive AUM 没有等价口径；用作 H2 对照需重做匹配。

## 4. HS300 RDD 数据层级

- **L3（官方）**：2020-11 到 2025-11 共 11 个批次，356 行（含 191 调入 + 165 备选对照）。详见 `docs/hs300_rdd_data_contract.md`。
- **L2（公开重建）**：1887 行；从公开调整新闻反推，**不等价于中证官方历史排名**。
- **L1（演示）**：合成数据，仅供 pipeline 测试。
- **当前主表使用**：默认 L3；缺失时返回 `missing` 状态而非自动降级。
- **若要支持论文级因果声明**：需扩展 L3 到 ≥10 年并补 McCrary 操纵性检验，
  详见 `docs/hs300_rdd_l3_collection_audit.md`。

## 5. 事件研究方法

- **AR 计算**：`ar = ret − benchmark_ret`（简单市场调整）；没有市场模型 β 估计。
- **σ 估计**：默认从 panel 内 `[-window_pre, -2]` 区间估计 (window_pre 默认 20)；
  这是 18 日的 in-panel proxy estimation window，比文献标准（120-250 日）短。
- **标准化**：`compute_patell_bmp_summary` 在简单 t 之外提供 Patell t 与 BMP t；
  在样本量充足时建议优先看 BMP（不假设零相关）。
- **长窗口** `[0,+120]`：样本会大幅缩水，仅作探索性结果，不进入主表。

## 6. 多重检验

- **当前阈值**：决定层 p<0.10（默认）；输出层附 Bonferroni 与 Benjamini-Hochberg q-value。
- **Pre-registration**：7 假说 PAP 草稿见 [`docs/pre_registration.md`](pre_registration.md)（冻结日 2026-05-03）。
  在 PAP §7 决策日志签字之前，仍按 **post-hoc** 表述；签字后可升级为 **confirmatory**。
  详细 verdict 迭代流程见 [`docs/verdict_iteration.md`](verdict_iteration.md)。

## 7. CMA 假说证据强度分层

- **核心假说（core, n 充足）**：H1（n=436）、H5（n=936）、H7（n=187）。
- **附录假说（supplementary, n 受限）**：
  - H2（n=12，年度 AUM）
  - H3（n=4 象限，dual-channel 判据）
  - H4（n=436 但回归 p=0.537，不显著）
  - H6（n=67）
- `cma_hypothesis_verdicts.csv` 的 `evidence_tier` 列记录该分层；
  论文主表只引用 core，supplementary 走附录。

## 8. 何时不要用本项目结论

- 若需要交易所自由流通市值精确口径 → 不要用 Yahoo `mkt_cap`。
- 若需要中证官方历史排名因果识别 → L3 数据不足以前不要用。
- 若需要长窗口 [0,+120] 退化效应 → 样本严重缩水，仅作探索性。
- 若需要时间序列 AUM 推断 → n=12 不够，结论以方向参考为主。

## 9. 引用格式建议

文中或表注中引用本项目结果时，建议同时标注：

> 数据来源：Yahoo Finance（价格、近似市值）、Federal Reserve Z.1（被动 AUM）、
> 中证指数公司公告与维基百科（事件清单）；HS300 RDD 当前使用 L3 官方候选边界样本，但覆盖期仍不足以支撑论文级强因果声明。
> 详细数据与方法限制见 `docs/limitations.md`。
