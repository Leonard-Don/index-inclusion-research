## 主结论:指数纳入是否产生显著超额收益

本节用事件研究 CAR[-1,+1] 直接回答论文核心问题。下方 7 条机制假说回答的是 CN/US 反应不一致的来源,**不是**回答"是否上涨"本身。

> **Disclosure: post-hoc, not pre-registered.** 下方 7 条 CMA 假说(H1–H7)是在观察到 announce-vs-effective 不对称结果**之后**形成的,属于 post-hoc 解释。本项目**未公开 Pre-Analysis Plan (PAP)**。意涵:
>
> - Verdict 阈值(默认 p<0.10、inner=0.05)合理但**未事前承诺**。
> - 多重检验校正(Bonferroni、Benjamini-Hochberg)已在 `cma_hypothesis_verdicts.csv` 中报告,但是在假说选定之**后**应用的。
> - 样本量限制是数据本身的约束(如 H2 n=12 来自 Federal Reserve Z.1 年度数据),不是看到结果后再剔除样本。
>
> 论文主表建议**只引用 `evidence_tier=core` 的假说(H1/H5/H7)**,supplementary 走附录。下一轮迭代前请按 `docs/verdict_iteration.md` 的预注册流程冻结假说与阈值。完整数据与方法限制见 [docs/limitations.md](limitations.md)。

| 市场 | 阶段 | n | mean CAR[-1,+1] | t | p |
|---|---|---|---|---|---|
| CN | announce | 118 | +1.75% | 4.93 | 0.0000 |
| CN | effective | 118 | +0.42% | 0.93 | 0.3551 |
| US | announce | 318 | +1.47% | 5.19 | 0.0000 |
| US | effective | 318 | -0.12% | -0.51 | 0.6105 |

**论文核心发现**

- **公告日均显著正向**:CN +1.75% (t=4.93, p=0.0000)、US +1.47% (t=5.19, p=0.0000)。与 Shleifer (1986)、Harris-Gurel (1986)、Lynch-Mendenhall (1997) 等指数效应文献方向一致。
- **生效日效应基本消散**:CN +0.42% (p=0.3551)、US -0.12% (p=0.6105)。公告日已完成主要 price discovery,与 Greenwood-Sammon (2022) "S&P500 inclusion effect 已弱化" 的发现方向一致。
- **机制定位**:超额收益主要发生在公告日,意味着"为何上涨"的解释焦点应推向公告期机制(信息泄露 / 行业结构 / 关注度提升),而非生效日的被动配置需求冲击。

---

## 机制层裁决:CN/US 不对称的结构性来源

下面 7 条假说回答的是 "为什么 CN/US 在公告日 / 生效日的反应不一致",而**不是**直接回答 "指数纳入是否产生超额收益"(那个问题在上节已回答)。

基于跨市场不对称(CMA)pipeline 自动产出,7 条 CN/US 不对称机制假说的当前裁决分布: **3 项支持 / 4 项证据不足**。 详见 `results/real_tables/cma_hypothesis_verdicts.csv`。

### 样本概述

真实样本覆盖 2010–2025 年间 CSI300(CN)与 S&P500(US)的指数纳入事件: 共 **CN 137 起 / US 319 起 inclusion 事件**(`inclusion=1`)。 每个事件采用 [-20, +20] 交易日的事件窗口，匹配对照组按 sector × 同期市值 quintile 抽取。

### 方法概述

- **事件研究**: 公告日 (`announce`) 与生效日 (`effective`) 双窗口,
  CAR 窗口包括 `[-1,+1]` / `[-3,+3]` / `[-5,+5]`,长窗口取 `[0,+20]` / `[0,+60]` / `[0,+120]`。
- **匹配回归**: `treatment_group` 二值变量 + sector / 对数市值 / 事件前收益作为协变量,
  使用 HC3 异方差稳健标准误。
- **CMA 7 条机制假说**: 见下方逐项裁决，每条假说都有自动产出的 verdict + 头条指标。
  完整 metric pipeline 见 `index-inclusion-cma`(`results/real_tables/cma_*.csv`)。
- **HS300 RDD**: cutoff=300 的运行变量断点，公告日 `[-1,+1]` 主结果。

### H1 · 信息泄露与预运行 —— 证据不足(可信度:中)
**bootstrap p = 0.875**, n = 436
CN-US pre-runup 差异 0.50% 在 bootstrap 下不显著 (p=0.875, CI95=[-3.25%, 4.70%])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。
_细节_: CN pre-runup=3.09%; US pre-runup=2.59%; diff=0.50%, bootstrap p=0.875, CI95=[-3.25%, 4.70%]

### H2 · 被动基金 AUM 差异 —— 证据不足(可信度:低)
**US AUM ratio = 13.481**, n = 12
AUM 与 US 生效日 rolling CAR 的方向关系不稳定，当前不支持 H2。
_细节_: US AUM 0.99→13.37; US effective rolling CAR 0.04%→0.05%

### H3 · 散户 vs 机构结构 —— 支持(可信度:高)
**双通道命中率 = 0.750**, n = 4
US announce 与 CN effective 两条预期量能集中四象限均双通道显著 (turnover + volume p<0.10),共 3/4 个象限通过双通道判据。
_细节_: channel concentration 3/4 both-sig: CN announce=✓, CN effective=✓, US announce=✓, US effective=T

### H4 · 卖空约束 —— 证据不足(可信度:中)
**regression p = 0.537**, n = 436
控制 gap_length_days 后 CN-US gap_drift 差异 0.61% 不显著 (p=0.537)，跨市场差异口径无法支持 H4 套利约束解释。
_细节_: CN gap_drift=0.76%; US gap_drift=-0.33%; regression cn_coef=0.61%, p=0.537, n=436

### H5 · 涨跌停限制 —— 支持(可信度:高)
**limit_coef p = 0.008**, n = 936
CN 事件级涨跌停命中率正向预测 announce-day CAR (limit_coef=0.1549, p=0.008, R²=0.011, n=936)，支持 H5 涨跌停截断机制。
_细节_: CN limit_coef=0.1549, p=0.008, R²=0.011, n=936

### H6 · 指数权重可预测性 —— 证据不足(可信度:中)
**heavy−light spread = -0.019**, n = 67
CN 重权重 announce_jump 并不明显高于轻权重 (+1.29% vs +3.20%,spread=-1.90%),H6 不被支持。
_细节_: matched=67, median weight=0.0039, heavy announce_jump=+1.29%, light=+3.20%, spread=-1.90%; robustness: ols_weight coef=-0.0061, p=0.001; sector_fe_weight coef=-0.0436, p=1.000; median_quantreg_weight coef=-0.0057, p=0.312

### H7 · 行业结构差异 —— 支持(可信度:中)
**US sector spread = 5.954**, n = 187
US 行业间 asymmetry_index spread = 5.95(Materials +3.90 vs Real Estate -2.05),行业结构在 inclusion 效应中显著起作用。CN 状态:已分行业。
_细节_: US eligible sectors=8, max=+3.90(Materials), min=-2.05(Real Estate), spread=5.95, n=187; CN sectors=35

### 限制与稳健性补强方向

**通用稳健性补强**:

- HS300 RDD 当前已使用 L3 官方候选边界样本，覆盖 2020-11 到 2025-11 共 11 个批次；
  在扩展到 ≥10 年（约 20 批次）以前，RDD 结论仍应限定为初步识别证据，不可表述为完整中证官方历史 ranking score 因果结论。
- RDD 稳健性面板（main / donut / placebo / polynomial）已落到 `rdd_robustness.csv` 与首页 forest plot；
  论文写作时建议在主表脚注同时引用稳健性结果，避免只报告显著的 main spec。
- 跨市场比较默认按事件汇总(announce vs effective × CN vs US 4 象限),后续可叠加事件级
  bootstrap / permutation 检验，以及 sector × size 的交互检验，进一步压低单通道误判风险。
- 长窗口(>120 日)的 retention ratio 在样本量收缩时会跳到 NA,
  当前 demand_curve 主线主要靠 `[0,+60]` / `[0,+120]` 给出方向性结论。

### HS300 RDD 稳健性面板

`results/literature/hs300_rdd/rdd_robustness.csv` 在 main 局部线性的基础上跑了 4 类稳健性 spec，**统一锁定到 main 自动选出的 bandwidth**（避免 placebo cutoff 的样本-窗口漂移把 spec 噪声混进 τ）：

| 设定 | τ (CAR[-1,+1]) | p | n_obs | 解读 |
|---|---|---|---|---|
| main · 局部线性 | **+3.92%** | **0.048** | 120 | 边界显著 |
| donut(±0.01) | +4.93% | 0.102 | 102 | 扔近邻后变化 |
| placebo cutoff +0.05 | -1.98% | 0.184 | 130 | placebo 不显著（识别合理） |
| placebo cutoff -0.05 | -2.44% | 0.259 | 72 | placebo 不显著（识别合理） |
| polynomial order=2 | +0.37% | 0.921 | 120 | spec sensitivity（高阶项吸收跳跃） |

**论文级表述建议**：HS300 RDD main 在公告日 CAR[-1,+1] 上的边界显著（τ=3.92%, p=0.048, n=120）；placebo cutoff 的 τ 都接近 0 支持识别合理，但 donut / polynomial 提示效应对设定敏感。**结论应当限定为初步识别证据**，论文需如实报告全套稳健性面板。

---

## 附录:工程产品与复现框架

本仓库除论文核心实证外,还包含两个独立的工程产品。它们**不属于论文核心叙事**,
但支撑研究透明度与复现性,论文中可作为方法附录或补充材料引用:

### Dashboard(界面层)

`index-inclusion-dashboard` 提供一页式总展板,3 分钟汇报 / 展示 / 完整材料三种模式,
支持 verdict ↔ literature 双向链接、ECharts 交互图表、真实证据卡 drilldown。
完整 CLI 参考见 [docs/cli_reference.md](cli_reference.md);架构见 [docs/dashboard_architecture.md](dashboard_architecture.md)。

### HS300 RDD L3 候选采集工作台

`/rdd-l3` 浏览器工作台 + `index-inclusion-prepare-hs300-rdd` / `reconstruct-hs300-rdd` / `plan-hs300-rdd-l3` CLI 工具链,
覆盖从公开重建(L2)到中证官方候选(L3)的完整采集流程。
完整工作流见 [docs/hs300_rdd_workflow.md](hs300_rdd_workflow.md)。
