## 主结论:指数纳入是否产生显著超额收益

本节用事件研究 CAR[-1,+1] 直接回答论文核心问题。下方 7 条机制假说回答的是 CN/US 反应不一致的来源,**不是**回答"是否上涨"本身。

> **Disclosure: post-hoc, not pre-registered.** 下方 7 条 CMA 假说(H1–H7)是在观察到 announce-vs-effective 不对称结果**之后**形成的,属于 post-hoc 解释。本项目**未公开 Pre-Analysis Plan (PAP)**。意涵:
>
> - Verdict 阈值(默认 p<0.10、inner=0.05)合理但**未事前承诺**。
> - 多重检验校正(Bonferroni、Benjamini-Hochberg)已在 `cma_hypothesis_verdicts.csv` 中报告,但是在假说选定之**后**应用的。
> - 样本量限制是数据本身的约束(如 H2 n=12 来自 Federal Reserve Z.1 年度数据),不是看到结果后再剔除样本。
>
> 论文主表建议**只引用 `evidence_tier=core` 的假说(H1/H5/H7)**,supplementary 走附录。这 7 条裁决为 post-hoc 探索性结论;若后续要做 confirmatory 检验,须在新样本上事前登记假说与阈值(参见 `docs/verdict_iteration.md`)。完整数据与方法限制见 [docs/limitations.md](limitations.md)。

| 市场 | 阶段 | n | mean CAR[-1,+1] | t | p |
|---|---|---|---|---|---|
| CN | announce | 137 | +2.07% | 6.48 | 0.0000 |
| CN | effective | 137 | +0.49% | 1.25 | 0.2117 |
| US | announce | 255 | +1.87% | 5.34 | 0.0000 |
| US | effective | 258 | -0.15% | -0.54 | 0.5879 |

**论文核心发现**

- **公告日均显著正向**:CN +2.07% (t=6.48, p=0.0000)、US +1.87% (t=5.34, p=0.0000)。与 Shleifer (1986)、Harris-Gurel (1986)、Lynch-Mendenhall (1997) 等指数效应文献方向一致。
- **生效日效应基本消散**:CN +0.49% (p=0.2117)、US -0.15% (p=0.5879)。公告日已完成主要 price discovery,与 Greenwood-Sammon (2022) "S&P500 inclusion effect 已弱化" 的发现方向一致。
- **机制定位**:超额收益主要发生在公告日,意味着"为何上涨"的解释焦点应推向公告期机制(信息泄露 / 行业结构 / 关注度提升),而非生效日的被动配置需求冲击。

---

## 机制层裁决:CN/US 不对称的结构性来源

下面 7 条假说回答的是 "为什么 CN/US 在公告日 / 生效日的反应不一致",而**不是**直接回答 "指数纳入是否产生超额收益"(那个问题在上节已回答)。

基于跨市场不对称(CMA)pipeline 自动产出,7 条 CN/US 不对称机制假说的当前裁决分布: **2 项支持 / 5 项证据不足**。 详见 `results/real_tables/cma_hypothesis_verdicts.csv`。

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
**bootstrap p = 0.965**, n = 455
CN-US pre-runup 差异 0.14% 在 bootstrap 下不显著 (p=0.965, CI95=[-3.22%, 3.96%])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。
_细节_: CN pre-runup=2.75%; US pre-runup=2.61%; diff=0.14%, bootstrap p=0.965, CI95=[-3.22%, 3.96%]

### H2 · 被动基金 AUM 差异 —— 证据不足(可信度:低)
**US AUM ratio = 13.481**, n = 18
两市场 AUM 与 effective rolling CAR 的方向关系不一致或不支持 H2。(双市场覆盖)
_细节_: US AUM 0.99→13.37 (2010→2025), effective CAR 0.04%→0.05% (2014→2025); CN AUM 0.19→1.12 (2020→2024), effective CAR -0.36%→0.65% (2020→2025)

### H3 · 散户 vs 机构结构 —— 支持(可信度:低)
**双通道命中率 = 0.500**, n = 4
US announce 与 CN effective 两条预期量能集中四象限均双通道显著 (turnover + volume p<0.10),共 2/4 个象限通过双通道判据。（功效不足: power=0.05，低于 0.50 阈值；为方向性/描述性证据，不作高置信结论）
_细节_: channel concentration 2/4 both-sig: CN announce=·, CN effective=✓, US announce=✓, US effective=T

### H4 · 卖空约束 —— 证据不足(可信度:低)
**regression p = 0.604**, n = 455
控制 gap_length_days 后 CN-US gap_drift 差异 0.50% 不显著 (p=0.604)，跨市场差异口径无法支持 H4 套利约束解释。（功效不足: power=0.08，低于 0.50 阈值；为方向性/描述性证据，不作高置信结论）
_细节_: CN gap_drift=0.63%; US gap_drift=-0.34%; regression cn_coef=0.50%, p=0.604, n=455

### H5 · 涨跌停限制 —— 证据不足(可信度:低)
**limit_coef p = 0.427**, n = 1096
CN 事件级涨跌停命中率对 announce-day CAR 不具显著预测力 (limit_coef=0.0744, p=0.427, n=1096)，H5 缺乏支持。（功效不足: power=0.12，低于 0.50 阈值；为方向性/描述性证据，不作高置信结论）
_细节_: CN limit_coef=0.0744, p=0.427, R²=0.003, n=1096

### H6 · 指数权重可预测性 —— 证据不足(可信度:中)
**heavy−light spread = -0.016**, n = 87
CN 重权重 announce_jump 并不明显高于轻权重 (+1.65% vs +3.28%,spread=-1.63%),H6 不被支持。
_细节_: matched=87, median weight=0.0035, heavy announce_jump=+1.65%, light=+3.28%, spread=-1.63%; robustness: ols_weight coef=-0.0057, p=0.001; sector_fe_weight coef=-0.0305, p=1.000; median_quantreg_weight coef=-0.0051, p=0.266; permutation_quartile_spread coef=-0.0181, p=0.091

### H7 · 行业结构差异 —— 支持(可信度:中)
**US sector spread = 5.973**, n = 187
US 行业间 asymmetry_index spread = 5.97(Materials +3.92 vs Real Estate -2.05),行业结构在 inclusion 效应中显著起作用。CN 状态:已分行业。 进一步的 sector×phase/treatment 交互回归在 US 显著(joint p=0.095, n=1930)，增强 H7 的机制支持。
_细节_: US eligible sectors=8, max=+3.92(Materials), min=-2.05(Real Estate), spread=5.97, n=187; CN sectors=41; interaction US joint p=0.095, top=effective_x_sector_Industrials, n=1930

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
| main · 局部线性 | **+2.96%** | **0.095** | 148 | 边界 marginal |
| donut(±0.01) | +3.84% | 0.165 | 124 | 扔近邻后变化 |
| placebo cutoff +0.05 | -2.00% | 0.116 | 174 | placebo 不显著（识别合理） |
| placebo cutoff -0.05 | -0.63% | 0.770 | 84 | placebo 不显著（识别合理） |
| polynomial order=2 | -0.25% | 0.939 | 148 | spec sensitivity（高阶项吸收跳跃） |

**论文级表述建议**：HS300 RDD main 在公告日 CAR[-1,+1] 上的边界显著（τ=2.96%, p=0.095, n=148）；placebo cutoff 的 τ 都接近 0 支持识别合理，但 donut / polynomial 提示效应对设定敏感。**结论应当限定为初步识别证据**，论文需如实报告全套稳健性面板。

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
