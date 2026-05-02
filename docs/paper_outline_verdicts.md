## 假说裁决叙述

基于跨市场不对称(CMA)pipeline 自动产出,7 条机制假说的当前裁决分布: **1 项支持 / 1 项部分支持 / 5 项证据不足**。 详见 `results/real_tables/cma_hypothesis_verdicts.csv`。

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
**bootstrap p = 0.640**, n = 436
CN-US pre-runup 差异 0.50% 在 bootstrap 下不显著 (p=0.640, CI95=[-1.65%, 2.74%])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。
_细节_: CN pre-runup=3.09%; US pre-runup=2.59%; diff=0.50%, bootstrap p=0.640, CI95=[-1.65%, 2.74%]

### H2 · 被动基金 AUM 差异 —— 证据不足(可信度:低)
**US AUM ratio = 13.481**, n = 12
AUM 与 US 生效日 rolling CAR 的方向关系不稳定，当前不支持 H2。
_细节_: US AUM 0.99→13.37; US effective rolling CAR 0.04%→0.05%

### H3 · 散户 vs 机构结构 —— 部分支持(可信度:中)
**双通道命中率 = 0.500**, n = 4
仅 US announce 一个预期象限双通道显著，共 2/4 个象限通过双通道判据，另一条预期象限只有单通道显著，不能完全确认 H3。
_细节_: channel concentration 2/4 both-sig: CN announce=✓, CN effective=T, US announce=✓, US effective=T

### H4 · 卖空约束 —— 证据不足(可信度:中)
**regression p = 0.537**, n = 436
控制 gap_length_days 后 CN-US gap_drift 差异 0.61% 不显著 (p=0.537)，跨市场差异口径无法支持 H4 套利约束解释。
_细节_: CN gap_drift=0.76%; US gap_drift=-0.33%; regression cn_coef=0.61%, p=0.537, n=436

### H5 · 涨跌停限制 —— 证据不足(可信度:中)
**limit_coef p = 0.213**, n = 936
CN 事件级涨跌停命中率对 announce-day CAR 不具显著预测力 (limit_coef=0.0757, p=0.213, n=936)，H5 缺乏支持。
_细节_: CN limit_coef=0.0757, p=0.213, R²=0.006, n=936

### H6 · 指数权重可预测性 —— 证据不足(可信度:中)
**heavy−light spread = -0.019**, n = 67
CN 重权重 announce_jump 并不明显高于轻权重 (+1.29% vs +3.20%,spread=-1.90%),H6 不被支持。
_细节_: matched=67, median weight=0.0039, heavy announce_jump=+1.29%, light=+3.20%, spread=-1.90%

### H7 · 行业结构差异 —— 支持(可信度:中)
**US sector spread = 5.954**, n = 187
US 行业间 asymmetry_index spread = 5.95(Materials +3.90 vs Real Estate -2.05),行业结构在 inclusion 效应中显著起作用。CN 状态:已分行业。
_细节_: US eligible sectors=8, max=+3.90(Materials), min=-2.05(Real Estate), spread=5.95, n=187; CN sectors=35

### 限制与稳健性补强方向

**通用稳健性补强**:

- HS300 RDD 当前 `running_variable` 是公开重建排名(顶=600..尾=1),不等同于真实流通市值;
  正式批次候选样本(L3)上线前，RDD 结论限定为公开重建口径，不可表述为中证官方历史候选排名。
- 跨市场比较默认按事件汇总(announce vs effective × CN vs US 4 象限),后续可叠加事件级
  bootstrap / permutation 检验，以及 sector × size 的交互检验，进一步压低单通道误判风险。
- 长窗口(>120 日)的 retention ratio 在样本量收缩时会跳到 NA,
  当前 demand_curve 主线主要靠 `[0,+60]` / `[0,+120]` 给出方向性结论。
