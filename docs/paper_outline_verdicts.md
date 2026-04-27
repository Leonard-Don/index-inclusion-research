## 假说裁决叙述

基于跨市场不对称(CMA)pipeline 自动产出,7 条机制假说的当前裁决分布: **6 项证据不足 / 1 项待补数据**。 详见 `results/real_tables/cma_hypothesis_verdicts.csv`。

### H1 · 信息泄露与预运行 —— 证据不足(可信度:中)
**bootstrap p = 1.000**, n = 4
CN-US pre-runup 差异 0.00% 在 bootstrap 下不显著 (p=1.000, CI95=[0.00%, 0.00%])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。
_细节_: CN pre-runup=2.00%; US pre-runup=2.00%; diff=0.00%, bootstrap p=1.000, CI95=[0.00%, 0.00%]

### H2 · 被动基金 AUM 差异 —— 待补数据(可信度:低)
当前有 rolling CAR，但缺少被动 AUM 年度数据，不能检验 AUM 上升与生效日效应衰减的关系。
_细节_: NA

### H3 · 散户 vs 机构结构 —— 证据不足(可信度:中)
**双通道命中率 = 0.000**, n = 4
四象限内没有任何象限同时通过 turnover + volume 显著性 (共 0/4),单通道证据不足以支持 H3 量能集中机制。
_细节_: channel concentration 0/4 both-sig: CN announce=·, CN effective=·, US announce=·, US effective=·

### H4 · 卖空约束 —— 证据不足(可信度:中)
gap_drift 方向没有形成 CN 正、US 零或负的稳定对照。
_细节_: CN gap_drift=1.50%, t=NA; US gap_drift=1.50%, t=NA

### H5 · 涨跌停限制 —— 证据不足(可信度:中)
CN price_limit_hit_share 没有形成公告/生效双正向信号。
_细节_: CN announce limit coef=NA, t=NA; CN effective limit coef=NA, t=NA

### H6 · 指数权重可预测性 —— 证据不足(可信度:低)
**Q1Q2−Q4Q5 spread = 0.000**, n = 2
市值异质性没有显示小市值更强的不对称，当前 proxy 不支持 H6。
_细节_: CN size Q1-Q2 avg=2.23; Q4-Q5 avg=2.23

### H7 · 行业结构差异 —— 证据不足(可信度:低)
US sector 桶内 n>=10 的行业不足 2 个,无法计算跨行业 asymmetry spread。CN 状态:已分行业。
_细节_: US eligible sectors=0; CN sector 状态=已分行业
