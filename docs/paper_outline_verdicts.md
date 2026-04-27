## 假说裁决叙述

基于跨市场不对称(CMA)pipeline 自动产出,7 条机制假说的当前裁决分布: **3 项部分支持 / 3 项证据不足 / 1 项待补数据**。 详见 `results/real_tables/cma_hypothesis_verdicts.csv`。

### H1 · 信息泄露与预运行 —— 证据不足(可信度:中)
**bootstrap p = 0.640**, n = 436
CN-US pre-runup 差异 0.50% 在 bootstrap 下不显著 (p=0.640, CI95=[-1.65%, 2.74%])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。
_细节_: CN pre-runup=3.09%; US pre-runup=2.59%; diff=0.50%, bootstrap p=0.640, CI95=[-1.65%, 2.74%]

### H2 · 被动基金 AUM 差异 —— 待补数据(可信度:低)
当前有 rolling CAR，但缺少被动 AUM 年度数据，不能检验 AUM 上升与生效日效应衰减的关系。
_细节_: NA

### H3 · 散户 vs 机构结构 —— 部分支持(可信度:中)
**双通道命中率 = 0.500**, n = 4
仅 US announce 一个预期象限双通道显著, 共 2/4 个象限通过双通道判据,另一条预期象限只有单通道显著, 不能完全确认 H3。
_细节_: channel concentration 2/4 both-sig: CN announce=✓, CN effective=T, US announce=✓, US effective=T

### H4 · 卖空约束 —— 证据不足(可信度:中)
**regression p = 0.537**, n = 436
控制 gap_length_days 后 CN-US gap_drift 差异 0.61% 不显著 (p=0.537)，跨市场差异口径无法支持 H4 套利约束解释。
_细节_: CN gap_drift=0.76%; US gap_drift=-0.33%; regression cn_coef=0.61%, p=0.537, n=436

### H5 · 涨跌停限制 —— 证据不足(可信度:中)
**limit_coef p = 0.213**, n = 936
CN 事件级涨跌停命中率对 announce-day CAR 不具显著预测力 (limit_coef=0.0757, p=0.213, n=936)，H5 缺乏支持。
_细节_: CN limit_coef=0.0757, p=0.213, R²=0.006, n=936

### H6 · 指数权重可预测性 —— 部分支持(可信度:低)
**Q1Q2−Q4Q5 spread = 1.172**, n = 118
CN 小市值 cell 的不对称指数高于大市值，方向符合权重难预判的 proxy 解释。
_细节_: CN size Q1-Q2 avg=1.11; Q4-Q5 avg=-0.07

### H7 · 行业结构差异 —— 部分支持(可信度:低)
**US sector spread = 5.954**, n = 187
US 行业间 asymmetry_index spread = 5.95(Materials +3.90 vs Real Estate -2.05),行业结构在 inclusion 效应中显著起作用。CN 状态:待补 sector。
_细节_: US eligible sectors=8, max=+3.90(Materials), min=-2.05(Real Estate), spread=5.95, n=187; CN sector 未填充
