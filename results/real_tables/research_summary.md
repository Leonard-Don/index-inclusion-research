# 研究结果自动摘要

## 一、事件研究主结论

- CN 市场在公告日 `CAR[-1,+1]` 的平均值为 `0.0175`，方向为正向，t 值为 `4.9273`，p 值为 `0.0000`，统计上显著。
- US 市场在公告日 `CAR[-1,+1]` 的平均值为 `0.0147`，方向为正向，t 值为 `5.1933`，p 值为 `0.0000`，统计上显著。
- CN 市场在生效日 `CAR[-1,+1]` 的平均值为 `0.0042`，方向为正向，t 值为 `0.9285`，p 值为 `0.3551`，统计上不显著。
- US 市场在生效日 `CAR[-1,+1]` 的平均值为 `-0.0012`，方向为负向，t 值为 `-0.5099`，p 值为 `0.6105`，统计上不显著。

## 二、机制检验摘要

- CN 市场 announce 阶段中，处理组变量对 `turnover_change` 的系数为 `-0.0000`，表现为负相关，统计上不显著。
- CN 市场 effective 阶段中，处理组变量对 `turnover_change` 的系数为 `0.0011`，表现为正相关，统计上显著。
- US 市场 announce 阶段中，处理组变量对 `turnover_change` 的系数为 `0.0277`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `turnover_change` 的系数为 `-0.0278`，表现为负相关，统计上显著。
- CN 市场 announce 阶段中，处理组变量对 `volume_change` 的系数为 `0.0212`，表现为正相关，统计上不显著。
- CN 市场 effective 阶段中，处理组变量对 `volume_change` 的系数为 `0.0481`，表现为正相关，统计上显著。
- US 市场 announce 阶段中，处理组变量对 `volume_change` 的系数为 `1.2367`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `volume_change` 的系数为 `-0.5643`，表现为负相关，统计上显著。
- CN 市场 announce 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0001`，表现为正相关，统计上不显著。
- CN 市场 effective 阶段中，处理组变量对 `volatility_change` 的系数为 `-0.0022`，表现为负相关，统计上显著。
- US 市场 announce 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0046`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0004`，表现为正相关，统计上不显著。

## 三、识别与证据状态

- 当前中国 RDD 扩展的证据等级为 `L3`，证据状态为 `正式边界样本`。
- 当前来源为 `正式候选样本文件`，对应模式 `real`。
- 覆盖与校验说明：356 条候选；11 个批次；调入 191 / 对照 165；11 个批次覆盖 cutoff 两侧。

## 四、论文可直接使用的讨论句式

- 若公告日效应强于生效日，可解释为投资者将纳入指数视作质量背书，信息效应占主导。
- 若生效日效应强于公告日，可解释为被动指数基金在调仓时点集中买入，需求冲击更关键。
- 若成交量和换手率同步上升，说明纳入指数伴随着交易活跃度和流动性改善。
- 若波动率也明显抬升，则表明指数纳入可能伴随短期交易拥挤和价格压力。

## 五、模型覆盖情况

- CN announce main_car：样本量 `936`，R² 为 `0.0796`。
- CN announce turnover_mechanism：样本量 `936`，R² 为 `0.0313`。
- CN announce volatility_mechanism：样本量 `936`，R² 为 `0.0026`。
- CN announce volume_mechanism：样本量 `936`，R² 为 `0.0412`。
- CN effective main_car：样本量 `936`，R² 为 `0.0869`。
- CN effective turnover_mechanism：样本量 `936`，R² 为 `0.0211`。
- CN effective volatility_mechanism：样本量 `936`，R² 为 `0.0085`。
- CN effective volume_mechanism：样本量 `936`，R² 为 `0.0297`。
- US announce main_car：样本量 `1366`，R² 为 `0.0402`。
- US announce turnover_mechanism：样本量 `1366`，R² 为 `0.3522`。
- US announce volatility_mechanism：样本量 `1360`，R² 为 `0.0218`。
- US announce volume_mechanism：样本量 `1366`，R² 为 `0.3649`。
- US effective main_car：样本量 `1368`，R² 为 `0.0026`。
- US effective turnover_mechanism：样本量 `1368`，R² 为 `0.2755`。
- US effective volatility_mechanism：样本量 `1368`，R² 为 `0.0258`。
- US effective volume_mechanism：样本量 `1368`，R² 为 `0.0786`。

## 六、美股 vs A股 不对称

### 4 象限 CAR[-1,+1] 摘要
- CN announce：CAR[-1,+1] = `0.0175`，t = `4.93`，n = `118`
- CN effective：CAR[-1,+1] = `0.0042`，t = `0.93`，n = `118`
- US announce：CAR[-1,+1] = `0.0147`，t = `5.19`，n = `318`
- US effective：CAR[-1,+1] = `-0.0012`，t = `-0.51`，n = `318`

### 空窗期与生效日
- CN gap_length_days：均值 `14.0000`，t = `nan`，n = `137`
- CN pre_announce_runup：均值 `0.0309`，t = `3.06`，n = `118`
- CN announce_jump：均值 `0.0175`，t = `4.93`，n = `118`
- CN gap_drift：均值 `0.0076`，t = `1.04`，n = `118`
- CN effective_jump：均值 `0.0042`，t = `0.93`，n = `118`
- CN post_effective_reversal：均值 `-0.0180`，t = `-1.96`，n = `118`
- US gap_length_days：均值 `7.0878`，t = `24.86`，n = `319`
- US pre_announce_runup：均值 `0.0259`，t = `5.39`，n = `318`
- US announce_jump：均值 `0.0147`，t = `5.19`，n = `318`
- US gap_drift：均值 `-0.0033`，t = `-1.21`，n = `318`
- US effective_jump：均值 `-0.0012`，t = `-0.51`，n = `318`
- US post_effective_reversal：均值 `-0.0068`，t = `-1.61`，n = `318`

### 机制差异（no_fe）
- CN announce car_1_1：coef = `0.0157`，t = `3.98`
- CN announce turnover_change：coef = `0.0015`，t = `2.91`
- CN announce price_limit_hit_share：coef = `0.0041`，t = `1.71`
- CN effective car_1_1：coef = `0.0063`，t = `1.30`
- CN effective turnover_change：coef = `0.0014`，t = `2.57`
- CN effective price_limit_hit_share：coef = `-0.0019`，t = `-0.65`
- US announce car_1_1：coef = `0.0140`，t = `4.36`
- US announce turnover_change：coef = `0.0297`，t = `20.28`
- US announce price_limit_hit_share：coef = `0.0036`，t = `1.32`
- US effective car_1_1：coef = `0.0005`，t = `0.18`
- US effective turnover_change：coef = `0.0026`，t = `1.97`
- US effective price_limit_hit_share：coef = `0.0058`，t = `2.36`

### CN/US 不对称机制裁决

下面 7 条假说回答的是 "为什么 CN/US 反应不一致",不是回答 "指数纳入是否产生超额收益"(后者见上文事件研究主结论)。

| 假说 | 名称 | 裁决 | 可信度 | 头条指标 | 值 | n | 关键证据 |
|---|---|---|---|---|---|---|---|
| H1 | 信息泄露与预运行 | 证据不足 | 中 | bootstrap p | 0.875 | 436 | CN-US pre-runup 差异 0.50% 在 bootstrap 下不显著 (p=0.875, CI95=[-3.25%, 4.70%])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。 |
| H2 | 被动基金 AUM 差异 | 证据不足 | 低 | US AUM ratio | 13.481 | 12 | AUM 与 US 生效日 rolling CAR 的方向关系不稳定，当前不支持 H2。 |
| H3 | 散户 vs 机构结构 | 支持 | 高 | 双通道命中率 | 0.750 | 4 | US announce 与 CN effective 两条预期量能集中四象限均双通道显著 (turnover + volume p<0.10),共 3/4 个象限通过双通道判据。 |
| H4 | 卖空约束 | 证据不足 | 中 | regression p | 0.537 | 436 | 控制 gap_length_days 后 CN-US gap_drift 差异 0.61% 不显著 (p=0.537)，跨市场差异口径无法支持 H4 套利约束解释。 |
| H5 | 涨跌停限制 | 支持 | 高 | limit_coef p | 0.008 | 936 | CN 事件级涨跌停命中率正向预测 announce-day CAR (limit_coef=0.1549, p=0.008, R²=0.011, n=936)，支持 H5 涨跌停截断机制。 |
| H6 | 指数权重可预测性 | 证据不足 | 中 | heavy−light spread | -0.019 | 67 | CN 重权重 announce_jump 并不明显高于轻权重 (+1.29% vs +3.20%,spread=-1.90%),H6 不被支持。 |
| H7 | 行业结构差异 | 支持 | 中 | US sector spread | 5.954 | 187 | US 行业间 asymmetry_index spread = 5.95(Materials +3.90 vs Real Estate -2.05),行业结构在 inclusion 效应中显著起作用。CN 状态:已分行业。 |
