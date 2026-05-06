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
- US 市场 announce 阶段中，处理组变量对 `volume_change` 的系数为 `1.2351`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `volume_change` 的系数为 `-0.5659`，表现为负相关，统计上显著。
- CN 市场 announce 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0001`，表现为正相关，统计上不显著。
- CN 市场 effective 阶段中，处理组变量对 `volatility_change` 的系数为 `-0.0022`，表现为负相关，统计上显著。
- US 市场 announce 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0045`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0003`，表现为正相关，统计上不显著。

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
- US announce main_car：样本量 `1390`，R² 为 `0.0403`。
- US announce turnover_mechanism：样本量 `1390`，R² 为 `0.3529`。
- US announce volatility_mechanism：样本量 `1384`，R² 为 `0.0205`。
- US announce volume_mechanism：样本量 `1390`，R² 为 `0.3649`。
- US effective main_car：样本量 `1392`，R² 为 `0.0030`。
- US effective turnover_mechanism：样本量 `1392`，R² 为 `0.2766`。
- US effective volatility_mechanism：样本量 `1392`，R² 为 `0.0245`。
- US effective volume_mechanism：样本量 `1392`，R² 为 `0.0788`。
