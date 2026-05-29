# 研究结果自动摘要

## 一、事件研究主结论

- CN 市场在公告日 `CAR[-1,+1]` 的平均值为 `0.0207`，方向为正向，t 值为 `6.4800`，p 值为 `0.0000`，统计上显著。
- US 市场在公告日 `CAR[-1,+1]` 的平均值为 `0.0187`，方向为正向，t 值为 `5.3356`，p 值为 `0.0000`，统计上显著。
- CN 市场在生效日 `CAR[-1,+1]` 的平均值为 `0.0049`，方向为正向，t 值为 `1.2549`，p 值为 `0.2117`，统计上不显著。
- US 市场在生效日 `CAR[-1,+1]` 的平均值为 `-0.0015`，方向为负向，t 值为 `-0.5426`，p 值为 `0.5879`，统计上不显著。

## 二、机制检验摘要

- CN 市场 announce 阶段中，处理组变量对 `turnover_change` 的系数为 `0.0013`，表现为正相关，统计上显著。
- CN 市场 effective 阶段中，处理组变量对 `turnover_change` 的系数为 `0.0028`，表现为正相关，统计上显著。
- US 市场 announce 阶段中，处理组变量对 `turnover_change` 的系数为 `0.0281`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `turnover_change` 的系数为 `-0.0283`，表现为负相关，统计上显著。
- CN 市场 announce 阶段中，处理组变量对 `volume_change` 的系数为 `0.1219`，表现为正相关，统计上显著。
- CN 市场 effective 阶段中，处理组变量对 `volume_change` 的系数为 `0.1216`，表现为正相关，统计上显著。
- US 市场 announce 阶段中，处理组变量对 `volume_change` 的系数为 `1.2301`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `volume_change` 的系数为 `-0.5658`，表现为负相关，统计上显著。
- CN 市场 announce 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0000`，表现为正相关，统计上不显著。
- CN 市场 effective 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0010`，表现为正相关，统计上不显著。
- US 市场 announce 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0047`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，处理组变量对 `volatility_change` 的系数为 `0.0000`，表现为正相关，统计上不显著。

## 三、识别与证据状态

- 当前中国 RDD 扩展的证据等级为 `L3`，证据状态为 `正式边界样本`。
- 当前来源为 `正式候选样本文件`，对应模式 `real`。
- 覆盖与校验说明：356 条候选；11 个批次；调入 191 / 对照 165；11 个批次覆盖断点两侧。

## 四、论文可直接使用的讨论句式

- 若公告日效应强于生效日，可解释为投资者将纳入指数视作质量背书，信息效应占主导。
- 若生效日效应强于公告日，可解释为被动指数基金在调仓时点集中买入，需求冲击更关键。
- 若成交量和换手率同步上升，说明纳入指数伴随着交易活跃度和流动性改善。
- 若波动率也明显抬升，则表明指数纳入可能伴随短期交易拥挤和价格压力。

## 五、模型覆盖情况

- CN announce main_car：样本量 `1096`，R² 为 `0.0604`。
- CN announce turnover_mechanism：样本量 `1096`，R² 为 `0.0134`。
- CN announce volatility_mechanism：样本量 `1096`，R² 为 `0.0016`。
- CN announce volume_mechanism：样本量 `1096`，R² 为 `0.0311`。
- CN effective main_car：样本量 `1096`，R² 为 `0.0862`。
- CN effective turnover_mechanism：样本量 `1096`，R² 为 `0.0396`。
- CN effective volatility_mechanism：样本量 `1096`，R² 为 `0.0105`。
- CN effective volume_mechanism：样本量 `1096`，R² 为 `0.0557`。
- US announce main_car：样本量 `1394`，R² 为 `0.0344`。
- US announce turnover_mechanism：样本量 `1394`，R² 为 `0.3531`。
- US announce volatility_mechanism：样本量 `1388`，R² 为 `0.0209`。
- US announce volume_mechanism：样本量 `1394`，R² 为 `0.3620`。
- US effective main_car：样本量 `1396`，R² 为 `0.0072`。
- US effective turnover_mechanism：样本量 `1396`，R² 为 `0.2806`。
- US effective volatility_mechanism：样本量 `1396`，R² 为 `0.0229`。
- US effective volume_mechanism：样本量 `1396`，R² 为 `0.0793`。
