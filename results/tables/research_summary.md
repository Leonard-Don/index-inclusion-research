# 研究结果自动摘要

## 一、事件研究主结论

- CN 市场在公告日 `CAR[-1,+1]` 的平均值为 `0.0120`，方向为正向，t 值为 `0.6809`，p 值为 `0.5662`，统计上不显著。
- US 市场在公告日 `CAR[-1,+1]` 的平均值为 `0.0417`，方向为正向，t 值为 `6.2361`，p 值为 `0.0248`，统计上显著。
- CN 市场在生效日 `CAR[-1,+1]` 的平均值为 `0.0430`，方向为正向，t 值为 `3.3357`，p 值为 `0.0793`，统计上显著。
- US 市场在生效日 `CAR[-1,+1]` 的平均值为 `0.0787`，方向为正向，t 值为 `4.6544`，p 值为 `0.0432`，统计上显著。

## 二、机制检验摘要

- CN 市场 announce 阶段中，指数纳入变量对 `turnover_change` 的系数为 `0.0039`，表现为正相关，统计上显著。
- CN 市场 effective 阶段中，指数纳入变量对 `turnover_change` 的系数为 `0.0022`，表现为正相关，统计上显著。
- US 市场 announce 阶段中，指数纳入变量对 `turnover_change` 的系数为 `0.0019`，表现为正相关，统计上不显著。
- US 市场 effective 阶段中，指数纳入变量对 `turnover_change` 的系数为 `0.0071`，表现为正相关，统计上显著。
- CN 市场 announce 阶段中，指数纳入变量对 `volume_change` 的系数为 `0.0980`，表现为正相关，统计上显著。
- CN 市场 effective 阶段中，指数纳入变量对 `volume_change` 的系数为 `0.0591`，表现为正相关，统计上显著。
- US 市场 announce 阶段中，指数纳入变量对 `volume_change` 的系数为 `0.2038`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，指数纳入变量对 `volume_change` 的系数为 `0.0842`，表现为正相关，统计上显著。
- CN 市场 announce 阶段中，指数纳入变量对 `volatility_change` 的系数为 `0.0005`，表现为正相关，统计上不显著。
- CN 市场 effective 阶段中，指数纳入变量对 `volatility_change` 的系数为 `0.0079`，表现为正相关，统计上不显著。
- US 市场 announce 阶段中，指数纳入变量对 `volatility_change` 的系数为 `0.0126`，表现为正相关，统计上显著。
- US 市场 effective 阶段中，指数纳入变量对 `volatility_change` 的系数为 `-0.0108`，表现为负相关，统计上显著。

## 三、论文可直接使用的讨论句式

- 若公告日效应强于生效日，可解释为投资者将纳入指数视作质量背书，信息效应占主导。
- 若生效日效应强于公告日，可解释为被动指数基金在调仓时点集中买入，需求冲击更关键。
- 若成交量和换手率同步上升，说明纳入指数伴随着交易活跃度和流动性改善。
- 若波动率也明显抬升，则表明指数纳入可能伴随短期交易拥挤和价格压力。

## 四、模型覆盖情况

- CN announce main_car：样本量 `12`，R² 为 `0.3320`。
- CN announce turnover_mechanism：样本量 `12`，R² 为 `0.4982`。
- CN announce volatility_mechanism：样本量 `12`，R² 为 `0.1877`。
- CN announce volume_mechanism：样本量 `12`，R² 为 `0.5556`。
- CN effective main_car：样本量 `12`，R² 为 `0.5537`。
- CN effective turnover_mechanism：样本量 `12`，R² 为 `0.1742`。
- CN effective volatility_mechanism：样本量 `12`，R² 为 `0.1581`。
- CN effective volume_mechanism：样本量 `12`，R² 为 `0.6120`。
- US announce main_car：样本量 `12`，R² 为 `0.7511`。
- US announce turnover_mechanism：样本量 `12`，R² 为 `0.1267`。
- US announce volatility_mechanism：样本量 `12`，R² 为 `0.5826`。
- US announce volume_mechanism：样本量 `12`，R² 为 `0.8703`。
- US effective main_car：样本量 `12`，R² 为 `0.5835`。
- US effective turnover_mechanism：样本量 `12`，R² 为 `0.6624`。
- US effective volatility_mechanism：样本量 `12`，R² 为 `0.3610`。
- US effective volume_mechanism：样本量 `12`，R² 为 `0.5360`。
