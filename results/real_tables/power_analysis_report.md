# 假说后验统计功效分析

对各假说做 post-hoc 功效计算 (H3 / H4 / H5 / H6 单口径 + H1 / H2 分引擎)，α=0.05, target power = 80%。

## 1. 功效一览表

| 假说 | 名称 | n | 测试族 | 观测效应 | 在观测效应下的功效 | 80% 功效下的 MDE |
|---|---|---:|---|---:|---:|---:|
| H3 | 散户 vs 机构结构 | 4 | binomial_proportion_z_two_sided | +0.000 (hit_rate − 0.5) | 0.050 | 0.499 (proportion_gap_p1_minus_p0) |
| H4 | 卖空约束 | 455 | regression_coef_t_two_sided | +0.005 (cn_coef_gap_drift) | 0.081 | 0.027 (coef_at_target_power) |
| H5 | 涨跌停限制 | 1096 | regression_coef_t_two_sided | +0.074 (limit_coef_announce_car) | 0.125 | 0.263 (coef_at_target_power) |
| H6 | 指数权重可预测性 | 87 | one_sample_t_two_sided | -0.473 (cohens_d) | 0.992 | 0.304 (cohens_d_at_target_power) |
| H1 | 信息泄露与预运行 | 436 | bootstrap_diff_two_sided | +0.005 (cn_minus_us_pre_runup) | 0.057 | 0.057 (diff_at_target_power) |
| H1 | 信息泄露与预运行 | 436 | bootstrap_diff_two_sided | +0.021 (cn_minus_us_pre_runup) | 0.839 | 0.020 (diff_at_target_power) |
| H2 | 被动基金 AUM 差异 | 15 | one_sample_t_two_sided | -0.037 (cohens_d_car_delta) | 0.052 | 0.778 (cohens_d_at_target_power) |
| H2 | 被动基金 AUM 差异 | 15 | one_sample_t_two_sided | -0.365 (cohens_d_car_delta) | 0.261 | 0.778 (cohens_d_at_target_power) |

## 2. 逐假说释义

### H3 · 散户 vs 机构结构 (n=4)

normal-approx power=0.050 · exact-binomial power=0.000 · posterior P(p>0.60|data)=0.317 (Beta(1,1) uniform prior). MDE@80%=+0.499 概率差（p1≈0.999）。严重欠功效 (power=0.05 < 0.30): n=4 无法在 α=0.05 检出真实命中率 50%；结果按 supplementary 处理是合理的。

**额外指标**:

- `exact_power` (精确二项功效) = 0.0000
- `bayes_p_gt_0.60` (后验 P(p>0.60)) = 0.3174
- `successes` (成功次数) = 2.0000

### H4 · 卖空约束 (n=455)

HC3 regression coef=+0.0050 (SE=0.0097, t=+0.519, p=0.6037), df≈452; two-sided t-test power=0.081. MDE@80% = |coef|≈0.0272 (≈ 5.4× the observed coefficient). 严重欠功效 (power=0.08 < 0.30): n=455 下观测系数 +0.0050 太小 (相对 SE=0.0097)，无法在 α=0.05 下稳健检出。证据不足的判定是 n 不够大，不是 H4 一定错，因此保留为 supplementary 是合理的。

**额外指标**:

- `coef_observed` (观测系数) = 0.0050
- `se_observed` (系数标准误) = 0.0097
- `t_observed` (t 统计量) = 0.5190
- `p_value_observed` (p 值) = 0.6037
- `n_covariates` (协变量数) = 2.0000

### H5 · 涨跌停限制 (n=1096)

HC3 regression coef=+0.0744 (SE=0.0937, t=+0.794, p=0.4270), df≈1094; two-sided t-test power=0.125. MDE@80% = |coef|≈0.2628 (≈ 3.53× the observed coefficient). 严重欠功效 (power=0.12 < 0.30): n=1096 下仍无法稳健检出该效应。

**额外指标**:

- `coef_observed` (观测系数) = 0.0744
- `se_observed` (系数标准误) = 0.0937
- `t_observed` (t 统计量) = 0.7943
- `p_value_observed` (p 值) = 0.4270
- `n_covariates` (协变量数) = 1.0000

### H6 · 指数权重可预测性 (n=87)

Cohen's d (observed) ≈ -0.473 (bucket-SD=0.0325); two-sided t-test power=0.992. 对比小/中/大效应 (d=0.2/0.5/0.8) 的功效 = 0.45 / 1.00 / 1.00. MDE@80% = |d|=0.304. 功效充足 (power=0.99 >= 0.80) — n 足以检出该效应，但观测方向 (heavy<light) 与 H6 预测 (heavy>light) 相反，所以 verdict='证据不足' 并不是 n 不够，而是方向不符。

**额外指标**:

- `cohens_d_observed` (Cohen d) = -0.4728
- `power_at_d_0.20` (d=0.20 功效) = 0.4542
- `power_at_d_0.50` (d=0.50 功效) = 0.9960
- `power_at_d_0.80` (d=0.80 功效) = 1.0000

### H1 · 信息泄露与预运行 (n=436)

engine=adjusted: diff=+0.0050, bootstrap SE≈0.0203, bootstrap p=0.8748; 在该 SE 与 n=436 下，两侧 z-test 功效=0.057; MDE@80%≈|diff|=0.0569. 功效偏低 (power=0.06 < 0.50): adjusted 引擎下 bootstrap SE=0.0203 太宽，差异需达到 |diff|≈0.0569 才能在 80% 功效下被检出。

**额外指标**:

- `bootstrap_se` (Bootstrap 标准误) = 0.0203
- `bootstrap_p_value` (Bootstrap p 值) = 0.8748
- `ci_low` (CI 下界) = -0.0325
- `ci_high` (CI 上界) = 0.0470

### H1 · 信息泄露与预运行 (n=436)

engine=market: diff=+0.0206, bootstrap SE≈0.0070, bootstrap p=0.0004; 在该 SE 与 n=436 下，两侧 z-test 功效=0.839; MDE@80%≈|diff|=0.0196. 功效充足 (power=0.84 >= 0.80): market 引擎下 n=436 足以检出 |diff|≈0.0206 的真实效应。

**额外指标**:

- `bootstrap_se` (Bootstrap 标准误) = 0.0070
- `bootstrap_p_value` (Bootstrap p 值) = 0.0004
- `ci_low` (CI 下界) = 0.0084
- `ci_high` (CI 上界) = 0.0358

### H2 · 被动基金 AUM 差异 (n=15)

engine=adjusted: Cohen's d≈-0.037 (empirical deltas (n_used=15, SD=0.0029)); n_combined=15, two-sided t-test power=0.052, MDE@80%=|d|=0.778. 功效偏低 (power=0.05 < 0.50): adjusted 引擎下 样本 n=15 太小，|d| 必须 ≥0.778 才能在 80% 功效下检出。

**额外指标**:

- `cohens_d` (Cohen d) = -0.0373
- `trend_sd` (趋势标准差) = 0.0029

### H2 · 被动基金 AUM 差异 (n=15)

engine=market: Cohen's d≈-0.365 (empirical deltas (n_used=15, SD=0.0037)); n_combined=15, two-sided t-test power=0.261, MDE@80%=|d|=0.778. 功效偏低 (power=0.26 < 0.50): market 引擎下 样本 n=15 太小，|d| 必须 ≥0.778 才能在 80% 功效下检出。

**额外指标**:

- `cohens_d` (Cohen d) = -0.3651
- `trend_sd` (趋势标准差) = 0.0037

## 3. 方法学说明

- H3 (n=4) 使用比例 z-test（正态近似）；同时提供 exact-binomial 对照。因为正态近似在小样本下偏乐观，**只有当两个计算给出相近结论**时才能把 H3 的判断扣在 normal-approx 上。
- H4 (n=436) 与 H5 (n=936) 使用 HC3 回归单系数 t-test：观测 ``coef/SE`` 作为非中心 t 的 ncp，``df = n − k − 1`` (k=协变量数)。MDE 是 ``80% 功效下能检出的最小 |coef|``，由非中心 t 反演的二分搜索给出；它和闭式 ``(z_{1-α/2}+z_{power})·SE`` 在 n 足够大时一致。
- H6 (n=67) 使用单样本 t-test，Cohen's *d* = mean / SD。在面板可用时以中位数 weight 切重/轻 bucket 并算 pooled SD；面板缺失时，回退到 H6 OLS-HC3 r²=0.033 反推的 |d|≈0.18。
- 80% 功效下的 MDE 由二分搜索求解；当 n 太小，returned MDE 可能超过实际可能的效应上界（H3 即如此：MDE≈0.50 意味着只有 p1≈1.0 才能在 80% 功效下检出）。
- Bayesian 后验 (H3) 默认采用 uniform Beta(1,1) 先验；先验是 Bayesian 陈述里最有争议的输入，更换需明确说明。

