# 指数纳入溢价的来源：来自中美两市场的信息渠道证据

**作者**: [TODO: 作者姓名 · 单位 —— 请由作者填写]
**日期**: 2026-05-23
**摘要**: [TODO: 100-150 字摘要 — 概述研究问题（信息渠道 vs 需求压力渠道）、事件研究方法、中国 117 个纳入事件、美国 254 个纳入事件、主要发现（公告窗 CAR 显著、生效窗约为零、中美量级相近）和渠道识别推断。]

完整 prose 见 paper/manuscript.tex 摘要。

---

## 1. 引言

核心论点：[TODO: 引言 prose — 铺陈指数纳入溢价的信息渠道 vs 需求压力渠道之争，引出本文公告窗/生效窗跨制度分解策略。可借鉴 paper/manuscript.tex §1。]

### 1.1 研究背景

[TODO: 背景 prose — 引用 Shleifer 1986 / Harris & Gurel 1986 作为奠基文献，过渡到 Greenwood & Sammon (2022) 的"消失的指数效应"。完整 prose 见 paper/manuscript.tex §1.1。]

### 1.2 研究问题与贡献

[TODO: 三项贡献 prose — 中美制度差异比较、公告窗/生效窗时序分解、诚实承认替代解释。完整 prose 见 paper/manuscript.tex §1.2。]

---

## 2. 文献综述

[TODO: 文献综述 prose — 16 篇核心文献按需求曲线派 / 信息渠道派 / 中国市场文献三条主线展开。完整 author-year 综述见 `docs/literature_review_author_year_cn.md`。]

本项目共索引 16 篇核心文献，分为 3 条研究主线：price_pressure, demand_curve, identification。

### 2.1 需求曲线与价格压力派

[TODO: prose — Shleifer 1986、Harris & Gurel 1986、Kaul 等 (2000)、Wurgler & Zhuravskaya (2002)。完整 prose 见 paper/manuscript.tex §2.1。]

### 2.2 信息/认证渠道与效应消退证据

[TODO: prose — Lynch & Mendenhall (1997)、Denis 等 (2003)、Greenwood & Sammon (2022)。完整 prose 见 paper/manuscript.tex §2.2。]

### 2.3 中国市场文献

[TODO: prose — Chu 等 (2021)、Yao 等 (2022)。完整 prose 见 paper/manuscript.tex §2.3。]

### 2.4 文献评述与本文定位

[TODO: prose — 争论焦点演进与本文定位。完整 prose 见 paper/manuscript.tex §2.4。]

---

## 3. 研究设计

### 3.1 样本与数据

[TODO: 样本期、数据源、清洗规则。关键规模：893 个真实事件（CN 117 个，US 254 个）。数据来源：Yahoo Finance 日频；被动 AUM：US Federal Reserve Z.1，CN ETF TNA proxy。完整 prose 见 paper/manuscript.tex §3.1。]

### 3.2 实证方法

[TODO: 两层策略 — 事件研究（CAR[-1,+1]主，[-3,+3]、[-5,+5]辅，简单市场调整 AR，Patell Z + BMP t）+ 匹配回归（212,756 行面板，block bootstrap 5000 次）。完整 prose 见 paper/manuscript.tex §3.2。]

### 3.3 识别策略：公告窗与生效窗的跨市场分解

关键识别逻辑：信息渠道预测公告窗显著、生效窗约为零、中美量级相近；需求压力渠道预测生效窗显著、且美国（被动规模更大）效应应强于中国。识别局限：描述性事件研究，非准实验因果识别；生效窗约为零也与需求被提前套利相容。完整 prose 见 paper/manuscript.tex §3.3。

---

## 4. 实证结果

### 4.1 核心结果：公告窗与生效窗的比较

本节为论文实证主线。主要结果（来自 `results/real_tables/event_study_summary.csv`）：

- 中国公告日 CAR[-1,+1] = ++1.76%（t=4.93，p<0.001，n=117）
- 美国公告日 CAR[-1,+1] = ++1.84%（t=5.25，p<0.001，n=254）
- 中国生效日 CAR[-1,+1] = +0.42%（t=0.93，p==0.355，不显著）
- 美国生效日 CAR[-1,+1] = -0.14%（t=-0.51，p==0.611，不显著）

完整表格与 CAR 路径图见 paper/manuscript.tex §4.1（表 1，图 1–4）。

---

## 5. 稳健性

本节从五个维度检验 §4 核心结果，各子节所引数字均来自相应结果文件。

### 5.1 纳入 vs 剔除不对称

[TODO: prose — 若信息/认证渠道为主导，则纳入与剔除应呈方向不对称。中国：纳入 +1.76%，剔除 -0.59%；美国：纳入 +1.84%，剔除 +0.05%。完整 prose 见 paper/manuscript.tex §5.1。]

### 5.2 长窗口 CAR 的持续性

[TODO: prose — 中国 [0,+120] 均值 CAR = +1.56%（t=0.66，不显著）；美国 = +1.96%（t=1.57，不显著）。点估计正向，不支持大幅反转。完整 prose 见 paper/manuscript.tex §5.2。]

### 5.3 公告效应的跨年稳定性

[TODO: prose — 按公告年份分解，中国 2021–2025 各年均值均为正；美国 2010–2025 大多数年份均值为正。完整 prose 见 paper/manuscript.tex §5.3。]

### 5.4 匹配对照组的协变量平衡

[TODO: prose — SMD<0.25 门禁，三项协变量（对数市值、前期收益、前期波动率）全部通过。完整 prose 见 paper/manuscript.tex §5.4。]

### 5.5 预公告漂移：一项无法排除的不确定性

[TODO: prose — 中国公告前均值漂移 +3.09%，美国 +2.59%，两市场差异 bootstrap p=0.875。关键诚实声明：本文无法排除公告前已部分消化信息的可能。完整 prose 见 paper/manuscript.tex §5.5。]

---

## 6. 讨论

本节围绕三个讨论要点，解读实证结果对信息 vs 需求渠道识别的含义，并诚实披露早期探索性假说的背景。

**讨论点 (1)**：[TODO: 为什么"跨制度相似性"指向信息渠道 — 中美被动 AUM 量级差异极大，需求渠道预测应产生可识别的跨市场量级分化，但数据中两市场公告窗 CAR 相差仅 0.08 个百分点。完整 prose 见 paper/manuscript.tex §6。]

**讨论点 (2)**：[TODO: 生效窗约为零的两种并列解释 — (a) 机械调仓冲击已被市场深度吸收；(b) 套利者将需求效应提前定价压平（Greenwood & Sammon, 2022）。完整 prose 见 paper/manuscript.tex §6。]

**讨论点 (3)**：[TODO: 早期探索性假说的诚实交代 — 本项目曾探索 7 条跨市场机制假说（H1–H7），但这些假说形成于观测结果之后（post-hoc）、本项目无预分析计划、部分假说依赖极小样本，故不纳入本文主线。相关分析参数见 `docs/analysis_parameters.md`。完整 prose 见 paper/manuscript.tex §6。]

---

## 7. 结论与局限

### 7.1 主要结论

[TODO: prose — 三点主要结论：(1) 公告窗 CAR 在中美两市场均显著为正；(2) 生效窗 CAR 在中美两市场均不显著；(3) 效应集中于公告窗且跨制度量级相近，指向信息/认证渠道。完整 prose 见 paper/manuscript.tex §7.1。]

### 7.2 局限

[TODO: prose — 六项明确局限：(1) 描述性事件研究，非准实验因果识别；(2) 增量贡献主要在跨制度系统比较；(3) 中国有效事件约 117 个，样本期仅 5 年；(4) 市值与换手率数据口径为近似；(5) 生效窗约为零的可替代解释无法排除；(6) 公告前预漂移无法排除。完整 prose 见 paper/manuscript.tex §7.2。]

下文为 `docs/limitations.md` 的自动嵌入，便于审稿人无须翻附录直接阅读：

---

# 数据与方法限制

本文档集中记录项目的关键数据近似与方法约束，论文写作和读者评估时请同时引用此页。

## 1. 价格与市值数据

- **价格 / 收益**：Yahoo Finance（yfinance）日频 OHLCV；按当前交易日复权。
- **市值（mkt_cap）**：用 Yahoo `sharesOutstanding`（当前值）× 历史价格近似得到。
  **不等价于交易所历史自由流通市值**，仅适合机制分析与课程汇报。
- **换手率（turnover）**：volume / shares_outstanding 近似，没有过滤大宗 / 协议交易。
- **基准（benchmark_ret）**：CN 用 CSI300 指数收益，US 用 S&P 500 指数收益（`benchmarks.csv`）。

## 2. 事件清单

- **CN 事件**：中证指数公司官方调整公告 PDF + 公开新闻补充转录（`source` 列记录来源）。
- **US 事件**：维基百科 S&P 500 成分股表 + S&P Dow Jones 官方脚注。
- **未覆盖**：增发 / 分拆事件、内部技术性调整（如 ticker 变更）。
- **样本规模**：893 个真实事件（274 CN + 619 US，2010-2025）。

## 3. 被动 AUM（H2 假说）

- **US 来源**：Federal Reserve Z.1 系列（`BOGZ1FL564090005A`，US ETF Total Financial Assets）。
  12 个年度观测（2010-2025），按 USD trillion 计。
- **CN 来源**：自建 ETF TNA 聚合 proxy，存于 `data/raw/cn_passive_aum_proxy.csv`。
  方法：年终（12-31 或最近交易日）抓取 CSI300 / CSI500 跟踪 ETF 的份额×单位净值
  (akshare `fund_etf_scale_sse` + `fund_scale_daily_szse` + `fund_etf_fund_info_em`
  with `fund_etf_hist_em` 收盘价作 NAV 兜底)，按指数汇总后再相加。
  生成命令：`uv run index-inclusion-download-cn-passive-aum-proxy`。
- **CN 数据局限（必须在论文中披露）**：
  - 这是 **TNA 聚合 proxy**，不是基金业协会披露的官方“被动 AUM”口径；
  - ETF 宇宙逐年扩张（2024 年下半年是机构 ETF 配置爆发期），早年快照天然
    低估真实被动跟踪 AUM；
  - n=5 个年终快照（2020-2024），仍少于 US 的 12 个，但 H2 verdict 已升级为
    "core" 因为合并 n（CN rolling CAR 5 + US rolling CAR 12 = 17）越过 `EVIDENCE_TIER_PROMOTION_FLOOR["H2"]=15` 阈值；
  - 数据新鲜度依赖 akshare（东方财富 / 上交所 / 深交所）公开接口，刷新周期由
    `download_cn_passive_aum_proxy` 控制；
  - 单位是 CNY trillion，而 US 行是 USD trillion，**不能跨币种直接比较绝对值**。
    H2 verdict 只用市场内首尾趋势方向，所以币种不一致不污染裁决结果。
- **保留的另一份 CN 数据**：`data/raw/passive_aum.csv` 仍保留 top-down `download_passive_aum_cn`
  写入的 CN 行（公募基金总规模 × 指数型占比），但 CMA 编排时被 proxy 覆盖。
  如果未来要回退到旧口径，删除 `data/raw/cn_passive_aum_proxy.csv` 即可。

## 4. HS300 RDD 数据层级

- **L3（官方）**：2020-11 到 2025-11 共 11 个批次，356 行（含 191 调入 + 165 备选对照）。详见 `docs/hs300_rdd_data_contract.md`。
- **L2（公开重建）**：1887 行；从公开调整新闻反推，**不等价于中证官方历史排名**。
- **L1（演示）**：合成数据，仅供 pipeline 测试。
- **当前主表使用**：默认 L3；缺失时返回 `missing` 状态而非自动降级。
- **若要支持论文级因果声明**：需扩展 L3 到 ≥10 年并补 McCrary 操纵性检验，
  详见 `docs/hs300_rdd_l3_collection_audit.md`。

## 5. 事件研究方法

- **AR 计算**：默认仍为简单市场调整（`ar = ret − benchmark_ret`，向后兼容；
  主表与 CMA verdict 都钉在这一引擎上）。通过
  `index-inclusion-run-event-study --ar-model market` 可切换到带 β 估计的市场模型 AR
  （`ar_market_model = ret − (α + β·benchmark_ret)`，估计窗口默认 (-120, -10)，
  与短窗口事件研究文献一致；用 `--estimation-window LOW,HIGH` 改写）。切换引擎
  在 CN 样本上经验上会让 CAR 偏移约 5-15 bps（β 与 1 之差带来的修正），主表
  保持不变；若要把另一引擎纳入论文，应在 `docs/analysis_parameters.md` 的变更日志中记录这次口径变更。
- **σ 估计**：默认从 panel 内 `[-window_pre, -2]` 区间估计 (window_pre 默认 20)；
  这是 18 日的 in-panel proxy estimation window，比文献标准（120-250 日）短。
- **标准化**：`compute_patell_bmp_summary` 在简单 t 之外提供 Patell t 与 BMP t；
  在样本量充足时建议优先看 BMP（不假设零相关）。
- **长窗口** `[0,+120]`：样本会大幅缩水，仅作探索性结果，不进入主表。

## 6. 多重检验与 post-hoc 披露

- **当前阈值**：决定层 p<0.10（默认）；输出层附 Bonferroni 与 Benjamini-Hochberg q-value。
- **H1–H7 是 post-hoc、探索性假说**：本项目 **没有预分析计划 (pre-analysis plan)**。
  7 条 CMA 假说是在观察到 announce-vs-effective、CN-vs-US 的不对称结果**之后**形成的，
  属于探索性解释，**不是 confirmatory 验证**。裁决阈值（p<0.10、内阈值 0.05）是在
  已知结果的情况下选定的分析参数，没有事前承诺。多重检验校正
  （Bonferroni、Benjamini-Hochberg）虽已在 `cma_hypothesis_verdicts.csv` 中报告，
  但同样是在假说选定**之后**应用的。因此论文与汇报中引用 H1–H7 时，应明确表述为
  post-hoc 探索性证据，并优先只把 `evidence_tier=core` 的假说放进主表。
- **分析参数记录**：7 假说的判据、阈值与样本边界集中记录在
  [`docs/analysis_parameters.md`](analysis_parameters.md)——这是一份透明性文档，
  **不是 pre-analysis plan**。verdict 跨时间稳定性可用 `index-inclusion-verdict-summary
  --vs-pap` 对比裁决基线快照查看；详细 verdict 迭代流程见
  [`docs/verdict_iteration.md`](verdict_iteration.md)。

## 7. 每假说的统计功效（post-hoc）

`index-inclusion-power-analysis` 会对各假说做后验功效计算并把结果落到
`results/real_tables/power_analysis_report.csv` 与 `power_analysis_report.md`。
α=0.05、target power=80%，覆盖 H3 / H4 / H5 / H6 单口径以及 H1 / H2 分引擎。
当前主裁结论：

| 假说 | n | 观测效应 | 在观测效应下的功效 | 80% 功效下的 MDE | 解读 |
|---|---:|---:|---:|---:|---|
| H3 (双通道命中率) | 4 | hit_rate=0.75（差值 +0.25） | ≈ 0.13（normal-approx）/ 0.00（exact） | 概率差 ≈ +0.50（即 p1≈1.0） | 严重欠功效；exact-binomial 在 α=0.05 下不存在 rejection region。结果按 supplementary 处理是合理的。Bayesian P(p>0.60 \| 3/4, Beta(1,1) 先验) ≈ 0.66 — 给方向性参考。 |
| H4 (cn_coef on gap_drift) | 436 | coef=+0.0061（SE=0.0099，t=+0.62，p=0.537） | ≈ 0.09 | \|coef\| ≈ 0.028（≈ 4.5× 观测效应） | 严重欠功效。n=436 听起来不少，但观测系数只有 SE 的 0.6 倍，离 α=0.05 显著很远；若 H4 真正的卖空约束效应只有现在观测的规模，本研究无法把它和零区分。**保留为 supplementary，并把裁决文字从"证据不足支持 H4"改述为"现有 n 下证据不足以拒零，需要 n≈n_now × (0.028/0.006)² ≈ 9000 才能可靠检出此规模效应"**。 |
| H5 (limit_coef on announce CAR) | 936 | coef=+0.155（SE=0.059，t=+2.64，p=0.008） | ≈ 0.75 | \|coef\| ≈ 0.164（≈ 1.06× 观测效应） | **功效中等且落在 0.80 阈值正下方**。p=0.008 的 supportive 裁决在 frequentist 层面成立，但观测系数刚好等于 MDE 量级（power ≈ target_power 临界）：把 H5 写在 main findings 是合理的，**但应在 §5 加一句"该效应处于 n=936 的可检验边界，若真实 coef 略低于 +0.155 即可能被错过"**，避免读者把 0.75 误读为"压倒性证据"。 |
| H6 (heavy−light spread) | 67 | Cohen's d ≈ −0.73（pooled SD≈0.032） | ≈ 1.00 | \|d\| ≈ 0.35 | 功效充足，n=67 足以检出该规模效应，但观测方向 (heavy<light) 与 H6 预测 (heavy>light) 相反 → "证据不足" 来自方向不符，**不是** n 太小。 |

- **方法学**：H3 使用一比例 z-test（正态近似）+ exact-binomial 对照；H4 / H5 使用 HC3 回归单系数 t-test（ncp = coef/SE，df = n − k − 1）；H6 使用单样本 t-test，Cohen's d = mean/pooled-SD；MDE 由二分搜索求解（H4/H5 与闭式 (z_{1-α/2}+z_β)·SE 一致）。
- **数据源**：
  - H4 → `results/real_tables/cma_gap_drift_market_regression.csv`（`cn_coef`, `cn_se`, `cn_p_value`, `n_obs`，n_covariates=2: cn_dummy + gap_length_days）。
  - H5 → `results/real_tables/cma_h5_limit_predictive_regression.csv`（`limit_coef`, `limit_se`, `limit_p_value`, `n_obs`，n_covariates=1）。
  - H6 的 pooled SD 由 `data/processed/hs300_weight_change.csv` × `results/real_tables/cma_gap_event_level.csv` 按 weight_proxy 中位数切重/轻 bucket 重算（n_heavy=34，n_light=33）；面板缺失时回退到 H6 OLS-HC3 r²=0.033 反推的 \|d\|≈0.18，并在 interpretation 里明文说明。
- **可重现**：`index-inclusion-power-analysis` 是 48 个 console scripts 的第 48 号；它会按当前 verdicts / 回归 CSV 即时重算，不需要单独缓存。
- **诚实读图**：
  - **H3** 的 power<0.30 意味着即便真实命中率确实是 75%，本研究在 n=4 下也很难把它"测出来"；这是把 H3 归入 supplementary 的统计学依据，而不是"我们不喜欢这个结论"。
  - **H4** 的 power≈0.09 同样不允许"证据不足 ⇒ H4 错"的反推。n=436 看似充足，但**观测效应**太小（coef 仅 0.6 倍 SE）使 post-hoc 功效塌到 < 0.10；MDE/coef ≈ 4.5 表示：要把这一项升级为"支持"，需要 effect 翻 4-5 倍**或** n 翻 ~20 倍。
  - **H5** 的 power≈0.75 是"恰好低于 0.80"——p=0.008 在 frequentist 上仍然是显著的，但**不该把它当成"功效充足"**：观测系数处在 MDE 临界，若样本里的极端观察点稍有抖动，效应就可能跌进无法识别的区间。建议 §5 显式标注。
  - **H6** 的 power≈1 配合 d=−0.73 则说明：**没把 H6 升级为支持**是数据驱动的，不是测试力度不够。

### 7.1 各假说功效裁决（paper-ready 摘要）

- **H3 的"支持 / 高置信度"裁决建立在 n=4 上，而项目自己的功效分析给出 power≈0**。
  H3 的裁决变量是 4 个 CN/US × announce/effective 象限的 dual-channel 命中率（3/4=0.75）。
  在 n=4、α=0.05 下：正态近似功效仅 ≈ 0.13，**精确二项检验下根本不存在 rejection
  region（exact-binomial power = 0.000）**——也就是说，即便真实命中率确实是 75%，
  本设计在 n=4 下几乎无法把它从零区分开。因此 H3 的"支持"必须读作**方向性、
  描述性**的证据，其"高置信度"标签反映的是命中率点估计本身（3/4），**不是**一个有
  统计功效支撑的结论。论文里 H3 只应作为 supplementary，并明确写出 n=4 / power≈0
  的限制；不要让 §3.3 的"支持/高"被读成强证据。
- **H4 is severely underpowered (n=436, observed power ≈ 0.09)**。 paper §5 应保留为 supplementary 并把口径写成"在当前样本下证据不足以拒零，**不构成对 H4 的反证**"。
- **H5 is moderately powered (n=936, observed power ≈ 0.75)**，恰好落在 0.80 阈值之下。 frequentist 显著（p=0.008）+ 方向正确，可继续作为 main finding，**但在 §5 局限性段落明示"观测效应处于可检测边界"**。
- **H6 is direction-mismatched (power ≈ 1.0, d=−0.73)**：保留既有处理（H6 reframed as 'evidence against' rather than 'evidence for'）。

## 8. CMA 假说证据强度分层

- **核心假说（core, n 充足）**：H1（n=436）、H5（n=936）、H7（sector spread n=187；交互回归 n=1882）；
  H2 在补入 CN ETF TNA proxy 后由 supplementary 升级为 core（合并 n=17：US rolling 12 + CN rolling 5,超过 `EVIDENCE_TIER_PROMOTION_FLOOR["H2"]=15` 阈值）。
- **附录假说（supplementary, n 受限）**：
  - H3（n=4 象限，dual-channel 判据）
  - H4（n=436 但回归 p=0.537，不显著）
  - H6（n=67）
- 该分层在 `analysis/cross_market_asymmetry/verdicts/_core.py` 中由 `EVIDENCE_TIER` 与
  `EVIDENCE_TIER_PROMOTION_FLOOR` 联合决定，并由 `_make_verdict` 写入
  `cma_hypothesis_verdicts.csv` 的 `evidence_tier` 列；H2 是当前唯一启用
  combined-n 数据驱动升级的假说，目的就是在 CN AUM 数据补齐后避免人工硬改。

## 9. 何时不要用本项目结论

- 若需要交易所自由流通市值精确口径 → 不要用 Yahoo `mkt_cap`。
- 若需要中证官方历史排名因果识别 → L3 数据不足以前不要用。
- 若需要长窗口 [0,+120] 退化效应 → 样本严重缩水，仅作探索性。
- 若需要时间序列 AUM 推断 → US n=12 且 CN 可比 AUM 缺失，结论以方向参考为主。

## 10. 引用格式建议

文中或表注中引用本项目结果时，建议同时标注：

> 数据来源：Yahoo Finance（价格、近似市值）、Federal Reserve Z.1（US 被动 AUM）、
> akshare 上交所/深交所 ETF 份额与东方财富 ETF NAV（CN 被动 AUM proxy，详见 §3）、
> 中证指数公司公告与维基百科（事件清单）；HS300 RDD 当前使用 L3 官方候选边界样本，但覆盖期仍不足以支撑论文级强因果声明。
> 详细数据与方法限制见 `docs/limitations.md`。

---

## 参考文献

下列 16 篇文献来自 `literature_catalog.PAPER_LIBRARY`（项目核心文献库）：

启发式文献关联网络（自动）：本项目文献库共 16 篇，共 52 条"主题/方法/年代"关联边；关联最多：Shleifer '86、Harris '86、Wurgler '02；桥梁文献（betweenness）：Shleifer '86、Harris '86、Wurgler '02。这不是已验证引用关系，也不是逐条 bibliography citation 核验；只用于文献综述导航，不得作为被引/引用证据。可视化见 `results/literature/citation_network.png`（中心性 CSV：`results/literature/citation_centrality.csv`，由 `index-inclusion-citation-graph` 生成）。

1. Lawrence Harris; Eitan Gurel (1986). *Price and Volume Effects Associated with Changes in the S&P 500 List: New Evidence for the Existence of Price Pressures*. 美国 / S&P 500. `paper_id=harris_gurel_1986`.
2. Andrei Shleifer (1986). *Do Demand Curves for Stocks Slope Down?*. 美国 / S&P 500. `paper_id=shleifer_1986`.
3. Anthony W. Lynch; Richard R. Mendenhall (1997). *New Evidence on Stock Price Effects Associated with Changes in the S&P 500 Index*. 美国 / S&P 500. `paper_id=lynch_mendenhall_1997`.
4. Aditya Kaul; Vikas Mehrotra; Randall Morck (2000). *Demand Curves for Stocks Do Slope Down: New Evidence from an Index Weights Adjustment*. 加拿大 / TSE 300. `paper_id=kaul_mehrotra_morck_2000`.
5. Diane K. Denis; John J. McConnell; Alexei V. Ovtchinnikov; Yun Yu (2003). *S&P 500 Index Additions and Earnings Expectations*. 美国 / S&P 500. `paper_id=denis_et_al_2003`.
6. Jeffrey Wurgler; Ekaterina Zhuravskaya (2002). *Does Arbitrage Flatten Demand Curves for Stocks?*. 跨市场 / 机制. `paper_id=wurgler_zhuravskaya_2002`.
7. Ananth Madhavan (2003). *The Russell Reconstitution Effect*. 美国 / Russell. `paper_id=madhavan_2003`.
8. Antti Petajisto (2011). *The index premium and its hidden cost for index funds*. 美国 / S&P 500, Russell 2000. `paper_id=petajisto_2011`.
9. Maria Kasch; Asani Sarkar (2011). *Is There an S&P 500 Index Effect?*. 美国 / S&P 500. `paper_id=kasch_sarkar_2011`.
10. Byung Hyun Ahn; Panos N. Patatoukas (2022). *Identifying the Effect of Stock Indexing: Impetus or Impediment to Arbitrage and Price Discovery?*. 美国. `paper_id=ahn_patatoukas_2022`.
11. Jerry Coakley; George Dotsis; Apostolos Kourtis; Dimitris Psychoyios (2022). *The S&P 500 Index inclusion effect: Evidence from the options market*. 美国 / S&P 500. `paper_id=coakley_et_al_2022`.
12. Robin Greenwood; Marco Sammon (2022). *The Disappearing Index Effect*. 美国 / S&P 500. `paper_id=greenwood_sammon_2022`.
13. Yen-Cheng Chang; Harrison Hong; Inessa Liskovich (2014). *Regression Discontinuity and the Price Effects of Stock Market Indexing*. 美国 / Russell. `paper_id=chang_hong_liskovich_2014`.
14. Gang Chu; John W. Goodell; Xiao Li; Yongjie Zhang (2021). *Long-term impacts of index reconstitutions: Evidence from the CSI 300 additions and deletions*. 中国 / CSI300. `paper_id=chu_et_al_2021`.
15. 姚东旻; 张日升; 李嘉晟 (年份待核验). *指数效应存在吗？——来自“沪深300”断点回归的证据*. 中国 / 沪深300. `paper_id=yao_zhang_li_hs300`.
16. Dongmin Yao; Shiyu Zhou; Yijing Chen (2022). *Price effects in the Chinese stock market: Evidence from the China securities index (CSI300) based on regression discontinuity*. 中国 / CSI300. `paper_id=yao_zhou_chen_2022`.

## 附录

### A. 数据契约

[TODO: 数据契约 — 三个输入表字段约定（events.csv、prices.csv、benchmarks.csv）及被动 AUM 与行业标签口径说明。完整契约见 `docs/hs300_rdd_data_contract.md` 与 `docs/real_data_notes.md`。]

### B. CLI 入口 (48 个)

[TODO: CLI 入口列表 — 完整 48 个 console scripts 的分组、用法与示例命令见 `docs/cli_reference.md`。RDD 相关命令保留于 CLI 中但 RDD 结果不进入本文实证主线。]

### C. 复现指南

```bash
make rebuild           # 10 步流水线：从原始数据到事件研究结果
make figures-tables    # 重绘 5 张论文级 figure
make paper             # 论文交付包：自动复制到 paper/
index-inclusion-export-public-summary  # 刷新公共摘要
index-inclusion-paper-bundle --force   # 重新生成本骨架
```

公共摘要工件：`data/public/index_research_summary.json`（schema v1），是面向外部消费者（sibling 项目、GitHub Pages 日报）的稳定入口。
