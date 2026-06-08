# 数据与方法限制

本文档集中记录项目的关键数据近似与方法约束，论文写作和读者评估时请同时引用此页。

## 1. 价格与市值数据

- **价格 / 收益**：Yahoo Finance（yfinance）日频 OHLCV；按当前交易日复权。
- **市值（mkt_cap）**：用 Yahoo `sharesOutstanding`（当前值）× 历史价格近似得到。
  **不等价于交易所历史自由流通市值**，仅适合机制分析与课程汇报。
- **换手率（turnover）**：volume / shares_outstanding 近似，没有过滤大宗 / 协议交易。
- **Tushare 可选 CN 口径**：`index-inclusion-download-real-data --cn-price-source tushare`
  会用 Tushare 日线 / `daily_basic` 刷新 A 股价格、总市值与换手率，并用 Tushare
  指数日线刷新 CSI300 基准；US 侧仍走 Yahoo。该路径需要 `TUSHARE_TOKEN`，
  且受 Tushare 权限、积分和接口可用性约束。
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
  - n=5 个年终快照（2020-2024），仍少于 US 的 13 个，但 H2 verdict 已升级为
    "core" 因为合并 n（CN rolling CAR 5 + US rolling CAR 13 = 18）越过 `EVIDENCE_TIER_PROMOTION_FLOOR["H2"]=15` 阈值；
    注意 core 仅指"n 充足进入主表"，H2 在 Tushare 口径下裁决仍为"证据不足"；
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
| H3 (双通道命中率) | 4 | hit_rate=0.50（差值 +0.00） | ≈ 0.05（normal-approx）/ 0.00（exact） | 概率差 ≈ +0.50（即 p1≈1.0） | 严重欠功效；exact-binomial 在 α=0.05 下不存在 rejection region。结果按 supplementary 处理是合理的。Bayesian P(p>0.60 \| 2/4, Beta(1,1) 先验) ≈ 0.32 — 给方向性参考。 |
| H4 (cn_coef on gap_drift) | 455 | coef=+0.0050（SE=0.0097，t=+0.52，p=0.604） | ≈ 0.08 | \|coef\| ≈ 0.027（≈ 5.4× 观测效应） | 严重欠功效。n=455 听起来不少，但观测系数只有 SE 的 0.5 倍，离 α=0.05 显著很远；若 H4 真正的卖空约束效应只有现在观测的规模，本研究无法把它和零区分。**保留为 supplementary，并把裁决文字写成"现有 n 下证据不足以拒零，不构成对 H4 的反证"**。 |
| H5 (limit_coef on announce CAR) | 1096 | coef=+0.0744（SE=0.094，t=+0.79，p=0.427） | ≈ 0.263 | \|coef\| ≈ 0.263（≈ 3.5× 观测效应） | **从"支持"翻转为"证据不足"**。换用 Tushare A 股口径重算后，涨跌停命中率不再显著预测 announce-day CAR（p=0.427，远高于 0.05），power 仅 ≈ 0.13。此前 Yahoo 口径下的 p=0.008/power=0.75 已被推翻——这是免费数据涨跌停/价格字段不可靠造成的假阳性，准确数据将其纠正。H5 现按 §5 局限性诚实披露，**不再作为 main finding**。 |
| H6 (heavy−light spread) | 87 | Cohen's d ≈ −0.47（pooled SD≈0.033） | ≈ 0.30 | \|d\| ≈ 0.30 | 功效充足（≈0.99），n=87 足以检出该规模效应，但观测方向 (heavy<light) 与 H6 预测 (heavy>light) 相反 → "证据不足" 来自方向不符，**不是** n 太小。 |

- **方法学**：H3 使用一比例 z-test（正态近似）+ exact-binomial 对照；H4 / H5 使用 HC3 回归单系数 t-test（ncp = coef/SE，df = n − k − 1）；H6 使用单样本 t-test，Cohen's d = mean/pooled-SD；MDE 由二分搜索求解（H4/H5 与闭式 (z_{1-α/2}+z_β)·SE 一致）。
- **数据源**：
  - H4 → `results/real_tables/cma_gap_drift_market_regression.csv`（`cn_coef`, `cn_se`, `cn_p_value`, `n_obs`，n_covariates=2: cn_dummy + gap_length_days）。
  - H5 → `results/real_tables/cma_h5_limit_predictive_regression.csv`（`limit_coef`, `limit_se`, `limit_p_value`, `n_obs`，n_covariates=1）。
  - H6 的 pooled SD 由 `data/processed/hs300_weight_change.csv` × `results/real_tables/cma_gap_event_level.csv` 按 weight_proxy 中位数切重/轻 bucket 重算；面板缺失时回退到 H6 OLS-HC3 反推的 \|d\|，并在 interpretation 里明文说明。
- **可重现**：`index-inclusion-power-analysis` 是 43 个 console scripts 的第 42 号；它会按当前 verdicts / 回归 CSV 即时重算，不需要单独缓存。
- **诚实读图**：
  - **H3** 的 power<0.30 意味着即便真实命中率确实偏离 50%，本研究在 n=4 下也很难把它"测出来"；这是把 H3 归入 supplementary 的统计学依据，而不是"我们不喜欢这个结论"。
  - **H4** 的 power≈0.08 同样不允许"证据不足 ⇒ H4 错"的反推。n=455 看似充足，但**观测效应**太小（coef 仅 0.5 倍 SE）使 post-hoc 功效塌到 < 0.10；MDE/coef ≈ 5.4 表示：要把这一项升级为"支持"，需要 effect 翻 5 倍**或** n 翻 ~30 倍。
  - **H5** 在 Tushare 口径下 p=0.427、power≈0.13——**既不显著、也欠功效**。这与早先 Yahoo 口径的 p=0.008/power=0.75 截然相反；说明此前的"支持"高度依赖免费数据中不可靠的涨跌停/价格字段。结论：H5 现为"证据不足"，论文不得再把它列为 main finding。
  - **H6** 的 power≈0.99 配合 d=−0.47 则说明：**没把 H6 升级为支持**是数据驱动的（方向相反），不是测试力度不够。

### 7.1 各假说功效裁决（paper-ready 摘要）

- **H3 的"支持"裁决建立在 n=4 上，而项目自己的功效分析给出 power≈0**。
  H3 的裁决变量是 4 个 CN/US × announce/effective 象限的 dual-channel 命中率（当前 2/4=0.50）。
  在 n=4、α=0.05 下：正态近似功效仅 ≈ 0.05，**精确二项检验下根本不存在 rejection
  region（exact-binomial power = 0.000）**——也就是说，本设计在 n=4 下几乎无法把命中率
  从零区分开。因此 H3 的"支持"必须读作**方向性、描述性**的证据，**不是**一个有
  统计功效支撑的结论。论文里 H3 只应作为 supplementary，并明确写出 n=4 / power≈0
  的限制；不要让 §3.3 被读成强证据。
- **H4 is severely underpowered (n=455, observed power ≈ 0.08)**。 paper §5 应保留为 supplementary 并把口径写成"在当前样本下证据不足以拒零，**不构成对 H4 的反证**"。
- **H5 翻转为"证据不足"（n=1096, p=0.427, observed power ≈ 0.13）**。 换用 Tushare A 股口径重算后，涨跌停命中率不再显著预测 announce-day CAR；此前 Yahoo 口径下的 p=0.008/power=0.75 是免费数据涨跌停字段不可靠造成的假阳性。**H5 不得再作为 main finding**，应在 §5 与 §7 诚实披露这一数据源驱动的翻转。
- **H6 is direction-mismatched (power ≈ 0.99, d=−0.47)**：保留既有处理（H6 reframed as 'evidence against' rather than 'evidence for'）。

## 8. CMA 假说证据强度分层

- **核心假说（core, n 充足）**：H1（n=455）、H5（n=1096）、H7（sector spread n=187；交互回归 n=1930）；
  H2 在补入 CN ETF TNA proxy 后由 supplementary 升级为 core（合并 n=18：US rolling 13 + CN rolling 5,超过 `EVIDENCE_TIER_PROMOTION_FLOOR["H2"]=15` 阈值）。
  注意：core 仅表示"n 充足、进入主表披露"，不等于"被支持"——H5 虽为 core，但 Tushare 口径下裁决为"证据不足"。
- **附录假说（supplementary, n 受限）**：
  - H3（n=4 象限，dual-channel 判据）
  - H4（n=455 但回归 p=0.604，不显著）
  - H6（n=87）
- 该分层在 `analysis/cross_market_asymmetry/verdicts/_core.py` 中由 `EVIDENCE_TIER` 与
  `EVIDENCE_TIER_PROMOTION_FLOOR` 联合决定，并由 `_make_verdict` 写入
  `cma_hypothesis_verdicts.csv` 的 `evidence_tier` 列；H2 是当前唯一启用
  combined-n 数据驱动升级的假说，目的就是在 CN AUM 数据补齐后避免人工硬改。

## 9. 何时不要用本项目结论

- 若需要交易所自由流通市值精确口径 → 不要用默认 Yahoo `mkt_cap`；可先尝试
  Tushare CN 口径改善总市值 / 换手率，但它仍不是完整自由流通市值审计。
- 若需要中证官方历史排名因果识别 → L3 数据不足以前不要用。
- 若需要长窗口 [0,+120] 退化效应 → 样本严重缩水，仅作探索性。
- 若需要时间序列 AUM 推断 → US n=12 且 CN 可比 AUM 缺失，结论以方向参考为主。

## 10. 引用格式建议

文中或表注中引用本项目结果时，建议同时标注：

> 数据来源：Yahoo Finance（默认价格、近似市值；Tushare 可选刷新 A 股日线、市值、换手率与 CSI300 基准）、Federal Reserve Z.1（US 被动 AUM）、
> akshare 上交所/深交所 ETF 份额与东方财富 ETF NAV（CN 被动 AUM proxy，详见 §3）、
> 中证指数公司公告与维基百科（事件清单）；HS300 RDD 当前使用 L3 官方候选边界样本，但覆盖期仍不足以支撑论文级强因果声明。
> 详细数据与方法限制见 `docs/limitations.md`。
