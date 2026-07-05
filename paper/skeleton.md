# 指数纳入溢价集中在公告窗还是生效窗？来自中美两市场的描述性证据

**作者**: 作者待补（投稿前替换为作者姓名与单位）
**日期**: 2026-07-05
**摘要**: 本文用统一的事件研究口径，描述性地比较指数纳入溢价在公告窗与生效窗的时序分布，以及该格局在中国（沪深300）与美国（标普500）两个制度环境中的异同。样本覆盖 中国 137 个纳入事件、美国 255 个纳入事件。三组稳健的经验事实：（1）公告窗 CAR[-1,+1] 在中美两市场均显著为正；（2）生效窗 CAR 在两市场均约为零、不显著；（3）两市场公告窗量级接近。这一"公告强、生效弱、跨制度相近"的格局与信息/认证渠道的预测一致，并复制了 Greenwood & Sammon (2022) "消失的指数效应"在中国市场的表现。**本文是描述性研究，不主张因果识别**：生效窗约为零也与需求被套利者提前定价相容，本文不以单一窗口排除需求压力渠道。作为透明性，我们披露结论对数据口径的敏感性——将 A 股行情由免费源切换为持牌 Tushare 数据后，两条事后机制假说（涨跌停、被动 AUM）的判定即由"支持"翻转为"证据不足"，说明机制层结论脆弱、仅作附录探索性证据。

---

## 1. 引言

指数纳入长期被视为检验需求曲线是否向下、被动资金是否影响价格，以及指数委员会认证效应是否存在的自然场景。早期 S&P 500 文献强调加入指数后的价格压力和流动性变化；近年的研究则指出，随着指数规则更透明、套利者提前布局，被动资金冲击可能在公告期之前或公告期内被重新定价。本文把争论拆解为两个可观察维度：价格反应发生在公告日还是生效日，以及这种反应在中国和美国两个制度环境中是否呈现显著量级差异。

### 1.1 研究背景

Shleifer (1986) 和 Harris & Gurel (1986) 证明指数成分调整会伴随可观的价格与成交量反应，奠定了需求曲线与价格压力解释。随后 Wurgler & Zhuravskaya、Lynch & Mendenhall 与 Denis 等研究把短期压力、长期保留、信息认证和公司基本面预期纳入同一讨论。Greenwood & Sammon (2022) 进一步提示，指数效应并非静态制度事实，而会随着可预测性、套利能力和被动资金生态变化而弱化。中国市场的涨跌停、投资者结构和指数编制流程不同于美国，因此为区分信息渠道与需求压力渠道提供了制度对照。

### 1.2 研究问题与贡献

本文回答三个**描述性**问题。第一，指数纳入的短期超额收益在公告窗和生效窗分别如何表现？第二，中美市场的量级是否如机械被动买盘解释所预期那样出现显著分化？第三，哪些结论稳健到可以进入论文主线，哪些只能作为附录或探索性证据。

本文的贡献是经验性与方法透明性的，而非因果识别上的：（1）用同一套事件研究口径系统比较中美两个制度差异极大的市场，把"跨制度相似性即对信息渠道有利"这一论证落实为可量化的描述性事实；（2）把公告窗与生效窗的时序分解作为渠道讨论的主要观察工具，并在中国市场首次系统给出这一分解；（3）以高标准的透明性披露稳健性——包括把 H1–H7 机制假说如实标注为事后（post-hoc）探索性证据、把 HS300 RDD 标注为不满足识别假设的 illustrative 附录、并报告核心机制结论对数据口径（Yahoo vs Tushare）的敏感性。

**识别边界（必须明写）**：本文是描述性事件研究，不构成准实验因果识别。announce/effective 时序分解与跨市场比较是**与信息渠道一致**的证据，不是对需求渠道的排除——生效窗约为零同样与"需求冲击被套利者提前定价"（Greenwood & Sammon 2022）相容。任何因果措辞都被刻意避免。

---

## 2. 文献综述

本项目共索引 16 篇核心文献，分为 3 条研究主线：price_pressure, demand_curve, identification。文献综述按需求曲线与价格压力、信息/认证渠道与效应消退、中国市场证据三组组织，目的是把本文的公告窗/生效窗分解放回既有争论中。

### 2.1 需求曲线与价格压力派

需求曲线派的核心命题是：如果股票不是完全替代品，指数基金和基准跟踪者的非信息性需求会推高纳入股票价格。Shleifer (1986) 提出向下倾斜需求曲线，Harris & Gurel (1986) 记录加入 S&P 500 附近的价格与成交量反应，Kaul 等 (2000) 以及 Wurgler & Zhuravskaya (2002) 则把价格压力与替代性不足、套利成本联系起来。这条文献预测生效日附近的机械需求冲击应较强，且被动资金更发达的市场中效应更明显。

### 2.2 信息/认证渠道与效应消退证据

信息/认证渠道认为，指数纳入不只是机械买盘，还可能传递指数委员会筛选、流动性改善、投资者关注和基本面预期变化。Lynch & Mendenhall (1997) 区分公告与生效阶段，Denis 等 (2003) 讨论纳入后经营表现与市场预期，Greenwood & Sammon (2022) 则强调随着套利者提前交易和指数规则透明化，传统生效日指数效应会减弱。这条文献预测公告窗可能更重要，生效日窗口未必显著。

### 2.3 中国市场文献

中国市场文献补充了不同交易制度、投资者结构和约束条件下的指数效应证据。Chu 等、Yao 等以及相关 A 股指数研究关注涨跌停、散户参与、ETF 发展和指数调整规则对价格反应的影响。中国市场较短样本期和数据口径限制要求本文在结论中保持谨慎，但它也提供了与美国市场对照的制度差异。

### 2.4 文献评述与本文定位

既有文献从“指数纳入是否产生价格效应”逐步转向“价格效应通过何种渠道、在何时被定价”。本文不把单一机制作为先验答案，而是用公告窗、生效窗和跨市场量级三项事实约束解释空间：若机械需求压力主导，应看到生效窗显著且美国更强；若信息/认证与提前定价主导，则应看到公告窗集中、生效窗弱化，并可能出现跨制度相似的公告反应。

---

## 3. 研究设计

### 3.1 样本与数据

样本使用真实指数纳入/剔除事件、日频价格和市场基准收益，核心纳入事件规模为 中国 137 个纳入事件、美国 255 个纳入事件。价格、基准和事件清单分别由 `data/raw/real_prices.csv`、`data/raw/real_benchmarks.csv` 和 `data/raw/real_events.csv` 管理；具体数据源口径以 `results/real_tables/data_sources.csv` 和 `docs/real_data_notes.md` 为准。被动 AUM 证据使用美国 Federal Reserve Z.1 与中国 ETF TNA proxy。中国端市值、换手率与被动规模均存在可得性限制，因此本文把这些变量作为机制讨论和附录证据，而非无条件的因果识别变量。

### 3.2 实证方法

主方法是事件研究：以公告日和生效日为事件日，报告 CAR[-1,+1] 作为主窗口，并用 [-3,+3]、[-5,+5] 以及 Patell Z、BMP t 等标准化异常收益作为稳健性补充。第二层方法是匹配样本回归与协变量平衡检查，用当前生成的匹配回归面板和 block bootstrap 评估处理组与对照组差异。所有统计结果均从项目结果表自动派生，避免手工复制造成漂移。

### 3.3 识别策略：公告窗与生效窗的跨市场分解

关键识别逻辑是：信息渠道预测公告窗显著、生效窗约为零、中美量级相近；需求压力渠道预测生效窗显著，且美国因被动规模更大应出现更强反应。本文承认该设计仍是描述性事件研究，不是严格准实验；生效窗约为零也可能由套利者提前交易、市场深度吸收或数据窗口选择共同造成。因此本文把结论限定为“当前证据更支持公告阶段定价”，而不是排除所有需求压力机制。

---

## 4. 实证结果

### 4.1 核心结果：公告窗与生效窗的比较

本节为论文实证主线。主要结果（来自 `results/real_tables/event_study_summary.csv`）：

- 中国公告日 CAR[-1,+1] = +2.07%（t=6.48，p<0.001，n=137）
- 美国公告日 CAR[-1,+1] = +1.87%（t=5.34，p<0.001，n=255）
- 中国生效日 CAR[-1,+1] = +0.49%（t=1.25，p=0.212，不显著）
- 美国生效日 CAR[-1,+1] = -0.15%（t=-0.54，p=0.588，不显著）

完整表格与 CAR 路径图见本骨架 §4.1、以及 paper/tables/ 与 paper/figures/（导出 LaTeX 后对应 manuscript.tex §4.1 的表 1、图 1–4）。公告窗在中美两市场均显著为正，而生效窗均不显著，是本文最重要的三事实组合：有纳入效应、效应集中在公告阶段、跨制度公告窗量级接近。

---

## 5. 限制与讨论

本节把稳健性证据和方法边界放在同一处：先说明核心结果在哪些检验下保持一致，再说明哪些解释不能被当前设计排除。

### 5.1 纳入 vs 剔除不对称

若信息/认证渠道为主导，纳入与剔除应呈方向不对称。当前结果显示，中国纳入公告窗约 +2.07%，剔除约 -1.09%；美国纳入约 +1.87%，剔除约 +0.09%。这一方向差异更接近“纳入带来正向认证或注意力冲击”的解释，而不是简单对称的机械买卖压力。

### 5.2 长窗口 CAR 的持续性

长窗口用于检验短期公告效应是否随后大幅反转。中国 [0,+120] 均值 CAR 约 +0.14%（t=0.07，不显著），美国约 +1.92%（t=1.54，不显著）。点估计为正但统计不显著，说明本文不能声称长期持续超额收益，但也没有看到与纯短暂价格压力一致的大幅反转。

### 5.3 公告效应的跨年稳定性

按公告年份分解，中国 2021–2025 各年均值均为正；美国 2010–2025 大多数年份均值为正。跨年结果支持公告窗正反应不是单一年份驱动，但年度样本差异较大，不能把每个年度都解释为独立显著证据。

### 5.4 匹配对照组的协变量平衡

匹配样本回归把对数市值、前期收益和前期波动率作为关键协变量，并以 SMD<0.25 作为平衡门禁。三项协变量均通过门禁，降低了可观察特征差异驱动主结果的风险。不过匹配不能处理所有不可观察差异，因此其定位是稳健性补充，不是唯一识别来源。

### 5.5 预公告漂移：一项无法排除的不确定性

公告前均值漂移在中国约 +2.75%，美国约 +2.61%，两市场差异 bootstrap p=0.965。该事实要求论文诚实披露：市场可能在正式公告前已部分消化信息，公告窗显著并不等同于“公告当天才第一次被定价”。这也是本文避免强因果表述的关键原因。

### 5.6 数据与方法限制摘要

事后统计功效（来自 `results/real_tables/power_analysis_report.csv`）：

- `H3`: n=4；检验族=binomial_proportion_z_two_sided；power@observed=5.0%；MDE@80% power=49.95%；解读：normal-approx power=0.050 · exact-binomial power=0.000 · posterior P(p>0.60|data)=0.317 (Beta(1,1) uniform prior). MDE@80%=+0.499 概率差（p1≈0.999）。严重欠功效 (power=0.05 < 0.30): n=4 无法在 α=0.05 检出真实命中率 50%；结果按 supplementary 处理是合理的。
- `H4`: n=455；检验族=regression_coef_t_two_sided；power@observed=8.1%；MDE@80% power=2.72%；解读：HC3 regression coef=+0.0050 (SE=0.0097, t=+0.519, p=0.6037), df≈452; two-sided t-test power=0.081. MDE@80% = |coef|≈0.0272 (≈ 5.4× the observed coefficient). 严重欠功效 (power=0.08 < 0.30): n=455 下观测系数 +0.0050 太小 (相对 SE=0.0097)，无法在 α=0.05 下稳健检出。证据不足的判定是 n 不够大，不是 H4 一定错，因此保留为 supplementary 是合理的。
- `H5`: n=1096；检验族=regression_coef_t_two_sided；power@observed=12.5%；MDE@80% power=26.28%；解读：HC3 regression coef=+0.0744 (SE=0.0937, t=+0.794, p=0.4270), df≈1094; two-sided t-test power=0.125. MDE@80% = |coef|≈0.2628 (≈ 3.53× the observed coefficient). 严重欠功效 (power=0.12 < 0.30): n=1096 下仍无法稳健检出该效应。
- `H6`: n=87；检验族=one_sample_t_two_sided；power@observed=99.2%；MDE@80% power=30.38%；解读：Cohen's d (observed) ≈ -0.473 (bucket-SD=0.0325); two-sided t-test power=0.992. 对比小/中/大效应 (d=0.2/0.5/0.8) 的功效 = 0.45 / 1.00 / 1.00. MDE@80% = |d|=0.304. 功效充足 (power=0.99 >= 0.80) — n 足以检出该效应，但观测方向 (heavy<light) 与 H6 预测 (heavy>light) 相反，所以 verdict='证据不足' 并不是 n 不够，而是方向不符。
- `H1`: n=436；检验族=bootstrap_diff_two_sided；power@observed=5.7%；MDE@80% power=5.69%；解读：engine=adjusted: diff=+0.0050, bootstrap SE≈0.0203, bootstrap p=0.8748; 在该 SE 与 n=436 下，两侧 z-test 功效=0.057; MDE@80%≈|diff|=0.0569. 功效偏低 (power=0.06 < 0.50): adjusted 引擎下 bootstrap SE=0.0203 太宽，差异需达到 |diff|≈0.0569 才能在 80% 功效下被检出。
- `H1`: n=436；检验族=bootstrap_diff_two_sided；power@observed=83.9%；MDE@80% power=1.96%；解读：engine=market: diff=+0.0206, bootstrap SE≈0.0070, bootstrap p=0.0004; 在该 SE 与 n=436 下，两侧 z-test 功效=0.839; MDE@80%≈|diff|=0.0196. 功效充足 (power=0.84 >= 0.80): market 引擎下 n=436 足以检出 |diff|≈0.0206 的真实效应。
- `H2`: n=15；检验族=one_sample_t_two_sided；power@observed=5.2%；MDE@80% power=77.80%；解读：engine=adjusted: Cohen's d≈-0.037 (empirical deltas (n_used=15, SD=0.0029)); n_combined=15, two-sided t-test power=0.052, MDE@80%=|d|=0.778. 功效偏低 (power=0.05 < 0.50): adjusted 引擎下 样本 n=15 太小，|d| 必须 ≥0.778 才能在 80% 功效下检出。
- `H2`: n=15；检验族=one_sample_t_two_sided；power@observed=26.1%；MDE@80% power=77.80%；解读：engine=market: Cohen's d≈-0.365 (empirical deltas (n_used=15, SD=0.0037)); n_combined=15, two-sided t-test power=0.261, MDE@80%=|d|=0.778. 功效偏低 (power=0.26 < 0.50): market 引擎下 样本 n=15 太小，|d| 必须 ≥0.778 才能在 80% 功效下检出。
以下限制来自 `docs/limitations.md`，用于约束正文措辞与答辩口径。它们说明哪些结果可以作为主结论，哪些只能作为探索性或附录材料。

---

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
- **事件窗有效 AR 缺失率与幸存者偏差（必须在论文中披露）**：在
  `real_matched_event_panel.csv` 中 **US 处理事件约 39.3%（611 中 240）在 `[-1,+1]`
  窗内无有效 `ar`**（announce / effective 为同一批 240 个证券；宽窗 `[-3,+3]`/`[-5,+5]`
  有效 N 略升至 372/373）；CN 处理与 US/CN 对照均为 0%。
  - **根因（真实价格缺口，非 build/match bug）**：240 个里 236 个整段面板 `close`/`ret`
    全空——它们是退市 / 被并购 / 改代码的旧标的（如 Allergan、ACE→CB），yfinance
    不再返回其 OHLCV。匹配面板按事件×相对日建满网格再左连价格；缺价标的
    `close`/`ret`→NaN（`benchmark_ret` 以日历日为键仍在），故 `ar` NaN，缺失经网格
    padding 暴露，且偏向早年（2010–2017）。
  - **对主结果的影响**：主表（`event_study_summary.csv`）与全部稳健性检验都先
    `dropna(CAR)` 再算，用的都是有效样本——US announce 主表 N 已是加入 255 + 剔除
    116 = **371**（非 611）。故缺失**不改主表数字**，真正限制是**样本选择 / 幸存者偏差**：
    有效样本偏向存续公司，被剔的 240 个非随机集中于并购 / 退市标的，论文须标 N=371。
    `robustness_placebo_car.csv` 现并列 `n_events`（611）与 `n_events_effective`（371），
    以后者为准。
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

---

## 6. 结论与启示

### 6.1 主要结论

第一，中美纳入事件均存在显著正向公告窗 CAR，说明指数纳入效应并未消失。第二，生效窗 CAR 在两国市场均不显著，说明当前样本不支持“生效日机械买盘是主要来源”的强叙事。第三，中美公告窗量级接近，尽管两国被动资金规模、交易制度和投资者结构差异显著；这一事实更符合公告阶段信息、认证、注意力和制度约束共同定价的解释。

### 6.2 实务与研究启示

对投资者而言，纳入事件更应被理解为公告期信息冲击和预期重估，而非可机械追逐的生效日买盘。对研究者而言，下一步应优先改进历史市值、自由流通口径和中国被动 AUM 数据，并把 HS300 RDD 扩展到更多批次后再作为主识别设计。对本文而言，最稳妥的表述是：指数纳入公告带来短期价格反应，但该反应的机制不是单一被动买盘，而是信息、关注、套利和制度约束的组合。

---

## 7. 分析参数

H1-H7 跨市场机制假说、HS300 RDD、灵敏度阈值和裁决基线均属于透明性与复现材料。H1-H7 是观察到 announce-vs-effective / CN-vs-US 不对称结果之后形成的探索性假说；本项目没有预分析计划，因此这些假说不得在论文中表述为事前注册检验。

- 裁决基线日期：2026-05-31
- 裁决基线路径：`snapshots/pre-registration-2026-05-31.csv`
- baseline 冻结天数：35
- PAP deviation：unchanged=7，tightened=0，weakened=0，flipped=0，unverifiable=0
分析参数、阈值、样本边界和机制裁决口径见 `docs/analysis_parameters.md`；公共摘要工件 `data/public/index_research_summary.json`（schema v1）面向外部消费者提供稳定、去路径泄露的机器可读快照。

---

## 参考文献

下列 16 篇文献来自 `literature_catalog.PAPER_LIBRARY`（项目核心文献库）：

启发式文献关联网络（自动）：本项目文献库共 16 篇，共 52 条"主题/方法/年代"关联边；关联最多：Shleifer '86、Harris '86、Wurgler '02；桥梁文献（betweenness）：Shleifer '86、Harris '86、Wurgler '02。这不是已验证引用关系，也不是逐条 bibliography citation 核验；只用于文献综述导航，不得作为被引/引用证据。可视化见 `results/literature/citation_network.png`（中心性 CSV：`results/literature/citation_centrality.csv`，由 `python3 -m index_inclusion_research.citation_graph` 生成）。

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

核心输入表为 `events.csv`、`prices.csv` 和 `benchmarks.csv`。事件表记录市场、ticker、公告日、生效日和纳入/剔除方向；价格表记录日频 close 与可用 OHLCV；基准表记录市场基准收益。被动 AUM 和行业标签属于机制证据字段，口径说明分别见 `docs/real_data_notes.md`、`docs/hs300_rdd_data_contract.md` 和 `docs/analysis_parameters.md`。

### B. CLI 入口 (43 个)

完整 43 个 console scripts 的分组、用法与示例命令见 `docs/cli_reference.md`。RDD 相关命令保留于 CLI 中用于复现与扩展，但 RDD 当前定位为附录 / 方法论补充，不进入本文实证主线。

### C. 复现指南

```bash
make rebuild           # 10 步流水线：从原始数据到事件研究结果
make figures-tables    # 重绘论文级 figure
make paper             # 论文交付包：自动复制到 paper/
index-inclusion-export-public-summary  # 刷新公共摘要
index-inclusion-paper-bundle --force   # 重新生成本骨架
```

交付前执行 `index-inclusion-submission-ready --fail-on-warn`、`index-inclusion-paper-integrity --fail-on-warn`、`make doctor-strict` 与 `make ci`。公共摘要工件：`data/public/index_research_summary.json`（schema v1），是面向外部消费者（sibling 项目、GitHub Pages 日报）的稳定入口。
