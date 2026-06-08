# 论文与答辩交付包

本文档是当前研究版本的写作和答辩总入口。它不替代自动产出的结果表，
而是规定哪些证据进入正文、哪些证据只进附录，以及答辩时应该如何表述边界。

## 1. 一句话结论

指数纳入事件在中美市场都主要表现为**公告日显著正向超额收益**，而不是生效日集中跳涨：
CN inclusion 的公告日 `CAR[-1,+1]` 为 **+2.07%** (t=6.48, p<0.001)，
US inclusion 为 **+1.87%** (t=5.34, p<0.001)；生效日窗口在两国市场都不显著。

论文主线因此应写成：

> 指数纳入效应仍然存在，但主要在公告阶段完成定价；不同市场的制度、行业结构和交易限制
> 影响这一价格反应的传导机制。被动资金需求冲击是候选机制之一，但当前结果不支持把上涨
> 主要归因于生效日机械买盘。

## 2. 正文证据边界

7 条 CMA 假说的证据强度概览图（推荐放论文 §4.3 机制裁决主图，附录另放
H7 行业交互稳健性）：

![CMA 跨市场不对称 — 7 条假说证据强度对比](../results/figures/cma_verdicts_forest.png)

> 图说：H1-H7 在 y 轴，support-strength 评分 (0-1) 在 x 轴。颜色按
> `evidence_tier`（core = 深青色 / supplementary = 灰色），右侧 monospace
> 列为 `n=N | tier | verdict/conf`。评分 = f(verdict, confidence)，
> 仅用于跨假说可视化对比，不构成新的统计推断。重绘：
> `index-inclusion-build-cma-verdicts-forest`，详见
> [docs/cli_reference.md](cli_reference.md)。

正文只保留三类证据：

| 位置 | 证据 | 当前口径 | 来源 |
|---|---|---|---|
| 主结果 | 事件研究 `CAR[-1,+1]` | 公告日显著，生效日不显著 | `results/real_tables/event_study_summary.csv` |
| 方法稳健性 | Patell/BMP 标准化异常收益 | CN/US inclusion 公告日仍显著 | `results/real_event_study/patell_bmp_summary.csv` |
| 机制主表 | `evidence_tier=core` 的 CMA 假说 | H1 证据不足，H5 证据不足，H7 支持；H7 另有行业交互回归补强 | `results/real_tables/cma_hypothesis_verdicts.csv`、`cma_h7_sector_interaction.csv`；证据强度概览图 `results/figures/cma_verdicts_forest.{png,pdf}` |

正文不要把 7 条 CMA 假说全部并列成主表。`docs/analysis_parameters.md` 记录的主表 / 附录划分如下：

- **正文 core**：H1 信息泄露与预运行、H5 涨跌停限制、H7 行业结构差异；
  H2 被动基金 AUM 在补入 CN ETF TNA proxy 后由 supplementary 升级为 core
  (combined-n 阈值 15,US rolling 12 + CN rolling 5 = 17,通过 `EVIDENCE_TIER_PROMOTION_FLOOR`)。
- **附录 supplementary**：H3 散户 vs 机构结构、H4 卖空约束、H6 指数权重可预测性。
- **HS300 RDD**：**尝试过但识别失败的设计**，定位为 preliminary，**不进入主表**、不作因果支柱。
  它在数学上不构成 RDD（running variable 是按序号铺出的等距数列、与处理变量 100% 共线、断点两侧零重叠、
  识别假设无从陈述、McCrary p≈0.44 是等距构造的假象），仅在附录里诚实记录这次失败的识别尝试（见
  `docs/paper_outline_verdicts.md` 的"HS300 RDD：为何无法作为识别"）。

H2 现在以 proxy 形式呈现：CN 一侧用 `data/raw/cn_passive_aum_proxy.csv`
(CSI300 + CSI500 跟踪 ETF 的年终 TNA 聚合,经 akshare 抓取),US 一侧仍是
Federal Reserve Z.1。论文写作时必须披露：
(1) CN proxy 不是基金业协会披露的官方被动 AUM 口径；
(2) ETF 宇宙逐年扩张，2020-2023 快照可能低估；
(3) 当前 verdict 是"证据不足"（切换到持牌 Tushare A 股口径后由"部分支持"翻转）——
    CN 端方向符合 H2(AUM 上升 + effective CAR 下降),US 端方向不一致(effective CAR 没有持续衰减),
    所以不能写"中美都被验证"，也不能写成"被动买盘单一机制"。

## 3. 推荐论文结构

### 引言

先回答“是否上涨”：公告日 CAR 在 CN 和 US 都显著为正。随后提出真正的问题：
既然生效日不显著，为什么市场提前定价，且中美市场的格局如何对照？贡献定位为**描述性**而非因果：
(a) 中国市场首次系统的"公告日 vs 生效日"分解；(b) 对 Greenwood-Sammon (2022)"消失的指数效应"
的**跨市场(CN vs US)复制**；(c) 诚实的稳健性 / 功效追踪。引言须明写：本文不主张因果识别。

### 数据与样本

引用 `results/real_tables/sample_scope.csv`、`event_counts.csv`、`data_sources.csv`。
必须同时引用 [docs/limitations.md](limitations.md)，尤其说明 `mkt_cap` 和 `turnover`
使用 Yahoo `sharesOutstanding` 近似，不是交易所历史自由流通口径。

### 研究设计

主设计包括（全部为**描述性**，不主张因果识别）：

1. 公告日 / 生效日双窗口事件研究（描述性时序分解，**不是 DiD**：无 pre-period、无 event×time 交互、无平行趋势检验）。
2. 匹配样本回归与 covariate balance 检查（treatment 系数是横截面 CAR **水平差**，不是双重差分处理效应）。
3. CMA 机制假说裁决，正文只引用 core 层；H1–H7 为**事后 / 探索性**假说。
4. HS300 RDD 仅作附录里"**尝试过但识别失败**"的设计记录，不作识别证据。

不要把 HS300 RDD 写成因果识别主轴：它在数学上不构成 RDD（running variable 是按序号铺出的等距数列、
与处理变量 100% 共线、断点两侧零重叠、识别假设无从陈述），即便把 L3 从当前 11 批次 / 356 行扩到
≥20 批次 / ≥10 年也救不回这一点。

### 实证结果

正文结果建议按这个顺序写：

1. **主结果**：公告日显著正向，生效日不显著（描述性事实）。
2. **标准化稳健性**：Patell/BMP 仍支持 announcement inclusion effect。
3. **稳健性证据**（见"稳健性"一节）：逐日 AAR 平行性图 / 伪事件日 placebo / 事件级置换检验
   将集中呈现——这些证据用于约束描述性结论的稳健性，不把它包装成因果识别。
4. **机制裁决（探索性 / 附录）**：H7 支持，H1 / H5 证据不足；H1 不支持信息泄露解释。
5. **附录里的失败识别尝试**：HS300 RDD 不构成 RDD（见研究设计一节），全套稳健性面板如实附上，但不作识别结论。

### 结论

结论应强调“公告期定价 + 制度机制差异”，而不是泛泛写“指数基金买入导致上涨”。
更稳妥的表述是：

> 本研究支持指数纳入公告带来的短期价格反应，但当前证据更像是公告阶段的信息、
> 注意力和制度约束共同作用，而不是生效日被动资金机械买盘的单一解释。

## 4. 主表与附录清单

| 类型 | 建议标题 | 使用文件 |
|---|---|---|
| 表 1 | 样本覆盖与数据来源 | `event_counts.tex`、`sample_scope.csv`、`data_sources.tex` |
| 表 2 | 公告日 / 生效日事件研究 | `event_study_summary.tex` |
| 表 3 | CMA core 假说裁决 | `cma_hypothesis_verdicts.csv` 中 `evidence_tier=core` |
| 图 1 | CAR path by market and event phase | `real_figures/*_car_path.png` |
| 图 2 | CMA 机制热力图 | `real_figures/cma_mechanism_heatmap.png` |
| 图 3 | CMA 跨假说证据强度森林图 | `results/figures/cma_verdicts_forest.png`（同名 `.pdf` 为矢量版本） |
| 附录 A | supplementary 假说裁决 | H2/H3/H4/H6 相关 `cma_*.csv` |
| 附录 B | H7 行业交互稳健性 | `cma_h7_sector_interaction.csv` |
| 附录 C | HS300 RDD | `results/literature/hs300_rdd/`；稳健性森林图 `results/figures/hs300_rdd_robustness_forest.png`（同名 `.pdf` 为论文用矢量版本） |
| 附录 D | 数据与方法限制 | `docs/limitations.md` |
| 附录 E | 分析参数与 verdict diff | `docs/analysis_parameters.md`、`docs/verdict_iteration.md`、`results/real_tables/pap_deviation_report.csv`（每行一条假说 unchanged/tightened/weakened/flipped/unverifiable 分类） |
| 附录 F | 假说裁决演进时间线 | `results/figures/verdict_timeline.{png,pdf}`（从 git log 重建 H1-H7 swimlane 图，配合裁决基线快照 diff 提供视觉化研究迭代档案；由 `index-inclusion-verdict-timeline` 生成） |

`make paper` 会把上述核心材料聚合到 `paper/` 目录，其中叙事文件在
`paper/narrative/`，表格在 `paper/tables/`（含 `pap_deviation_report.csv` 裁决基线偏离审计
快照），论文级跨假说 / RDD 稳健性森林图在 `paper/figures/` 中以 PNG + PDF 双格式提供
（`cma_verdicts_forest.{png,pdf}`、`hs300_rdd_robustness_forest.{png,pdf}`），RDD 材料在
`paper/rdd/`。`paper/manifest.json` 给每个拷贝过来的产物记录 sha256 / size / source /
target，便于检查交付包是否 drift。`paper-bundle` 默认在拷贝前会自动重跑这两张森林图
和裁决基线偏离审计 CSV，所以即使 `make rebuild` 早于最新一次 verdict 修改，`make paper` 仍
能交付一致的快照（已跑过 rebuild 时可加 `--no-regenerate` 跳过这一步）。

## 5. 答辩口径

常见追问可以这样回答：

| 追问 | 推荐回答 |
|---|---|
| 这是不是严格因果？ | **不是。** 本文是描述性事件研究：announce vs effective 时序分解是事实，不是 DiD（无 pre-period / event×time 交互 / 平行趋势检验）；匹配回归 treatment 系数是横截面水平差。HS300 RDD 在数学上不构成 RDD，只作附录里"尝试过但识别失败"的记录。全文不主张因果参数。 |
| 为什么不是被动基金生效日买盘？ | 生效日 CAR 不显著。H2 已补入 CN 一侧 ETF TNA proxy 后合并 n=18（tier=core 仅指 n 充足，不等于被支持）；Tushare 口径下 verdict 为"证据不足/低"——两市场 AUM 与 effective rolling CAR 的方向关系不一致，无法支持"被动买盘单一机制"。当前仍支持公告期定价 + 制度机制为主的解释。 |
| 7 条假说是否事前注册？ | **否。** H1–H7 是 post-hoc、探索性假说，在观察到 announce-vs-effective / CN-vs-US 不对称结果之后才形成，本项目没有预分析计划。判据与阈值记录在 `docs/analysis_parameters.md`（透明性文档，非 pre-analysis plan）；引用时按 post-hoc 探索性证据表述，详见 `docs/limitations.md` §6。 |
| RDD 为什么不进主表？ | 因为它在数学上**不是 RDD**：running variable 是按官方名单序号铺出的等距数列（间隔 0.01），与处理变量 100% 共线、断点两侧零重叠，识别假设根本无从陈述；McCrary p≈0.44 是等距构造的假象，placebo +0.05 反而接近显著（警告信号）。这不是"样本不够"，而是设计本身不成立——扩样本也救不回。它只作附录里诚实的失败记录。 |
| 数据口径最大限制是什么？ | 历史市值和换手率是 Yahoo sharesOutstanding 近似；HS300 官方 ranking score 不公开。 |

## 6. 交付前验证

每次准备提交论文材料前跑（**`submission-ready` 是最后一道实际的 go/no-go 门禁**）：

```bash
make rebuild
make paper
make paper-audit
index-inclusion-paper-integrity --fail-on-warn   # 跨文档一致性
index-inclusion-submission-ready --fail-on-warn  # 提交就绪 go/no-go
index-inclusion-tex-export --force               # Overleaf / XeLaTeX 源文件
make doctor-strict
make ci
```

`index-inclusion-submission-ready` 把 ~17 项检查（prose TODO 完成度 / 9 张图存在+尺寸 / TeX + BibTeX / paper-integrity 重跑 / 裁决基线 all_unchanged / doctor 全检 / 3 个 raw CSV schema / sensitivity + verdict-timeline 新鲜度等）聚合成单一 `ready` / `partially_ready` / `not_ready` 结论，并给出粗略剩余工时估算。详细规约见 [docs/cli_reference.md §21](cli_reference.md)。任何 `fail` → 论文未到提交就绪状态。

`index-inclusion-paper-integrity` 是论文交付的**最后一道门禁**：每个单独的生成器（`cma` / `paper-skeleton` / `methodology-summary` / `export-public-summary` / `pap-diff`）单测都已通过，但它们生成的工件**互相之间**仍可能 drift（一边重生成、另一边没追上）。此 CLI 跑 10 个跨文档比对：verdicts CSV 的 7 行 H ⇄ skeleton/methodology 的表格；skeleton 的 5 张 figure 引用 ⇄ `results/figures/` 实际文件；`pap_deviation_report.csv` 的 per-row classification ⇄ `data/public/index_research_summary.json` 的聚合计数；`pyproject.toml [project.scripts]` 的入口数 ⇄ README CLI badge；`literature_catalog.PAPER_LIBRARY` 的 16 篇 ⇄ skeleton §References；methodology summary 的 n_obs ⇄ verdicts CSV；methodology summary 的稳健性 stable/cell 计数 ⇄ public summary `sensitivity_robustness`。退出码 0=全通过 / 1=warn / 2=fail。**任何 fail → 论文未到发布就绪状态**。详情见 [docs/cli_reference.md §19](cli_reference.md)。

`index-inclusion-tex-export --force` 是 gate 之后的写作格式出口：它不会重新解释证据，只把已生成的 `paper/skeleton.md` 与 `paper/methodology_summary.md` 转成 `paper/manuscript.tex`，并从 16 篇文献库生成 `paper/references.bib`。默认保留 `\TODO{...}` 方便 Overleaf 续写；送审草稿可加 `--include-todos false`。

`paper/references.bib` 默认携带 `[TODO: journal]` / `[TODO: volume]` / `[TODO: pages]` / `[TODO: doi]` 占位符——主流财经期刊（RFS / JFE / JFQA / JBF / MS / JF）要求完整 bibliography，缺字段 → desk reject。运行 `index-inclusion-enrich-bib` 用 CrossRef 自动补全这些字段（confidence 阈值 0.7，低于阈值保留 TODO，原始 `author`/`title`/`year` 永不覆盖），缓存命中跳过网络调用，CrossRef 不可达时输出 bib 等价于输入 bib：

```bash
index-inclusion-enrich-bib                       # 默认：references.bib → references.enriched.bib
index-inclusion-tex-export --force --enrich-bib  # 或者一次性 inline 补全
```

当前 16 条文献的实际跑分：15 条 enriched / 1 条 kept TODO（中文期刊文章，confidence 0.5 < 0.7）。详见 [docs/cli_reference.md §22](cli_reference.md)。

如果改过 dashboard 或截图，再跑：

```bash
make smoke
```

交付包生成后先看：

1. `paper/bundle_summary.md`：自动研究状态快照。
2. `paper/narrative/research_delivery_package.md`：本文档副本。
3. `paper/narrative/paper_outline_verdicts.md`：当前裁决叙事。
4. `paper/rdd/rdd_robustness.csv`：RDD 全套稳健性。配套图：`paper/figures/hs300_rdd_robustness_forest.{png,pdf}`（同时保留 `paper/rdd/rdd_robustness_forest.png` 给 dashboard），把 main / donut / placebo±0.05 / polynomial 共五个规格的 τ 与 95% CI 放在同一张森林图里，避免论文只引用显著的 main spec（详见 [docs/limitations.md](limitations.md) §RDD 稳健性透明披露要求）。
5. `paper/figures/cma_verdicts_forest.{png,pdf}`：H1–H7 跨假说 support-strength 森林图，按 evidence_tier 上色，给答辩 / 论文 figure 1 用。
6. `paper/tables/pap_deviation_report.csv`：每条假说 baseline → current 的 unchanged / tightened / weakened / flipped / unverifiable 分类，便于答辩前快速回答"哪几条假说自上一次裁决基线快照后发生了变化"。
7. `paper/manifest.json`：每个产物的 sha256 / size / 来源路径 + ``regenerated`` 状态块，给归档 / CI / paper-audit drift 检测用。

裁决基线稳定性现在由 `index-inclusion-doctor` 主动把关——`pap_deviation_no_flips` 检查见到任何 `flipped` 假说直接 `fail`（提示在 `docs/analysis_parameters.md` §7 记录这次裁决口径变更），`tightened` / `weakened` 报 `warn`；`pap_snapshot_freshness` 在最新 `snapshots/pre-registration-YYYY-MM-DD.csv` 超过 90 天时 `warn`，提示季度刷新裁决基线快照。两张森林图（HS300 RDD + CMA verdicts）的 PNG/PDF 也被 doctor 追 mtime——比对应的输入 CSV 旧就 `warn` 提示 `make figures-tables` 漏跑。`make doctor-strict` 让这些 `warn` 全部转成非零退出码。

## 7. Public Summary Artifact

`data/public/index_research_summary.json` 是一份小而稳定（3–5 KB）的 schema-versioned JSON，蒸馏自 `results/real_tables/cma_hypothesis_verdicts.csv` / `pap_deviation_report.csv` / `results/literature/hs300_rdd/rdd_robustness.csv` / 最新 `snapshots/pre-registration-*.csv` / `results/figures/*.png`。它是这份交付包对**外部消费者**（例如 sibling 项目 `cn-altdata-brief`、未来的 GitHub Pages 日报、CI 集成）的标准入口。

**关键属性**：

- **不需要跑任何东西**：消费者只需 `requests.get(raw_github_url)`（或 `git pull` + `open(path)`）即可拿到最新一次提交时的 7 条假说裁决、裁决基线偏离五类计数、threshold × AR-engine 稳健性、HS300 RDD 主结果与文献覆盖。
- **schema 稳定**：顶层 `schema_version`（当前 1）控制破坏性变更；additive 字段不 bump。
- **NO file path / debug 字段泄露**：`path_ref` 是相对 repo root 的字面值，没有 absolute path、没有 raw narrative text；CSV 多行 `evidence_summary` / `metric_snapshot` 只保留 4 sig figs 的 `headline_metric`。
- **figures_published** manifest 列出所有 doctor 守护的 PNG 路径，消费者据此渲染预览。
- **doctor 守护**：`public_summary_freshness` 检查在任何输入 CSV mtime 比 summary 新时 `warn`，提示 `index-inclusion-export-public-summary` 漏跑。
- **deterministic**：同输入 → 同输出（除 `generated_at`），`git diff` 只显示真实数据变化。

更新触发条件：每次 `index-inclusion-cma` / `pap-diff` / `make figures-tables` 后，跑一次 `index-inclusion-export-public-summary` 即可刷新；CI 也可以把这一步纳入 paper-bundle 之后的同一条 pipeline。

## 8. Paper skeleton automation

`paper/skeleton.md` 是项目内置的论文骨架，由 console script `index-inclusion-paper-skeleton` 一键生成。它把当前 verdict CSV、裁决基线偏离报告、HS300 RDD 主结果、threshold × AR-engine × 2D 稳健性结论、`docs/limitations.md` 全文与 16 篇参考文献蒸馏成一份 ~21 KB 的 Markdown 论文模板。

**对论文写作流程的价值**：

- **节省 1-2 天结构性工作**：所有 section 标题、figure 引用 (`![图 X](../results/figures/...)`)、verdict 表、参考文献 16 条枚举、裁决基线偏离 7 行 deviation 表都已就位，写作者只需扫过 `[TODO: prose]` 标记，逐节填写中文段落。
- **永不需要手动同步数据**：H1-H7 verdict、HS300 τ/p/n、阈值/AR/2D 稳健性结论、裁决基线偏离 5 类计数都从当前 artifact 自动派生；任何一处 verdict 更新，跑 `index-inclusion-paper-skeleton --force` 即可让骨架重新对齐。
- **doctor 守护**：`paper_skeleton_freshness` 检查在任何输入 (`cma_hypothesis_verdicts.csv` / `pap_deviation_report.csv` / `rdd_robustness.csv` / `index_research_summary.json`) mtime 比 skeleton 新时 `warn`。
- **paper-bundle 集成**：`make paper` 在 `_regenerate_artifacts` 内部自动重生成 skeleton，所以 bundle 永远 self-consistent — 拉一份 zip 出去，骨架就是当前最新数据状态的论文模板。

**写作动线**：

1. `make rebuild && make figures-tables` 刷新 verdict + figure。
2. `index-inclusion-paper-skeleton --force` 重生成骨架（或直接 `make paper` 一键全套）。
3. 打开 `paper/skeleton.md`，`grep "TODO"` 列出所有待写段落（约 17 处）。
4. 逐节填写 prose，需要的 figure / 表 / 数据均已被骨架引用。
5. 输出最终论文（`.docx` / `.tex` / `.pdf`），proofread 与编辑器整理。

## 9. 更新规则

- 修改 verdict 计算逻辑、阈值、样本边界或 evidence_tier 后，在
  [docs/analysis_parameters.md](analysis_parameters.md) §7 变更日志记录原因与影响。
- 如果新增外部 HS300 L3 数据，先跑 `index-inclusion-prepare-hs300-rdd --check-only`，
  再跑 `make doctor-strict`。
- 如果 README、paper outline 和 CSV 不一致，以 CSV 为准，随后同步文档与 `docs/analysis_parameters.md`。

## 附录 G. Methodology summary card

`paper/methodology_summary.md` 是项目内置的单页方法论摘要卡，由 console script `index-inclusion-methodology-summary` 自动生成。和 `paper/skeleton.md` 不同，摘要卡**完全不出 `[TODO: prose]` 标记**——它把当前 verdicts CSV、事件研究面板行数 (894 / 212,757)、`data/public/index_research_summary.json` 的阈值 / AR 引擎 / 联合二维稳健性、裁决基线偏离五类计数、`results/literature/citation_centrality.csv` 的 top-5 eigenvector 中心节点、`pyproject.toml` console-scripts 总数与 `doctor.DEFAULT_CHECKS` 健康检查总数蒸馏成一份 ~3-5 KB 的速查卡。

**面向场景**：答辩 / 评审 / 课程汇报 / 同行简短问答（「你到底做了什么？」「样本多大？」「稳健性覆盖几个轴？」「裁决相对基线快照稳定吗？」），不需要拉开论文 §3 prose。

**8 节速览**：

1. 样本规模（H1-H7 假说表 + 事件研究面板 + 匹配对照面板 + 时间窗）
2. 估计方法（AR 模型 / 标准化 / 多重检验 / Bootstrap / RDD）
3. 稳健性覆盖（阈值 / AR 引擎 / 联合二维，自动派生 stable/cell 计数）
4. 裁决基线快照（基线 + 偏离分类 + 审计 CLI + Doctor 主动监控）
5. 数据契约（`events.csv` / `prices.csv` / `benchmarks.csv` 字段速览）
6. 复现命令（`make rebuild` / `make-figures-tables` / `paper-bundle --force` / `methodology-summary`）
7. 关键文献基础（top-5 中心性 + 立场）
8. 工具链（CLI / Doctor / Public summary / Paper bundle 计数）

**值与监控**：

- **永不需要手动同步**：所有 8 节的数值都从当前 artifact 自动派生；任何 verdict / 稳健性 / 文献库 / pyproject 变化，跑 `index-inclusion-methodology-summary` 即可同步。
- **doctor 守护**：`methodology_summary_freshness` 检查在任何输入（verdicts CSV / public summary JSON / citation centrality CSV）mtime 比摘要卡新时 `warn`。
- **paper-bundle 集成**：`make paper` 在 `_regenerate_artifacts` 第 7 步自动重生成摘要卡，bundle 永远 self-consistent。

**用法**：

```bash
index-inclusion-methodology-summary            # 默认覆盖 paper/methodology_summary.md
index-inclusion-methodology-summary --print    # 直接打到 stdout（适合 paper-skeleton 附录嵌入）
make paper                                     # 与 skeleton.md 一起重生成
```
