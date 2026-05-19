# 命令行入口参考

48 个 console scripts 按用途分组：

- **数据流水线**：`build-event-sample` / `build-price-panel` / `match-controls` / `match-robustness` / `run-event-study` / `run-regressions`
- **样本数据**：`generate-sample-data` / `download-real-data`
- **报表与图表**：`make-figures-tables` / `generate-research-report` / `paper-bundle` / `paper-audit` / `build-hs300-rdd-forest` / `build-cma-verdicts-forest` / `build-cma-sensitivity-forest` / `build-cma-ar-engine-forest` / `build-cma-2d-robustness-heatmap` / `citation-graph` / `verdict-timeline` / `literature-timeline`
- **Dashboard 与三条主线**：`dashboard` / `price-pressure` / `demand-curve` / `identification`
- **HS300 RDD 工具链**：`hs300-rdd` / `prepare-hs300-rdd` / `reconstruct-hs300-rdd` / `plan-hs300-rdd-l3` / `collect-hs300-rdd-l3`（详见 [docs/hs300_rdd_workflow.md](hs300_rdd_workflow.md)）
- **跨市场不对称 + 假说证据**：`cma`（7 条假说 verdict）/ `prepare-passive-aum` / `download-passive-aum-cn` / `download-cn-passive-aum-proxy` / `compute-h6-weight-change` / `refresh-real-evidence` / `power-analysis`（H3/H6 post-hoc 功效）
- **总入口**：`rebuild-all`（10 步流水线一键跑）/ `verdict-summary`（终端速览）/ `pap-diff`（PAP 偏离审计）/ `doctor`（项目健康检查）/ `export-public-summary`（生成 data/public/index_research_summary.json）/ `paper-skeleton`（自动生成 paper/skeleton.md 论文骨架）/ `methodology-summary`（自动生成 paper/methodology_summary.md 方法论摘要卡）/ `paper-integrity`（论文交付前的跨文档一致性发布门禁）/ `tex-export`（生成 Overleaf/XeLaTeX 论文源文件）/ `submission-ready`（论文提交前最后一道发布就绪 go/no-go 门禁）/ `enrich-bib`（用 CrossRef 自动补全 BibTeX 期刊 / 卷 / 页 / DOI）/ `add-paper`（交互式添加新文献到 PAPER_LIBRARY 并同步下游 6 个工件）

> `citation-graph` 生成的是启发式文献关联网络（主题/方法/年代链接），不是逐条 bibliography 引用核验。

所有入口都通过 `pyproject.toml` 的 console scripts 或 `python3 -m index_inclusion_research.<module>` 调用，也可以用 `make rebuild` / `make verdicts` / `make doctor` / `make sync` 简写。安装时使用 `make sync` 会按 `uv.lock` 装锁定版本（CI 也走这条路径）。

## 1. 生成示例数据

```bash
index-inclusion-generate-sample-data
```

如果还没有安装 console script：

```bash
python3 -m index_inclusion_research.sample_data
```

## 2. 下载真实公开数据

```bash
index-inclusion-download-real-data
```

或：`python3 -m index_inclusion_research.real_data`

下载完成后回填 A 股行业标签 + 重建 H6 权重 proxy：

```bash
python3 -m index_inclusion_research.enrich_cn_sectors --force
index-inclusion-compute-h6-weight-change --force
```

把真实样本、H6 权重、CMA verdict、报告与 dashboard 证据覆盖一次刷完：

```bash
index-inclusion-refresh-real-evidence
```

它在 `results/real_tables/evidence_refresh_manifest.json` 和 `.csv` 里写入证据覆盖、doctor 摘要与每步刷新记录；dashboard full/demo 模式直接读取这份 manifest。每张真实证据卡都可以点进 `/evidence/<item>` 看明细表，机器可读版本在 `/api/evidence/<item>`。真实数据说明见 [docs/real_data_notes.md](real_data_notes.md)。

真实样本主结果统一导出到 `results/real_tables/`，包含：`event_study_summary.csv`、`long_window_event_study_summary.csv`、`retention_summary.csv`、`regression_coefficients.csv`、`regression_models.csv`、`data_sources.csv`、`sample_scope.csv`、`identification_scope.csv`。

## 3-7. 流水线 5 步

下面这些都支持 `--profile auto|sample|real`，默认 auto 优先走 real：

```bash
# 3. 清洗事件样本
index-inclusion-build-event-sample

# 4. 构建事件窗口面板
index-inclusion-build-price-panel

# 5. 运行事件研究
index-inclusion-run-event-study

# 6. 构建匹配样本并回归
index-inclusion-match-controls
index-inclusion-build-price-panel \
  --events data/processed/real_matched_events.csv \
  --output data/processed/real_matched_event_panel.csv
index-inclusion-run-regressions

# 7. 导出论文图表 + 表格
index-inclusion-make-figures-tables
```

`make-figures-tables` 默认自动识别 real/sample 工作流；显式回到 sample 路径：`--profile sample`。

`make-figures-tables` 在 `results/literature/hs300_rdd/rdd_robustness.csv` 存在时，会自动调用 `build_hs300_rdd_forest_plot` 输出稳健性森林图（main / donut / placebo±0.05 / polynomial），写到 `results/figures/hs300_rdd_robustness_forest.{png,pdf}`，并把 PNG 镜像到 `results/literature/hs300_rdd/figures/rdd_robustness_forest.png` 给 dashboard 用。如需单独重绘可执行：`index-inclusion-build-hs300-rdd-forest`（支持 `--robustness-csv` / `--png` / `--pdf` 路径覆盖，`--no-mirror-dashboard` 关闭 dashboard 镜像）。

同一调用还会在 `results/real_tables/cma_hypothesis_verdicts.csv` 存在时自动渲染 CMA 跨假说证据强度森林图（H1-H7 在 y 轴，0-1 的 support-strength 评分在 x 轴，按 evidence_tier 上色），写到 `results/figures/cma_verdicts_forest.{png,pdf}`。评分 = f(verdict, confidence)：(支持·高)=1.0 / (支持·中)=0.7 / (部分支持·高)=0.6 / (部分支持·中)=0.5 / (证据不足·中)=0.3 / (证据不足·低)=0.0，仅用于可视化对比，不构成新的统计推断。如需单独重绘：`index-inclusion-build-cma-verdicts-forest`（支持 `--verdicts-csv` / `--png` / `--pdf` 路径覆盖，传空 `--pdf ""` 跳过 PDF）。

针对"verdict 取决于阈值选择"的审稿人质疑，`index-inclusion-build-cma-sensitivity-forest` 会一次性扫 0.05 / 0.10 / 0.15 / 0.20 四个阈值（可用 `--thresholds 0.01 0.05 0.10` 覆盖；自定义阈值至多两位小数），把 CMA pipeline 的 verdicts 重跑结果落到 `results/sensitivity/threshold_<T>/cma_hypothesis_verdicts.csv` 缓存，并生成 H1-H7 × 4 阈值的轨迹图 `results/figures/cma_verdicts_sensitivity.{png,pdf}`：每条假说一根灰色连线串起 4 个 dot，颜色按 evidence_tier、形状区分 circle（相对上一阈值 verdict 稳定）与 triangle（在该阈值 verdict 翻转），右侧 margin 注 `stable` / `1 flip` / `2+ flips`。解释边界：threshold knob 当前只影响 H1/H4/H5 这些 p-gated 假说；H2/H3/H6/H7 是非 p 头条 gate，图中用于参照整体证据强度。详见 [docs/sensitivity_workflow.md](sensitivity_workflow.md) §Forest visualization。

针对"verdict 取决于 AR 模型选择"的审稿人质疑，`index-inclusion-build-cma-ar-engine-forest` 会在同一个阈值（默认 0.10，可用 `--threshold` 覆盖）下扫描两条 AR 引擎（默认 `adjusted` + `market`，可用 `--ar-models adjusted market` 覆盖），把 CMA pipeline 的 verdicts 重跑结果落到 `results/sensitivity/ar_<engine>/cma_hypothesis_verdicts.csv` 缓存，并生成 H1-H7 × 2 引擎的对比图 `results/figures/cma_verdicts_ar_engine.{png,pdf}`：每条假说两个 dot（adjusted=圆形/teal，market=方形/purple），strength 不同时由灰色短箭头串起，右侧 margin 注 `stable` / `flipped`。同目录的 `cma_ar_engine_cache_metadata.json` 会记录 threshold，所以 `--threshold 0.05` 不会复用 `0.10` 的 verdicts。`adjusted` 即文献标准的 `ret − benchmark_ret`，`market` 是市场模型 β-AR，估计窗口 `(-120, -10)` trading days（commit 1e29476）；AR-engine sweep 直接 materialize `market_model_event_panel.csv` / `market_model_matched_event_panel.csv`，不会额外写 `event_study_skipped_events.csv`，如需该 sidecar 请跑 `index-inclusion-run-event-study --ar-model market`。**首次跑 market 引擎需要先做一次完整的 CMA pipeline（约 2-5 分钟），metadata threshold 匹配且上游未更新时下次只是 cache hit。** 详见 [docs/sensitivity_workflow.md](sensitivity_workflow.md) §AR Engine Robustness。

把两条单轴 sweep 合并成同一张图就是 2D 稳健性热力图：`index-inclusion-build-cma-2d-robustness-heatmap` 会跨乘 4 阈值 × 2 引擎 = 8 单元，生成 H1-H7 × 8 单元的色温图 `results/figures/cma_verdicts_2d_robustness.{png,pdf}`，回答"两条方法学 axis 同时变会不会让结论翻"的"同时"那一半。色温编码 support-strength（深红=insufficient/0.0，白=partial/0.5，深蓝=support/1.0），单元中央 ASCII tag (S+/S/P+/I) 提供 greyscale-friendly 解码，列分两组（左 4 列 = adjusted 引擎，右 4 列 = market 引擎，中间用粗黑分隔线），右侧 margin 注每条假说 `stable` / `1 flip` / `2+ flips`（按 8 单元里的 distinct verdict 数）。运行 runner 会先尝试**复用**已有的单轴 cache：(0.10, adjusted) 来自 `ar_adjusted/`、(0.10, market) 来自 `ar_market/`、(T, adjusted) 来自 `threshold_<T>/`，只有 (T≠0.10, market) 三个单元真正需要 fresh CMA pass。新缓存落在 `results/sensitivity/grid_<T>_<engine>/cma_hypothesis_verdicts.csv`。详见 [docs/sensitivity_workflow.md](sensitivity_workflow.md) §2D Robustness。

`match-controls` 现在会同时输出 covariate-balance 表（默认 `match_balance.csv`，与 `--output-diagnostics` 同目录）。`index-inclusion-doctor` 的 `matched_sample_balance` 检查会扫这份表，遇到 |SMD|≥0.25 时变 warn。

`build-price-panel` 默认 AR 仍是基准调整后的 `ret - benchmark_ret`；想额外得到 market-model 残差，加上 `--include-market-model-ar` 即可。该 flag 会在面板上追加四列 `ar_market_model`、`market_model_alpha`、`market_model_beta`、`market_model_estimation_obs`（按事件 × phase 在估计窗口 [-20,-2] 上对 `ret = α + β·benchmark_ret` 做 OLS，估计窗口数据不足时整个事件留 NaN）。`market_model_estimation_obs` 记录该事件 × phase 在估计窗口内同时具备 `ret` 与 `benchmark_ret` 的配对观测数，便于下游审计 NaN 是因为窗口太薄、基准方差为零，还是事件外行未参与估计。把面板传给 `index_inclusion_research.analysis.summarize_market_model_estimation_obs` 可以拿到一行汇总：`n_events_total` / `n_events_finite_ar` / `n_events_nan_ar` / `n_events_below_min_obs` / `minimum_estimation_obs`，其中阈值与模型自带的 OLS 最小观测门槛（2）一致，不引入新的策略选择。

### `index-inclusion-run-event-study --ar-model` 与 `--estimation-window`

`run-event-study` 现在支持两个 AR 引擎，默认与历史输出位级一致：

```bash
# 默认：简单市场调整 (ar = ret − benchmark_ret)；输出与新增 flag 前一致
index-inclusion-run-event-study

# 切换到市场模型 β-AR，估计窗口 (-120, -10)（文献短窗口标准）
index-inclusion-run-event-study --ar-model market

# 自定义估计窗口：LOW,HIGH 为正整数，内部带负号（要求 LOW > HIGH）
index-inclusion-run-event-study --ar-model market --estimation-window 250,21
```

引擎选择建议：

- `adjusted`（默认）：速度快、口径标准，PAP 主表与 CMA verdict 都钉在这一支；
  默认仍是这条路径，不要在论文最终稿换。
- `market`：β 调整后推断力更强，但每个事件至少需要约 30 个估计窗口配对观测
  （`ret` 与 `benchmark_ret` 同时非缺失）才会得到非 NaN 的 AR；样本稀的事件会被
  整段写成 NaN，并落到 `event_study_skipped_events.csv` 而不是静默贡献 0。
  切换引擎在 CN 样本上经验上会让 CAR 偏移约 5-15 bps。

`--ar-model market` 会在输出目录额外写两个文件：

- `event_study_meta.json`：记录 `ar_model` / `ar_column` / `estimation_window` /
  `profile` / `panel` / `car_windows`，便于审稿人和重跑校验。`adjusted` 模式也会
  写这个文件，`estimation_window` 字段为 `null`。
- `event_study_skipped_events.csv`：列出估计窗口内观测不足或基准方差退化导致
  AR=NaN 的 event × phase；含 `event_id` / `event_phase` /
  `market_model_estimation_obs` / `minimum_estimation_obs` / `reason`
  （`insufficient_estimation_obs` 或 `degenerate_benchmark_variance`）。该文件仅
  在 `--ar-model market` 时生成。

注意：Patell/BMP 汇总 (`patell_bmp_summary.csv`) 仍始终基于简单 `ar` 计算；
切换引擎不会把 Patell/BMP 提升进主表。

## 8. 自动生成论文结果摘要

```bash
index-inclusion-generate-research-report
```

默认读 `results/real_event_study/` 与 `results/real_regressions/`，写到 `results/real_tables/research_summary.md`。回到 sample：`--profile sample`。

## 8b. 聚合并审计论文交付包

```bash
index-inclusion-paper-bundle --force            # 默认先刷新衍生图/PAP 审计再拷贝
index-inclusion-paper-bundle --force --no-regenerate   # 跳过预刷新（已跑过 make rebuild 时更快）
index-inclusion-paper-audit --fail-on-warn
```

`paper-bundle` 会把正文表、图、叙事、RDD 附录和 PAP snapshot 聚合到 `paper/`，
默认在拷贝前自动重跑三个会被现有 CSV 驱动的衍生产物（任何一项失败都只 warn，
不打断 bundle）：

1. HS300 RDD 稳健性森林图（`build_hs300_rdd_forest_plot`，输入
   `results/literature/hs300_rdd/rdd_robustness.csv`）→
   `results/figures/hs300_rdd_robustness_forest.{png,pdf}`，同时镜像到
   `results/literature/hs300_rdd/figures/rdd_robustness_forest.png` 给 dashboard。
2. CMA 跨假说证据强度森林图（`build_cma_verdicts_forest_plot`，输入
   `results/real_tables/cma_hypothesis_verdicts.csv`）→
   `results/figures/cma_verdicts_forest.{png,pdf}`。
3. PAP 偏离审计（`build_pap_diff`，输入最新 `snapshots/pre-registration-*.csv` +
   当前 verdicts CSV）→ `results/real_tables/pap_deviation_report.csv`。

输出 (`paper/`)：

- `paper/tables/` — `*.tex` 主表 + `patell_bmp_summary.csv` + `pap_deviation_report.csv`
- `paper/figures/` — `*.png`（CMA / 事件研究 / 新增两个森林图）+ 两张森林图的 `.pdf`
- `paper/rdd/` — HS300 RDD CSV / TeX / 子图
- `paper/narrative/` — paper_outline / limitations / pre_registration 等
- `paper/data/` — `hs300_rdd_candidates.csv` + `snapshots/pre-registration-*.csv`
- `paper/README.md` — 人类可读清单
- `paper/bundle_summary.md` — 研究状态快照
- `paper/manifest.json` — 机器可读清单：每个产物的 source / target / sha256 /
  size_bytes，外加 ``regenerated`` 字段记录三个预刷新步骤的状态
  (`ok` / `skipped` / `error`)。下游审计 / 归档可比对 sha256 判定 drift。

`paper-audit` 逐项检查正文主结论、Patell/BMP 稳健性、CMA core 机制主表、RDD 附录、PAP/limitations 与 `paper/` 交付包是否都有可追溯产物。机器可读输出：

```bash
index-inclusion-paper-audit --format json
```

fresh checkout 尚未生成 `paper/` 时，可以只检查源产物：

```bash
index-inclusion-paper-audit --source-only --fail-on-warn
```

## 9. 打开仪表盘

```bash
index-inclusion-dashboard
```

或 `python3 -m index_inclusion_research.literature_dashboard`。常用入口：

- `/`：一页式总展板（默认 `展示版`，3 分钟汇报切 `?mode=brief`，完整材料切 `?mode=full`）
- `/paper/<paper_id>`：单篇文献速读
- `/paper/<paper_id>/pdf`：原文 PDF
- `/verdict/<hid>`：跳转 verdict 卡片（可分享 URL）
- `/evidence/<item>`：真实证据卡 drilldown
- `/rdd-l3`：HS300 RDD L3 官方候选导入工作台
- `/rdd-l3` 同时展示并可刷新线上采集诊断：`online_search_diagnostics.csv`、`online_year_coverage.csv`、`online_manual_gap_worklist.csv` 与 `online_gap_source_hints.csv`

历史副页 (`/library`、`/review`、`/framework`、`/supplement`、`/analysis/<id>`) 全部 302 redirect 到首页对应锚点。改默认端口：`--port 5002`。

## 10. 直接运行三条研究主线

```bash
index-inclusion-price-pressure
index-inclusion-demand-curve
index-inclusion-identification
```

## 11. 跨市场不对称（CMA）扩展

`index-inclusion-cma` 在 CN / US × announce / effective 四象限上做事件集中度差异（M1 路径 / M2 空窗期 / M3 机制回归 / M4 异质性 / M5 时序 + 假设表）。依赖真实样本 (`real_event_panel.csv`、`real_matched_event_panel.csv`、`real_events_clean.csv`)，缺一即报错，不回退 demo。

```bash
index-inclusion-cma
```

主要产出（`results/real_tables/cma_*.csv` + `results/real_figures/cma_*.png` + `cma_mechanism_panel.tex`）：

- `cma_hypothesis_verdicts.csv`：H1..H7 verdict + confidence + 下一步建议
- `cma_pre_runup_bootstrap.csv`：H1 信息预运行 bootstrap 检验（默认按 `announce_date` 做 block bootstrap，cluster_method 列记录采样方式）
- `cma_gap_drift_market_regression.csv`：H4 卖空约束 OLS-HC3
- `cma_h3_channel_concentration.csv`：H3 双通道显著性
- `cma_h5_limit_predictive_regression.csv`：H5 涨跌停预测回归
- `cma_h6_weight_robustness.csv` / `cma_h6_weight_explanation.csv`：H6 权重稳健性
- `evidence_refresh_manifest.json`：证据覆盖清单
- `research_summary.md`：自动追加 "六、美股 vs A股 不对称" 章节（幂等）

只想刷新 LaTeX：`index-inclusion-cma --tex-only`。叠加 H2 的被动基金 AUM：默认读 `data/raw/passive_aum.csv`（`market, year, aum_trillion`，US 列使用 FRED [`BOGZ1FL564090005A`](https://fred.stlouisfed.org/series/BOGZ1FL564090005A)）；自定义路径用 `--aum`。原始 AUM 列名非标准，先归一化：

```bash
index-inclusion-prepare-passive-aum --input /path/to/raw_aum.csv \
  --output data/raw/passive_aum.csv --force
```

CMA 的 dashboard 集成是自包含 helper：`index_inclusion_research.analysis.cross_market_asymmetry.dashboard_section.build_cross_market_section(...)` 返回 presenter-agnostic context。

## HS300 RDD L3 官方来源采集

```bash
index-inclusion-collect-hs300-rdd-l3 \
  --since 2020-01-01 \
  --until 2022-12-31 \
  --notice-rows 120 \
  --search-term "调整沪深300指数样本股" \
  --force
```

`--since` / `--until` 按中证公告发布日期过滤，适合先补 2020-2022 这类历史窗口；`--notice-rows` 控制每个搜索词最多返回的公告数量；`--search-term` 可以重复传入，作为默认中证搜索词之外的历史标题补充。命令只写采集草稿、来源审计、搜索诊断、年份覆盖、补录缺口和来源查找入口，确认后再用 `index-inclusion-prepare-hs300-rdd --check-only` 验收并写入正式 L3。

默认输出位于 `results/literature/hs300_rdd_l3_collection/`：

- `official_candidate_draft.csv`：可验收的候选草稿
- `online_source_audit.csv`：公告、附件、解析状态和失败原因；Excel 调入/调出名单会被审计，但缺少备选对照时不会写入正式 L3
- `online_search_diagnostics.csv`：每个搜索词的原始返回、标题匹配、主题匹配、日期窗口内匹配情况
- `online_year_coverage.csv`：每个请求年份的 `candidate_found` / `notice_only` / `no_notice` 覆盖状态，并列出已解析调入/对照行数
- `online_manual_gap_worklist.csv`：按 P1/P2/P3 排序的人工补录清单，优先处理“已有调入、缺备选对照”的年份
- `online_gap_source_hints.csv`：为每个缺口生成中证详情页、官方附件、Wayback、站内网页搜索和巨潮全文搜索入口
- `online_collection_report.md`：人类可读汇总和下一步命令

## 12. PAP 偏离审计（`pap-diff`）

把当前 7 条假说 verdict 和 [`docs/pre_registration.md`](pre_registration.md) 冻结的 PAP 基线做结构化比对。和 `verdict-summary --vs-pap` 的字段级 diff 不同，`pap-diff` 把每条假说强制分到 5 类之一，并写一份机器可读 CSV，便于 PAP §7 决策签字与审稿人答复：

| 分类 | 含义 |
|---|---|
| `unchanged` | verdict / confidence / evidence_tier / n_obs / key_value 全部匹配（容差内）|
| `tightened` | verdict 不变，但 confidence 上升（低 → 中 → 高）或 p-value 显著下降 |
| `weakened` | verdict 不变，但 confidence 下降或 p-value 显著上升 |
| `flipped` | verdict 文本变化（如 证据不足 → 支持），**需 PAP §7 签字** |
| `unverifiable` | 基线 / 当前缺行，或 key_value 在某一侧为 NaN |

```bash
# 默认对最新 snapshots/pre-registration-*.csv，写 results/real_tables/pap_deviation_report.csv
index-inclusion-pap-diff

# 信息性模式（默认）—— 始终 exit 0，即便有 flipped
index-inclusion-pap-diff --no-color | tee /tmp/pap_audit.txt

# 当作 CI 闸门 —— flipped 即 exit 1
index-inclusion-pap-diff --strict

# 比对指定基线 / 调阈值
index-inclusion-pap-diff --baseline snapshots/pre-registration-2026-05-03.csv \
  --p-delta-threshold 0.01 --key-value-rel-threshold 0.05

# 只打印不写盘
index-inclusion-pap-diff --no-write
```

输出 CSV `results/real_tables/pap_deviation_report.csv` 每行一条 H1..H7，列：`hid, name_cn, classification, baseline_verdict, current_verdict, baseline_confidence, current_confidence, baseline_evidence_tier, current_evidence_tier, baseline_n_obs, current_n_obs, baseline_key_label, current_key_label, baseline_key_value, current_key_value, notes`。

`verdict-summary --vs-pap` 仍然是日常 diff 的首选（彩色终端 + 字段级前后值）；`pap-diff` 是预注册答辩 / 审计场景的结构化版本。

## 13. 项目健康检查（`doctor`）

`index-inclusion-doctor` 运行一组有界的健康探针，按 pass / warn / fail 打印每项检查 + 建议修复命令。默认 exit 码只数 `fail`；`--fail-on-warn` 把 `warn` 也算进去（`make doctor-strict` / `make ci` 走这条路径）。

当前覆盖的检查（节选，完整定义见 [src/index_inclusion_research/doctor.py](../src/index_inclusion_research/doctor.py) 的 `DEFAULT_CHECKS`）：

- `hypothesis_paper_ids_resolve` — 7 条假说引用的 paper_id 都能在文献目录里找到
- `verdicts_csv_health` — `cma_hypothesis_verdicts.csv` 行齐 H1..H7
- `results_directory_populated` — `results/real_tables/` 12 个 canonical CMA 输出齐全
- `paper_verdict_section_synced` — `docs/paper_outline_verdicts.md` 与 verdict CSV 一致
- `p_gated_verdict_sensitivity` — p-gated 假说没有处于 `[0.05, 0.10)` 边界
- `pending_data_verdicts` — 没有 verdict 卡在 "待补数据"
- `h6_weight_change_readiness` / `h7_cn_sector_readiness` — 机制数据覆盖
- `rdd_l3_sample_readiness` / `rdd_robustness_panel` — HS300 RDD L3 + 4-spec 稳健性面板
- `matched_sample_balance` / `match_robustness_grid` — 配对样本 SMD + 稳健性网格
- `pap_deviation_no_flips` — PAP 偏离审计：任何 `flipped` 假说 → `fail`（需 PAP §7 签字），`tightened` / `weakened` → `warn`，全部 `unchanged` → `pass`；CSV 缺失时调用 `pap_diff.build_pap_diff` 现场重生成
- `pap_snapshot_freshness` — `snapshots/pre-registration-YYYY-MM-DD.csv` > 90 天未刷 → `warn`（建议季度 re-baseline），目录或 snapshot 完全缺失 → `fail`
- `hs300_rdd_forest_artifact` — `results/figures/hs300_rdd_robustness_forest.{png,pdf}` 存在且 mtime ≥ `rdd_robustness.csv`；缺失或 stale 触发 `make figures-tables` 提示
- `cma_verdicts_forest_artifact` — `results/figures/cma_verdicts_forest.{png,pdf}` vs `cma_hypothesis_verdicts.csv` 的同款 mtime 检查
- `citation_graph_artifact` — `results/literature/citation_network.{png,pdf}` vs `citation_centrality.csv` 的同款 mtime 检查；缺失或 stale 触发 `index-inclusion-citation-graph` 提示
- `public_summary_freshness` — `data/public/index_research_summary.json` 存在且 mtime ≥ `cma_hypothesis_verdicts.csv` / `pap_deviation_report.csv` / `rdd_robustness.csv`；缺失或 stale 触发 `index-inclusion-export-public-summary` 提示
- `paper_skeleton_freshness` — `paper/skeleton.md` 存在且 mtime ≥ `cma_hypothesis_verdicts.csv` / `pap_deviation_report.csv` / `rdd_robustness.csv` / `index_research_summary.json`；缺失或 stale 触发 `index-inclusion-paper-skeleton --force` 提示
- `chart_builders_register` — `CHART_BUILDERS` ≥ 12 项
- `console_scripts_importable` — 所有 `pyproject.toml` 入口都能 import
- `paper_audit_claims` — `paper_audit` 不报 warn/fail

```bash
# 默认人读输出，warn 不算失败
index-inclusion-doctor

# CI 严格模式，warn 也算失败
index-inclusion-doctor --fail-on-warn

# 机器可读，喂给 jq / make
index-inclusion-doctor --format json --fail-on-warn
```

## 14. 公开摘要导出（`export-public-summary`）

`index-inclusion-export-public-summary` 把 `results/real_tables/cma_hypothesis_verdicts.csv` / `pap_deviation_report.csv` / `results/literature/hs300_rdd/rdd_robustness.csv` / `snapshots/pre-registration-*.csv` / 已发布 figure 文件汇总为单一精简 JSON `data/public/index_research_summary.json`（~3-5 KB），可安全提交进 Git。下游消费者（例如 sibling 项目 `cn-altdata-brief`、未来的 GitHub Pages 日报）只读这份文件即可拿到 7 条假说裁决、PAP 偏离汇总、threshold × AR-engine 稳健性、HS300 RDD 主结果、文献覆盖、已发布 figure 路径——不需要直接访问 runtime caches、不需要跑 `index-inclusion-cma` 或 `make figures-tables`。

设计要点：

- **schema 稳定**：顶层 `schema_version`（当前 1）控制破坏性变更；同输入同输出（除了 `generated_at`），方便 `git diff` 看出真实数据变化。
- **safety**：永远不写入 absolute path（`path_ref` 是相对 repo root 的字面值）、debug 字段或 CSV 多行 narrative；只保留 `headline_metric` 等小型结构化字段。
- **graceful degrade**：缺 CSV 时对应 key 直接缺席，不写入合成数据。
- **atomic write**：通过 `tempfile.mkstemp` + `os.rename` 保证读者永远看不到半写文件。
- **doctor 守护**：`public_summary_freshness` 检查 mtime，输入 CSV 任何一项比 summary 新 → `warn`。

```bash
# 写入 data/public/index_research_summary.json
index-inclusion-export-public-summary

# 自定义输出位置
index-inclusion-export-public-summary --output /tmp/foo.json

# 不写盘，只打印到 stdout
index-inclusion-export-public-summary --print

# 通过 module 调用（与 console script 等价）
python3 -m index_inclusion_research.export_public_summary
```

## 15. 论文骨架生成（`paper-skeleton`）

`index-inclusion-paper-skeleton` 把当前 verdict CSV / PAP 偏离报告 / HS300 RDD 主结果 / sensitivity 公开摘要 / `docs/limitations.md` / 16 篇文献库蒸馏为一份完整的 Markdown 论文骨架 `paper/skeleton.md`（约 21 KB）。论文写作者只需逐节填写 `[TODO: prose]` 标记的段落，所有数据表、figure 引用、稳健性结论、PAP 合规块都已自动填好。

设计要点：

- **永不编造内容**：每一处需要散文的位置都标注 `[TODO: prose]`，作者可以 `grep "TODO" paper/skeleton.md` 找到所有待写章节。
- **数据自动同步**：H1-H7 verdict 表、HS300 τ/p/n、阈值/AR 引擎/2D 稳健性结论、PAP 偏离 5 类计数与 16 篇参考文献全部来自当前 artifact，不需要手动同步。
- **doctor 守护**：`paper_skeleton_freshness` 检查任意一个输入 (`cma_hypothesis_verdicts.csv` / `pap_deviation_report.csv` / `rdd_robustness.csv` / `index_research_summary.json`) 比 skeleton 新 → `warn`，提示 `index-inclusion-paper-skeleton --force` 漏跑。
- **paper-bundle 集成**：`make paper` (即 `index-inclusion-paper-bundle`) 在 `_regenerate_artifacts` 里自动重生成 skeleton，保证 bundle 永远 self-consistent。
- **sanity 门**：渲染出的 markdown 字节数被 `[6 KB, 28 KB]` 区间约束，越界报 warn，提示骨架被截断或被输入污染。

```bash
# 写入 paper/skeleton.md（已存在则报错）
index-inclusion-paper-skeleton

# 覆盖现有骨架
index-inclusion-paper-skeleton --force

# 自定义输出位置
index-inclusion-paper-skeleton --output /tmp/skeleton.md --force

# 不写盘，直接打印到 stdout
index-inclusion-paper-skeleton --print

# 通过 module 调用（与 console script 等价）
python3 -m index_inclusion_research.paper_skeleton
```

骨架结构（节选）：

- `# 标题 / 摘要 (TODO)`
- §1 引言（3 个 TODO prose 子段）
- §2 文献综述（自动指向 `docs/literature_review_author_year_cn.md`）
- §3 研究设计 + §3.3 H1-H7 假说表（自动填）
- §4 实证结果：§4.1 主结果 figure 引用 + §4.2 H1-H7 逐条 prose TODO + §4.3 HS300 RDD τ/p/n 自动填 + §4.4 稳健性 3 张图 + 自动结论
- §5 限制（`docs/limitations.md` 全文嵌入）
- §6 结论 TODO + §7 PAP 合规自动表
- §参考文献（16 篇自动枚举）+ §附录 ABC

## 16. 启发式文献关联网络（`citation-graph`）

`index-inclusion-citation-graph` 把 16 篇文献库的 `related_paper_ids` 启发式链接（按年代/主题/方法学的相似性，**不是** bibliography 验证的引用）汇总成有向图、计算中心性、输出可视化图与 CSV。生成三件套放在 `results/literature/`：

- `citation_network.png` — 力导向布局 PNG（确定性 seed=0），节点大小 = 启发式入度，颜色按立场（反方 / 中性 / 正方），箭头方向 = 关联对象。
- `citation_network.pdf` — 矢量版同图，供论文 / 演讲嵌入。
- `citation_centrality.csv` — 每行一篇文献，列：`paper_id, in_degree, out_degree, betweenness, eigenvector, top_linked_by, top_links_to`（最后两列 pipe-join 前 3 名 paper_id）。

中心性算法纯 stdlib：Brandes BFS 算无向投影的 betweenness、PageRank-flavored damped 幂迭代算 eigenvector，避免引入 networkx。所有排序的 tie-break 按 paper_id 字典序，输出在 seed 固定时跨运行可复现。

```bash
# 默认产出（覆盖现有 PNG/PDF/CSV）
index-inclusion-citation-graph

# 自定义路径
index-inclusion-citation-graph --png /tmp/net.png --pdf /tmp/net.pdf --csv /tmp/cent.csv

# 跳过 PDF（传空字符串）
index-inclusion-citation-graph --pdf ""

# 换 layout seed（默认 0 — 论文 canonical 图）
index-inclusion-citation-graph --seed 7

# 通过 module 调用（与 console script 等价）
python3 -m index_inclusion_research.citation_graph
```

边的语义在 export-public-summary JSON 的 `literature_network.edge_semantics` 字段里显式标为 `heuristic_similarity_not_bibliographic_citation`，下游消费者不会误把这些链接当成正式引用图。Doctor `citation_graph_artifact` 检查 PNG/PDF 是否比 centrality CSV 新；`heuristic_citation_centrality_schema` 检查 CSV 列名仍是 `top_linked_by` / `top_links_to`（旧版 `top_cited_by` / `top_cites` 视作语言污染立即报 fail）。

## 17. 假说裁决演进时间线（`verdict-timeline`）

`index-inclusion-verdict-timeline` 是 43 个 console scripts 的第 40 号。它通过 `git log --follow` 与 `git show <sha>:results/real_tables/cma_hypothesis_verdicts.csv` 把 H1..H7 的历史裁决从仓库 git 史里重建出来，渲染成一张 7 swimlane 时间线，给 PAP 自律一份**视觉的演化档案**——配合现有的 `pap-diff` 偏离审计（commit `48a22f0`），从“静态对比 PAP 基线”补到“动态展示研究迭代”。

- `results/figures/verdict_timeline.png` — 14×8 in @ 100 dpi 主图，每个 H 一行 swimlane；每个 commit 一个圆点（裁决保持）或方块（裁决文本改变）；颜色按裁决类别（绿=支持，黄=部分支持，红=证据不足）；2026-05-16 PAP baseline 画一条虚线；右侧 annotation 标注每条 H 的最新裁决。
- `results/figures/verdict_timeline.pdf` — 矢量版同图。

```bash
# 默认产出
index-inclusion-verdict-timeline

# 限制只走最近 N 个 commit（默认 50，足够覆盖当前历史）
index-inclusion-verdict-timeline --max-history 20

# 自定义 PAP baseline 日期 / 输出路径 / 跳过 PDF
index-inclusion-verdict-timeline --pap-baseline-date 2026-05-16 --no-pdf

# 模块等价调用
python3 -m index_inclusion_research.outputs.verdict_timeline
```

`export-public-summary` JSON 同步产出 `verdict_timeline` 段：`total_commits_tracked`、`first_commit_date`、`last_commit_date`、`total_verdict_changes`、`verdict_changes_per_hypothesis`。Doctor `verdict_timeline_artifact` 检查 PNG/PDF 是否比源 CSV (`cma_hypothesis_verdicts.csv`) 新；非 git 仓库（如解压的 tarball）下检查与图生成都会自动跳过，不抛错。

## 18. 方法论摘要卡（`methodology-summary`）

`index-inclusion-methodology-summary` 是 43 个 console scripts 的第 41 号。它把当前 verdicts CSV、`data/processed/real_events_clean.csv` 与 `real_matched_event_panel.csv` 行数、`data/public/index_research_summary.json` 的稳健性 / PAP 偏离块、`results/literature/citation_centrality.csv` 的 top-5 eigenvector 中心性、`pyproject.toml` 的 console-scripts 总数与 `doctor.DEFAULT_CHECKS` 的健康检查总数蒸馏成一份 ~3-5 KB 的单页 Markdown「方法论摘要卡」，落地到 `paper/methodology_summary.md`。

与 `paper-skeleton` 的区别：摘要卡**完全不出 `[TODO: prose]` 标记**，所有数值与表格全部从工件自动派生，是答辩 / 评审「你到底做了什么？」一问的速查页。

```bash
# 默认产出（覆盖 paper/methodology_summary.md，~3-5 KB）
index-inclusion-methodology-summary

# 自定义输出位置
index-inclusion-methodology-summary --output /tmp/methodology.md

# 不写盘，直接打印到 stdout
index-inclusion-methodology-summary --print

# 模块等价调用
python3 -m index_inclusion_research.methodology_summary
```

摘要卡结构（8 节）：

- §1 样本规模（H1-H7 假说表 + 事件研究面板 894/212,757 行）
- §2 估计方法（AR 模型 / 标准化 / 多重检验 / Bootstrap / RDD）
- §3 稳健性覆盖（阈值 / AR 引擎 / 联合二维，自动派生 stable/cell 计数）
- §4 PAP 纪律（基线 + 偏离分类 + 审计 CLI + Doctor 主动监控）
- §5 数据契约（`events.csv` / `prices.csv` / `benchmarks.csv` 字段速览）
- §6 复现命令（`make rebuild` / `make-figures-tables` / `paper-bundle --force` / `methodology-summary`）
- §7 关键文献基础（top-5 中心性 + 立场，链路语义启发式相似性而非 bibliography）
- §8 工具链（48 CLI / 30 doctor checks / public summary schema v1 / paper bundle 72 artifacts）

Doctor `methodology_summary_freshness` 检查在任何输入（verdicts CSV / public summary JSON / citation centrality CSV）mtime 比摘要卡新时 `warn`；CI 环境下因 checkout mtime 不可信而自动 pass。`make paper` 与 `paper-bundle --force` 在 `_regenerate_artifacts` 第 7 步自动重生成本摘要卡，使 bundle 永远 self-consistent。

## 19. 跨文档一致性发布门禁（`paper-integrity`）

`index-inclusion-paper-integrity` 是 43 个 console scripts 的第 42 号，也是论文交付前的最后一道**跨文档**门禁。每条单独的生成器（`cma`、`paper-skeleton`、`methodology-summary`、`export-public-summary`、`pap-diff` 等）单测都已经通过，本 CLI 不再核对每个工件的内部正确性，而是核对它们**互相之间**是否仍然 self-consistent。

10 类检查（同源 `paper_integrity.DEFAULT_INTEGRITY_CHECKS`）：

- **hypothesis_set**：`cma_hypothesis_verdicts.csv` 的 7 个 H 行 ⇄ `paper/skeleton.md` 表格 H 行；同样 ⇄ `paper/methodology_summary.md`。
- **figures**：`paper/skeleton.md` 里 `![]()` 引用的 5 张图 ⇄ `results/figures/` 实际文件是否存在。
- **pap**：`pap_deviation_report.csv` 各 hid 的 classification ⇄ `data/public/index_research_summary.json` `pap_deviation_summary` 的聚合计数；同时 ⇄ `paper/skeleton.md` §7 PAP 表。
- **references**：`literature_catalog.PAPER_LIBRARY` 的 16 篇 paper_id ⇄ `paper/skeleton.md` 参考文献章节。
- **cli_count**：`pyproject.toml [project.scripts]` 实际入口数 ⇄ `README.md` CLI shield badge。
- **sample_sizes**：`paper/methodology_summary.md` §1 n_obs 列 ⇄ `cma_hypothesis_verdicts.csv` n_obs 字段。
- **sensitivity**：`paper/methodology_summary.md` §3 稳健性覆盖（stable/cell 计数）⇄ `data/public/index_research_summary.json` `sensitivity_robustness` 块。
- **doctor**：`docs/cli_reference.md` 里提到的 doctor check 总数 ⇄ `doctor.DEFAULT_CHECKS` 实际长度（弱约束 / warn）。

退出码：`0`（全部通过）/ `1`（任何 warn 或 `--fail-on-warn`）/ `2`（任何 fail）。

```bash
# 默认 text 输出（按 info / warn / fail 排序，附 fix command）
index-inclusion-paper-integrity

# JSON 输出供 CI consume
index-inclusion-paper-integrity --format json

# Markdown 表格（嵌入 PR / status report）
index-inclusion-paper-integrity --format markdown

# 严格 CI 模式：任何 warn 也阻断
index-inclusion-paper-integrity --fail-on-warn
```

它也作为 `check_paper_integrity` 接入 `doctor.DEFAULT_CHECKS`：doctor 在 strict 模式（`--fail-on-warn`）下若 integrity 出现 warn 就会一起阻断。把它放到 paper 发布的 last-mile：

```bash
make paper                                  # regenerate bundle artifacts
index-inclusion-paper-integrity --fail-on-warn  # gate
git push                                    # only if integrity passes
```

## 20. LaTeX / Overleaf 导出（`tex-export`）

`index-inclusion-tex-export` 是 43 个 console scripts 的第 43 号。它读取 `paper/skeleton.md` 与 `paper/methodology_summary.md`，生成：

- `paper/manuscript.tex`：含 `ctex`（默认）或 `xeCJK` preamble 的完整 LaTeX 稿件；
- `paper/references.bib`：从 16 篇 `literature_catalog` 条目导出的 BibTeX 草稿。

常用命令：

```bash
index-inclusion-tex-export --force
index-inclusion-tex-export --include-todos false --force
index-inclusion-tex-export --cjk-engine xeCJK --force
```

该导出器只消费已通过 `paper-integrity` 的 Markdown/方法论产物，不改动 verdicts、public summary 或 paper audit 逻辑。默认保留 `\TODO{...}` 方便在 Overleaf 中继续写作；`--include-todos false` 会从 `manuscript.tex` 中去掉 TODO 标记，适合生成送审草稿。

## 21. 论文提交就绪发布门禁（`submission-ready`）

`index-inclusion-submission-ready` 是 44 个 console scripts 的第 44 号，也是论文实际提交前的**最后一道 go/no-go 门禁**。`paper-integrity` 检查的是工件之间的一致性（cross-document drift）；`submission-ready` 则把视角放宽到**整个提交包**：

- **PAPER STRUCTURE**：`paper/skeleton.md` 存在 + 8 个顶级章节齐全（引言 / 文献综述 / 研究设计 / 实证结果 / 限制与讨论 / 结论与启示 / PAP / 参考文献）。
- **PROSE**：扫描 `paper/skeleton.md` 的 `[TODO: ...]` 标记，按章节归类。任何 TODO 都是 `warn`（散文未完稿，不阻断流水线但需要写完）。
- **METHODOLOGY**：`paper/methodology_summary.md` 存在；如果它比 `skeleton.md` 旧超过 1 天则 `warn`。
- **FIGURES**：9 张关键论文图全部存在、非空、宽×高 ≥ 800×600（基于 PNG IHDR 头解析，零额外依赖）。
- **TEX**：`paper/manuscript.tex` + `paper/references.bib` 都存在；BibTeX 条目数 == 16。如果 `pdflatex` 在 PATH 上，则在临时目录里尝试编译一次（失败则 `fail`；`pdflatex` 不可用则 `warn` 表示跳过）。
- **INTEGRITY**：调用 `paper_integrity.check_paper_integrity()`，把它的 fail / warn 桥接到本门禁的同级 status。
- **PAP**：`data/public/index_research_summary.json` 的 `pap_deviation_summary.all_unchanged == true`；任何 `flipped` 直接 `fail`，`tightened` / `weakened` / `unverifiable` 报 `warn`。
- **DOCTOR**：重跑 `doctor.run_all_checks()`，把全部 health checks 的 fail / warn 一起冒泡。
- **PUBLIC SUMMARY**：JSON 存在且不比 `cma_hypothesis_verdicts.csv` 旧（1 分钟时钟漂移容忍）。
- **DATA**：`data/raw/real_events.csv` / `real_prices.csv` / `real_benchmarks.csv` 都存在且必备列 schema 通过。
- **LITERATURE**：`literature_catalog.PAPER_LIBRARY` 至少 16 条目。
- **SENSITIVITY**：3 张 CMA 稳健性图（threshold / AR engine / 2D heatmap）都存在且不比 verdicts CSV 旧。
- **TIMELINE**：`verdict_timeline.png` 存在且不比 verdicts CSV 旧。
- **TESTS**：CLI 内不跑 pytest（避免 8 分钟阻塞 + 写 `.pytest_cache`）；输出 `warn` 提示外部执行 `pytest --maxfail=1 -q`。

聚合结果：

- `ready` — 0 warn / 0 fail，全绿，可以提交；
- `partially_ready` — 0 fail，有 warn，软阻断（默认 exit 1，可继续）；
- `not_ready` — 任何 fail，硬阻断，列出 N 项 blocker。

退出码（与 doctor / paper-integrity 同协议）：`0` ready / `1` partially_ready（或 warn + `--fail-on-warn`）/ `2` not_ready。

```bash
# 默认 text 输出（按 pass / warn / fail 顺序 + 估算剩余工时）
index-inclusion-submission-ready

# JSON 输出供 CI consume
index-inclusion-submission-ready --format json

# Markdown 表格（嵌入 PR / status report）
index-inclusion-submission-ready --format markdown

# 严格 CI 模式：warn 也阻断
index-inclusion-submission-ready --fail-on-warn
```

**估算剩余工时** = `blocker_count * 2.0h + warning_count * 0.5h + skeleton_TODO_count * 1.0h`。它是粗略启发式，意在让"还差多少"这件事有一个数字承诺，而不是模糊的"快了"。

把它放到 paper 发布的 last-mile（接在 `paper-integrity` 之后）：

```bash
make rebuild && make paper
index-inclusion-paper-integrity --fail-on-warn
index-inclusion-submission-ready --fail-on-warn   # 最终 go/no-go
index-inclusion-tex-export --include-todos false --force
```

它是**完全只读的** —— 从不修改任何工件，所有 fix command 都指向对应的生成器（`make-figures-tables` / `paper-skeleton` / `methodology-summary` / `tex-export` 等）。

## 22. BibTeX CrossRef 补全（`enrich-bib`）

`index-inclusion-enrich-bib` 是 45 个 console scripts 的第 45 号，专门解决一个具体的期刊投稿要求：**主流财经期刊（RFS / JFE / JFQA / JBF / MS / JF）要求 bibliography 中每条 `@article` 必须包含完整的 journal name / volume / issue / pages / DOI，缺失字段 → desk reject**。`tex-export` 默认生成的 `paper/references.bib` 只携带文献库里的 `author`、`title`、`year`、`note`，其他字段全是 `[TODO: journal]` 占位符。

`enrich-bib` 把每条占位符喂给 CrossRef 的免费 REST API（`https://api.crossref.org/works`），按"作者 surname 重叠 + 标题 token Jaccard + 年份惩罚"打 0–1 的 confidence score。`--min-confidence`（默认 0.7）以上 → 写回 journal / volume / issue / pages / DOI；以下 → 保留 `[TODO: ...]` 占位符。`author` / `title` / `year` **永远不会被 CrossRef 覆盖**——那是研究者亲自选定的版本。

```bash
# 默认：读 paper/references.bib，写 paper/references.enriched.bib
index-inclusion-enrich-bib

# 严格阈值（只接受非常确信的匹配）
index-inclusion-enrich-bib --min-confidence 0.9

# 看会发生什么但不写文件
index-inclusion-enrich-bib --dry-run

# 也可以通过 tex-export 一次性跑（保留 TODO 占位 + 顺手补全）
index-inclusion-tex-export --force --enrich-bib
```

工程细节：

- **缓存**：CrossRef 响应缓存到 `cache/crossref_cache.json`（包括 miss），重复运行不产生网络流量；删除该文件即可强制刷新。
- **客户端礼貌**：单进程 5 req/s 上限（远低于 CrossRef polite-pool 的 50 req/s 配额），User-Agent 标识项目 URL + 维护者邮箱以获取较高的速率配额。
- **网络容错**：CrossRef 不可达 / 超时 / 返回非-200 → 全部条目低 confidence → 输出 bib 等价于输入 bib（保留所有 `[TODO: journal]`）。**永远不会用空字符串覆盖原始字段**。
- **HTML 实体解码**：CrossRef 的 `container-title` 经常返回 `International Journal of Finance &amp; Economics` 这种 HTML-escape 形式；输出会先 `html.unescape` 再 BibTeX-escape 为 `\&`。
- **BibTeX 格式化**：输出按 `author / title / year / journal / volume / issue / pages / doi / note` 固定顺序排列，便于 diff review。

第一次跑当前 `paper/references.bib`（16 条）的实际结果：**15 条 enriched / 1 条 kept TODO**。剩下的 1 条是姚东旻/张日升/李嘉晟那篇中文期刊文章（CrossRef 返回的 fuzzy match 落在 0.5 confidence，低于 0.7 阈值；作者可手填）。

## 23. 交互式文献库扩展（`add-paper`）

`index-inclusion-add-paper` 是 46 个 console scripts 的第 46 号，专门解决一个具体的研究工作流问题：**论文进入毕业季后，每个月仍会有新的指数效应文献被加入综述（2024-2025 的 JFE/RFS/JF 顶刊出文非常活跃）。手动新增一篇要触碰 6 个文件：`literature_catalog/_data.py` 添加 `LiteraturePaper(...)` 条目、`paper/references.bib` 追加 BibTeX、`citation_graph` 重新渲染 PNG/PDF/CSV、`paper/skeleton.md` 重新生成 §References、`paper/methodology_summary.md` 重新计算 top-5 centrality 引用、`data/public/index_research_summary.json` 更新 `papers_indexed` 计数。手动改一遍要 20-30 分钟且非常容易遗漏其中一处。

`add-paper` 把整套手术封进一个 CLI 入口，使「添加一篇文献」收敛为「填一张表 + 等几秒」：

```bash
# 交互式（在终端逐字段提问）
index-inclusion-add-paper

# 先生成一份可复制修改的 JSON 模板（不读/不写 catalog 或 BibTeX）
index-inclusion-add-paper --print-json-template > greenwood_sammon_2024.json

# 非交互式（从 JSON 文件读全部字段）
index-inclusion-add-paper --from-json greenwood_sammon_2024.json

# Dry-run：只看会发生什么，不写盘
index-inclusion-add-paper --from-json greenwood_sammon_2024.json --dry-run

# 批量添加：维护 PAPER_LIBRARY + references.bib，下游一次性补
index-inclusion-add-paper --from-json a.json --skip-downstream
index-inclusion-add-paper --from-json b.json --skip-downstream
index-inclusion-paper-skeleton --force \
  && index-inclusion-methodology-summary \
  && index-inclusion-export-public-summary \
  && index-inclusion-citation-graph

# 写完并立即跑跨文档门禁
index-inclusion-add-paper --from-json greenwood_sammon_2024.json --run-integrity
```

Mandatory 字段（`add_paper.NewPaper.__post_init__` 强制）：`paper_id`（小写下划线，正则 `[a-z][a-z0-9_]*`）/ `authors`（分号分隔）/ `year`（1800-2199 的四位年份）/ `title` / `position`（`pro_index_effect` / `contra` / `neutral`）/ `market_focus`（`US` / `CN` / `both`）。`related_paper_ids` 也必须是合法 `paper_id` 字符串；`camp` 必须落在现有五个文献阵营内。自由文本字段会用安全 Python literal / BibTeX escaping 写盘，避免 JSON 或 TTY 输入里的换行、反斜杠、花括号、控制字符把 `_data.py` 或 `references.bib` 打坏。可选字段不被允许被编造——缺省一律落到 `[TODO: not provided]` 占位符，研究者后续可以在 `_data.py` 中手填。

设计契约：

- **paper_id 唯一性**：与现有 16 篇库的任何条目重名 → 拒绝（带提示）。这让批量重跑同一 JSON 是幂等的——第二次会报"已存在"而不是默默写两遍。
- **保留现有 thematic tuple**：现有 16 篇不是全局 alphabetical；新增条目会按 `paper_id` 做一次字典序扫描，插到第一个比它大的现有 `paper_id` 前面，否则放到 tuple 尾部。文本编辑而不是 import-time mutation——后者对 frozen dataclass 不可行，且重启 Python 后丢失。
- **paper_id 受限于 ASCII**：避免下游 BibTeX cite-key 解析、文件路径生成、citation graph 节点 ID 等多处对 unicode 不友好的链路出问题。
- **`--dry-run` 不写一个字节**：只打印 catalog diff 字符数 + BibTeX 草稿；适合提交前先看看。
- **`--print-json-template` 是纯模板入口**：只把一份可验证的 `NewPaper` JSON starter payload 打到 stdout，方便研究者另存、编辑、提交 review；它在解析参数后立即退出，不读取或写入 catalog / BibTeX。
- **`--skip-downstream` 仍维护 catalog + BibTeX**：批量场景下避免每次都重渲 citation_network.png / .pdf 的 matplotlib 开销，但 `paper/references.bib` 会随每次 add 同步追加。
- **`--run-integrity` 可选门禁**：写盘后运行 `index-inclusion-paper-integrity --fail-on-warn`；若门禁非零，CLI 返回非零并在报告里保留已写入的部分状态。

输出 `AddPaperReport`：

- `paper_library_count_before / after`（确认 16 → 17 的跨越）
- `catalog_updated` / `bibtex_updated` / `downstream_artifacts`（哪些下游 CLI 跑成功了）
- `dry_run` / `skipped_downstream`（行为标志，便于测试断言）
- `notes`（关联 paper_id 中有未知 ID、批量补丁的下一步指令等）

CI/local 流程建议：每次添加完用 `index-inclusion-paper-integrity --fail-on-warn` 确认跨文档一致性，再 commit。

## 24. 文献年表时间线（`literature-timeline`）

`index-inclusion-literature-timeline` 是 48 个 console scripts 的第 47 号。它把 `PAPER_LIBRARY` 的 16 篇文献按发表年份 × 研究主线（短期价格压力 / 需求曲线效应 / 沪深300论文复现）渲染成一张二维散点图，颜色编码立场（正方=蓝、反方=红、中性=灰），标记大小映射 `results/literature/citation_centrality.csv` 的 `in_degree` 启发式链入度，背景叠加三段「时代带」让综述读者一眼读出场域演化：

- `results/literature/literature_timeline.png` — 14×7 in @ 100 dpi 主图，X 轴 1986→2026，Y 轴三条主线（top-down），每个 marker 标注 `Surname 'YY` 短引用；1986-2002 浅灰带为 classical（Shleifer / Harris-Gurel 创世之战）、2002-2014 浅橙带为 skeptics（Wurgler / Madhavan / Petajisto / Kasch-Sarkar）、2014+ 浅绿带为 China + identification（Chang-Hong-Liskovich / 沪深300 复现 / Greenwood-Sammon）。
- `results/literature/literature_timeline.pdf` — 矢量版同图，论文 §2 文献综述直接 `\includegraphics` 引入。

这一图与 `citation-graph` 的网络图互补：网络图回答「16 篇文献怎么相互连接」，时间线回答「这场争论何时开始、何时转向、何时被中国市场接力」，两张图同放综述章节就把空间与时间两个维度都铺开。

```bash
# 默认产出（覆盖 results/literature/literature_timeline.{png,pdf}）
index-inclusion-literature-timeline

# 只看 PNG，跳过矢量版
index-inclusion-literature-timeline --no-pdf

# 自定义 X 轴范围（例如只画 1986-2015 的「未数中国市场」视角）
index-inclusion-literature-timeline --year-min 1986 --year-max 2015

# 自定义 centrality CSV 路径（用旧版备份对比效应）
index-inclusion-literature-timeline --centrality-csv /tmp/citation_centrality_v1.csv

# 模块等价调用
python3 -m index_inclusion_research.outputs.literature_timeline
```

`export-public-summary` JSON 同步产出 `literature_timeline` 段：`years_covered`、`n_papers`、`n_papers_pre_2002` / `2002_to_2014` / `post_2014`、`dominant_position_by_era`、`anchors_by_era`。Doctor `literature_timeline_artifact` 检查 PNG/PDF 是否比源 CSV (`citation_centrality.csv`) 新；citation CSV 缺失时渲染回退到等大 marker，论文 bundle 永不空。

设计契约：
- **不发明文献元数据**：每个 marker 的位置 / 颜色 / 主线分类完全来自 `literature_catalog._data.py` 的 `LiteraturePaper`，禁止任何 hard-code 覆盖。
- **`年份待核验` 占位安全降级**：Yao-Zhang-Li 沪深300 那篇中文 year_label 解析失败 → fallback 2014 + INFO 日志；未来 catalog 修正后图自动校正。
- **空输入也出 ≥800×600 PNG**：极端「无文献」分支也能让 `paper-bundle` 拿到合法图，避免半成品交付。
- **确定性渲染**：matplotlib 后端 `Agg`，`np.random.seed(0)`，同一 catalog 多次跑出字节一致 PNG/PDF（便于 git diff 与 CI 缓存）。

## 25. 低-n 假说后验功效（`power-analysis`）

`index-inclusion-power-analysis` 是 48 个 console scripts 的第 48 号。它解决审稿人对低-n 假说的「**有这么小的 n，你究竟能不能检出真实效应？**」反驳——把"我不知道"变成具体的功效数字（power at observed effect、80% 功效下的 MDE、与小/中/大 Cohen's *d* 的对照）。

输出 `results/real_tables/power_analysis_report.csv` + `power_analysis_report.md`：

- **H3 (n=4, 双通道命中率)**：
  - 测试族：单比例 z-test（正态近似）+ exact-binomial 对照。
  - 在观测命中率 75% 下功效 ≈ 0.13（normal-approx）/ 0.00（exact，α=0.05 下不存在 rejection region）。
  - 80% 功效下的 MDE ≈ +0.50 概率差（即 p1≈1.0）。
  - Bayesian 后验 P(p > 0.60 \| 3/4 hits, Beta(1,1) uniform prior) ≈ 0.66。
  - 释义：**严重欠功效**，把 H3 归入 supplementary 是数据驱动的，不是出于偏好。
- **H6 (n=67, heavy−light spread)**：
  - 测试族：单样本 t-test，Cohen's *d* = mean/pooled-SD。
  - 当 H6 weight × event panel 在盘上时，pooled SD 由实际数据重算（n_heavy=34，n_light=33）；缺失时回退到 H6 OLS-HC3 r²=0.033 反推的 \|d\|≈0.18，并在 interpretation 里明文说明。
  - 在观测 d≈-0.73 下功效 ≈ 1.0；与 d=0.2/0.5/0.8 对比的功效 ≈ 0.36/0.98/1.00。
  - 80% 功效下的 MDE ≈ \|d\|=0.35。
  - 释义：**功效充足**，但观测方向 (heavy<light) 与 H6 预测 (heavy>light) 相反 → "证据不足" 是方向不符，**不是** n 太小。

模块 API:

- `binomial_proportion_power(n, p0, p1, alpha=0.05, alternative='two-sided') → PowerResult`
- `t_test_power(n, effect_size, alpha=0.05, alternative='two-sided') → PowerResult`
- `mde_at_power(n, test='t'|'proportion', target_power=0.80, alpha=0.05) → float`
- `bootstrap_observed_power(observed_data, null_value=0.0, alpha=0.05, n_bootstrap=1000) → BootstrapPowerResult`
- `beta_posterior_probability_above(successes, n, threshold, prior_alpha=1.0, prior_beta=1.0) → float`
- `compute_h3_power(observed_hit_rate=0.75, n=4) → HypothesisPowerReport`
- `compute_h6_power(observed_spread=-0.019, observed_sd=None, n=67, ...) → HypothesisPowerReport`

```bash
# 默认产出
index-inclusion-power-analysis

# 自定义 α / target power
index-inclusion-power-analysis --alpha 0.10 --target-power 0.90

# 只看 markdown 报告，不写盘
index-inclusion-power-analysis --print

# 跳过 markdown 二份，只写 CSV
index-inclusion-power-analysis --md-output ''

# 模块等价调用
python3 -m index_inclusion_research.power_analysis
```

设计契约：

- **scipy 内核，不重新发明轮子**：z-test / nct.cdf / binom.cdf 全走 scipy.stats。
- **明示 Bayesian 先验**：H3 的 P(p>0.60) 默认使用 Beta(1, 1)（uniform）；任何更换都需要在 interpretation 文字里明说。
- **小样本警示**：n=4 在 α=0.05 二侧 exact-binomial 不存在 rejection region；这是 H3 "证据不足" 的统计依据，而非审美选择。
- **方向 vs 强度的分离**：H6 power≈1 与 verdict "证据不足" 同时存在，是因为方向相反——这是 power analysis 区别于 simple p-value reporting 的本质价值。
- **集成到 `docs/limitations.md` §7**：与 `index-inclusion-export-public-summary` 的 `power_analysis` 段同步：reviewer 看到 "power\<0.30" 这一行不是攻击点，是 n=4 自带的物理限制。

## Verdicts ↔ Literature 双向链接

每条 H1..H7 在 [hypotheses.py](../src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py) 注册时同时声明 `paper_ids`。两端都消费这个映射：

- Dashboard verdict 卡片显示 "支持文献 (N) [paper_id_chip]"，每个 chip 链到 `/paper/<paper_id>`。
- `/paper/<paper_id>` 详情页渲染 "CMA 假说证据" 段，列出引用本论文的假说 + 当前 verdict + 头条指标。
- `/verdict/<hid>` 302-redirect 到 `/?mode=full#hypothesis-<hid>`；typo 返回 404。

## 交互式 ECharts 图层

dashboard `demo` / `full` 模式下渲染交互图（基于 ECharts CDN），`/api/chart/<chart_id>` 拉 JSON。`car_path` / `car_heatmap` / `price_pressure` / `gap_decomposition` / `heterogeneity_size` / `time_series_rolling` / `main_regression` / `mechanism_regression` / `event_counts` / `cma_mechanism_heatmap` / `cma_gap_length_distribution` / `rdd_scatter` 都已注册。

`IntersectionObserver` 懒加载；未识别 chart_id 返回 404。新增图见 [chart_data.py](../src/index_inclusion_research/chart_data.py) 的 `CHART_BUILDERS` 注册表与 [interactive_charts.js](../src/index_inclusion_research/web/static/dashboard/interactive_charts.js) 的 `CHART_OPTION_BUILDERS`。
