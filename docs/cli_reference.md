# 命令行入口参考

40 个 console scripts 按用途分组：

- **数据流水线**：`build-event-sample` / `build-price-panel` / `match-controls` / `match-robustness` / `run-event-study` / `run-regressions`
- **样本数据**：`generate-sample-data` / `download-real-data`
- **报表与图表**：`make-figures-tables` / `generate-research-report` / `paper-bundle` / `paper-audit` / `build-hs300-rdd-forest` / `build-cma-verdicts-forest` / `build-cma-sensitivity-forest` / `build-cma-ar-engine-forest` / `build-cma-2d-robustness-heatmap` / `citation-graph` / `verdict-timeline`
- **Dashboard 与三条主线**：`dashboard` / `price-pressure` / `demand-curve` / `identification`
- **HS300 RDD 工具链**：`hs300-rdd` / `prepare-hs300-rdd` / `reconstruct-hs300-rdd` / `plan-hs300-rdd-l3` / `collect-hs300-rdd-l3`（详见 [docs/hs300_rdd_workflow.md](hs300_rdd_workflow.md)）
- **跨市场不对称 + 假说证据**：`cma`（7 条假说 verdict）/ `prepare-passive-aum` / `download-passive-aum-cn` / `download-cn-passive-aum-proxy` / `compute-h6-weight-change` / `refresh-real-evidence`
- **总入口**：`rebuild-all`（10 步流水线一键跑）/ `verdict-summary`（终端速览）/ `pap-diff`（PAP 偏离审计）/ `doctor`（项目健康检查）/ `export-public-summary`（生成 data/public/index_research_summary.json）/ `paper-skeleton`（自动生成 paper/skeleton.md 论文骨架）

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

`index-inclusion-verdict-timeline` 是 40 个 console scripts 的第 40 号。它通过 `git log --follow` 与 `git show <sha>:results/real_tables/cma_hypothesis_verdicts.csv` 把 H1..H7 的历史裁决从仓库 git 史里重建出来，渲染成一张 7 swimlane 时间线，给 PAP 自律一份**视觉的演化档案**——配合现有的 `pap-diff` 偏离审计（commit `48a22f0`），从“静态对比 PAP 基线”补到“动态展示研究迭代”。

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

## Verdicts ↔ Literature 双向链接

每条 H1..H7 在 [hypotheses.py](../src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py) 注册时同时声明 `paper_ids`。两端都消费这个映射：

- Dashboard verdict 卡片显示 "支持文献 (N) [paper_id_chip]"，每个 chip 链到 `/paper/<paper_id>`。
- `/paper/<paper_id>` 详情页渲染 "CMA 假说证据" 段，列出引用本论文的假说 + 当前 verdict + 头条指标。
- `/verdict/<hid>` 302-redirect 到 `/?mode=full#hypothesis-<hid>`；typo 返回 404。

## 交互式 ECharts 图层

dashboard `demo` / `full` 模式下渲染交互图（基于 ECharts CDN），`/api/chart/<chart_id>` 拉 JSON。`car_path` / `car_heatmap` / `price_pressure` / `gap_decomposition` / `heterogeneity_size` / `time_series_rolling` / `main_regression` / `mechanism_regression` / `event_counts` / `cma_mechanism_heatmap` / `cma_gap_length_distribution` / `rdd_scatter` 都已注册。

`IntersectionObserver` 懒加载；未识别 chart_id 返回 404。新增图见 [chart_data.py](../src/index_inclusion_research/chart_data.py) 的 `CHART_BUILDERS` 注册表与 [interactive_charts.js](../src/index_inclusion_research/web/static/dashboard/interactive_charts.js) 的 `CHART_OPTION_BUILDERS`。
