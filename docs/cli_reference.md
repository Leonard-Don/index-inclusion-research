# 命令行入口参考

32 个 console scripts 按用途分组：

- **数据流水线**：`build-event-sample` / `build-price-panel` / `match-controls` / `match-robustness` / `run-event-study` / `run-regressions`
- **样本数据**：`generate-sample-data` / `download-real-data`
- **报表与图表**：`make-figures-tables` / `generate-research-report` / `paper-bundle` / `paper-audit` / `build-hs300-rdd-forest`
- **Dashboard 与三条主线**：`dashboard` / `price-pressure` / `demand-curve` / `identification`
- **HS300 RDD 工具链**：`hs300-rdd` / `prepare-hs300-rdd` / `reconstruct-hs300-rdd` / `plan-hs300-rdd-l3` / `collect-hs300-rdd-l3`（详见 [docs/hs300_rdd_workflow.md](hs300_rdd_workflow.md)）
- **跨市场不对称 + 假说证据**：`cma`（7 条假说 verdict）/ `prepare-passive-aum` / `download-passive-aum-cn` / `download-cn-passive-aum-proxy` / `compute-h6-weight-change` / `refresh-real-evidence`
- **总入口**：`rebuild-all`（10 步流水线一键跑）/ `verdict-summary`（终端速览）/ `pap-diff`（PAP 偏离审计）/ `doctor`（项目健康检查）

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
index-inclusion-paper-bundle --force
index-inclusion-paper-audit --fail-on-warn
```

`paper-bundle` 会把正文表、图、叙事、RDD 附录和 PAP snapshot 聚合到 `paper/`。
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

## Verdicts ↔ Literature 双向链接

每条 H1..H7 在 [hypotheses.py](../src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py) 注册时同时声明 `paper_ids`。两端都消费这个映射：

- Dashboard verdict 卡片显示 "支持文献 (N) [paper_id_chip]"，每个 chip 链到 `/paper/<paper_id>`。
- `/paper/<paper_id>` 详情页渲染 "CMA 假说证据" 段，列出引用本论文的假说 + 当前 verdict + 头条指标。
- `/verdict/<hid>` 302-redirect 到 `/?mode=full#hypothesis-<hid>`；typo 返回 404。

## 交互式 ECharts 图层

dashboard `demo` / `full` 模式下渲染交互图（基于 ECharts CDN），`/api/chart/<chart_id>` 拉 JSON。`car_path` / `car_heatmap` / `price_pressure` / `gap_decomposition` / `heterogeneity_size` / `time_series_rolling` / `main_regression` / `mechanism_regression` / `event_counts` / `cma_mechanism_heatmap` / `cma_gap_length_distribution` / `rdd_scatter` 都已注册。

`IntersectionObserver` 懒加载；未识别 chart_id 返回 404。新增图见 [chart_data.py](../src/index_inclusion_research/chart_data.py) 的 `CHART_BUILDERS` 注册表与 [interactive_charts.js](../src/index_inclusion_research/web/static/dashboard/interactive_charts.js) 的 `CHART_OPTION_BUILDERS`。
