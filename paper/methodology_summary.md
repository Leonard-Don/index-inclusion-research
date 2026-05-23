# 指数纳入效应跨市场不对称研究 · 方法论摘要

**生成时间**: 2026-05-23 | **裁决基线快照**: 2026-05-16

## 1. 样本规模

| 假说 | 名称 | n_obs | 证据层级 | 主线 |
|---|---|---:|---|---|
| H1 | 信息泄露与预运行 | 436 | core | identification |
| H2 | 被动基金 AUM 差异 | 17 | core | demand_curve |
| H3 | 散户 vs 机构结构 | 4 | supplementary | price_pressure |
| H4 | 卖空约束 | 436 | supplementary | identification |
| H5 | 涨跌停限制 | 936 | core | identification |
| H6 | 指数权重可预测性 | 67 | supplementary | demand_curve |
| H7 | 行业结构差异 | 187 | core | identification |

**事件研究面板**:

- 真实事件：893 行 (`data/processed/real_events_clean.csv`)
- 匹配对照面板：212,756 行 (`data/processed/real_matched_event_panel.csv`，Stuart 2010 SMD；covariate balance pass)
- 时间窗：CAR[-1,+1] / [-3,+3] / [-5,+5]

## 2. 估计方法

| 维度 | 方法 |
|---|---|
| AR 模型 | 默认 `ret - benchmark_ret` (简单市场调整)；可选 market-model β (估计窗口 -120 to -10) |
| 标准化 | t 检验 (默认) + Patell Z (1976) + BMP t (1991) |
| 多重检验 | Bonferroni & Benjamini-Hochberg |
| Bootstrap | Block bootstrap (按 `announce_date` 分块，5000 iterations) |
| RDD (HS300) | Local linear regression (bandwidth 0.06)；donut / polynomial / placebo 稳健性 |

## 3. 稳健性覆盖

| 轴 | 范围 | 假说稳定数 |
|---|---|---|
| 阈值 | 0.05 / 0.1 / 0.15 / 0.2 | 7/7 |
| AR 引擎 | adjusted / market | 5/7 |
| 联合 | 8 cells = 4 阈值 × 2 AR 引擎 | 5/7 |

### 3.1 各假说后验功效（H3 / H4 / H5 / H6 + H1 / H2 分引擎）

| 假说 | n | 测试族 | 在观测效应下的功效 | 80% 功效下的 MDE |
|---|---:|---|---:|---:|
| H3 | 4 | binomial_proportion_z_two_sided | 0.134 | 0.499 (proportion_gap_p1_minus_p0) |
| H4 | 436 | regression_coef_t_two_sided | 0.095 | 0.028 (coef_at_target_power) |
| H5 | 936 | regression_coef_t_two_sided | 0.752 | 0.164 (coef_at_target_power) |
| H6 | 67 | one_sample_t_two_sided | 1.000 | 0.347 (cohens_d_at_target_power) |
| H1 | 436 | bootstrap_diff_two_sided | 0.057 | 0.057 (diff_at_target_power) |
| H1 | 436 | bootstrap_diff_two_sided | 0.839 | 0.020 (diff_at_target_power) |
| H2 | 15 | one_sample_t_two_sided | 0.052 | 0.778 (cohens_d_at_target_power) |
| H2 | 15 | one_sample_t_two_sided | 0.261 | 0.778 (cohens_d_at_target_power) |

完整解读见 `results/real_tables/power_analysis_report.md` 与 `docs/limitations.md` §7。

## 4. 裁决基线快照

H1-H7 为事后探索性假说（在观察到公告日 vs 生效日、中美市场的不对称之后形成），本研究无预注册分析计划；下表的裁决基线快照仅用于裁决稳定性追踪。

| 项 | 状态 |
|---|---|
| 裁决基线快照 | 冻结于 2026-05-16 (`snapshots/pre-registration-2026-05-16.csv`) |
| 当前偏离 | 全部未偏离（all_unchanged=True） |
| 偏离审计 CLI | `index-inclusion-pap-diff` (默认非阻断 / `--strict` 阻断) |
| Doctor 主动监控 | `check_pap_deviation_no_flips` · `check_pap_snapshot_freshness` |

## 5. 数据契约

- `events.csv`：`market` / `index_name` / `ticker` / `announce_date` / `effective_date` / `event_type` / `source` / `sector` / `note`（后 4 列可选）
- `prices.csv`：`market` / `ticker` / `date` / `close` / `ret` / `volume` / `turnover` / `mkt_cap` / `sector`
- `benchmarks.csv`：`market` / `date` / `benchmark_ret`

## 6. 复现命令

```bash
make rebuild                              # 10-step pipeline refresh
index-inclusion-make-figures-tables       # all figures
index-inclusion-paper-bundle --force      # paper artifacts
index-inclusion-methodology-summary       # regenerate this card
```

## 7. 关键文献基础（前 5 中心节点 · 共 16 篇文献库）

| Paper | Authors | Year | Position | Eigenvector |
|---|---|---|---|---:|
| `shleifer_1986` | Andrei Shleifer | 1986 | 正方 | 0.611 |
| `harris_gurel_1986` | Lawrence Harris; Eitan Gurel | 1986 | 反方 | 0.552 |
| `wurgler_zhuravskaya_2002` | Jeffrey Wurgler; Ekaterina Zhuravskaya | 2002 | 中性 | 0.270 |
| `lynch_mendenhall_1997` | Anthony W. Lynch; Richard R. Mendenhall | 1997 | 正方 | 0.264 |
| `chang_hong_liskovich_2014` | Yen-Cheng Chang; Harrison Hong; Inessa Liskovich | 2014 | 正方 | 0.172 |

边的语义为启发式相似性，非 bibliography 验证的引用。完整 16 节点中心性见 `results/literature/citation_centrality.csv`。

## 8. 工具链

- 48 个 console scripts（见 `docs/cli_reference.md`）
- Doctor：30 个 health checks（主动监控 verdicts + figures + PAP + paper skeleton + methodology summary）
- Public summary：`data/public/index_research_summary.json` (schema v1)
- Paper bundle：72 artifacts auto-collected from `results/`，含本卡片 (`paper/methodology_summary.md`)
