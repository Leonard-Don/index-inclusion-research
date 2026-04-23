---
title: 跨市场公告—生效不对称分析包（CMA）设计稿
date: 2026-04-23
status: design-approved
owner: leo
---

# 跨市场公告—生效不对称分析包（Cross-Market Asymmetry，CMA）

## 一、研究问题与现象

### 1.1 研究问题

> 为什么美股指数纳入效应主要集中在公告日，而 A 股在公告日和生效日都有异常——
> 更重要的是：**美股 vs A 股**在事件集中度上到底差在哪、为什么差。

### 1.2 从现有数据核对的精细化现象

由 `results/real_tables/research_summary.md` 的当前口径反向核对：

| 市场 | 事件日 | CAR[-1,+1] | t | 显著 |
|---|---|---|---|---|
| CN | announce | +1.75% | 4.93 | 是 |
| CN | effective | +0.42% | 0.93 | 否 |
| US | announce | +1.47% | 5.19 | 是 |
| US | effective | −0.12% | −0.51 | 否 |

但是机制回归（真实匹配样本）显示：

| 市场 | 事件日 | turnover | volume | volatility |
|---|---|---|---|---|
| CN | announce | NS | NS | NS |
| **CN** | **effective** | **+ ★** | **+ ★** | **− ★** |
| US | announce | + ★ | + ★ | + ★ |
| US | effective | **− ★** | **− ★** | NS |

因此研究的真正现象是：

- **A 股**：公告日拉价（CAR 显著）、但微结构静默；生效日 CAR 尚未显著但**换手 / 成交量同升 + 波动压低**，呈现明显"需求冲击"签名
- **美股**：公告日 CAR 与微结构**同时**被触发；生效日 CAR 无额外变化，微结构反而**反向抽回**
- 所以核心叙事不是"双峰 vs 单峰 CAR"，而是 **"A 股：公告拉价 / 生效拉量；美股：公告两样都拉 / 生效两样都撤"**

### 1.3 为什么这个现象值得做成一个模块

- 直接对应论文核心贡献：**在统一口径下把信息效应与需求 / 流动性效应在两个制度下分开**
- 16 篇文献框架高度偏美股，跨市场不对称是项目天然的差异化点
- 已有底座足以支撑：`real_event_panel` / `real_matched_event_panel` / robustness 工具链 / dashboard 分层都可以直接接入

---

## 二、目标与非目标

### 2.1 目标

- 交付一个可独立运行的分析包 `cross_market_asymmetry`
- 产出 5 组模块结果（M1–M5）、1 套结构假设表、1 个 dashboard section、1 条 CLI、1 套 LaTeX 论文表 / 图导出
- 直接对接论文的一整章（"美股 vs A 股 事件集中度差异"）

### 2.2 非目标（v1 明确不做）

- 不动 `literature_catalog.py` 的 3 主线结构；CMA 作为跨市场对比的独立 section，不成为"第 4 主线"
- 不改现有 `real_event_study` / `real_regressions` / `robustness_*` 的输出路径或 schema
- 不引入新数据源；ETF AUM / 机构持股作为 M5 的**可选扩展**，数据不存在时跳过而不是阻断
- 不做因子模型异常收益升级（FF3 / FF5）——留作后续独立功能
- 不做 placebo / 跨市场扩展（FTSE、TOPIX）——留作后续独立功能

---

## 三、代码结构

```
src/index_inclusion_research/
  analysis/
    cross_market_asymmetry/
      __init__.py              # 公开 API 聚合
      paths.py                 # M1
      gap_period.py            # M2
      mechanism_panel.py       # M3
      heterogeneity.py         # M4
      time_series.py           # M5
      hypotheses.py            # 结构假设表
      orchestrator.py          # run_cma_pipeline()
  cross_market_asymmetry.py    # CLI 薄入口（argparse + 调 orchestrator）
  cli.py                       # 新增 run_cma_main()
```

说明：
- 子包结构对齐现有 `analysis/rdd_candidates.py`、`analysis/rdd_reconstruction.py` 的组织风格
- 薄 CLI 入口对齐现有 `hs300_rdd.py` / `figures_tables.py` 的拆法
- 入口在 `cli.py` 里用 `_run_package_main("index_inclusion_research.cross_market_asymmetry")`

---

## 四、模块契约

每个模块都给出：输入 / 主函数签名 / 输出表 schema / 图表清单。

### M1 `paths.py` — 事件窗口路径对比

**输入**：`data/processed/real_event_panel.csv`

**主函数**：

```python
def build_daily_ar_panel(panel: pd.DataFrame) -> pd.DataFrame: ...
def compute_average_paths(ar_panel: pd.DataFrame) -> pd.DataFrame: ...
def compute_window_summary(
    ar_panel: pd.DataFrame,
    windows: list[tuple[int, int]] = DEFAULT_WINDOWS,
) -> pd.DataFrame: ...
```

`DEFAULT_WINDOWS = [(-1, 1), (-3, 3), (-5, 5), (-20, -1), (2, 20), (0, 60)]`

**输出表 schema**（关键列）：

- `cma_ar_path.csv`：`market, event_phase, day_offset, n_events, ar_mean, ar_se, ar_t`
- `cma_car_path.csv`：`market, event_phase, day_offset, n_events, car_mean, car_se, car_t`
- `cma_window_summary.csv`：`market, event_phase, window_start, window_end, car_mean, car_se, car_t, p_value, n_events`

**图表**：

- `cma_ar_path_comparison.png`（2×2 subplot）
- `cma_car_path_comparison.png`（2×2 subplot）

### M2 `gap_period.py` — 公告—生效空窗期分析

**输入**：`data/processed/real_events_clean.csv` + `real_event_panel.csv`

**event-level 关键量**：

| 列 | 定义 |
|---|---|
| `gap_length_days` | `effective_date − announce_date`（日历日 / 交易日两种；默认交易日）|
| `pre_announce_runup` | 相对 announce 的 CAR[-20, -1] |
| `announce_jump` | 相对 announce 的 CAR[-1, +1] |
| `gap_drift` | 从 announce+1 到 effective-1 的 CAR |
| `effective_jump` | 相对 effective 的 CAR[-1, +1] |
| `post_effective_reversal` | 相对 effective 的 CAR[+2, +20] |

**主函数**：

```python
def compute_gap_metrics(events: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame: ...
def summarize_gap_metrics(gap_df: pd.DataFrame) -> pd.DataFrame: ...
```

**输出**：

- `cma_gap_event_level.csv`：event 级别，列为上表 6 个指标 + `market, ticker, announce_date, effective_date`
- `cma_gap_summary.csv`：`market, metric, mean, median, se, t, p_value, n_events`

**图表**：

- `cma_gap_length_distribution.png`（直方图 / 核密度，CN vs US 叠加）
- `cma_gap_decomposition.png`（柱状图，announce_jump / gap_drift / effective_jump / post_effective_reversal 四段，CN vs US 双市场对比）——论文主图之一

### M3 `mechanism_panel.py` — 机制差异回归

**输入**：`data/processed/real_matched_event_panel.csv`

**基础回归**：

```
outcome ~ β·treated + γ·controls + FE + ε
```

- `outcomes = [car_1_1, turnover_change, volume_change, volatility_change, price_limit_hit_share]`
- `controls = [log_mktcap_pre, pre_turnover]`
- `FE ∈ {∅, event_year, event_year + sector}`（3 规格）
- 估计时按 `(market, event_phase)` 四象限独立做
- SE：默认 HC3；样本允许时按 `event_date` cluster（对齐现有 `run_regressions` 口径）

**新增变量**：

- `price_limit_hit_share`：事件窗口 `[-5, +5]` 内日度 `|ret| ≥ 0.099` 的占比——A 股近似涨跌停命中率，美股天然 ≈ 0，构成制度差异的定量刻画

**主函数**：

```python
def build_mechanism_panel(matched_panel: pd.DataFrame) -> pd.DataFrame: ...
def estimate_quadrant_regression(
    panel: pd.DataFrame,
    market: str,
    event_phase: str,
    outcome: str,
    spec: str,
) -> RegressionResult: ...
def assemble_mechanism_comparison_table(results: list[RegressionResult]) -> pd.DataFrame: ...
```

**输出**：

- `cma_mechanism_panel.csv`：`market, event_phase, outcome, spec, coef, se, t, p_value, n_obs, r_squared`
- `cma_mechanism_panel.tex`（Booktabs 风格，论文可直贴）
- `cma_mechanism_heatmap.png`（4 象限 × 5 outcome 的 signed-t heatmap）

### M4 `heterogeneity.py` — 横截面异质性矩阵

**切分维度**（各自独立产出一份结果）：

| dim | 分箱 |
|---|---|
| `size` | pre-event 市值五分位（**市场内**分位）|
| `liquidity` | pre-event 30 天平均换手五分位（市场内）|
| `sector` | 按现有 `sector` 列（CN 用申万一级 / US 用 GICS）|
| `gap_bucket` | `gap_length_days` 离散化：`≤10 / 11–20 / >20` |

**每个 cell 计算**：

- `CAR[-1,+1]` announce
- `CAR[-1,+1]` effective
- `gap_drift`
- `asymmetry_index = (effective_jump + gap_drift) / (announce_jump + ε)`
  - ε = 1e-4（避免零除）
  - 语义：> 1 说明该 cell 高度偏向"A 股式"双阶段冲击

**主函数**：

```python
def build_heterogeneity_panel(panel: pd.DataFrame, dim: str) -> pd.DataFrame: ...
def compute_cell_statistics(het_panel: pd.DataFrame) -> pd.DataFrame: ...
```

**输出**：

- `cma_heterogeneity_{size|liquidity|sector|gap_bucket}.csv`：`market, dim, bucket, announce_car, effective_car, gap_drift, asymmetry_index, n_events, se_*`
- `cma_heterogeneity_matrix_{dim}.png`：market × bucket heatmap，色度为 `asymmetry_index`

### M5 `time_series.py` — 时序演变

**规格**：

- 滚动 5 年窗口、步长 1 年
- 每窗口内计算：`mean_announce_car`、`mean_effective_car`、`mean_gap_drift` 及 t
- 结构变点：轻量版 pre/post 对比（pre/post 2010，两市场分别做）

**主函数**：

```python
def build_rolling_car(
    event_panel: pd.DataFrame,
    window_years: int = 5,
    step_years: int = 1,
) -> pd.DataFrame: ...
def summarize_structural_break(
    rolling_df: pd.DataFrame,
    split_year: int = 2010,
) -> pd.DataFrame: ...
```

**可选扩展**（ETF AUM）：

- 如果 `data/raw/passive_aum.csv` 存在（`market, year, aum_trillion`），叠加到时序图的右轴
- 否则跳过叠加，在输出 manifest 里标注 `aum_overlay = False`

**输出**：

- `cma_time_series_rolling.csv`：`market, event_phase, window_end_year, car_mean, car_se, car_t, n_events`
- `cma_time_series_break.csv`：`market, event_phase, period, car_mean, car_se, diff_vs_other, t_diff`
- `cma_time_series_rolling.png`：4 条线 + 可选 AUM 右轴

---

## 五、结构假设表 `hypotheses.py`

把 6 个候选机制写成结构化数据，让 dashboard 可直接渲染"假设—证据"对照表：

```python
@dataclass(frozen=True)
class StructuralHypothesis:
    hid: str                    # "H1"
    name_cn: str
    mechanism: str              # 一句话机制描述
    implications: list[str]     # 可检验命题
    evidence_refs: list[str]    # 指向 M1-M5 的具体输出单元
    verdict_logic: str          # "若 X → 支持"
```

六个假设：

1. **H1 信息泄露与预运行**——CN 公告前漂移应显著高于 US
2. **H2 被动基金 AUM 差异**——US 生效日效应衰减与美股被动规模负相关
3. **H3 散户 vs 机构结构**——CN 生效日量能更集中在散户时段
4. **H4 卖空约束**——CN 缺少做空通道，套利不能压平生效日冲击
5. **H5 涨跌停限制**——CN 公告日被涨停截断，需求"溢出"到生效日
6. **H6 权重变化可预测性**——CN 规则下权重更难预判，生效日重新定价

**输出**：`cma_hypothesis_map.csv`（带证据指针，给论文引用）。

M1–M5 的实证只**支持 / 不支持**各假设，不强行下结论——这是论文讨论章的抓手。

---

## 六、Dashboard 新 section

### 6.1 位置

新增独立 section：**"美股 vs A 股 公告—生效事件集中度差异"**

- 放在"制度识别与中国市场证据"主线**之后**
- 作为独立的跨市场对比块，不归入现有 3 主线

### 6.2 Mode 行为

| mode | 内容 |
|---|---|
| `brief` | 4 象限摘要表（4 行）+ 1 句结论 |
| `demo`（默认）| + 3 张主图（M1 CAR 路径 / M2 空窗期分解 / M3 机制 heatmap）|
| `full` | + 异质性矩阵（M4，显示所选 dim）+ 时序演变（M5）+ 结构假设对照表 |

### 6.3 代码触点

- `dashboard_content.py`：新增 section 的文案 keys（标题 / 副标题 / 摘要 / 结论）
- `dashboard_page_sections_runtime.py`：新增 `build_cross_market_section`
- `dashboard_sections.py`：注册 section
- `dashboard_home.py`：在 `DashboardHomeContextBuilder` 里接入
- `dashboard_figures.py`：新 PNG 暴露给 section
- `dashboard_metrics.py`：追加 4 象限的摘要指标
- `dashboard_presenters.py`：新 section 的 presenter 包装
- Jinja 模板：在 `home.html` 对应位置渲染新 section
- 静态资源：若需要独立卡片样式，追加到 `dashboard.css`

---

## 七、CLI 与 LaTeX 导出

### 7.1 新 CLI

- 入口名：**`index-inclusion-cma`**
- 源：`src/index_inclusion_research/cross_market_asymmetry.py::main()`
- 注册：`pyproject.toml` 的 `[project.scripts]` 新增一行
- 行为：
  1. 依赖 `real_event_panel.csv` 和 `real_matched_event_panel.csv`；缺则报错，**不自动回退 demo**（对齐 RDD L3 契约）
  2. 依次跑 M1 → M5，所有结果写入 `results/real_tables/` 和 `results/real_figures/`
  3. 输出导入汇总到 `results/literature/cma_import_summary.md`
  4. 关键 cells 追加到 `results/real_tables/research_summary.md` 的新章节 **"六、美股 vs A 股 不对称"**

### 7.2 LaTeX 与 figures_tables 集成

- 默认场景：`index-inclusion-make-figures-tables` 跑完标准表格后自动调 CMA 的 LaTeX 导出
- 新增独立开关：`index-inclusion-cma --tex-only`（跳过计算，只基于已有 CSV 重新生成 tex）

### 7.3 覆盖与拆分

- 所有新输出使用 `cma_` 前缀，不与 legacy `asymmetry_summary.csv` / `time_series_event_study_summary.csv` 冲突
- legacy 文件原地保留

---

## 八、测试策略

### 8.1 新增测试文件

```
tests/test_cma_paths.py
tests/test_cma_gap_period.py
tests/test_cma_mechanism_panel.py
tests/test_cma_heterogeneity.py
tests/test_cma_time_series.py
tests/test_cma_hypotheses.py
tests/test_cma_orchestrator.py        # 集成：demo 数据跑一遍全链
tests/test_dashboard_cma_section.py   # 新 section snapshot
```

### 8.2 每个单元测试的共同约束

- 构造 **deterministic toy data**（种子固定）
- 断言输出 schema（列名 / 类型）
- 断言关键数值（人工算出的 baseline，容差 ≤ 1e-6 或 ≤ 0.01%）
- 断言 SE / t 的合理性（符号方向、数量级）
- 边界：空输入、单市场输入、缺列输入都应该抛出明确的错误

### 8.3 现有测试的扩展

- `tests/test_dashboard_browser_smoke.py`：追加新 section 的 DOM 断言（标题、图片 src、表格行数）
- `tests/test_make_figures_tables.py`：确认 CMA 导出被触发、文件存在
- `tests/test_generate_research_report.py` 与 `tests/test_reporting.py`：确认新章节追加到 `research_summary.md`

### 8.4 验收命令

```bash
python3 -m ruff check .
pytest -q
RUN_BROWSER_SMOKE=1 pytest -q tests/test_dashboard_browser_smoke.py
```

---

## 九、风险与决策

1. **涨跌停命中率近似**：直接用 `|ret| ≥ 0.099`；不引入额外涨跌停状态源。若日后拿到官方涨跌停标志，替换函数内部即可
2. **ETF AUM 可选**：M5 的 AUM 叠加不作为阻断条件；缺数据时跳过叠加，图右轴不画，日志告警
3. **空窗期 CAR 对齐**：独立按 (announce_date, effective_date) 从 panel 抽取日度 ret，不依赖现有事件窗对齐
4. **异质性分箱在市场内做**：市值 / 流动性分位 market-internal，避免量级污染
5. **legacy 输出保留**：`asymmetry_summary.csv` / `time_series_event_study_summary.csv` 不删，并行存在
6. **承接 RDD 的"demo 不自动回退"契约**：CMA CLI 对真实数据缺失直接报错，不静默退化
7. **Dashboard cross_market section 是独立 section，不是主线**：避免改 `literature_catalog` 主干
8. **commit 边界**：CMA 功能自成一条 feature commit，不和当前 pending 的 dashboard 重构 / README 大改混一起
9. **事件样本范围（v1）**：仅 `event_type == "addition"`（纳入事件）；`deletion` 事件虽然也在 `real_event_panel.csv` 里，但留给 v2 做"剔除对称性"扩展。过滤在 `orchestrator.run_cma_pipeline` 入口统一做，各模块接到的 panel 已经是纯 addition

---

## 十、后续扩展（明确 out-of-scope for v1）

- 剔除事件（deletion）对称性分析：把 `event_type == "deletion"` 的样本接进 CMA，和 addition 做 4×2 = 8 象限对比
- 因子模型异常收益（FF3 / FF5 / Carhart）
- 跨市场扩展（FTSE100 / TOPIX / MSCI EM）
- Placebo / 证伪测试套件
- 机构持股面板（如果拿到 EDGAR 13F / CSMAR 持股数据）
- ETF 资金流入（作为需求冲击的直接代理）
- 结构变点重型检验（Quandt-Andrews sup-F）
- 双语 dashboard

---

## 十一、验收标准

v1 完成时应同时满足：

- [ ] `index-inclusion-cma` CLI 成功运行，所有表格与图写入预期路径，summary 文件追加新章节
- [ ] Dashboard 新 section 在 `brief / demo / full` 三种 mode 下都正确渲染
- [ ] 所有新增单元与集成测试在 `pytest -q` 下通过
- [ ] `python3 -m ruff check .` 通过
- [ ] `RUN_BROWSER_SMOKE=1 pytest -q tests/test_dashboard_browser_smoke.py` 通过
- [ ] `results/real_tables/research_summary.md` 的"六、美股 vs A 股 不对称"章节自动生成，含 4 象限 CAR + 空窗期 + 机制摘要
- [ ] LaTeX 导出在 `results/real_tables/cma_*.tex` 下生成，可直接插入论文
- [ ] 新 section 的 3 条核心结论（价格集中在哪、量能集中在哪、异质性在哪）能在 dashboard `brief` mode 3 分钟内讲完

---

## 十二、参考文件

- `src/index_inclusion_research/literature_catalog.py`
- `src/index_inclusion_research/analysis/` 已有子模块组织风格
- `src/index_inclusion_research/dashboard_*`（layered dashboard 架构）
- `docs/dashboard_architecture.md`
- `docs/literature_to_project_guide.md`
- `results/real_tables/research_summary.md`
