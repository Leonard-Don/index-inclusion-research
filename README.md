# Index Inclusion Research Toolkit

`index-inclusion-research` 是一个围绕“股票被纳入指数后为什么会上涨”这一问题搭建的实证研究项目。  
项目现在不再按“3 篇核心文献 + 后续补充”组织，而是以 `16 篇指数效应文献库` 为理论底座，统一抽象成 3 条研究主线：

- `短期价格压力与效应减弱`
- `需求曲线与长期保留`
- `制度识别与中国市场证据`

这三条主线分别对应你在文献中最关心的三个问题：

- 指数纳入后的上涨是不是只是短期交易冲击？
- 价格效应会不会只部分回吐，从而支持需求曲线向下倾斜？
- 不同市场制度和识别方法会不会改变结论，尤其是在中国市场？

## 你应该先看什么

如果你只是想快速进入项目，最推荐的顺序是：

1. 看 [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
   这里解释 16 篇文献如何统一映射到当前项目。
2. 启动界面：
   ```bash
   python3 scripts/start_literature_dashboard.py
   ```
   然后打开 <http://127.0.0.1:5001>
3. 在界面里先看首页 `/`：
    - `3 分钟汇报`：只保留开场结论、样本摘要、三条主线核心证据和研究边界，适合直接给教授讲
    - `展示版`：默认模式，适合课堂展示和研究讨论
    - `完整材料`：显示更多表格、图表和支撑材料，适合自己检查细节

如果你是要直接跑数据和结果，推荐从“命令行入口”一节开始。

## 项目结构

```text
config/
  markets.yml

data/
  raw/                 原始示例数据、真实数据、RDD 模板与显式 demo 数据
  processed/           清洗后的事件样本和事件窗口面板

docs/
  index_effect_literature_map.md
  literature_deep_analysis_cn.md
  literature_review_author_year_cn.md
  literature_to_project_guide.md
  paper_outline.md
  real_data_notes.md

results/
  event_study/         事件研究结果
  regressions/         回归与匹配诊断结果
  figures/             论文图表
  tables/              论文表格与结果摘要
  real_tables/         真实样本主结果表、数据来源表、样本范围表
  literature/          仪表盘三条主线对应的结果包

scripts/
  数据清洗、事件研究、回归、文献仪表盘入口脚本

src/index_inclusion_research/
  analysis/            事件研究、回归、RDD
  loaders/             数据读写
  pipeline/            样本构建与匹配
  literature.py        机制与汇总逻辑
  literature_catalog.py 16 篇文献目录与项目映射

tests/
  测试
```

## 16 篇文献驱动的三条主线

### 1. 短期价格压力与效应减弱

这条主线回答：

`指数纳入后的上涨是不是主要来自短期交易冲击？`

在项目里，它主要依赖：

- 短窗口 `CAR[-1,+1]`、`CAR[-3,+3]`、`CAR[-5,+5]`
- 公告日 / 生效日平均异常收益路径
- 成交量、换手率、波动率的短期变化

界面入口：

- 首页的 `短期价格压力与效应减弱`

研究主线入口：

- `scripts/start_price_pressure_track.py`

兼容旧入口：

- `scripts/start_harris_gurel.py`

### 2. 需求曲线与长期保留

这条主线回答：

`价格上涨会不会只部分回吐，从而支持需求曲线向下倾斜？`

在项目里，它主要依赖：

- 长窗口 `CAR[0,+20]`、`CAR[0,+60]`、`CAR[0,+120]`
- retention ratio
- short-window 和 long-window CAR 的对比

界面入口：

- 首页的 `需求曲线与长期保留`

研究主线入口：

- `scripts/start_demand_curve_track.py`

兼容旧入口：

- `scripts/start_shleifer.py`

### 3. 制度识别与中国市场证据

这条主线回答：

`指数效应的结论会不会因为制度背景和识别方法而改变？`

在项目里，它主要依赖：

- 中国样本事件研究
- 匹配对照组、DID 风格汇总
- RDD 扩展与分箱图

界面入口：

- 首页的 `制度识别与中国市场证据`

研究主线入口：

- `scripts/start_identification_china_track.py`

兼容旧入口：

- `scripts/start_hs300_style.py`
- `scripts/start_hs300_rdd.py`

其中 `RDD` 默认不再自动回退 demo。  
只有当 [data/raw/hs300_rdd_candidates.csv](data/raw/hs300_rdd_candidates.csv) 存在且通过校验时，`RDD` 才会进入正式证据链；如需开发演示，请显式运行：

```bash
python3 scripts/start_hs300_rdd.py --demo
```

正式字段模板见 [data/raw/hs300_rdd_candidates.template.csv](data/raw/hs300_rdd_candidates.template.csv)，数据契约见 [docs/hs300_rdd_data_contract.md](docs/hs300_rdd_data_contract.md)。

## 文献相关文件

这些文件现在是项目的“理论入口”：

- [docs/index_effect_literature_map.md](docs/index_effect_literature_map.md)
  16 篇文献的立场分类
- [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
  16 篇文献如何映射到三条研究主线
- [docs/literature_review_author_year_cn.md](docs/literature_review_author_year_cn.md)
  可直接放进论文的作者（年份）版中文文献综述
- [docs/literature_deep_analysis_cn.md](docs/literature_deep_analysis_cn.md)
  16 篇文献的深度分析，重点拆解每篇论文识别了什么、挑战了什么假设、对当前论文有什么用途
- [docs/literature_five_camps_framework_cn.md](docs/literature_five_camps_framework_cn.md)
  把 16 篇文献组织成五大阵营与会议表达框架
- [docs/index_inclusion_playbook_cn.md](docs/index_inclusion_playbook_cn.md)
  把事件时钟、机制链和冲击估算整理成投研补充层
- [src/index_inclusion_research/literature_catalog.py](src/index_inclusion_research/literature_catalog.py)
  项目内的结构化文献目录、五大阵营、项目映射与实战用法

## 数据输入契约

`events.csv` 必需列：

- `market`
- `index_name`
- `ticker`
- `announce_date`
- `effective_date`

可选列：

- `event_type`
- `source`
- `sector`
- `note`

`prices.csv` 必需列：

- `market`
- `ticker`
- `date`
- `close`
- `ret`
- `volume`
- `turnover`
- `mkt_cap`

可选列：

- `sector`

`benchmarks.csv` 必需列：

- `market`
- `date`
- `benchmark_ret`

## 命令行入口

### 1. 生成示例数据

```bash
python3 scripts/generate_sample_data.py
```

### 2. 下载真实公开数据

```bash
python3 scripts/download_real_data.py
```

真实数据说明见 [docs/real_data_notes.md](docs/real_data_notes.md)。

真实样本主结果目前会统一导出到 `results/real_tables/`，其中包括：

- `event_study_summary.csv`
- `long_window_event_study_summary.csv`
- `retention_summary.csv`
- `regression_coefficients.csv`
- `regression_models.csv`
- `data_sources.csv`
- `sample_scope.csv`
- `identification_scope.csv`

### 3. 清洗事件样本

```bash
python3 scripts/build_event_sample.py \
  --input data/raw/sample_events.csv \
  --output data/processed/events_clean.csv
```

### 4. 构建事件窗口面板

```bash
python3 scripts/build_price_panel.py \
  --events data/processed/events_clean.csv \
  --prices data/raw/sample_prices.csv \
  --benchmarks data/raw/sample_benchmarks.csv \
  --output data/processed/event_panel.csv
```

### 5. 运行事件研究

```bash
python3 scripts/run_event_study.py \
  --panel data/processed/event_panel.csv \
  --output-dir results/event_study
```

### 6. 构建匹配样本并回归

```bash
python3 scripts/match_controls.py \
  --events data/processed/events_clean.csv \
  --prices data/raw/sample_prices.csv \
  --output-events data/processed/matched_events.csv \
  --output-diagnostics results/regressions/match_diagnostics.csv

python3 scripts/build_price_panel.py \
  --events data/processed/matched_events.csv \
  --prices data/raw/sample_prices.csv \
  --benchmarks data/raw/sample_benchmarks.csv \
  --output data/processed/matched_event_panel.csv

python3 scripts/run_regressions.py \
  --panel data/processed/matched_event_panel.csv \
  --output-dir results/regressions
```

### 7. 导出论文图表和表格

```bash
python3 scripts/make_figures_tables.py
```

### 8. 自动生成论文结果摘要

```bash
python3 scripts/generate_research_report.py
```

### 9. 打开文献与结果仪表盘

```bash
python3 scripts/start_literature_dashboard.py
```

这就是当前项目唯一推荐的前端启动方式。
首页默认进入 `展示版`，更适合汇报和展示；需要更短的口头汇报可切到 `3 分钟汇报`，需要更多表格时可切到 `完整材料`。

打开后常用入口：

- `/`：一页式总展板，包含主线结果、文献框架、机制补充和研究边界
- `/?mode=brief`：3 分钟汇报模式
- `/?mode=demo`：展示版
- `/?mode=full`：完整材料
- `/paper/<paper_id>`：单篇文献讲义页
- `/paper/<paper_id>/pdf`：打开对应原文 PDF

历史副页入口已经并入首页：

- `/library`、`/review`、`/framework` 会重定向到 `/#framework`
- `/supplement` 会重定向到 `/#supplement`
- `/analysis/<analysis_id>` 会重定向到首页对应研究主线锚点

### 10. 直接运行三条研究主线

```bash
python3 scripts/start_price_pressure_track.py
python3 scripts/start_demand_curve_track.py
python3 scripts/start_identification_china_track.py
```

## 哪些文件是“核心文件”

如果你时间不多，优先看这些：

- [README.md](README.md)
- [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
- [docs/literature_review_author_year_cn.md](docs/literature_review_author_year_cn.md)
- [scripts/start_literature_dashboard.py](scripts/start_literature_dashboard.py)
- [src/index_inclusion_research/literature_catalog.py](src/index_inclusion_research/literature_catalog.py)
- [results/real_tables/research_summary.md](results/real_tables/research_summary.md)

## 哪些文件主要是生成产物

下面这些目录里的多数文件都可以重新生成：

- `data/processed/`
- `results/event_study/`
- `results/regressions/`
- `results/figures/`
- `results/tables/`
- `results/literature/`

所以平时真正需要维护的“源文件”主要还是：

- `scripts/`
- `src/index_inclusion_research/`
- `docs/`
- `config/markets.yml`

## 论文写作建议

论文模板见 [docs/paper_outline.md](docs/paper_outline.md)。

最推荐的写法是：

1. 文献综述按 `反方 / 中性 / 正方` 展开
2. 实证设计按三条研究主线展开
3. 结果部分按 `短期冲击 -> 长期保留 -> 中国市场识别扩展` 展开

## 测试

运行：

```bash
pytest -q
```

当前项目包含：

- 事件研究与机制汇总测试
- RDD 测试
- 文献目录与主线映射测试
- 报表与页面相关测试

## 备注

如果你接下来继续做清理，最值得优先保持稳定的是：

- `src/index_inclusion_research/literature_catalog.py`
- `scripts/start_literature_dashboard.py`
- `docs/literature_to_project_guide.md`

因为这三处现在定义了整个项目的统一主线。
