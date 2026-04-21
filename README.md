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
2. 如果你要继续维护 dashboard 主干，先看：
   [docs/dashboard_architecture.md](docs/dashboard_architecture.md)
   和 [docs/dashboard_commit_boundary.md](docs/dashboard_commit_boundary.md)
3. 启动界面：
   ```bash
   index-inclusion-dashboard
   ```
   然后打开 <http://localhost:5001>
4. 在界面里先看首页 `/`：
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
  历史兼容脚本与本地 bootstrap 薄 wrapper

src/index_inclusion_research/
  analysis/            事件研究、回归、RDD
  loaders/             数据读写
  pipeline/            样本构建与匹配
  literature.py        机制与汇总逻辑
  literature_catalog.py 16 篇文献目录与项目映射

tests/
  测试
```

## 推荐入口与兼容层

当前推荐的运行面只有两类：

- 已安装项目时：优先使用 `index-inclusion-*` 这组 CLI。
- 未安装 console script 时：优先使用 `python3 -m index_inclusion_research.<module>`。

`scripts/*.py` 现在只保留历史兼容意义：

- 方便旧命令、旧笔记和已有本地工作流继续运行。
- 本身不再承载主要业务实现，默认不应把它们当成首选入口。

下面正文默认只写当前推荐入口；历史兼容脚本统一放到文末附录。

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

- `index-inclusion-price-pressure`

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

- `index-inclusion-demand-curve`

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

- `index-inclusion-identification`

其中 `RDD` 默认不再自动回退 demo。  
当 [data/raw/hs300_rdd_candidates.csv](data/raw/hs300_rdd_candidates.csv) 存在且通过校验时，`RDD` 会进入 `L3` 正式边界样本；当 [data/raw/hs300_rdd_candidates.reconstructed.csv](data/raw/hs300_rdd_candidates.reconstructed.csv) 存在且通过校验时，`RDD` 会进入 `L2` 公开重建样本。如需开发演示，请显式运行：

```bash
index-inclusion-hs300-rdd --demo
```

正式字段模板见 [data/raw/hs300_rdd_candidates.template.csv](data/raw/hs300_rdd_candidates.template.csv)，数据契约见 [docs/hs300_rdd_data_contract.md](docs/hs300_rdd_data_contract.md)。
如果你拿到的是原始候选名单表（CSV / Excel，列名不一定标准），推荐先运行：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.xlsx \
  --sheet 0 \
  --announce-date 2024-11-29 \
  --effective-date 2024-12-16 \
  --source CSIndex \
  --source-url https://www.csindex.com.cn/
```

它会先把原始列名规范化成项目要求的字段，再输出标准候选文件、批次审计表和导入摘要；如果只想先验收而不落盘，可以加 `--check-only`。

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
index-inclusion-generate-sample-data
```

如果你还没有安装 console script，也可以直接运行模块：

```bash
python3 -m index_inclusion_research.sample_data
```

### 2. 下载真实公开数据

```bash
index-inclusion-download-real-data
```

如果你还没有安装 console script，也可以直接运行模块：

```bash
python3 -m index_inclusion_research.real_data
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

### HS300 RDD 候选样本导入

当你拿到沪深 300 调样候选名单后，推荐先用导入脚本做标准化和校验：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.csv \
  --check-only
```

如果你还没有安装 console script，也可以直接运行模块：

```bash
python3 -m index_inclusion_research.prepare_hs300_rdd_candidates \
  --input /path/to/raw_candidates.csv \
  --check-only
```

确认通过后，再写入正式候选文件：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.csv \
  --output data/raw/hs300_rdd_candidates.csv \
  --force
```

脚本默认会补 `market=CN`、`index_name=CSI300`、`cutoff=300`，并在 `results/literature/hs300_rdd_import/` 下生成：

- `candidate_batch_audit.csv`
- `import_summary.md`

如果你手里没有官方候选排名表，但希望先基于公开口径重建一版边界样本，也可以运行：

```bash
index-inclusion-reconstruct-hs300-rdd \
  --all-batches \
  --output data/raw/hs300_rdd_candidates.reconstructed.csv \
  --force
```

这条路径会用当前 CSI300 成分股、后续真实调样批次回滚、以及公开价格/总股本代理口径，优先重建当前事件源里“可以稳定回滚”的连续批次后缀；如果只想做单批次，也可以改用 `--announce-date 2024-05-31`。它适合课程论文、方法复现和公开数据版本的稳健性补充，但不应表述为中证官方历史候选排名表。

下面这些命令都推荐优先使用包内 CLI。它们和脚本入口一样都支持 `--profile auto|sample|real`，默认会自动优先走 real 工作流；如果你想显式生成 sample 版本，可以在相应命令后加 `--profile sample`。

### 3. 清洗事件样本

```bash
index-inclusion-build-event-sample
```

### 4. 构建事件窗口面板

```bash
index-inclusion-build-price-panel
```

### 5. 运行事件研究

```bash
index-inclusion-run-event-study
```

### 6. 构建匹配样本并回归

```bash
index-inclusion-match-controls

index-inclusion-build-price-panel \
  --events data/processed/real_matched_events.csv \
  --output data/processed/real_matched_event_panel.csv

index-inclusion-run-regressions
```

### 7. 导出论文图表和表格

```bash
index-inclusion-make-figures-tables
```

这条命令现在会默认自动识别当前工作流：如果仓库里已经存在 `real_*` 数据与结果文件，就优先刷新 `results/real_figures/` 和 `results/real_tables/`，并把当前 `hs300_rdd` 状态一并写入 `identification_scope.csv`。如果你想显式回到旧的 sample 路径，可以改用：

```bash
index-inclusion-make-figures-tables --profile sample
```

如果你还没有安装 console script，也可以直接运行模块：

```bash
python3 -m index_inclusion_research.figures_tables
```

### 8. 自动生成论文结果摘要

```bash
index-inclusion-generate-research-report
```

它也会沿用同样的自动识别逻辑：默认优先读取 `results/real_event_study/` 与 `results/real_regressions/`，并把摘要写到 `results/real_tables/research_summary.md`。如果你要显式生成 sample 版本，可以改用：

```bash
index-inclusion-generate-research-report --profile sample
```

如果你还没有安装 console script，也可以直接运行模块：

```bash
python3 -m index_inclusion_research.research_report
```

### 9. 打开文献与结果仪表盘

```bash
index-inclusion-dashboard
```

如果你还没有安装 console script，也可以直接运行模块：

```bash
python3 -m index_inclusion_research.literature_dashboard
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

如果你需要避免占用默认端口，也可以显式指定：

```bash
index-inclusion-dashboard --port 5002
```

安装后的 CLI 入口目前包括：

```bash
index-inclusion-build-event-sample
index-inclusion-build-price-panel
index-inclusion-match-controls
index-inclusion-run-event-study
index-inclusion-run-regressions
index-inclusion-generate-sample-data
index-inclusion-download-real-data
index-inclusion-make-figures-tables
index-inclusion-generate-research-report
index-inclusion-dashboard
index-inclusion-price-pressure
index-inclusion-demand-curve
index-inclusion-identification
index-inclusion-hs300-rdd
index-inclusion-prepare-hs300-rdd
index-inclusion-reconstruct-hs300-rdd
```

前五个分别覆盖清洗事件、构面板、匹配对照、事件研究和回归；接着四个分别覆盖示例数据、真实数据、图表表格导出和研究摘要；中间四个对应 dashboard 与三条研究主线；最后三个分别对应 HS300 RDD 运行、候选样本导入和公开口径重建。默认调用面应该是 CLI；`scripts/` 只保留历史兼容。

## 历史兼容脚本附录

只有在你需要复用旧命令、旧笔记或已有本地脚本时，才建议直接碰这些文件：

- `index-inclusion-dashboard` 对应 `scripts/start_literature_dashboard.py`
- `index-inclusion-price-pressure` 对应 `scripts/start_harris_gurel.py`
- `index-inclusion-demand-curve` 对应 `scripts/start_shleifer.py`
- `index-inclusion-identification` 对应 `scripts/start_hs300_style.py` 与 `scripts/start_hs300_rdd.py`
- `index-inclusion-build-event-sample` 对应 `scripts/build_event_sample.py`
- `index-inclusion-build-price-panel` 对应 `scripts/build_price_panel.py`
- `index-inclusion-match-controls` 对应 `scripts/match_controls.py`
- `index-inclusion-run-event-study` 对应 `scripts/run_event_study.py`
- `index-inclusion-run-regressions` 对应 `scripts/run_regressions.py`
- `index-inclusion-generate-sample-data` 对应 `scripts/generate_sample_data.py`
- `index-inclusion-download-real-data` 对应 `scripts/download_real_data.py`
- `index-inclusion-make-figures-tables` 对应 `scripts/make_figures_tables.py`
- `index-inclusion-generate-research-report` 对应 `scripts/generate_research_report.py`
- `index-inclusion-prepare-hs300-rdd` 对应 `scripts/prepare_hs300_rdd_candidates.py`
- `index-inclusion-reconstruct-hs300-rdd` 对应 `scripts/reconstruct_hs300_rdd_candidates.py`

## 开发与验证

如果你要继续开发这个项目，推荐先装上开发依赖：

```bash
python3 -m pip install -e ".[dev]"
```

日常回归：

```bash
python3 -m ruff check .
pytest -q
```

浏览器 smoke test 默认不会在本地 `pytest` 里自动跑；需要时可以显式执行：

```bash
python3 -m playwright install chromium
RUN_BROWSER_SMOKE=1 pytest -q tests/test_dashboard_browser_smoke.py
```

仓库里的 GitHub Actions 也会按这个思路分成两步：

- `ruff` lint + 常规单元测试
- 安装 Chromium 后再跑 dashboard 浏览器 smoke test

如果你准备继续改 dashboard 主干，推荐先看：

- [docs/dashboard_architecture.md](docs/dashboard_architecture.md)
- [docs/dashboard_commit_boundary.md](docs/dashboard_commit_boundary.md)

### 10. 直接运行三条研究主线

```bash
index-inclusion-price-pressure
index-inclusion-demand-curve
index-inclusion-identification
```

## 哪些文件是“核心文件”

如果你时间不多，优先看这些：

- [README.md](README.md)
- [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
- [docs/dashboard_architecture.md](docs/dashboard_architecture.md)
- [docs/dashboard_commit_boundary.md](docs/dashboard_commit_boundary.md)
- [docs/literature_review_author_year_cn.md](docs/literature_review_author_year_cn.md)
- [src/index_inclusion_research/literature_dashboard.py](src/index_inclusion_research/literature_dashboard.py)
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

- `src/index_inclusion_research/`
- `docs/`
- `config/markets.yml`

`scripts/` 仍然值得保留，但更适合作为兼容层检查对象，而不是默认开发入口。

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
- `src/index_inclusion_research/literature_dashboard.py`
- `docs/literature_to_project_guide.md`

因为这三处现在定义了整个项目的统一主线。
