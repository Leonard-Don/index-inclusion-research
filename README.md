# Index Inclusion Research Toolkit

`index-inclusion-research` 是一个以 `16 篇指数效应文献库` 为理论底座、为“股票被纳入指数后为什么会上涨”这类论文准备的 Python 实证分析项目。它默认支持 A 股和美股的跨市场比较，并把整套项目统一组织成三条研究主线：

- `短期价格压力与效应减弱`
- `需求曲线与长期保留`
- `制度识别与中国市场证据`

项目从原始事件表和价格表出发，生成事件窗口面板、事件研究结果、匹配对照样本、回归结果以及论文可直接引用的图表和表格。

## 目录

```text
config/markets.yml
data/raw/
data/processed/
results/event_study/
results/regressions/
results/figures/
results/tables/
scripts/
src/index_inclusion_research/
tests/
```

## 输入数据契约

`events.csv` 必含列：

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

`prices.csv` 必含列：

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

`benchmarks.csv` 必含列：

- `market`
- `date`
- `benchmark_ret`

## 快速开始

1. 生成一套可运行的示例数据：

```bash
python3 scripts/generate_sample_data.py
```

如果你想直接切到真实公开数据：

```bash
python3 scripts/download_real_data.py
```

2. 清洗并标准化指数纳入事件：

```bash
python3 scripts/build_event_sample.py \
  --input data/raw/sample_events.csv \
  --output data/processed/events_clean.csv
```

3. 构建事件窗口面板并运行事件研究：

```bash
python3 scripts/build_price_panel.py \
  --events data/processed/events_clean.csv \
  --prices data/raw/sample_prices.csv \
  --benchmarks data/raw/sample_benchmarks.csv \
  --output data/processed/event_panel.csv

python3 scripts/run_event_study.py \
  --panel data/processed/event_panel.csv \
  --output-dir results/event_study
```

4. 构建匹配对照样本并跑回归：

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

5. 导出图表和论文表格：

```bash
python3 scripts/make_figures_tables.py
```

6. 自动生成一份论文结果摘要：

```bash
python3 scripts/generate_research_report.py
```

7. 启动文献结果界面：

```bash
python3 scripts/start_literature_dashboard.py
```

然后打开 <http://127.0.0.1:5001>。

界面中现在的 3 个分析入口，不再对应“3 篇核心论文”，而是对应从 16 篇文献中抽出来的 3 条研究主线。
同时还内置了：
- `16 篇文献库` 页面：查看反方、中性、正方文献的项目映射，并从页面打开 PDF
- `文献综述` 页面：按反方、中性、正方三组分别展示文献，方便直接写综述

## 16 篇文献驱动的项目结构

- `短期价格压力与效应减弱`
  使用短窗口事件研究结果，重点对应 Harris and Gurel、Kasch and Sarkar、Greenwood and Sammon 等文献
- `需求曲线与长期保留`
  使用长窗口 CAR 和 retention 指标，重点对应 Shleifer、Kaul et al.、Wurgler and Zhuravskaya 等文献
- `制度识别与中国市场证据`
  使用匹配对照组、DID 风格分析和 RDD 扩展，重点对应 Ahn and Patatoukas、Chang et al.、姚东旻等、Chu et al. 等文献

## 核心脚本对应关系

- `run_event_study.py`：验证公告日/生效日前后的超额收益路径
- `match_controls.py`：生成课程论文够用的 matched sample
- `run_regressions.py`：将指数纳入变量与 CAR、换手率、成交量、波动率变化联结起来
- `make_figures_tables.py`：导出可直接嵌入论文的图和表
- `generate_research_report.py`：把结果自动整理成中文 Markdown 摘要，便于写论文

## 文献与写作

- 论文写作模板见 [docs/paper_outline.md](docs/paper_outline.md)
- 自动生成的结果摘要默认输出到 `results/tables/research_summary.md`
- 推荐流程：
- 先用真实数据替换 `sample_*`
- 跑完整条管线
- 打开 `results/tables/research_summary.md`
- 再把其中的结论句式改成你的论文语言

真实数据说明见 [docs/real_data_notes.md](docs/real_data_notes.md)。
- 16 篇文献分类见 [docs/index_effect_literature_map.md](docs/index_effect_literature_map.md)
- 16 篇文献到项目主线的使用说明见 [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
- 文献综述初稿见 [docs/literature_review_draft_cn.md](docs/literature_review_draft_cn.md)
- 作者（年份）版综述见 [docs/literature_review_author_year_cn.md](docs/literature_review_author_year_cn.md)

## 扩展建议

- 将 `benchmark_ret` 换成 CAPM 或 Fama-French 因子收益
- 在 `config/markets.yml` 中加入更细的市场日历和指数配置
- 把示例 `sample_*` 数据替换成你自己的真实事件和价格数据
