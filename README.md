# Index Inclusion Research Toolkit

`index-inclusion-research` 是一个为“股票被纳入指数后为什么会上涨”这类论文准备的 Python 实证分析骨架。它默认支持 A 股和美股的跨市场比较，围绕三类机制组织代码：

- 被动资金需求冲击
- 流动性 / 关注度提升
- 信息背书效应

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

## 论文写作对应关系

- `run_event_study.py`：验证公告日/生效日前后的超额收益路径
- `match_controls.py`：生成课程论文够用的 matched sample
- `run_regressions.py`：将指数纳入变量与 CAR、换手率、成交量、波动率变化联结起来
- `make_figures_tables.py`：导出可直接嵌入论文的图和表
- `generate_research_report.py`：把结果自动整理成中文 Markdown 摘要，便于写论文

## 论文辅助写作

- 论文写作模板见 [docs/paper_outline.md](docs/paper_outline.md)
- 自动生成的结果摘要默认输出到 `results/tables/research_summary.md`
- 推荐流程：
- 先用真实数据替换 `sample_*`
- 跑完整条管线
- 打开 `results/tables/research_summary.md`
- 再把其中的结论句式改成你的论文语言

真实数据说明见 [docs/real_data_notes.md](docs/real_data_notes.md)。

## 扩展建议

- 将 `benchmark_ret` 换成 CAPM 或 Fama-French 因子收益
- 在 `config/markets.yml` 中加入更细的市场日历和指数配置
- 把示例 `sample_*` 数据替换成你自己的真实事件和价格数据
