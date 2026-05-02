# Index Inclusion Research Toolkit

[![CI](https://github.com/Leonard-Don/index-inclusion-research/actions/workflows/ci.yml/badge.svg)](https://github.com/Leonard-Don/index-inclusion-research/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-3776AB)
![Research](https://img.shields.io/badge/focus-index%20inclusion%20research-1f6feb)

`index-inclusion-research` 是一个把指数纳入效应文献、真实样本结果与识别设计放到同一工作流里的实证研究项目。它把 `16 篇指数效应文献库`、3 条研究主线、真实样本表和 HS300 RDD 扩展统一到同一个 dashboard 与 CLI 体系里，适合做：

- 指数纳入效应相关论文的文献综述与研究展示
- 事件研究、匹配回归与中国市场识别的实证复现
- 面向课堂汇报、导师讨论和项目维护的同一套界面输出

项目不再按“3 篇核心文献 + 后续补充”组织，而是围绕 3 条研究主线展开：

- `短期价格压力与效应减弱`
- `需求曲线与长期保留`
- `制度识别与中国市场证据`

这三条主线分别回答 3 个核心问题：

- 指数纳入后的上涨是不是只是短期交易冲击？
- 价格效应会不会只部分回吐，从而支持需求曲线向下倾斜？
- 不同市场制度和识别方法会不会改变结论，尤其是在中国市场？

## 研究当前结论速览

跨市场不对称(CMA)pipeline 在真实样本上的 7 条机制假说裁决(`index-inclusion-verdict-summary` 也能终端打印):

| 假说 | 名称 | 裁决 | 头条指标 | 主线 |
|---|---|---|---|---|
| H1 | 信息泄露与预运行 | 证据不足 | bootstrap p = 0.640 (n=436) | 制度识别 |
| H2 | 被动基金 AUM 差异 | 待补数据 | — | 需求曲线 |
| H3 | 散户 vs 机构结构 | 部分支持 | 双通道命中率 = 0.500 | 短期价格压力 |
| H4 | 卖空约束 | 证据不足 | regression p = 0.537 (n=436) | 制度识别 |
| H5 | 涨跌停限制 | 证据不足 | limit_coef p = 0.213 (n=936) | 制度识别 |
| H6 | 指数权重可预测性 | 部分支持 | Q1Q2−Q4Q5 spread = 1.17 (n=118) | 需求曲线 |
| H7 | 行业结构差异 | 部分支持 | US sector spread = 5.95 (n=187) | 制度识别 |

数据可重现:`make rebuild` 跑 10 步流水线刷新所有产出。详见 [results/real_tables/cma_hypothesis_verdicts.csv](results/real_tables/cma_hypothesis_verdicts.csv) 和 [docs/paper_outline_verdicts.md](docs/paper_outline_verdicts.md)。

想知道 "如果阈值是 0.05 而不是 0.10,结论会怎样?" — 项目把这个问题做成了**五层入口**(决定 / 数据 / CLI / dashboard / doctor),终端一行看 sweep:`index-inclusion-verdict-summary --sensitivity` ,详见 [#p-阈值灵敏度分析](#p-阈值灵敏度分析)。

## GitHub 首页先看什么

如果你是第一次点进这个仓库，建议先看这 4 件事：

1. 看下面的”界面预览”，先知道项目最终交付长什么样。
2. 看”快速开始”，在本地把 dashboard 拉起来。
3. 看 [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)，理解 16 篇文献如何映射到当前项目。
4. 如果你要继续维护 dashboard 主干，再看 [docs/dashboard_architecture.md](docs/dashboard_architecture.md)。

## 界面预览

<table>
  <tr>
    <td><strong>首页总览</strong></td>
    <td><strong>单篇文献速读</strong></td>
    <td><strong>移动端阅读</strong></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/dashboard-home.png" alt="Dashboard homepage" width="100%"></td>
    <td><img src="docs/screenshots/paper-brief.png" alt="Paper brief page" width="100%"></td>
    <td><img src="docs/screenshots/dashboard-mobile.png" alt="Dashboard mobile view" width="100%"></td>
  </tr>
</table>

当前仓库没有公开在线 demo，推荐直接在本地运行并打开 `http://localhost:5001`。

## 快速开始

### 1. 安装

```bash
python3 -m pip install -e ".[dev]"
```

### 2. 启动 dashboard

```bash
index-inclusion-dashboard
```

然后打开 <http://localhost:5001>

### 3. 先看哪些页面

- `/`：一页式总展板，包含文献脉络、样本结果、机制补充和研究边界
- `/?mode=brief`：3 分钟汇报模式
- `/?mode=demo`：展示版
- `/?mode=full`：完整材料
- `/paper/<paper_id>`：单篇文献速读页
- `/paper/<paper_id>/pdf`：对应原文 PDF

### 4. 常用验证

```bash
python3 -m ruff check .
pytest -q
RUN_BROWSER_SMOKE=1 pytest -q tests/test_dashboard_browser_smoke.py
```

## 维护与扩展前先看什么

如果你已经准备继续维护或扩展这个项目，建议按这个顺序看：

1. 看 [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
   这里解释 16 篇文献如何统一映射到当前项目。
2. 如果你要维护 dashboard 主干，再看：
   [docs/dashboard_architecture.md](docs/dashboard_architecture.md)
3. 启动界面：
   ```bash
   index-inclusion-dashboard
   ```
   然后打开 <http://localhost:5001>
4. 在界面里先看首页 `/`：
    - `3 分钟汇报`：只保留结论、样本范围和研究边界，适合快速汇报
    - `展示版`：默认模式，适合课堂展示和研究讨论
    - `完整材料`：补全表格、图表和支撑材料，适合核对细节

如果你是要直接跑数据和结果，可以直接跳到“命令行入口”一节。

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

src/index_inclusion_research/
  analysis/            事件研究、回归、RDD
  loaders/             数据读写
  pipeline/            样本构建与匹配
  web/
    templates/         Flask Jinja 模板
    static/            CSS / JS / 图标
  literature.py        机制与汇总逻辑
  literature_catalog.py 16 篇文献目录与项目映射

tests/
  测试
```

## 推荐入口

当前推荐的运行面只有两类：

- 已安装项目时：优先使用 `index-inclusion-*` 这组 CLI。
- 未安装 console script 时：优先使用 `python3 -m index_inclusion_research.<module>`。

模板与静态资源位于 `src/index_inclusion_research/web/`（通过 `setuptools.package-data` 一并打包）。

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

如果你已经准备去采集 L3 正式候选样本（中证官方 / 公告附件 / 人工摘录），可以先生成一份采集包,把每个批次需要哪些字段、用哪条命令验收和写入都列清楚:

```bash
index-inclusion-plan-hs300-rdd-l3 --force
```

它默认读 `data/raw/hs300_rdd_candidates.reconstructed.csv` 作为参考批次,在 `results/literature/hs300_rdd_l3_collection/` 下生成:

- `batch_collection_checklist.csv`：每批次的 `acceptance_command` / `write_command` / `refresh_command` 三条建议命令
- `formal_candidate_template.csv`：可以直接补字段写正式候选名单的模板
- `boundary_reference.csv`：每批次 cutoff 附近的参考排名快照
- `collection_plan.md`：人类可读的采集计划摘要

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
首页默认进入 `展示版`。需要快速汇报时切到 `3 分钟汇报`；需要核对更多表格时切到 `完整材料`。

打开后常用入口：

- `/`：一页式总展板，包含主线结果、文献框架、机制补充和研究边界
- `/?mode=brief`：3 分钟汇报模式
- `/?mode=demo`：展示版
- `/?mode=full`：完整材料
- `/paper/<paper_id>`：单篇文献速读页
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
index-inclusion-plan-hs300-rdd-l3
index-inclusion-prepare-passive-aum
index-inclusion-compute-h6-weight-change
index-inclusion-cma
index-inclusion-rebuild-all
index-inclusion-verdict-summary
```

按用途分组(共 23 个 console scripts):
- **数据流水线**:`build-event-sample` / `build-price-panel` / `match-controls` / `run-event-study` / `run-regressions`
- **样本数据**:`generate-sample-data` / `download-real-data`
- **报表与图表**:`make-figures-tables` / `generate-research-report`
- **Dashboard 与三条主线**:`dashboard` / `price-pressure` / `demand-curve` / `identification`
- **HS300 RDD 工具链**:`hs300-rdd` / `prepare-hs300-rdd` / `reconstruct-hs300-rdd` / `plan-hs300-rdd-l3`
- **跨市场不对称 + 假说证据**:`cma`(7 条假说 verdict)/ `prepare-passive-aum`(为 H2 准备 AUM 数据)/ `compute-h6-weight-change`(为 H6 重建真实流通市值)
- **总入口**:`rebuild-all`(10 步流水线一键跑)/ `verdict-summary`(终端速览 7 条假说裁决)/ `doctor`(项目健康检查)

所有入口均通过 `pyproject.toml` 的 console scripts 或 `python3 -m index_inclusion_research.<module>` 调用,也可以用 `make rebuild` / `make verdicts` / `make doctor` 简写。

### Verdicts ↔ Literature 双向链接

每条 H1..H7 假说在 [hypotheses.py](src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py) 注册时同时声明 `paper_ids`,从 16 篇文献库挑出"支撑这条假说"的论文。系统的两端都消费这个映射:

- **Dashboard verdict 卡片** 在 metric 下方显示"支持文献 (N) [paper_id_chip] [paper_id_chip]...",每个 chip 链到 `/paper/<paper_id>`。
- **`/paper/<paper_id>` 论文详情页** 渲染"CMA 假说证据"段,列出所有引用本论文的假说 + 其当前 verdict tier + 头条指标,每行末尾"看 dashboard 裁决 →"链到 `/verdict/<hid>`。
- **`/verdict/<hid>`**(浏览器可分享的稳定 URL)302-redirect 到 `/?mode=full#hypothesis-<hid>`,直接定位到对应 verdict 卡片。`/verdict/H99` 等 typo 返回 404。

整个 verdict↔literature 网络可深链、可双向跳转、可分享。

### Verdict 迭代追踪

每跑一次 `index-inclusion-cma`,orchestrator 会自动把上一次的 verdicts 复制到 `results/real_tables/cma_hypothesis_verdicts.previous.csv`,所以补 H2 AUM / 跑 H6 weight_change / 加新批次以后能直接 diff:

```bash
# 看哪些 verdict 翻转 / key_value 漂了多少
index-inclusion-verdict-summary --compare-with results/real_tables/cma_hypothesis_verdicts.previous.csv

# 想自己保留时点快照(例如发版前)
index-inclusion-verdict-summary --snapshot snapshots/before-aum-data.csv
# ...再跑 CMA / 改数据...
index-inclusion-verdict-summary --compare-with snapshots/before-aum-data.csv

# 机器可读输出(CI / 后续工具消费)
index-inclusion-verdict-summary --format json | jq '.aggregate'
```

diff 输出形如:
```
VERDICT DIFF · 当前 vs 快照
  changed: 1, added: 0, removed: 0, unchanged: 6

已变更:
  H1 · 信息泄露与预运行
    verdict        : 证据不足  →  支持
    key_value      : 0.640  →  0.012  (Δ -0.628)
```

### p 阈值灵敏度分析

H1 / H4 / H5 的 verdict 由单一 p 决定(分别是 bootstrap p / regression p / limit_coef p),H2 / H3 / H6 / H7 的头条指标是 spread / 命中率 / AUM 比率,不在 p 阈值 sweep 范围内。审稿人最爱的追问 "如果阈值是 0.05 而不是 0.10,结论会怎样?" — 项目把回答这个问题的能力做成**五层入口**,从重新生成 verdict 到机器可读到点鼠标都覆盖:

**0. 决定层** — `index-inclusion-cma --threshold 0.05` 让 verdict 字段(`支持` / `部分支持` / `证据不足`)本身在自定义阈值下重新生成。语义:`THRESHOLD` 是边界 p,inner cutoff 是 `THRESHOLD/2`(支持/高 confidence),outer 是 `THRESHOLD`(部分支持/中)。默认 0.10 与历史行为字节兼容(inner 0.05 / outer 0.10)。下面四层(数据 / CLI sweep / GUI / CI)都基于这一次跑出的 CSV。

**1. 数据层(读)** — `cma_hypothesis_verdicts.csv` 自带 `p_value` 列,H1/H4/H5 填 boot/reg/limit p,其他四个为 NaN。下游可以直接 pandas:

```python
import pandas as pd
df = pd.read_csv("results/real_tables/cma_hypothesis_verdicts.csv")
df.loc[df["p_value"].notna() & (df["p_value"] < 0.05), ["hid", "name_cn", "p_value"]]
```

**2. CLI 层** — `index-inclusion-verdict-summary --sensitivity`,默认阈值 (0.05, 0.10, 0.15),也可自定义:

```bash
# 默认三阈值
index-inclusion-verdict-summary --sensitivity

# 自定义阈值(自动去重 + 排序)
index-inclusion-verdict-summary --sensitivity 0.01 0.05 0.10 0.15 0.20

# JSON 输出供 CI / 下游脚本消费
index-inclusion-verdict-summary --format json --sensitivity 0.05 0.10 | jq '.sensitivity'
```

终端表格示意:

```
 假说 verdict p 值灵敏度(3 阈值)
  hid    p_value  p<0.05   p<0.1  p<0.15
  ──────────────────────────────────────
  H1      0.6396       —       —       —
  H4      0.5366       —       —       —
  H5      0.2134       —       —       —
  p<0.05: 0/3 显著 · p<0.1: 0/3 显著 · p<0.15: 0/3 显著
  注:H2 H3 H6 H7 头条指标不是 p,不在 sweep 范围内。
```

**3. GUI 层** — dashboard CMA section 的 verdict 网格上方有 5 个阈值 chip(0.01 / 0.05 / 0.10 / 0.15 / 0.20),默认 0.10 active。点击其他 chip,H1/H4/H5 卡片底部的 sensitivity strip 实时翻"在 p<X 下显著(p=...)"或"在 p<X 下不显著(p=...)";non-p 卡片始终标"头条指标不是 p,不在 sweep 范围"。给 advisor / 同事演示时直接点鼠标即可。

**4. CI 层** — `index-inclusion-doctor` 第 4 项 `p_gated_verdict_sensitivity` 自动 flag 处于 [0.05, 0.10) 边缘区间的假说(default 显著但 strict 翻 not_sig — 审稿人会追问 robustness 的典型情形):

```text
✓  p_gated_verdict_sensitivity
    3 p-gated hypotheses; 0 significant at strict (0.05), 0 at default (0.1);
    none sit in the [0.05, 0.1) boundary.
```

如果出现 boundary 项,doctor 会变 warn 并在 details 列出 hid + p,fix 建议指向 `verdict-summary --sensitivity` 同时看双阈值。默认 GitHub Actions 会运行 `index-inclusion-doctor --format json` 并展示这些 warning，但不会因为 warning fail(语义是 "research robustness signal" 而非 "project broken")；本地 `make doctor` 也会显示同一组信号。

### Doctor 严格门禁与机器可读输出

`index-inclusion-doctor` 默认只在 `fail` 时返回非零退出码；`warn` 用来标记研究边界或数据缺口，例如 H2 AUM 待补、HS300 RDD 仍处在公开重建 L2 样本。常规 CI 使用默认模式，让 warning 保持可见但不阻断；需要让 warning 也阻断发布门禁或严格 CI 时，可以显式开启严格模式：

```bash
index-inclusion-doctor --fail-on-warn
```

需要给后续脚本或 CI 注释机器人消费时，使用 JSON 输出：

```bash
index-inclusion-doctor --format json | jq '.summary'
index-inclusion-doctor --format json --fail-on-warn
```

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

如果你准备继续改 dashboard 主干，先看：

- [docs/dashboard_architecture.md](docs/dashboard_architecture.md)

### 10. 直接运行三条研究主线

```bash
index-inclusion-price-pressure
index-inclusion-demand-curve
index-inclusion-identification
```

### 11. 跨市场不对称（CMA）扩展

`index-inclusion-cma` 在 CN / US × announce / effective 四象限上系统化对比事件集中度差异（M1 事件窗口路径 / M2 公告—生效空窗期 / M3 机制回归 / M4 异质性矩阵 / M5 时序演变 + 结构假设表）。

```bash
index-inclusion-cma
```

它依赖真实样本（`real_event_panel.csv`、`real_matched_event_panel.csv`、`real_events_clean.csv`）；任何一个缺失都会直接报错，不回退 demo（和 RDD L3 契约一致）。

产出：

- `results/real_tables/cma_*.csv`（ar/car path、window summary、gap event/summary、mechanism panel、4 个 heterogeneity 维度、rolling、break、hypothesis map、hypothesis verdicts）
- `results/real_tables/cma_hypothesis_verdicts.csv`：每条机制假设的 verdict（`支持` / `部分支持` / `反对` / `待补数据`）+ confidence + 下一步建议；dashboard 在 demo / full 模式下渲染成 verdict 卡片
- `results/real_tables/cma_pre_runup_bootstrap.csv`：H1 信息预运行的 CN-US 跨市场差异 bootstrap 检验(diff_mean / boot_p_value / 95% CI),自动喂给 H1 verdict 替代单市场显著性判断
- `results/real_tables/cma_gap_drift_market_regression.csv`：H4 卖空约束的 `gap_drift ~ cn_dummy + gap_length_days` OLS-HC3 回归结果(cn_coef / cn_p_value / r_squared),自动喂给 H4 verdict 替代单市场比较
- `results/real_tables/cma_h3_channel_concentration.csv`：H3 散户/机构结构的 4 象限双通道(turnover + volume)显著性表(turnover_sig / volume_sig / both_channels_sig),自动喂给 H3 verdict 替代单通道判断
- `results/real_tables/cma_h5_limit_predictive_regression.csv`：H5 涨跌停限制的 CN 事件级预测回归(`car_1_1 ~ price_limit_hit_share + log_mktcap_pre`),自动喂给 H5 verdict 替代单市场 t 检验
- `results/real_tables/cma_mechanism_panel.tex`（论文可直接插入）
- `results/real_figures/cma_*.png`（7 张主图）
- `results/real_tables/research_summary.md` 新增章节"六、美股 vs A股 不对称"（幂等追加，不会重复）

只想重新生成 LaTeX 而跳过计算：

```bash
index-inclusion-cma --tex-only
```

需要叠加被动基金 AUM 到时序图(同时解锁 H2 verdict):准备 `data/raw/passive_aum.csv`(列:`market, year, aum_trillion`)后

```bash
index-inclusion-cma --aum data/raw/passive_aum.csv
```

如果你拿到的 AUM 原始文件列名是 `Country / Year / AUM` 或中文 `市场 / 年份 / 被动AUM` 之类的非标准格式,先用导入工具归一化:

```bash
index-inclusion-prepare-passive-aum \
  --input /path/to/raw_aum.csv \
  --output data/raw/passive_aum.csv \
  --force
```

它会自动把列名 / 市场代号 / 数值类型规范化,丢掉无效行(未识别市场、年份非数、aum 非正)并打印审计行。`--check-only` 只校验不写盘。模板见 [data/raw/passive_aum.template.csv](data/raw/passive_aum.template.csv)。

`index-inclusion-make-figures-tables` 跑完标准表格后，如果检测到已存在的 `cma_mechanism_panel.csv`，会自动调 `regenerate_tex_only` 把 `.tex` 刷新到最新。

CMA 的 dashboard 集成以**自包含 helper** 形式交付：`index_inclusion_research.analysis.cross_market_asymmetry.dashboard_section.build_cross_market_section(tables_dir=..., figures_dir=..., mode=...)` 返回一个 presenter-agnostic 的 section context，dashboard 层可以在任何 mode（`brief` / `demo` / `full`）下直接接入。

### 交互式 ECharts 图层

dashboard 在 `demo` / `full` 模式下渲染交互式图表(基于 ECharts CDN)。每张交互图通过 `/api/chart/<chart_id>` 端点拉取 JSON,数据由 `index_inclusion_research.chart_data` 模块根据现有 CSV 输出按需构建,PNG 仍作 fallback 渲染:

| chart_id | 数据来源 | 渲染位置 |
|---|---|---|
| `car_path` | `cma_ar_path.csv` + `cma_car_path.csv` | CMA 段头 |
| `car_heatmap` | `event_study_summary.csv` | sample design 段 |
| `price_pressure` | `time_series_event_study_summary.csv` | 价格压力 track |
| `gap_decomposition` | `cma_gap_summary.csv` | CMA 段图卡 |
| `heterogeneity_size` | `cma_heterogeneity_size.csv` | CMA 段图卡 |
| `time_series_rolling` | `cma_time_series_rolling.csv` | CMA 段图卡 |
| `main_regression` | `regression_coefficients.csv` | sample design 段(main_car forest plot) |
| `mechanism_regression` | `regression_coefficients.csv` | sample design 段(turnover_mechanism forest plot) |
| `event_counts` | `event_counts_by_year.csv` | sample design 段(年份分布柱图) |
| `cma_mechanism_heatmap` | `cma_mechanism_panel.csv` | CMA 段(t 值热力图) |
| `cma_gap_length_distribution` | `cma_gap_event_level.csv` | CMA 段(窗口长度柱图) |
| `rdd_scatter` | `event_level_with_running.csv` | identification track(RDD 散点图带 cutoff) |

图表通过 `IntersectionObserver` 懒加载;未识别的 `chart_id` 返回 404。新增图表见 [src/index_inclusion_research/chart_data.py](src/index_inclusion_research/chart_data.py) 的 `CHART_BUILDERS` 注册表与 [src/index_inclusion_research/web/static/dashboard/interactive_charts.js](src/index_inclusion_research/web/static/dashboard/interactive_charts.js) 的 `CHART_OPTION_BUILDERS`。

## 哪些文件是“核心文件”

如果你时间不多，优先看这几项：

- [README.md](README.md)
- [docs/literature_to_project_guide.md](docs/literature_to_project_guide.md)
- [docs/dashboard_architecture.md](docs/dashboard_architecture.md)
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
