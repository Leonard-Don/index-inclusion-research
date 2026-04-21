# Dashboard Commit Boundary Guide

这份清单的目标不是解释架构，而是帮助你在当前 dirty worktree 里把 dashboard 这条 refactor 线单独拎出来，避免和其他工作混在一起提交。

## 结论先说

如果你要把当前 dashboard 重构线整理成更干净的提交，最推荐拆成下面几组：

1. `dashboard core + runtime/route refactor`
2. `dashboard browser/CI and static assets`
3. `track wrapper compatibility`
4. `HS300 RDD candidate import / data contract`
5. `docs-only follow-up`

其中第 1 组和第 2 组可以一起提交；第 3、4 组更适合单独拆开。

## 当前推荐运行面

如果你只是想判断“哪些文件代表现在的主运行面”，可以先记这条：

- 当前推荐入口优先看 `src/index_inclusion_research/` 里的包内模块。
- 对外命令面优先看 `index-inclusion-*` 这组 CLI，或 `python3 -m index_inclusion_research.<module>`。
- `scripts/*.py` 现在更多是历史兼容层，不应再被当成主干实现的默认落点。

换句话说，这份文档里凡是提到 `scripts/`，大多是在讨论：

- 旧入口是否还能继续跑
- 兼容层是否应该单独提交
- 哪些 wrapper 不该和主干重构绑死

而不是在定义“今天这个项目应该从哪里开始运行”。

## 建议归入 Dashboard Refactor 主提交的范围

这些文件属于当前 dashboard 主干重构的核心范围：

```text
.github/workflows/ci.yml
pyproject.toml
scripts/static/
scripts/templates/
src/index_inclusion_research/__init__.py
src/index_inclusion_research/dashboard_*.py
src/index_inclusion_research/results_snapshot.py
tests/test_dashboard*.py
tests/test_results_snapshot.py
```

如果你想先把这条线单独 stage，建议从下面这条命令开始：

```bash
git add \
  .github/workflows/ci.yml \
  pyproject.toml \
  scripts/static \
  scripts/templates \
  src/index_inclusion_research/__init__.py \
  src/index_inclusion_research/dashboard_*.py \
  src/index_inclusion_research/results_snapshot.py \
  tests/test_dashboard*.py \
  tests/test_results_snapshot.py
```

这组里包含的核心变化有：

- dashboard 从单脚本走向包内装配
- `ResultsSnapshot` 缓存层
- `application / factory / services / runtime / routes` 的显式分层
- browser smoke 和 CI 接入
- dashboard 静态资源与模板

## 建议单独拆出的提交

### A. 历史兼容层

下面这些文件更像“历史兼容层”，不一定要和 dashboard 主干重构绑定在一个 commit 里：

```text
src/index_inclusion_research/literature_dashboard.py
scripts/start_literature_dashboard.py
scripts/start_harris_gurel.py
scripts/start_shleifer.py
scripts/start_hs300_style.py
src/index_inclusion_research/cli.py
```

如果你想把提交边界压得更干净，可以把它们单独做成一个兼容性提交。
这组文件存在的主要意义是保旧入口，不是定义当前推荐运行面。
真正的推荐运行面仍然在包内模块，例如 `src/index_inclusion_research/literature_dashboard.py`
和 `src/index_inclusion_research/*_track.py`。

### B. HS300 RDD 候选样本导入与数据契约

下面这些文件建议单独成组，因为它们解决的是 RDD 输入和校验问题，不是 dashboard 架构问题。
其中包内实现是主干，`scripts/` 入口更多只是兼容层：

```text
docs/hs300_rdd_data_contract.md
src/index_inclusion_research/prepare_hs300_rdd_candidates.py
src/index_inclusion_research/reconstruct_hs300_rdd_candidates.py
scripts/prepare_hs300_rdd_candidates.py
scripts/start_hs300_rdd.py
src/index_inclusion_research/analysis/rdd_candidates.py
tests/test_hs300_rdd.py
tests/test_prepare_hs300_rdd_candidates.py
```

建议单独 stage：

```bash
git add \
  docs/hs300_rdd_data_contract.md \
  src/index_inclusion_research/prepare_hs300_rdd_candidates.py \
  src/index_inclusion_research/reconstruct_hs300_rdd_candidates.py \
  scripts/prepare_hs300_rdd_candidates.py \
  scripts/start_hs300_rdd.py \
  src/index_inclusion_research/analysis/rdd_candidates.py \
  tests/test_hs300_rdd.py \
  tests/test_prepare_hs300_rdd_candidates.py
```

### C. README / docs / ignore 规则

下面这些文件建议最后再处理，因为它们往往会跨多条工作线：

```text
README.md
.gitignore
```

如果 README 同时覆盖了 dashboard、RDD、CLI、文献入口和开发说明，最好放在最后做一个 docs-only commit，避免把功能变更和文档大改搅在一起。

## 当前 worktree 的实用拆法

如果按“最少回头成本”的顺序来，我建议：

1. 先提交 dashboard core
   `dashboard_*.py`、`results_snapshot.py`、dashboard tests、静态资源、模板、CI、`pyproject.toml`
2. 再提交 wrapper / CLI 兼容
   `src/index_inclusion_research/literature_dashboard.py`、`scripts/start_literature_dashboard.py`、`src/index_inclusion_research/cli.py`、旧研究主线 wrapper
3. 再提交 RDD 候选样本导入
4. 最后单独提交 README 和架构文档

## 推荐提交信息

如果你想把这条线拆得更清晰，下面这几条 commit message 会比较顺手：

1. `refactor: modularize literature dashboard application stack`
2. `test: add dashboard browser smoke and route/runtime coverage`
3. `refactor: split dashboard runtime and route dependencies into explicit layers`
4. `feat: add hs300 rdd candidate import and validation flow`
5. `docs: document dashboard architecture and commit boundary`

## 停止规则

如果你已经满足下面三条，就可以认为 dashboard 这条线的“提交边界整理”已经够用了：

- dashboard 主干文件能够被单独 `git add`
- RDD 输入契约和候选样本导入不会混进 dashboard core commit
- README / docs / ignore 规则被留到最后单独检查

再继续细拆通常收益就会明显递减。
