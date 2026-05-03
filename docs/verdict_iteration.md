# Verdict 迭代追踪

## Pre-registration status

7 条 CMA 假说截至 2026-05 仍是 **post-hoc**（详见 `docs/limitations.md` §6 与 `docs/paper_outline_verdicts.md`）。本文档定义的 verdict-diff 工作流就是为了支持**未来的预注册迭代**：

1. 在打 tag 的 commit 里冻结：假说文本、p 阈值、样本筛选规则。
2. 跑 `index-inclusion-cma`，把当前 `cma_hypothesis_verdicts.csv` 快照到 `cma_hypothesis_verdicts.previous.csv`（`--snapshot <path>` 可指定别处）。
3. 修数据 / 改代码后再跑，用 `--compare-with <previous>` 对比方向变化；verdict 文本和阈值保持不变。

任何偏离预注册设计（增删假说、改阈值、换样本口径）必须在本文档下方"决策日志"里记录日期与原因。

## diff 工作流

每跑一次 `index-inclusion-cma`，orchestrator 会自动把上一次的 verdicts 复制到 `results/real_tables/cma_hypothesis_verdicts.previous.csv`。补 H2 AUM / 跑 H6 weight_change / 加新批次以后能直接 diff：

```bash
# 看哪些 verdict 翻转 / key_value 漂了多少
index-inclusion-verdict-summary --compare-with results/real_tables/cma_hypothesis_verdicts.previous.csv

# 想自己保留时点快照（例如发版前）
index-inclusion-verdict-summary --snapshot snapshots/before-aum-data.csv
# ...再跑 CMA / 改数据...
index-inclusion-verdict-summary --compare-with snapshots/before-aum-data.csv

# 机器可读输出（CI / 后续工具消费）
index-inclusion-verdict-summary --format json | jq '.aggregate'
```

diff 输出形如：

```
VERDICT DIFF · 当前 vs 快照
  changed: 1, added: 0, removed: 0, unchanged: 6

已变更:
  H1 · 信息泄露与预运行
    verdict        : 证据不足  →  支持
    key_value      : 0.640  →  0.012  (Δ -0.628)
```
