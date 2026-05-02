# Verdict 迭代追踪

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
