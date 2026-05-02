# p 阈值灵敏度分析

H1 / H4 / H5 的 verdict 由单一 p 决定（分别是 bootstrap p / regression p / limit_coef p）。H2 / H3 / H6 / H7 的头条指标是 spread / 命中率 / AUM 比率，不在 p 阈值 sweep 范围内。

审稿人最常追问 "如果阈值是 0.05 而不是 0.10，结论会怎样？" 项目把回答这个问题的能力做成 **5 层入口**：

## 0. 决定层（重新生成 verdict）

```bash
index-inclusion-cma --threshold 0.05
```

让 verdict 字段（`支持` / `部分支持` / `证据不足`）在自定义阈值下重新生成。语义：

- `THRESHOLD` 是边界 p，inner cutoff = `THRESHOLD/2`（支持 / 高 confidence）
- outer = `THRESHOLD`（部分支持 / 中 confidence）

默认 0.10 与历史行为字节兼容（inner 0.05 / outer 0.10）。下面四层都基于这一次跑出的 CSV。

## 1. 数据层（读 CSV）

`cma_hypothesis_verdicts.csv` 自带 `p_value` 列（H1/H4/H5 填 boot/reg/limit p，其他 4 个为 NaN）：

```python
import pandas as pd
df = pd.read_csv("results/real_tables/cma_hypothesis_verdicts.csv")
df.loc[df["p_value"].notna() & (df["p_value"] < 0.05), ["hid", "name_cn", "p_value"]]
```

## 2. CLI 层（sweep + 多重检验校正）

```bash
# 默认三阈值 (0.05, 0.10, 0.15)
index-inclusion-verdict-summary --sensitivity

# 自定义阈值（自动去重 + 排序）
index-inclusion-verdict-summary --sensitivity 0.01 0.05 0.10 0.15 0.20

# JSON 输出供 CI / 下游脚本消费
index-inclusion-verdict-summary --format json --sensitivity 0.05 0.10 | jq '.sensitivity'
```

终端表格示意：

```
 假说 verdict p 值灵敏度（3 阈值）
  hid    p_value   bonf_p     bh_q  p<0.05   p<0.1  p<0.15
  ──────────────────────────────────────────────────────
  H1      0.8748   1.0000   0.8748       —       —       —
  H4      0.5366   1.0000   0.8050       —       —       —
  H5      0.2134   0.6403   0.6403       —       —       —
  p<0.05: 0/3 显著 · p<0.1: 0/3 显著 · p<0.15: 0/3 显著
  Bonferroni (raw·m): bonf_p<0.10 通过 0/3; Benjamini-Hochberg: bh_q<0.10 通过 0/3
  注:H2 H3 H6 H7 头条指标不是 p,不在 sweep 范围内。
```

`bonferroni_p` 是 `min(1.0, p × m)`，`bh_q` 是 Benjamini-Hochberg 调整 q-value。`m` 是登记了结构化 p_value 的假说数（当前 3）。两个都内嵌在 `cma_hypothesis_verdicts.csv` 输出列里，下游 pandas 可直接读。

## 3. GUI 层（dashboard 阈值 chip）

dashboard CMA section 的 verdict 网格上方有 5 个阈值 chip（0.01 / 0.05 / 0.10 / 0.15 / 0.20），默认 0.10 active。点击其他 chip：

- H1/H4/H5 卡片底部 sensitivity strip 实时翻 "在 p<X 下显著（p=…）" / "在 p<X 下不显著（p=…）"
- non-p 卡片始终标 "头条指标不是 p,不在 sweep 范围"

给 advisor / 同事演示时直接点鼠标即可。

## 4. CI 层（doctor 边界检测）

`index-inclusion-doctor` 的 `p_gated_verdict_sensitivity` 检查会自动 flag 处于 `[0.05, 0.10)` 边缘区间的假说（default 显著但 strict 翻 not_sig — 审稿人会追问 robustness 的典型情形）：

```text
✓  p_gated_verdict_sensitivity
    3 p-gated hypotheses; 0 significant at strict (0.05), 0 at default (0.1);
    none sit in the [0.05, 0.1) boundary.
```

如果出现 boundary 项，doctor 变 warn 并在 details 列出 hid + p，fix 建议指向 `verdict-summary --sensitivity` 双阈值。GitHub Actions 默认运行 `index-inclusion-doctor --format json` 显示 warning 但不阻断；本地 `make doctor` 同。

## Doctor 严格门禁与机器可读输出

`index-inclusion-doctor` 默认只在 `fail` 时返回非零退出码；`warn` 用来标记研究边界、生成物漂移或数据缺口（H2 AUM、H6 weight_change、H7 CN sector、HS300 RDD L2/L3 状态、matched_sample_balance）。常规 CI 让 warning 可见但不阻断；严格模式：

```bash
index-inclusion-doctor --fail-on-warn
index-inclusion-doctor --format json | jq '.summary'
index-inclusion-doctor --format json --fail-on-warn
```
