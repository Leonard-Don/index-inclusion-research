# 研究状态快照

## 裁决基线快照

- 快照文件：`snapshots/pre-registration-2026-05-16.csv`（文件名为历史命名）
- 基线日期：`2026-05-16`
- 假说数：7 行
- 说明：H1–H7 为 post-hoc、探索性裁决，此快照用于跨时间观察 verdict 稳定性，非预注册。

## CMA 假说裁决

- 当前裁决分布：3 项支持 / 1 项部分支持 / 3 项证据不足
- 主表入选 (`evidence_tier=core`)：4 条假说
- 详细裁决见 `tables/cma_hypothesis_verdicts.tex` 与 `narrative/paper_outline_verdicts.md`。

## HS300 RDD L3 样本

- 候选行数 / 批次数：356 行 / 11 批次
- 样本期：2020-11-27 至 2025-11-28
- 覆盖摘要：356 条候选；11 个批次；调入 191 / 对照 165；11 个批次覆盖 cutoff 两侧。
- 论文级门槛：≥20 批次 / ≥10 年。当前为初步识别证据。

## HS300 RDD 主结果

- main 局部线性 τ = 4.01% (p = 0.045, n = 118)
- 完整稳健性面板见 `rdd/rdd_robustness.csv` 与 `rdd/rdd_robustness_forest.png`。

---

由 `index-inclusion-paper-bundle` 自动生成。研究状态来自当前真实数据；要刷新先跑 `make rebuild`。
