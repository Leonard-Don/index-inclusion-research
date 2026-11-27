# Tier B 统计/数值修复 — 设计

日期：2026-05-31 ｜ 状态：已批准（用户确认 power 阈值 = 0.5）

源自项目评估的 Tier B：四项真正的统计/数值修复。三项会改动 published 结果，按用户的研究设计选择执行；一项是纯防御 bug 修复。

## 决策（用户已定）

- **#5 事件聚类**：加为**稳健性列**（现有 iid t 检验保持为主表口径，不替换）。
- **#6 H3/H7 裁决 vs 功效**：**功效感知降置信 + 标注**（verdict 词不变；事后 power < 0.5 时把 confidence 上限压到"低"并标注 underpowered）。PAP 记为 weakened，非 flipped。
- **#4 H1 bootstrap**：换成**置换检验**（permutation）。
- **基线**：四项改完后**重打诚实预注册快照**（新日期）+ analysis_parameters 变更日志记录方法学修正的来龙去脉。
- **power 阈值** = **0.5**（与项目已有"power<30% 严重欠功效"措辞兼容）。

## 四项修复

### #7 RDD 除零守卫（纯防御，零结果改动）
- 文件：`src/index_inclusion_research/analysis/rdd.py`（`_fit_single_rdd`）。
- 改：拟合前若 `len(local) <= design.shape[1]`（4 个参数）或自由度 `<= 0`，返回既有的 NaN/空结果 dict（带真实 `n_obs`/`n_left`/`n_right`），不再把 NaN τ 当有效值返回。
- TDD：`tests/test_rdd.py` 加一例——退化窗口（n≤4）返回 NaN、不抛 divide-by-zero、不产 RuntimeWarning。
- 级联：committed 真实 spec 均 n≫4 → 结果零改动。

### #4 H1 bootstrap → 置换检验
- 文件：`src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py`（pre-runup bootstrap）。
- 改：把"各组绕自身均值重采样、数零穿越"换成 H₀（合并标签）下置换 CN/US，`p = 比例(|perm_diff| >= |observed_diff|)`（双侧）。保留既有 bootstrap 分位 CI 作为区间估计另列（区分"区间"与"p 值"）。
- TDD：`tests/test_cma_gap_period.py`——明显有组间差异的合成数据 → p 小；无差异 → p 大（≈1）；置换 p 在 [0,1]。
- 级联：`boot_p_value` 数字变；H1 diff=0.14% → verdict 仍"证据不足"。docs/CSV 同步。

### #5 事件聚类（稳健性列）
- 文件：`src/index_inclusion_research/analysis/event_study.py`（summary 构建）+ 复用 `analysis/pyfixest_cluster.py::estimate_announcement_day_cluster_se`（CRV1）。
- 改：在 event-study summary 增列 `se_car_clustered` / `p_value_clustered`，按事件日期聚类；现有 `se_car`/`p_value`（iid t）保持为主列。若 pyfixest 不可用则该列为 NaN（与现有 optional-dep 处理一致）。
- 文件可能波及：`outputs/schema_registry.py`（登记新列）、`tests/test_event_study_summary.py`、`tests/test_output_tables.py`。
- TDD：同日相关事件 → 聚类 SE ≥ iid SE；列存在且类型正确。
- 级联：仅加列；headline t=6.48/5.34 → 聚类后仍显著，verdict 不变。

### #6 H3/H7 功效感知降置信
- 文件：`src/index_inclusion_research/analysis/cross_market_asymmetry/verdicts/_h_functions.py` + 在 `_core`/导出层做后处理。
- 改：verdict + confidence 算好后，若该假说事后 power < 0.5，把 confidence 上限压到"低"，并在 evidence_summary 末尾追加 `（underpowered: power=…）`。verdict 词不变。power 来源：`analysis/power_analysis.py` 的事后 power（H3 exact-binomial / 各 H 对应口径）。
- TDD：power≈0 的 H3 → confidence 被压到"低"且带标注；功效充足的假说 confidence 不受影响。
- 级联：H3 confidence 高→低、H7 中→（视 power）；verdict 词不变。PAP 记 weakened。

## 收尾（settle）
1. 重算 verdicts/power/event-study → 同步 README/outline/delivery/analysis_parameters（+ narrative 副本）。
2. **重打诚实预注册快照**（`snapshots/pre-registration-2026-05-31.csv`）+ analysis_parameters 变更日志一条：记录"置换检验/聚类稳健性列/功效感知置信"的方法学修正与影响。
3. 重建 bundle（`paper-bundle --no-regenerate`）。
4. 全门禁到位：ruff / mypy / pytest / paper-audit / paper-integrity / doctor（重打基线后 documented-flip 应清零）。

## 实现纪律
- 每项 TDD：先写失败测试（red），最小实现（green），再清理。
- #7、#4 文件隔离，可并行；#5、#6 触及 schema/verdict 口径，主线串行实现并人工复核统计正确性。
- 不改任何结果除非测试与口径都对齐；置换检验与聚类的统计正确性需独立复核。
