# p 阈值灵敏度分析

H1 / H4 / H5 的 verdict 由单一 p 决定（分别是 bootstrap p / regression p / limit_coef p）。H2 / H3 / H6 / H7 的头条指标是 spread / 命中率 / AUM 比率，不在 p 阈值 sweep 范围内。

审稿人最常追问 "如果阈值是 0.05 而不是 0.10，结论会怎样？" 项目把回答这个问题的能力做成 **6 层入口**：

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

## 5. Forest visualization 层（多阈值 CMA 森林图）

`index-inclusion-verdict-summary --sensitivity` 是终端表格视图；如果要把灵敏度结果放进论文 / 答辩 PPT，可执行。注意当前 significance threshold 只影响 H1 / H4 / H5 这三条 p-gated 假说；H2 / H3 / H6 / H7 的头条 gate 是方向 spread / 命中率 / AUM 比率，不应解读为 p 阈值翻转。

```bash
# 默认 0.05 / 0.10 / 0.15 / 0.20 四阈值
index-inclusion-build-cma-sensitivity-forest

# 自定义阈值
index-inclusion-build-cma-sensitivity-forest --thresholds 0.01 0.05 0.10

# 显式落盘路径
index-inclusion-build-cma-sensitivity-forest \
  --png results/figures/cma_verdicts_sensitivity.png \
  --pdf results/figures/cma_verdicts_sensitivity.pdf
```

输出图：
- **横轴**：support-strength 评分 [0, 1]（同 `cma_verdicts_forest`，公式见 `outputs/cma_verdicts_forest.py::classify_strength`）
- **纵轴**：H1-H7
- **每条假说**：4 个 dot（每个阈值一个），用细灰线串起来，便于看 strength 的轨迹
- **dot 颜色**：按该阈值的 `evidence_tier` 上色（core=蓝、supplementary=灰）
- **dot 形状**：相对前一阈值 verdict 是否翻转。circle=稳定 / triangle=该阈值发生翻转
- **右侧 margin**：每条假说的 flip 计数注解（`stable` / `1 flip` / `2+ flips`），最能 sensitivity-sensitive 的假说一眼可辨
- **解释边界**：真正由阈值 knob 驱动的翻转只应出现在 H1 / H4 / H5；H2 / H3 / H6 / H7 留在图中是为了给整体 evidence strength 做参照。

每次运行会把单阈值 CMA pipeline 的 verdicts 缓存到 `results/sensitivity/threshold_<T>/cma_hypothesis_verdicts.csv`（dot 替换成下划线，e.g. `threshold_0_05`）。自定义阈值必须至多两位小数，避免 `0.104` 与 `0.10` 共用同一个 cache label。显式 CLI 只有在缓存旧于任一 CMA 输入（event panels / events / passive AUM / CN passive-AUM proxy / H6 weight-change）时才重跑；强制刷新：删除对应 `threshold_<T>/` 目录即可。

`make figures-tables` 在 `results/sensitivity/` 已有缓存时会自动从缓存重渲该图（不会自动跑 CMA pipeline 四遍），与 `cma_verdicts_forest` 一起更新；`make paper` 通过 `paper_bundle.py::_regenerate_artifacts` 也只做 cache-only 重绘。`index-inclusion-doctor` 的 `cma_sensitivity_forest_artifact` 检查在缓存非空但 PNG/PDF 缺失 / 过期时变 warn。

## 6. AR Engine Robustness 层（AR 引擎选择敏感性）

阈值灵敏度回答的是"p 阈值挪一挪 verdict 会不会翻"；AR 引擎灵敏度回答的是另一条同样常被审稿人提的问题——"AR 本身的定义（市场调整 vs 市场模型）换一下会不会翻"。两者是 robustness defense 的两条独立 axis：**threshold sensitivity 证明 verdict 不依赖于阈值选择；AR engine sensitivity 证明 verdict 不依赖于 AR 模型选择**。

```bash
# 默认 adjusted + market 两条引擎，threshold=0.10
index-inclusion-build-cma-ar-engine-forest

# 显式覆盖引擎列表（重复 / 顺序混乱时会去重并规范化到 (adjusted, market) 顺序）
index-inclusion-build-cma-ar-engine-forest --ar-models adjusted market

# 自定义共用阈值（默认 0.10）
index-inclusion-build-cma-ar-engine-forest --threshold 0.05

# 显式落盘路径
index-inclusion-build-cma-ar-engine-forest \
  --png results/figures/cma_verdicts_ar_engine.png \
  --pdf results/figures/cma_verdicts_ar_engine.pdf
```

引擎定义（与 `run-event-study --ar-model` 同义，commit 1e29476 引入）：

- **adjusted**（项目默认）：`ar = ret − benchmark_ret`，文献标准的简单市场调整。
- **market**：`ar = ret − (α + β · benchmark_ret)`，α/β 在事件每条 (event_id, phase) 上以 (-120, -10) trading days 估计窗口跑 OLS；估计窗口数据不足或基准方差为零的事件留 NaN。注意 AR-engine sweep 走的是直接面板 materialization：审计信息保留在 `market_model_event_panel.csv` / `market_model_matched_event_panel.csv` 的 `market_model_estimation_obs` 与 `ar_market_model` 列中；不会额外生成 `event_study_skipped_events.csv`（该 sidecar 只属于 `index-inclusion-run-event-study --ar-model market`）。

输出图：
- **横轴**：support-strength 评分 [0, 1]（同 `cma_verdicts_forest::classify_strength`）
- **纵轴**：H1-H7
- **每条假说**：2 个 dot（每条引擎一个），strength 不同时由灰色短箭头串起，便于看翻转方向
- **dot 颜色 + 形状**：adjusted = circle / teal，market = square / purple（greyscale 打印也能解码）
- **右侧 margin**：每条假说 `stable`（两条引擎 verdict 一致）/ `flipped`（verdict 不一致，这一行对 AR 模型敏感）
- **解释边界**：market 引擎在估计窗口太薄或基准方差退化时会把对应事件×phase 的 `ar` 留 NaN，CAR 聚合 + p_value 都会随之轻微移动；H3 / H7 这种命中率 / spread 头条 gate 也会因 NaN 比例变化而受影响，但量级与 p-gated H1/H4/H5 不可比。

每次运行会把单引擎 CMA pipeline 的 verdicts 缓存到 `results/sensitivity/ar_<engine>/cma_hypothesis_verdicts.csv`（engine ∈ `adjusted` / `market`），并在同目录写 `cma_ar_engine_cache_metadata.json` 记录当前 threshold；只有 metadata threshold 与 CLI threshold 一致且 CMA 上游未更新时才会走 cache hit，避免 `--threshold 0.05` 复用 `0.10` 的 verdicts。`adjusted` 缓存与历史 CMA pipeline 的输出位级一致（默认 ar 列就是 `ret − benchmark_ret`）；`market` 缓存额外会在同目录写 `market_model_event_panel.csv` / `market_model_matched_event_panel.csv` 两个临时面板（`ar` 列被市场模型 β-AR 覆写后整面板回灌给 orchestrator）。**首次跑 market 引擎需要等一次完整的 CMA pipeline 跑完（约 2-5 分钟）；之后只要 metadata threshold 匹配且 CMA 上游 (event / matched panel / events_clean / passive_aum / weight_change) 没变就走 cache hit。**

`make figures-tables` 与 `make paper` 同样只做 cache-only 重绘（不会自动 fire 一次 fresh market-engine 跑）。`index-inclusion-doctor` 的 `cma_ar_engine_forest_artifact` 检查在缓存非空但 PNG/PDF 缺失 / 过期时变 warn，与 `cma_sensitivity_forest_artifact` 平行。

## 7. 2D Robustness 层（阈值 × AR 引擎 同时变）

阈值灵敏度证明 verdict 不依赖于阈值；AR 引擎灵敏度证明 verdict 不依赖于 AR 模型；reviewer 的下一问通常是把两条合成一条：**"两条 axis 同时变会不会翻？"** 2D 稳健性热力图就是回答这一问的 headline 图。

```bash
# 默认 4 阈值 × 2 引擎 = 8 单元（每假说一行，56 单元）
index-inclusion-build-cma-2d-robustness-heatmap

# 自定义阈值 / 引擎组合
index-inclusion-build-cma-2d-robustness-heatmap \
  --thresholds 0.05 0.10 --ar-models adjusted market

# 显式落盘路径
index-inclusion-build-cma-2d-robustness-heatmap \
  --png results/figures/cma_verdicts_2d_robustness.png \
  --pdf results/figures/cma_verdicts_2d_robustness.pdf
```

输出图：
- **行**：H1-H7（同 forest 图）
- **列**：8 列，左 4 列为 adjusted 引擎（按 threshold 升序排列），右 4 列为 market 引擎，中间用粗黑分隔线区分两个引擎组；列顶部还有一行 `adjusted engine` / `market engine` 文字标签
- **单元色温**：support-strength 评分，深红 = 0.0（无支持），白 = 0.5（部分），深蓝 = 1.0（强支持）
- **单元 ASCII tag**：`S+` (支持/高) / `S` (支持/中-低) / `P+` (部分支持) / `I` (证据不足)，greyscale 打印仍可解码
- **右侧 margin**：每行一个 `stable` / `1 flip` / `2+ flips` 标签，按 8 单元里 distinct verdict 字符串数量算 — 1 种 verdict = `stable`，2 种 = `1 flip`，≥3 种 = `2+ flips`
- **解释边界**：色温与 verdict tag 都来自单轴 sweep 同一个 `classify_strength`，没有新的统计推断；reviewer 在意的是 "哪些假说在 8 单元里仍 100% 一致"（rock solid）vs "哪些假说仅在某些 (T, engine) 组合下翻"（fragile）

Cache 设计的优先级：

1. **dedicated 2D cache**：`results/sensitivity/grid_<T>_<engine>/cma_hypothesis_verdicts.csv`（同目录的 `cma_2d_robustness_cache_metadata.json` 记录 threshold + engine，threshold metadata mismatch 会被 invalid）。
2. **single-axis fallback**：(0.10, adjusted) → `ar_adjusted/`；(0.10, market) → `ar_market/`；(T, adjusted) → `threshold_<T>/`。fallback 命中后会同步写一份到 dedicated cache，下次直接命中第一层。
3. **fresh CMA pass**：只剩 (T≠0.10, market) 三个单元真正需要 fire CMA 一次（市场模型 panel 还要 materialize）；因为复用了 87d624c 和 1a6ba77 的缓存工作，首次跑 8 单元的 wall-clock 大概只是单一 market run 的 3 倍（约 6-10 分钟），之后所有单元都走 cache。

`make figures-tables` 与 `make paper` 都走 cache-only 重绘（`build_cma_2d_robustness_heatmap_from_cache`），自动发现 `grid_*/` 或单轴 fallback，缺失的单元静默跳过（不会触发 fresh CMA）。`index-inclusion-doctor` 的 `cma_2d_robustness_heatmap_artifact` 检查在任意类型 cache 非空但 PNG/PDF 缺失/过期时变 warn，与前两个 robustness 检查平行。

## Doctor 严格门禁与机器可读输出

`index-inclusion-doctor` 默认只在 `fail` 时返回非零退出码；`warn` 用来标记研究边界、生成物漂移或数据缺口（H2 AUM、H6 weight_change、H7 CN sector、HS300 RDD L2/L3 状态、matched_sample_balance）。常规 CI 让 warning 可见但不阻断；严格模式：

```bash
index-inclusion-doctor --fail-on-warn
index-inclusion-doctor --format json | jq '.summary'
index-inclusion-doctor --format json --fail-on-warn
```
