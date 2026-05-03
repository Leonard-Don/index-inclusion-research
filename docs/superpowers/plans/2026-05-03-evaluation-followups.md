# Evaluation Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the 8 follow-ups from the 2026-05-03 evaluation: README sync, limitations doc, post-hoc disclosure, doctor strict mode, Patell/BMP standardization, McCrary density test, CMA hypothesis triage, and L3 coverage audit.

**Architecture:**
- Methodology additions live alongside existing modules (`analysis/event_study.py`, `analysis/rdd.py`) as new helpers; old simple-t output remains so existing consumers keep working.
- Verdicts CSV gains an `evidence_tier` column (`core` for H1/H5/H7, `supplementary` for H2/H3/H4/H6) — no row-removal.
- Doctor stays opt-in for `--fail-on-warn` but Makefile gains `make doctor-strict`; CI continues current behavior.
- Documentation deltas are concentrated in 3 files (README.md, paper_outline_verdicts.md, new docs/limitations.md).

**Tech Stack:** Python 3.11, pandas, numpy, scipy.stats, statsmodels, pytest.

**Out of scope:** Extending HS300 RDD L3 dataset to ≥10 years requires manual transcription from CSIndex PDFs and is documented (Task 8) but not executed here.

---

## File Map

| Task | Create | Modify |
|---|---|---|
| 1 | — | `README.md` |
| 2 | `docs/limitations.md` | `README.md` (link) |
| 3 | — | `docs/paper_outline_verdicts.md`, `docs/verdict_iteration.md` |
| 4 | — | `Makefile`, optional `docs/cli_reference.md` |
| 5 | `tests/test_event_study_patell.py` | `src/index_inclusion_research/analysis/event_study.py`, `src/index_inclusion_research/run_event_study.py` |
| 6 | `tests/test_rdd_mccrary.py` | `src/index_inclusion_research/analysis/rdd.py`, `src/index_inclusion_research/hs300_rdd.py` |
| 7 | — | `src/index_inclusion_research/analysis/cross_market_asymmetry/verdicts/_h_functions.py` (or wherever verdicts assemble), `tests/test_cma_*` |
| 8 | `docs/hs300_rdd_l3_collection_audit.md` | — |

---

### Task 1 — Sync README stale H3 verdict

**Files:** Modify `README.md`

- [ ] **Step 1.1 — Read CSV ground truth and find README mismatch**

```bash
grep -n "H3\|双通道命中率" /Users/leonardodon/index-inclusion-research/README.md
```

Current README claims `部分支持`, `双通道命中率 = 0.500`. Current CSV (`results/real_tables/cma_hypothesis_verdicts.csv`) shows `支持`, `0.75`. Update README.

- [ ] **Step 1.2 — Edit README**

Replace:
```
| H3 | 散户 vs 机构结构 | 部分支持 | 双通道命中率 = 0.500 | 短期价格压力 |
```
With:
```
| H3 | 散户 vs 机构结构 | 支持 | 双通道命中率 = 0.75 | 短期价格压力 |
```

- [ ] **Step 1.3 — Verify**

```bash
grep -A1 "H3 | 散户" README.md
```

Expected: shows `支持` and `0.75`.

- [ ] **Step 1.4 — Commit**

```bash
git add README.md
git commit -m "docs: sync README H3 verdict to current CSV"
```

---

### Task 2 — Add limitations & data caveats doc

**Files:** Create `docs/limitations.md`; modify `README.md`

- [ ] **Step 2.1 — Draft `docs/limitations.md`**

Contents (full markdown, no placeholders):

```markdown
# 数据与方法限制

本文档集中记录项目的关键数据近似与方法约束，论文写作和读者评估时请同时引用此页。

## 1. 价格与市值数据

- **价格 / 收益**：Yahoo Finance（yfinance）日频 OHLCV；按当前交易日复权。
- **市值（mkt_cap）**：用 Yahoo `sharesOutstanding`（当前值）× 历史价格近似得到。
  **不等价于交易所历史自由流通市值**，仅适合机制分析与课程汇报。
- **换手率（turnover）**：volume / shares_outstanding 近似，没有过滤大宗 / 协议交易。
- **基准（benchmark_ret）**：CN 用 CSI300 指数收益，US 用 S&P 500 指数收益（`benchmarks.csv`）。

## 2. 事件清单

- **CN 事件**：中证指数公司官方调整公告 PDF + 公开新闻补充转录（`source` 列记录来源）。
- **US 事件**：维基百科 S&P 500 成分股表 + S&P Dow Jones 官方脚注。
- **未覆盖**：增发 / 分拆事件、内部技术性调整（如 ticker 变更）。

## 3. 被动 AUM（H2 假说）

- **来源**：Federal Reserve Z.1 系列（`BOGZ1FL564090005A`，US ETF Total Financial Assets）。
- **样本**：仅 12 个年度观测（2010-2025），结构性 underpower，不能支持时间序列层面的强结论。
- **缺口**：CN passive AUM 没有等价口径；用作 H2 对照需重做匹配。

## 4. HS300 RDD 数据层级

- **L3（官方）**：仅 2023-05 到 2025-11 共 6 个批次，159 行。详见 `docs/hs300_rdd_data_contract.md`。
- **L2（公开重建）**：1887 行；从公开调整新闻反推，**不等价于中证官方历史排名**。
- **L1（演示）**：合成数据，仅供 pipeline 测试。
- **当前主表使用**：默认 L3；缺失时返回 `missing` 状态而非自动降级。
- **若要支持论文级因果声明**：需扩展 L3 到 ≥10 年并补 McCrary 操纵性检验。

## 5. 事件研究方法

- **AR 计算**：`ar = ret − benchmark_ret`（简单市场调整）；没有市场模型 β 估计。
- **σ 估计**：默认从 panel 内 `[-window_pre, -2]` 区间估计 (window_pre 默认 20)；
  这是 18 日的 in-panel proxy estimation window，比文献标准（120-250 日）短。
- **标准化**：`compute_event_study_patell` 在简单 t 之外提供 Patell t 与 BMP t；
  在样本量充足时建议优先看 BMP（不假设零相关）。

## 6. 多重检验

- **当前阈值**：决定层 p<0.10（默认）；输出层附 Bonferroni 与 Benjamini-Hochberg q-value。
- **Pre-registration**：本项目的 7 假说 **post-hoc** 拟合自数据，**未做 Pre-Analysis Plan 公开**。
  下一轮研究迭代前再做出 verdict 修订时建议先冻结假说与阈值，详见 `docs/verdict_iteration.md`。

## 7. CMA 假说证据强度分层

- **核心假说（core, n 充足）**：H1、H5、H7。
- **附录假说（supplementary, n 受限）**：H2 (n=12)、H3 (n=4 象限)、H4 (gap drift 回归)、H6 (n=67)。
  `cma_hypothesis_verdicts.csv` 的 `evidence_tier` 列记录该分层；论文主表只引用 core，supplementary 走附录。

## 8. 何时不要用本项目结论

- 若需要交易所自由流通市值精确口径 → 不要用 Yahoo `mkt_cap`。
- 若需要中证官方历史排名因果识别 → L3 数据不足以前不要用。
- 若需要长窗口 [0,+120] 退化效应 → 样本严重缩水，仅作探索性。
- 若需要时间序列 AUM 推断 → n=12 不够，结论以方向参考为主。
```

- [ ] **Step 2.2 — Add link in README "限制" section**

Insert into `README.md` after "## 研究当前结论速览" or near "## 备注":

```markdown
## 数据与方法限制

完整限制清单见 [docs/limitations.md](docs/limitations.md)。关键提醒：

- `mkt_cap` / `turnover` 来自 Yahoo `sharesOutstanding` 近似，不是交易所历史口径。
- HS300 RDD 当前 L3 覆盖 2023-05 到 2025-11 共 6 个批次、159 条候选，论文级因果声明需扩展到 ≥10 年。
- 7 条 CMA 假说为 post-hoc 拟合，未做 pre-registration；阈值 sweep 见 `docs/sensitivity_workflow.md`。
- 假说证据强度分层：`core`（H1/H5/H7）vs `supplementary`（H2/H3/H4/H6），见 `cma_hypothesis_verdicts.csv` 的 `evidence_tier` 列。
```

- [ ] **Step 2.3 — Commit**

```bash
git add docs/limitations.md README.md
git commit -m "docs: centralize limitations & data caveats"
```

---

### Task 3 — Document post-hoc disclosure

**Files:** Modify `docs/paper_outline_verdicts.md`, `docs/verdict_iteration.md`

- [ ] **Step 3.1 — Add disclosure block to `docs/paper_outline_verdicts.md`**

Insert near top, after the title:

```markdown
## Disclosure: post-hoc, not pre-registered

The 7 CMA hypotheses (H1–H7) below were formulated **after** observing the
announce-vs-effective asymmetry in the main event study. They are **post-hoc**
explanations: there is no Pre-Analysis Plan (PAP) deposited prior to data analysis.

Implications:
- Verdict thresholds (default p<0.10 inner=0.05) are reasonable but not pre-committed.
- Multiple-testing corrections (Bonferroni, Benjamini-Hochberg) are reported in
  `cma_hypothesis_verdicts.csv` but were applied after hypothesis selection.
- Sample-size limitations are honest data constraints (e.g. H2 n=12 is from
  Federal Reserve Z.1 annual data); no data was excluded after seeing results.

Future iterations should freeze hypotheses + thresholds before re-running the
pipeline; see `docs/verdict_iteration.md` for the verdict-diff workflow that
supports this.
```

- [ ] **Step 3.2 — Append same disclosure (shorter) to `docs/verdict_iteration.md`**

```markdown
## Pre-registration status

The 7 CMA hypotheses are post-hoc as of 2026-05. The verdict-diff workflow in
this document is designed to support **pre-registered** future iterations:

1. Freeze hypothesis text + p-threshold + sample definition in a tagged commit.
2. Run `index-inclusion-cma` and snapshot `cma_hypothesis_verdicts.csv` to
   `cma_hypothesis_verdicts.previous.csv`.
3. Use `index-inclusion-verdict-summary --compare-with <previous>` to surface
   directional changes only — verdict text and thresholds stay constant.

Deviation from pre-registered design must be logged in this document with date
and rationale.
```

- [ ] **Step 3.3 — Commit**

```bash
git add docs/paper_outline_verdicts.md docs/verdict_iteration.md
git commit -m "docs: explicit post-hoc disclosure for CMA hypotheses"
```

---

### Task 4 — Strengthen doctor strict mode

**Files:** Modify `Makefile`

- [ ] **Step 4.1 — Add `make doctor-strict` target**

Edit the `.PHONY:` line to include `doctor-strict`, then append a new target:

```make
doctor-strict: ## Run project health checks with --fail-on-warn (CI-strict)
	index-inclusion-doctor --fail-on-warn
```

- [ ] **Step 4.2 — Verify Makefile help**

```bash
make help | grep doctor
```

Expected output includes both `doctor` and `doctor-strict` lines.

- [ ] **Step 4.3 — Commit**

```bash
git add Makefile
git commit -m "chore: add make doctor-strict target"
```

---

### Task 5 — Implement Patell/BMP standardized abnormal returns

**Files:** Create `tests/test_event_study_patell.py`; modify `src/index_inclusion_research/analysis/event_study.py`

**Background:** Existing `compute_event_study` returns simple t. Patell (1976) standardizes each AR by σ_i estimated from a pre-event estimation window and aggregates:
- `Z_Patell = sum(SAR_it) / sqrt(N)` with `SAR_it = AR_it / σ_i`.
- `Z_BMP = mean(SCAR_i) / (std(SCAR_i)/sqrt(N))` (Boehmer/Musumeci/Poulsen 1991, robust to event-induced variance).

We use the in-panel `[-window_pre, -2]` range as a proxy estimation window (limitation documented in `docs/limitations.md`).

- [ ] **Step 5.1 — Write failing test**

```python
# tests/test_event_study_patell.py
from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis.event_study import (
    compute_patell_bmp_summary,
)


def _make_panel(n_events: int = 30, *, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for event_id in range(n_events):
        for relative_day in range(-20, 21):
            base = rng.normal(scale=0.01)
            shock = 0.03 if relative_day == 0 else 0.0
            rows.append(
                {
                    "event_id": f"e{event_id:03d}",
                    "event_phase": "announce",
                    "market": "TEST",
                    "inclusion": 1,
                    "relative_day": relative_day,
                    "ar": base + shock,
                }
            )
    return pd.DataFrame(rows)


def test_patell_bmp_runs_and_has_expected_columns() -> None:
    panel = _make_panel()
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1), (-3, 3)],
        estimation_window=(-20, -2),
    )
    expected = {
        "market",
        "event_phase",
        "inclusion",
        "window",
        "n_events",
        "patell_z",
        "patell_p",
        "bmp_t",
        "bmp_p",
    }
    assert expected.issubset(summary.columns)
    assert (summary["n_events"] > 0).all()


def test_patell_detects_event_day_shock() -> None:
    panel = _make_panel(n_events=50, seed=42)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    # Event-day +3% shock should produce a strongly positive Patell Z
    row = summary.iloc[0]
    assert row["patell_z"] > 3.0
    assert row["patell_p"] < 0.01


def test_patell_handles_short_estimation_window() -> None:
    panel = _make_panel(n_events=10)
    # Reasonable: 5-day estimation window
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-6, -2),
    )
    assert not summary.empty
    assert summary["n_events"].iloc[0] == 10


def test_patell_skips_events_with_zero_estimation_variance() -> None:
    rows: list[dict[str, object]] = []
    # Constant AR -> sigma=0; should be excluded
    for event_id in range(5):
        for relative_day in range(-20, 21):
            rows.append(
                {
                    "event_id": f"const{event_id}",
                    "event_phase": "announce",
                    "market": "TEST",
                    "inclusion": 1,
                    "relative_day": relative_day,
                    "ar": 0.0,
                }
            )
    panel = pd.DataFrame(rows)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    # All sigmas are zero, so n_events post-filter is 0 -> empty / NaN row
    if not summary.empty:
        assert summary["n_events"].iloc[0] == 0
```

- [ ] **Step 5.2 — Run test, verify it fails**

```bash
pytest tests/test_event_study_patell.py -v
```

Expected: `ImportError` on `compute_patell_bmp_summary`.

- [ ] **Step 5.3 — Implement `compute_patell_bmp_summary`**

Append to `src/index_inclusion_research/analysis/event_study.py`:

```python
def compute_patell_bmp_summary(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
    *,
    estimation_window: tuple[int, int] = (-20, -2),
    group_columns: tuple[str, ...] = ("market", "event_phase", "inclusion"),
) -> pd.DataFrame:
    """Patell (1976) and BMP (1991) standardized event-study test.

    Standardizes each abnormal return by sigma estimated from an in-panel
    estimation window. Returns one row per (group_columns, window).

    Note: estimation_window defaults to [-20, -2] (an in-panel proxy);
    the literature standard is [-250, -21]. See docs/limitations.md.
    """
    if panel.empty or "ar" not in panel.columns:
        return pd.DataFrame()
    if "relative_day" not in panel.columns:
        return pd.DataFrame()

    work = panel.copy()
    if "treatment_group" in work.columns:
        work = work.loc[work["treatment_group"] == 1]
    if work.empty:
        return pd.DataFrame()

    est_lo, est_hi = estimation_window
    if est_hi < est_lo:
        raise ValueError("estimation_window must be (low, high) with low <= high")

    est_mask = work["relative_day"].between(est_lo, est_hi, inclusive="both")
    est_window = work.loc[est_mask, ["event_id", "event_phase", "ar"]].dropna(subset=["ar"])
    if est_window.empty:
        return pd.DataFrame()
    sigma_per_event = (
        est_window.groupby(["event_id", "event_phase"], dropna=False)["ar"]
        .std(ddof=1)
        .rename("sigma_estimation")
        .reset_index()
    )
    n_per_event = (
        est_window.groupby(["event_id", "event_phase"], dropna=False)["ar"]
        .count()
        .rename("n_estimation")
        .reset_index()
    )

    summary_rows: list[dict[str, object]] = []
    windows = _normalise_windows(car_windows)

    for window in windows:
        win_mask = work["relative_day"].between(window.start, window.end, inclusive="both")
        window_frame = work.loc[win_mask].copy()
        if window_frame.empty:
            continue

        car_per_event = (
            window_frame.groupby(["event_id", "event_phase"], dropna=False)
            .agg(
                car=("ar", "sum"),
                window_obs=("ar", "count"),
                **{
                    col: (col, "first")
                    for col in group_columns
                    if col in window_frame.columns
                },
            )
            .reset_index()
        )

        merged = car_per_event.merge(
            sigma_per_event, on=["event_id", "event_phase"], how="left"
        ).merge(n_per_event, on=["event_id", "event_phase"], how="left")
        valid = merged.dropna(subset=["sigma_estimation"])
        valid = valid.loc[valid["sigma_estimation"] > 0].copy()

        # Patell standardization: SCAR_i = CAR_i / (sigma_i * sqrt(L_window))
        window_length = max(window.end - window.start + 1, 1)
        valid["scar"] = valid["car"] / (valid["sigma_estimation"] * np.sqrt(window_length))

        for keys, group in valid.groupby(list(group_columns), dropna=False):
            n = len(group)
            row: dict[str, object] = dict(zip(group_columns, keys, strict=False))
            row["window"] = window.label
            row["window_slug"] = window.slug
            row["n_events"] = n
            if n < 1:
                row.update(
                    {
                        "patell_z": np.nan,
                        "patell_p": np.nan,
                        "bmp_t": np.nan,
                        "bmp_p": np.nan,
                    }
                )
                summary_rows.append(row)
                continue

            scar_values = group["scar"].to_numpy()

            # Patell Z assumes SCAR_i ~ N(0, 1) under H0 -> Z = sum / sqrt(N)
            patell_z = float(scar_values.sum() / np.sqrt(n))
            patell_p = float(2.0 * stats.norm.sf(abs(patell_z)))

            # BMP cross-sectional t: mean(SCAR) / (std(SCAR)/sqrt(N))
            if n > 1:
                bmp_t_stat, bmp_p_val = stats.ttest_1samp(scar_values, popmean=0.0, nan_policy="omit")
                bmp_t = float(bmp_t_stat)
                bmp_p = float(bmp_p_val)
            else:
                bmp_t = np.nan
                bmp_p = np.nan

            row.update(
                {
                    "patell_z": patell_z,
                    "patell_p": patell_p,
                    "bmp_t": bmp_t,
                    "bmp_p": bmp_p,
                    "mean_scar": float(np.mean(scar_values)),
                    "std_scar": float(np.std(scar_values, ddof=1)) if n > 1 else np.nan,
                }
            )
            summary_rows.append(row)

    return pd.DataFrame(summary_rows)
```

- [ ] **Step 5.4 — Run tests**

```bash
pytest tests/test_event_study_patell.py -v
```

Expected: 4 tests pass.

- [ ] **Step 5.5 — Wire into `run_event_study.py` to write `patell_bmp_summary.csv`**

Inspect existing `run_event_study.py` flow; add a call to `compute_patell_bmp_summary` and write to `results/event_study/patell_bmp_summary.csv` (and `results/real_event_study/...` for the real flow). Add a doctor check ensuring file exists.

- [ ] **Step 5.6 — Run full test suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 5.7 — Commit**

```bash
git add tests/test_event_study_patell.py src/index_inclusion_research/analysis/event_study.py src/index_inclusion_research/run_event_study.py
git commit -m "feat: add Patell and BMP standardized event-study tests"
```

---

### Task 6 — Implement McCrary density discontinuity test

**Files:** Create `tests/test_rdd_mccrary.py`; modify `src/index_inclusion_research/analysis/rdd.py`

**Background:** McCrary (2008) tests whether the density of the running variable is discontinuous at the cutoff (manipulation indicator). We implement a simple histogram-based variant: compute log-density to the left and right of cutoff over a small window, and report the difference + bootstrap SE.

- [ ] **Step 6.1 — Write failing test**

```python
# tests/test_rdd_mccrary.py
from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis.rdd import compute_mccrary_density_test


def test_mccrary_no_manipulation() -> None:
    rng = np.random.default_rng(0)
    frame = pd.DataFrame({"distance_to_cutoff": rng.normal(scale=10.0, size=2000)})
    result = compute_mccrary_density_test(frame, running_col="distance_to_cutoff")
    assert "log_density_diff" in result
    assert "p_value" in result
    # No manipulation -> not significant at 0.05
    assert result["p_value"] > 0.05


def test_mccrary_detects_strong_manipulation() -> None:
    rng = np.random.default_rng(1)
    smooth = rng.normal(scale=10.0, size=1500)
    # Add 800 extra points on the right side -> sharp density jump
    extra_right = np.abs(rng.normal(scale=2.0, size=800))
    distances = np.concatenate([smooth, extra_right])
    frame = pd.DataFrame({"distance_to_cutoff": distances})
    result = compute_mccrary_density_test(frame, running_col="distance_to_cutoff", n_bootstrap=200)
    # Should detect manipulation
    assert result["log_density_diff"] > 0
    assert result["p_value"] < 0.05


def test_mccrary_returns_nan_on_empty() -> None:
    frame = pd.DataFrame({"distance_to_cutoff": []})
    result = compute_mccrary_density_test(frame, running_col="distance_to_cutoff")
    assert np.isnan(result["log_density_diff"])
    assert result["n_obs"] == 0
```

- [ ] **Step 6.2 — Run test, verify failure**

```bash
pytest tests/test_rdd_mccrary.py -v
```

Expected: `ImportError`.

- [ ] **Step 6.3 — Implement `compute_mccrary_density_test`**

Append to `src/index_inclusion_research/analysis/rdd.py`:

```python
def compute_mccrary_density_test(
    frame: pd.DataFrame,
    *,
    running_col: str = "distance_to_cutoff",
    bandwidth: float | None = None,
    bin_size: float | None = None,
    n_bootstrap: int = 500,
    seed: int = 42,
) -> dict[str, float | int]:
    """Histogram-based density discontinuity test (McCrary 2008 spirit).

    Computes log(density_right) - log(density_left) at the cutoff using local
    bin counts; bootstrap SE for p-value. Not the original triangular-kernel
    estimator, but adequate as a manipulation screen for index-inclusion RDDs.
    """
    distances = frame[running_col].dropna().astype(float).to_numpy()
    n_obs = int(distances.size)
    result: dict[str, float | int] = {
        "n_obs": n_obs,
        "bandwidth": np.nan,
        "bin_size": np.nan,
        "log_density_diff": np.nan,
        "std_error": np.nan,
        "z_stat": np.nan,
        "p_value": np.nan,
    }
    if n_obs == 0:
        return result

    sigma = float(np.std(distances, ddof=1)) if n_obs > 1 else 0.0
    if bandwidth is None:
        # IK-ish rule constrained to non-degenerate
        bandwidth = max(1.84 * sigma * (n_obs ** (-1 / 5)), 1e-6) if sigma > 0 else float(np.max(np.abs(distances)) or 1.0)
    if bin_size is None:
        bin_size = max(bandwidth / 5.0, 1e-6)

    def _density_diff(sample: np.ndarray) -> float:
        left = sample[(sample < 0) & (sample >= -bandwidth)]
        right = sample[(sample >= 0) & (sample <= bandwidth)]
        if left.size == 0 or right.size == 0:
            return float("nan")
        # Density approximation: count in [-bin_size, 0) vs [0, +bin_size]
        n_left = int(((left >= -bin_size) & (left < 0)).sum())
        n_right = int(((right >= 0) & (right < bin_size)).sum())
        if n_left == 0 or n_right == 0:
            return float("nan")
        density_left = n_left / (bin_size * sample.size)
        density_right = n_right / (bin_size * sample.size)
        return float(np.log(density_right) - np.log(density_left))

    point = _density_diff(distances)
    if not np.isfinite(point):
        result["bandwidth"] = float(bandwidth)
        result["bin_size"] = float(bin_size)
        return result

    rng = np.random.default_rng(seed)
    boot = np.empty(n_bootstrap, dtype=float)
    for i in range(n_bootstrap):
        sample = rng.choice(distances, size=n_obs, replace=True)
        boot[i] = _density_diff(sample)
    boot = boot[np.isfinite(boot)]
    if boot.size < 5:
        return result

    se = float(np.std(boot, ddof=1))
    z = float(point / se) if se > 0 else float("nan")
    p = float(2.0 * stats.norm.sf(abs(z))) if np.isfinite(z) else float("nan")

    result.update(
        {
            "bandwidth": float(bandwidth),
            "bin_size": float(bin_size),
            "log_density_diff": float(point),
            "std_error": se,
            "z_stat": z,
            "p_value": p,
        }
    )
    return result
```

(Add `from scipy import stats` import if not already present.)

- [ ] **Step 6.4 — Run tests**

```bash
pytest tests/test_rdd_mccrary.py -v
```

Expected: 3 tests pass.

- [ ] **Step 6.5 — Wire into `hs300_rdd.py` workflow**

Add a call to `compute_mccrary_density_test` in the HS300 RDD orchestration so that `mccrary_density_test.csv` gets written next to `rdd_summary.csv`. Add a `doctor` check that the file exists when L3 status is `ready`.

- [ ] **Step 6.6 — Commit**

```bash
git add tests/test_rdd_mccrary.py src/index_inclusion_research/analysis/rdd.py src/index_inclusion_research/hs300_rdd.py
git commit -m "feat: add McCrary-style density discontinuity test for HS300 RDD"
```

---

### Task 7 — Triage CMA hypotheses into core vs supplementary

**Files:** Modify the verdicts assembly module and tests.

- [ ] **Step 7.1 — Locate verdict assembly**

```bash
grep -rn "cma_hypothesis_verdicts" /Users/leonardodon/index-inclusion-research/src/ | head
```

The verdicts CSV is built in `src/index_inclusion_research/analysis/cross_market_asymmetry/verdicts/_h_functions.py` or related orchestrator. Find the function that produces the rows.

- [ ] **Step 7.2 — Add `evidence_tier` per hypothesis**

Define a constant mapping near the top of the verdicts module:

```python
EVIDENCE_TIER: dict[str, str] = {
    "H1": "core",
    "H2": "supplementary",
    "H3": "supplementary",
    "H4": "supplementary",
    "H5": "core",
    "H6": "supplementary",
    "H7": "core",
}
```

Add the column when assembling each row: `row["evidence_tier"] = EVIDENCE_TIER.get(hid, "supplementary")`.

- [ ] **Step 7.3 — Add a unit test to fix the tier mapping**

```python
# tests/test_verdict_evidence_tier.py
from __future__ import annotations

import pandas as pd

from index_inclusion_research.paths import results_dir


def test_evidence_tier_in_verdicts_csv() -> None:
    path = results_dir() / "real_tables" / "cma_hypothesis_verdicts.csv"
    if not path.exists():
        # Pipeline may not have been run in this checkout
        return
    df = pd.read_csv(path)
    assert "evidence_tier" in df.columns, "Run index-inclusion-cma to refresh verdicts"
    assert set(df["evidence_tier"].unique()).issubset({"core", "supplementary"})
    core = set(df.loc[df["evidence_tier"] == "core", "hid"])
    assert {"H1", "H5", "H7"} <= core
    supplementary = set(df.loc[df["evidence_tier"] == "supplementary", "hid"])
    assert {"H2", "H3", "H4", "H6"} <= supplementary
```

- [ ] **Step 7.4 — Re-run CMA and verify CSV**

```bash
index-inclusion-cma
head -2 results/real_tables/cma_hypothesis_verdicts.csv | tr ',' '\n' | grep evidence_tier
```

Expected: `evidence_tier` listed among columns.

- [ ] **Step 7.5 — Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry tests/test_verdict_evidence_tier.py results/real_tables/cma_hypothesis_verdicts.csv
git commit -m "feat: add evidence_tier column to CMA verdicts (core vs supplementary)"
```

---

### Task 8 — HS300 RDD L3 coverage audit

**Files:** Create `docs/hs300_rdd_l3_collection_audit.md`

- [ ] **Step 8.1 — Inventory current L3 coverage**

```bash
awk -F, 'NR>1 {print $1}' data/raw/hs300_rdd_candidates.csv | sort -u
```

Records the list of `batch_id` values present.

- [ ] **Step 8.2 — Inspect scraper module**

```bash
grep -n "def \|class " src/index_inclusion_research/hs300_rdd_online_sources.py | head
```

Lists scraper entry points (which official URLs / PDFs it can fetch).

- [ ] **Step 8.3 — Write the audit document**

Create `docs/hs300_rdd_l3_collection_audit.md` with:

```markdown
# HS300 RDD L3 数据收集审计

## 当前 L3 覆盖

`data/raw/hs300_rdd_candidates.csv` 当前包含以下批次：

<insert batch list from Step 8.1>

总行数：159；时间跨度：2023-05 ~ 2025-11。

## 论文目标覆盖

支持论文级 RDD 因果推断需要：≥10 年（约 20 个批次），覆盖 2014-2024。
当前缺口约 14 个批次（2014-2022）。

## 可用收集路径

1. `src/index_inclusion_research/hs300_rdd_online_sources.py`：
   - <insert function names from Step 8.2>
   - 当前对接的官方 URL 模式：CSIndex 调整公告附件 PDF。
   - 历史批次的 PDF URL 命名规则在 2020 之后稳定，2014-2019 需要单独排查。

2. 手工转录回路：
   - 2014-2019 部分批次只有公告新闻，没有 attachment PDF；需要从 CNInfo
     或 CSIndex 的"重要事项"档案中翻找。

## 推荐执行顺序

1. 跑 `index-inclusion-prepare-hs300-rdd --scrape` 把 2020-2022 的 PDF 抓全。
2. 用 `data/raw/hs300_rdd_candidates.template.csv` 模板手工填 2014-2019 共 ~10 个批次。
3. 每补一个批次跑一次 `index-inclusion-doctor`，确保 schema 对齐。
4. 等总样本 ≥10 年、≥1500 行后切到 L3 主表（移除当前 L3-fallback 警告）。

## 风险与限制

- CSIndex 历史公告 PDF 经过若干次站点改版，部分链接已 404；需要用 archive.org snapshot。
- 中证早期使用的"新增/剔除"列表口径与 2020 之后不完全一致；schema 字段映射需要逐批校对。
- 即便补全，L3 仍是"官方调整名单序"重建，不是 ranking score 本体；论文表达时要标注。

## 当前论文层面的临时定位

在 L3 ≥10 年补全之前：
- HS300 RDD 结果在论文中作为 illustrative / preliminary 呈现，不作主表。
- 主要识别策略仍是 announce vs effective 事件研究 + matched DiD。
- 完整方法论限制见 `docs/limitations.md`。
```

- [ ] **Step 8.4 — Commit**

```bash
git add docs/hs300_rdd_l3_collection_audit.md
git commit -m "docs: audit HS300 RDD L3 coverage and collection plan"
```

---

## Self-Review Checklist

- [ ] All 8 tasks have at least one concrete file change with a commit step.
- [ ] No "TODO/TBD" placeholders.
- [ ] Methodology tasks (5, 6) have working test code, not just descriptions.
- [ ] Documentation tasks (1, 2, 3, 8) have the actual prose to insert.
- [ ] Type and method names referenced in later tasks (`compute_patell_bmp_summary`, `compute_mccrary_density_test`, `EVIDENCE_TIER`) are defined in the corresponding implementation tasks.
- [ ] Commit messages follow project convention (`feat:`, `docs:`, `chore:`).

---

## Execution Notes

- After Task 5 wiring (5.5) and Task 6 wiring (6.5), re-run `make rebuild` to refresh `results/`. If `make rebuild` is too expensive, run only `index-inclusion-cma` and `index-inclusion-run-event-study` for the new outputs.
- Final step: `make lint && make test && make doctor`; commit any incidental changes that came out of regenerating outputs.
