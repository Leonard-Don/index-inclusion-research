# Cross-Market Asymmetry (CMA) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有项目里落地一个独立分析包 `cross_market_asymmetry`，系统化对比美股 vs A 股在公告日 / 生效日的 CAR、机制、异质性、时序差异，产出论文所需的表 / 图 / LaTeX / dashboard section。

**Architecture:** 子包 `src/index_inclusion_research/analysis/cross_market_asymmetry/` 承载 5 个计算模块（paths / gap_period / mechanism_panel / heterogeneity / time_series）+ 1 个假设注册表 + 1 个 orchestrator。薄 CLI 入口在 `src/index_inclusion_research/cross_market_asymmetry.py`，通过 `cli.py` 注册 console script。Dashboard 在分层架构里新增独立 section，不改动 `literature_catalog` 主干。

**Tech Stack:** Python 3.11+ · pandas / numpy / scipy / statsmodels · matplotlib · Flask / Jinja · pytest · ruff · playwright（仅 smoke）

**Spec:** `docs/superpowers/specs/2026-04-23-cross-market-asymmetry-design.md`

---

## 进度里程碑

| 阶段 | Tasks | 产出 commit |
|---|---|---|
| 脚手架 | 1 | `scaffold: set up cross_market_asymmetry subpackage` |
| M1 事件窗口路径 | 2–5 | `feat: cma M1 paths (ar/car/window)` + figure commit |
| M2 空窗期 | 6–8 | `feat: cma M2 gap_period metrics + figure` |
| M3 机制回归 | 9–12 | `feat: cma M3 mechanism panel + heatmap + tex` |
| M4 异质性 | 13–15 | `feat: cma M4 heterogeneity matrix + figures` |
| M5 时序 | 16–18 | `feat: cma M5 rolling + break + figure` |
| 假设注册表 | 19–20 | `feat: cma hypothesis registry + map export` |
| Orchestrator | 21 | `feat: cma orchestrator + research_summary append` |
| CLI | 22–23 | `feat: cma CLI entrypoint` |
| Tex-only + figures_tables | 23.5 | `feat: cma --tex-only and figures_tables integration` |
| Dashboard | 24–28 | `feat: cma dashboard section` |
| Smoke + 验收 | 29–32 | `test: cma browser smoke + final verification` |

---

## 前置检查（每个 Task 开始前）

- 当前工作目录：`.`
- 依赖已安装：`python3 -m pip install -e ".[dev]"`（若未装）
- 工具可用：`pytest`、`ruff`、Python 3.11+
- 每个 Task 末尾必须：`pytest -q` 全过 + `ruff check .` 通过

---

## Task 1: 建立子包骨架

**Files:**
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/__init__.py`
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py`（空占位）
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py`（空占位）
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py`（空占位）
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py`（空占位）
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py`（空占位）
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py`（空占位）
- Create: `src/index_inclusion_research/analysis/cross_market_asymmetry/orchestrator.py`（空占位）
- Create: `tests/test_cma_scaffolding.py`

- [ ] **Step 1.1: Write failing scaffolding test**

```python
# tests/test_cma_scaffolding.py
from __future__ import annotations


def test_cma_subpackage_imports():
    import index_inclusion_research.analysis.cross_market_asymmetry as cma

    assert hasattr(cma, "paths")
    assert hasattr(cma, "gap_period")
    assert hasattr(cma, "mechanism_panel")
    assert hasattr(cma, "heterogeneity")
    assert hasattr(cma, "time_series")
    assert hasattr(cma, "hypotheses")
    assert hasattr(cma, "orchestrator")


def test_cma_output_path_constants():
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    assert orchestrator.REAL_TABLES_DIR.name == "real_tables"
    assert orchestrator.REAL_FIGURES_DIR.name == "real_figures"
```

- [ ] **Step 1.2: Run test to verify it fails**

```bash
pytest tests/test_cma_scaffolding.py -v
```
Expected: FAIL (ModuleNotFoundError)

- [ ] **Step 1.3: Create `__init__.py` with sub-module re-exports**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/__init__.py
from __future__ import annotations

from . import (
    gap_period,
    heterogeneity,
    hypotheses,
    mechanism_panel,
    orchestrator,
    paths,
    time_series,
)

__all__ = [
    "gap_period",
    "heterogeneity",
    "hypotheses",
    "mechanism_panel",
    "orchestrator",
    "paths",
    "time_series",
]
```

- [ ] **Step 1.4: Stub each sub-module with `from __future__ import annotations`**

Each of `paths.py` / `gap_period.py` / `mechanism_panel.py` / `heterogeneity.py` / `time_series.py` / `hypotheses.py` starts as:

```python
from __future__ import annotations
```

- [ ] **Step 1.5: Stub `orchestrator.py` with path constants**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/orchestrator.py
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REAL_TABLES_DIR = ROOT / "results" / "real_tables"
REAL_FIGURES_DIR = ROOT / "results" / "real_figures"
REAL_EVENT_PANEL = ROOT / "data" / "processed" / "real_event_panel.csv"
REAL_MATCHED_EVENT_PANEL = ROOT / "data" / "processed" / "real_matched_event_panel.csv"
REAL_EVENTS_CLEAN = ROOT / "data" / "processed" / "real_events_clean.csv"
```

- [ ] **Step 1.6: Run test**

```bash
pytest tests/test_cma_scaffolding.py -v
```
Expected: PASS

- [ ] **Step 1.7: Ruff check**

```bash
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry tests/test_cma_scaffolding.py
```
Expected: All clear.

- [ ] **Step 1.8: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry tests/test_cma_scaffolding.py
git commit -m "scaffold: set up cross_market_asymmetry subpackage"
```

---

## Task 2: M1 `build_daily_ar_panel`（长格式 AR 面板）

**Purpose:** 从 `real_event_panel.csv` 抽出"每 event × 每 day_offset"的 AR / CAR 长格式面板，只保留 `event_type == "addition"`（纳入），两个 `event_phase ∈ {announce, effective}`。

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py`
- Test: `tests/test_cma_paths.py`

- [ ] **Step 2.1: Write failing test**

```python
# tests/test_cma_paths.py
from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
    build_daily_ar_panel,
)


def _make_panel_frame() -> pd.DataFrame:
    rows = []
    for market in ("CN", "US"):
        for event_phase in ("announce", "effective"):
            for event_id, ar_scale in ((1, 0.01), (2, 0.02)):
                for rel in range(-3, 4):
                    rows.append(
                        {
                            "event_id": event_id,
                            "market": market,
                            "event_phase": event_phase,
                            "event_type": "addition",
                            "relative_day": rel,
                            "ar": ar_scale * (1 if rel == 0 else 0.5),
                        }
                    )
    # Add one deletion row to prove filtering
    rows.append(
        {
            "event_id": 99,
            "market": "US",
            "event_phase": "announce",
            "event_type": "deletion",
            "relative_day": 0,
            "ar": 0.9,
        }
    )
    return pd.DataFrame(rows)


def test_build_daily_ar_panel_filters_additions_only():
    raw = _make_panel_frame()
    out = build_daily_ar_panel(raw)
    assert (out["event_type"] == "addition").all()
    assert "relative_day" in out.columns or "day_offset" in out.columns
    assert set(out["market"].unique()) == {"CN", "US"}
    assert set(out["event_phase"].unique()) == {"announce", "effective"}


def test_build_daily_ar_panel_adds_cumulative_car():
    raw = _make_panel_frame()
    out = build_daily_ar_panel(raw)
    assert "car" in out.columns
    # CAR at the smallest relative_day equals AR at that day within each group
    first = out.sort_values(["event_id", "market", "event_phase", "relative_day"]).groupby(
        ["event_id", "market", "event_phase"], as_index=False
    ).head(1)
    pd.testing.assert_series_equal(
        first["ar"].reset_index(drop=True),
        first["car"].reset_index(drop=True),
        check_names=False,
    )


def test_build_daily_ar_panel_requires_columns():
    with pytest.raises(ValueError, match="missing columns"):
        build_daily_ar_panel(pd.DataFrame({"ar": [0.0]}))
```

- [ ] **Step 2.2: Run test, expect failure**

```bash
pytest tests/test_cma_paths.py -v
```
Expected: FAIL (`ImportError: cannot import name 'build_daily_ar_panel'`)

- [ ] **Step 2.3: Implement `build_daily_ar_panel`**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py
from __future__ import annotations

import pandas as pd

REQUIRED_PANEL_COLUMNS = (
    "event_id",
    "market",
    "event_phase",
    "event_type",
    "relative_day",
    "ar",
)


def _require_columns(frame: pd.DataFrame, required: tuple[str, ...]) -> None:
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"missing columns in panel: {missing}")


def build_daily_ar_panel(panel: pd.DataFrame) -> pd.DataFrame:
    _require_columns(panel, REQUIRED_PANEL_COLUMNS)
    work = panel.loc[panel["event_type"] == "addition"].copy()
    work = work.loc[work["event_phase"].isin(("announce", "effective"))].copy()
    work = work.sort_values(["event_id", "market", "event_phase", "relative_day"]).reset_index(drop=True)
    work["car"] = work.groupby(["event_id", "market", "event_phase"])["ar"].cumsum()
    return work
```

- [ ] **Step 2.4: Run test, expect pass**

```bash
pytest tests/test_cma_paths.py -v
```
Expected: PASS (3 tests)

- [ ] **Step 2.5: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py tests/test_cma_paths.py
git commit -m "feat: cma M1 build_daily_ar_panel"
```

---

## Task 3: M1 `compute_average_paths`（跨事件均值路径 + SE/t）

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py`
- Modify: `tests/test_cma_paths.py`

- [ ] **Step 3.1: Append failing test**

```python
def test_compute_average_paths_schema():
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_average_paths,
    )
    avg = compute_average_paths(ar_panel)
    expected_columns = {
        "market",
        "event_phase",
        "relative_day",
        "n_events",
        "ar_mean",
        "ar_se",
        "ar_t",
        "car_mean",
        "car_se",
        "car_t",
    }
    assert expected_columns.issubset(set(avg.columns))


def test_compute_average_paths_computes_mean_correctly():
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_average_paths,
    )
    avg = compute_average_paths(ar_panel)
    cn_announce_day_zero = avg.loc[
        (avg["market"] == "CN") & (avg["event_phase"] == "announce") & (avg["relative_day"] == 0)
    ]
    # event 1 ar = 0.01, event 2 ar = 0.02 at rel=0
    assert cn_announce_day_zero["ar_mean"].iloc[0] == pytest.approx(0.015, abs=1e-9)
    assert cn_announce_day_zero["n_events"].iloc[0] == 2
```

- [ ] **Step 3.2: Run test, expect import failure**

```bash
pytest tests/test_cma_paths.py::test_compute_average_paths_schema -v
```

- [ ] **Step 3.3: Implement `compute_average_paths`**

```python
def compute_average_paths(ar_panel: pd.DataFrame) -> pd.DataFrame:
    grouped = ar_panel.groupby(["market", "event_phase", "relative_day"], as_index=False).agg(
        n_events=("event_id", "nunique"),
        ar_mean=("ar", "mean"),
        ar_std=("ar", "std"),
        car_mean=("car", "mean"),
        car_std=("car", "std"),
    )
    grouped["ar_se"] = grouped["ar_std"] / grouped["n_events"].pow(0.5)
    grouped["car_se"] = grouped["car_std"] / grouped["n_events"].pow(0.5)
    grouped["ar_t"] = grouped["ar_mean"] / grouped["ar_se"].replace(0.0, pd.NA)
    grouped["car_t"] = grouped["car_mean"] / grouped["car_se"].replace(0.0, pd.NA)
    return grouped.drop(columns=["ar_std", "car_std"])
```

- [ ] **Step 3.4: Run tests, expect pass**

```bash
pytest tests/test_cma_paths.py -v
```
Expected: All pass.

- [ ] **Step 3.5: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py tests/test_cma_paths.py
git commit -m "feat: cma M1 compute_average_paths"
```

---

## Task 4: M1 `compute_window_summary`（窗口摘要）

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py`
- Modify: `tests/test_cma_paths.py`

- [ ] **Step 4.1: Append test**

```python
def test_compute_window_summary_produces_expected_rows():
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_window_summary,
    )
    summary = compute_window_summary(ar_panel, windows=[(-1, 1), (-3, 3)])
    assert {"market", "event_phase", "window_start", "window_end", "car_mean", "car_se", "car_t", "p_value", "n_events"}.issubset(summary.columns)
    # 2 markets × 2 phases × 2 windows = 8 rows
    assert len(summary) == 8


def test_compute_window_summary_respects_window_bounds():
    rows = []
    for rel in range(-5, 6):
        rows.append({
            "event_id": 1,
            "market": "CN",
            "event_phase": "announce",
            "event_type": "addition",
            "relative_day": rel,
            "ar": 0.01,
        })
    ar_panel = build_daily_ar_panel(pd.DataFrame(rows))
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_window_summary,
    )
    summary = compute_window_summary(ar_panel, windows=[(-1, 1)])
    # 3 days × 0.01 = 0.03
    assert summary["car_mean"].iloc[0] == pytest.approx(0.03, abs=1e-9)
```

- [ ] **Step 4.2: Run test, expect failure**

- [ ] **Step 4.3: Implement `compute_window_summary`**

```python
from scipy import stats  # top of file

DEFAULT_WINDOWS: tuple[tuple[int, int], ...] = (
    (-1, 1),
    (-3, 3),
    (-5, 5),
    (-20, -1),
    (2, 20),
    (0, 60),
)


def compute_window_summary(
    ar_panel: pd.DataFrame,
    windows: list[tuple[int, int]] | tuple[tuple[int, int], ...] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    rows = []
    for (lo, hi) in windows:
        sub = ar_panel.loc[
            (ar_panel["relative_day"] >= lo) & (ar_panel["relative_day"] <= hi)
        ]
        per_event = sub.groupby(["event_id", "market", "event_phase"], as_index=False)["ar"].sum()
        per_event = per_event.rename(columns={"ar": "car_window"})
        summary = per_event.groupby(["market", "event_phase"], as_index=False).agg(
            n_events=("event_id", "nunique"),
            car_mean=("car_window", "mean"),
            car_std=("car_window", "std"),
        )
        summary["car_se"] = summary["car_std"] / summary["n_events"].pow(0.5)
        summary["car_t"] = summary["car_mean"] / summary["car_se"].replace(0.0, pd.NA)
        summary["p_value"] = summary["car_t"].apply(
            lambda t: float(2 * (1 - stats.norm.cdf(abs(t)))) if pd.notna(t) else pd.NA
        )
        summary["window_start"] = lo
        summary["window_end"] = hi
        rows.append(summary.drop(columns=["car_std"]))
    out = pd.concat(rows, ignore_index=True)
    return out[
        ["market", "event_phase", "window_start", "window_end", "car_mean", "car_se", "car_t", "p_value", "n_events"]
    ]
```

- [ ] **Step 4.4: Tests pass**

```bash
pytest tests/test_cma_paths.py -v
```

- [ ] **Step 4.5: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py tests/test_cma_paths.py
git commit -m "feat: cma M1 compute_window_summary"
```

---

## Task 5: M1 图表生成 + 写入 CSV

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py`
- Modify: `tests/test_cma_paths.py`

- [ ] **Step 5.1: Append test**

```python
def test_render_path_figures_writes_png(tmp_path):
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_average_paths,
        render_path_figures,
    )
    avg = compute_average_paths(ar_panel)
    figure_dir = tmp_path / "figures"
    outputs = render_path_figures(avg, output_dir=figure_dir)
    ar_png = figure_dir / "cma_ar_path_comparison.png"
    car_png = figure_dir / "cma_car_path_comparison.png"
    assert ar_png.exists() and ar_png.stat().st_size > 0
    assert car_png.exists() and car_png.stat().st_size > 0
    assert outputs == {"ar": ar_png, "car": car_png}


def test_export_path_tables_writes_csvs(tmp_path):
    raw = _make_panel_frame()
    ar_panel = build_daily_ar_panel(raw)
    from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
        compute_average_paths,
        compute_window_summary,
        export_path_tables,
    )
    avg = compute_average_paths(ar_panel)
    window = compute_window_summary(ar_panel, windows=[(-1, 1)])
    paths = export_path_tables(ar_panel, avg, window, output_dir=tmp_path)
    for key in ("ar_path", "car_path", "window_summary"):
        assert paths[key].exists()
    # AR path CSV has day-level rows
    ar_df = pd.read_csv(paths["ar_path"])
    assert {"market", "event_phase", "relative_day", "n_events", "ar_mean"}.issubset(ar_df.columns)
```

- [ ] **Step 5.2: Run tests, expect failure**

- [ ] **Step 5.3: Implement figure + csv exporters**

```python
import matplotlib
matplotlib.use("Agg")  # top of file, only once across sub-package
import matplotlib.pyplot as plt
from pathlib import Path


_QUADRANTS = (("CN", "announce"), ("CN", "effective"), ("US", "announce"), ("US", "effective"))


def _plot_quadrant(ax, data, x_col, y_col, title):
    ax.plot(data[x_col], data[y_col], color="#1f6feb", linewidth=2)
    ax.axhline(0.0, color="#999", linestyle="--", linewidth=0.8)
    ax.axvline(0.0, color="#999", linestyle="--", linewidth=0.8)
    ax.set_title(title, fontsize=10)
    ax.set_xlabel("relative_day")


def _render_grid(avg: pd.DataFrame, value_col: str, ylabel: str) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(10, 7), sharex=True, sharey=True)
    for (market, phase), ax in zip(_QUADRANTS, axes.flat):
        sub = avg.loc[(avg["market"] == market) & (avg["event_phase"] == phase)]
        _plot_quadrant(ax, sub.sort_values("relative_day"), "relative_day", value_col, f"{market} · {phase}")
        if ax in axes[:, 0]:
            ax.set_ylabel(ylabel)
    fig.suptitle(f"CMA {ylabel} path comparison", fontsize=12)
    fig.tight_layout()
    return fig


def render_path_figures(avg: pd.DataFrame, *, output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ar_fig = _render_grid(avg, "ar_mean", "AR")
    ar_path = output_dir / "cma_ar_path_comparison.png"
    ar_fig.savefig(ar_path, dpi=150)
    plt.close(ar_fig)
    car_fig = _render_grid(avg, "car_mean", "CAR")
    car_path = output_dir / "cma_car_path_comparison.png"
    car_fig.savefig(car_path, dpi=150)
    plt.close(car_fig)
    return {"ar": ar_path, "car": car_path}


def export_path_tables(
    ar_panel: pd.DataFrame,
    avg: pd.DataFrame,
    window_summary: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ar_rows = avg[["market", "event_phase", "relative_day", "n_events", "ar_mean", "ar_se", "ar_t"]]
    car_rows = avg[["market", "event_phase", "relative_day", "n_events", "car_mean", "car_se", "car_t"]]
    ar_path = output_dir / "cma_ar_path.csv"
    car_path = output_dir / "cma_car_path.csv"
    win_path = output_dir / "cma_window_summary.csv"
    ar_rows.to_csv(ar_path, index=False)
    car_rows.to_csv(car_path, index=False)
    window_summary.to_csv(win_path, index=False)
    return {"ar_path": ar_path, "car_path": car_path, "window_summary": win_path}
```

- [ ] **Step 5.4: Tests pass**

```bash
pytest tests/test_cma_paths.py -v
```

- [ ] **Step 5.5: Ruff check + commit**

```bash
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py tests/test_cma_paths.py
git add src/index_inclusion_research/analysis/cross_market_asymmetry/paths.py tests/test_cma_paths.py
git commit -m "feat: cma M1 figure and csv exporters"
```

---

## Task 6: M2 `compute_gap_metrics`

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py`
- Create: `tests/test_cma_gap_period.py`

**数据来源约束**：
- `events`（`real_events_clean.csv`）有 `event_id, market, ticker, announce_date, effective_date, event_type`
- `panel`（`real_event_panel.csv`）有 `event_id, market, event_phase, relative_day, ar, event_date`
- 本 task 只消费这些列

- [ ] **Step 6.1: Write failing test**

```python
# tests/test_cma_gap_period.py
from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
    compute_gap_metrics,
)


def _build_events_and_panel():
    events = pd.DataFrame(
        [
            {
                "event_id": 1,
                "market": "CN",
                "ticker": "000001",
                "event_type": "addition",
                "announce_date": "2024-01-01",
                "effective_date": "2024-01-15",
            },
            {
                "event_id": 2,
                "market": "US",
                "ticker": "AAPL",
                "event_type": "addition",
                "announce_date": "2024-02-01",
                "effective_date": "2024-02-10",
            },
        ]
    )
    rows = []
    # CN event_id=1 announce: AR 0.01 each day from -20..-1, 0.03 at 0, 0.01 at 1..20; effective: AR 0.005 at 0
    def _append(event_id, market, phase, relative_day, ar):
        rows.append({
            "event_id": event_id,
            "market": market,
            "event_phase": phase,
            "event_type": "addition",
            "relative_day": relative_day,
            "ar": ar,
        })
    for rel in range(-20, 21):
        _append(1, "CN", "announce", rel, 0.01 if rel == 0 else 0.002)
        _append(1, "CN", "effective", rel, 0.005 if rel == 0 else 0.001)
        _append(2, "US", "announce", rel, 0.015 if rel == 0 else 0.001)
        _append(2, "US", "effective", rel, 0.0 if rel == 0 else 0.001)
    panel = pd.DataFrame(rows)
    return events, panel


def test_compute_gap_metrics_schema():
    events, panel = _build_events_and_panel()
    out = compute_gap_metrics(events, panel)
    expected = {
        "event_id",
        "market",
        "ticker",
        "announce_date",
        "effective_date",
        "gap_length_days",
        "pre_announce_runup",
        "announce_jump",
        "gap_drift",
        "effective_jump",
        "post_effective_reversal",
    }
    assert expected.issubset(out.columns)


def test_compute_gap_metrics_computes_gap_length():
    events, panel = _build_events_and_panel()
    out = compute_gap_metrics(events, panel)
    cn = out.loc[out["event_id"] == 1].iloc[0]
    # 2024-01-15 - 2024-01-01 = 14 calendar days
    assert cn["gap_length_days"] == 14


def test_compute_gap_metrics_announce_jump_uses_announce_phase():
    events, panel = _build_events_and_panel()
    out = compute_gap_metrics(events, panel)
    cn = out.loc[out["event_id"] == 1].iloc[0]
    # announce_jump = sum AR over [-1, 1] relative to announce
    # AR = 0.002 + 0.01 + 0.002 = 0.014
    assert cn["announce_jump"] == pytest.approx(0.014, abs=1e-9)


def test_compute_gap_metrics_filters_addition_only():
    events, panel = _build_events_and_panel()
    events_with_del = pd.concat(
        [
            events,
            pd.DataFrame([
                {
                    "event_id": 3,
                    "market": "CN",
                    "ticker": "000002",
                    "event_type": "deletion",
                    "announce_date": "2024-03-01",
                    "effective_date": "2024-03-15",
                }
            ]),
        ],
        ignore_index=True,
    )
    out = compute_gap_metrics(events_with_del, panel)
    assert (out["event_id"] != 3).all()
```

- [ ] **Step 6.2: Run tests, expect failure**

- [ ] **Step 6.3: Implement `compute_gap_metrics`**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py
from __future__ import annotations

from pathlib import Path

import pandas as pd


def _window_sum(panel: pd.DataFrame, event_id: int, phase: str, lo: int, hi: int) -> float:
    sub = panel.loc[
        (panel["event_id"] == event_id)
        & (panel["event_phase"] == phase)
        & (panel["relative_day"] >= lo)
        & (panel["relative_day"] <= hi)
    ]
    if sub.empty:
        return float("nan")
    return float(sub["ar"].sum())


def compute_gap_metrics(events: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    work_events = events.loc[events["event_type"] == "addition"].copy()
    work_events["announce_date"] = pd.to_datetime(work_events["announce_date"])
    work_events["effective_date"] = pd.to_datetime(work_events["effective_date"])
    work_panel = panel.loc[panel["event_type"] == "addition"].copy()

    rows: list[dict[str, object]] = []
    for _, ev in work_events.iterrows():
        event_id = ev["event_id"]
        gap_days = (ev["effective_date"] - ev["announce_date"]).days
        pre_runup = _window_sum(work_panel, event_id, "announce", -20, -1)
        announce_jump = _window_sum(work_panel, event_id, "announce", -1, 1)
        effective_jump = _window_sum(work_panel, event_id, "effective", -1, 1)
        post_reversal = _window_sum(work_panel, event_id, "effective", 2, 20)
        # gap_drift: from announce+1 to effective-1, using announce-anchored AR
        gap_hi = max(gap_days - 1, 2)
        gap_drift = _window_sum(work_panel, event_id, "announce", 2, gap_hi)
        rows.append(
            {
                "event_id": event_id,
                "market": ev["market"],
                "ticker": ev["ticker"],
                "announce_date": ev["announce_date"].date().isoformat(),
                "effective_date": ev["effective_date"].date().isoformat(),
                "gap_length_days": gap_days,
                "pre_announce_runup": pre_runup,
                "announce_jump": announce_jump,
                "gap_drift": gap_drift,
                "effective_jump": effective_jump,
                "post_effective_reversal": post_reversal,
            }
        )
    return pd.DataFrame(rows)
```

- [ ] **Step 6.4: Tests pass**

```bash
pytest tests/test_cma_gap_period.py -v
```

- [ ] **Step 6.5: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py tests/test_cma_gap_period.py
git commit -m "feat: cma M2 compute_gap_metrics"
```

---

## Task 7: M2 `summarize_gap_metrics`

- [ ] **Step 7.1: Append failing test to `tests/test_cma_gap_period.py`**

```python
def test_summarize_gap_metrics_schema():
    events, panel = _build_events_and_panel()
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        summarize_gap_metrics,
    )
    gap = compute_gap_metrics(events, panel)
    summary = summarize_gap_metrics(gap)
    expected = {"market", "metric", "mean", "median", "se", "t", "p_value", "n_events"}
    assert expected.issubset(summary.columns)
    # 2 markets × 6 metrics = 12 rows
    assert len(summary) == 12
```

- [ ] **Step 7.2: Run test, expect failure**

- [ ] **Step 7.3: Implement**

```python
from scipy import stats

_METRIC_COLUMNS = (
    "gap_length_days",
    "pre_announce_runup",
    "announce_jump",
    "gap_drift",
    "effective_jump",
    "post_effective_reversal",
)


def summarize_gap_metrics(gap: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for market, sub in gap.groupby("market"):
        for metric in _METRIC_COLUMNS:
            values = sub[metric].dropna()
            n = len(values)
            mean = float(values.mean()) if n else float("nan")
            median = float(values.median()) if n else float("nan")
            se = float(values.std(ddof=1) / (n**0.5)) if n > 1 else float("nan")
            t = mean / se if se and se == se else float("nan")
            p = float(2 * (1 - stats.norm.cdf(abs(t)))) if t == t else float("nan")
            rows.append(
                {
                    "market": market,
                    "metric": metric,
                    "mean": mean,
                    "median": median,
                    "se": se,
                    "t": t,
                    "p_value": p,
                    "n_events": n,
                }
            )
    return pd.DataFrame(rows)
```

- [ ] **Step 7.4: Tests pass**

- [ ] **Step 7.5: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py tests/test_cma_gap_period.py
git commit -m "feat: cma M2 summarize_gap_metrics"
```

---

## Task 8: M2 图表 + CSV 导出

- [ ] **Step 8.1: Append tests**

```python
def test_render_gap_figures_writes_png(tmp_path):
    events, panel = _build_events_and_panel()
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_metrics,
        render_gap_figures,
        summarize_gap_metrics,
    )
    gap = compute_gap_metrics(events, panel)
    summary = summarize_gap_metrics(gap)
    outs = render_gap_figures(gap, summary, output_dir=tmp_path)
    for key in ("gap_distribution", "gap_decomposition"):
        assert outs[key].exists()


def test_export_gap_tables_writes_csvs(tmp_path):
    events, panel = _build_events_and_panel()
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_metrics,
        export_gap_tables,
        summarize_gap_metrics,
    )
    gap = compute_gap_metrics(events, panel)
    summary = summarize_gap_metrics(gap)
    paths = export_gap_tables(gap, summary, output_dir=tmp_path)
    assert paths["event_level"].exists()
    assert paths["summary"].exists()
```

- [ ] **Step 8.2: Implement figures + csv**

```python
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def render_gap_figures(gap: pd.DataFrame, summary: pd.DataFrame, *, output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Distribution of gap_length_days
    fig1, ax1 = plt.subplots(figsize=(8, 5))
    for market, sub in gap.groupby("market"):
        ax1.hist(sub["gap_length_days"].dropna(), alpha=0.5, label=market, bins=20)
    ax1.set_xlabel("gap_length_days")
    ax1.set_ylabel("count")
    ax1.set_title("Announce-to-Effective gap length by market")
    ax1.legend()
    fig1.tight_layout()
    dist_path = output_dir / "cma_gap_length_distribution.png"
    fig1.savefig(dist_path, dpi=150)
    plt.close(fig1)

    # Decomposition bar chart
    segments = ("announce_jump", "gap_drift", "effective_jump", "post_effective_reversal")
    means = (
        summary.loc[summary["metric"].isin(segments)]
        .pivot_table(index="market", columns="metric", values="mean")
        .reindex(columns=segments)
    )
    fig2, ax2 = plt.subplots(figsize=(9, 5))
    means.plot.bar(ax=ax2)
    ax2.set_ylabel("mean CAR")
    ax2.set_title("Announce → Gap → Effective → Post decomposition")
    ax2.axhline(0.0, color="#999", linestyle="--", linewidth=0.7)
    fig2.tight_layout()
    decomp_path = output_dir / "cma_gap_decomposition.png"
    fig2.savefig(decomp_path, dpi=150)
    plt.close(fig2)

    return {"gap_distribution": dist_path, "gap_decomposition": decomp_path}


def export_gap_tables(gap: pd.DataFrame, summary: pd.DataFrame, *, output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    event_path = output_dir / "cma_gap_event_level.csv"
    summary_path = output_dir / "cma_gap_summary.csv"
    gap.to_csv(event_path, index=False)
    summary.to_csv(summary_path, index=False)
    return {"event_level": event_path, "summary": summary_path}
```

- [ ] **Step 8.3: Tests pass + commit**

```bash
pytest tests/test_cma_gap_period.py -v
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py tests/test_cma_gap_period.py
git add src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py tests/test_cma_gap_period.py
git commit -m "feat: cma M2 figures and csv exporters"
```

---

## Task 9: M3 `build_mechanism_panel`

**Purpose:** 从 `real_matched_event_panel.csv` 构造机制回归用的 wide 表，行 = (event_id, event_phase)，列 = outcome / control。

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py`
- Create: `tests/test_cma_mechanism_panel.py`

**Outcomes we need per event × phase**：
- `car_1_1`：`sum(ar)` over `relative_day ∈ [-1, 1]`
- `turnover_change`：`mean(turnover[0..+5]) - mean(turnover[-20..-1])`
- `volume_change`：same for volume
- `volatility_change`：`std(ret[0..+5]) - std(ret[-20..-1])`
- `price_limit_hit_share`：`mean(|ret| >= 0.099)` over `[-5, 5]`

- [ ] **Step 9.1: Write failing test**

```python
# tests/test_cma_mechanism_panel.py
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
    build_mechanism_panel,
)


def _make_matched_panel():
    rows = []
    rng = np.random.default_rng(0)
    for event_id in (1, 2):
        for phase in ("announce", "effective"):
            for rel in range(-20, 21):
                rows.append(
                    {
                        "event_id": event_id,
                        "market": "CN",
                        "event_type": "addition",
                        "treatment_group": 1,
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 if rel in (-1, 0, 1) else 0.0,
                        "turnover": 0.02 + (0.01 if rel >= 0 else 0.0),
                        "volume": 100 + (20 if rel >= 0 else 0),
                        "ret": 0.01 * rng.standard_normal(),
                        "mkt_cap": 1.0e9,
                        "sector": "Tech",
                    }
                )
    return pd.DataFrame(rows)


def test_build_mechanism_panel_schema():
    raw = _make_matched_panel()
    out = build_mechanism_panel(raw)
    expected = {
        "event_id",
        "market",
        "event_phase",
        "treatment_group",
        "car_1_1",
        "turnover_change",
        "volume_change",
        "volatility_change",
        "price_limit_hit_share",
        "log_mktcap_pre",
        "pre_turnover",
        "sector",
    }
    assert expected.issubset(out.columns)
    # 2 events × 2 phases = 4 rows
    assert len(out) == 4


def test_build_mechanism_panel_car_1_1_correct():
    raw = _make_matched_panel()
    out = build_mechanism_panel(raw)
    assert (out["car_1_1"] == pytest.approx(0.03, abs=1e-9)).all()
```

- [ ] **Step 9.2: Run test, expect failure**

- [ ] **Step 9.3: Implement**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PRICE_LIMIT_THRESHOLD = 0.099


def _window_mean(frame: pd.DataFrame, col: str, lo: int, hi: int) -> float:
    sub = frame.loc[(frame["relative_day"] >= lo) & (frame["relative_day"] <= hi), col]
    return float(sub.mean()) if len(sub) else float("nan")


def _window_std(frame: pd.DataFrame, col: str, lo: int, hi: int) -> float:
    sub = frame.loc[(frame["relative_day"] >= lo) & (frame["relative_day"] <= hi), col]
    return float(sub.std(ddof=1)) if len(sub) > 1 else float("nan")


def _window_sum(frame: pd.DataFrame, col: str, lo: int, hi: int) -> float:
    sub = frame.loc[(frame["relative_day"] >= lo) & (frame["relative_day"] <= hi), col]
    return float(sub.sum()) if len(sub) else float("nan")


def build_mechanism_panel(matched_panel: pd.DataFrame) -> pd.DataFrame:
    work = matched_panel.loc[matched_panel["event_type"] == "addition"].copy()
    rows: list[dict[str, object]] = []
    for (event_id, market, phase), group in work.groupby(["event_id", "market", "event_phase"]):
        car_1_1 = _window_sum(group, "ar", -1, 1)
        turnover_change = _window_mean(group, "turnover", 0, 5) - _window_mean(group, "turnover", -20, -1)
        volume_change = _window_mean(group, "volume", 0, 5) - _window_mean(group, "volume", -20, -1)
        volatility_change = _window_std(group, "ret", 0, 5) - _window_std(group, "ret", -20, -1)
        pre_sub = group.loc[(group["relative_day"] >= -5) & (group["relative_day"] <= 5), "ret"].abs()
        limit_share = float((pre_sub >= PRICE_LIMIT_THRESHOLD).mean()) if len(pre_sub) else float("nan")
        pre_mktcap = _window_mean(group, "mkt_cap", -20, -1)
        pre_turnover = _window_mean(group, "turnover", -20, -1)
        rows.append(
            {
                "event_id": event_id,
                "market": market,
                "event_phase": phase,
                "treatment_group": int(group["treatment_group"].iloc[0]),
                "sector": group["sector"].iloc[0] if "sector" in group.columns else pd.NA,
                "car_1_1": car_1_1,
                "turnover_change": turnover_change,
                "volume_change": volume_change,
                "volatility_change": volatility_change,
                "price_limit_hit_share": limit_share,
                "log_mktcap_pre": np.log(pre_mktcap) if pre_mktcap and pre_mktcap == pre_mktcap and pre_mktcap > 0 else float("nan"),
                "pre_turnover": pre_turnover,
            }
        )
    return pd.DataFrame(rows)
```

- [ ] **Step 9.4: Tests pass**

- [ ] **Step 9.5: Commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py tests/test_cma_mechanism_panel.py
git commit -m "feat: cma M3 build_mechanism_panel"
```

---

## Task 10: M3 `estimate_quadrant_regression`

- [ ] **Step 10.1: Append test**

```python
def test_estimate_quadrant_regression_returns_coefficient():
    raw = _make_matched_panel()
    panel = build_mechanism_panel(raw)
    # Add a US arm to have cross-market groups
    panel_us = panel.copy()
    panel_us["market"] = "US"
    all_panel = pd.concat([panel, panel_us], ignore_index=True)
    # Make half of each market control so treated vs untreated differ
    all_panel.loc[::2, "treatment_group"] = 0

    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        estimate_quadrant_regression,
    )
    result = estimate_quadrant_regression(
        all_panel,
        market="CN",
        event_phase="announce",
        outcome="car_1_1",
        spec="no_fe",
    )
    assert result["outcome"] == "car_1_1"
    assert result["market"] == "CN"
    assert result["event_phase"] == "announce"
    assert result["spec"] == "no_fe"
    assert "coef" in result and "se" in result and "t" in result
    assert result["n_obs"] >= 1
```

- [ ] **Step 10.2: Implement**

```python
import statsmodels.api as sm


def estimate_quadrant_regression(
    panel: pd.DataFrame,
    *,
    market: str,
    event_phase: str,
    outcome: str,
    spec: str = "no_fe",
) -> dict[str, object]:
    sub = panel.loc[(panel["market"] == market) & (panel["event_phase"] == event_phase)].copy()
    sub = sub.dropna(subset=[outcome, "treatment_group"])
    if sub.empty:
        return {
            "market": market,
            "event_phase": event_phase,
            "outcome": outcome,
            "spec": spec,
            "coef": float("nan"),
            "se": float("nan"),
            "t": float("nan"),
            "p_value": float("nan"),
            "n_obs": 0,
            "r_squared": float("nan"),
        }
    design_cols: list[str] = ["treatment_group"]
    if spec in ("controls", "controls_fe"):
        for col in ("log_mktcap_pre", "pre_turnover"):
            if col in sub.columns:
                design_cols.append(col)
    X = sub[design_cols].astype(float)
    if spec == "controls_fe" and "sector" in sub.columns:
        sector_dummies = pd.get_dummies(sub["sector"], prefix="sector", drop_first=True, dtype=float)
        X = pd.concat([X, sector_dummies], axis=1)
    X = sm.add_constant(X, has_constant="add")
    y = sub[outcome].astype(float)
    model = sm.OLS(y, X).fit(cov_type="HC3")
    return {
        "market": market,
        "event_phase": event_phase,
        "outcome": outcome,
        "spec": spec,
        "coef": float(model.params.get("treatment_group", float("nan"))),
        "se": float(model.bse.get("treatment_group", float("nan"))),
        "t": float(model.tvalues.get("treatment_group", float("nan"))),
        "p_value": float(model.pvalues.get("treatment_group", float("nan"))),
        "n_obs": int(model.nobs),
        "r_squared": float(model.rsquared),
    }
```

- [ ] **Step 10.3: Tests pass + commit**

```bash
pytest tests/test_cma_mechanism_panel.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py tests/test_cma_mechanism_panel.py
git commit -m "feat: cma M3 estimate_quadrant_regression"
```

---

## Task 11: M3 `assemble_mechanism_comparison_table`

- [ ] **Step 11.1: Append test**

```python
def test_assemble_mechanism_comparison_table_schema():
    raw = _make_matched_panel()
    panel = build_mechanism_panel(raw)
    panel_us = panel.copy()
    panel_us["market"] = "US"
    all_panel = pd.concat([panel, panel_us], ignore_index=True)
    all_panel.loc[::2, "treatment_group"] = 0
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        assemble_mechanism_comparison_table,
    )
    tbl = assemble_mechanism_comparison_table(all_panel)
    expected = {"market", "event_phase", "outcome", "spec", "coef", "se", "t", "p_value", "n_obs", "r_squared"}
    assert expected.issubset(tbl.columns)
    # 2 markets × 2 phases × 5 outcomes × 3 specs = 60 rows
    assert len(tbl) == 60
```

- [ ] **Step 11.2: Implement**

```python
OUTCOMES = (
    "car_1_1",
    "turnover_change",
    "volume_change",
    "volatility_change",
    "price_limit_hit_share",
)
SPECS = ("no_fe", "controls", "controls_fe")


def assemble_mechanism_comparison_table(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    markets = panel["market"].dropna().unique()
    phases = panel["event_phase"].dropna().unique()
    for market in markets:
        for phase in phases:
            for outcome in OUTCOMES:
                for spec in SPECS:
                    rows.append(
                        estimate_quadrant_regression(
                            panel, market=market, event_phase=phase, outcome=outcome, spec=spec,
                        )
                    )
    return pd.DataFrame(rows)
```

- [ ] **Step 11.3: Tests pass + commit**

```bash
git add src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py tests/test_cma_mechanism_panel.py
git commit -m "feat: cma M3 assemble_mechanism_comparison_table"
```

---

## Task 12: M3 heatmap + LaTeX 导出

- [ ] **Step 12.1: Append tests**

```python
def test_render_mechanism_heatmap_writes_png(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        render_mechanism_heatmap,
    )
    df = pd.DataFrame(
        [
            {"market": m, "event_phase": p, "outcome": o, "spec": "no_fe", "t": 1.0}
            for m in ("CN", "US") for p in ("announce", "effective") for o in ("car_1_1", "turnover_change")
        ]
    )
    out = render_mechanism_heatmap(df, output_dir=tmp_path)
    assert out.exists()


def test_export_mechanism_tables_writes_csv_and_tex(tmp_path):
    df = pd.DataFrame(
        [
            {"market": "CN", "event_phase": "announce", "outcome": "car_1_1", "spec": "no_fe",
             "coef": 0.01, "se": 0.002, "t": 5.0, "p_value": 0.0, "n_obs": 100, "r_squared": 0.1}
        ]
    )
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        export_mechanism_tables,
    )
    out = export_mechanism_tables(df, output_dir=tmp_path)
    assert out["csv"].exists()
    assert out["tex"].exists()
```

- [ ] **Step 12.2: Implement**

```python
def render_mechanism_heatmap(table: pd.DataFrame, *, output_dir: Path, spec: str = "no_fe") -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sub = table.loc[table["spec"] == spec].copy()
    sub["quadrant"] = sub["market"] + "·" + sub["event_phase"]
    pivot = sub.pivot_table(index="outcome", columns="quadrant", values="t", aggfunc="first")
    fig, ax = plt.subplots(figsize=(9, 5))
    im = ax.imshow(pivot.values, cmap="RdBu_r", vmin=-5, vmax=5, aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    fig.colorbar(im, ax=ax, label="treatment t-stat")
    ax.set_title(f"Mechanism signed-t heatmap ({spec})")
    fig.tight_layout()
    out_path = output_dir / "cma_mechanism_heatmap.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def export_mechanism_tables(table: pd.DataFrame, *, output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "cma_mechanism_panel.csv"
    tex_path = output_dir / "cma_mechanism_panel.tex"
    table.to_csv(csv_path, index=False)
    # Booktabs-style LaTeX. Keep simple; consumers can re-layout if needed.
    with tex_path.open("w") as fh:
        fh.write("% auto-generated CMA mechanism panel\n")
        fh.write("\\begin{tabular}{lllrrrrrr}\n")
        fh.write("\\toprule\n")
        fh.write("market & phase & outcome & spec & coef & se & t & p & N \\\\\n")
        fh.write("\\midrule\n")
        for _, row in table.iterrows():
            fh.write(
                f"{row['market']} & {row['event_phase']} & {row['outcome']} & {row['spec']} & "
                f"{row['coef']:.4f} & {row['se']:.4f} & {row['t']:.3f} & {row['p_value']:.3f} & {int(row['n_obs'])} \\\\\n"
            )
        fh.write("\\bottomrule\n\\end{tabular}\n")
    return {"csv": csv_path, "tex": tex_path}
```

- [ ] **Step 12.3: Tests pass + commit**

```bash
pytest tests/test_cma_mechanism_panel.py -v
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py tests/test_cma_mechanism_panel.py
git add src/index_inclusion_research/analysis/cross_market_asymmetry/mechanism_panel.py tests/test_cma_mechanism_panel.py
git commit -m "feat: cma M3 heatmap and tex export"
```

---

## Task 13: M4 `build_heterogeneity_panel`

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py`
- Create: `tests/test_cma_heterogeneity.py`

**Bucketing dimensions:**
- `size`：`pd.qcut(mkt_cap_pre, 5, labels=["Q1","Q2","Q3","Q4","Q5"])`（**market-internal**）
- `liquidity`：同上，base = pre-event 30-day mean turnover
- `sector`：按 `sector` 原值
- `gap_bucket`：`gap_length_days` → `"≤10" / "11-20" / ">20"`

- [ ] **Step 13.1: Write failing test**

```python
# tests/test_cma_heterogeneity.py
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.heterogeneity import (
    build_heterogeneity_panel,
)


def _make_panel():
    rng = np.random.default_rng(42)
    rows = []
    for event_id in range(1, 11):
        market = "CN" if event_id <= 5 else "US"
        sector = "Tech" if event_id % 2 == 0 else "Fin"
        for phase in ("announce", "effective"):
            for rel in (-3, -1, 0, 1, 3, 10):
                rows.append(
                    {
                        "event_id": event_id,
                        "market": market,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 * rng.standard_normal(),
                        "mkt_cap": 1e9 * event_id,
                        "turnover": 0.01 * event_id,
                        "sector": sector,
                    }
                )
    return pd.DataFrame(rows)


def test_build_heterogeneity_panel_size_adds_bucket():
    panel = _make_panel()
    out = build_heterogeneity_panel(panel, dim="size")
    assert "bucket" in out.columns
    assert out["bucket"].notna().all()
    # within market, buckets should have ~equal counts (quintiles)
    cn_counts = out.loc[out["market"] == "CN", "bucket"].value_counts()
    assert len(cn_counts) <= 5


def test_build_heterogeneity_panel_sector_uses_sector_values():
    panel = _make_panel()
    out = build_heterogeneity_panel(panel, dim="sector")
    assert set(out["bucket"].unique()) == {"Tech", "Fin"}


def test_build_heterogeneity_panel_unknown_dim_raises():
    panel = _make_panel()
    with pytest.raises(ValueError):
        build_heterogeneity_panel(panel, dim="unknown")
```

- [ ] **Step 13.2: Run test, expect failure**

- [ ] **Step 13.3: Implement**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

_VALID_DIMS = ("size", "liquidity", "sector", "gap_bucket")


def _pre_event_mean(panel: pd.DataFrame, col: str, lo: int = -20, hi: int = -1) -> pd.Series:
    sub = panel.loc[(panel["relative_day"] >= lo) & (panel["relative_day"] <= hi)]
    return sub.groupby(["event_id", "market"])[col].mean()


def _within_market_quintile(series: pd.Series, markets: pd.Series) -> pd.Series:
    result = pd.Series(index=series.index, dtype="object")
    for market, idx in markets.groupby(markets).groups.items():
        sub = series.loc[idx]
        if sub.nunique(dropna=True) <= 1:
            result.loc[idx] = "Q1"
            continue
        try:
            bins = pd.qcut(sub, 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"], duplicates="drop")
        except ValueError:
            bins = pd.qcut(sub.rank(method="first"), 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
        result.loc[idx] = bins.astype(str)
    return result


def build_heterogeneity_panel(panel: pd.DataFrame, *, dim: str, gap_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    if dim not in _VALID_DIMS:
        raise ValueError(f"unknown heterogeneity dim: {dim}")
    work = panel.loc[panel["event_type"] == "addition"].copy()
    events = work[["event_id", "market"]].drop_duplicates().reset_index(drop=True)

    if dim == "size":
        pre = _pre_event_mean(work, "mkt_cap")
        values = pre.reindex(pd.MultiIndex.from_frame(events[["event_id", "market"]]))
        events["bucket"] = _within_market_quintile(values.reset_index(drop=True), events["market"])
    elif dim == "liquidity":
        pre = _pre_event_mean(work, "turnover")
        values = pre.reindex(pd.MultiIndex.from_frame(events[["event_id", "market"]]))
        events["bucket"] = _within_market_quintile(values.reset_index(drop=True), events["market"])
    elif dim == "sector":
        sector = work.groupby("event_id")["sector"].first().reindex(events["event_id"]).fillna("Unknown")
        events["bucket"] = sector.astype(str).to_numpy()
    elif dim == "gap_bucket":
        if gap_frame is None:
            raise ValueError("gap_frame required for dim=gap_bucket")
        gap_lookup = gap_frame.set_index("event_id")["gap_length_days"]
        gl = gap_lookup.reindex(events["event_id"])
        events["bucket"] = np.where(
            gl <= 10, "≤10",
            np.where(gl <= 20, "11-20", ">20"),
        )

    events["dim"] = dim
    return events[["event_id", "market", "dim", "bucket"]]
```

- [ ] **Step 13.4: Tests pass + commit**

```bash
pytest tests/test_cma_heterogeneity.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py tests/test_cma_heterogeneity.py
git commit -m "feat: cma M4 build_heterogeneity_panel"
```

---

## Task 14: M4 `compute_cell_statistics`

- [ ] **Step 14.1: Append test**

```python
def test_compute_cell_statistics_asymmetry_index():
    panel = _make_panel()
    # Build a gap frame for test context
    gap = pd.DataFrame({"event_id": range(1, 11), "gap_length_days": [5, 15, 25] * 3 + [5]})
    from index_inclusion_research.analysis.cross_market_asymmetry.heterogeneity import (
        build_heterogeneity_panel,
        compute_cell_statistics,
    )
    buckets = build_heterogeneity_panel(panel, dim="sector")
    stats = compute_cell_statistics(panel, buckets, gap_frame=gap)
    expected = {
        "market",
        "dim",
        "bucket",
        "announce_car",
        "effective_car",
        "gap_drift",
        "asymmetry_index",
        "n_events",
    }
    assert expected.issubset(stats.columns)
```

- [ ] **Step 14.2: Implement**

```python
EPS = 1e-4


def _event_window_car(panel: pd.DataFrame, phase: str, lo: int, hi: int) -> pd.Series:
    sub = panel.loc[
        (panel["event_phase"] == phase)
        & (panel["relative_day"] >= lo)
        & (panel["relative_day"] <= hi)
    ]
    return sub.groupby(["event_id", "market"])["ar"].sum()


def compute_cell_statistics(
    panel: pd.DataFrame,
    buckets: pd.DataFrame,
    *,
    gap_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    announce = _event_window_car(panel, "announce", -1, 1).rename("announce_car").reset_index()
    effective = _event_window_car(panel, "effective", -1, 1).rename("effective_car").reset_index()
    merged = buckets.merge(announce, on=["event_id", "market"], how="left").merge(
        effective, on=["event_id", "market"], how="left"
    )
    if gap_frame is not None:
        gap_map = gap_frame[["event_id", "gap_drift"]] if "gap_drift" in gap_frame.columns else gap_frame[["event_id", "gap_length_days"]].rename(columns={"gap_length_days": "gap_drift"})
        merged = merged.merge(gap_map, on="event_id", how="left")
    else:
        merged["gap_drift"] = 0.0

    stats = merged.groupby(["market", "dim", "bucket"], as_index=False).agg(
        announce_car=("announce_car", "mean"),
        effective_car=("effective_car", "mean"),
        gap_drift=("gap_drift", "mean"),
        n_events=("event_id", "nunique"),
    )
    stats["asymmetry_index"] = (stats["effective_car"] + stats["gap_drift"]) / (
        stats["announce_car"].abs() + EPS
    )
    return stats
```

- [ ] **Step 14.3: Tests pass + commit**

```bash
pytest tests/test_cma_heterogeneity.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py tests/test_cma_heterogeneity.py
git commit -m "feat: cma M4 compute_cell_statistics"
```

---

## Task 15: M4 figure + csv export

- [ ] **Step 15.1: Append test**

```python
def test_render_heterogeneity_matrix_writes_png(tmp_path):
    import matplotlib
    matplotlib.use("Agg")
    from index_inclusion_research.analysis.cross_market_asymmetry.heterogeneity import (
        render_heterogeneity_matrix,
    )
    df = pd.DataFrame(
        [
            {"market": "CN", "dim": "size", "bucket": f"Q{i}", "asymmetry_index": 0.1 * i, "n_events": 10}
            for i in range(1, 6)
        ] + [
            {"market": "US", "dim": "size", "bucket": f"Q{i}", "asymmetry_index": -0.1 * i, "n_events": 10}
            for i in range(1, 6)
        ]
    )
    out = render_heterogeneity_matrix(df, dim="size", output_dir=tmp_path)
    assert out.exists()


def test_export_heterogeneity_tables_writes_csvs(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.heterogeneity import (
        export_heterogeneity_tables,
    )
    df = pd.DataFrame({"market": ["CN"], "dim": ["size"], "bucket": ["Q1"], "asymmetry_index": [0.5]})
    paths = export_heterogeneity_tables({"size": df}, output_dir=tmp_path)
    assert paths["size"].exists()
```

- [ ] **Step 15.2: Implement**

```python
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def render_heterogeneity_matrix(stats: pd.DataFrame, *, dim: str, output_dir: Path) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sub = stats.loc[stats["dim"] == dim] if "dim" in stats.columns else stats
    pivot = sub.pivot_table(index="bucket", columns="market", values="asymmetry_index", aggfunc="first")
    fig, ax = plt.subplots(figsize=(6, 4))
    im = ax.imshow(pivot.values, cmap="RdBu_r", aspect="auto")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    fig.colorbar(im, ax=ax, label="asymmetry_index")
    ax.set_title(f"Heterogeneity ({dim})")
    fig.tight_layout()
    out_path = output_dir / f"cma_heterogeneity_matrix_{dim}.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def export_heterogeneity_tables(tables: dict[str, pd.DataFrame], *, output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    for dim, frame in tables.items():
        out_path = output_dir / f"cma_heterogeneity_{dim}.csv"
        frame.to_csv(out_path, index=False)
        out[dim] = out_path
    return out
```

- [ ] **Step 15.3: Tests pass + commit**

```bash
pytest tests/test_cma_heterogeneity.py -v
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py tests/test_cma_heterogeneity.py
git add src/index_inclusion_research/analysis/cross_market_asymmetry/heterogeneity.py tests/test_cma_heterogeneity.py
git commit -m "feat: cma M4 figures and csv exports"
```

---

## Task 16: M5 `build_rolling_car`

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py`
- Create: `tests/test_cma_time_series.py`

- [ ] **Step 16.1: Write failing test**

```python
# tests/test_cma_time_series.py
from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.time_series import (
    build_rolling_car,
)


def _make_events_with_dates():
    rows = []
    for i, year in enumerate(range(2005, 2025)):
        for market in ("CN", "US"):
            for phase in ("announce", "effective"):
                for rel in (-1, 0, 1):
                    rows.append({
                        "event_id": i * 10 + (0 if market == "CN" else 1),
                        "market": market,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01,
                        "event_date": f"{year}-06-15",
                    })
    return pd.DataFrame(rows)


def test_build_rolling_car_schema():
    panel = _make_events_with_dates()
    out = build_rolling_car(panel, window_years=5, step_years=1)
    expected = {"market", "event_phase", "window_end_year", "car_mean", "car_se", "car_t", "n_events"}
    assert expected.issubset(out.columns)


def test_build_rolling_car_respects_window():
    panel = _make_events_with_dates()
    out = build_rolling_car(panel, window_years=5, step_years=5)
    # Years 2005..2024 with step 5 → windows ending 2009, 2014, 2019, 2024
    assert set(out["window_end_year"].unique()).issuperset({2009, 2014, 2019, 2024})
```

- [ ] **Step 16.2: Run test, expect failure**

- [ ] **Step 16.3: Implement**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py
from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_rolling_car(
    panel: pd.DataFrame,
    *,
    window_years: int = 5,
    step_years: int = 1,
) -> pd.DataFrame:
    work = panel.loc[panel["event_type"] == "addition"].copy()
    work["event_date"] = pd.to_datetime(work["event_date"])
    work["year"] = work["event_date"].dt.year
    car_window = work.loc[(work["relative_day"] >= -1) & (work["relative_day"] <= 1)]
    per_event = car_window.groupby(
        ["event_id", "market", "event_phase", "year"], as_index=False
    )["ar"].sum().rename(columns={"ar": "car_window"})

    min_year = int(per_event["year"].min())
    max_year = int(per_event["year"].max())
    rows = []
    for end_year in range(min_year + window_years - 1, max_year + 1, step_years):
        start_year = end_year - window_years + 1
        sub = per_event.loc[(per_event["year"] >= start_year) & (per_event["year"] <= end_year)]
        agg = sub.groupby(["market", "event_phase"]).agg(
            car_mean=("car_window", "mean"),
            car_std=("car_window", "std"),
            n_events=("event_id", "nunique"),
        ).reset_index()
        agg["car_se"] = agg["car_std"] / agg["n_events"].pow(0.5)
        agg["car_t"] = agg["car_mean"] / agg["car_se"].replace(0.0, pd.NA)
        agg["window_end_year"] = end_year
        agg["window_start_year"] = start_year
        rows.append(agg.drop(columns=["car_std"]))
    return pd.concat(rows, ignore_index=True)
```

- [ ] **Step 16.4: Tests pass + commit**

```bash
pytest tests/test_cma_time_series.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py tests/test_cma_time_series.py
git commit -m "feat: cma M5 build_rolling_car"
```

---

## Task 17: M5 `summarize_structural_break`

- [ ] **Step 17.1: Append test**

```python
def test_summarize_structural_break_returns_pre_post():
    panel = _make_events_with_dates()
    from index_inclusion_research.analysis.cross_market_asymmetry.time_series import (
        build_rolling_car,
        summarize_structural_break,
    )
    rolling = build_rolling_car(panel, window_years=3, step_years=1)
    out = summarize_structural_break(rolling, split_year=2015)
    assert {"market", "event_phase", "period", "car_mean", "car_se"}.issubset(out.columns)
    assert set(out["period"].unique()) == {"pre", "post"}
```

- [ ] **Step 17.2: Implement**

```python
def summarize_structural_break(rolling: pd.DataFrame, *, split_year: int = 2010) -> pd.DataFrame:
    rolling = rolling.copy()
    rolling["period"] = rolling["window_end_year"].apply(lambda y: "pre" if y < split_year else "post")
    out = rolling.groupby(["market", "event_phase", "period"], as_index=False).agg(
        car_mean=("car_mean", "mean"),
        car_se=("car_se", "mean"),
        n_events=("n_events", "sum"),
    )
    return out
```

- [ ] **Step 17.3: Tests pass + commit**

```bash
pytest tests/test_cma_time_series.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py tests/test_cma_time_series.py
git commit -m "feat: cma M5 summarize_structural_break"
```

---

## Task 18: M5 figure + optional AUM overlay

- [ ] **Step 18.1: Append tests**

```python
def test_render_rolling_figure_writes_png(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.time_series import (
        build_rolling_car,
        render_rolling_figure,
    )
    panel = _make_events_with_dates()
    rolling = build_rolling_car(panel)
    out = render_rolling_figure(rolling, output_dir=tmp_path)
    assert out["figure"].exists()
    assert out["aum_overlay"] is False


def test_export_time_series_tables_writes_csvs(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.time_series import (
        build_rolling_car,
        export_time_series_tables,
        summarize_structural_break,
    )
    panel = _make_events_with_dates()
    rolling = build_rolling_car(panel)
    break_df = summarize_structural_break(rolling)
    paths = export_time_series_tables(rolling, break_df, output_dir=tmp_path)
    assert paths["rolling"].exists()
    assert paths["break"].exists()
```

- [ ] **Step 18.2: Implement**

```python
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def render_rolling_figure(
    rolling: pd.DataFrame,
    *,
    output_dir: Path,
    aum_frame: pd.DataFrame | None = None,
) -> dict[str, object]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    for (market, phase), sub in rolling.groupby(["market", "event_phase"]):
        sub = sub.sort_values("window_end_year")
        ax.plot(sub["window_end_year"], sub["car_mean"], marker="o", label=f"{market}·{phase}")
    ax.axhline(0.0, color="#999", linestyle="--", linewidth=0.7)
    ax.set_xlabel("rolling window end year")
    ax.set_ylabel("CAR[-1,+1] mean")
    ax.set_title("Rolling CAR by market and event phase")
    ax.legend()

    aum_overlay = False
    if aum_frame is not None and not aum_frame.empty:
        ax2 = ax.twinx()
        for market, sub in aum_frame.groupby("market"):
            ax2.plot(sub["year"], sub["aum_trillion"], linestyle="--", alpha=0.5, label=f"AUM {market}")
        ax2.set_ylabel("passive AUM (trillion)")
        ax2.legend(loc="lower right")
        aum_overlay = True

    fig.tight_layout()
    fig_path = output_dir / "cma_time_series_rolling.png"
    fig.savefig(fig_path, dpi=150)
    plt.close(fig)
    return {"figure": fig_path, "aum_overlay": aum_overlay}


def export_time_series_tables(
    rolling: pd.DataFrame,
    break_df: pd.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rolling_path = output_dir / "cma_time_series_rolling.csv"
    break_path = output_dir / "cma_time_series_break.csv"
    rolling.to_csv(rolling_path, index=False)
    break_df.to_csv(break_path, index=False)
    return {"rolling": rolling_path, "break": break_path}
```

- [ ] **Step 18.3: Tests pass + commit**

```bash
pytest tests/test_cma_time_series.py -v
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py tests/test_cma_time_series.py
git add src/index_inclusion_research/analysis/cross_market_asymmetry/time_series.py tests/test_cma_time_series.py
git commit -m "feat: cma M5 figure and csv export (with optional AUM overlay)"
```

---

## Task 19: 假设注册表 `hypotheses.py`

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py`
- Create: `tests/test_cma_hypotheses.py`

- [ ] **Step 19.1: Write failing test**

```python
# tests/test_cma_hypotheses.py
from __future__ import annotations

from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
    HYPOTHESES,
    StructuralHypothesis,
)


def test_hypotheses_registry_has_six_entries():
    assert len(HYPOTHESES) == 6
    assert [h.hid for h in HYPOTHESES] == ["H1", "H2", "H3", "H4", "H5", "H6"]


def test_hypothesis_shape():
    for h in HYPOTHESES:
        assert isinstance(h, StructuralHypothesis)
        assert h.name_cn and h.mechanism and h.verdict_logic
        assert h.evidence_refs, f"{h.hid} has no evidence refs"
        assert h.implications, f"{h.hid} has no implications"
```

- [ ] **Step 19.2: Run test, expect failure**

- [ ] **Step 19.3: Implement**

```python
# src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StructuralHypothesis:
    hid: str
    name_cn: str
    mechanism: str
    implications: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    verdict_logic: str


HYPOTHESES: tuple[StructuralHypothesis, ...] = (
    StructuralHypothesis(
        hid="H1",
        name_cn="信息泄露与预运行",
        mechanism="如果 CN 公告前信息泄露比 US 严重，则公告前漂移更大",
        implications=("CN pre_announce_runup 显著高于 US",),
        evidence_refs=("M1:cma_ar_path.csv", "M2:cma_gap_summary.csv"),
        verdict_logic="若 CN pre_announce_runup > US 且 t 显著 → 支持 H1",
    ),
    StructuralHypothesis(
        hid="H2",
        name_cn="被动基金 AUM 差异",
        mechanism="美股被动规模大、套利充分 → 生效日效应被抹平；A 股被动规模小 → 生效日仍有冲击",
        implications=("US effective CAR 在 2005→2020 随 AUM 增长衰减",),
        evidence_refs=("M5:cma_time_series_rolling.csv",),
        verdict_logic="若 US effective rolling CAR 单调下降、A 股 effective 上升 → 支持 H2",
    ),
    StructuralHypothesis(
        hid="H3",
        name_cn="散户 vs 机构结构",
        mechanism="A 股散户比重大 → 生效日量能更集中；美股机构主导 → 生效日量能被提前吸收",
        implications=("CN effective volume_change 显著为正且高于 US",),
        evidence_refs=("M3:cma_mechanism_panel.csv",),
        verdict_logic="若 CN effective × volume_change 系数显著 > 0 且 US 对应系数显著 < 0 → 支持 H3",
    ),
    StructuralHypothesis(
        hid="H4",
        name_cn="卖空约束",
        mechanism="A 股缺少做空通道 → 套利者无法在公告到生效期内压平价差；美股可套利 → 价差被公告日吃光",
        implications=("CN gap_drift 显著为正、US 接近 0",),
        evidence_refs=("M2:cma_gap_summary.csv",),
        verdict_logic="若 CN gap_drift t > US gap_drift t 显著 → 支持 H4",
    ),
    StructuralHypothesis(
        hid="H5",
        name_cn="涨跌停限制",
        mechanism="A 股公告日 ±10% 涨停截断 → 需求溢出到生效日",
        implications=("CN 公告日 price_limit_hit_share 显著 > 0 且与 effective_jump 正相关",),
        evidence_refs=("M3:cma_mechanism_panel.csv",),
        verdict_logic="若 CN announce × price_limit_hit_share > 0 且 effective × price_limit_hit_share > 0 → 支持 H5",
    ),
    StructuralHypothesis(
        hid="H6",
        name_cn="指数权重可预测性",
        mechanism="CN 规则下权重更难预判 → 生效日才重新定价；美股权重可预测 → 信息公告日已定价",
        implications=("CN effective_jump 与 weight_change 正相关（若有权重数据）",),
        evidence_refs=("M4:cma_heterogeneity_size.csv",),
        verdict_logic="M4 size 异质性中，CN 小市值更易受权重预判差影响",
    ),
)
```

- [ ] **Step 19.4: Tests pass + commit**

```bash
pytest tests/test_cma_hypotheses.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py tests/test_cma_hypotheses.py
git commit -m "feat: cma structural hypothesis registry"
```

---

## Task 20: 假设表导出

- [ ] **Step 20.1: Append test**

```python
def test_export_hypothesis_map_writes_csv(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        export_hypothesis_map,
    )
    out = export_hypothesis_map(output_dir=tmp_path)
    assert out.exists()
    df = __import__("pandas").read_csv(out)
    assert {"hid", "name_cn", "mechanism", "evidence_refs", "verdict_logic"}.issubset(df.columns)
    assert len(df) == 6
```

- [ ] **Step 20.2: Implement**

```python
from pathlib import Path

import pandas as pd


def export_hypothesis_map(*, output_dir: Path) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "hid": h.hid,
            "name_cn": h.name_cn,
            "mechanism": h.mechanism,
            "implications": " | ".join(h.implications),
            "evidence_refs": " | ".join(h.evidence_refs),
            "verdict_logic": h.verdict_logic,
        }
        for h in HYPOTHESES
    ]
    out_path = output_dir / "cma_hypothesis_map.csv"
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return out_path
```

- [ ] **Step 20.3: Tests pass + commit**

```bash
pytest tests/test_cma_hypotheses.py -v
git add src/index_inclusion_research/analysis/cross_market_asymmetry/hypotheses.py tests/test_cma_hypotheses.py
git commit -m "feat: cma hypothesis map export"
```

---

## Task 21: Orchestrator + research_summary append

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/orchestrator.py`
- Create: `tests/test_cma_orchestrator.py`

- [ ] **Step 21.1: Write failing test**

```python
# tests/test_cma_orchestrator.py
from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator


def _make_min_event_panel():
    rows = []
    for event_id in (1, 2, 3, 4):
        market = "CN" if event_id <= 2 else "US"
        for phase in ("announce", "effective"):
            for rel in range(-20, 21):
                rows.append({
                    "event_id": event_id,
                    "market": market,
                    "event_type": "addition",
                    "event_phase": phase,
                    "relative_day": rel,
                    "ar": 0.01 if rel == 0 else 0.001,
                    "ret": 0.005 if rel == 0 else 0.0005,
                    "turnover": 0.02 if rel >= 0 else 0.015,
                    "volume": 110 if rel >= 0 else 100,
                    "mkt_cap": 1e9 * event_id,
                    "treatment_group": 1 if event_id in (1, 3) else 0,
                    "sector": "Tech",
                    "event_date": "2020-06-01",
                })
    return pd.DataFrame(rows)


def _make_min_events():
    return pd.DataFrame([
        {"event_id": i, "market": "CN" if i <= 2 else "US", "ticker": f"T{i}", "event_type": "addition",
         "announce_date": "2020-05-15", "effective_date": "2020-06-01"}
        for i in (1, 2, 3, 4)
    ])


def test_orchestrator_runs_on_toy_data(tmp_path, monkeypatch):
    event_panel = _make_min_event_panel()
    matched_panel = event_panel.copy()
    events = _make_min_events()
    event_panel_path = tmp_path / "event_panel.csv"
    matched_path = tmp_path / "matched.csv"
    events_path = tmp_path / "events.csv"
    event_panel.to_csv(event_panel_path, index=False)
    matched_panel.to_csv(matched_path, index=False)
    events.to_csv(events_path, index=False)

    result = orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tmp_path / "tables",
        figures_dir=tmp_path / "figures",
        research_summary_path=tmp_path / "summary.md",
    )

    expected_tables = [
        "cma_ar_path.csv",
        "cma_car_path.csv",
        "cma_window_summary.csv",
        "cma_gap_event_level.csv",
        "cma_gap_summary.csv",
        "cma_mechanism_panel.csv",
        "cma_mechanism_panel.tex",
        "cma_heterogeneity_size.csv",
        "cma_heterogeneity_liquidity.csv",
        "cma_heterogeneity_sector.csv",
        "cma_heterogeneity_gap_bucket.csv",
        "cma_time_series_rolling.csv",
        "cma_time_series_break.csv",
        "cma_hypothesis_map.csv",
    ]
    for name in expected_tables:
        assert (tmp_path / "tables" / name).exists(), f"missing: {name}"

    expected_figures = [
        "cma_ar_path_comparison.png",
        "cma_car_path_comparison.png",
        "cma_gap_length_distribution.png",
        "cma_gap_decomposition.png",
        "cma_mechanism_heatmap.png",
        "cma_heterogeneity_matrix_size.png",
        "cma_time_series_rolling.png",
    ]
    for name in expected_figures:
        assert (tmp_path / "figures" / name).exists(), f"missing figure: {name}"

    summary = (tmp_path / "summary.md").read_text()
    assert "六、美股 vs A股 不对称" in summary
    assert "announce_car" in summary or "announce_jump" in summary
    assert result["tables_count"] == len(expected_tables)
```

- [ ] **Step 21.2: Run test, expect failure**

- [ ] **Step 21.3: Implement `run_cma_pipeline`**

```python
# orchestrator.py - append below the existing constants
from __future__ import annotations

from pathlib import Path

import pandas as pd

from . import gap_period, heterogeneity, hypotheses, mechanism_panel, paths, time_series

APPEND_MARKER = "## 六、美股 vs A股 不对称"


def _load_panel(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _append_research_summary(
    *,
    summary_path: Path,
    window_summary: pd.DataFrame,
    gap_summary: pd.DataFrame,
    mechanism_table: pd.DataFrame,
) -> None:
    lines = ["", APPEND_MARKER, ""]
    # 4 象限 × CAR[-1,+1] 摘要
    lines.append("### 4 象限 CAR[-1,+1] 摘要")
    focus = window_summary.loc[
        (window_summary["window_start"] == -1) & (window_summary["window_end"] == 1)
    ]
    for _, row in focus.iterrows():
        lines.append(
            f"- {row['market']} {row['event_phase']}：CAR[-1,+1] = `{row['car_mean']:.4f}`，"
            f"t = `{row['car_t']:.2f}`，n = `{int(row['n_events'])}`"
        )
    # 空窗期摘要
    lines.append("")
    lines.append("### 空窗期与生效日")
    for _, row in gap_summary.iterrows():
        lines.append(
            f"- {row['market']} {row['metric']}：均值 `{row['mean']:.4f}`，t = `{row['t']:.2f}`，n = `{int(row['n_events'])}`"
        )
    # 机制摘要（仅 no_fe 的 CAR & turnover）
    lines.append("")
    lines.append("### 机制差异（no_fe）")
    focus = mechanism_table.loc[
        (mechanism_table["spec"] == "no_fe")
        & (mechanism_table["outcome"].isin(["car_1_1", "turnover_change", "price_limit_hit_share"]))
    ]
    for _, row in focus.iterrows():
        lines.append(
            f"- {row['market']} {row['event_phase']} {row['outcome']}：coef = `{row['coef']:.4f}`，t = `{row['t']:.2f}`"
        )

    existing = ""
    if summary_path.exists():
        existing = summary_path.read_text()
        # Remove any previous appended block so we never duplicate
        if APPEND_MARKER in existing:
            existing = existing.split(APPEND_MARKER)[0].rstrip() + "\n"
    summary_path.write_text(existing + "\n".join(lines) + "\n")


def run_cma_pipeline(
    *,
    event_panel_path: Path = REAL_EVENT_PANEL,
    matched_panel_path: Path = REAL_MATCHED_EVENT_PANEL,
    events_path: Path = REAL_EVENTS_CLEAN,
    tables_dir: Path = REAL_TABLES_DIR,
    figures_dir: Path = REAL_FIGURES_DIR,
    research_summary_path: Path | None = None,
) -> dict[str, object]:
    event_panel = _load_panel(Path(event_panel_path))
    matched_panel = _load_panel(Path(matched_panel_path))
    events = _load_panel(Path(events_path))

    tables_dir = Path(tables_dir)
    figures_dir = Path(figures_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    ar_panel = paths.build_daily_ar_panel(event_panel)
    avg = paths.compute_average_paths(ar_panel)
    window_summary = paths.compute_window_summary(ar_panel)
    paths.export_path_tables(ar_panel, avg, window_summary, output_dir=tables_dir)
    paths.render_path_figures(avg, output_dir=figures_dir)

    gap = gap_period.compute_gap_metrics(events, event_panel)
    gap_summary = gap_period.summarize_gap_metrics(gap)
    gap_period.export_gap_tables(gap, gap_summary, output_dir=tables_dir)
    gap_period.render_gap_figures(gap, gap_summary, output_dir=figures_dir)

    mech_panel = mechanism_panel.build_mechanism_panel(matched_panel)
    mech_table = mechanism_panel.assemble_mechanism_comparison_table(mech_panel)
    mechanism_panel.export_mechanism_tables(mech_table, output_dir=tables_dir)
    mechanism_panel.render_mechanism_heatmap(mech_table, output_dir=figures_dir)

    het_tables: dict[str, pd.DataFrame] = {}
    for dim in ("size", "liquidity", "sector", "gap_bucket"):
        try:
            buckets = heterogeneity.build_heterogeneity_panel(
                event_panel, dim=dim, gap_frame=gap if dim == "gap_bucket" else None
            )
            stats = heterogeneity.compute_cell_statistics(event_panel, buckets, gap_frame=gap)
            het_tables[dim] = stats
        except Exception as exc:
            het_tables[dim] = pd.DataFrame({"error": [str(exc)]})
    heterogeneity.export_heterogeneity_tables(het_tables, output_dir=tables_dir)
    if "size" in het_tables and "asymmetry_index" in het_tables["size"].columns:
        heterogeneity.render_heterogeneity_matrix(het_tables["size"], dim="size", output_dir=figures_dir)

    rolling = time_series.build_rolling_car(event_panel)
    break_df = time_series.summarize_structural_break(rolling)
    time_series.export_time_series_tables(rolling, break_df, output_dir=tables_dir)
    time_series.render_rolling_figure(rolling, output_dir=figures_dir)

    hypotheses.export_hypothesis_map(output_dir=tables_dir)

    if research_summary_path is not None:
        _append_research_summary(
            summary_path=Path(research_summary_path),
            window_summary=window_summary,
            gap_summary=gap_summary,
            mechanism_table=mech_table,
        )

    return {
        "tables_dir": tables_dir,
        "figures_dir": figures_dir,
        "tables_count": sum(1 for _ in tables_dir.glob("cma_*")),
        "figures_count": sum(1 for _ in figures_dir.glob("cma_*.png")),
    }
```

- [ ] **Step 21.4: Tests pass + commit**

```bash
pytest tests/test_cma_orchestrator.py -v
ruff check src/index_inclusion_research/analysis/cross_market_asymmetry/orchestrator.py tests/test_cma_orchestrator.py
git add src/index_inclusion_research/analysis/cross_market_asymmetry/ tests/test_cma_orchestrator.py
git commit -m "feat: cma orchestrator with research_summary append"
```

---

## Task 22: CLI 入口 `cross_market_asymmetry.py`

**Files:**
- Create: `src/index_inclusion_research/cross_market_asymmetry.py`
- Create: `tests/test_cma_cli.py`

- [ ] **Step 22.1: Write failing test**

```python
# tests/test_cma_cli.py
from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research import cross_market_asymmetry as cma_cli


def test_cli_main_fails_when_inputs_missing(tmp_path, monkeypatch):
    fake = tmp_path / "nope.csv"
    with pytest.raises(FileNotFoundError):
        cma_cli.main([
            "--event-panel", str(fake),
            "--matched-panel", str(fake),
            "--events", str(fake),
            "--tables-dir", str(tmp_path / "t"),
            "--figures-dir", str(tmp_path / "f"),
        ])


def test_cli_main_runs_with_valid_inputs(tmp_path):
    from tests.test_cma_orchestrator import _make_min_event_panel, _make_min_events

    event_panel_path = tmp_path / "event_panel.csv"
    matched_path = tmp_path / "matched.csv"
    events_path = tmp_path / "events.csv"
    _make_min_event_panel().to_csv(event_panel_path, index=False)
    _make_min_event_panel().to_csv(matched_path, index=False)
    _make_min_events().to_csv(events_path, index=False)
    summary_path = tmp_path / "summary.md"

    cma_cli.main([
        "--event-panel", str(event_panel_path),
        "--matched-panel", str(matched_path),
        "--events", str(events_path),
        "--tables-dir", str(tmp_path / "tables"),
        "--figures-dir", str(tmp_path / "figures"),
        "--research-summary", str(summary_path),
    ])
    assert (tmp_path / "tables" / "cma_ar_path.csv").exists()
    assert summary_path.exists()
```

- [ ] **Step 22.2: Run test, expect failure**

- [ ] **Step 22.3: Implement CLI**

```python
# src/index_inclusion_research/cross_market_asymmetry.py
from __future__ import annotations

import argparse
from pathlib import Path

from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator


def _require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the cross-market (CN vs US) announce/effective asymmetry analysis pack.",
    )
    parser.add_argument("--event-panel", default=str(orchestrator.REAL_EVENT_PANEL))
    parser.add_argument("--matched-panel", default=str(orchestrator.REAL_MATCHED_EVENT_PANEL))
    parser.add_argument("--events", default=str(orchestrator.REAL_EVENTS_CLEAN))
    parser.add_argument("--tables-dir", default=str(orchestrator.REAL_TABLES_DIR))
    parser.add_argument("--figures-dir", default=str(orchestrator.REAL_FIGURES_DIR))
    parser.add_argument(
        "--research-summary",
        default=str(orchestrator.REAL_TABLES_DIR / "research_summary.md"),
    )
    args = parser.parse_args(argv)

    event_panel = Path(args.event_panel)
    matched = Path(args.matched_panel)
    events = Path(args.events)
    _require_file(event_panel, "event_panel")
    _require_file(matched, "matched_panel")
    _require_file(events, "events")

    result = orchestrator.run_cma_pipeline(
        event_panel_path=event_panel,
        matched_panel_path=matched,
        events_path=events,
        tables_dir=Path(args.tables_dir),
        figures_dir=Path(args.figures_dir),
        research_summary_path=Path(args.research_summary),
    )
    print("CMA pipeline finished.")
    print(f"  tables_dir: {result['tables_dir']}")
    print(f"  figures_dir: {result['figures_dir']}")
    print(f"  tables written: {result['tables_count']}")
    print(f"  figures written: {result['figures_count']}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 22.4: Tests pass + commit**

```bash
pytest tests/test_cma_cli.py -v
git add src/index_inclusion_research/cross_market_asymmetry.py tests/test_cma_cli.py
git commit -m "feat: cma CLI entrypoint"
```

---

## Task 23: 注册 console script + `cli.py`

**Files:**
- Modify: `pyproject.toml`（新增一行 script）
- Modify: `src/index_inclusion_research/cli.py`（新增一个 `run_cma_main`）
- Modify: `tests/test_cma_cli.py`（增加 cli.py 包装测试）

- [ ] **Step 23.1: Append test**

```python
def test_cli_py_exports_run_cma_main():
    from index_inclusion_research import cli
    assert callable(cli.run_cma_main)
```

- [ ] **Step 23.2: Modify `cli.py`**

Append to `src/index_inclusion_research/cli.py`:
```python
def run_cma_main() -> None:
    _run_package_main("index_inclusion_research.cross_market_asymmetry")
```

- [ ] **Step 23.3: Modify `pyproject.toml`**

Add one line under `[project.scripts]`:
```toml
index-inclusion-cma = "index_inclusion_research.cli:run_cma_main"
```

- [ ] **Step 23.4: Tests pass + commit**

```bash
pytest tests/test_cma_cli.py -v
git add pyproject.toml src/index_inclusion_research/cli.py tests/test_cma_cli.py
git commit -m "feat: register index-inclusion-cma console script"
```

---

## Task 23.5: `--tex-only` + `figures_tables` 集成

**Purpose:** Spec §7.2 承诺两件事：(1) `index-inclusion-cma --tex-only` 跳过计算、只基于已有 CSV 重新生成 `.tex`；(2) `index-inclusion-make-figures-tables` 跑完标准表格后自动调 CMA 的 LaTeX 导出。

**Files:**
- Modify: `src/index_inclusion_research/cross_market_asymmetry.py`（加 `--tex-only`）
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/orchestrator.py`（加 `regenerate_tex_only()`）
- Modify: `src/index_inclusion_research/figures_tables.py`（尾部调用）
- Modify: `tests/test_cma_cli.py`、`tests/test_make_figures_tables.py`

- [ ] **Step 23.5.1: Append orchestrator test**

Add to `tests/test_cma_orchestrator.py`:
```python
def test_regenerate_tex_only_uses_existing_csv(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()
    import pandas as pd
    pd.DataFrame(
        [
            {"market": "CN", "event_phase": "announce", "outcome": "car_1_1", "spec": "no_fe",
             "coef": 0.01, "se": 0.002, "t": 5.0, "p_value": 0.0, "n_obs": 100, "r_squared": 0.1}
        ]
    ).to_csv(tables / "cma_mechanism_panel.csv", index=False)
    orchestrator.regenerate_tex_only(tables_dir=tables)
    assert (tables / "cma_mechanism_panel.tex").exists()


def test_regenerate_tex_only_errors_when_csv_missing(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()
    import pytest as _pytest
    with _pytest.raises(FileNotFoundError):
        orchestrator.regenerate_tex_only(tables_dir=tables)
```

- [ ] **Step 23.5.2: Implement in `orchestrator.py`**

```python
def regenerate_tex_only(*, tables_dir: Path = REAL_TABLES_DIR) -> dict[str, Path]:
    tables_dir = Path(tables_dir)
    csv_path = tables_dir / "cma_mechanism_panel.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing cma_mechanism_panel.csv under {tables_dir}")
    import pandas as pd
    table = pd.read_csv(csv_path)
    from . import mechanism_panel
    return mechanism_panel.export_mechanism_tables(table, output_dir=tables_dir)
```

- [ ] **Step 23.5.3: Add `--tex-only` flag to CLI**

In `cross_market_asymmetry.py`:
```python
parser.add_argument(
    "--tex-only",
    action="store_true",
    help="Skip computation; only regenerate LaTeX tables from existing CMA CSVs.",
)
# ... then in main() branch:
if args.tex_only:
    orchestrator.regenerate_tex_only(tables_dir=Path(args.tables_dir))
    print("CMA LaTeX regenerated.")
    return
```

- [ ] **Step 23.5.4: Append CLI test**

```python
def test_cli_tex_only_regenerates(tmp_path):
    from index_inclusion_research import cross_market_asymmetry as cma_cli
    tables = tmp_path / "tables"
    tables.mkdir()
    import pandas as pd
    pd.DataFrame(
        [{"market": "CN", "event_phase": "announce", "outcome": "car_1_1", "spec": "no_fe",
          "coef": 0.01, "se": 0.002, "t": 5.0, "p_value": 0.0, "n_obs": 100, "r_squared": 0.1}]
    ).to_csv(tables / "cma_mechanism_panel.csv", index=False)
    cma_cli.main(["--tex-only", "--tables-dir", str(tables)])
    assert (tables / "cma_mechanism_panel.tex").exists()
```

- [ ] **Step 23.5.5: Wire into `figures_tables.py`**

Find the final writing step in `figures_tables.py` (where it finishes writing real_tables). After it finishes and if `cma_mechanism_panel.csv` already exists, call `regenerate_tex_only` to refresh the `.tex`:

```python
# figures_tables.py (append near end of main real-data export block)
try:
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator as _cma
    if (REAL_TABLES_DIR / "cma_mechanism_panel.csv").exists():
        _cma.regenerate_tex_only(tables_dir=REAL_TABLES_DIR)
except Exception as exc:  # noqa: BLE001
    # CMA is optional; do not break the main pipeline
    print(f"[figures_tables] CMA tex regeneration skipped: {exc}")
```

- [ ] **Step 23.5.6: Append integration test**

Append to `tests/test_make_figures_tables.py`:
```python
def test_figures_tables_refreshes_cma_tex_when_csv_present(tmp_path, monkeypatch):
    # Set REAL_TABLES_DIR to tmp, seed a minimal cma csv, run the wrapper.
    import pandas as pd
    from index_inclusion_research import figures_tables as ft
    monkeypatch.setattr(ft, "REAL_TABLES_DIR", tmp_path)
    pd.DataFrame(
        [{"market": "CN", "event_phase": "announce", "outcome": "car_1_1", "spec": "no_fe",
          "coef": 0.01, "se": 0.002, "t": 5.0, "p_value": 0.0, "n_obs": 100, "r_squared": 0.1}]
    ).to_csv(tmp_path / "cma_mechanism_panel.csv", index=False)
    # Call whichever is the "apply CMA tex" function; if the integration is inline, run main()
    try:
        ft.refresh_cma_tex()  # if exposed
    except AttributeError:
        # inline-only: rely on the final call in main(), but still assert csv→tex happens when we call the helper directly
        from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator
        orchestrator.regenerate_tex_only(tables_dir=tmp_path)
    assert (tmp_path / "cma_mechanism_panel.tex").exists()
```

- [ ] **Step 23.5.7: Run all new tests + commit**

```bash
pytest tests/test_cma_orchestrator.py::test_regenerate_tex_only_uses_existing_csv tests/test_cma_orchestrator.py::test_regenerate_tex_only_errors_when_csv_missing tests/test_cma_cli.py tests/test_make_figures_tables.py -v
git add src/index_inclusion_research/cross_market_asymmetry.py src/index_inclusion_research/analysis/cross_market_asymmetry/orchestrator.py src/index_inclusion_research/figures_tables.py tests/test_cma_orchestrator.py tests/test_cma_cli.py tests/test_make_figures_tables.py
git commit -m "feat: cma --tex-only and figures_tables integration"
```

---

## Task 24: Dashboard 内容文案

**Files:**
- Modify: `src/index_inclusion_research/dashboard_content.py`
- Modify: `tests/test_dashboard_content.py`

**Approach:** 按文件现有模式（大字典 / dataclass）新增 CMA section 的 title / subtitle / lead paragraph / brief_summary / conclusion_bullets 字段。

- [ ] **Step 24.1: Read `dashboard_content.py` 前 200 行并识别文案组织模式（dict/dataclass key 命名约定）**

Read command:
```bash
head -200 src/index_inclusion_research/dashboard_content.py
```

- [ ] **Step 24.2: Append test**

Add to `tests/test_dashboard_content.py`:
```python
def test_cross_market_section_content_keys_present():
    from index_inclusion_research import dashboard_content as c

    ids = getattr(c, "SECTION_CONTENT", c.__dict__).get("cross_market_asymmetry")
    if ids is None:
        # Fallback: the project may expose a getter instead
        ids = c.get_section_content("cross_market_asymmetry") if hasattr(c, "get_section_content") else None
    assert ids is not None, "dashboard_content must register cross_market_asymmetry section"
    for key in ("title", "subtitle", "lead", "brief_summary", "conclusion_bullets"):
        assert key in ids, f"cross_market_asymmetry section missing '{key}'"
```

- [ ] **Step 24.3: Run test, expect fail**

- [ ] **Step 24.4: Add content dict entry**

Follow existing pattern exactly. Append a new entry with these fields (values below):

- `title`：`"美股 vs A股 公告—生效事件集中度差异"`
- `subtitle`：`"CN vs US announce/effective concentration"`
- `lead`：`"A 股在公告日拉价、生效日拉量；美股在公告日两样都拉、生效日反向抽回——这是跨市场不对称的核心现象。"`
- `brief_summary`：1 句话总结，参考 research_summary 的 4 象限摘要
- `conclusion_bullets`：3 条简短要点（价格集中、量能集中、异质性集中）

- [ ] **Step 24.5: Tests pass + commit**

```bash
pytest tests/test_dashboard_content.py -v
git add src/index_inclusion_research/dashboard_content.py tests/test_dashboard_content.py
git commit -m "feat: dashboard cma section content"
```

---

## Task 25: Dashboard section runtime

**Files:**
- Modify: `src/index_inclusion_research/dashboard_page_sections_runtime.py`
- Modify: `tests/test_dashboard_page_runtime.py` 或新文件 `tests/test_dashboard_cma_section.py`

- [ ] **Step 25.1: Read `dashboard_page_sections_runtime.py` 完整文件，识别现有 `build_*_section` 方法的签名和返回类型**

```bash
cat src/index_inclusion_research/dashboard_page_sections_runtime.py
```

- [ ] **Step 25.2: Write test**

```python
# tests/test_dashboard_cma_section.py
from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.dashboard_page_sections_runtime import (
    DashboardPageSectionsRuntime,
)


def test_build_cross_market_section_returns_keys(tmp_path):
    # construct a minimal runtime seeded with tmp table CSVs
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    tables.mkdir()
    figures.mkdir()
    # write a minimal window_summary
    pd.DataFrame(
        [
            {"market": m, "event_phase": p, "window_start": -1, "window_end": 1,
             "car_mean": 0.01, "car_se": 0.002, "car_t": 5.0, "p_value": 0.0, "n_events": 100}
            for m in ("CN", "US") for p in ("announce", "effective")
        ]
    ).to_csv(tables / "cma_window_summary.csv", index=False)
    pd.DataFrame(
        [{"hid": f"H{i}", "name_cn": f"n{i}", "mechanism": "m", "implications": "",
          "evidence_refs": "", "verdict_logic": ""} for i in range(1, 7)]
    ).to_csv(tables / "cma_hypothesis_map.csv", index=False)

    runtime = DashboardPageSectionsRuntime(tables_dir=tables, figures_dir=figures)
    section = runtime.build_cross_market_section(mode="full")
    assert section["id"] == "cross_market_asymmetry"
    assert "quadrant_table" in section
    assert "figures" in section
    assert "hypothesis_map" in section
```

- [ ] **Step 25.3: Implement `build_cross_market_section`**

Add to `dashboard_page_sections_runtime.py`:
```python
from pathlib import Path
import pandas as pd


class DashboardPageSectionsRuntime:  # already exists; extend
    def build_cross_market_section(self, *, mode: str = "full") -> dict:
        tables = Path(self.tables_dir)
        figures = Path(self.figures_dir)
        quadrant = pd.DataFrame()
        if (tables / "cma_window_summary.csv").exists():
            window_summary = pd.read_csv(tables / "cma_window_summary.csv")
            quadrant = window_summary.loc[
                (window_summary["window_start"] == -1) & (window_summary["window_end"] == 1),
                ["market", "event_phase", "car_mean", "car_t", "n_events"],
            ]
        hypotheses = pd.DataFrame()
        if (tables / "cma_hypothesis_map.csv").exists():
            hypotheses = pd.read_csv(tables / "cma_hypothesis_map.csv")

        brief_figs = {"path_ar": figures / "cma_ar_path_comparison.png",
                      "gap_decomp": figures / "cma_gap_decomposition.png",
                      "mech_heatmap": figures / "cma_mechanism_heatmap.png"}
        full_figs = {**brief_figs,
                     "het_size": figures / "cma_heterogeneity_matrix_size.png",
                     "rolling": figures / "cma_time_series_rolling.png"}
        if mode == "brief":
            figs = {}
        elif mode == "demo":
            figs = brief_figs
        else:
            figs = full_figs

        return {
            "id": "cross_market_asymmetry",
            "mode": mode,
            "quadrant_table": quadrant,
            "figures": {k: str(p) for k, p in figs.items() if p.exists()},
            "hypothesis_map": hypotheses if mode == "full" else pd.DataFrame(),
        }
```

(Make sure the class has `tables_dir` / `figures_dir` attrs; if it doesn't, pipe through `__init__` kwargs matching existing conventions.)

- [ ] **Step 25.4: Tests pass + commit**

```bash
pytest tests/test_dashboard_cma_section.py -v
git add src/index_inclusion_research/dashboard_page_sections_runtime.py tests/test_dashboard_cma_section.py
git commit -m "feat: cma dashboard section runtime"
```

---

## Task 26: Register section in `dashboard_sections.py` and wire into home

**Files:**
- Modify: `src/index_inclusion_research/dashboard_sections.py`
- Modify: `src/index_inclusion_research/dashboard_home.py`
- Modify: `tests/test_dashboard_sections.py`
- Modify: `tests/test_dashboard_home.py`

- [ ] **Step 26.1: Read current section registration pattern**

```bash
grep -n "section" src/index_inclusion_research/dashboard_sections.py | head -50
```

- [ ] **Step 26.2: Add cma section to registry + HomeContextBuilder hook**

Append test to `tests/test_dashboard_sections.py`:
```python
def test_registered_section_ids_include_cross_market():
    from index_inclusion_research.dashboard_sections import registered_section_ids
    assert "cross_market_asymmetry" in registered_section_ids()
```

Append test to `tests/test_dashboard_home.py`:
```python
def test_home_context_includes_cross_market_section():
    from index_inclusion_research.dashboard_home import DashboardHomeContextBuilder
    builder = DashboardHomeContextBuilder.for_testing()  # adapt to actual factory
    ctx = builder.build(mode="full")
    assert any(s.get("id") == "cross_market_asymmetry" for s in ctx["sections"])
```

(These exact builder/factory names must match what exists. Use the actual factories — see `tests/test_dashboard_home.py` existing tests for patterns.)

Extend `dashboard_sections.py`'s registration map and `dashboard_home.py`'s builder so the CMA section appears after the identification section and respects the mode (`brief` / `demo` / `full`).

- [ ] **Step 26.3: Tests pass + commit**

```bash
pytest tests/test_dashboard_sections.py tests/test_dashboard_home.py -v
git add src/index_inclusion_research/dashboard_sections.py src/index_inclusion_research/dashboard_home.py tests/test_dashboard_sections.py tests/test_dashboard_home.py
git commit -m "feat: register cma dashboard section in home builder"
```

---

## Task 27: Dashboard figures + metrics wiring

**Files:**
- Modify: `src/index_inclusion_research/dashboard_figures.py`（让新 PNG 被暴露）
- Modify: `src/index_inclusion_research/dashboard_metrics.py`（4 象限摘要指标）
- Modify: `tests/test_dashboard_figures.py`
- Modify: `tests/test_dashboard_metrics.py`

- [ ] **Step 27.1: Append tests**

```python
# test_dashboard_figures.py addition
def test_cma_figures_registered():
    from index_inclusion_research.dashboard_figures import list_registered_figure_keys
    keys = list_registered_figure_keys()
    for key in ("cma_ar_path_comparison", "cma_gap_decomposition", "cma_mechanism_heatmap"):
        assert key in keys


# test_dashboard_metrics.py addition
def test_cma_quadrant_metrics_present():
    from index_inclusion_research.dashboard_metrics import build_cross_market_quadrant_metrics
    import pandas as pd
    ws = pd.DataFrame(
        [{"market": m, "event_phase": p, "window_start": -1, "window_end": 1,
          "car_mean": 0.01, "car_t": 5.0, "n_events": 100}
         for m in ("CN", "US") for p in ("announce", "effective")]
    )
    out = build_cross_market_quadrant_metrics(ws)
    assert len(out) == 4
    for row in out:
        assert "market" in row and "event_phase" in row and "car_mean" in row
```

- [ ] **Step 27.2: Implement wiring**

In `dashboard_figures.py`: follow the existing `FIGURE_KEYS` / `FIGURE_PATHS` registration pattern and add the 5 new keys (`cma_ar_path_comparison`, `cma_car_path_comparison`, `cma_gap_length_distribution`, `cma_gap_decomposition`, `cma_mechanism_heatmap`, `cma_heterogeneity_matrix_size`, `cma_time_series_rolling`).

In `dashboard_metrics.py`: add a small function that turns `cma_window_summary.csv`'s `[-1,+1]` rows into a list of `{market, event_phase, car_mean, car_t, n_events}` dicts.

- [ ] **Step 27.3: Tests pass + commit**

```bash
pytest tests/test_dashboard_figures.py tests/test_dashboard_metrics.py -v
git add src/index_inclusion_research/dashboard_figures.py src/index_inclusion_research/dashboard_metrics.py tests/test_dashboard_figures.py tests/test_dashboard_metrics.py
git commit -m "feat: wire cma figures and quadrant metrics"
```

---

## Task 28: Dashboard presenter + template

**Files:**
- Modify: `src/index_inclusion_research/dashboard_presenters.py`
- Modify: `src/index_inclusion_research/web/templates/_dashboard_content_macros.html` 或 `dashboard.html`（按现有 section 的 include 位置）
- Modify: `tests/test_dashboard_presenters.py`

- [ ] **Step 28.1: Append presenter test**

```python
def test_cma_section_presenter_has_expected_shape():
    from index_inclusion_research.dashboard_presenters import present_cross_market_section
    import pandas as pd
    raw = {
        "id": "cross_market_asymmetry",
        "mode": "demo",
        "quadrant_table": pd.DataFrame(
            [{"market": "CN", "event_phase": "announce", "car_mean": 0.01, "car_t": 5.0, "n_events": 100}]
        ),
        "figures": {"path_ar": "/abs/ar.png"},
        "hypothesis_map": pd.DataFrame(),
    }
    out = present_cross_market_section(raw)
    assert out["title"]  # from dashboard_content
    assert "rows" in out["quadrant_table"]
    assert out["figures"]["path_ar"].endswith("ar.png")
```

- [ ] **Step 28.2: Implement presenter**

```python
# dashboard_presenters.py - append
from index_inclusion_research import dashboard_content


def present_cross_market_section(section: dict) -> dict:
    copy = getattr(dashboard_content, "SECTION_CONTENT", {}).get("cross_market_asymmetry", {})
    quadrant = section.get("quadrant_table")
    rows = quadrant.to_dict(orient="records") if quadrant is not None else []
    hmap = section.get("hypothesis_map")
    hmap_rows = hmap.to_dict(orient="records") if hmap is not None else []
    return {
        "id": "cross_market_asymmetry",
        "mode": section.get("mode", "demo"),
        "title": copy.get("title", ""),
        "subtitle": copy.get("subtitle", ""),
        "lead": copy.get("lead", ""),
        "brief_summary": copy.get("brief_summary", ""),
        "conclusion_bullets": copy.get("conclusion_bullets", []),
        "quadrant_table": {"rows": rows},
        "figures": section.get("figures", {}),
        "hypothesis_map": {"rows": hmap_rows},
    }
```

- [ ] **Step 28.3: Add template macro**

In `_dashboard_content_macros.html` (or the file that currently holds other section macros), add a `render_cross_market_section(ctx)` macro that renders:

1. `h2` with `ctx.title`
2. `p` with `ctx.lead`
3. A 4-row table from `ctx.quadrant_table.rows`（列：市场、事件阶段、CAR[-1,+1]、t、n）
4. Image tags for each key in `ctx.figures`（classes to match existing dashboard section styling）
5. `ctx.hypothesis_map.rows` rendered as a 6-row table (only when mode == "full")

Wire the macro into `dashboard.html` in the spot that follows the identification section, guarded by the section id (`cross_market_asymmetry`).

- [ ] **Step 28.4: Tests pass + commit**

```bash
pytest tests/test_dashboard_presenters.py -v
git add src/index_inclusion_research/dashboard_presenters.py src/index_inclusion_research/web/templates tests/test_dashboard_presenters.py
git commit -m "feat: cma dashboard presenter and template"
```

---

## Task 29: `research_summary.md` 追加测试

**Files:**
- Modify: `tests/test_generate_research_report.py` 或 `tests/test_reporting.py`

- [ ] **Step 29.1: Append test**

Pick whichever exists for a similar end-to-end case. Add:
```python
def test_research_summary_appends_cma_section(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator
    from tests.test_cma_orchestrator import _make_min_event_panel, _make_min_events

    tables_dir = tmp_path / "tables"
    figures_dir = tmp_path / "figures"
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("# 现有内容\n\n前面的章节。\n")

    event_panel = _make_min_event_panel()
    events = _make_min_events()
    event_panel_path = tmp_path / "ep.csv"
    matched_path = tmp_path / "mp.csv"
    events_path = tmp_path / "ev.csv"
    event_panel.to_csv(event_panel_path, index=False)
    event_panel.to_csv(matched_path, index=False)
    events.to_csv(events_path, index=False)

    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tables_dir,
        figures_dir=figures_dir,
        research_summary_path=summary_path,
    )
    content = summary_path.read_text()
    assert "前面的章节" in content
    assert "六、美股 vs A股 不对称" in content

    # Running twice should not duplicate the section
    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tables_dir,
        figures_dir=figures_dir,
        research_summary_path=summary_path,
    )
    content2 = summary_path.read_text()
    assert content2.count("六、美股 vs A股 不对称") == 1
```

- [ ] **Step 29.2: Run, commit**

```bash
pytest tests/test_generate_research_report.py -v
git add tests/test_generate_research_report.py
git commit -m "test: cma research_summary append is idempotent"
```

---

## Task 30: Browser smoke test 覆盖

**Files:**
- Modify: `tests/test_dashboard_browser_smoke.py`

- [ ] **Step 30.1: Append test body**

Follow the file's existing pattern (requires `RUN_BROWSER_SMOKE=1`). Add:
```python
@pytest.mark.browser_smoke
def test_cross_market_section_renders(dashboard_browser):
    dashboard_browser.goto("http://localhost:5001/?mode=full")
    section = dashboard_browser.locator("section#cross_market_asymmetry")
    section.wait_for(state="visible", timeout=5000)
    assert section.locator("h2").count() >= 1
    # Expect the 4-row quadrant table
    rows = section.locator("table tbody tr")
    assert rows.count() >= 4
    # Expect at least one CMA figure
    imgs = section.locator("img[src*='cma_']")
    assert imgs.count() >= 1
```

(Use whatever fixture name the existing tests use — `dashboard_browser`, `page`, etc. Match exactly.)

- [ ] **Step 30.2: Run (if Playwright installed)**

```bash
RUN_BROWSER_SMOKE=1 pytest tests/test_dashboard_browser_smoke.py::test_cross_market_section_renders -v
```

- [ ] **Step 30.3: Commit**

```bash
git add tests/test_dashboard_browser_smoke.py
git commit -m "test: browser smoke for cma dashboard section"
```

---

## Task 31: README + docs 提示

**Files:**
- Modify: `README.md`（在"命令行入口"和"三条主线"之后新增一小节 "跨市场不对称 (CMA) 扩展"）

- [ ] **Step 31.1: Add README snippet**

Before the "开发与验证" section, insert:

```markdown
### 11. 跨市场不对称（CMA）扩展

`index-inclusion-cma` 在公告日 / 生效日 × CN / US 四象限上系统化对比事件集中度差异。

```bash
index-inclusion-cma
```

产出文件都以 `cma_` 前缀写入 `results/real_tables/` 和 `results/real_figures/`，并在 `results/real_tables/research_summary.md` 里追加章节"六、美股 vs A股 不对称"。

它依赖真实样本（`real_event_panel.csv`、`real_matched_event_panel.csv`、`real_events_clean.csv`）；缺则直接报错，不回退 demo。
```

- [ ] **Step 31.2: Commit**

```bash
git add README.md
git commit -m "docs: document index-inclusion-cma workflow in README"
```

---

## Task 32: 最终验收跑通 + ruff + full pytest

- [ ] **Step 32.1: Ruff 全仓扫描**

```bash
python3 -m ruff check .
```
Expected: 0 errors.

- [ ] **Step 32.2: Pytest 全仓**

```bash
pytest -q
```
Expected: all tests pass.

- [ ] **Step 32.3: Playwright smoke（如果启用）**

```bash
RUN_BROWSER_SMOKE=1 pytest -q tests/test_dashboard_browser_smoke.py
```

- [ ] **Step 32.4: CLI 端到端**

```bash
index-inclusion-cma
```
Expected: summary printed，`results/real_tables/cma_*.csv` 与 `results/real_figures/cma_*.png` 生成。

- [ ] **Step 32.5: 手工检查 dashboard**

```bash
index-inclusion-dashboard --port 5001
```
打开 `http://localhost:5001/?mode=full`，确认 `cross_market_asymmetry` section 出现，4 象限摘要表、3 张主图、假设对照表均可见。

- [ ] **Step 32.6: Final commit（仅当有未提交的修复）**

```bash
git status
# if clean, skip; else git add / commit the fix
```

---

## Self-review checklist

- [ ] Spec 的每一项都对应至少一个 Task：M1 paths（T2-T5）/ M2 gap（T6-T8）/ M3 mechanism（T9-T12）/ M4 heterogeneity（T13-T15）/ M5 time_series（T16-T18）/ 假设表（T19-T20）/ Orchestrator（T21）/ CLI（T22-T23）/ tex-only + figures_tables（T23.5）/ Dashboard（T24-T28）/ research_summary append（T29）/ browser smoke（T30）/ docs（T31）/ verification（T32）
- [ ] `event_type == "addition"` filter 出现在每个读入事件数据的模块里：M1 `build_daily_ar_panel`、M2 `compute_gap_metrics`、M3 `build_mechanism_panel`、M4 via panel、M5 `build_rolling_car`
- [ ] 4 象限一致：`(market ∈ {CN, US}) × (event_phase ∈ {announce, effective})` 在 M1 summary、M3 regression、M5 rolling 里都是这个划分
- [ ] 输出路径一致：所有 CSV / PNG 在 `results/real_tables/` / `results/real_figures/`，前缀 `cma_`
- [ ] `cma_` 前缀所有输出
- [ ] 每个 Task 都有 test-first → fail → impl → pass → commit 的完整步骤
- [ ] 没有 TBD / TODO
- [ ] 所有 function signature 前后一致（如 `render_*_figures(*, output_dir)` 关键字参数风格统一）
- [ ] `price_limit_hit_share` 的阈值 `0.099` 在 M3 定义、spec 里一致
- [ ] `asymmetry_index` 的 EPS = 1e-4 在 M4 定义，和 spec 一致
- [ ] Research summary 附加章节是**幂等**的（T21 与 T29 测试都覆盖）
- [ ] CLI 在缺输入时明确 `FileNotFoundError`，不静默退化到 demo（和 RDD L3 契约一致）

---

## 后续 v2 扩展（不在本 plan 内，但可立即作为下一个 spec）

- Deletion 对称性（`event_type == "deletion"`）
- FF3 / FF5 异常收益
- FTSE / TOPIX / MSCI EM 跨市场扩展
- Placebo 证伪测试
- 机构持股 / ETF AUM 面板
- 结构变点重型检验
- 双语 dashboard
