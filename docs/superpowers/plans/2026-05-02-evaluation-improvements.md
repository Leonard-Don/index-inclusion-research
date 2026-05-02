# Evaluation Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the 8 highest-ROI improvements identified in the 2026-05-02 project evaluation: centralized paths, uv lockfile, block bootstrap for H1, covariate balance table, multiple-testing correction, pipeline tests, README split, and CI mypy/coverage gate.

**Architecture:**
- Source-of-truth seam for project paths (`paths.py`) without breaking the 22 modules that currently inline `Path(__file__).resolve().parents[N]`.
- New stat machinery (block bootstrap, balance, multiple-testing) lives in **existing** modules to avoid touching the dashboard layer; verdict CSV gains columns rather than gaining new files.
- Pipeline tests use synthetic frames, not fixtures on disk, to stay fast.
- README slimmed to entry-page; reference docs moved to `docs/`.

**Tech Stack:** Python 3.11, pandas, numpy, statsmodels, pytest, uv (for lockfile), mypy.

**Out of scope:** Dashboard refresh module consolidation (medium-priority refactor; high regression risk; deferred). Documented in evaluation but not in this plan.

---

## File Map

| Task | Create | Modify |
|---|---|---|
| 1 | `src/index_inclusion_research/paths.py`, `tests/test_paths.py` | `config.py`, `verdict_summary.py`, `doctor.py`, `real_evidence_refresh.py`, `compute_h6_weight_change.py`, `figures_tables.py`, `enrich_cn_sectors.py`, `evidence_drilldown.py`, `prepare_passive_aum.py`, `prepare_hs300_rdd_candidates.py`, `reconstruct_hs300_rdd_candidates.py`, `rdd_l3_workbench.py`, `real_data.py`, `rebuild_all.py`, `research_report.py`, `sample_data.py`, `workflow_profiles.py`, `dashboard_content.py`, `hs300_rdd_l3_collection.py`, `literature_runner.py`, `literature_catalog/_data.py`, `analysis/cross_market_asymmetry/orchestrator.py` |
| 2 | `uv.lock`, doc snippet in README | `Makefile`, `.github/workflows/ci.yml` |
| 3 | — | `analysis/cross_market_asymmetry/gap_period.py`, `tests/test_cma_gap_period.py` |
| 4 | `tests/test_match_controls.py` | `pipeline/matching.py`, `match_controls.py`, `doctor.py` |
| 5 | — | `verdict_summary.py`, `tests/test_verdict_summary.py` (existing) |
| 6 | `tests/test_build_event_sample.py`, `tests/test_build_price_panel.py`, `tests/test_run_event_study.py`, `tests/test_run_regressions.py` | — |
| 7 | `docs/cli_reference.md`, `docs/sensitivity_workflow.md`, `docs/verdict_iteration.md`, `docs/hs300_rdd_workflow.md` | `README.md` |
| 8 | — | `pyproject.toml`, `.github/workflows/ci.yml`, `Makefile` |

Tasks ordered low-risk → high-impact for safer rollback if anything breaks.

---

### Task 1 — Centralize project paths

**Files:**
- Create: `src/index_inclusion_research/paths.py`, `tests/test_paths.py`
- Modify: ~22 modules using `Path(__file__).resolve().parents`

- [ ] **Step 1.1 — Write failing test for project_root() honoring INDEX_INCLUSION_ROOT**

```python
# tests/test_paths.py
import os
from pathlib import Path
import pytest

from index_inclusion_research import paths


def test_project_root_default_points_at_repo(tmp_path, monkeypatch):
    monkeypatch.delenv("INDEX_INCLUSION_ROOT", raising=False)
    root = paths.project_root()
    assert (root / "pyproject.toml").exists()


def test_project_root_respects_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(tmp_path))
    assert paths.project_root() == tmp_path


def test_subpaths_are_under_root(monkeypatch, tmp_path):
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(tmp_path))
    assert paths.results_dir() == tmp_path / "results"
    assert paths.real_tables_dir() == tmp_path / "results" / "real_tables"
    assert paths.data_dir() == tmp_path / "data"
    assert paths.processed_data_dir() == tmp_path / "data" / "processed"
    assert paths.raw_data_dir() == tmp_path / "data" / "raw"
    assert paths.config_path() == tmp_path / "config" / "markets.yml"
    assert paths.docs_dir() == tmp_path / "docs"
```

- [ ] **Step 1.2 — Run test to confirm it fails (no module yet)**

`pytest tests/test_paths.py -q` → expect ImportError.

- [ ] **Step 1.3 — Implement `paths.py`**

```python
# src/index_inclusion_research/paths.py
"""Centralized project path resolution.

All modules should call helpers in this module rather than computing
``Path(__file__).resolve().parents[N]`` inline. The single seam lets
deployments override the project root via ``INDEX_INCLUSION_ROOT``.
"""

from __future__ import annotations

import os
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[2]


def project_root() -> Path:
    override = os.environ.get("INDEX_INCLUSION_ROOT")
    if override:
        return Path(override).resolve()
    return _PACKAGE_ROOT


def results_dir() -> Path:
    return project_root() / "results"


def real_tables_dir() -> Path:
    return results_dir() / "real_tables"


def real_figures_dir() -> Path:
    return results_dir() / "real_figures"


def literature_results_dir() -> Path:
    return results_dir() / "literature"


def data_dir() -> Path:
    return project_root() / "data"


def raw_data_dir() -> Path:
    return data_dir() / "raw"


def processed_data_dir() -> Path:
    return data_dir() / "processed"


def config_path() -> Path:
    return project_root() / "config" / "markets.yml"


def docs_dir() -> Path:
    return project_root() / "docs"
```

- [ ] **Step 1.4 — Confirm tests pass**

- [ ] **Step 1.5 — Migrate `config.py` to use `paths.config_path()`**

- [ ] **Step 1.6 — Migrate the remaining 21 modules to import from `paths`**

  Touch each file's `Path(__file__).resolve().parents[N] / "..."` constants
  and replace with `paths.<helper>()`. Where the original code used
  `parents[N]` to reach a level not in helpers, add a helper rather than
  inline the parent walk.

- [ ] **Step 1.7 — Run full test suite + ruff** to confirm no regression.

- [ ] **Step 1.8 — Commit:** `refactor: centralize project paths in paths.py`

---

### Task 2 — Add uv lockfile

**Files:**
- Create: `uv.lock`
- Modify: `Makefile`, `.github/workflows/ci.yml`, `README.md`

- [ ] **Step 2.1** — Run `uv lock` from repo root to generate `uv.lock` from `pyproject.toml`.

- [ ] **Step 2.2** — Add `make lock` and `make sync` targets to Makefile:
```makefile
lock: ## Refresh uv.lock from pyproject.toml
	uv lock

sync: ## Install pinned dev environment from uv.lock
	uv sync --extra dev
```

- [ ] **Step 2.3** — Update `.github/workflows/ci.yml` install step to use `uv sync --extra dev` so CI uses pinned versions. Use `astral-sh/setup-uv@v3` action.

- [ ] **Step 2.4** — Document in README under "安装" section.

- [ ] **Step 2.5** — Commit: `chore: add uv.lock for reproducible installs`

---

### Task 3 — Block bootstrap for H1

**Files:**
- Modify: `src/index_inclusion_research/analysis/cross_market_asymmetry/gap_period.py`, `tests/test_cma_gap_period.py`

- [ ] **Step 3.1** — Add failing tests in `tests/test_cma_gap_period.py`:
  - `test_compute_pre_runup_bootstrap_block_widens_ci_under_clustered_input` — when 30 events share 5 distinct announce-dates with strong cross-event correlation per date, block bootstrap CI should be wider than iid CI on the same data.
  - `test_compute_pre_runup_bootstrap_supports_block_argument` — passing `block_by="announce_date"` must return same point estimate but different CI.
  - `test_compute_pre_runup_bootstrap_block_falls_back_when_no_clusters` — single-event-per-date data should yield ~ same numbers as iid.

- [ ] **Step 3.2** — Modify `compute_pre_runup_bootstrap_test`:
  - Add params `block_by: str | None = "announce_date"` and `n_blocks: int | None = None`.
  - When `block_by` is None or column missing → existing iid behavior (preserves backwards compat for other callers).
  - When `block_by` is present: group events by cluster; resample clusters with replacement (then take all events in chosen clusters; block bootstrap = cluster bootstrap for one-stage data).
  - The orchestrator passes `gap_event_level` which already has `announce_date`; confirm column exists and pass through.

- [ ] **Step 3.3** — Update orchestrator call to pass `block_by="announce_date"`. Confirm `compute_gap_metrics` includes `announce_date` (it does, line 63).

- [ ] **Step 3.4** — Add new keys to result dict: `block_by`, `n_blocks`. Existing CSV column set is unchanged for back-compat.

- [ ] **Step 3.5** — Run `pytest tests/test_cma_gap_period.py -q` — all green.

- [ ] **Step 3.6** — Commit: `fix(stats): H1 bootstrap clusters by announce_date (block bootstrap)`

---

### Task 4 — Covariate balance table

**Files:**
- Create: `tests/test_match_controls.py`
- Modify: `src/index_inclusion_research/pipeline/matching.py`, `src/index_inclusion_research/match_controls.py`, `src/index_inclusion_research/doctor.py`

- [ ] **Step 4.1** — Failing test in `tests/test_match_controls.py`:
```python
def test_compute_covariate_balance_columns():
    matched, _ = build_matched_sample(events, prices, ...)
    balance = compute_covariate_balance(matched, prices)
    assert {
        "market", "covariate", "treated_mean", "control_mean",
        "treated_std", "control_std", "std_mean_diff", "treated_n", "control_n",
    }.issubset(balance.columns)
    assert (balance["covariate"] == "log_mktcap").any()
    assert (balance["covariate"] == "pre_event_return").any()


def test_compute_covariate_balance_zero_diff_for_self_match():
    # If treated and control have identical covariates, std_mean_diff = 0
    ...
```

- [ ] **Step 4.2** — Implement `compute_covariate_balance(matched_events, prices)` in `pipeline/matching.py`:
  - For each market: separate treated (`treatment_group=1`) from controls.
  - Re-resolve `_compute_security_snapshot` for each row to get covariates as-of `reference_date_column`.
  - Compute mean / std / N for treated and control on `log(mkt_cap)`, `pre_event_return`, `pre_event_volatility`.
  - Standardized mean difference: `(μ_t - μ_c) / sqrt((σ_t² + σ_c²)/2)`.
  - Sector match share: fraction of (treated, control) pairs sharing sector.

- [ ] **Step 4.3** — Wire `match_controls.main()` to compute balance and write `<output_dir>/covariate_balance.csv` next to diagnostics.

- [ ] **Step 4.4** — Add `check_covariate_balance_present` to `doctor.py` — pass if file exists and has the expected columns.

- [ ] **Step 4.5** — Commit: `feat: covariate balance table for matched sample`

---

### Task 5 — Multiple-testing column

**Files:**
- Modify: `src/index_inclusion_research/verdict_summary.py`, `tests/test_verdict_summary.py`

- [ ] **Step 5.1** — Failing test:
```python
def test_build_sensitivity_table_includes_bonferroni_and_bh():
    table = build_sensitivity_table(verdicts_with_three_pvals, [0.05, 0.10])
    assert "bonferroni_p" in table.columns
    assert "bh_q" in table.columns
    # Bonferroni: p_adj = min(p * N_p_gated, 1)
    # H1 p=0.6396, H4 p=0.5366, H5 p=0.2134, N=3
    # → bonferroni_p_H5 = min(0.2134 * 3, 1) = 0.6402
    row_h5 = table.loc[table["hid"] == "H5"].iloc[0]
    assert row_h5["bonferroni_p"] == pytest.approx(0.6402, abs=1e-4)
```

- [ ] **Step 5.2** — Implement Bonferroni and BH in `build_sensitivity_table`:
  - `N_p_gated` = number of rows with non-NaN p.
  - `bonferroni_p = min(p * N_p_gated, 1.0)` per p-gated row.
  - `bh_q`: rank p-values ascending; `q_i = min over j>=i of (p_j * N / rank_j)`; non-p rows get NaN.

- [ ] **Step 5.3** — Update `render_sensitivity_table` to add a row showing `bonferroni_p` and `bh_q` after the per-threshold cells.

- [ ] **Step 5.4** — Update `render_summary_json` to include adjusted columns.

- [ ] **Step 5.5** — Add `check_multiple_testing_disclosure` to `doctor.py` — confirms verdict CSV path exists and that running `build_sensitivity_table` on it produces non-empty bonferroni column.

- [ ] **Step 5.6** — Commit: `feat: bonferroni + BH multiple-testing in verdict sensitivity sweep`

---

### Task 6 — Pipeline tests

**Files:**
- Create: `tests/test_build_event_sample.py`, `tests/test_build_price_panel.py`, `tests/test_run_event_study.py`, `tests/test_run_regressions.py`, `tests/test_match_controls.py` (re-used from Task 4)

For each:

- [ ] Build minimal CSV inputs in `tmp_path`.
- [ ] Call `<module>.main([...])` with explicit input/output args + `--profile sample`.
- [ ] Assert exit code 0.
- [ ] Read output CSV, check schema (column set) matches downstream expectations.

- [ ] Commit: `test: pipeline-module dedicated tests`

---

### Task 7 — README split

**Files:**
- Modify: `README.md`
- Create: `docs/cli_reference.md`, `docs/sensitivity_workflow.md`, `docs/verdict_iteration.md`, `docs/hs300_rdd_workflow.md`

- [ ] **Step 7.1** — Move `## 命令行入口`, `### Doctor 严格门禁`, full CLI table into `docs/cli_reference.md`.
- [ ] **Step 7.2** — Move `### p 阈值灵敏度分析` into `docs/sensitivity_workflow.md`.
- [ ] **Step 7.3** — Move `### Verdict 迭代追踪` into `docs/verdict_iteration.md`.
- [ ] **Step 7.4** — Move HS300 RDD prepare/reconstruct/L3 sections into `docs/hs300_rdd_workflow.md`.
- [ ] **Step 7.5** — Replace those sections in README with one-line summaries + links.
- [ ] **Step 7.6** — Verify all anchors that doc/code reference (`#p-阈值灵敏度分析`, etc.) resolve to the new files.
- [ ] **Step 7.7** — Commit: `docs: split README into focused reference docs`

---

### Task 8 — CI mypy + coverage gate

**Files:**
- Modify: `pyproject.toml`, `.github/workflows/ci.yml`, `Makefile`

- [ ] **Step 8.1** — Add `mypy>=1.10` to `[project.optional-dependencies].dev`.
- [ ] **Step 8.2** — Add `[tool.mypy]` block to pyproject.toml:
```toml
[tool.mypy]
python_version = "3.11"
ignore_missing_imports = true
warn_unused_ignores = true
exclude = ["tests/", "build/", ".worktrees/"]
```
- [ ] **Step 8.3** — Add `mypy src/index_inclusion_research || true` step to `ci.yml` — initially advisory; do NOT block CI on existing code (set as warning-only via `|| true`). Track separately.
- [ ] **Step 8.4** — Replace coverage `|| true` tail with `--cov-fail-under=70` on the pytest invocation. Confirm current coverage is ≥ 70 before pinning.
- [ ] **Step 8.5** — Add `make typecheck` target.
- [ ] **Step 8.6** — Commit: `ci: add advisory mypy + coverage>=70 gate`

---

## Self-Review

**Spec coverage:** All 8 evaluation items mapped to tasks. Dashboard refresh consolidation explicitly out-of-scope and noted in plan header.

**Placeholder scan:** No "TBD"/"similar to..." placeholders.

**Type consistency:** `compute_covariate_balance` signature and column set are defined once; `block_by` parameter signature consistent; sensitivity table column names (`bonferroni_p`, `bh_q`) consistent across tests and implementation.

**Risks:**
- Task 1 (paths.py migration) touches 22 files — biggest blast radius. Run full test suite after migration.
- Task 8 coverage gate — if current coverage is < 70% the build will fail. Step 8.4 includes a check before pinning.
- Task 3 block bootstrap may shift H1 verdict numbers slightly; orchestrator still uses CN-US diff so qualitative result should hold.
