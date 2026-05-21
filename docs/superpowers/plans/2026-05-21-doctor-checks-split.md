# doctor/_checks.py Split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the 2296-line `src/index_inclusion_research/doctor/_checks.py` into seven focused private submodules behind the existing `doctor/__init__.py` facade, with zero change to public API or behavior.

**Architecture:** Incremental extraction. Tasks 1-6 each move one cohesive group out of `_checks.py` into a new private submodule; `_checks.py` re-imports the moved names so every intermediate commit stays green (the doctor registry, runner, and `main` stay in `_checks.py` through Task 6, which keeps the unchanged `__init__.py` working). Task 7 then extracts the runner, rewrites `__init__.py`, and deletes `_checks.py` as one atomic change.

**Tech Stack:** Python 3.11, pytest, ruff. No new dependencies. Spec: `docs/superpowers/specs/2026-05-21-doctor-checks-split-design.md`.

**Conventions for every task:**
- Run commands from the repo root with the project venv active (use `.venv/bin/python` if it is not).
- "Move function X" means: cut its full body (and decorators) from `_checks.py`, paste into the target module, then add `from <target> import X` back into `_checks.py` so existing references keep resolving.
- After moving, each new module needs the imports its functions use. Copy a superset from `_checks.py`'s import block, then run `ruff check` — `F401` flags unused imports; delete those.
- Per-task gate: `python -m pytest tests/test_doctor.py -q` passes and `ruff check src/index_inclusion_research/doctor/` is clean.

---

## Task 1: Capture baselines and extract `_common.py`

**Files:**
- Create: `src/index_inclusion_research/doctor/_common.py`
- Modify: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Capture the behavioral baseline**

```bash
python -m index_inclusion_research.doctor --no-color > /tmp/doctor_baseline.txt 2>&1
python -c "import index_inclusion_research.doctor as d; print('\n'.join(sorted(n for n in dir(d) if not n.startswith('_'))))" > /tmp/doctor_api_baseline.txt
```

These two files are the equivalence oracle for the whole refactor. Do not delete them until Task 8.

- [ ] **Step 2: Create `_common.py`**

New module holding the shared primitives. Header:

```python
"""Shared primitives for doctor checks: result type, paths, helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from index_inclusion_research import paths
```

Then move, from `_checks.py` into `_common.py`, in this order:
- `ROOT = paths.project_root()` and every module-level `DEFAULT_*` constant (the `ROOT`/`DEFAULT_*` block near the top of `_checks.py`).
- the `CheckResult` dataclass.
- the `_relative_label` helper.

Add any extra imports those definitions need (e.g. `datetime`) and let Step 4's ruff run prune extras.

- [ ] **Step 3: Re-point `_checks.py` at `_common.py`**

Delete the moved definitions from `_checks.py`. At the top of `_checks.py` (after `from __future__ import annotations`) add:

```python
from ._common import _relative_label  # noqa: F401
from ._common import *  # noqa: F401,F403
```

`from ._common import *` re-exposes `CheckResult`, `ROOT`, and every `DEFAULT_*` constant so the check bodies still resolve them. The explicit line is for `_relative_label`, which `import *` skips because it is underscore-prefixed. `_common.py` needs no `__all__`.

- [ ] **Step 4: Lint and prune imports**

Run: `ruff check src/index_inclusion_research/doctor/`
Fix every `F401` (unused import) it reports in `_common.py` and `_checks.py`. Expected after fixes: `All checks passed!`

- [ ] **Step 5: Run doctor tests**

Run: `python -m pytest tests/test_doctor.py -q`
Expected: PASS (same count as before the task).

- [ ] **Step 6: Commit**

```bash
git add src/index_inclusion_research/doctor/_common.py src/index_inclusion_research/doctor/_checks.py
git commit -m "refactor(doctor): extract shared primitives into _common"
```

---

## Task 2: Extract `_verdicts.py`

**Files:**
- Create: `src/index_inclusion_research/doctor/_verdicts.py`
- Modify: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Create `_verdicts.py`**

Header:

```python
"""Verdict, PAP, and hypothesis-set doctor checks."""

from __future__ import annotations

from ._common import CheckResult, _relative_label
```

Move these functions from `_checks.py` into `_verdicts.py` (with their decorators and full bodies):
- `check_hypothesis_paper_ids_resolve`
- `check_verdicts_csv_health`
- `check_paper_verdict_section_synced`
- `check_p_gated_verdict_sensitivity`
- `check_pending_data_verdicts`
- `check_pap_deviation_no_flips`
- `check_pap_snapshot_freshness`
- helper `_ensure_pap_deviation_report`
- helper `_parse_snapshot_date`

Add the imports these bodies use (copy from `_checks.py`'s import block; common ones: `pandas as pd`, `datetime`, `Path`). Import any `DEFAULT_*` constants they reference explicitly from `._common` (e.g. `from ._common import DEFAULT_VERDICTS_CSV, DEFAULT_PAP_DEVIATION_REPORT_CSV`).

- [ ] **Step 2: Re-point `_checks.py`**

Delete the moved functions from `_checks.py`. Add near the top:

```python
from ._verdicts import (  # noqa: F401
    _ensure_pap_deviation_report,
    _parse_snapshot_date,
    check_hypothesis_paper_ids_resolve,
    check_p_gated_verdict_sensitivity,
    check_pap_deviation_no_flips,
    check_pap_snapshot_freshness,
    check_paper_verdict_section_synced,
    check_pending_data_verdicts,
    check_verdicts_csv_health,
)
```

- [ ] **Step 3: Lint** — `ruff check src/index_inclusion_research/doctor/`, fix all `F401`. Expected: `All checks passed!`
- [ ] **Step 4: Test** — `python -m pytest tests/test_doctor.py -q`, Expected: PASS.
- [ ] **Step 5: Commit**

```bash
git add src/index_inclusion_research/doctor/_verdicts.py src/index_inclusion_research/doctor/_checks.py
git commit -m "refactor(doctor): extract verdict/PAP checks into _verdicts"
```

---

## Task 3: Extract `_readiness.py`

**Files:**
- Create: `src/index_inclusion_research/doctor/_readiness.py`
- Modify: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Create `_readiness.py`**

Header:

```python
"""Data- and sample-readiness doctor checks."""

from __future__ import annotations

from ._common import CheckResult, _relative_label
```

Add the imports the moved bodies use plus the `DEFAULT_*` constants they reference from `._common`. Move these functions from `_checks.py`:
- `check_h6_weight_change_readiness`
- `check_h7_cn_sector_readiness`
- `check_rdd_l3_sample_readiness`
- `check_rdd_robustness_panel`
- `check_matched_sample_balance`
- `check_match_robustness_grid`

- [ ] **Step 2: Re-point `_checks.py`**

Delete the moved functions; add near the top:

```python
from ._readiness import (  # noqa: F401
    check_h6_weight_change_readiness,
    check_h7_cn_sector_readiness,
    check_match_robustness_grid,
    check_matched_sample_balance,
    check_rdd_l3_sample_readiness,
    check_rdd_robustness_panel,
)
```

- [ ] **Step 3: Lint** — `ruff check src/index_inclusion_research/doctor/`, fix `F401`.
- [ ] **Step 4: Test** — `python -m pytest tests/test_doctor.py -q`, Expected: PASS.
- [ ] **Step 5: Commit**

```bash
git add src/index_inclusion_research/doctor/_readiness.py src/index_inclusion_research/doctor/_checks.py
git commit -m "refactor(doctor): extract readiness checks into _readiness"
```

---

## Task 4: Extract `_artifacts.py`

**Files:**
- Create: `src/index_inclusion_research/doctor/_artifacts.py`
- Modify: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Create `_artifacts.py`**

Header:

```python
"""Figure / forest / timeline artifact-freshness doctor checks."""

from __future__ import annotations

from ._common import CheckResult, _relative_label
```

Add the imports the moved bodies use plus the `DEFAULT_*` constants they reference from `._common`. Move these functions from `_checks.py`:
- `check_citation_graph_artifact`
- `check_verdict_timeline_artifact`
- `check_literature_timeline_artifact`
- `check_hs300_rdd_forest_artifact`
- `check_cma_verdicts_forest_artifact`
- `check_cma_ar_engine_forest_artifact`
- `check_cma_2d_robustness_heatmap_artifact`
- `check_cma_sensitivity_forest_artifact`
- helper `_forest_artifact_status`

- [ ] **Step 2: Re-point `_checks.py`**

Delete the moved functions; add near the top:

```python
from ._artifacts import (  # noqa: F401
    _forest_artifact_status,
    check_citation_graph_artifact,
    check_cma_2d_robustness_heatmap_artifact,
    check_cma_ar_engine_forest_artifact,
    check_cma_sensitivity_forest_artifact,
    check_cma_verdicts_forest_artifact,
    check_hs300_rdd_forest_artifact,
    check_literature_timeline_artifact,
    check_verdict_timeline_artifact,
)
```

- [ ] **Step 3: Lint** — `ruff check src/index_inclusion_research/doctor/`, fix `F401`.
- [ ] **Step 4: Test** — `python -m pytest tests/test_doctor.py -q`, Expected: PASS.
- [ ] **Step 5: Commit**

```bash
git add src/index_inclusion_research/doctor/_artifacts.py src/index_inclusion_research/doctor/_checks.py
git commit -m "refactor(doctor): extract artifact-freshness checks into _artifacts"
```

---

## Task 5: Extract `_paper.py`

**Files:**
- Create: `src/index_inclusion_research/doctor/_paper.py`
- Modify: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Create `_paper.py`**

Header:

```python
"""Paper integrity / audit / freshness doctor checks."""

from __future__ import annotations

from ._common import CheckResult, _relative_label
```

Add the imports the moved bodies use plus the `DEFAULT_*` constants they reference from `._common`. Move these functions from `_checks.py`:
- `check_paper_integrity`
- `check_paper_audit`
- `check_public_summary_freshness`
- `check_paper_skeleton_freshness`
- `check_methodology_summary_freshness`

- [ ] **Step 2: Re-point `_checks.py`**

Delete the moved functions; add near the top:

```python
from ._paper import (  # noqa: F401
    check_methodology_summary_freshness,
    check_paper_audit,
    check_paper_integrity,
    check_paper_skeleton_freshness,
    check_public_summary_freshness,
)
```

- [ ] **Step 3: Lint** — `ruff check src/index_inclusion_research/doctor/`, fix `F401`.
- [ ] **Step 4: Test** — `python -m pytest tests/test_doctor.py -q`, Expected: PASS.
- [ ] **Step 5: Commit**

```bash
git add src/index_inclusion_research/doctor/_paper.py src/index_inclusion_research/doctor/_checks.py
git commit -m "refactor(doctor): extract paper checks into _paper"
```

---

## Task 6: Extract `_misc.py`

**Files:**
- Create: `src/index_inclusion_research/doctor/_misc.py`
- Modify: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Create `_misc.py`**

Header:

```python
"""Results-directory, registry, and schema doctor checks."""

from __future__ import annotations

from ._common import CheckResult, _relative_label
```

Add the imports the moved bodies use plus the `DEFAULT_*` constants they reference from `._common`. Move these functions from `_checks.py`:
- `check_results_directory_populated`
- `check_chart_builders_register`
- `check_console_scripts_importable`
- `check_heuristic_citation_centrality_schema`

- [ ] **Step 2: Re-point `_checks.py`**

Delete the moved functions; add near the top:

```python
from ._misc import (  # noqa: F401
    check_chart_builders_register,
    check_console_scripts_importable,
    check_heuristic_citation_centrality_schema,
    check_results_directory_populated,
)
```

- [ ] **Step 3: Lint** — `ruff check src/index_inclusion_research/doctor/`, fix `F401`.
- [ ] **Step 4: Test** — `python -m pytest tests/test_doctor.py -q`, Expected: PASS.
- [ ] **Step 5: Commit**

```bash
git add src/index_inclusion_research/doctor/_misc.py src/index_inclusion_research/doctor/_checks.py
git commit -m "refactor(doctor): extract misc checks into _misc"
```

---

## Task 7: Extract `_runner.py`, rewrite `__init__.py`, delete `_checks.py`

This task is atomic: the runner extraction, the facade rewrite, and the `_checks.py` deletion are interdependent (the `main()` monkeypatch hook must move with the runner), so they share one commit. After Task 6, `_checks.py` still holds the registry, runner, and `main` — those move here.

**Files:**
- Create: `src/index_inclusion_research/doctor/_runner.py`
- Modify: `src/index_inclusion_research/doctor/__init__.py`
- Delete: `src/index_inclusion_research/doctor/_checks.py`

- [ ] **Step 1: Create `_runner.py`**

Header:

```python
"""Doctor check registry, runner, result rendering, and CLI."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence

from ._common import CheckResult
from ._artifacts import (
    check_citation_graph_artifact,
    check_cma_2d_robustness_heatmap_artifact,
    check_cma_ar_engine_forest_artifact,
    check_cma_sensitivity_forest_artifact,
    check_cma_verdicts_forest_artifact,
    check_hs300_rdd_forest_artifact,
    check_literature_timeline_artifact,
    check_verdict_timeline_artifact,
)
from ._misc import (
    check_chart_builders_register,
    check_console_scripts_importable,
    check_heuristic_citation_centrality_schema,
    check_results_directory_populated,
)
from ._paper import (
    check_methodology_summary_freshness,
    check_paper_audit,
    check_paper_integrity,
    check_paper_skeleton_freshness,
    check_public_summary_freshness,
)
from ._readiness import (
    check_h6_weight_change_readiness,
    check_h7_cn_sector_readiness,
    check_match_robustness_grid,
    check_matched_sample_balance,
    check_rdd_l3_sample_readiness,
    check_rdd_robustness_panel,
)
from ._verdicts import (
    check_hypothesis_paper_ids_resolve,
    check_p_gated_verdict_sensitivity,
    check_pap_deviation_no_flips,
    check_pap_snapshot_freshness,
    check_paper_verdict_section_synced,
    check_pending_data_verdicts,
    check_verdicts_csv_health,
)
```

Then move from `_checks.py` into `_runner.py`, unchanged: `DEFAULT_CHECKS`, `run_all_checks`, `render_results`, `results_summary`, `results_payload`, `render_results_json`, `doctor_exit_code`, `build_arg_parser`, `main`. Some `check_*` names in the import block above may already be imported by `_checks.py`'s re-import lines — once `_checks.py` is deleted in Step 3 they are needed here; keep them and let Step 5's ruff confirm none are unused.

**Critical:** the `DEFAULT_CHECKS` tuple must keep the exact same check order it has today — doctor output order is asserted by tests. Copy the tuple verbatim.

- [ ] **Step 2: Rewrite `__init__.py`**

Replace the whole file with:

```python
"""Project health-check CLI and reusable doctor checks."""

from __future__ import annotations

from collections.abc import Sequence

from . import _artifacts, _common, _misc, _paper, _readiness, _runner, _verdicts
from ._common import (
    ROOT as ROOT,
)
from ._common import (
    CheckResult as CheckResult,
)
from ._runner import (
    DEFAULT_CHECKS as DEFAULT_CHECKS,
)
from ._runner import (
    doctor_exit_code as doctor_exit_code,
)
from ._runner import (
    render_results as render_results,
)
from ._runner import (
    render_results_json as render_results_json,
)
from ._runner import (
    results_payload as results_payload,
)
from ._runner import (
    results_summary as results_summary,
)
from ._runner import (
    run_all_checks as run_all_checks,
)

# Re-export every remaining public name (all check_* functions, every
# DEFAULT_* constant) from the themed submodules so external callers keep
# importing from `index_inclusion_research.doctor` unchanged.
for _module in (_common, _verdicts, _readiness, _artifacts, _paper, _misc, _runner):
    for _name, _value in vars(_module).items():
        if not _name.startswith("_") and _name not in globals() and _name != "main":
            globals()[_name] = _value

__all__ = [
    _name
    for _name in globals()
    if not _name.startswith("_") and _name not in {"Sequence", "annotations"}
]
__all__.append("main")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the doctor CLI while preserving package-level monkeypatch hooks."""

    original_run_all_checks = _runner.run_all_checks
    try:
        _runner.run_all_checks = globals()["run_all_checks"]
        return _runner.main(argv)
    finally:
        _runner.run_all_checks = original_run_all_checks
```

- [ ] **Step 3: Delete `_checks.py`**

```bash
git rm src/index_inclusion_research/doctor/_checks.py
```

- [ ] **Step 4: Verify the public API is unchanged**

```bash
python -c "import index_inclusion_research.doctor as d; print('\n'.join(sorted(n for n in dir(d) if not n.startswith('_'))))" > /tmp/doctor_api_after.txt
diff /tmp/doctor_api_baseline.txt /tmp/doctor_api_after.txt && echo "API IDENTICAL"
```

Expected: `API IDENTICAL`, no diff. If a name is missing, add an explicit `from ._<module> import <name> as <name>` line for it. (A clean refactor should not drop anything: every import in today's `_checks.py` passes ruff, so each one survives in some submodule and the catch-all re-exports it.)

- [ ] **Step 5: Lint** — `ruff check src/index_inclusion_research/doctor/`, Expected: `All checks passed!`
- [ ] **Step 6: Test** — `python -m pytest tests/test_doctor.py -q`, Expected: PASS.
- [ ] **Step 7: Commit**

```bash
git add src/index_inclusion_research/doctor/_runner.py src/index_inclusion_research/doctor/__init__.py
git commit -m "refactor(doctor): extract runner into _runner, drop _checks.py shim"
```

---

## Task 8: Full verification

**Files:** none (verification only).

- [ ] **Step 1: Behavioral equivalence**

```bash
python -m index_inclusion_research.doctor --no-color > /tmp/doctor_after.txt 2>&1
diff /tmp/doctor_baseline.txt /tmp/doctor_after.txt && echo "DOCTOR OUTPUT IDENTICAL"
```

Expected: `DOCTOR OUTPUT IDENTICAL`, no diff. Any diff is a regression — investigate before proceeding.

- [ ] **Step 2: Full test suite**

Run: `python -m pytest -q`
Expected: same pass/skip counts as before the refactor (about 1193 passed, 2 skipped).

- [ ] **Step 3: Lint the whole tree**

Run: `ruff check .`
Expected: `All checks passed!`

- [ ] **Step 4: Push and let CI confirm types**

```bash
git push origin main
```

GitHub Actions runs `make ci` (ruff + mypy + coverage gate + doctor-strict). Watch the run to `success` — mypy is the authoritative type gate here, since a local `make ci` can spuriously fail typecheck when the `.venv` is out of sync with `uv.lock` (run `make sync` first if checking locally).

- [ ] **Step 5: Clean up baseline files**

```bash
rm /tmp/doctor_baseline.txt /tmp/doctor_api_baseline.txt /tmp/doctor_after.txt /tmp/doctor_api_after.txt
```

---

## Done criteria

- `doctor/_checks.py` no longer exists; `doctor/` contains `_common.py`, `_verdicts.py`, `_readiness.py`, `_artifacts.py`, `_paper.py`, `_misc.py`, `_runner.py`, plus `__init__.py` and `__main__.py`.
- `index-inclusion-doctor` output is byte-identical to the pre-refactor baseline.
- The public API of `index_inclusion_research.doctor` is unchanged; no external source file or test was modified.
- Full `pytest` and `ruff` pass; GitHub CI is green.
