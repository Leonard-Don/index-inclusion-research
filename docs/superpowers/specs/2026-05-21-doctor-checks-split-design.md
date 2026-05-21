# Design: Split `doctor/_checks.py` into themed submodules

- **Date:** 2026-05-21
- **Status:** Design approved; pending implementation plan

## Motivation

`src/index_inclusion_research/doctor/_checks.py` is 2296 lines (86 KB) — the
largest source file in the repository. It bundles four unrelated concerns:
30 independent health-check functions, the `CheckResult` dataclass, ~20
module-level path constants, and the doctor CLI runner. The project
evaluation flagged it as the top single-file complexity hotspot.

The sibling package `literature_catalog/` already demonstrates the intended
pattern: underscore-prefixed private submodules (`_data.py`, `_frames.py`,
`_markdown.py`) behind an `__init__.py` facade. This design applies the same
pattern to `doctor/`.

## Scope

**In scope:** split `doctor/_checks.py` into themed private submodules within
the `doctor/` package.

**Out of scope:** moving the 27 top-level `dashboard_*.py` modules into a
`dashboard/` subpackage. That touches ~102 import sites across 18 source
files and 29 test files; it is deliberately deferred and tracked separately.

This is a **pure structural move**. No check logic, thresholds, severities,
messages, or check ordering changes.

## Target layout

```
doctor/
  __init__.py     facade — re-exports the same public API (unchanged surface)
  __main__.py     unchanged
  _common.py      CheckResult; ROOT + DEFAULT_* path constants; _relative_label
  _verdicts.py    verdict / PAP / hypothesis checks
  _readiness.py   H6 / H7 / RDD / matched-sample readiness checks
  _artifacts.py   figure / forest / timeline artifact-freshness checks
  _paper.py       paper-integrity / audit / freshness checks
  _misc.py        results-dir, chart-registry, console-scripts, citation-schema checks
  _runner.py      DEFAULT_CHECKS registry; run_all_checks; result renderers;
                  doctor_exit_code; build_arg_parser; main
```

`_checks.py` is deleted once its contents are distributed.

## Function -> module mapping

| Module | Check functions | Private helpers |
|---|---|---|
| `_verdicts.py` | `check_hypothesis_paper_ids_resolve`, `check_verdicts_csv_health`, `check_paper_verdict_section_synced`, `check_p_gated_verdict_sensitivity`, `check_pending_data_verdicts`, `check_pap_deviation_no_flips`, `check_pap_snapshot_freshness` | `_ensure_pap_deviation_report`, `_parse_snapshot_date` |
| `_readiness.py` | `check_h6_weight_change_readiness`, `check_h7_cn_sector_readiness`, `check_rdd_l3_sample_readiness`, `check_rdd_robustness_panel`, `check_matched_sample_balance`, `check_match_robustness_grid` | — |
| `_artifacts.py` | `check_citation_graph_artifact`, `check_verdict_timeline_artifact`, `check_literature_timeline_artifact`, `check_hs300_rdd_forest_artifact`, `check_cma_verdicts_forest_artifact`, `check_cma_ar_engine_forest_artifact`, `check_cma_2d_robustness_heatmap_artifact`, `check_cma_sensitivity_forest_artifact` | `_forest_artifact_status` |
| `_paper.py` | `check_paper_integrity`, `check_paper_audit`, `check_public_summary_freshness`, `check_paper_skeleton_freshness`, `check_methodology_summary_freshness` | — |
| `_misc.py` | `check_results_directory_populated`, `check_chart_builders_register`, `check_console_scripts_importable`, `check_heuristic_citation_centrality_schema` | — |

30 check functions total. Each themed module imports shared symbols
(`CheckResult`, path constants, `_relative_label`) from `_common`.

## Facade and compatibility

- **Public API is byte-identical.** `doctor/__init__.py` continues to
  re-export every currently-exported name (`check_*`, `CheckResult`,
  `DEFAULT_CHECKS`, `DEFAULT_VERDICTS_CSV`, `ROOT`, `run_all_checks`,
  `render_results`, `doctor_exit_code`, `main`, ...) — now sourced from the
  new submodules instead of `_checks`. `__all__` stays equivalent.
- The 18 external source files and all tests that import from
  `index_inclusion_research.doctor` need **no changes** — verified:
  `from ._checks import` appears only inside `doctor/__init__.py`.
- `_runner.py` builds `DEFAULT_CHECKS` from the themed modules. **The
  tuple's order is preserved exactly** — doctor output order is observable
  and asserted by tests.
- `__init__.py`'s `main()` wrapper, which preserves monkeypatch hooks for
  `run_all_checks`, retargets from the old `_impl` (`_checks`) to `_runner`.
  Behavior is unchanged: `test_doctor.py` patches `doctor.run_all_checks`
  and the CLI honors it.

## Verification

- `make ci` (ruff + mypy + coverage gate + doctor-strict) passes unchanged.
- Full `pytest` passes — especially `test_doctor.py`, plus suites that
  exercise doctor checks transitively (`test_paper_integrity.py` and
  `test_submission_ready.py`, which re-run doctor checks).
- Behavioral equivalence: `index-inclusion-doctor --no-color` output is
  byte-identical before and after the split.

## Risks

Low. The refactor is contained within the `doctor/` package; no external
import path changes. The only non-mechanical points are the `__init__.py`
re-export rewrite and the `main()` monkeypatch retarget — both covered by
`test_doctor.py`. The byte-identical-output check is the backstop.
