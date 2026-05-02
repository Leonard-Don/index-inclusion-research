"""Project health-check CLI: ``index-inclusion-doctor``.

Runs a set of bounded sanity checks and prints PASS / WARN / FAIL per
check with a short suggested fix. Useful for:

- onboarding (run after fresh install to verify everything is wired)
- CI gates (return code = number of failed checks)
- debugging "I broke something but I'm not sure what" moments

Each check is a small function returning a ``CheckResult`` so they
stay easily testable in isolation; the CLI just composes them.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

from index_inclusion_research import paths

ROOT = paths.project_root()
DEFAULT_VERDICTS_CSV = ROOT / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
DEFAULT_RESULTS_DIR = ROOT / "results" / "real_tables"
DEFAULT_RDD_STATUS_DIR = ROOT / "results" / "literature" / "hs300_rdd"
DEFAULT_PAPER_VERDICTS_DOC = ROOT / "docs" / "paper_outline_verdicts.md"
DEFAULT_EVENT_COUNTS_CSV = DEFAULT_RESULTS_DIR / "event_counts_by_year.csv"
DEFAULT_EVENT_STUDY_SUMMARY_CSV = DEFAULT_RESULTS_DIR / "event_study_summary.csv"
DEFAULT_WEIGHT_CHANGE_CSV = ROOT / "data" / "processed" / "hs300_weight_change.csv"
DEFAULT_HETEROGENEITY_SECTOR_CSV = DEFAULT_RESULTS_DIR / "cma_heterogeneity_sector.csv"
DEFAULT_MATCH_BALANCE_CSV = ROOT / "results" / "real_regressions" / "match_balance.csv"

EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")
EXPECTED_CMA_OUTPUTS: tuple[str, ...] = (
    "cma_hypothesis_verdicts.csv",
    "cma_hypothesis_verdicts.tex",
    "cma_hypothesis_map.csv",
    "cma_track_verdict_summary.csv",
    "cma_mechanism_panel.csv",
    "cma_gap_summary.csv",
    "cma_pre_runup_bootstrap.csv",
    "cma_gap_drift_market_regression.csv",
    "cma_h3_channel_concentration.csv",
    "cma_h5_limit_predictive_regression.csv",
    "cma_h6_weight_robustness.csv",
    "cma_h6_weight_explanation.csv",
)

Status = str  # one of "pass" / "warn" / "fail"

_STATUS_GLYPH: dict[Status, str] = {
    "pass": "✓",
    "warn": "!",
    "fail": "✗",
}
_STATUS_COLOR: dict[Status, str] = {
    "pass": "\033[32m",
    "warn": "\033[33m",
    "fail": "\033[31m",
}
_RESET = "\033[0m"


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: Status
    message: str
    fix: str = ""
    details: tuple[str, ...] = field(default_factory=tuple)


# ── individual checks ────────────────────────────────────────────────


def _relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def check_hypothesis_paper_ids_resolve(
    *,
    catalog_loader: Callable[[], pd.DataFrame] | None = None,
) -> CheckResult:
    """Every paper_id on every H1..H7 must exist in the literature catalog."""
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        HYPOTHESES,
    )

    if catalog_loader is None:
        from index_inclusion_research.literature_catalog import (
            build_literature_catalog_frame,
        )

        catalog_loader = build_literature_catalog_frame

    catalog = catalog_loader()
    catalog_ids = set(catalog["paper_id"].astype(str).tolist())
    missing: list[str] = []
    for h in HYPOTHESES:
        for pid in h.paper_ids:
            if pid not in catalog_ids:
                missing.append(f"{h.hid} → {pid}")
    if missing:
        return CheckResult(
            name="hypothesis_paper_ids_resolve",
            status="fail",
            message=f"{len(missing)} hypothesis paper reference(s) point at non-existent catalog entries.",
            fix="Add the missing papers to literature_catalog or remove the typo'd paper_id from HYPOTHESES.",
            details=tuple(missing),
        )
    total = sum(len(h.paper_ids) for h in HYPOTHESES)
    return CheckResult(
        name="hypothesis_paper_ids_resolve",
        status="pass",
        message=f"All {total} paper references across {len(HYPOTHESES)} hypotheses resolve.",
    )


def check_verdicts_csv_health(
    *,
    csv_path: Path = DEFAULT_VERDICTS_CSV,
) -> CheckResult:
    """The verdicts CSV should exist, parse, and carry rows for every H1..H7."""
    if not csv_path.exists():
        return CheckResult(
            name="verdicts_csv_health",
            status="warn",
            message=f"verdicts CSV not found: {csv_path}",
            fix="Run `index-inclusion-cma` to generate the verdicts CSV.",
        )
    try:
        df = pd.read_csv(csv_path)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="verdicts_csv_health",
            status="fail",
            message=f"verdicts CSV is unreadable: {exc}",
            fix="Inspect / regenerate cma_hypothesis_verdicts.csv via `index-inclusion-cma`.",
        )
    if "hid" not in df.columns:
        return CheckResult(
            name="verdicts_csv_health",
            status="fail",
            message="verdicts CSV is missing the 'hid' column.",
            fix="Regenerate via `index-inclusion-cma`; do not hand-edit the CSV.",
        )
    actual_hids = set(df["hid"].astype(str).tolist())
    expected = set(EXPECTED_HIDS)
    missing = expected - actual_hids
    extra = actual_hids - expected
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing: {sorted(missing)}")
        if extra:
            details.append(f"unexpected: {sorted(extra)}")
        return CheckResult(
            name="verdicts_csv_health",
            status="fail",
            message="verdicts CSV doesn't match the canonical H1..H7 set.",
            fix="Regenerate via `index-inclusion-cma` after syncing HYPOTHESES registry.",
            details=tuple(details),
        )
    return CheckResult(
        name="verdicts_csv_health",
        status="pass",
        message=f"verdicts CSV at {csv_path.relative_to(ROOT) if csv_path.is_relative_to(ROOT) else csv_path} carries all {len(EXPECTED_HIDS)} expected hids.",
    )


def check_results_directory_populated(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    expected_files: Sequence[str] = EXPECTED_CMA_OUTPUTS,
) -> CheckResult:
    """At least the canonical CMA output files should be present."""
    if not results_dir.exists():
        return CheckResult(
            name="results_directory_populated",
            status="warn",
            message=f"results directory missing: {results_dir}",
            fix="Run `index-inclusion-cma` (or `make rebuild`) to populate it.",
        )
    missing = [name for name in expected_files if not (results_dir / name).exists()]
    if missing:
        return CheckResult(
            name="results_directory_populated",
            status="warn",
            message=f"{len(missing)} of {len(expected_files)} canonical CMA outputs missing.",
            fix="Run `index-inclusion-cma` to refresh CMA artifacts.",
            details=tuple(missing),
        )
    return CheckResult(
        name="results_directory_populated",
        status="pass",
        message=f"All {len(expected_files)} canonical CMA outputs are present.",
    )


def check_paper_verdict_section_synced(
    *,
    csv_path: Path = DEFAULT_VERDICTS_CSV,
    doc_path: Path = DEFAULT_PAPER_VERDICTS_DOC,
    event_counts_path: Path = DEFAULT_EVENT_COUNTS_CSV,
    event_study_summary_path: Path = DEFAULT_EVENT_STUDY_SUMMARY_CSV,
) -> CheckResult:
    """Generated paper verdict markdown should match the current verdict CSV."""
    if not csv_path.exists():
        return CheckResult(
            name="paper_verdict_section_synced",
            status="warn",
            message=f"verdicts CSV not found: {csv_path}",
            fix="Run `index-inclusion-cma` to regenerate verdict artifacts.",
        )
    if not doc_path.exists():
        return CheckResult(
            name="paper_verdict_section_synced",
            status="warn",
            message=f"paper verdict section not found: {doc_path}",
            fix="Run `index-inclusion-cma` to regenerate docs/paper_outline_verdicts.md.",
        )
    try:
        verdicts = pd.read_csv(csv_path, keep_default_na=False)
        event_counts = (
            pd.read_csv(event_counts_path)
            if event_counts_path.exists()
            else None
        )
        event_study_summary = (
            pd.read_csv(event_study_summary_path)
            if event_study_summary_path.exists()
            else None
        )
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="paper_verdict_section_synced",
            status="warn",
            message=f"Unable to read verdict inputs: {exc}",
            fix="Inspect / regenerate CMA verdict outputs via `index-inclusion-cma`.",
        )
    try:
        from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
            render_paper_verdict_section,
        )

        expected = render_paper_verdict_section(
            verdicts,
            event_counts=event_counts,
            event_study_summary=event_study_summary,
        )
        actual = doc_path.read_text()
    except (OSError, ValueError, KeyError) as exc:
        return CheckResult(
            name="paper_verdict_section_synced",
            status="warn",
            message=f"Unable to render or read paper verdict section: {exc}",
            fix="Run `index-inclusion-cma` and inspect docs/paper_outline_verdicts.md.",
        )
    if actual == expected:
        return CheckResult(
            name="paper_verdict_section_synced",
            status="pass",
            message=f"{_relative_label(doc_path)} matches the current CMA verdict CSV.",
        )

    def _summary_line(text: str) -> str:
        return next(
            (
                line.strip()
                for line in text.splitlines()
                if "当前裁决分布" in line
            ),
            "(summary line not found)",
        )

    return CheckResult(
        name="paper_verdict_section_synced",
        status="warn",
        message=f"{_relative_label(doc_path)} is out of sync with the current CMA verdict CSV.",
        fix="Run `index-inclusion-cma` to regenerate docs/paper_outline_verdicts.md.",
        details=(
            f"expected: {_summary_line(expected)}",
            f"actual: {_summary_line(actual)}",
        ),
    )


def check_chart_builders_register(
    *,
    expected_min: int = 12,
) -> CheckResult:
    """The chart_data registry should expose at least the documented chart count."""
    try:
        chart_data = importlib.import_module("index_inclusion_research.chart_data")
    except ImportError as exc:
        return CheckResult(
            name="chart_builders_register",
            status="fail",
            message=f"chart_data module fails to import: {exc}",
            fix="Investigate the import error; this likely breaks the dashboard too.",
        )
    builders = getattr(chart_data, "CHART_BUILDERS", None)
    if not isinstance(builders, dict):
        return CheckResult(
            name="chart_builders_register",
            status="fail",
            message="chart_data.CHART_BUILDERS is not a dict.",
            fix="Restore the registry in src/index_inclusion_research/chart_data.py.",
        )
    if len(builders) < expected_min:
        return CheckResult(
            name="chart_builders_register",
            status="warn",
            message=(
                f"chart_data registry has {len(builders)} entries, expected ≥ {expected_min}."
            ),
            fix="Re-run after a regression test; some builders may have been removed.",
            details=tuple(sorted(builders.keys())),
        )
    return CheckResult(
        name="chart_builders_register",
        status="pass",
        message=f"chart_data registry exposes {len(builders)} builders (≥ {expected_min}).",
    )


def check_p_gated_verdict_sensitivity(
    *,
    csv_path: Path = DEFAULT_VERDICTS_CSV,
    strict_threshold: float = 0.05,
    default_threshold: float = 0.10,
) -> CheckResult:
    """Flag p-gated hypotheses sitting in the boundary [strict, default).

    H1 / H4 / H5 (gated by a single p) carry a structured ``p_value`` in
    the verdict CSV. If any of them clears the default threshold (0.10)
    but flips not_sig at the strict threshold (0.05), that's a fragile
    "support" the referee will probe — doctor surfaces it as a warn so
    the researcher knows to add robustness or report both thresholds.

    H2 / H3 / H6 / H7 (spread / share / ratio headlines) carry NaN
    ``p_value`` and are out of scope.
    """
    if not csv_path.exists():
        return CheckResult(
            name="p_gated_verdict_sensitivity",
            status="warn",
            message=f"verdicts CSV not found: {csv_path}",
            fix="Run `index-inclusion-cma` to generate the verdicts CSV.",
        )
    try:
        df = pd.read_csv(csv_path)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="p_gated_verdict_sensitivity",
            status="warn",
            message=f"verdicts CSV unreadable: {exc}",
            fix="Inspect / regenerate cma_hypothesis_verdicts.csv via `index-inclusion-cma`.",
        )
    if "p_value" not in df.columns:
        return CheckResult(
            name="p_gated_verdict_sensitivity",
            status="warn",
            message="verdicts CSV is missing the 'p_value' column (likely pre-8a2272e).",
            fix="Run `index-inclusion-cma` to repopulate the verdicts CSV with structured p_value.",
        )
    p_gated = df.loc[df["p_value"].notna()].copy()
    if p_gated.empty:
        return CheckResult(
            name="p_gated_verdict_sensitivity",
            status="warn",
            message="No hypothesis carries a structured p_value (all NaN).",
            fix=(
                "Confirm bootstrap / regression / limit_regression inputs to "
                "build_hypothesis_verdicts are reaching H1 / H4 / H5."
            ),
        )
    p_gated["p_value"] = p_gated["p_value"].astype(float)
    boundary_mask = (p_gated["p_value"] >= strict_threshold) & (
        p_gated["p_value"] < default_threshold
    )
    boundary = p_gated.loc[boundary_mask]
    n_total = len(p_gated)
    n_strict_sig = int((p_gated["p_value"] < strict_threshold).sum())
    n_default_sig = int((p_gated["p_value"] < default_threshold).sum())
    if not boundary.empty:
        details = tuple(
            f"{row['hid']}: p={float(row['p_value']):.4f}"
            f" → sig at p<{default_threshold} but flips not_sig at p<{strict_threshold}"
            for _, row in boundary.iterrows()
        )
        return CheckResult(
            name="p_gated_verdict_sensitivity",
            status="warn",
            message=(
                f"{len(boundary)} of {n_total} p-gated hypotheses sit in the boundary"
                f" [{strict_threshold}, {default_threshold}) — referee will probe robustness."
            ),
            fix=(
                "Add covariates / expand sample, or report both thresholds. "
                "`index-inclusion-verdict-summary --sensitivity 0.05 0.10` shows "
                "both side by side."
            ),
            details=details,
        )
    return CheckResult(
        name="p_gated_verdict_sensitivity",
        status="pass",
        message=(
            f"{n_total} p-gated hypotheses; {n_strict_sig} significant at strict"
            f" ({strict_threshold}), {n_default_sig} at default ({default_threshold});"
            f" none sit in the [{strict_threshold}, {default_threshold}) boundary."
        ),
    )


def check_pending_data_verdicts(
    *,
    csv_path: Path = DEFAULT_VERDICTS_CSV,
) -> CheckResult:
    """Surface hypotheses that are structurally generated but still data-gapped."""
    if not csv_path.exists():
        return CheckResult(
            name="pending_data_verdicts",
            status="warn",
            message=f"verdicts CSV not found: {csv_path}",
            fix="Run `index-inclusion-cma` to generate the verdicts CSV.",
        )
    try:
        df = pd.read_csv(csv_path)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="pending_data_verdicts",
            status="warn",
            message=f"verdicts CSV unreadable: {exc}",
            fix="Inspect / regenerate cma_hypothesis_verdicts.csv via `index-inclusion-cma`.",
        )
    required = {"hid", "verdict"}
    missing_columns = required - set(df.columns)
    if missing_columns:
        return CheckResult(
            name="pending_data_verdicts",
            status="warn",
            message=f"verdicts CSV is missing column(s): {sorted(missing_columns)}.",
            fix="Run `index-inclusion-cma` to regenerate the current verdict schema.",
        )

    pending = df.loc[df["verdict"].astype(str).str.strip() == "待补数据"].copy()
    if pending.empty:
        return CheckResult(
            name="pending_data_verdicts",
            status="pass",
            message="No hypotheses are currently marked as 待补数据.",
        )

    details: list[str] = []
    for _, row in pending.iterrows():
        hid = str(row.get("hid", "")).strip() or "unknown"
        name = str(row.get("name_cn", row.get("name", ""))).strip()
        headline = str(row.get("key_label", row.get("headline_metric", ""))).strip()
        pieces = [hid]
        if name:
            pieces.append(name)
        if headline and headline != "nan":
            pieces.append(headline)
        details.append(" · ".join(pieces))
    return CheckResult(
        name="pending_data_verdicts",
        status="warn",
        message=f"{len(pending)} hypothesis verdict(s) are still marked 待补数据.",
        fix="Collect the missing mechanism data or keep the dashboard/paper language explicitly framed as a data gap.",
        details=tuple(details),
    )


def check_h6_weight_change_readiness(
    *,
    weight_change_path: Path = DEFAULT_WEIGHT_CHANGE_CSV,
    verdicts_csv_path: Path = DEFAULT_VERDICTS_CSV,
) -> CheckResult:
    """H6 should be explicit when it still relies on size proxy instead of weight_change."""
    label = _relative_label(weight_change_path)
    if weight_change_path.exists():
        try:
            weight_change = pd.read_csv(weight_change_path)
        except (OSError, ValueError) as exc:
            return CheckResult(
                name="h6_weight_change_readiness",
                status="warn",
                message=f"H6 weight_change table is unreadable: {exc}",
                fix="Regenerate it with `index-inclusion-compute-h6-weight-change --force`.",
            )
        required = {"market", "ticker", "weight_proxy"}
        missing = required - set(weight_change.columns)
        if missing:
            return CheckResult(
                name="h6_weight_change_readiness",
                status="warn",
                message=f"{label} is missing column(s): {sorted(missing)}.",
                fix="Regenerate it with `index-inclusion-compute-h6-weight-change --force`.",
            )
        cn_rows = weight_change.loc[
            (weight_change["market"].astype(str) == "CN")
            & weight_change["weight_proxy"].notna()
        ]
        if cn_rows.empty:
            return CheckResult(
                name="h6_weight_change_readiness",
                status="warn",
                message=f"{label} exists but has no CN rows with weight_proxy.",
                fix="Regenerate with CN market-cap coverage, then rerun `index-inclusion-cma`.",
            )
        return CheckResult(
            name="h6_weight_change_readiness",
            status="pass",
            message=f"H6 has {len(cn_rows)} CN weight_change row(s) available.",
        )

    details: list[str] = [f"missing: {label}"]
    if verdicts_csv_path.exists():
        try:
            verdicts = pd.read_csv(verdicts_csv_path)
            h6 = verdicts.loc[verdicts["hid"].astype(str) == "H6"]
            if not h6.empty:
                row = h6.iloc[0]
                details.append(
                    f"current H6 headline: {row.get('key_label', 'unknown')} = {row.get('key_value', 'NA')}"
                )
        except (OSError, ValueError, KeyError):
            pass
    return CheckResult(
        name="h6_weight_change_readiness",
        status="warn",
        message="H6 is still using size heterogeneity as a proxy because weight_change is missing.",
        fix="Run `index-inclusion-compute-h6-weight-change --force`, then `index-inclusion-cma` to replace the size proxy.",
        details=tuple(details),
    )


def check_h7_cn_sector_readiness(
    *,
    sector_csv_path: Path = DEFAULT_HETEROGENEITY_SECTOR_CSV,
) -> CheckResult:
    """H7 should say when sector evidence is US-only because CN sector is missing."""
    label = _relative_label(sector_csv_path)
    if not sector_csv_path.exists():
        return CheckResult(
            name="h7_cn_sector_readiness",
            status="warn",
            message=f"sector heterogeneity table not found: {label}",
            fix="Run `index-inclusion-cma` after filling sector data.",
        )
    try:
        sector = pd.read_csv(sector_csv_path)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="h7_cn_sector_readiness",
            status="warn",
            message=f"sector heterogeneity table is unreadable: {exc}",
            fix="Regenerate CMA outputs via `index-inclusion-cma`.",
        )
    required = {"market", "bucket", "n_events"}
    missing = required - set(sector.columns)
    if missing:
        return CheckResult(
            name="h7_cn_sector_readiness",
            status="warn",
            message=f"{label} is missing column(s): {sorted(missing)}.",
            fix="Regenerate CMA outputs via `index-inclusion-cma`.",
        )
    cn = sector.loc[sector["market"].astype(str) == "CN"].copy()
    if cn.empty:
        return CheckResult(
            name="h7_cn_sector_readiness",
            status="warn",
            message="H7 sector table has no CN rows.",
            fix="Populate CN sector fields, then rerun `index-inclusion-cma`.",
        )
    known = cn.loc[
        ~cn["bucket"].astype(str).str.strip().str.lower().isin(
            {"", "unknown", "nan", "none"}
        )
    ]
    if known.empty:
        total_events = int(cn["n_events"].fillna(0).sum())
        return CheckResult(
            name="h7_cn_sector_readiness",
            status="warn",
            message="H7 CN sector is not populated; current sector evidence is US-only.",
            fix="Fill CN sector in the source event/metadata tables, then rerun `index-inclusion-cma`.",
            details=(f"CN Unknown events: {total_events}",),
        )
    return CheckResult(
        name="h7_cn_sector_readiness",
        status="pass",
        message=f"H7 has {len(known)} CN sector bucket(s) available.",
        details=tuple(
            f"{row['bucket']}: n={int(row['n_events'])}"
            for _, row in known.head(5).iterrows()
        ),
    )


def check_rdd_l3_sample_readiness(
    *,
    root: Path = ROOT,
    status_dir: Path = DEFAULT_RDD_STATUS_DIR,
) -> CheckResult:
    """Keep CLI doctor aligned with dashboard result-health around HS300 RDD evidence."""
    formal_path = root / "data" / "raw" / "hs300_rdd_candidates.csv"
    reconstructed_path = root / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
    try:
        from index_inclusion_research.result_contract import load_rdd_status

        live_status = load_rdd_status(root, output_dir=status_dir)
    except Exception as exc:  # noqa: BLE001 - doctor should report diagnostics, not crash
        return CheckResult(
            name="rdd_l3_sample_readiness",
            status="warn",
            message=f"Unable to read HS300 RDD status: {exc}",
            fix="Run `index-inclusion-hs300-rdd` and inspect results/literature/hs300_rdd/rdd_status.csv.",
        )

    mode = str(live_status.get("mode", "") or "")
    source_kind = str(live_status.get("source_kind", "") or "")
    rows = live_status.get("candidate_rows")
    batches = live_status.get("candidate_batches")
    if formal_path.exists() and (mode == "real" or source_kind == "real"):
        suffix = (
            f" ({rows} candidate rows across {batches} batches)"
            if rows and batches
            else ""
        )
        return CheckResult(
            name="rdd_l3_sample_readiness",
            status="pass",
            message=f"Formal HS300 RDD L3 sample is active{suffix}.",
        )

    formal_label = _relative_label(formal_path)
    reconstructed_label = _relative_label(reconstructed_path)
    if formal_path.exists():
        return CheckResult(
            name="rdd_l3_sample_readiness",
            status="warn",
            message=(
                f"{formal_label} exists, but live RDD status is still "
                f"{mode or source_kind or 'unknown'}."
            ),
            fix="Rerun `index-inclusion-hs300-rdd && index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma`.",
        )

    if reconstructed_path.exists():
        details = [f"active fallback: {reconstructed_label}"]
        if rows and batches:
            details.append(f"current status: {rows} candidate rows across {batches} batches")
        return CheckResult(
            name="rdd_l3_sample_readiness",
            status="warn",
            message=(
                f"Formal HS300 RDD L3 sample is missing ({formal_label}); "
                "dashboard evidence remains on the public reconstructed L2 sample."
            ),
            fix="Import a formal boundary candidate file with `index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --check-only` before promoting this to L3 evidence.",
            details=tuple(details),
        )

    return CheckResult(
        name="rdd_l3_sample_readiness",
        status="warn",
        message=f"Neither {formal_label} nor {reconstructed_label} is available.",
        fix="Run `index-inclusion-reconstruct-hs300-rdd --all-batches --force` or import a formal candidate file.",
    )


def check_console_scripts_importable() -> CheckResult:
    """Every console_script declared in pyproject.toml resolves to a callable."""
    pyproject = ROOT / "pyproject.toml"
    if not pyproject.exists():
        return CheckResult(
            name="console_scripts_importable",
            status="fail",
            message="pyproject.toml is missing.",
            fix="Restore pyproject.toml from git.",
        )
    try:
        import tomllib  # py311+
    except ImportError:
        return CheckResult(
            name="console_scripts_importable",
            status="warn",
            message="tomllib unavailable (Python < 3.11); skipped.",
        )
    data = tomllib.loads(pyproject.read_text())
    scripts = data.get("project", {}).get("scripts", {})
    if not scripts:
        return CheckResult(
            name="console_scripts_importable",
            status="fail",
            message="pyproject.toml has no [project.scripts] table.",
            fix="Restore the console-script declarations.",
        )
    failures: list[str] = []
    for name, target in scripts.items():
        module_name, _, attr = str(target).partition(":")
        if not module_name or not attr:
            failures.append(f"{name} → bad target {target!r}")
            continue
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            failures.append(f"{name} → import {module_name}: {exc}")
            continue
        if not callable(getattr(module, attr, None)):
            failures.append(f"{name} → {module_name}:{attr} is not callable")
    if failures:
        return CheckResult(
            name="console_scripts_importable",
            status="fail",
            message=f"{len(failures)} console_script entry points unresolvable.",
            fix="Reinstall in editable mode (`pip install -e .`) and confirm cli.py wiring.",
            details=tuple(failures),
        )
    return CheckResult(
        name="console_scripts_importable",
        status="pass",
        message=f"All {len(scripts)} console_script entry points resolve to callables.",
    )


# ── orchestrator ─────────────────────────────────────────────────────


def check_matched_sample_balance(
    *,
    csv_path: Path = DEFAULT_MATCH_BALANCE_CSV,
    smd_threshold: float = 0.25,
) -> CheckResult:
    """Matched-sample covariate balance (Stuart 2010): warn when |SMD| > 0.25."""
    if not csv_path.exists():
        return CheckResult(
            name="matched_sample_balance",
            status="warn",
            message=f"covariate balance CSV not found: {_relative_label(csv_path)}",
            fix="Re-run `index-inclusion-match-controls` to emit match_balance.csv.",
        )
    try:
        df = pd.read_csv(csv_path)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="matched_sample_balance",
            status="fail",
            message=f"covariate balance CSV is unreadable: {exc}",
            fix="Re-run `index-inclusion-match-controls` to regenerate match_balance.csv.",
        )
    if df.empty:
        return CheckResult(
            name="matched_sample_balance",
            status="warn",
            message="covariate balance CSV is empty.",
            fix="Confirm matched_events has both treatment_group=1 and =0 rows, then re-run match-controls.",
        )
    if "smd" not in df.columns:
        return CheckResult(
            name="matched_sample_balance",
            status="fail",
            message="covariate balance CSV is missing the 'smd' column.",
            fix="Regenerate via `index-inclusion-match-controls`; do not hand-edit the CSV.",
        )
    abs_smd = df["smd"].abs()
    over = df.loc[abs_smd >= smd_threshold]
    max_abs = float(abs_smd.max()) if not abs_smd.empty else float("nan")
    if not over.empty:
        rows = ", ".join(
            f"{r['market']}/{r['covariate']}={r['smd']:+.2f}"
            for _, r in over.head(5).iterrows()
        )
        return CheckResult(
            name="matched_sample_balance",
            status="warn",
            message=(
                f"{len(over)} covariate(s) exceed |SMD|>={smd_threshold:.2f}: {rows}"
            ),
            fix="Tighten the matching distance or relax sector/cap criteria, then re-run match-controls.",
        )
    return CheckResult(
        name="matched_sample_balance",
        status="pass",
        message=f"all covariates pass |SMD|<{smd_threshold:.2f} (max={max_abs:.3f}).",
    )


DEFAULT_CHECKS: tuple[Callable[[], CheckResult], ...] = (
    check_hypothesis_paper_ids_resolve,
    check_verdicts_csv_health,
    check_results_directory_populated,
    check_paper_verdict_section_synced,
    check_p_gated_verdict_sensitivity,
    check_pending_data_verdicts,
    check_h6_weight_change_readiness,
    check_h7_cn_sector_readiness,
    check_rdd_l3_sample_readiness,
    check_matched_sample_balance,
    check_chart_builders_register,
    check_console_scripts_importable,
)


def run_all_checks(
    checks: Sequence[Callable[[], CheckResult]] = DEFAULT_CHECKS,
) -> list[CheckResult]:
    """Run *checks* in order and return results.  Exceptions are caught
    and converted into a ``fail`` CheckResult so doctor never crashes."""
    results: list[CheckResult] = []
    for check in checks:
        try:
            results.append(check())
        except Exception as exc:  # noqa: BLE001 — diagnostic harness, swallow + log
            logger.exception("check %s raised", check.__name__)
            results.append(
                CheckResult(
                    name=check.__name__,
                    status="fail",
                    message=f"check raised {type(exc).__name__}: {exc}",
                )
            )
    return results


def render_results(results: Sequence[CheckResult], *, color: bool = True) -> str:
    lines: list[str] = []
    lines.append("=" * 64)
    lines.append(" INDEX-INCLUSION-RESEARCH · doctor")
    lines.append("=" * 64)
    pass_count = sum(1 for r in results if r.status == "pass")
    warn_count = sum(1 for r in results if r.status == "warn")
    fail_count = sum(1 for r in results if r.status == "fail")
    lines.append(
        f"  {pass_count} pass · {warn_count} warn · {fail_count} fail"
        f" · {len(results)} checks total"
    )
    lines.append("")
    for r in results:
        glyph = _STATUS_GLYPH[r.status]
        head = f"  {glyph}  {r.name}"
        if color:
            head = f"  {_STATUS_COLOR[r.status]}{glyph}{_RESET}  {r.name}"
        lines.append(head)
        lines.append(f"      {r.message}")
        for detail in r.details:
            lines.append(f"        - {detail}")
        if r.status != "pass" and r.fix:
            lines.append(f"      → {r.fix}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def results_summary(results: Sequence[CheckResult]) -> dict[str, int]:
    return {
        "pass": sum(1 for r in results if r.status == "pass"),
        "warn": sum(1 for r in results if r.status == "warn"),
        "fail": sum(1 for r in results if r.status == "fail"),
        "total": len(results),
    }


def results_payload(results: Sequence[CheckResult]) -> dict[str, object]:
    return {
        "summary": results_summary(results),
        "checks": [
            {
                "name": result.name,
                "status": result.status,
                "message": result.message,
                "fix": result.fix,
                "details": list(result.details),
            }
            for result in results
        ],
    }


def render_results_json(results: Sequence[CheckResult]) -> str:
    return json.dumps(results_payload(results), ensure_ascii=False, indent=2) + "\n"


def doctor_exit_code(results: Sequence[CheckResult], *, fail_on_warn: bool = False) -> int:
    summary = results_summary(results)
    if fail_on_warn:
        return summary["fail"] + summary["warn"]
    return summary["fail"]


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a sequence of project health-check probes. Useful in CI "
            "and after fresh installs."
        )
    )
    parser.add_argument(
        "--no-color", action="store_true",
        help="Disable ANSI colour escape codes.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Choose human-readable text or machine-readable JSON output.",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Return a non-zero exit code when checks produce warnings.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    import sys

    parser = build_arg_parser()
    args = parser.parse_args(argv)

    results = run_all_checks()
    if args.format == "json":
        print(render_results_json(results), end="")
    else:
        enable_color = not args.no_color and sys.stdout.isatty()
        print(render_results(results, color=enable_color), end="")
    return doctor_exit_code(results, fail_on_warn=args.fail_on_warn)


if __name__ == "__main__":
    raise SystemExit(main())
