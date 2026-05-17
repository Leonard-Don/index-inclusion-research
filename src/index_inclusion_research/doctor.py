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
import datetime as _dt
import importlib
import json
import logging
import os
import re
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
DEFAULT_MATCH_ROBUSTNESS_GRID_CSV = ROOT / "results" / "real_regressions" / "match_robustness_grid.csv"
DEFAULT_PAP_DEVIATION_REPORT_CSV = DEFAULT_RESULTS_DIR / "pap_deviation_report.csv"
DEFAULT_SNAPSHOTS_DIR = ROOT / "snapshots"
DEFAULT_HS300_RDD_FOREST_PNG = ROOT / "results" / "figures" / "hs300_rdd_robustness_forest.png"
DEFAULT_HS300_RDD_FOREST_PDF = ROOT / "results" / "figures" / "hs300_rdd_robustness_forest.pdf"
DEFAULT_HS300_RDD_ROBUSTNESS_CSV = DEFAULT_RDD_STATUS_DIR / "rdd_robustness.csv"
DEFAULT_CMA_VERDICTS_FOREST_PNG = ROOT / "results" / "figures" / "cma_verdicts_forest.png"
DEFAULT_CMA_VERDICTS_FOREST_PDF = ROOT / "results" / "figures" / "cma_verdicts_forest.pdf"
DEFAULT_CMA_SENSITIVITY_FOREST_PNG = (
    ROOT / "results" / "figures" / "cma_verdicts_sensitivity.png"
)
DEFAULT_CMA_SENSITIVITY_FOREST_PDF = (
    ROOT / "results" / "figures" / "cma_verdicts_sensitivity.pdf"
)
DEFAULT_CMA_SENSITIVITY_ROOT = ROOT / "results" / "sensitivity"
DEFAULT_CMA_AR_ENGINE_FOREST_PNG = (
    ROOT / "results" / "figures" / "cma_verdicts_ar_engine.png"
)
DEFAULT_CMA_AR_ENGINE_FOREST_PDF = (
    ROOT / "results" / "figures" / "cma_verdicts_ar_engine.pdf"
)
DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PNG = (
    ROOT / "results" / "figures" / "cma_verdicts_2d_robustness.png"
)
DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PDF = (
    ROOT / "results" / "figures" / "cma_verdicts_2d_robustness.pdf"
)
DEFAULT_PUBLIC_SUMMARY_JSON = (
    ROOT / "data" / "public" / "index_research_summary.json"
)
DEFAULT_RDD_ROBUSTNESS_CSV_FOR_SUMMARY = (
    ROOT / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
)
DEFAULT_CITATION_CENTRALITY_CSV = (
    ROOT / "results" / "literature" / "citation_centrality.csv"
)
DEFAULT_CITATION_NETWORK_PNG = (
    ROOT / "results" / "literature" / "citation_network.png"
)
DEFAULT_CITATION_NETWORK_PDF = (
    ROOT / "results" / "literature" / "citation_network.pdf"
)
DEFAULT_PAPER_SKELETON_MD = ROOT / "paper" / "skeleton.md"
PAP_SNAPSHOT_GLOB = "pre-registration-*.csv"
PAP_SNAPSHOT_STALE_DAYS = 90

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


def check_rdd_robustness_panel(
    *,
    root: Path = ROOT,
) -> CheckResult:
    """Verify the RDD robustness panel (main / donut / placebo / polynomial)
    is on disk alongside rdd_summary.csv. Locks the new robustness suite
    into the project health gate so a broken hs300_rdd run can't leave
    rdd_summary fresh while rdd_robustness silently goes stale."""
    name = "rdd_robustness_panel"
    summary_path = root / "results" / "literature" / "hs300_rdd" / "rdd_summary.csv"
    robustness_path = root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
    if not summary_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=f"{_relative_label(summary_path)} is missing; skipping robustness check.",
            fix="Run `index-inclusion-hs300-rdd` to regenerate the RDD outputs.",
        )
    if not robustness_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{_relative_label(summary_path)} exists, but "
                f"{_relative_label(robustness_path)} is missing — "
                "the robustness panel never ran or failed silently."
            ),
            fix="Run `index-inclusion-hs300-rdd` to regenerate rdd_robustness.csv alongside rdd_summary.csv.",
        )
    try:
        df = pd.read_csv(robustness_path)
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name=name,
            status="warn",
            message=f"Unable to read {_relative_label(robustness_path)}: {exc}",
            fix="Inspect rdd_robustness.csv and rerun `index-inclusion-hs300-rdd` if corrupted.",
        )
    expected_kinds = {"main", "donut", "placebo", "polynomial"}
    actual_kinds = set(df.get("spec_kind", pd.Series(dtype=str)).astype(str).unique())
    missing = expected_kinds - actual_kinds
    if missing:
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{_relative_label(robustness_path)} is missing spec kind(s): "
                f"{sorted(missing)}; expected all four (main / donut / placebo / polynomial)."
            ),
            fix="Rerun `index-inclusion-hs300-rdd` to regenerate the full robustness panel.",
        )
    n_rows = int(df.shape[0])
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"RDD robustness panel covers all 4 spec kinds across {n_rows} row(s)."
        ),
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


def check_heuristic_citation_centrality_schema(
    *,
    csv_path: Path = DEFAULT_CITATION_CENTRALITY_CSV,
) -> CheckResult:
    """Guard generated literature-network CSV semantics.

    ``citation_centrality.csv`` is a legacy filename, but its link-list
    columns must stay heuristic: ``top_linked_by`` / ``top_links_to``.
    Old ``top_cited_by`` / ``top_cites`` headers make the generated
    output look like verified bibliography evidence, so doctor fails
    them explicitly.
    """
    name = "heuristic_citation_centrality_schema"
    if not csv_path.exists():
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"heuristic literature centrality CSV not present at {_relative_label(csv_path)}; "
                "schema guard will validate top_linked_by/top_links_to when the generated file exists."
            ),
        )
    try:
        df = pd.read_csv(csv_path, nrows=0)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name=name,
            status="fail",
            message=f"heuristic literature centrality CSV is unreadable: {exc}",
            fix="Regenerate with `index-inclusion-citation-graph`.",
        )

    columns = set(df.columns)
    legacy_columns = sorted(columns & {"top_cited_by", "top_cites"})
    if legacy_columns:
        return CheckResult(
            name=name,
            status="fail",
            message=(
                f"{_relative_label(csv_path)} uses legacy citation-language column(s): "
                f"{legacy_columns}."
            ),
            fix=(
                "Regenerate with `index-inclusion-citation-graph`; heuristic links "
                "must use top_linked_by/top_links_to and must not be represented as "
                "verified citations."
            ),
            details=tuple(legacy_columns),
        )

    required_columns = {
        "paper_id",
        "in_degree",
        "out_degree",
        "betweenness",
        "eigenvector",
        "top_linked_by",
        "top_links_to",
    }
    missing = sorted(required_columns - columns)
    if missing:
        return CheckResult(
            name=name,
            status="fail",
            message=f"{_relative_label(csv_path)} is missing heuristic column(s): {missing}.",
            fix="Regenerate with `index-inclusion-citation-graph`.",
            details=tuple(missing),
        )

    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"{_relative_label(csv_path)} uses heuristic link columns "
            "top_linked_by/top_links_to."
        ),
    )


def check_citation_graph_artifact(
    *,
    png_path: Path = DEFAULT_CITATION_NETWORK_PNG,
    pdf_path: Path = DEFAULT_CITATION_NETWORK_PDF,
    centrality_csv_path: Path = DEFAULT_CITATION_CENTRALITY_CSV,
) -> CheckResult:
    """Warn if the heuristic literature-link network figure is missing or stale.

    Mirrors :func:`check_hs300_rdd_forest_artifact` and the CMA forest checks:
    the PNG / PDF twins must both exist and have an mtime ≥ the centrality
    CSV they accompany. Re-run ``index-inclusion-citation-graph`` to refresh
    all three artifacts together (idempotent — overwrites in place).
    """
    return _forest_artifact_status(
        name="citation_graph_artifact",
        png_path=png_path,
        pdf_path=pdf_path,
        input_csv_path=centrality_csv_path,
        fix_command=(
            "Run `index-inclusion-citation-graph` to refresh the citation "
            "network figure (PNG + PDF) and centrality CSV together."
        ),
        input_label="citation_centrality.csv",
    )


def check_paper_audit() -> CheckResult:
    """Paper-facing claims should be backed by current artifacts and bundle copies."""
    from index_inclusion_research.paper_audit import run_paper_audit, summarize_audit

    results = run_paper_audit(ROOT, require_bundle=False)
    summary = summarize_audit(results)
    warn_or_fail = [item for item in results if item.status in {"warn", "fail"}]
    if warn_or_fail:
        details = tuple(f"{item.name}: {item.message}" for item in warn_or_fail)
        status: Status = "fail" if any(item.status == "fail" for item in warn_or_fail) else "warn"
        return CheckResult(
            name="paper_audit_claims",
            status=status,
            message=(
                f"Paper audit has {summary['pass']} pass, {summary['warn']} warn, "
                f"{summary['fail']} fail."
            ),
            fix="Run `make paper` and inspect `index-inclusion-paper-audit` output.",
            details=details,
        )
    return CheckResult(
        name="paper_audit_claims",
        status="pass",
        message=f"Paper audit maps all {summary['total']} claim group(s) to current artifacts.",
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


def check_match_robustness_grid(
    *,
    csv_path: Path = DEFAULT_MATCH_ROBUSTNESS_GRID_CSV,
    expected_min_specs: int = 3,
) -> CheckResult:
    """Confirm the local matched-sample robustness grid is available."""
    if not csv_path.exists():
        return CheckResult(
            name="match_robustness_grid",
            status="warn",
            message=f"match robustness grid not found: {_relative_label(csv_path)}",
            fix="Run `index-inclusion-match-robustness` to refresh the local-only grid.",
        )
    try:
        grid = pd.read_csv(csv_path)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name="match_robustness_grid",
            status="fail",
            message=f"match robustness grid is unreadable: {exc}",
            fix="Regenerate it with `index-inclusion-match-robustness`.",
        )
    if grid.empty:
        return CheckResult(
            name="match_robustness_grid",
            status="warn",
            message="match robustness grid is empty.",
            fix="Confirm the matched sample and local prices exist, then re-run `index-inclusion-match-robustness`.",
        )
    required = {"spec_id", "over_threshold_covariates", "max_abs_smd"}
    missing = required - set(grid.columns)
    if missing:
        return CheckResult(
            name="match_robustness_grid",
            status="fail",
            message=f"match robustness grid is missing column(s): {sorted(missing)}.",
            fix="Regenerate it with the current `index-inclusion-match-robustness` CLI.",
        )

    over = pd.to_numeric(grid["over_threshold_covariates"], errors="coerce")
    max_abs = pd.to_numeric(grid["max_abs_smd"], errors="coerce")
    ranked = grid.assign(
        _over_sort=over.fillna(float("inf")),
        _max_abs_sort=max_abs.fillna(float("inf")),
    ).sort_values(["_over_sort", "_max_abs_sort", "spec_id"], ignore_index=True)
    best = ranked.iloc[0]
    best_over = int(float(best["_over_sort"])) if float(best["_over_sort"]) < float("inf") else 0
    best_max = (
        float(best["_max_abs_sort"])
        if float(best["_max_abs_sort"]) < float("inf")
        else float("nan")
    )
    details: list[str] = []
    if best_over:
        details.append(
            f"best spec still has {best_over} covariate(s) over threshold; matched_sample_balance remains the quality gate"
        )
    if len(grid) < expected_min_specs:
        return CheckResult(
            name="match_robustness_grid",
            status="warn",
            message=(
                f"match robustness grid has {len(grid)} spec(s), expected at least "
                f"{expected_min_specs}."
            ),
            fix="Re-run with multiple `--control-ratios` or `--reference-date-columns`.",
            details=tuple(details),
        )
    return CheckResult(
        name="match_robustness_grid",
        status="pass",
        message=(
            f"{len(grid)} local robustness spec(s) available; best={best['spec_id']} "
            f"(over={best_over}, max|SMD|={best_max:.3f})."
        ),
        details=tuple(details),
    )


# ── PAP discipline + figure-freshness checks ─────────────────────────


def _ensure_pap_deviation_report(
    *,
    report_path: Path,
    verdicts_csv_path: Path,
    snapshots_dir: Path,
) -> Path | None:
    """Regenerate ``pap_deviation_report.csv`` in-process if it's missing.

    Imports :mod:`index_inclusion_research.pap_diff` lazily so doctor
    stays cheap when the PAP audit has already been run. Returns the
    path on success, or ``None`` if regeneration was impossible (no
    baseline / no current verdicts).
    """
    if report_path.exists():
        return report_path

    from index_inclusion_research import pap_diff

    baseline_path = pap_diff.resolve_default_baseline(snapshots_dir)
    if baseline_path is None or not baseline_path.exists():
        return None
    if not verdicts_csv_path.exists():
        return None

    baseline = pap_diff._read_csv(baseline_path)
    current = pap_diff._read_csv(verdicts_csv_path)
    report = pap_diff.build_pap_diff(baseline, current)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(report_path, index=False)
    return report_path


def check_pap_deviation_no_flips(
    *,
    report_path: Path = DEFAULT_PAP_DEVIATION_REPORT_CSV,
    verdicts_csv_path: Path = DEFAULT_VERDICTS_CSV,
    snapshots_dir: Path = DEFAULT_SNAPSHOTS_DIR,
) -> CheckResult:
    """Surface PAP drift: warn on tightened/weakened, fail on flipped.

    Reads ``results/real_tables/pap_deviation_report.csv`` (regenerating
    it in-process via :mod:`pap_diff` if missing). Compares every
    hypothesis row's ``classification`` against the frozen PAP baseline
    so any verdict that flipped since PAP sign-off shows up as a hard
    failure — that's the case the referee will hit hardest.
    """
    name = "pap_deviation_no_flips"
    resolved = _ensure_pap_deviation_report(
        report_path=report_path,
        verdicts_csv_path=verdicts_csv_path,
        snapshots_dir=snapshots_dir,
    )
    if resolved is None or not resolved.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"PAP deviation report not found and could not regenerate: "
                f"{_relative_label(report_path)}"
            ),
            fix=(
                "Confirm snapshots/pre-registration-*.csv exists and run "
                "`index-inclusion-pap-diff`."
            ),
        )
    try:
        df = pd.read_csv(resolved, keep_default_na=False)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name=name,
            status="warn",
            message=f"PAP deviation report unreadable: {exc}",
            fix="Regenerate with `index-inclusion-pap-diff`.",
        )
    if "classification" not in df.columns:
        return CheckResult(
            name=name,
            status="warn",
            message="PAP deviation report is missing the 'classification' column.",
            fix="Regenerate with `index-inclusion-pap-diff` to refresh the schema.",
        )
    if df.empty:
        return CheckResult(
            name=name,
            status="warn",
            message=f"{_relative_label(resolved)} is empty.",
            fix="Regenerate with `index-inclusion-pap-diff`.",
        )

    classifications = df["classification"].astype(str).str.strip()
    flipped_rows = df.loc[classifications == "flipped"]
    drifted_rows = df.loc[classifications.isin({"tightened", "weakened"})]

    def _row_label(row: pd.Series) -> str:
        hid = str(row.get("hid", "")).strip() or "?"
        cls = str(row.get("classification", "")).strip()
        base = str(row.get("baseline_verdict", "")).strip()
        cur = str(row.get("current_verdict", "")).strip()
        return f"{hid} · {cls}: {base} → {cur}"

    if not flipped_rows.empty:
        details = tuple(_row_label(row) for _, row in flipped_rows.iterrows())
        return CheckResult(
            name=name,
            status="fail",
            message=(
                f"{len(flipped_rows)} of {len(df)} hypothesis verdict(s) "
                f"have flipped vs the frozen PAP baseline."
            ),
            fix="Run `make verdicts && make paper` to inspect changed rows; PAP §7 sign-off required for any flip.",
            details=details,
        )
    if not drifted_rows.empty:
        details = tuple(_row_label(row) for _, row in drifted_rows.iterrows())
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(drifted_rows)} of {len(df)} hypothesis verdict(s) "
                f"drifted (tightened/weakened) vs the frozen PAP baseline."
            ),
            fix="Run `make verdicts && make paper` to inspect changed rows.",
            details=details,
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"All {len(df)} hypothesis verdict(s) are unchanged vs the frozen "
            f"PAP baseline."
        ),
    )


def _parse_snapshot_date(path: Path) -> _dt.date | None:
    """Extract the ``YYYY-MM-DD`` date from a pre-registration filename."""
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", path.name)
    if match is None:
        return None
    try:
        return _dt.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def check_pap_snapshot_freshness(
    *,
    snapshots_dir: Path = DEFAULT_SNAPSHOTS_DIR,
    stale_days: int = PAP_SNAPSHOT_STALE_DAYS,
    today: _dt.date | None = None,
) -> CheckResult:
    """Warn when the latest PAP baseline snapshot is > ``stale_days`` old.

    The PAP should be re-baselined quarterly to keep ``pap-diff`` honest;
    a snapshot older than 90 days is a sign the team forgot to refresh
    after a verdict iteration. Missing snapshots entirely is treated as
    a hard error — there's nothing for ``pap-diff`` to compare against.
    """
    name = "pap_snapshot_freshness"
    if not snapshots_dir.exists():
        return CheckResult(
            name=name,
            status="fail",
            message=f"snapshots directory missing: {_relative_label(snapshots_dir)}",
            fix=(
                "Create snapshots/pre-registration-YYYY-MM-DD.csv from the "
                "current cma_hypothesis_verdicts.csv to seed the PAP baseline."
            ),
        )
    candidates = sorted(snapshots_dir.glob(PAP_SNAPSHOT_GLOB))
    if not candidates:
        return CheckResult(
            name=name,
            status="fail",
            message=(
                f"No PAP snapshots found under "
                f"{_relative_label(snapshots_dir)}/{PAP_SNAPSHOT_GLOB}."
            ),
            fix=(
                "Copy results/real_tables/cma_hypothesis_verdicts.csv to "
                "snapshots/pre-registration-YYYY-MM-DD.csv to seed the PAP baseline."
            ),
        )

    latest = candidates[-1]
    snapshot_date = _parse_snapshot_date(latest)
    reference_date = today if today is not None else _dt.date.today()
    if snapshot_date is None:
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"Latest snapshot {_relative_label(latest)} doesn't carry a "
                f"YYYY-MM-DD date suffix."
            ),
            fix=(
                "Rename to snapshots/pre-registration-YYYY-MM-DD.csv to make "
                "freshness auditable."
            ),
        )
    age_days = (reference_date - snapshot_date).days
    if age_days > stale_days:
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"Latest PAP snapshot {_relative_label(latest)} is "
                f"{age_days} days old (> {stale_days}); recommend quarterly "
                f"re-baselining."
            ),
            fix=(
                "Copy results/real_tables/cma_hypothesis_verdicts.csv to "
                "snapshots/pre-registration-YYYY-MM-DD.csv after the next "
                "verdict iteration."
            ),
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"Latest PAP snapshot {_relative_label(latest)} is {age_days} day(s) old "
            f"(≤ {stale_days})."
        ),
    )


def _forest_artifact_status(
    *,
    name: str,
    png_path: Path,
    pdf_path: Path,
    input_csv_path: Path,
    fix_command: str,
    input_label: str,
) -> CheckResult:
    """Shared core for the two forest-plot freshness checks."""
    missing = [p for p in (png_path, pdf_path) if not p.exists()]
    if missing:
        labels = ", ".join(_relative_label(p) for p in missing)
        return CheckResult(
            name=name,
            status="warn",
            message=f"forest plot artifact(s) missing: {labels}",
            fix=fix_command,
        )
    if not input_csv_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"forest plot input {_relative_label(input_csv_path)} "
                f"({input_label}) is missing; cannot verify freshness."
            ),
            fix=fix_command,
        )
    if (
        os.getenv("CI", "").lower() == "true"
        and png_path.is_relative_to(ROOT)
        and pdf_path.is_relative_to(ROOT)
        and input_csv_path.is_relative_to(ROOT)
    ):
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"forest plot artifacts ({_relative_label(png_path)}, "
                f"{_relative_label(pdf_path)}) are present; skipping mtime "
                "freshness in CI because checkout mtimes are not generation times."
            ),
        )
    input_mtime = input_csv_path.stat().st_mtime
    stale = [
        p for p in (png_path, pdf_path) if p.stat().st_mtime < input_mtime
    ]
    if stale:
        details = tuple(
            f"{_relative_label(p)} mtime older than "
            f"{_relative_label(input_csv_path)}"
            for p in stale
        )
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(stale)} forest plot artifact(s) older than input "
                f"{_relative_label(input_csv_path)}; re-run of "
                f"`make figures-tables` overdue."
            ),
            fix=fix_command,
            details=details,
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"forest plot artifacts ({_relative_label(png_path)}, "
            f"{_relative_label(pdf_path)}) are fresher than "
            f"{_relative_label(input_csv_path)}."
        ),
    )


def check_hs300_rdd_forest_artifact(
    *,
    png_path: Path = DEFAULT_HS300_RDD_FOREST_PNG,
    pdf_path: Path = DEFAULT_HS300_RDD_FOREST_PDF,
    robustness_csv_path: Path = DEFAULT_HS300_RDD_ROBUSTNESS_CSV,
) -> CheckResult:
    """Warn if the HS300 RDD robustness forest plot is missing or stale."""
    return _forest_artifact_status(
        name="hs300_rdd_forest_artifact",
        png_path=png_path,
        pdf_path=pdf_path,
        input_csv_path=robustness_csv_path,
        fix_command=(
            "Run `make figures-tables` (or "
            "`index-inclusion-build-hs300-rdd-forest`) to refresh the figure."
        ),
        input_label="rdd_robustness.csv",
    )


def check_cma_verdicts_forest_artifact(
    *,
    png_path: Path = DEFAULT_CMA_VERDICTS_FOREST_PNG,
    pdf_path: Path = DEFAULT_CMA_VERDICTS_FOREST_PDF,
    verdicts_csv_path: Path = DEFAULT_VERDICTS_CSV,
) -> CheckResult:
    """Warn if the CMA cross-hypothesis verdict forest plot is missing or stale."""
    return _forest_artifact_status(
        name="cma_verdicts_forest_artifact",
        png_path=png_path,
        pdf_path=pdf_path,
        input_csv_path=verdicts_csv_path,
        fix_command=(
            "Run `make figures-tables` (or "
            "`index-inclusion-build-cma-verdicts-forest`) to refresh the figure."
        ),
        input_label="cma_hypothesis_verdicts.csv",
    )


def check_cma_ar_engine_forest_artifact(
    *,
    png_path: Path = DEFAULT_CMA_AR_ENGINE_FOREST_PNG,
    pdf_path: Path = DEFAULT_CMA_AR_ENGINE_FOREST_PDF,
    sensitivity_root: Path = DEFAULT_CMA_SENSITIVITY_ROOT,
) -> CheckResult:
    """Warn if the AR-engine-sweep CMA forest plot is missing or stale.

    Sister of :func:`check_cma_sensitivity_forest_artifact`: same three
    regimes, but the inputs are the per-engine CSVs under
    ``results/sensitivity/ar_<engine>/cma_hypothesis_verdicts.csv``
    (currently ``ar_adjusted`` and ``ar_market``). If no caches exist
    yet the user simply hasn't opted into the AR engine sweep — the
    check stays a soft warn rather than a fail so a fresh checkout
    isn't blocked.
    """
    fix_command = (
        "Run `index-inclusion-build-cma-ar-engine-forest` to refresh "
        "the AR-engine-sweep figure."
    )
    name = "cma_ar_engine_forest_artifact"
    if not sensitivity_root.exists():
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"sensitivity cache {_relative_label(sensitivity_root)} not "
                "populated; AR-engine forest figure is opt-in."
            ),
        )
    cached_csvs = sorted(
        sensitivity_root.glob("ar_*/cma_hypothesis_verdicts.csv")
    )
    if not cached_csvs:
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"sensitivity cache {_relative_label(sensitivity_root)} has "
                "no per-engine CSVs; AR-engine forest figure is opt-in."
            ),
        )
    missing_outputs = [p for p in (png_path, pdf_path) if not p.exists()]
    if missing_outputs:
        labels = ", ".join(_relative_label(p) for p in missing_outputs)
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"AR-engine forest plot artifact(s) missing despite "
                f"populated cache ({len(cached_csvs)} engine CSV(s)): {labels}"
            ),
            fix=fix_command,
        )
    png_mtime = png_path.stat().st_mtime
    pdf_mtime = pdf_path.stat().st_mtime
    output_mtime = min(png_mtime, pdf_mtime)
    stale_inputs = [
        p for p in cached_csvs if p.stat().st_mtime > output_mtime
    ]
    if stale_inputs:
        details = tuple(
            f"{_relative_label(p)} newer than "
            f"{_relative_label(png_path)} / {_relative_label(pdf_path)}"
            for p in stale_inputs
        )
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(stale_inputs)} cached AR-engine CSV(s) newer than "
                "the AR-engine forest plot; re-run of the build CLI overdue."
            ),
            fix=fix_command,
            details=details,
        )
    engine_count = len(cached_csvs)
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"AR-engine forest plot artifacts ({_relative_label(png_path)}, "
            f"{_relative_label(pdf_path)}) are fresher than {engine_count} "
            "cached AR-engine CSV(s)."
        ),
    )


def check_cma_2d_robustness_heatmap_artifact(
    *,
    png_path: Path = DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PNG,
    pdf_path: Path = DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PDF,
    sensitivity_root: Path = DEFAULT_CMA_SENSITIVITY_ROOT,
) -> CheckResult:
    """Warn if the 2D (threshold × AR engine) robustness heatmap is
    missing or stale.

    Sister of :func:`check_cma_sensitivity_forest_artifact` and
    :func:`check_cma_ar_engine_forest_artifact`: same three regimes,
    but the inputs are the union of the dedicated 2D caches under
    ``results/sensitivity/grid_<T>_<engine>/cma_hypothesis_verdicts.csv``
    and the single-axis fallback caches under ``threshold_<T>/`` and
    ``ar_<engine>/``. If no caches exist yet the user simply hasn't
    opted into any sweep — the check stays a soft warn rather than a
    fail so a fresh checkout isn't blocked.
    """
    fix_command = (
        "Run `index-inclusion-build-cma-2d-robustness-heatmap` to refresh "
        "the 2D (threshold × engine) heatmap."
    )
    name = "cma_2d_robustness_heatmap_artifact"
    if not sensitivity_root.exists():
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"sensitivity cache {_relative_label(sensitivity_root)} not "
                "populated; 2D robustness heatmap is opt-in."
            ),
        )
    cached_csvs = sorted(
        list(sensitivity_root.glob("grid_*/cma_hypothesis_verdicts.csv"))
        + list(sensitivity_root.glob("threshold_*/cma_hypothesis_verdicts.csv"))
        + list(sensitivity_root.glob("ar_*/cma_hypothesis_verdicts.csv"))
    )
    if not cached_csvs:
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"sensitivity cache {_relative_label(sensitivity_root)} has "
                "no per-cell CSVs (grid_*, threshold_*, ar_*); 2D heatmap "
                "is opt-in."
            ),
        )
    missing_outputs = [p for p in (png_path, pdf_path) if not p.exists()]
    if missing_outputs:
        labels = ", ".join(_relative_label(p) for p in missing_outputs)
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"2D robustness heatmap artifact(s) missing despite "
                f"populated cache ({len(cached_csvs)} cell CSV(s)): {labels}"
            ),
            fix=fix_command,
        )
    png_mtime = png_path.stat().st_mtime
    pdf_mtime = pdf_path.stat().st_mtime
    output_mtime = min(png_mtime, pdf_mtime)
    stale_inputs = [p for p in cached_csvs if p.stat().st_mtime > output_mtime]
    if stale_inputs:
        details = tuple(
            f"{_relative_label(p)} newer than "
            f"{_relative_label(png_path)} / {_relative_label(pdf_path)}"
            for p in stale_inputs
        )
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(stale_inputs)} cached cell CSV(s) newer than the 2D "
                "robustness heatmap; re-run of the build CLI overdue."
            ),
            fix=fix_command,
            details=details,
        )
    cell_count = len(cached_csvs)
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"2D robustness heatmap artifacts ({_relative_label(png_path)}, "
            f"{_relative_label(pdf_path)}) are fresher than {cell_count} "
            "cached cell CSV(s)."
        ),
    )


def check_cma_sensitivity_forest_artifact(
    *,
    png_path: Path = DEFAULT_CMA_SENSITIVITY_FOREST_PNG,
    pdf_path: Path = DEFAULT_CMA_SENSITIVITY_FOREST_PDF,
    sensitivity_root: Path = DEFAULT_CMA_SENSITIVITY_ROOT,
) -> CheckResult:
    """Warn if the threshold-sweep CMA forest plot is missing or stale.

    Mirrors :func:`check_cma_verdicts_forest_artifact` (commit e049bbd)
    but for the sensitivity-aware multi-threshold version. The sweep
    inputs are the per-threshold CSVs under
    ``results/sensitivity/threshold_<T>/cma_hypothesis_verdicts.csv``;
    if no caches exist yet the user simply hasn't opted into the
    sweep, so the check stays a soft warn rather than a fail.

    The check has three regimes:

    1. **No cache directory or empty cache** → ``pass`` with a hint
       to run the CLI. This is the "fresh checkout" case and shouldn't
       block CI.
    2. **PNG/PDF missing but cache populated** → ``warn``: the cache
       implies the user wanted the sweep, but the figure was never
       built or got deleted.
    3. **PNG/PDF older than any cached CSV** → ``warn``: a CSV was
       refreshed (re-run at that threshold) but the figure didn't
       follow. Fix is the same CLI.
    """
    fix_command = (
        "Run `index-inclusion-build-cma-sensitivity-forest` to refresh "
        "the threshold-sweep figure."
    )
    name = "cma_sensitivity_forest_artifact"
    if not sensitivity_root.exists():
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"sensitivity cache {_relative_label(sensitivity_root)} not "
                "populated; threshold-sweep figure is opt-in."
            ),
        )
    cached_csvs = sorted(
        sensitivity_root.glob("threshold_*/cma_hypothesis_verdicts.csv")
    )
    if not cached_csvs:
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"sensitivity cache {_relative_label(sensitivity_root)} has "
                "no per-threshold CSVs; threshold-sweep figure is opt-in."
            ),
        )
    missing_outputs = [p for p in (png_path, pdf_path) if not p.exists()]
    if missing_outputs:
        labels = ", ".join(_relative_label(p) for p in missing_outputs)
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"sensitivity forest plot artifact(s) missing despite "
                f"populated cache ({len(cached_csvs)} threshold CSV(s)): {labels}"
            ),
            fix=fix_command,
        )
    png_mtime = png_path.stat().st_mtime
    pdf_mtime = pdf_path.stat().st_mtime
    output_mtime = min(png_mtime, pdf_mtime)
    stale_inputs = [
        p for p in cached_csvs if p.stat().st_mtime > output_mtime
    ]
    if stale_inputs:
        details = tuple(
            f"{_relative_label(p)} newer than "
            f"{_relative_label(png_path)} / {_relative_label(pdf_path)}"
            for p in stale_inputs
        )
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(stale_inputs)} cached threshold CSV(s) newer than "
                "the sensitivity forest plot; re-run of the build CLI overdue."
            ),
            fix=fix_command,
            details=details,
        )
    threshold_count = len(cached_csvs)
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"sensitivity forest plot artifacts ({_relative_label(png_path)}, "
            f"{_relative_label(pdf_path)}) are fresher than {threshold_count} "
            "cached threshold CSV(s)."
        ),
    )


def check_public_summary_freshness(
    *,
    summary_path: Path = DEFAULT_PUBLIC_SUMMARY_JSON,
    verdicts_csv_path: Path = DEFAULT_VERDICTS_CSV,
    pap_csv_path: Path = DEFAULT_PAP_DEVIATION_REPORT_CSV,
    rdd_csv_path: Path = DEFAULT_RDD_ROBUSTNESS_CSV_FOR_SUMMARY,
) -> CheckResult:
    """Warn if ``data/public/index_research_summary.json`` is missing or older
    than any of the CSVs it summarizes.

    The public summary is a committed downstream artifact for external
    consumers (cn-altdata-brief, GitHub Pages digests). If the upstream
    CSVs have changed but the summary was not regenerated, downstream will
    serve stale numbers. Mirrors the freshness pattern used by
    :func:`check_cma_verdicts_forest_artifact`.
    """
    name = "public_summary_freshness"
    fix_command = (
        "Run `index-inclusion-export-public-summary` to regenerate "
        "data/public/index_research_summary.json."
    )
    if not summary_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"public summary {_relative_label(summary_path)} is missing."
            ),
            fix=fix_command,
        )
    # Verdicts CSV is the only HARD-required input; everything else is
    # optional (sensitivity / pap / rdd may legitimately not exist on a
    # fresh checkout). If verdicts is also missing we can't judge staleness.
    if not verdicts_csv_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"public summary input {_relative_label(verdicts_csv_path)} "
                "is missing; cannot verify freshness."
            ),
            fix=fix_command,
        )
    if (
        os.getenv("CI", "").lower() == "true"
        and summary_path.is_relative_to(ROOT)
        and verdicts_csv_path.is_relative_to(ROOT)
    ):
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"public summary {_relative_label(summary_path)} is present; "
                "skipping mtime freshness in CI because checkout mtimes are "
                "not generation times."
            ),
        )
    summary_mtime = summary_path.stat().st_mtime
    stale_inputs: list[Path] = []
    for csv in (verdicts_csv_path, pap_csv_path, rdd_csv_path):
        if csv.exists() and csv.stat().st_mtime > summary_mtime:
            stale_inputs.append(csv)
    if stale_inputs:
        details = tuple(
            f"{_relative_label(p)} mtime newer than "
            f"{_relative_label(summary_path)}"
            for p in stale_inputs
        )
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(stale_inputs)} input CSV(s) newer than "
                f"{_relative_label(summary_path)}; re-run of "
                f"`index-inclusion-export-public-summary` overdue."
            ),
            fix=fix_command,
            details=details,
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"public summary {_relative_label(summary_path)} is fresher "
            f"than {_relative_label(verdicts_csv_path)} and siblings."
        ),
    )


def check_paper_skeleton_freshness(
    *,
    skeleton_path: Path = DEFAULT_PAPER_SKELETON_MD,
    verdicts_csv_path: Path = DEFAULT_VERDICTS_CSV,
    pap_csv_path: Path = DEFAULT_PAP_DEVIATION_REPORT_CSV,
    rdd_csv_path: Path = DEFAULT_RDD_ROBUSTNESS_CSV_FOR_SUMMARY,
    public_summary_path: Path = DEFAULT_PUBLIC_SUMMARY_JSON,
) -> CheckResult:
    """Warn if ``paper/skeleton.md`` is missing or older than any of its
    auto-populated input artifacts.

    The skeleton is a generated paper template that bakes in the current
    verdict table, sensitivity counts, HS300 RDD headline, and PAP
    deviation block. If any input CSV has been refreshed but the skeleton
    was not regenerated, the rendered template will misrepresent the
    current research state. Mirrors the freshness pattern used by
    :func:`check_public_summary_freshness`.
    """
    name = "paper_skeleton_freshness"
    fix_command = (
        "Run `index-inclusion-paper-skeleton --force` to regenerate "
        "paper/skeleton.md."
    )
    if (
        os.getenv("CI", "").lower() == "true"
        and skeleton_path.is_relative_to(ROOT)
        and verdicts_csv_path.is_relative_to(ROOT)
    ):
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"paper skeleton {_relative_label(skeleton_path)} is "
                "generated/gitignored; skipping presence and mtime freshness "
                "in CI because fresh checkouts do not include paper/ and "
                "checkout mtimes are not generation times."
            ),
        )
    if not skeleton_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"paper skeleton {_relative_label(skeleton_path)} is missing."
            ),
            fix=fix_command,
        )
    if not verdicts_csv_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"paper skeleton input {_relative_label(verdicts_csv_path)} "
                "is missing; cannot verify freshness."
            ),
            fix=fix_command,
        )
    skeleton_mtime = skeleton_path.stat().st_mtime
    stale_inputs: list[Path] = []
    for csv in (
        verdicts_csv_path,
        pap_csv_path,
        rdd_csv_path,
        public_summary_path,
    ):
        if csv.exists() and csv.stat().st_mtime > skeleton_mtime:
            stale_inputs.append(csv)
    if stale_inputs:
        details = tuple(
            f"{_relative_label(p)} mtime newer than "
            f"{_relative_label(skeleton_path)}"
            for p in stale_inputs
        )
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(stale_inputs)} input(s) newer than "
                f"{_relative_label(skeleton_path)}; re-run of "
                f"`index-inclusion-paper-skeleton --force` overdue."
            ),
            fix=fix_command,
            details=details,
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"paper skeleton {_relative_label(skeleton_path)} is fresher "
            f"than {_relative_label(verdicts_csv_path)} and siblings."
        ),
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
    check_rdd_robustness_panel,
    check_matched_sample_balance,
    check_match_robustness_grid,
    check_pap_deviation_no_flips,
    check_pap_snapshot_freshness,
    check_hs300_rdd_forest_artifact,
    check_cma_verdicts_forest_artifact,
    check_cma_sensitivity_forest_artifact,
    check_cma_ar_engine_forest_artifact,
    check_cma_2d_robustness_heatmap_artifact,
    check_public_summary_freshness,
    check_paper_skeleton_freshness,
    check_chart_builders_register,
    check_console_scripts_importable,
    check_heuristic_citation_centrality_schema,
    check_citation_graph_artifact,
    check_paper_audit,
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
