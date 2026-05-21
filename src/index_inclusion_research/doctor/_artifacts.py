"""Figure / forest / timeline artifact-freshness doctor checks."""

from __future__ import annotations

import os
from pathlib import Path

from ._common import (
    DEFAULT_CITATION_CENTRALITY_CSV,
    DEFAULT_CITATION_NETWORK_PDF,
    DEFAULT_CITATION_NETWORK_PNG,
    DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PDF,
    DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PNG,
    DEFAULT_CMA_AR_ENGINE_FOREST_PDF,
    DEFAULT_CMA_AR_ENGINE_FOREST_PNG,
    DEFAULT_CMA_SENSITIVITY_FOREST_PDF,
    DEFAULT_CMA_SENSITIVITY_FOREST_PNG,
    DEFAULT_CMA_SENSITIVITY_ROOT,
    DEFAULT_CMA_VERDICTS_FOREST_PDF,
    DEFAULT_CMA_VERDICTS_FOREST_PNG,
    DEFAULT_HS300_RDD_FOREST_PDF,
    DEFAULT_HS300_RDD_FOREST_PNG,
    DEFAULT_HS300_RDD_ROBUSTNESS_CSV,
    DEFAULT_LITERATURE_TIMELINE_PDF,
    DEFAULT_LITERATURE_TIMELINE_PNG,
    DEFAULT_LITERATURE_TIMELINE_SOURCE_CSV,
    DEFAULT_VERDICT_TIMELINE_PDF,
    DEFAULT_VERDICT_TIMELINE_PNG,
    DEFAULT_VERDICT_TIMELINE_SOURCE_CSV,
    DEFAULT_VERDICTS_CSV,
    ROOT,
    CheckResult,
    _relative_label,
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
        allow_ci_missing_generated=True,
    )


def check_verdict_timeline_artifact(
    *,
    png_path: Path = DEFAULT_VERDICT_TIMELINE_PNG,
    pdf_path: Path = DEFAULT_VERDICT_TIMELINE_PDF,
    source_csv_path: Path = DEFAULT_VERDICT_TIMELINE_SOURCE_CSV,
) -> CheckResult:
    """Warn if the H1..H7 verdict-evolution timeline figure is missing or stale.

    Mirrors :func:`check_citation_graph_artifact`: the PNG / PDF twins
    must exist and both have an mtime ≥ the verdicts CSV they were
    rendered from. Re-run ``index-inclusion-verdict-timeline`` to refresh
    both artifacts (idempotent — overwrites in place).
    """
    return _forest_artifact_status(
        name="verdict_timeline_artifact",
        png_path=png_path,
        pdf_path=pdf_path,
        input_csv_path=source_csv_path,
        fix_command=(
            "Run `index-inclusion-verdict-timeline` to refresh the verdict "
            "timeline figure (PNG + PDF) reconstructed from the git log "
            "of cma_hypothesis_verdicts.csv."
        ),
        input_label="cma_hypothesis_verdicts.csv",
        allow_ci_missing_generated=True,
    )


def check_literature_timeline_artifact(
    *,
    png_path: Path = DEFAULT_LITERATURE_TIMELINE_PNG,
    pdf_path: Path = DEFAULT_LITERATURE_TIMELINE_PDF,
    source_csv_path: Path = DEFAULT_LITERATURE_TIMELINE_SOURCE_CSV,
) -> CheckResult:
    """Warn if the 16-paper literature chronology figure is missing or stale.

    Mirrors :func:`check_verdict_timeline_artifact`: the PNG / PDF twins
    must exist and both have an mtime ≥ the centrality CSV they were
    rendered from (the renderer's marker-size scale depends on the CSV's
    ``in_degree`` column). Re-run ``index-inclusion-literature-timeline``
    to refresh both artifacts (idempotent — overwrites in place).
    """
    return _forest_artifact_status(
        name="literature_timeline_artifact",
        png_path=png_path,
        pdf_path=pdf_path,
        input_csv_path=source_csv_path,
        fix_command=(
            "Run `index-inclusion-literature-timeline` to refresh the "
            "literature chronology figure (PNG + PDF) rendered from "
            "PAPER_LIBRARY + citation_centrality.csv."
        ),
        input_label="citation_centrality.csv",
        allow_ci_missing_generated=True,
    )


def _forest_artifact_status(
    *,
    name: str,
    png_path: Path,
    pdf_path: Path,
    input_csv_path: Path,
    fix_command: str,
    input_label: str,
    allow_ci_missing_generated: bool = False,
) -> CheckResult:
    """Shared core for the two forest-plot freshness checks."""
    missing = [p for p in (png_path, pdf_path) if not p.exists()]
    if missing:
        if (
            allow_ci_missing_generated
            and os.getenv("CI", "").lower() == "true"
            and png_path.is_relative_to(ROOT)
            and pdf_path.is_relative_to(ROOT)
            and input_csv_path.is_relative_to(ROOT)
        ):
            labels = ", ".join(_relative_label(p) for p in missing)
            return CheckResult(
                name=name,
                status="pass",
                message=(
                    f"generated/gitignored forest plot artifact(s) missing in CI: {labels}; "
                    "skipping presence check for fresh checkouts."
                ),
            )
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
