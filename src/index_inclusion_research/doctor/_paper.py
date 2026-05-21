"""Paper integrity / audit / freshness doctor checks."""

from __future__ import annotations

import os
from pathlib import Path

from ._common import (
    DEFAULT_CITATION_CENTRALITY_CSV_FOR_SUMMARY,
    DEFAULT_METHODOLOGY_SUMMARY_MD,
    DEFAULT_PAP_DEVIATION_REPORT_CSV,
    DEFAULT_PAPER_SKELETON_MD,
    DEFAULT_PUBLIC_SUMMARY_JSON,
    DEFAULT_RDD_ROBUSTNESS_CSV_FOR_SUMMARY,
    DEFAULT_VERDICTS_CSV,
    ROOT,
    CheckResult,
    Status,
    _relative_label,
)


def check_paper_integrity() -> CheckResult:
    """Cross-document integrity gate: verify the paper bundle's artifacts agree.

    Thin doctor adapter around
    :func:`index_inclusion_research.paper_integrity.check_paper_integrity`.
    Surfaces the worst severity (fail > warn > pass) so doctor's strict
    mode can flag a broken paper bundle without crowding the per-issue
    list. Run ``index-inclusion-paper-integrity`` for the full report.
    """
    from index_inclusion_research.paper_integrity import (
        check_paper_integrity_doctor,
    )

    return check_paper_integrity_doctor()


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


def check_methodology_summary_freshness(
    *,
    summary_path: Path = DEFAULT_METHODOLOGY_SUMMARY_MD,
    verdicts_csv_path: Path = DEFAULT_VERDICTS_CSV,
    public_summary_path: Path = DEFAULT_PUBLIC_SUMMARY_JSON,
    centrality_csv_path: Path = DEFAULT_CITATION_CENTRALITY_CSV_FOR_SUMMARY,
) -> CheckResult:
    """Warn if ``paper/methodology_summary.md`` is missing or older than any
    of its auto-populated input artifacts.

    The methodology summary is a generated paper artifact that bakes in
    the current verdict sample-sizes, public-summary robustness coverage,
    PAP deviation block, and top-5 centrality citations. If any input has
    been refreshed but the summary was not regenerated, the rendered card
    will misrepresent the current research state. Mirrors the freshness
    pattern used by :func:`check_paper_skeleton_freshness` and
    :func:`check_public_summary_freshness`.
    """
    name = "methodology_summary_freshness"
    fix_command = (
        "Run `index-inclusion-methodology-summary` to regenerate "
        "paper/methodology_summary.md."
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
                f"methodology summary {_relative_label(summary_path)} is "
                "generated/gitignored; skipping presence and mtime freshness "
                "in CI because fresh checkouts may not include paper/ and "
                "checkout mtimes are not generation times."
            ),
        )
    if not summary_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"methodology summary {_relative_label(summary_path)} is missing."
            ),
            fix=fix_command,
        )
    if not verdicts_csv_path.exists():
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"methodology summary input {_relative_label(verdicts_csv_path)} "
                "is missing; cannot verify freshness."
            ),
            fix=fix_command,
        )
    summary_mtime = summary_path.stat().st_mtime
    stale_inputs: list[Path] = []
    for csv in (
        verdicts_csv_path,
        public_summary_path,
        centrality_csv_path,
    ):
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
                f"{len(stale_inputs)} input(s) newer than "
                f"{_relative_label(summary_path)}; re-run of "
                f"`index-inclusion-methodology-summary` overdue."
            ),
            fix=fix_command,
            details=details,
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"methodology summary {_relative_label(summary_path)} is fresher "
            f"than {_relative_label(verdicts_csv_path)} and siblings."
        ),
    )
