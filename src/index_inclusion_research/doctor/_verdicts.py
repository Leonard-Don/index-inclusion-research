"""Verdict, verdict-baseline snapshot, and hypothesis-set doctor checks."""

from __future__ import annotations

import datetime as _dt
import re
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from ._common import (
    DEFAULT_EVENT_COUNTS_CSV,
    DEFAULT_EVENT_STUDY_SUMMARY_CSV,
    DEFAULT_PAP_DEVIATION_REPORT_CSV,
    DEFAULT_PAPER_VERDICTS_DOC,
    DEFAULT_SNAPSHOTS_DIR,
    DEFAULT_VERDICTS_CSV,
    EXPECTED_HIDS,
    PAP_SNAPSHOT_GLOB,
    PAP_SNAPSHOT_STALE_DAYS,
    ROOT,
    CheckResult,
    _relative_label,
)


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
    """Surface verdict drift: warn on tightened/weakened, fail on flipped.

    Reads ``results/real_tables/pap_deviation_report.csv`` (regenerating
    it in-process via :mod:`pap_diff` if missing). Compares every
    hypothesis row's ``classification`` against the frozen verdict baseline
    snapshot so any verdict that flipped since the baseline shows up as a
    hard failure — that's the case the referee will hit hardest.
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
                f"Verdict baseline deviation report not found and could not regenerate: "
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
            message=f"Verdict baseline deviation report unreadable: {exc}",
            fix="Regenerate with `index-inclusion-pap-diff`.",
        )
    if "classification" not in df.columns:
        return CheckResult(
            name=name,
            status="warn",
            message="Verdict baseline deviation report is missing the 'classification' column.",
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
                f"have flipped vs the frozen verdict baseline snapshot."
            ),
            fix="Run `make verdicts && make paper` to inspect changed rows; document any flip before presenting the new state.",
            details=details,
        )
    if not drifted_rows.empty:
        details = tuple(_row_label(row) for _, row in drifted_rows.iterrows())
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"{len(drifted_rows)} of {len(df)} hypothesis verdict(s) "
                f"drifted (tightened/weakened) vs the frozen verdict baseline snapshot."
            ),
            fix="Run `make verdicts && make paper` to inspect changed rows.",
            details=details,
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"All {len(df)} hypothesis verdict(s) are unchanged vs the frozen "
            f"verdict baseline snapshot."
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
    """Warn when the latest verdict baseline snapshot is > ``stale_days`` old.

    The verdict baseline snapshot should be re-baselined quarterly to keep
    ``pap-diff`` honest; a snapshot older than 90 days is a sign the team
    forgot to refresh after a verdict iteration. Missing snapshots entirely
    is treated as a hard error — there's nothing for ``pap-diff`` to compare
    against.
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
                f"No verdict baseline snapshots found under "
                f"{_relative_label(snapshots_dir)}/{PAP_SNAPSHOT_GLOB}."
            ),
            fix=(
                "Copy results/real_tables/cma_hypothesis_verdicts.csv to "
                "snapshots/pre-registration-YYYY-MM-DD.csv to seed the verdict baseline."
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
                f"Latest verdict baseline snapshot {_relative_label(latest)} doesn't carry a "
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
                f"Latest verdict baseline snapshot {_relative_label(latest)} is "
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
            f"Latest verdict baseline snapshot {_relative_label(latest)} is {age_days} day(s) old "
            f"(≤ {stale_days})."
        ),
    )
