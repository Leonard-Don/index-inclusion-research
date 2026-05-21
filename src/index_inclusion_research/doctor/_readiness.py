"""Data- and sample-readiness doctor checks."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ._common import (
    DEFAULT_HETEROGENEITY_SECTOR_CSV,
    DEFAULT_MATCH_BALANCE_CSV,
    DEFAULT_MATCH_ROBUSTNESS_GRID_CSV,
    DEFAULT_RDD_STATUS_DIR,
    DEFAULT_VERDICTS_CSV,
    DEFAULT_WEIGHT_CHANGE_CSV,
    ROOT,
    CheckResult,
    _relative_label,
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
