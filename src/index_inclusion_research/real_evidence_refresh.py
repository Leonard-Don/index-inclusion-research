"""One-click refresh for real-data evidence and machine-readable coverage.

``index-inclusion-refresh-real-evidence`` is deliberately narrower than the
generic rebuild command: it refreshes the real-data path, H6 weight evidence,
CMA verdicts, and then writes a manifest the dashboard can render as an
evidence-coverage card.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from index_inclusion_research import doctor, paths
from index_inclusion_research.rebuild_all import PipelineStep, filter_steps, run_step

ROOT = paths.project_root()
DEFAULT_TABLES_DIR = ROOT / "results" / "real_tables"
DEFAULT_MANIFEST_JSON = DEFAULT_TABLES_DIR / "evidence_refresh_manifest.json"
DEFAULT_MANIFEST_CSV = DEFAULT_TABLES_DIR / "evidence_refresh_manifest.csv"

BASE_REFRESH_STEPS: tuple[PipelineStep, ...] = (
    PipelineStep(
        slug="build-event-sample",
        callable_path="index_inclusion_research.build_event_sample:main",
        description="Clean real_events.csv into real_events_clean.csv",
    ),
    PipelineStep(
        slug="build-price-panel",
        callable_path="index_inclusion_research.build_price_panel:main",
        description="Build real event-window panel",
    ),
    PipelineStep(
        slug="match-controls",
        callable_path="index_inclusion_research.match_controls:main",
        description="Build matched real control events",
    ),
    PipelineStep(
        slug="match-robustness",
        callable_path="index_inclusion_research.match_robustness:main",
        description="Build local-only matched balance robustness grid",
    ),
    PipelineStep(
        slug="build-matched-panel",
        callable_path="index_inclusion_research.build_price_panel:main",
        argv=(
            "--events",
            "data/processed/real_matched_events.csv",
            "--output",
            "data/processed/real_matched_event_panel.csv",
        ),
        description="Build event-window panel for matched real events",
    ),
    PipelineStep(
        slug="run-event-study",
        callable_path="index_inclusion_research.run_event_study:main",
        description="Refresh real event-study summaries",
    ),
    PipelineStep(
        slug="run-regressions",
        callable_path="index_inclusion_research.run_regressions:main",
        description="Refresh real regression outputs",
    ),
    PipelineStep(
        slug="hs300-rdd",
        callable_path="index_inclusion_research.hs300_rdd:main",
        description="Refresh HS300 RDD status and outputs",
    ),
    PipelineStep(
        slug="compute-h6-weight-change",
        callable_path="index_inclusion_research.compute_h6_weight_change:main",
        argv=("--force",),
        description="Recompute H6 weight_proxy from local real market caps",
    ),
    PipelineStep(
        slug="cma",
        callable_path="index_inclusion_research.cross_market_asymmetry:main",
        description="Refresh CMA tables, figures, verdicts, and paper block",
    ),
    PipelineStep(
        slug="make-figures-tables",
        callable_path="index_inclusion_research.figures_tables:main",
        description="Refresh dashboard figure/table artifacts",
    ),
    PipelineStep(
        slug="generate-research-report",
        callable_path="index_inclusion_research.research_report:main",
        description="Refresh generated research report",
    ),
)


def _relative_label(path: Path, *, root: Path = ROOT) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _is_missing_text(value: object) -> bool:
    if value is None or pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "<na>", "unknown"}


def _float_metric(value: object, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    try:
        return float(str(value))
    except ValueError:
        return default


def _int_metric(value: object, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return default
    try:
        return int(float(str(value)))
    except ValueError:
        return default


def compute_cn_sector_coverage(root: Path = ROOT) -> dict[str, object]:
    frames: list[pd.DataFrame] = []
    for rel in ("data/raw/real_events.csv", "data/raw/real_metadata.csv"):
        path = root / rel
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path, dtype={"ticker": str})
        except (OSError, ValueError):
            continue
        if {"market", "ticker"}.issubset(frame.columns):
            if "sector" not in frame.columns:
                frame["sector"] = pd.NA
            frames.append(frame[["market", "ticker", "sector"]].copy())
    if not frames:
        return {"total": 0, "known": 0, "rate": 0.0, "missing_tickers": []}
    combined = pd.concat(frames, ignore_index=True)
    combined["market"] = combined["market"].astype(str).str.strip().str.upper()
    cn = combined.loc[combined["market"] == "CN"].copy()
    cn["ticker"] = cn["ticker"].astype(str).str.strip().str.zfill(6)
    by_ticker = (
        cn.groupby("ticker", dropna=False)["sector"]
        .apply(lambda s: any(not _is_missing_text(value) for value in s))
        .reset_index(name="known")
    )
    total = int(len(by_ticker))
    known = int(by_ticker["known"].sum()) if total else 0
    missing = sorted(by_ticker.loc[~by_ticker["known"], "ticker"].astype(str).tolist())
    return {
        "total": total,
        "known": known,
        "rate": float(known / total) if total else 0.0,
        "missing_tickers": missing,
    }


def build_refresh_steps(
    *,
    root: Path = ROOT,
    skip_sector_enrich: bool = False,
    force_sector_enrich: bool = False,
    sector_coverage_threshold: float = 0.95,
) -> tuple[list[PipelineStep], list[dict[str, object]]]:
    pre_records: list[dict[str, object]] = []
    steps: list[PipelineStep] = []
    coverage = compute_cn_sector_coverage(root)
    rate = _float_metric(coverage["rate"])
    detail = (
        f"CN sector coverage {_int_metric(coverage['known'])}/{_int_metric(coverage['total'])} "
        f"({rate:.1%})"
    )
    if skip_sector_enrich:
        pre_records.append(_step_record("enrich-cn-sectors", "skipped", detail=detail))
    elif force_sector_enrich or rate < sector_coverage_threshold:
        steps.append(
            PipelineStep(
                slug="enrich-cn-sectors",
                callable_path="index_inclusion_research.enrich_cn_sectors:main",
                argv=("--force",),
                description="Fill missing CN sectors before rebuilding real panels",
            )
        )
    else:
        pre_records.append(_step_record("enrich-cn-sectors", "skipped", detail=detail))
    steps.extend(BASE_REFRESH_STEPS)
    return steps, pre_records


def _step_record(
    slug: str,
    status: str,
    *,
    exit_code: int | None = None,
    elapsed_sec: float | None = None,
    detail: str = "",
) -> dict[str, object]:
    record: dict[str, object] = {"slug": slug, "status": status}
    if exit_code is not None:
        record["exit_code"] = int(exit_code)
    if elapsed_sec is not None:
        record["elapsed_sec"] = round(float(elapsed_sec), 3)
    if detail:
        record["detail"] = detail
    return record


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={"ticker": str})
    except (OSError, ValueError):
        return pd.DataFrame()


def _coverage_row(
    item: str,
    label: str,
    status: str,
    value: str,
    detail: str,
) -> dict[str, str]:
    return {
        "item": item,
        "label": label,
        "status": status,
        "value": value,
        "detail": detail,
    }


def _build_aum_coverage(root: Path) -> dict[str, str]:
    path = root / "data" / "raw" / "passive_aum.csv"
    frame = _read_csv(path)
    if frame.empty or not {"market", "year", "aum_trillion"}.issubset(frame.columns):
        return _coverage_row(
            "H2_passive_aum",
            "H2 passive AUM",
            "missing",
            "0 rows",
            f"missing or invalid: {_relative_label(path, root=root)}",
        )
    us = frame.loc[frame["market"].astype(str).str.upper() == "US"].copy()
    cn = frame.loc[frame["market"].astype(str).str.upper() == "CN"].copy()
    if us.empty:
        return _coverage_row(
            "H2_passive_aum",
            "H2 passive AUM",
            "warn",
            f"{len(frame)} total rows",
            "US AUM rows are required for current H2 trend evidence",
        )
    years = pd.to_numeric(frame["year"], errors="coerce").dropna()
    year_text = f"{int(years.min())}-{int(years.max())}" if not years.empty else "unknown years"
    status = "pass" if len(us) >= 2 and len(cn) >= 2 else "warn"
    detail = f"{_relative_label(path, root=root)} years {year_text}"
    if len(cn) < 2:
        detail += "; CN comparable passive AUM missing, so H2 stays supplementary"
    return _coverage_row(
        "H2_passive_aum",
        "H2 passive AUM",
        status,
        f"US {len(us)} rows; CN {len(cn)} rows",
        detail,
    )


def _build_h6_coverage(root: Path, tables_dir: Path) -> dict[str, str]:
    weight_path = root / "data" / "processed" / "hs300_weight_change.csv"
    weights = _read_csv(weight_path)
    cn_rows = 0
    if not weights.empty and {"market", "weight_proxy"}.issubset(weights.columns):
        cn_rows = int(
            (
                (weights["market"].astype(str).str.upper() == "CN")
                & pd.to_numeric(weights["weight_proxy"], errors="coerce").notna()
            ).sum()
        )
    robustness = _read_csv(tables_dir / "cma_h6_weight_robustness.csv")
    matched = None
    detail_bits = [f"weight rows CN={cn_rows}"]
    if not robustness.empty and {"test", "n_obs"}.issubset(robustness.columns):
        coverage = robustness.loc[robustness["test"].astype(str) == "coverage"]
        if not coverage.empty:
            matched = int(coverage.iloc[0].get("n_obs", 0) or 0)
            detail_bits.append(str(coverage.iloc[0].get("detail", "")))
    status = "pass" if cn_rows > 0 and (matched or 0) > 0 else "warn"
    value = f"CN rows={cn_rows}"
    if matched is not None:
        value += f"; matched={matched}"
    return _coverage_row(
        "H6_weight_change",
        "H6 weight_change",
        status,
        value,
        "; ".join(bit for bit in detail_bits if bit),
    )


def _build_h7_coverage(root: Path) -> dict[str, str]:
    coverage = compute_cn_sector_coverage(root)
    total = _int_metric(coverage["total"])
    known = _int_metric(coverage["known"])
    rate = _float_metric(coverage["rate"])
    missing = coverage.get("missing_tickers", [])
    missing_preview = ", ".join(list(missing)[:5]) if isinstance(missing, list) else ""
    return _coverage_row(
        "H7_cn_sector",
        "H7 CN sector",
        "pass" if total and rate >= 0.95 else "warn",
        f"{known}/{total} ({rate:.1%})",
        f"missing tickers: {missing_preview}" if missing_preview else "CN sectors usable",
    )


def _build_rdd_coverage(root: Path) -> dict[str, str]:
    formal = root / "data" / "raw" / "hs300_rdd_candidates.csv"
    reconstructed = root / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
    if formal.exists():
        rows = len(_read_csv(formal))
        return _coverage_row(
            "RDD_L3_boundary",
            "HS300 RDD L3",
            "pass",
            f"formal rows={rows}",
            _relative_label(formal, root=root),
        )
    if reconstructed.exists():
        rows = len(_read_csv(reconstructed))
        return _coverage_row(
            "RDD_L3_boundary",
            "HS300 RDD L3",
            "warn",
            f"fallback rows={rows}",
            (
                "formal boundary file missing; active public reconstructed sample: "
                f"{_relative_label(reconstructed, root=root)}"
            ),
        )
    return _coverage_row(
        "RDD_L3_boundary",
        "HS300 RDD L3",
        "missing",
        "0 rows",
        "no formal or reconstructed HS300 candidate file found",
    )


def _build_verdict_coverage(tables_dir: Path, *, root: Path = ROOT) -> dict[str, str]:
    path = tables_dir / "cma_hypothesis_verdicts.csv"
    verdicts = _read_csv(path)
    if verdicts.empty or "verdict" not in verdicts.columns:
        return _coverage_row(
            "CMA_verdicts",
            "CMA verdicts",
            "missing",
            "0 rows",
            f"missing or invalid: {_relative_label(path, root=root)}",
        )
    order = ["支持", "部分支持", "证据不足", "待补数据"]
    counts = verdicts["verdict"].astype(str).value_counts().to_dict()
    value = "; ".join(f"{label}={int(counts.get(label, 0))}" for label in order)
    pending = int(counts.get("待补数据", 0))
    return _coverage_row(
        "CMA_verdicts",
        "CMA verdicts",
        "pass" if pending == 0 and len(verdicts) >= 7 else "warn",
        value,
        f"{len(verdicts)} H-row verdicts under {_relative_label(tables_dir, root=root)}",
    )


def _build_match_robustness_coverage(root: Path) -> dict[str, str]:
    path = root / "results" / "real_regressions" / "match_robustness_grid.csv"
    grid = _read_csv(path)
    if grid.empty or "spec_id" not in grid.columns:
        return _coverage_row(
            "Match_robustness",
            "Match robustness",
            "missing",
            "0 specs",
            f"missing or invalid: {_relative_label(path, root=root)}",
        )
    over_source = (
        grid["over_threshold_covariates"]
        if "over_threshold_covariates" in grid.columns
        else pd.Series(index=grid.index, dtype="float64")
    )
    max_abs_source = (
        grid["max_abs_smd"]
        if "max_abs_smd" in grid.columns
        else pd.Series(index=grid.index, dtype="float64")
    )
    over = pd.to_numeric(over_source, errors="coerce")
    max_abs = pd.to_numeric(max_abs_source, errors="coerce")
    ranked = grid.assign(
        _over_sort=over.fillna(float("inf")),
        _max_abs_sort=max_abs.fillna(float("inf")),
    ).sort_values(["_over_sort", "_max_abs_sort", "spec_id"], ignore_index=True)
    best = ranked.iloc[0]
    best_over_value = float(best["_over_sort"])
    best_max_value = float(best["_max_abs_sort"])
    best_over = int(best_over_value) if best_over_value < float("inf") else 0
    best_max = best_max_value if best_max_value < float("inf") else float("nan")
    default = (
        grid.loc[grid["is_default"].astype(bool)]
        if "is_default" in grid.columns
        else pd.DataFrame()
    )
    detail_bits = [f"specs={len(grid)}"]
    if not default.empty:
        row = default.iloc[0]
        detail_bits.append(
            "default="
            f"{row.get('spec_id')} over={int(row.get('over_threshold_covariates', 0))}"
        )
    return _coverage_row(
        "Match_robustness",
        "Match robustness",
        "pass" if best_over == 0 else "warn",
        f"best={best.get('spec_id')}; over={best_over}; max|SMD|={best_max:.3f}",
        "; ".join(detail_bits),
    )


def build_evidence_manifest(
    *,
    root: Path = ROOT,
    tables_dir: Path = DEFAULT_TABLES_DIR,
    step_records: Sequence[dict[str, object]] = (),
    doctor_results: Sequence[doctor.CheckResult] | None = None,
) -> dict[str, object]:
    root = Path(root)
    tables_dir = Path(tables_dir)
    results = list(doctor_results) if doctor_results is not None else doctor.run_all_checks()
    summary = doctor.results_summary(results)
    doctor_status = "fail" if summary["fail"] else "warn" if summary["warn"] else "pass"
    coverage = [
        _build_aum_coverage(root),
        _build_h6_coverage(root, tables_dir),
        _build_h7_coverage(root),
        _build_rdd_coverage(root),
        _build_verdict_coverage(tables_dir, root=root),
        _build_match_robustness_coverage(root),
        _coverage_row(
            "doctor",
            "doctor",
            doctor_status,
            f"{summary['pass']} pass / {summary['warn']} warn / {summary['fail']} fail",
            f"{summary['total']} checks total",
        ),
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "root": str(root),
        "tables_dir": str(tables_dir),
        "steps": list(step_records),
        "coverage": coverage,
        "doctor": doctor.results_payload(results),
    }


def write_evidence_manifest(
    manifest: dict[str, object],
    *,
    json_path: Path = DEFAULT_MANIFEST_JSON,
    csv_path: Path = DEFAULT_MANIFEST_CSV,
) -> tuple[Path, Path]:
    json_path = Path(json_path)
    csv_path = Path(csv_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    pd.DataFrame(manifest.get("coverage", [])).to_csv(csv_path, index=False)
    return json_path, csv_path


def run_refresh_pipeline(
    *,
    root: Path = ROOT,
    tables_dir: Path = DEFAULT_TABLES_DIR,
    manifest_json_path: Path = DEFAULT_MANIFEST_JSON,
    manifest_csv_path: Path = DEFAULT_MANIFEST_CSV,
    only: Sequence[str] | None = None,
    start_from: str | None = None,
    skip: Sequence[str] | None = None,
    skip_sector_enrich: bool = False,
    force_sector_enrich: bool = False,
    sector_coverage_threshold: float = 0.95,
    step_runner: Callable[[PipelineStep], int] = run_step,
) -> dict[str, object]:
    steps, records = build_refresh_steps(
        root=root,
        skip_sector_enrich=skip_sector_enrich,
        force_sector_enrich=force_sector_enrich,
        sector_coverage_threshold=sector_coverage_threshold,
    )
    steps = filter_steps(steps, only=only, start_from=start_from, skip=skip)
    exit_code = 0
    for step in steps:
        print(f"[refresh-real-evidence] start {step.slug}")
        started = time.monotonic()
        try:
            rc = int(step_runner(step) or 0)
        except Exception as exc:  # noqa: BLE001
            elapsed = time.monotonic() - started
            records.append(
                _step_record(
                    step.slug,
                    "error",
                    exit_code=2,
                    elapsed_sec=elapsed,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            )
            exit_code = 2
            break
        elapsed = time.monotonic() - started
        status = "pass" if rc == 0 else "fail"
        records.append(_step_record(step.slug, status, exit_code=rc, elapsed_sec=elapsed))
        print(f"[refresh-real-evidence] {status} {step.slug} ({elapsed:.1f}s)")
        if rc != 0:
            exit_code = rc
            break

    doctor_results = doctor.run_all_checks()
    if exit_code == 0:
        exit_code = doctor.doctor_exit_code(doctor_results)
    manifest = build_evidence_manifest(
        root=root,
        tables_dir=tables_dir,
        step_records=records,
        doctor_results=doctor_results,
    )
    json_path, csv_path = write_evidence_manifest(
        manifest,
        json_path=manifest_json_path,
        csv_path=manifest_csv_path,
    )
    return {
        "exit_code": exit_code,
        "manifest": manifest,
        "json_path": json_path,
        "csv_path": csv_path,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh real-data evidence and write dashboard evidence manifest."
    )
    parser.add_argument("--list", action="store_true", help="Print planned steps and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned steps and exit.")
    parser.add_argument("--only", nargs="+", metavar="STEP", help="Run only these steps.")
    parser.add_argument("--from", dest="start_from", metavar="STEP", help="Start from STEP.")
    parser.add_argument("--skip", nargs="+", metavar="STEP", help="Skip these steps.")
    parser.add_argument(
        "--skip-sector-enrich",
        action="store_true",
        help="Do not run CN sector enrichment even if coverage is below threshold.",
    )
    parser.add_argument(
        "--force-sector-enrich",
        action="store_true",
        help="Run CN sector enrichment regardless of current coverage.",
    )
    parser.add_argument(
        "--sector-coverage-threshold",
        type=float,
        default=0.95,
        help="Auto-run sector enrichment when CN sector coverage is below this rate.",
    )
    parser.add_argument(
        "--tables-dir",
        default=str(DEFAULT_TABLES_DIR),
        help="CMA real tables directory.",
    )
    parser.add_argument(
        "--manifest-json",
        default=str(DEFAULT_MANIFEST_JSON),
        help="Manifest JSON output path.",
    )
    parser.add_argument(
        "--manifest-csv",
        default=str(DEFAULT_MANIFEST_CSV),
        help="Manifest coverage CSV output path.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        steps, records = build_refresh_steps(
            skip_sector_enrich=args.skip_sector_enrich,
            force_sector_enrich=args.force_sector_enrich,
            sector_coverage_threshold=args.sector_coverage_threshold,
        )
        steps = filter_steps(
            steps,
            only=args.only,
            start_from=args.start_from,
            skip=args.skip,
        )
    except ValueError as exc:
        print(f"[refresh-real-evidence] {exc}")
        return 1

    if args.list or args.dry_run:
        print(f"[refresh-real-evidence] planned: {len(steps)} step(s)")
        for record in records:
            print(f"  - {record['slug']}: {record['status']} ({record.get('detail', '')})")
        for step in steps:
            argv_text = (" " + " ".join(step.argv)) if step.argv else ""
            print(f"  - {step.slug}: {step.callable_path}{argv_text}")
            if step.description:
                print(f"      {step.description}")
        return 0

    result = run_refresh_pipeline(
        tables_dir=Path(args.tables_dir),
        manifest_json_path=Path(args.manifest_json),
        manifest_csv_path=Path(args.manifest_csv),
        only=args.only,
        start_from=args.start_from,
        skip=args.skip,
        skip_sector_enrich=args.skip_sector_enrich,
        force_sector_enrich=args.force_sector_enrich,
        sector_coverage_threshold=args.sector_coverage_threshold,
    )
    manifest = result["manifest"]
    coverage = manifest.get("coverage", []) if isinstance(manifest, dict) else []
    print(f"[refresh-real-evidence] manifest: {result['json_path']}")
    for row in coverage:
        if isinstance(row, dict):
            print(
                f"  - {row.get('label', row.get('item'))}: "
                f"{row.get('status')} · {row.get('value')}"
            )
    return _int_metric(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
