"""Audit whether paper-facing claims are backed by current artifacts.

The audit is intentionally claim-oriented: each item maps one writing or
defense claim to the concrete CSV / figure / narrative files that make it
safe to say. The same result powers the CLI, doctor, paper bundle gate, and
dashboard section so the project has one source of truth for delivery health.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from index_inclusion_research import paths

Status = str  # "pass" / "warn" / "fail"

ROOT = paths.project_root()
DEFAULT_EVENT_STUDY_SUMMARY = ROOT / "results" / "real_tables" / "event_study_summary.csv"
DEFAULT_PATELL_BMP_SUMMARY = ROOT / "results" / "real_event_study" / "patell_bmp_summary.csv"
DEFAULT_VERDICTS = ROOT / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
DEFAULT_RDD_STATUS = ROOT / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
DEFAULT_RDD_ROBUSTNESS = ROOT / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
DEFAULT_MCCRARY = ROOT / "results" / "literature" / "hs300_rdd" / "mccrary_density_test.csv"


@dataclass(frozen=True)
class AuditResult:
    name: str
    status: Status
    claim: str
    message: str
    fix: str = ""
    artifacts: tuple[str, ...] = field(default_factory=tuple)
    details: tuple[str, ...] = field(default_factory=tuple)


def _relative(path: Path, *, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _existing_artifacts(paths_: Sequence[Path], *, root: Path) -> tuple[str, ...]:
    return tuple(_relative(path, root=root) for path in paths_ if path.exists())


def _missing_artifacts(paths_: Sequence[Path], *, root: Path) -> tuple[str, ...]:
    return tuple(_relative(path, root=root) for path in paths_ if not path.exists())


def _fail_missing(
    *,
    name: str,
    claim: str,
    paths_: Sequence[Path],
    root: Path,
    fix: str,
) -> AuditResult | None:
    missing = _missing_artifacts(paths_, root=root)
    if not missing:
        return None
    return AuditResult(
        name=name,
        status="fail",
        claim=claim,
        message=f"{len(missing)} required artifact(s) are missing.",
        fix=fix,
        artifacts=_existing_artifacts(paths_, root=root),
        details=missing,
    )


def audit_main_event_study(root: Path = ROOT, *, require_bundle: bool = True) -> AuditResult:
    claim = "正文主结论：CN / US inclusion 的公告日 CAR[-1,+1] 显著为正，生效日不是机械跳涨主轴。"
    required: tuple[Path, ...] = (
        root / "docs" / "research_delivery_package.md",
        root / "docs" / "paper_outline.md",
        root / "results" / "real_tables" / "event_study_summary.csv",
        root / "results" / "real_tables" / "event_study_summary.tex",
    )
    if require_bundle:
        required = (
            *required,
            root / "paper" / "tables" / "event_study_summary.tex",
            root / "paper" / "narrative" / "research_delivery_package.md",
        )
    missing = _fail_missing(
        name="main_event_study_claim",
        claim=claim,
        paths_=required,
        root=root,
        fix="Run `make rebuild && make paper`, then inspect docs/research_delivery_package.md.",
    )
    if missing:
        return missing

    try:
        summary = _read_csv(root / "results" / "real_tables" / "event_study_summary.csv")
    except Exception as exc:  # noqa: BLE001
        return AuditResult(
            name="main_event_study_claim",
            status="fail",
            claim=claim,
            message=f"event_study_summary.csv is unreadable: {exc}",
            fix="Regenerate event-study outputs with `index-inclusion-run-event-study`.",
            artifacts=_existing_artifacts(required, root=root),
        )
    required_cols = {"market", "event_phase", "inclusion", "window_slug", "mean_car", "p_value"}
    missing_cols = required_cols - set(summary.columns)
    if missing_cols:
        return AuditResult(
            name="main_event_study_claim",
            status="fail",
            claim=claim,
            message=f"event_study_summary.csv is missing column(s): {sorted(missing_cols)}.",
            fix="Regenerate event-study outputs with the current schema.",
            artifacts=_existing_artifacts(required, root=root),
        )
    rows = summary.loc[
        (summary["event_phase"] == "announce")
        & (summary["window_slug"] == "m1_p1")
        & (summary["inclusion"] == 1)
        & (summary["market"].isin(["CN", "US"]))
    ].copy()
    if set(rows["market"].astype(str)) != {"CN", "US"}:
        return AuditResult(
            name="main_event_study_claim",
            status="fail",
            claim=claim,
            message="CN / US announce inclusion rows are not both present.",
            fix="Rebuild event-study outputs before citing the cross-market headline.",
            artifacts=_existing_artifacts(required, root=root),
        )
    weak = rows.loc[(rows["mean_car"].astype(float) <= 0) | (rows["p_value"].astype(float) >= 0.05)]
    details = tuple(
        f"{row['market']} announce inclusion CAR={float(row['mean_car']):.2%}, p={float(row['p_value']):.4f}"
        for _, row in rows.sort_values("market").iterrows()
    )
    if not weak.empty:
        return AuditResult(
            name="main_event_study_claim",
            status="warn",
            claim=claim,
            message="The cross-market announce headline is present but no longer uniformly positive and significant.",
            fix="Update the paper narrative or investigate the rebuilt event-study inputs.",
            artifacts=_existing_artifacts(required, root=root),
            details=details,
        )
    return AuditResult(
        name="main_event_study_claim",
        status="pass",
        claim=claim,
        message=(
            "CN and US announce inclusion rows are positive and significant"
            + (", and paper bundle copies are present." if require_bundle else ".")
        ),
        artifacts=_existing_artifacts(required, root=root),
        details=details,
    )


def audit_patell_bmp(root: Path = ROOT, *, require_bundle: bool = True) -> AuditResult:
    claim = "方法稳健性：Patell/BMP 标准化异常收益仍支持公告日 inclusion effect。"
    required: tuple[Path, ...] = (
        root / "results" / "real_event_study" / "patell_bmp_summary.csv",
        root / "docs" / "limitations.md",
        root / "docs" / "research_delivery_package.md",
    )
    if require_bundle:
        required = (
            *required,
            root / "paper" / "tables" / "patell_bmp_summary.csv",
            root / "paper" / "narrative" / "limitations.md",
        )
    missing = _fail_missing(
        name="patell_bmp_claim",
        claim=claim,
        paths_=required,
        root=root,
        fix="Run `make rebuild && make paper`; the bundle should copy patell_bmp_summary.csv into paper/tables/.",
    )
    if missing:
        return missing
    try:
        patell = _read_csv(root / "results" / "real_event_study" / "patell_bmp_summary.csv")
    except Exception as exc:  # noqa: BLE001
        return AuditResult(
            name="patell_bmp_claim",
            status="fail",
            claim=claim,
            message=f"patell_bmp_summary.csv is unreadable: {exc}",
            fix="Regenerate event-study outputs with `index-inclusion-run-event-study`.",
            artifacts=_existing_artifacts(required, root=root),
        )
    required_cols = {"market", "event_phase", "inclusion", "window_slug", "patell_p", "bmp_p"}
    missing_cols = required_cols - set(patell.columns)
    if missing_cols:
        return AuditResult(
            name="patell_bmp_claim",
            status="fail",
            claim=claim,
            message=f"patell_bmp_summary.csv is missing column(s): {sorted(missing_cols)}.",
            fix="Regenerate Patell/BMP outputs with the current schema.",
            artifacts=_existing_artifacts(required, root=root),
        )
    rows = patell.loc[
        (patell["event_phase"] == "announce")
        & (patell["window_slug"] == "m1_p1")
        & (patell["inclusion"] == 1)
        & (patell["market"].isin(["CN", "US"]))
    ].copy()
    if set(rows["market"].astype(str)) != {"CN", "US"}:
        return AuditResult(
            name="patell_bmp_claim",
            status="fail",
            claim=claim,
            message="CN / US announce inclusion Patell/BMP rows are not both present.",
            fix="Regenerate Patell/BMP outputs before citing standardized robustness.",
            artifacts=_existing_artifacts(required, root=root),
        )
    weak = rows.loc[(rows["patell_p"].astype(float) >= 0.05) | (rows["bmp_p"].astype(float) >= 0.10)]
    details = tuple(
        f"{row['market']} Patell p={float(row['patell_p']):.4g}, BMP p={float(row['bmp_p']):.4g}"
        for _, row in rows.sort_values("market").iterrows()
    )
    if not weak.empty:
        return AuditResult(
            name="patell_bmp_claim",
            status="warn",
            claim=claim,
            message="Standardized announce robustness is present but at least one market is weak.",
            fix="Tighten the narrative around standardized robustness or inspect the event-study inputs.",
            artifacts=_existing_artifacts(required, root=root),
            details=details,
        )
    return AuditResult(
        name="patell_bmp_claim",
        status="pass",
        claim=claim,
        message=(
            "Patell/BMP announce inclusion rows are available for both markets"
            + (" and copied into the paper bundle." if require_bundle else ".")
        ),
        artifacts=_existing_artifacts(required, root=root),
        details=details,
    )


def audit_cma_core(root: Path = ROOT, *, require_bundle: bool = True) -> AuditResult:
    claim = "机制主表：正文只引用 evidence_tier=core 的 H1 / H5 / H7，supplementary 走附录。"
    required: tuple[Path, ...] = (
        root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
        root / "results" / "real_tables" / "cma_hypothesis_verdicts.tex",
        root / "docs" / "pre_registration.md",
        root / "docs" / "paper_outline_verdicts.md",
        root / "snapshots" / "pre-registration-2026-05-03.csv",
    )
    if require_bundle:
        required = (
            *required,
            root / "paper" / "tables" / "cma_hypothesis_verdicts.tex",
            root / "paper" / "narrative" / "pre_registration.md",
        )
    missing = _fail_missing(
        name="cma_core_claim",
        claim=claim,
        paths_=required,
        root=root,
        fix="Run `index-inclusion-cma && make paper`; keep docs/pre_registration.md in sync with current verdicts.",
    )
    if missing:
        return missing
    try:
        verdicts = _read_csv(root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv")
    except Exception as exc:  # noqa: BLE001
        return AuditResult(
            name="cma_core_claim",
            status="fail",
            claim=claim,
            message=f"cma_hypothesis_verdicts.csv is unreadable: {exc}",
            fix="Regenerate CMA outputs with `index-inclusion-cma`.",
            artifacts=_existing_artifacts(required, root=root),
        )
    required_cols = {"hid", "evidence_tier", "verdict"}
    missing_cols = required_cols - set(verdicts.columns)
    if missing_cols:
        return AuditResult(
            name="cma_core_claim",
            status="fail",
            claim=claim,
            message=f"cma_hypothesis_verdicts.csv is missing column(s): {sorted(missing_cols)}.",
            fix="Regenerate CMA outputs with the current evidence_tier schema.",
            artifacts=_existing_artifacts(required, root=root),
        )
    core = set(verdicts.loc[verdicts["evidence_tier"].astype(str) == "core", "hid"].astype(str))
    expected = {"H1", "H5", "H7"}
    details = tuple(
        f"{row['hid']} · {row['evidence_tier']} · {row['verdict']}"
        for _, row in verdicts.sort_values("hid").iterrows()
    )
    if core != expected:
        return AuditResult(
            name="cma_core_claim",
            status="warn",
            claim=claim,
            message=f"Core hypothesis set is {sorted(core)}, expected {sorted(expected)}.",
            fix="Update docs/pre_registration.md §7 before changing evidence_tier, or restore the frozen tier mapping.",
            artifacts=_existing_artifacts(required, root=root),
            details=details,
        )
    return AuditResult(
        name="cma_core_claim",
        status="pass",
        claim=claim,
        message=(
            "Core mechanism set matches PAP (H1/H5/H7)"
            + (", with paper bundle and narrative copies present." if require_bundle else ".")
        ),
        artifacts=_existing_artifacts(required, root=root),
        details=details,
    )


def audit_rdd_appendix(root: Path = ROOT, *, require_bundle: bool = True) -> AuditResult:
    claim = "HS300 RDD：作为附录 / preliminary 识别补充，不能在 L3 ≥10 年前进主表。"
    required: tuple[Path, ...] = (
        root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv",
        root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv",
        root / "results" / "literature" / "hs300_rdd" / "mccrary_density_test.csv",
        root / "docs" / "hs300_rdd_l3_collection_audit.md",
        root / "docs" / "research_delivery_package.md",
    )
    if require_bundle:
        required = (
            *required,
            root / "paper" / "rdd" / "rdd_status.csv",
            root / "paper" / "rdd" / "rdd_robustness.csv",
            root / "paper" / "rdd" / "mccrary_density_test.csv",
            root / "paper" / "narrative" / "hs300_rdd_l3_collection_audit.md",
        )
    missing = _fail_missing(
        name="rdd_appendix_claim",
        claim=claim,
        paths_=required,
        root=root,
        fix="Run `index-inclusion-hs300-rdd && make paper`, then keep RDD language preliminary until L3 reaches the PAP threshold.",
    )
    if missing:
        return missing
    try:
        status = _read_csv(root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv")
        robustness = _read_csv(root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv")
        mccrary = _read_csv(root / "results" / "literature" / "hs300_rdd" / "mccrary_density_test.csv")
    except Exception as exc:  # noqa: BLE001
        return AuditResult(
            name="rdd_appendix_claim",
            status="fail",
            claim=claim,
            message=f"RDD audit input is unreadable: {exc}",
            fix="Regenerate RDD outputs with `index-inclusion-hs300-rdd`.",
            artifacts=_existing_artifacts(required, root=root),
        )
    if status.empty:
        return AuditResult(
            name="rdd_appendix_claim",
            status="fail",
            claim=claim,
            message="rdd_status.csv is empty.",
            fix="Regenerate RDD outputs with `index-inclusion-hs300-rdd`.",
            artifacts=_existing_artifacts(required, root=root),
        )
    first = status.iloc[0]
    batches = int(float(first.get("candidate_batches", 0) or 0))
    rows = int(float(first.get("candidate_rows", 0) or 0))
    kinds = set(robustness.get("spec_kind", pd.Series(dtype=str)).astype(str))
    expected_kinds = {"main", "donut", "placebo", "polynomial"}
    missing_kinds = expected_kinds - kinds
    details = [
        f"RDD L3 rows={rows}, batches={batches}, tier={first.get('evidence_tier', '')}",
        f"McCrary rows={len(mccrary)}",
    ]
    if missing_kinds:
        return AuditResult(
            name="rdd_appendix_claim",
            status="warn",
            claim=claim,
            message=f"RDD robustness panel is missing spec kind(s): {sorted(missing_kinds)}.",
            fix="Rerun `index-inclusion-hs300-rdd` before citing the RDD appendix.",
            artifacts=_existing_artifacts(required, root=root),
            details=tuple(details),
        )
    package_text = (root / "docs" / "research_delivery_package.md").read_text(encoding="utf-8")
    if "preliminary" not in package_text and "不进入主表" not in package_text and "不进主表" not in package_text:
        return AuditResult(
            name="rdd_appendix_claim",
            status="warn",
            claim=claim,
            message="RDD outputs are present, but the delivery package does not mark RDD as preliminary / appendix-only.",
            fix="Update docs/research_delivery_package.md before presenting the RDD result.",
            artifacts=_existing_artifacts(required, root=root),
            details=tuple(details),
        )
    return AuditResult(
        name="rdd_appendix_claim",
        status="pass",
        claim=claim,
        message="RDD appendix artifacts, robustness panel, McCrary output, and preliminary wording are present.",
        artifacts=_existing_artifacts(required, root=root),
        details=tuple(details),
    )


def audit_pap_limitations(root: Path = ROOT, *, require_bundle: bool = True) -> AuditResult:
    claim = "写作边界：PAP、limitations、verdict diff 与当前 verdicts 保持同一套口径。"
    required: tuple[Path, ...] = (
        root / "docs" / "pre_registration.md",
        root / "docs" / "limitations.md",
        root / "docs" / "verdict_iteration.md",
        root / "snapshots" / "pre-registration-2026-05-03.csv",
    )
    if require_bundle:
        required = (
            *required,
            root / "paper" / "narrative" / "pre_registration.md",
            root / "paper" / "narrative" / "limitations.md",
            root / "paper" / "narrative" / "verdict_iteration.md",
        )
    missing = _fail_missing(
        name="pap_limitations_claim",
        claim=claim,
        paths_=required,
        root=root,
        fix="Run `make paper`; if verdicts changed, update docs/pre_registration.md §7 before presenting the new state.",
    )
    if missing:
        return missing
    from index_inclusion_research.dashboard_loaders import load_pap_summary

    pap = load_pap_summary(root)
    if not pap.get("available"):
        return AuditResult(
            name="pap_limitations_claim",
            status="fail",
            claim=claim,
            message="No PAP snapshot is available.",
            fix="Create a snapshots/pre-registration-YYYY-MM-DD.csv baseline before treating hypotheses as confirmatory.",
            artifacts=_existing_artifacts(required, root=root),
        )
    drift_state = str(pap.get("drift_state", ""))
    details = (
        f"baseline={pap.get('baseline_date', '')}",
        f"drift_state={drift_state}",
        f"summary={pap.get('summary_label', '')}",
    )
    if drift_state == "drift":
        return AuditResult(
            name="pap_limitations_claim",
            status="warn",
            claim=claim,
            message="Current verdicts drift from the latest PAP snapshot.",
            fix="Record the change in docs/pre_registration.md §7 or restore the baseline verdicts.",
            artifacts=_existing_artifacts(required, root=root),
            details=details,
        )
    if drift_state == "missing":
        return AuditResult(
            name="pap_limitations_claim",
            status="fail",
            claim=claim,
            message="PAP exists but current cma_hypothesis_verdicts.csv is missing.",
            fix="Run `index-inclusion-cma` before building the paper package.",
            artifacts=_existing_artifacts(required, root=root),
            details=details,
        )
    return AuditResult(
        name="pap_limitations_claim",
        status="pass",
        claim=claim,
        message=(
            "PAP baseline, limitations, and verdict-diff workflow are in sync"
            + (" with paper narrative copies." if require_bundle else ".")
        ),
        artifacts=_existing_artifacts(required, root=root),
        details=details,
    )


def audit_paper_bundle(root: Path = ROOT, *, require_bundle: bool = True) -> AuditResult:
    claim = "交付包：paper/ 聚合正文表、图、叙事、RDD 与 PAP 数据，可以独立支撑写作/答辩。"
    required = (
        root / "paper" / "README.md",
        root / "paper" / "bundle_summary.md",
        root / "paper" / "tables" / "event_study_summary.tex",
        root / "paper" / "tables" / "cma_hypothesis_verdicts.tex",
        root / "paper" / "tables" / "patell_bmp_summary.csv",
        root / "paper" / "figures" / "cma_mechanism_heatmap.png",
        root / "paper" / "rdd" / "rdd_robustness.csv",
        root / "paper" / "narrative" / "research_delivery_package.md",
        root / "paper" / "data" / "pre-registration-2026-05-03.csv",
    )
    missing = _fail_missing(
        name="paper_bundle_claim",
        claim=claim,
        paths_=required,
        root=root,
        fix="Run `make paper` to refresh the bundle after changing results or narrative docs.",
    )
    if missing:
        return missing
    return AuditResult(
        name="paper_bundle_claim",
        status="pass",
        claim=claim,
        message="Paper bundle contains the expected tables, figures, RDD files, narrative docs, and PAP snapshot.",
        artifacts=_existing_artifacts(required, root=root),
    )


SOURCE_AUDITS: tuple[Callable[..., AuditResult], ...] = (
    audit_main_event_study,
    audit_patell_bmp,
    audit_cma_core,
    audit_rdd_appendix,
    audit_pap_limitations,
)

DEFAULT_AUDITS: tuple[Callable[..., AuditResult], ...] = (
    *SOURCE_AUDITS,
    audit_paper_bundle,
)


def run_paper_audit(
    root: Path = ROOT,
    audits: Sequence[Callable[..., AuditResult]] | None = None,
    *,
    require_bundle: bool = True,
) -> list[AuditResult]:
    selected_audits = audits or (DEFAULT_AUDITS if require_bundle else SOURCE_AUDITS)
    results: list[AuditResult] = []
    for audit in selected_audits:
        try:
            results.append(audit(root, require_bundle=require_bundle))
        except Exception as exc:  # noqa: BLE001
            results.append(
                AuditResult(
                    name=getattr(audit, "__name__", "paper_audit"),
                    status="fail",
                    claim="paper audit failed before claim mapping completed",
                    message=f"audit raised {type(exc).__name__}: {exc}",
                )
            )
    return results


def summarize_audit(results: Sequence[AuditResult]) -> dict[str, int]:
    counts = {"pass": 0, "warn": 0, "fail": 0, "total": len(results)}
    for result in results:
        if result.status in counts:
            counts[result.status] += 1
    return counts


def audit_exit_code(results: Sequence[AuditResult], *, fail_on_warn: bool = False) -> int:
    summary = summarize_audit(results)
    return int(summary["fail"] + (summary["warn"] if fail_on_warn else 0))


def render_audit_text(results: Sequence[AuditResult]) -> str:
    lines: list[str] = []
    for result in results:
        glyph = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}.get(result.status, result.status.upper())
        lines.append(f"[{glyph}] {result.name}: {result.message}")
        lines.append(f"  claim: {result.claim}")
        if result.artifacts:
            lines.append("  artifacts:")
            for artifact in result.artifacts[:8]:
                lines.append(f"    - {artifact}")
            if len(result.artifacts) > 8:
                lines.append(f"    - ... {len(result.artifacts) - 8} more")
        if result.details:
            lines.append("  details:")
            for detail in result.details[:8]:
                lines.append(f"    - {detail}")
            if len(result.details) > 8:
                lines.append(f"    - ... {len(result.details) - 8} more")
        if result.fix:
            lines.append(f"  fix: {result.fix}")
    summary = summarize_audit(results)
    lines.append("")
    lines.append(
        f"{summary['pass']} pass · {summary['warn']} warn · {summary['fail']} fail · {summary['total']} total"
    )
    return "\n".join(lines)


def render_audit_json(results: Sequence[AuditResult]) -> str:
    payload: dict[str, Any] = {
        "summary": summarize_audit(results),
        "checks": [
            {
                "name": result.name,
                "status": result.status,
                "claim": result.claim,
                "message": result.message,
                "fix": result.fix,
                "artifacts": list(result.artifacts),
                "details": list(result.details),
            }
            for result in results
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index-inclusion-paper-audit",
        description="Audit paper-facing claims against current result artifacts.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Return a non-zero exit code when any audit item warns.",
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Project root override (default: auto-detected project root).",
    )
    parser.add_argument(
        "--source-only",
        action="store_true",
        help="Skip generated paper/ bundle-copy checks; useful for fresh checkouts before `make paper`.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    root = Path(args.root).resolve() if args.root else ROOT
    results = run_paper_audit(root, require_bundle=not bool(args.source_only))
    if args.format == "json":
        print(render_audit_json(results))
    else:
        print(render_audit_text(results))
    return audit_exit_code(results, fail_on_warn=bool(args.fail_on_warn))


if __name__ == "__main__":
    raise SystemExit(main())
