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
import os
from collections.abc import Callable, Sequence
from pathlib import Path

import pandas as pd

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
from ._common import (
    _RESET,
    _STATUS_COLOR,
    _STATUS_GLYPH,
    DEFAULT_CITATION_CENTRALITY_CSV,
    DEFAULT_CITATION_CENTRALITY_CSV_FOR_SUMMARY,
    DEFAULT_METHODOLOGY_SUMMARY_MD,
    DEFAULT_PAP_DEVIATION_REPORT_CSV,
    DEFAULT_PAPER_SKELETON_MD,
    DEFAULT_PUBLIC_SUMMARY_JSON,
    DEFAULT_RDD_ROBUSTNESS_CSV_FOR_SUMMARY,
    DEFAULT_RESULTS_DIR,
    DEFAULT_VERDICTS_CSV,
    EXPECTED_CMA_OUTPUTS,
    ROOT,
    CheckResult,
    Status,
    _relative_label,
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

logger = logging.getLogger(__name__)


# ── individual checks ────────────────────────────────────────────────






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


# ── orchestrator ─────────────────────────────────────────────────────






# ── PAP discipline + figure-freshness checks ─────────────────────────






















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
    check_methodology_summary_freshness,
    check_chart_builders_register,
    check_console_scripts_importable,
    check_heuristic_citation_centrality_schema,
    check_citation_graph_artifact,
    check_verdict_timeline_artifact,
    check_literature_timeline_artifact,
    check_paper_audit,
    check_paper_integrity,
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
