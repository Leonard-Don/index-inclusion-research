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
import json
import logging
from collections.abc import Callable, Sequence

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
    CheckResult,
)
from ._misc import (
    check_chart_builders_register,
    check_console_scripts_importable,
    check_heuristic_citation_centrality_schema,
    check_results_directory_populated,
)
from ._paper import (
    check_methodology_summary_freshness,
    check_paper_audit,
    check_paper_integrity,
    check_paper_skeleton_freshness,
    check_public_summary_freshness,
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






































# ── orchestrator ─────────────────────────────────────────────────────






# ── PAP discipline + figure-freshness checks ─────────────────────────




























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
