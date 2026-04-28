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
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VERDICTS_CSV = ROOT / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
DEFAULT_RESULTS_DIR = ROOT / "results" / "real_tables"

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


DEFAULT_CHECKS: tuple[Callable[[], CheckResult], ...] = (
    check_hypothesis_paper_ids_resolve,
    check_verdicts_csv_health,
    check_results_directory_populated,
    check_p_gated_verdict_sensitivity,
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    import sys

    parser = build_arg_parser()
    args = parser.parse_args(argv)

    results = run_all_checks()
    enable_color = not args.no_color and sys.stdout.isatty()
    print(render_results(results, color=enable_color))
    fail_count = sum(1 for r in results if r.status == "fail")
    return fail_count


if __name__ == "__main__":
    raise SystemExit(main())
