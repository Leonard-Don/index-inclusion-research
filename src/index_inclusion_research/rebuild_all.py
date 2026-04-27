"""Reproducibility entry point: ``index-inclusion-rebuild-all``.

Runs the full pipeline in dependency order so any user can rebuild the
project from raw data → CMA verdicts → dashboard artifacts in a single
command. Each step is a tuple of (slug, module-main, argv) so tests can
stub them out and ``--from`` / ``--only`` can resume or cherry-pick.

Default order
~~~~~~~~~~~~~

1. ``build-event-sample``         events + sample → real_events_clean
2. ``build-price-panel``          events + prices → real_event_panel
3. ``match-controls``             events + panel → real_matched_events
4. ``build-matched-panel``        matched events → real_matched_event_panel
5. ``run-event-study``            panel → event_study_summary
6. ``run-regressions``            matched panel → regression_coefficients
7. ``hs300-rdd``                  CN events → hs300_rdd outputs
8. ``cma``                        full CMA pipeline
9. ``make-figures-tables``        refresh figures + tables
10. ``generate-research-report``  research_summary.md

Each step calls a module-local ``main(argv)`` so the orchestration stays
in-process; subprocess shelling is avoided to keep tests fast and
exit-code semantics consistent.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class PipelineStep:
    slug: str
    callable_path: str
    argv: tuple[str, ...] = ()
    description: str = ""


# Steps are referenced by dotted ``module:attr`` paths so we can lazy-import
# them inside ``run_step``; importing matplotlib + statsmodels eagerly via
# every step's ``main`` would blow up startup time.
DEFAULT_STEPS: tuple[PipelineStep, ...] = (
    PipelineStep(
        slug="build-event-sample",
        callable_path="index_inclusion_research.build_event_sample:main",
        description="Clean events.csv → data/processed/real_events_clean.csv",
    ),
    PipelineStep(
        slug="build-price-panel",
        callable_path="index_inclusion_research.build_price_panel:main",
        description="Build event-window panel from events + prices",
    ),
    PipelineStep(
        slug="match-controls",
        callable_path="index_inclusion_research.match_controls:main",
        description="Match treated events with sector + size controls",
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
        description="Build event-window panel for matched events",
    ),
    PipelineStep(
        slug="run-event-study",
        callable_path="index_inclusion_research.run_event_study:main",
        description="Compute event-study summaries (CAR + asymmetry)",
    ),
    PipelineStep(
        slug="run-regressions",
        callable_path="index_inclusion_research.run_regressions:main",
        description="Run main + mechanism regressions on matched panel",
    ),
    PipelineStep(
        slug="hs300-rdd",
        callable_path="index_inclusion_research.hs300_rdd:main",
        description="Run HS300 RDD analysis on CN events",
    ),
    PipelineStep(
        slug="cma",
        callable_path="index_inclusion_research.cross_market_asymmetry:main",
        description="Run cross-market asymmetry pipeline (verdicts + LaTeX)",
    ),
    PipelineStep(
        slug="make-figures-tables",
        callable_path="index_inclusion_research.figures_tables:main",
        description="Refresh figures and LaTeX tables",
    ),
    PipelineStep(
        slug="generate-research-report",
        callable_path="index_inclusion_research.research_report:main",
        description="Write research_summary.md",
    ),
)


def _resolve_callable(path: str) -> Callable[[Sequence[str] | None], int | None]:
    """Resolve ``module:attr`` lazily so import cost is per-step."""
    module_path, _, attr = path.partition(":")
    if not module_path or not attr:
        raise ValueError(f"Invalid callable path: {path!r}")
    module = __import__(module_path, fromlist=[attr])
    return getattr(module, attr)


def run_step(
    step: PipelineStep,
    *,
    callable_resolver: Callable[[str], Callable] = _resolve_callable,
) -> int:
    """Invoke ``step.callable_path(step.argv)`` and return its exit code."""
    fn = callable_resolver(step.callable_path)
    rc = fn(list(step.argv))
    return 0 if rc is None else int(rc)


def run_pipeline(
    steps: Sequence[PipelineStep] = DEFAULT_STEPS,
    *,
    callable_resolver: Callable[[str], Callable] = _resolve_callable,
    on_event: Callable[[str, str, int | None, float | None], None] | None = None,
) -> int:
    """Run *steps* in order. Stop + return at the first non-zero exit code.

    ``on_event`` receives ``(step_slug, phase, exit_code_or_None, elapsed)``
    where phase is ``"start"`` / ``"finish"`` / ``"error"``. Tests use this
    to assert sequencing without parsing stdout.
    """
    for step in steps:
        if on_event:
            on_event(step.slug, "start", None, None)
        started = time.monotonic()
        try:
            rc = run_step(step, callable_resolver=callable_resolver)
        except Exception as exc:
            elapsed = time.monotonic() - started
            logger.exception("step %s raised", step.slug)
            if on_event:
                on_event(step.slug, "error", None, elapsed)
            print(f"[rebuild-all] step {step.slug} raised {type(exc).__name__}: {exc}")
            return 2
        elapsed = time.monotonic() - started
        if rc != 0:
            if on_event:
                on_event(step.slug, "error", rc, elapsed)
            print(f"[rebuild-all] step {step.slug} exited with rc={rc}; aborting")
            return rc
        if on_event:
            on_event(step.slug, "finish", rc, elapsed)
        print(f"[rebuild-all] ✓ {step.slug}  ({elapsed:.1f}s)")
    return 0


def filter_steps(
    steps: Sequence[PipelineStep],
    *,
    only: Sequence[str] | None = None,
    start_from: str | None = None,
    skip: Sequence[str] | None = None,
) -> list[PipelineStep]:
    """Apply ``--only`` / ``--from`` / ``--skip`` filters to step list."""
    selected = list(steps)
    if start_from:
        for idx, step in enumerate(selected):
            if step.slug == start_from:
                selected = selected[idx:]
                break
        else:
            raise ValueError(f"--from step not found: {start_from}")
    if only:
        only_set = set(only)
        selected = [s for s in selected if s.slug in only_set]
    if skip:
        skip_set = set(skip)
        selected = [s for s in selected if s.slug not in skip_set]
    return selected


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full index-inclusion research pipeline in dependency "
            "order: events → panel → event-study → regressions → RDD → CMA "
            "→ figures → research report."
        )
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the planned step list and exit (no execution).",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="STEP",
        help="Run only the named steps (space-separated).",
    )
    parser.add_argument(
        "--from",
        dest="start_from",
        metavar="STEP",
        help="Skip earlier steps and start from STEP.",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        metavar="STEP",
        help="Skip the named steps even if they would otherwise run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print each step's callable path + argv but do not execute.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        steps = filter_steps(
            DEFAULT_STEPS,
            only=args.only,
            start_from=args.start_from,
            skip=args.skip,
        )
    except ValueError as exc:
        print(f"[rebuild-all] {exc}")
        return 1

    if args.list or args.dry_run:
        if not steps:
            print("[rebuild-all] no steps selected.")
            return 0
        print(f"[rebuild-all] planned: {len(steps)} step(s)")
        for step in steps:
            argv_text = (" " + " ".join(step.argv)) if step.argv else ""
            print(f"  - {step.slug}: {step.callable_path}{argv_text}")
            if step.description:
                print(f"      {step.description}")
        return 0

    if not steps:
        print("[rebuild-all] no steps selected after filters.")
        return 0
    print(f"[rebuild-all] running {len(steps)} step(s) in order:")
    return run_pipeline(steps)


if __name__ == "__main__":
    raise SystemExit(main())
