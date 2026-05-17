"""CLI for building the CMA verdicts sensitivity forest plot.

Wraps :func:`build_cma_sensitivity_forest_plot` with an argparse
front-end so reviewers can regenerate the threshold-sweep figure on
demand. Defaults: 0.05 / 0.10 / 0.15 / 0.20 thresholds; output written
under ``results/figures/cma_verdicts_sensitivity.{png,pdf}`` alongside
the single-snapshot forest plot.

The CLI re-uses cached verdicts CSVs under
``results/sensitivity/threshold_<T>/`` when fresher than all CMA inputs
that can alter H1-H7 verdicts (event panels, events, passive AUM, CN
passive-AUM proxy, H6 weight-change). Passing ``--force`` is *not* a
knob — to force a refresh delete the cache directory or update an
upstream input (the runner falls through to the orchestrator
automatically). ``make figures-tables`` / ``paper_bundle`` use the
separate cache-only renderer and never trigger fresh threshold runs.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from index_inclusion_research import paths
from index_inclusion_research.outputs import (
    DEFAULT_SENSITIVITY_THRESHOLDS,
    build_cma_sensitivity_forest_plot,
)

logger = logging.getLogger(__name__)


def _default_png_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_sensitivity.png"


def _default_pdf_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_sensitivity.pdf"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the sensitivity-aware cross-hypothesis CMA verdicts "
            "forest plot (H1-H7 on the y-axis, support-strength score on "
            "the x-axis, one dot per threshold per hypothesis). Re-runs "
            "the CMA pipeline at each requested threshold (cached under "
            "results/sensitivity/threshold_<T>/); the threshold gate "
            "affects the current p-gated hypotheses H1/H4/H5."
        )
    )
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        default=list(DEFAULT_SENSITIVITY_THRESHOLDS),
        metavar="P",
        help=(
            "Threshold values to sweep (default: %(default)s). Order "
            "doesn't matter — they are sorted ascending. Values must be "
            "representable with at most two decimal places."
        ),
    )
    parser.add_argument(
        "--png",
        default=str(_default_png_path()),
        help="Output PNG path. Parent directory is created if missing.",
    )
    parser.add_argument(
        "--pdf",
        default=str(_default_pdf_path()),
        help="Output PDF path. Pass empty string to skip PDF generation.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)

    pdf_target: str | None = args.pdf.strip() or None
    png_path = build_cma_sensitivity_forest_plot(
        output_png_path=args.png,
        output_pdf_path=pdf_target,
        thresholds=args.thresholds,
    )
    logger.info("CMA verdicts sensitivity forest plot written: %s", png_path)
    if pdf_target:
        logger.info(
            "CMA verdicts sensitivity forest plot (PDF): %s", pdf_target
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
