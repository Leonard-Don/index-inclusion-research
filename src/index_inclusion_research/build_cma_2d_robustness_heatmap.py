"""CLI for building the CMA 2D robustness heatmap (threshold × AR engine).

Wraps :func:`build_cma_2d_robustness_heatmap` with an argparse front
end so reviewers can regenerate the full 7 × (4 thresholds × 2
engines) = 56-cell robustness picture on demand. The runner first
falls back to existing single-axis caches under
``results/sensitivity/threshold_<T>/`` and ``results/sensitivity/ar_<engine>/``
so the previous threshold-sensitivity (commit 87d624c) and
AR-engine-sensitivity (commit 1a6ba77) work is reused. Only the cells
without a fallback cache trigger a fresh CMA pass.

Output written under ``results/figures/cma_verdicts_2d_robustness.{png,pdf}``
alongside the single-axis robustness forest plots. ``make
figures-tables`` / ``paper-bundle`` re-renderers use the separate
cache-only renderer and never trigger fresh 2D passes.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from index_inclusion_research import paths
from index_inclusion_research.outputs import (
    DEFAULT_2D_AR_MODELS,
    DEFAULT_2D_THRESHOLDS,
    build_cma_2d_robustness_heatmap,
)

logger = logging.getLogger(__name__)


def _default_png_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_2d_robustness.png"


def _default_pdf_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_2d_robustness.pdf"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the 2D (threshold × AR engine) robustness heatmap for "
            "CMA hypothesis verdicts (H1-H7 on the y-axis, 8 cells per "
            "hypothesis on the x-axis). Re-uses cached verdicts from the "
            "1D threshold sweep (results/sensitivity/threshold_<T>/) and "
            "the 1D AR-engine sweep (results/sensitivity/ar_<engine>/) "
            "and only fires the CMA pipeline for the cells without an "
            "existing cache. New caches are written under "
            "results/sensitivity/grid_<T>_<engine>/."
        )
    )
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        default=list(DEFAULT_2D_THRESHOLDS),
        metavar="P",
        help=(
            "Thresholds to sweep on the x-axis (default: %(default)s). "
            "Custom values must round to two decimals."
        ),
    )
    parser.add_argument(
        "--ar-models",
        nargs="+",
        type=str,
        default=list(DEFAULT_2D_AR_MODELS),
        metavar="ENGINE",
        help=(
            "AR engines to sweep alongside the thresholds (default: "
            "%(default)s). Supported: 'adjusted' (ret − benchmark_ret, "
            "project default) and 'market' (market-model β-AR with "
            "estimation window (-120, -10), commit 1e29476)."
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
    png_path = build_cma_2d_robustness_heatmap(
        output_png_path=args.png,
        output_pdf_path=pdf_target,
        thresholds=args.thresholds,
        ar_models=args.ar_models,
    )
    logger.info("CMA 2D robustness heatmap written: %s", png_path)
    if pdf_target:
        logger.info("CMA 2D robustness heatmap (PDF): %s", pdf_target)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
