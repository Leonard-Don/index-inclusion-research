"""CLI for building the CMA verdicts AR-engine forest plot.

Wraps :func:`build_cma_ar_engine_forest_plot` with an argparse
front-end so reviewers can regenerate the AR-engine-sweep figure on
demand. Defaults: ``adjusted`` (simple ``ret − benchmark``) + ``market``
(market-model β-AR with estimation window ``(-120, -10)``); output
written under ``results/figures/cma_verdicts_ar_engine.{png,pdf}``
alongside the threshold-sensitivity forest plot.

The CLI re-uses cached verdicts CSVs under
``results/sensitivity/ar_<engine>/`` when fresher than every CMA input
that can alter H1-H7 verdicts (event panels, events, passive AUM, CN
passive-AUM proxy, H6 weight-change). Passing ``--force`` is *not* a
knob — to force a refresh delete the cache directory or update an
upstream input (the runner falls through to the orchestrator
automatically). ``make figures-tables`` / ``paper_bundle`` use the
separate cache-only renderer and never trigger fresh AR-engine passes.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from index_inclusion_research import paths
from index_inclusion_research.outputs import (
    DEFAULT_AR_ENGINE_THRESHOLD,
    DEFAULT_AR_MODELS,
    build_cma_ar_engine_forest_plot,
)

logger = logging.getLogger(__name__)


def _default_png_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_ar_engine.png"


def _default_pdf_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_ar_engine.pdf"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the AR-engine-aware cross-hypothesis CMA verdicts "
            "forest plot (H1-H7 on the y-axis, support-strength score on "
            "the x-axis, one dot per AR engine per hypothesis). Re-runs "
            "the CMA pipeline once per requested engine (cached under "
            "results/sensitivity/ar_<engine>/); the threshold is held "
            "constant across the sweep so the comparison is purely "
            "between AR engines (default 0.10)."
        )
    )
    parser.add_argument(
        "--ar-models",
        nargs="+",
        type=str,
        default=list(DEFAULT_AR_MODELS),
        metavar="ENGINE",
        help=(
            "AR engines to sweep (default: %(default)s). Supported: "
            "'adjusted' (ret − benchmark_ret, project default) and "
            "'market' (market-model β-AR with estimation window "
            "(-120, -10) — commit 1e29476)."
        ),
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_AR_ENGINE_THRESHOLD,
        metavar="P",
        help=(
            "Significance threshold passed to the CMA pipeline for each "
            "engine pass (default: %(default)s). Held constant across "
            "the sweep on purpose — to interrogate threshold sensitivity, "
            "run `index-inclusion-build-cma-sensitivity-forest` instead."
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
    png_path = build_cma_ar_engine_forest_plot(
        output_png_path=args.png,
        output_pdf_path=pdf_target,
        threshold=args.threshold,
        ar_models=args.ar_models,
    )
    logger.info("CMA verdicts AR-engine forest plot written: %s", png_path)
    if pdf_target:
        logger.info(
            "CMA verdicts AR-engine forest plot (PDF): %s", pdf_target
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
