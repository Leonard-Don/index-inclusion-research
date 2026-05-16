"""CLI for building the CMA verdicts cross-hypothesis forest plot.

Wraps :func:`index_inclusion_research.outputs.build_cma_verdicts_forest_plot`
with an argparse front-end so reviewers can regenerate the figure on
demand without re-running the full ``index-inclusion-make-figures-tables``
pipeline. Defaults point at canonical paths so the common case is a
single zero-argument invocation.

Outputs are written to ``results/figures/`` (project-wide figure home)
alongside the HS300 RDD robustness forest plot so the two paper-ready
forests live together.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from index_inclusion_research import paths
from index_inclusion_research.outputs import build_cma_verdicts_forest_plot

logger = logging.getLogger(__name__)


def _default_csv_path() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_png_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_forest.png"


def _default_pdf_path() -> Path:
    return paths.results_dir() / "figures" / "cma_verdicts_forest.pdf"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the cross-hypothesis CMA verdicts forest plot "
            "(H1-H7 on the y-axis, support-strength score on the x-axis) "
            "from cma_hypothesis_verdicts.csv."
        )
    )
    parser.add_argument(
        "--verdicts-csv",
        default=str(_default_csv_path()),
        help="Path to cma_hypothesis_verdicts.csv (defaults to the canonical results path).",
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
    png_path = build_cma_verdicts_forest_plot(
        verdicts_csv_path=args.verdicts_csv,
        output_png_path=args.png,
        output_pdf_path=pdf_target,
    )
    logger.info("CMA verdicts forest plot written: %s", png_path)
    if pdf_target:
        logger.info("CMA verdicts forest plot (PDF): %s", pdf_target)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
