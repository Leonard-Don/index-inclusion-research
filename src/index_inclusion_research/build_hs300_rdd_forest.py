"""CLI for building the HS300 RDD robustness forest plot.

Wraps :func:`index_inclusion_research.outputs.build_hs300_rdd_forest_plot`
with an argparse front-end so reviewers can regenerate the figure on
demand without re-running the full ``index-inclusion-make-figures-tables``
pipeline. The defaults point at the project canonical paths so the
common case is a single zero-argument invocation.

Outputs are written to ``results/figures/`` (project-wide figure home)
and mirrored to ``results/literature/hs300_rdd/figures/`` so the
dashboard's existing entry point keeps working unchanged.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from index_inclusion_research import paths
from index_inclusion_research.outputs import build_hs300_rdd_forest_plot

logger = logging.getLogger(__name__)


def _default_csv_path() -> Path:
    return paths.literature_results_dir() / "hs300_rdd" / "rdd_robustness.csv"


def _default_png_path() -> Path:
    return paths.results_dir() / "figures" / "hs300_rdd_robustness_forest.png"


def _default_pdf_path() -> Path:
    return paths.results_dir() / "figures" / "hs300_rdd_robustness_forest.pdf"


def _default_dashboard_mirror_path() -> Path:
    return (
        paths.literature_results_dir()
        / "hs300_rdd"
        / "figures"
        / "rdd_robustness_forest.png"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Render the HS300 RDD robustness forest plot (main / donut / "
            "placebo±0.05 / polynomial) from rdd_robustness.csv."
        )
    )
    parser.add_argument(
        "--robustness-csv",
        default=str(_default_csv_path()),
        help="Path to rdd_robustness.csv (defaults to the canonical results path).",
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
    parser.add_argument(
        "--mirror-dashboard",
        action="store_true",
        default=True,
        help=(
            "Also copy the PNG to results/literature/hs300_rdd/figures/"
            "rdd_robustness_forest.png so the dashboard entry point sees "
            "a refreshed file. Defaults to on."
        ),
    )
    parser.add_argument(
        "--no-mirror-dashboard",
        dest="mirror_dashboard",
        action="store_false",
        help="Disable dashboard mirroring.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)

    pdf_target: str | None = args.pdf.strip() or None
    png_path = build_hs300_rdd_forest_plot(
        robustness_csv_path=args.robustness_csv,
        output_png_path=args.png,
        output_pdf_path=pdf_target,
    )
    logger.info("HS300 RDD forest plot written: %s", png_path)
    if pdf_target:
        logger.info("HS300 RDD forest plot (PDF): %s", pdf_target)

    if args.mirror_dashboard:
        mirror = _default_dashboard_mirror_path()
        mirror.parent.mkdir(parents=True, exist_ok=True)
        mirror.write_bytes(Path(png_path).read_bytes())
        logger.info("Dashboard mirror updated: %s", mirror)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
