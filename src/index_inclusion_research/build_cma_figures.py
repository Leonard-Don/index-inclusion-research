"""Unified CLI for the four CMA robustness figures.

This module replaces the four single-purpose argparse wrappers
(``build_cma_verdicts_forest`` / ``build_cma_sensitivity_forest`` /
``build_cma_ar_engine_forest`` / ``build_cma_2d_robustness_heatmap``)
with one entry point, ``index-inclusion-build-cma-figures``, that takes
``--which {forest,sensitivity,ar,heatmap,all}``.

Each sub-figure delegates to the *existing* builder under
:mod:`index_inclusion_research.outputs` — the plotting / sweep logic is
the research result and is intentionally left untouched. This file only
provides the argparse front-end and the path defaults the old wrappers
carried, so reviewers can regenerate one or all four figures on demand
without re-running the full ``index-inclusion-make-figures-tables``
pipeline.

Outputs land under ``results/figures/`` (project-wide figure home)
alongside the HS300 RDD robustness forest plot.

Cache semantics for the sweep figures (sensitivity / ar / heatmap):
each builder re-uses cached verdicts CSVs under
``results/sensitivity/`` when fresher than every CMA input that can
alter H1-H7 verdicts and otherwise falls through to the orchestrator.
There is no ``--force`` knob — to force a refresh delete the relevant
cache directory or update an upstream input. ``make figures-tables`` /
``paper_bundle`` use the separate cache-only renderers and never trigger
fresh passes from this CLI.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from pathlib import Path

from index_inclusion_research import paths
from index_inclusion_research.outputs import (
    DEFAULT_2D_AR_MODELS,
    DEFAULT_2D_THRESHOLDS,
    DEFAULT_AR_ENGINE_THRESHOLD,
    DEFAULT_AR_MODELS,
    DEFAULT_SENSITIVITY_THRESHOLDS,
    build_cma_2d_robustness_heatmap,
    build_cma_ar_engine_forest_plot,
    build_cma_sensitivity_forest_plot,
    build_cma_verdicts_forest_plot,
)

logger = logging.getLogger(__name__)

# Figure keys (the --which choices, minus the "all" meta-value).
FIGURE_KEYS: tuple[str, ...] = ("forest", "sensitivity", "ar", "heatmap")


def _figures_dir() -> Path:
    return paths.results_dir() / "figures"


def _verdicts_csv_path() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _png_path(stem: str) -> Path:
    return _figures_dir() / f"{stem}.png"


def _pdf_path(stem: str) -> Path:
    return _figures_dir() / f"{stem}.pdf"


def _build_forest() -> Path:
    """Single-snapshot cross-hypothesis verdict forest plot."""
    return build_cma_verdicts_forest_plot(
        verdicts_csv_path=_verdicts_csv_path(),
        output_png_path=_png_path("cma_verdicts_forest"),
        output_pdf_path=_pdf_path("cma_verdicts_forest"),
    )


def _build_sensitivity() -> Path:
    """Threshold-sweep forest plot (one dot per threshold per hypothesis)."""
    return build_cma_sensitivity_forest_plot(
        output_png_path=_png_path("cma_verdicts_sensitivity"),
        output_pdf_path=_pdf_path("cma_verdicts_sensitivity"),
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
    )


def _build_ar() -> Path:
    """AR-engine-sweep forest plot (one dot per AR engine per hypothesis)."""
    return build_cma_ar_engine_forest_plot(
        output_png_path=_png_path("cma_verdicts_ar_engine"),
        output_pdf_path=_pdf_path("cma_verdicts_ar_engine"),
        threshold=DEFAULT_AR_ENGINE_THRESHOLD,
        ar_models=DEFAULT_AR_MODELS,
    )


def _build_heatmap() -> Path:
    """2D (threshold × AR engine) robustness heatmap."""
    return build_cma_2d_robustness_heatmap(
        output_png_path=_png_path("cma_verdicts_2d_robustness"),
        output_pdf_path=_pdf_path("cma_verdicts_2d_robustness"),
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=DEFAULT_2D_AR_MODELS,
    )


# Dispatch table: --which key → (human label, builder thunk).
BUILDERS: dict[str, tuple[str, Callable[[], Path]]] = {
    "forest": ("CMA verdicts forest plot", _build_forest),
    "sensitivity": ("CMA verdicts sensitivity forest plot", _build_sensitivity),
    "ar": ("CMA verdicts AR-engine forest plot", _build_ar),
    "heatmap": ("CMA 2D robustness heatmap", _build_heatmap),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index-inclusion-build-cma-figures",
        description=(
            "Render one or all four CMA robustness figures: the "
            "single-snapshot verdicts forest plot, the threshold-sweep "
            "sensitivity forest plot, the AR-engine-sweep forest plot, "
            "and the 2D (threshold × AR engine) robustness heatmap. Each "
            "delegates to the existing outputs builder; figures land under "
            "results/figures/."
        ),
    )
    parser.add_argument(
        "--which",
        choices=(*FIGURE_KEYS, "all"),
        default="all",
        help=(
            "Which figure to build (default: all). 'forest' = "
            "single-snapshot verdicts; 'sensitivity' = threshold sweep; "
            "'ar' = AR-engine sweep; 'heatmap' = 2D threshold × engine grid."
        ),
    )
    return parser


def _selected_keys(which: str) -> tuple[str, ...]:
    return FIGURE_KEYS if which == "all" else (which,)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)

    for key in _selected_keys(args.which):
        label, builder = BUILDERS[key]
        png_path = builder()
        logger.info("%s written: %s", label, png_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
