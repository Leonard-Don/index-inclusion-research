from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

SectionMode = Literal["brief", "demo", "full"]

SECTION_ID = "cross_market_asymmetry"
SECTION_COPY: dict[str, object] = {
    "title": "美股 vs A股 公告—生效事件集中度差异",
    "subtitle": "CN vs US announce/effective concentration",
    "lead": (
        "A 股在公告日拉价、生效日拉量；美股在公告日两样都拉、生效日反向抽回——"
        "这是跨市场不对称的核心现象。"
    ),
    "brief_summary": (
        "4 象限（CN/US × announce/effective）在 CAR 与微结构两条通道上"
        "呈现互补的集中度差异。"
    ),
    "conclusion_bullets": [
        "价格集中：公告日是两市场共同的 CAR 显著点；生效日 CAR 在两市场均未显著。",
        "量能集中：A 股在生效日出现换手 / 成交量正、波动压低的需求签名；美股反向抽回。",
        "异质性集中：不对称在小市值 / 低流动性 cell 更显著（参见 M4 矩阵）。",
    ],
}

BRIEF_FIGURES = (
    "cma_ar_path_comparison.png",
    "cma_gap_decomposition.png",
    "cma_mechanism_heatmap.png",
)

FULL_FIGURES = BRIEF_FIGURES + (
    "cma_heterogeneity_matrix_size.png",
    "cma_time_series_rolling.png",
)


def _safe_read(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def build_cross_market_section(
    *,
    tables_dir: Path,
    figures_dir: Path,
    mode: SectionMode = "full",
) -> dict[str, object]:
    """Build a dashboard-ready context dict for the CMA section.

    The returned dict is presenter-agnostic: a dashboard layer can render
    the fields through any template or route wiring. This function only
    depends on CSV and PNG artifacts produced by `run_cma_pipeline`.
    """

    tables_dir = Path(tables_dir)
    figures_dir = Path(figures_dir)

    window_summary = _safe_read(tables_dir / "cma_window_summary.csv")
    if not window_summary.empty:
        quadrant = window_summary.loc[
            (window_summary["window_start"] == -1) & (window_summary["window_end"] == 1),
            ["market", "event_phase", "car_mean", "car_t", "n_events"],
        ].reset_index(drop=True)
    else:
        quadrant = pd.DataFrame(
            columns=["market", "event_phase", "car_mean", "car_t", "n_events"]
        )

    gap_summary = _safe_read(tables_dir / "cma_gap_summary.csv")
    hypothesis_map = _safe_read(tables_dir / "cma_hypothesis_map.csv")

    if mode == "brief":
        figure_names = ()
    elif mode == "demo":
        figure_names = BRIEF_FIGURES
    else:
        figure_names = FULL_FIGURES

    figures = {
        name: str(figures_dir / name)
        for name in figure_names
        if (figures_dir / name).exists()
    }

    return {
        "id": SECTION_ID,
        "mode": mode,
        "title": SECTION_COPY["title"],
        "subtitle": SECTION_COPY["subtitle"],
        "lead": SECTION_COPY["lead"],
        "brief_summary": SECTION_COPY["brief_summary"],
        "conclusion_bullets": SECTION_COPY["conclusion_bullets"],
        "quadrant_table": {
            "columns": ["market", "event_phase", "car_mean", "car_t", "n_events"],
            "rows": quadrant.to_dict(orient="records"),
        },
        "gap_summary": {
            "columns": list(gap_summary.columns),
            "rows": gap_summary.to_dict(orient="records"),
        },
        "figures": figures,
        "hypothesis_map": {
            "columns": list(hypothesis_map.columns),
            "rows": hypothesis_map.to_dict(orient="records") if mode == "full" else [],
        },
    }
