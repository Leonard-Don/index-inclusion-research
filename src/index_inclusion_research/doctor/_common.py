"""Shared primitives for doctor checks: result type, paths, helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from index_inclusion_research import paths

ROOT = paths.project_root()
DEFAULT_VERDICTS_CSV = ROOT / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
DEFAULT_RESULTS_DIR = ROOT / "results" / "real_tables"
DEFAULT_RDD_STATUS_DIR = ROOT / "results" / "literature" / "hs300_rdd"
DEFAULT_PAPER_VERDICTS_DOC = ROOT / "docs" / "paper_outline_verdicts.md"
DEFAULT_EVENT_COUNTS_CSV = DEFAULT_RESULTS_DIR / "event_counts_by_year.csv"
DEFAULT_EVENT_STUDY_SUMMARY_CSV = DEFAULT_RESULTS_DIR / "event_study_summary.csv"
DEFAULT_WEIGHT_CHANGE_CSV = ROOT / "data" / "processed" / "hs300_weight_change.csv"
DEFAULT_HETEROGENEITY_SECTOR_CSV = DEFAULT_RESULTS_DIR / "cma_heterogeneity_sector.csv"
DEFAULT_MATCH_BALANCE_CSV = ROOT / "results" / "real_regressions" / "match_balance.csv"
DEFAULT_MATCH_ROBUSTNESS_GRID_CSV = ROOT / "results" / "real_regressions" / "match_robustness_grid.csv"
DEFAULT_PAP_DEVIATION_REPORT_CSV = DEFAULT_RESULTS_DIR / "pap_deviation_report.csv"
DEFAULT_SNAPSHOTS_DIR = ROOT / "snapshots"
DEFAULT_HS300_RDD_FOREST_PNG = ROOT / "results" / "figures" / "hs300_rdd_robustness_forest.png"
DEFAULT_HS300_RDD_FOREST_PDF = ROOT / "results" / "figures" / "hs300_rdd_robustness_forest.pdf"
DEFAULT_HS300_RDD_ROBUSTNESS_CSV = DEFAULT_RDD_STATUS_DIR / "rdd_robustness.csv"
DEFAULT_CMA_VERDICTS_FOREST_PNG = ROOT / "results" / "figures" / "cma_verdicts_forest.png"
DEFAULT_CMA_VERDICTS_FOREST_PDF = ROOT / "results" / "figures" / "cma_verdicts_forest.pdf"
DEFAULT_CMA_SENSITIVITY_FOREST_PNG = (
    ROOT / "results" / "figures" / "cma_verdicts_sensitivity.png"
)
DEFAULT_CMA_SENSITIVITY_FOREST_PDF = (
    ROOT / "results" / "figures" / "cma_verdicts_sensitivity.pdf"
)
DEFAULT_CMA_SENSITIVITY_ROOT = ROOT / "results" / "sensitivity"
DEFAULT_CMA_AR_ENGINE_FOREST_PNG = (
    ROOT / "results" / "figures" / "cma_verdicts_ar_engine.png"
)
DEFAULT_CMA_AR_ENGINE_FOREST_PDF = (
    ROOT / "results" / "figures" / "cma_verdicts_ar_engine.pdf"
)
DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PNG = (
    ROOT / "results" / "figures" / "cma_verdicts_2d_robustness.png"
)
DEFAULT_CMA_2D_ROBUSTNESS_HEATMAP_PDF = (
    ROOT / "results" / "figures" / "cma_verdicts_2d_robustness.pdf"
)
DEFAULT_PUBLIC_SUMMARY_JSON = (
    ROOT / "data" / "public" / "index_research_summary.json"
)
DEFAULT_RDD_ROBUSTNESS_CSV_FOR_SUMMARY = (
    ROOT / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
)
DEFAULT_CITATION_CENTRALITY_CSV = (
    ROOT / "results" / "literature" / "citation_centrality.csv"
)
DEFAULT_CITATION_NETWORK_PNG = (
    ROOT / "results" / "literature" / "citation_network.png"
)
DEFAULT_CITATION_NETWORK_PDF = (
    ROOT / "results" / "literature" / "citation_network.pdf"
)
DEFAULT_VERDICT_TIMELINE_PNG = (
    ROOT / "results" / "figures" / "verdict_timeline.png"
)
DEFAULT_VERDICT_TIMELINE_PDF = (
    ROOT / "results" / "figures" / "verdict_timeline.pdf"
)
DEFAULT_VERDICT_TIMELINE_SOURCE_CSV = (
    ROOT / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
)
DEFAULT_LITERATURE_TIMELINE_PNG = (
    ROOT / "results" / "literature" / "literature_timeline.png"
)
DEFAULT_LITERATURE_TIMELINE_PDF = (
    ROOT / "results" / "literature" / "literature_timeline.pdf"
)
DEFAULT_LITERATURE_TIMELINE_SOURCE_CSV = (
    ROOT / "results" / "literature" / "citation_centrality.csv"
)
DEFAULT_PAPER_SKELETON_MD = ROOT / "paper" / "skeleton.md"
DEFAULT_METHODOLOGY_SUMMARY_MD = ROOT / "paper" / "methodology_summary.md"
DEFAULT_CITATION_CENTRALITY_CSV_FOR_SUMMARY = (
    ROOT / "results" / "literature" / "citation_centrality.csv"
)


Status = str  # one of "pass" / "warn" / "fail"


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: Status
    message: str
    fix: str = ""
    details: tuple[str, ...] = field(default_factory=tuple)


def _relative_label(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


PAP_SNAPSHOT_GLOB = "pre-registration-*.csv"
PAP_SNAPSHOT_STALE_DAYS = 90

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
    "cma_h6_weight_robustness.csv",
    "cma_h6_weight_explanation.csv",
)

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
