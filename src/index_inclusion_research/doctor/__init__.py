"""Project health-check CLI and reusable doctor checks."""

from __future__ import annotations

from collections.abc import Sequence

from . import _checks as _impl
from ._checks import (
    DEFAULT_CHECKS as DEFAULT_CHECKS,
)
from ._checks import (
    DEFAULT_CITATION_CENTRALITY_CSV as DEFAULT_CITATION_CENTRALITY_CSV,
)
from ._checks import (
    DEFAULT_VERDICT_TIMELINE_SOURCE_CSV as DEFAULT_VERDICT_TIMELINE_SOURCE_CSV,
)
from ._checks import (
    DEFAULT_VERDICTS_CSV as DEFAULT_VERDICTS_CSV,
)
from ._checks import (
    ROOT as ROOT,
)
from ._checks import (
    CheckResult as CheckResult,
)
from ._checks import (
    check_chart_builders_register as check_chart_builders_register,
)
from ._checks import (
    check_citation_graph_artifact as check_citation_graph_artifact,
)
from ._checks import (
    check_cma_2d_robustness_heatmap_artifact as check_cma_2d_robustness_heatmap_artifact,
)
from ._checks import (
    check_cma_ar_engine_forest_artifact as check_cma_ar_engine_forest_artifact,
)
from ._checks import (
    check_cma_sensitivity_forest_artifact as check_cma_sensitivity_forest_artifact,
)
from ._checks import (
    check_cma_verdicts_forest_artifact as check_cma_verdicts_forest_artifact,
)
from ._checks import (
    check_console_scripts_importable as check_console_scripts_importable,
)
from ._checks import (
    check_h6_weight_change_readiness as check_h6_weight_change_readiness,
)
from ._checks import (
    check_h7_cn_sector_readiness as check_h7_cn_sector_readiness,
)
from ._checks import (
    check_heuristic_citation_centrality_schema as check_heuristic_citation_centrality_schema,
)
from ._checks import (
    check_hs300_rdd_forest_artifact as check_hs300_rdd_forest_artifact,
)
from ._checks import (
    check_hypothesis_paper_ids_resolve as check_hypothesis_paper_ids_resolve,
)
from ._checks import (
    check_match_robustness_grid as check_match_robustness_grid,
)
from ._checks import (
    check_matched_sample_balance as check_matched_sample_balance,
)
from ._checks import (
    check_p_gated_verdict_sensitivity as check_p_gated_verdict_sensitivity,
)
from ._checks import (
    check_pap_deviation_no_flips as check_pap_deviation_no_flips,
)
from ._checks import (
    check_pap_snapshot_freshness as check_pap_snapshot_freshness,
)
from ._checks import (
    check_paper_audit as check_paper_audit,
)
from ._checks import (
    check_paper_skeleton_freshness as check_paper_skeleton_freshness,
)
from ._checks import (
    check_paper_verdict_section_synced as check_paper_verdict_section_synced,
)
from ._checks import (
    check_pending_data_verdicts as check_pending_data_verdicts,
)
from ._checks import (
    check_public_summary_freshness as check_public_summary_freshness,
)
from ._checks import (
    check_rdd_l3_sample_readiness as check_rdd_l3_sample_readiness,
)
from ._checks import (
    check_results_directory_populated as check_results_directory_populated,
)
from ._checks import (
    check_verdict_timeline_artifact as check_verdict_timeline_artifact,
)
from ._checks import (
    check_verdicts_csv_health as check_verdicts_csv_health,
)
from ._checks import (
    doctor_exit_code as doctor_exit_code,
)
from ._checks import (
    render_results as render_results,
)
from ._checks import (
    render_results_json as render_results_json,
)
from ._checks import (
    results_payload as results_payload,
)
from ._checks import (
    results_summary as results_summary,
)
from ._checks import (
    run_all_checks as run_all_checks,
)

for _name, _value in vars(_impl).items():
    if not _name.startswith("_") and _name not in globals() and _name != "main":
        globals()[_name] = _value

__all__ = [
    _name
    for _name in globals()
    if not _name.startswith("_") and _name not in {"Sequence", "annotations"}
]
__all__.append("main")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the doctor CLI while preserving package-level monkeypatch hooks."""

    original_run_all_checks = _impl.run_all_checks
    try:
        _impl.run_all_checks = globals()["run_all_checks"]
        return _impl.main(argv)
    finally:
        _impl.run_all_checks = original_run_all_checks
