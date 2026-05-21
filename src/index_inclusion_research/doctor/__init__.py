"""Project health-check CLI and reusable doctor checks."""

from __future__ import annotations

from collections.abc import Sequence

from . import (
    _artifacts,
    _common,
    _misc,
    _paper,
    _readiness,
    _runner,
    _verdicts,
)
from ._artifacts import (
    check_citation_graph_artifact as check_citation_graph_artifact,
)
from ._artifacts import (
    check_cma_2d_robustness_heatmap_artifact as check_cma_2d_robustness_heatmap_artifact,
)
from ._artifacts import (
    check_cma_ar_engine_forest_artifact as check_cma_ar_engine_forest_artifact,
)
from ._artifacts import (
    check_cma_sensitivity_forest_artifact as check_cma_sensitivity_forest_artifact,
)
from ._artifacts import (
    check_cma_verdicts_forest_artifact as check_cma_verdicts_forest_artifact,
)
from ._artifacts import (
    check_hs300_rdd_forest_artifact as check_hs300_rdd_forest_artifact,
)
from ._artifacts import (
    check_literature_timeline_artifact as check_literature_timeline_artifact,
)
from ._artifacts import (
    check_verdict_timeline_artifact as check_verdict_timeline_artifact,
)
from ._common import (
    DEFAULT_CITATION_CENTRALITY_CSV as DEFAULT_CITATION_CENTRALITY_CSV,
)
from ._common import (
    DEFAULT_VERDICT_TIMELINE_SOURCE_CSV as DEFAULT_VERDICT_TIMELINE_SOURCE_CSV,
)
from ._common import (
    DEFAULT_VERDICTS_CSV as DEFAULT_VERDICTS_CSV,
)
from ._common import (
    ROOT as ROOT,
)
from ._common import (
    CheckResult as CheckResult,
)
from ._misc import (
    check_chart_builders_register as check_chart_builders_register,
)
from ._misc import (
    check_console_scripts_importable as check_console_scripts_importable,
)
from ._misc import (
    check_heuristic_citation_centrality_schema as check_heuristic_citation_centrality_schema,
)
from ._misc import (
    check_results_directory_populated as check_results_directory_populated,
)
from ._paper import (
    check_methodology_summary_freshness as check_methodology_summary_freshness,
)
from ._paper import (
    check_paper_audit as check_paper_audit,
)
from ._paper import (
    check_paper_integrity as check_paper_integrity,
)
from ._paper import (
    check_paper_skeleton_freshness as check_paper_skeleton_freshness,
)
from ._paper import (
    check_public_summary_freshness as check_public_summary_freshness,
)
from ._readiness import (
    check_h6_weight_change_readiness as check_h6_weight_change_readiness,
)
from ._readiness import (
    check_h7_cn_sector_readiness as check_h7_cn_sector_readiness,
)
from ._readiness import (
    check_match_robustness_grid as check_match_robustness_grid,
)
from ._readiness import (
    check_matched_sample_balance as check_matched_sample_balance,
)
from ._readiness import (
    check_rdd_l3_sample_readiness as check_rdd_l3_sample_readiness,
)
from ._readiness import (
    check_rdd_robustness_panel as check_rdd_robustness_panel,
)
from ._runner import (
    DEFAULT_CHECKS as DEFAULT_CHECKS,
)
from ._runner import (
    doctor_exit_code as doctor_exit_code,
)
from ._runner import (
    render_results as render_results,
)
from ._runner import (
    render_results_json as render_results_json,
)
from ._runner import (
    results_payload as results_payload,
)
from ._runner import (
    results_summary as results_summary,
)
from ._runner import (
    run_all_checks as run_all_checks,
)
from ._verdicts import (
    check_hypothesis_paper_ids_resolve as check_hypothesis_paper_ids_resolve,
)
from ._verdicts import (
    check_p_gated_verdict_sensitivity as check_p_gated_verdict_sensitivity,
)
from ._verdicts import (
    check_pap_deviation_no_flips as check_pap_deviation_no_flips,
)
from ._verdicts import (
    check_pap_snapshot_freshness as check_pap_snapshot_freshness,
)
from ._verdicts import (
    check_paper_verdict_section_synced as check_paper_verdict_section_synced,
)
from ._verdicts import (
    check_pending_data_verdicts as check_pending_data_verdicts,
)
from ._verdicts import (
    check_verdicts_csv_health as check_verdicts_csv_health,
)

# Re-export every remaining public name from the themed submodules so
# external callers keep importing from ``index_inclusion_research.doctor``
# unchanged.
for _module in (_common, _verdicts, _readiness, _artifacts, _paper, _misc, _runner):
    for _name, _value in vars(_module).items():
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

    original_run_all_checks = _runner.run_all_checks
    try:
        _runner.run_all_checks = globals()["run_all_checks"]
        return _runner.main(argv)
    finally:
        _runner.run_all_checks = original_run_all_checks
