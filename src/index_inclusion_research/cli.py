from __future__ import annotations

import importlib


def _run_package_main(module_name: str) -> int | None:
    module = importlib.import_module(module_name)
    return module.main()


def run_dashboard_main() -> int | None:
    return _run_package_main("index_inclusion_research.literature_dashboard")


def run_price_pressure_track_main() -> int | None:
    return _run_package_main("index_inclusion_research.price_pressure_track")


def run_demand_curve_track_main() -> int | None:
    return _run_package_main("index_inclusion_research.demand_curve_track")


def run_identification_china_track_main() -> int | None:
    return _run_package_main("index_inclusion_research.identification_china_track")


def run_hs300_rdd_main() -> int | None:
    return _run_package_main("index_inclusion_research.hs300_rdd")


def run_build_event_sample_main() -> int | None:
    return _run_package_main("index_inclusion_research.build_event_sample")


def run_build_price_panel_main() -> int | None:
    return _run_package_main("index_inclusion_research.build_price_panel")


def run_match_controls_main() -> int | None:
    return _run_package_main("index_inclusion_research.match_controls")


def run_match_robustness_main() -> int | None:
    return _run_package_main("index_inclusion_research.match_robustness")


def run_event_study_main() -> int | None:
    return _run_package_main("index_inclusion_research.run_event_study")


def run_regressions_main() -> int | None:
    return _run_package_main("index_inclusion_research.run_regressions")


def run_prepare_hs300_rdd_candidates_main() -> int | None:
    return _run_package_main("index_inclusion_research.prepare_hs300_rdd_candidates")


def run_reconstruct_hs300_rdd_candidates_main() -> int | None:
    return _run_package_main("index_inclusion_research.reconstruct_hs300_rdd_candidates")


def run_plan_hs300_rdd_l3_main() -> int | None:
    return _run_package_main("index_inclusion_research.hs300_rdd_l3_collection")


def run_collect_hs300_rdd_l3_main() -> int | None:
    return _run_package_main("index_inclusion_research.hs300_rdd_online_sources")


def run_prepare_passive_aum_main() -> int | None:
    return _run_package_main("index_inclusion_research.prepare_passive_aum")


def run_download_passive_aum_cn_main() -> int | None:
    return _run_package_main("index_inclusion_research.download_passive_aum_cn")


def run_download_cn_passive_aum_proxy_main() -> int | None:
    return _run_package_main("index_inclusion_research.download_cn_passive_aum_proxy")


def run_compute_h6_weight_change_main() -> int | None:
    return _run_package_main("index_inclusion_research.compute_h6_weight_change")


def run_refresh_real_evidence_main() -> int | None:
    return _run_package_main("index_inclusion_research.real_evidence_refresh")


def run_rebuild_all_main() -> int | None:
    return _run_package_main("index_inclusion_research.rebuild_all")


def run_paper_bundle_main() -> int | None:
    return _run_package_main("index_inclusion_research.paper_bundle")


def run_paper_audit_main() -> int | None:
    return _run_package_main("index_inclusion_research.paper_audit")


def run_verdict_summary_main() -> int | None:
    return _run_package_main("index_inclusion_research.verdict_summary")


def run_pap_diff_main() -> int | None:
    return _run_package_main("index_inclusion_research.pap_diff")


def run_doctor_main() -> int | None:
    return _run_package_main("index_inclusion_research.doctor")


def run_generate_sample_data_main() -> int | None:
    return _run_package_main("index_inclusion_research.sample_data")


def run_download_real_data_main() -> int | None:
    return _run_package_main("index_inclusion_research.real_data")


def run_make_figures_tables_main() -> int | None:
    return _run_package_main("index_inclusion_research.figures_tables")


def run_generate_research_report_main() -> int | None:
    return _run_package_main("index_inclusion_research.research_report")


def run_cma_main() -> int | None:
    return _run_package_main("index_inclusion_research.cross_market_asymmetry")


def run_build_hs300_rdd_forest_main() -> int | None:
    return _run_package_main("index_inclusion_research.build_hs300_rdd_forest")


def run_build_cma_verdicts_forest_main() -> int | None:
    return _run_package_main("index_inclusion_research.build_cma_verdicts_forest")


def run_build_cma_sensitivity_forest_main() -> int | None:
    return _run_package_main(
        "index_inclusion_research.build_cma_sensitivity_forest"
    )


def run_build_cma_ar_engine_forest_main() -> int | None:
    return _run_package_main(
        "index_inclusion_research.build_cma_ar_engine_forest"
    )


def run_build_cma_2d_robustness_heatmap_main() -> int | None:
    return _run_package_main(
        "index_inclusion_research.build_cma_2d_robustness_heatmap"
    )


def run_export_public_summary_main() -> int | None:
    return _run_package_main("index_inclusion_research.export_public_summary")


def run_paper_skeleton_main() -> int | None:
    return _run_package_main("index_inclusion_research.paper_skeleton")
