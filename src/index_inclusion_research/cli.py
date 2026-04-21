from __future__ import annotations

import importlib

def _run_package_main(module_name: str) -> None:
    module = importlib.import_module(module_name)
    module.main()


def run_dashboard_main() -> None:
    _run_package_main("index_inclusion_research.literature_dashboard")


def run_price_pressure_track_main() -> None:
    _run_package_main("index_inclusion_research.price_pressure_track")


def run_demand_curve_track_main() -> None:
    _run_package_main("index_inclusion_research.demand_curve_track")


def run_identification_china_track_main() -> None:
    _run_package_main("index_inclusion_research.identification_china_track")


def run_hs300_rdd_main() -> None:
    _run_package_main("index_inclusion_research.hs300_rdd")


def run_build_event_sample_main() -> None:
    _run_package_main("index_inclusion_research.build_event_sample")


def run_build_price_panel_main() -> None:
    _run_package_main("index_inclusion_research.build_price_panel")


def run_match_controls_main() -> None:
    _run_package_main("index_inclusion_research.match_controls")


def run_event_study_main() -> None:
    _run_package_main("index_inclusion_research.run_event_study")


def run_regressions_main() -> None:
    _run_package_main("index_inclusion_research.run_regressions")


def run_prepare_hs300_rdd_candidates_main() -> None:
    _run_package_main("index_inclusion_research.prepare_hs300_rdd_candidates")


def run_reconstruct_hs300_rdd_candidates_main() -> None:
    _run_package_main("index_inclusion_research.reconstruct_hs300_rdd_candidates")


def run_generate_sample_data_main() -> None:
    _run_package_main("index_inclusion_research.sample_data")


def run_download_real_data_main() -> None:
    _run_package_main("index_inclusion_research.real_data")


def run_make_figures_tables_main() -> None:
    _run_package_main("index_inclusion_research.figures_tables")


def run_generate_research_report_main() -> None:
    _run_package_main("index_inclusion_research.research_report")
