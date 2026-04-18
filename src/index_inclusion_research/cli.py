from __future__ import annotations

import importlib

from index_inclusion_research.dashboard_bootstrap import bootstrap_dashboard_paths


def _bootstrap_repo_paths() -> None:
    bootstrap_dashboard_paths(__file__)


def _run_script_main(module_name: str) -> None:
    _bootstrap_repo_paths()
    module = importlib.import_module(module_name)
    module.main()


def run_price_pressure_track_main() -> None:
    _run_script_main("start_price_pressure_track")


def run_demand_curve_track_main() -> None:
    _run_script_main("start_demand_curve_track")


def run_identification_china_track_main() -> None:
    _run_script_main("start_identification_china_track")


def run_hs300_rdd_main() -> None:
    _run_script_main("start_hs300_rdd")


def run_prepare_hs300_rdd_candidates_main() -> None:
    _run_script_main("prepare_hs300_rdd_candidates")
