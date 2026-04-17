from __future__ import annotations

import time

from flask import request, url_for

from index_inclusion_research.dashboard_application import build_dashboard_application
from index_inclusion_research.dashboard_bootstrap import bootstrap_dashboard_paths
from index_inclusion_research import dashboard_cli

PATHS = bootstrap_dashboard_paths(__file__)
ROOT = PATHS.root

from start_demand_curve_track import run_analysis as run_demand_curve_track
from start_identification_china_track import run_analysis as run_identification_china_track
from start_price_pressure_track import run_analysis as run_price_pressure_track
from index_inclusion_research.literature_catalog import get_literature_paper

dashboard_application = build_dashboard_application(
    import_name=__name__,
    root=ROOT,
    template_folder=str(PATHS.templates),
    static_folder=str(PATHS.static),
    run_price_pressure_track=run_price_pressure_track,
    run_demand_curve_track=run_demand_curve_track,
    run_identification_china_track=run_identification_china_track,
    request_proxy=request,
    url_builder=url_for,
    time_module=time,
    get_literature_paper=get_literature_paper,
)
shell = dashboard_application.shell
services = dashboard_application.services
route_views = dashboard_application.route_views
ANALYSES = shell.analyses
LIBRARY_CARD = shell.library_card
REVIEW_CARD = shell.review_card
FRAMEWORK_CARD = shell.framework_card
SUPPLEMENT_CARD = shell.supplement_card
PROJECT_MODULE_DISPLAY = shell.project_module_display
DETAILS_QUERY_PARAM = shell.details_query_param
DETAILS_PANEL_KEYS = shell.details_panel_keys
runtime = shell.runtime

RUN_CACHE = runtime.run_cache
refresh_coordinator = shell.refresh_coordinator

REFRESH_LOCK = refresh_coordinator.lock
REFRESH_STATE = refresh_coordinator.state

TABLE_LABELS = runtime.table_labels
COLUMN_LABELS = runtime.column_labels
VALUE_LABELS = runtime.value_labels

app = shell.app
parse_dashboard_args = dashboard_cli.parse_dashboard_args


def main(argv: list[str] | None = None) -> None:
    dashboard_cli.run_dashboard_app(app, argv)


if __name__ == "__main__":
    main()
