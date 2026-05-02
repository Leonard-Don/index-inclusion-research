from __future__ import annotations

import sys
import tomllib
from pathlib import Path

from index_inclusion_research import cli
from index_inclusion_research.dashboard_bootstrap import bootstrap_dashboard_paths
from index_inclusion_research.literature_dashboard import (
    parse_dashboard_args,
    run_dashboard_app,
)

ROOT = Path(__file__).resolve().parents[1]


def test_bootstrap_dashboard_paths_resolves_repo_layout() -> None:
    current_file = Path("/tmp/example/src/index_inclusion_research/dashboard_app.py")
    paths = bootstrap_dashboard_paths(current_file)
    expected_root = current_file.resolve().parents[2]
    expected_web = expected_root / "src" / "index_inclusion_research" / "web"

    assert paths.root == expected_root
    assert paths.src == expected_root / "src"
    assert paths.templates == expected_web / "templates"
    assert paths.static == expected_web / "static"
    assert str(paths.src) in sys.path


def test_parse_dashboard_args_accepts_host_and_port() -> None:
    args = parse_dashboard_args(["--host", "0.0.0.0", "--port", "5012"])

    assert args.host == "0.0.0.0"
    assert args.port == 5012


def test_run_dashboard_app_prints_localhost_tip_and_forwards_host_port(capsys) -> None:
    calls: list[dict[str, object]] = []

    class DummyApp:
        def run(self, **kwargs) -> None:
            calls.append(kwargs)

    run_dashboard_app(DummyApp(), ["--host", "127.0.0.1", "--port", "5013"])

    captured = capsys.readouterr().out
    assert "http://localhost:5013" in captured
    assert "Firefox 对 localhost 的兼容性更稳定" in captured
    assert calls == [{"host": "127.0.0.1", "port": 5013, "debug": False}]


def test_project_metadata_declares_flask_and_console_scripts() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]

    assert "flask>=3.0" in project["dependencies"]
    assert "yfinance>=0.2" in project["dependencies"]
    assert project["scripts"]["index-inclusion-dashboard"] == "index_inclusion_research.cli:run_dashboard_main"
    assert project["scripts"]["index-inclusion-build-event-sample"] == "index_inclusion_research.cli:run_build_event_sample_main"
    assert project["scripts"]["index-inclusion-build-price-panel"] == "index_inclusion_research.cli:run_build_price_panel_main"
    assert project["scripts"]["index-inclusion-match-controls"] == "index_inclusion_research.cli:run_match_controls_main"
    assert project["scripts"]["index-inclusion-run-event-study"] == "index_inclusion_research.cli:run_event_study_main"
    assert project["scripts"]["index-inclusion-run-regressions"] == "index_inclusion_research.cli:run_regressions_main"
    assert project["scripts"]["index-inclusion-price-pressure"] == "index_inclusion_research.cli:run_price_pressure_track_main"
    assert project["scripts"]["index-inclusion-demand-curve"] == "index_inclusion_research.cli:run_demand_curve_track_main"
    assert project["scripts"]["index-inclusion-identification"] == "index_inclusion_research.cli:run_identification_china_track_main"
    assert project["scripts"]["index-inclusion-hs300-rdd"] == "index_inclusion_research.cli:run_hs300_rdd_main"
    assert (
        project["scripts"]["index-inclusion-prepare-hs300-rdd"]
        == "index_inclusion_research.cli:run_prepare_hs300_rdd_candidates_main"
    )
    assert (
        project["scripts"]["index-inclusion-reconstruct-hs300-rdd"]
        == "index_inclusion_research.cli:run_reconstruct_hs300_rdd_candidates_main"
    )
    assert (
        project["scripts"]["index-inclusion-generate-sample-data"]
        == "index_inclusion_research.cli:run_generate_sample_data_main"
    )
    assert (
        project["scripts"]["index-inclusion-download-real-data"]
        == "index_inclusion_research.cli:run_download_real_data_main"
    )
    assert (
        project["scripts"]["index-inclusion-make-figures-tables"]
        == "index_inclusion_research.cli:run_make_figures_tables_main"
    )
    assert (
        project["scripts"]["index-inclusion-generate-research-report"]
        == "index_inclusion_research.cli:run_generate_research_report_main"
    )


def test_track_console_wrappers_delegate_to_expected_package_modules(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "_run_package_main", lambda module_name: calls.append(module_name))

    cli.run_build_event_sample_main()
    cli.run_build_price_panel_main()
    cli.run_match_controls_main()
    cli.run_event_study_main()
    cli.run_regressions_main()
    cli.run_prepare_hs300_rdd_candidates_main()
    cli.run_reconstruct_hs300_rdd_candidates_main()

    assert calls == [
        "index_inclusion_research.build_event_sample",
        "index_inclusion_research.build_price_panel",
        "index_inclusion_research.match_controls",
        "index_inclusion_research.run_event_study",
        "index_inclusion_research.run_regressions",
        "index_inclusion_research.prepare_hs300_rdd_candidates",
        "index_inclusion_research.reconstruct_hs300_rdd_candidates",
    ]


def test_package_console_wrappers_delegate_to_expected_package_modules(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "_run_package_main", lambda module_name: calls.append(module_name))

    cli.run_dashboard_main()
    cli.run_price_pressure_track_main()
    cli.run_demand_curve_track_main()
    cli.run_identification_china_track_main()
    cli.run_hs300_rdd_main()
    cli.run_generate_sample_data_main()
    cli.run_download_real_data_main()
    cli.run_make_figures_tables_main()
    cli.run_generate_research_report_main()

    assert calls == [
        "index_inclusion_research.literature_dashboard",
        "index_inclusion_research.price_pressure_track",
        "index_inclusion_research.demand_curve_track",
        "index_inclusion_research.identification_china_track",
        "index_inclusion_research.hs300_rdd",
        "index_inclusion_research.sample_data",
        "index_inclusion_research.real_data",
        "index_inclusion_research.figures_tables",
        "index_inclusion_research.research_report",
    ]


def test_console_wrapper_propagates_package_main_return_code(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_run_package_main", lambda module_name: 7)

    assert cli.run_doctor_main() == 7


def test_python_module_fallback_targets_are_directly_runnable() -> None:
    module_files = [
        "literature_dashboard.py",
        "price_pressure_track.py",
        "demand_curve_track.py",
        "identification_china_track.py",
        "hs300_rdd.py",
        "build_event_sample.py",
        "build_price_panel.py",
        "match_controls.py",
        "run_event_study.py",
        "run_regressions.py",
        "prepare_hs300_rdd_candidates.py",
        "reconstruct_hs300_rdd_candidates.py",
        "sample_data.py",
        "real_data.py",
        "enrich_cn_sectors.py",
        "figures_tables.py",
        "research_report.py",
    ]

    for module_file in module_files:
        source = (ROOT / "src" / "index_inclusion_research" / module_file).read_text(encoding="utf-8")
        assert 'if __name__ == "__main__":' in source
