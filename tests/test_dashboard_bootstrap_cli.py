from __future__ import annotations

import sys
from pathlib import Path
import tomllib

from index_inclusion_research import cli
from index_inclusion_research.dashboard_bootstrap import bootstrap_dashboard_paths
from index_inclusion_research.dashboard_cli import parse_dashboard_args, run_dashboard_app


ROOT = Path(__file__).resolve().parents[1]


def test_bootstrap_dashboard_paths_resolves_repo_layout() -> None:
    current_file = Path("/tmp/example/src/index_inclusion_research/dashboard_app.py")
    paths = bootstrap_dashboard_paths(current_file)
    expected_root = current_file.resolve().parents[2]

    assert paths.root == expected_root
    assert paths.scripts == expected_root / "scripts"
    assert paths.src == expected_root / "src"
    assert paths.templates == expected_root / "scripts" / "templates"
    assert paths.static == expected_root / "scripts" / "static"
    assert str(paths.scripts) in sys.path
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
    assert project["scripts"]["index-inclusion-dashboard"] == "index_inclusion_research.dashboard_app:main"
    assert project["scripts"]["index-inclusion-price-pressure"] == "index_inclusion_research.cli:run_price_pressure_track_main"
    assert project["scripts"]["index-inclusion-demand-curve"] == "index_inclusion_research.cli:run_demand_curve_track_main"
    assert project["scripts"]["index-inclusion-identification"] == "index_inclusion_research.cli:run_identification_china_track_main"
    assert project["scripts"]["index-inclusion-hs300-rdd"] == "index_inclusion_research.cli:run_hs300_rdd_main"
    assert (
        project["scripts"]["index-inclusion-prepare-hs300-rdd"]
        == "index_inclusion_research.cli:run_prepare_hs300_rdd_candidates_main"
    )


def test_track_console_wrappers_delegate_to_expected_script_modules(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "_run_script_main", lambda module_name: calls.append(module_name))

    cli.run_price_pressure_track_main()
    cli.run_demand_curve_track_main()
    cli.run_identification_china_track_main()
    cli.run_hs300_rdd_main()
    cli.run_prepare_hs300_rdd_candidates_main()

    assert calls == [
        "start_price_pressure_track",
        "start_demand_curve_track",
        "start_identification_china_track",
        "start_hs300_rdd",
        "prepare_hs300_rdd_candidates",
    ]
