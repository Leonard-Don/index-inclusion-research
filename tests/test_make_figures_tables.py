from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
for path in [ROOT / "src", ROOT / "scripts"]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import make_figures_tables as export_script


def _args(**overrides: str) -> argparse.Namespace:
    values = {
        "profile": "auto",
        "events": "",
        "panel": "",
        "prices": "",
        "benchmarks": "",
        "metadata": "",
        "matched_panel": "",
        "average_paths": "",
        "event_summary": "",
        "regression_coefs": "",
        "regression_models": "",
        "rdd_summary": "",
        "rdd_summary_note": "",
        "long_window_output_dir": "",
        "figures_dir": "",
        "tables_dir": "",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n", encoding="utf-8")


def test_resolve_cli_args_prefers_real_profile_when_real_workflow_exists(tmp_path: Path) -> None:
    _touch(tmp_path / "data" / "processed" / "real_events_clean.csv")
    _touch(tmp_path / "data" / "processed" / "real_event_panel.csv")
    _touch(tmp_path / "results" / "real_event_study" / "event_study_summary.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_coefficients.csv")

    resolved = export_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "real"
    assert resolved.events == str(tmp_path / "data" / "processed" / "real_events_clean.csv")
    assert resolved.rdd_summary == str(tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_summary.csv")
    assert resolved.rdd_summary_note == str(tmp_path / "results" / "literature" / "hs300_rdd" / "summary.md")
    assert resolved.figures_dir == str(tmp_path / "results" / "real_figures")
    assert resolved.tables_dir == str(tmp_path / "results" / "real_tables")


def test_resolve_cli_args_falls_back_to_sample_profile_without_real_markers(tmp_path: Path) -> None:
    resolved = export_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.events == str(tmp_path / "data" / "processed" / "events_clean.csv")
    assert resolved.event_summary == str(tmp_path / "results" / "event_study" / "event_study_summary.csv")
    assert resolved.rdd_summary == ""
    assert resolved.rdd_summary_note == ""
    assert resolved.tables_dir == str(tmp_path / "results" / "tables")


def test_resolve_cli_args_honors_explicit_sample_profile_even_when_real_exists(tmp_path: Path) -> None:
    _touch(tmp_path / "data" / "processed" / "real_events_clean.csv")
    _touch(tmp_path / "data" / "processed" / "real_event_panel.csv")
    _touch(tmp_path / "results" / "real_event_study" / "event_study_summary.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_coefficients.csv")

    resolved = export_script._resolve_cli_args(_args(profile="sample"), root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.events == str(tmp_path / "data" / "processed" / "events_clean.csv")
    assert resolved.tables_dir == str(tmp_path / "results" / "tables")


def test_resolve_cli_args_preserves_explicit_overrides() -> None:
    resolved = export_script._resolve_cli_args(
        _args(
            profile="real",
            events="/tmp/custom-events.csv",
            tables_dir="/tmp/custom-tables",
        ),
        root=Path("/tmp/project"),
    )

    assert resolved.profile == "real"
    assert resolved.events == "/tmp/custom-events.csv"
    assert resolved.tables_dir == "/tmp/custom-tables"
    assert resolved.figures_dir == "/tmp/project/results/real_figures"
