from __future__ import annotations

import argparse
from pathlib import Path

from index_inclusion_research import workflow_profiles


def _args(**overrides: str) -> argparse.Namespace:
    values = {
        "profile": "auto",
        "input": "",
        "output": "",
        "events": "",
        "prices": "",
        "benchmarks": "",
        "panel": "",
        "output_events": "",
        "output_diagnostics": "",
        "output_dir": "",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n", encoding="utf-8")


def test_detect_profile_prefers_real_when_stage_markers_exist(tmp_path: Path) -> None:
    _touch(tmp_path / "data" / "processed" / "real_matched_event_panel.csv")

    assert workflow_profiles.detect_profile("run_regressions", root=tmp_path) == "real"


def test_detect_profile_falls_back_to_sample_without_real_markers(tmp_path: Path) -> None:
    assert workflow_profiles.detect_profile("run_event_study", root=tmp_path) == "sample"


def test_resolve_profile_args_sets_real_defaults_for_matching_stage(tmp_path: Path) -> None:
    _touch(tmp_path / "data" / "processed" / "real_events_clean.csv")
    _touch(tmp_path / "data" / "raw" / "real_prices.csv")

    resolved = workflow_profiles.resolve_profile_args(_args(), workflow="match_controls", root=tmp_path)

    assert resolved.profile == "real"
    assert resolved.events == str(tmp_path / "data" / "processed" / "real_events_clean.csv")
    assert resolved.prices == str(tmp_path / "data" / "raw" / "real_prices.csv")
    assert resolved.output_events == str(tmp_path / "data" / "processed" / "real_matched_events.csv")
    assert resolved.output_diagnostics == str(tmp_path / "results" / "real_regressions" / "match_diagnostics.csv")


def test_resolve_profile_args_sets_sample_defaults_for_price_panel_without_real_inputs(tmp_path: Path) -> None:
    resolved = workflow_profiles.resolve_profile_args(_args(), workflow="build_price_panel", root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.events == str(tmp_path / "data" / "processed" / "events_clean.csv")
    assert resolved.prices == str(tmp_path / "data" / "raw" / "sample_prices.csv")
    assert resolved.output == str(tmp_path / "data" / "processed" / "event_panel.csv")


def test_resolve_profile_args_honors_explicit_sample_profile(tmp_path: Path) -> None:
    _touch(tmp_path / "data" / "processed" / "real_event_panel.csv")

    resolved = workflow_profiles.resolve_profile_args(
        _args(profile="sample"),
        workflow="run_event_study",
        root=tmp_path,
    )

    assert resolved.profile == "sample"
    assert resolved.panel == str(tmp_path / "data" / "processed" / "event_panel.csv")
    assert resolved.output_dir == str(tmp_path / "results" / "event_study")


def test_resolve_profile_args_preserves_explicit_overrides() -> None:
    resolved = workflow_profiles.resolve_profile_args(
        _args(profile="real", panel="/tmp/custom-panel.csv", output_dir="/tmp/custom-output"),
        workflow="run_event_study",
        root=Path("/tmp/project"),
    )

    assert resolved.profile == "real"
    assert resolved.panel == "/tmp/custom-panel.csv"
    assert resolved.output_dir == "/tmp/custom-output"
