from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
for path in [ROOT / "src", ROOT / "scripts"]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import generate_research_report as report_script


def _args(**overrides: str) -> argparse.Namespace:
    values = {
        "profile": "auto",
        "event_summary": "",
        "regression_models": "",
        "regression_coefficients": "",
        "output": "",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n", encoding="utf-8")


def test_resolve_cli_args_prefers_real_profile_when_real_outputs_exist(tmp_path: Path) -> None:
    _touch(tmp_path / "results" / "real_event_study" / "event_study_summary.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_models.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_coefficients.csv")

    resolved = report_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "real"
    assert resolved.event_summary == str(tmp_path / "results" / "real_event_study" / "event_study_summary.csv")
    assert resolved.output == str(tmp_path / "results" / "real_tables" / "research_summary.md")


def test_resolve_cli_args_falls_back_to_sample_profile_without_real_outputs(tmp_path: Path) -> None:
    resolved = report_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.event_summary == str(tmp_path / "results" / "event_study" / "event_study_summary.csv")
    assert resolved.output == str(tmp_path / "results" / "tables" / "research_summary.md")


def test_resolve_cli_args_honors_explicit_sample_profile_even_when_real_outputs_exist(tmp_path: Path) -> None:
    _touch(tmp_path / "results" / "real_event_study" / "event_study_summary.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_models.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_coefficients.csv")

    resolved = report_script._resolve_cli_args(_args(profile="sample"), root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.output == str(tmp_path / "results" / "tables" / "research_summary.md")


def test_resolve_cli_args_preserves_explicit_overrides() -> None:
    resolved = report_script._resolve_cli_args(
        _args(
            profile="real",
            event_summary="/tmp/custom-event-summary.csv",
            output="/tmp/custom-summary.md",
        ),
        root=Path("/tmp/project"),
    )

    assert resolved.profile == "real"
    assert resolved.event_summary == "/tmp/custom-event-summary.csv"
    assert resolved.output == "/tmp/custom-summary.md"
    assert resolved.regression_models == "/tmp/project/results/real_regressions/regression_models.csv"
