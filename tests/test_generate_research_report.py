from __future__ import annotations

import argparse
from pathlib import Path

from index_inclusion_research import research_report as report_script


def _args(**overrides: str) -> argparse.Namespace:
    values = {
        "profile": "auto",
        "event_summary": "",
        "regression_models": "",
        "regression_coefficients": "",
        "results_manifest": "",
        "rdd_output_dir": "",
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
    assert resolved.results_manifest == str(tmp_path / "results" / "real_tables" / "results_manifest.csv")
    assert resolved.rdd_output_dir == str(tmp_path / "results" / "literature" / "hs300_rdd")


def test_resolve_cli_args_falls_back_to_sample_profile_without_real_outputs(tmp_path: Path) -> None:
    resolved = report_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.event_summary == str(tmp_path / "results" / "event_study" / "event_study_summary.csv")
    assert resolved.output == str(tmp_path / "results" / "tables" / "research_summary.md")
    assert resolved.results_manifest == str(tmp_path / "results" / "tables" / "results_manifest.csv")


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
    assert resolved.results_manifest == "/tmp/project/results/real_tables/results_manifest.csv"


def test_build_report_text_includes_shared_identification_contract() -> None:
    event_summary = report_script.pd.DataFrame(
        [
            {"market": "CN", "event_phase": "announce", "window_slug": "m1_p1", "inclusion": 1, "mean_car": 0.01, "t_stat": 2.1, "p_value": 0.04},
        ]
    )
    regression_models = report_script.pd.DataFrame()
    regression_coefficients = report_script.pd.DataFrame()

    text = report_script.build_report_text(
        event_summary,
        regression_models,
        regression_coefficients,
        results_manifest={
            "rdd_evidence_tier": "L2",
            "rdd_evidence_status": "公开重建样本",
            "rdd_source_label": "公开重建候选样本文件",
            "rdd_mode": "reconstructed",
            "rdd_coverage_note": "311 条候选；6 个批次；调入 6 / 对照 305。",
        },
    )

    assert "## 三、识别与证据状态" in text
    assert "`L2`" in text
    assert "`公开重建样本`" in text
    assert "`公开重建候选样本文件`" in text
