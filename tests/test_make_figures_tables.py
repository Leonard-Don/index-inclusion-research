from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from index_inclusion_research import figures_tables as export_script


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
        "rdd_output_dir": "",
        "long_window_output_dir": "",
        "figures_dir": "",
        "tables_dir": "",
        "results_manifest": "",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n", encoding="utf-8")


def _write_sensitivity_cache(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "hid,name_cn,verdict,confidence,evidence_tier,n_obs\n"
        "H1,信息泄露与预运行,证据不足,中,core,436\n"
        "H2,被动基金 AUM 差异,部分支持,中,core,17\n"
        "H3,散户 vs 机构结构,支持,高,supplementary,4\n"
        "H4,卖空约束,证据不足,中,supplementary,40\n"
        "H5,涨跌停限制,支持,高,core,936\n"
        "H6,指数权重可预测性,证据不足,中,supplementary,67\n"
        "H7,行业结构差异,支持,中,core,187\n",
        encoding="utf-8",
    )


def test_resolve_cli_args_prefers_real_profile_when_real_workflow_exists(tmp_path: Path) -> None:
    _touch(tmp_path / "data" / "processed" / "real_events_clean.csv")
    _touch(tmp_path / "data" / "processed" / "real_event_panel.csv")
    _touch(tmp_path / "results" / "real_event_study" / "event_study_summary.csv")
    _touch(tmp_path / "results" / "real_regressions" / "regression_coefficients.csv")

    resolved = export_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "real"
    assert resolved.events == str(tmp_path / "data" / "processed" / "real_events_clean.csv")
    assert resolved.rdd_summary == str(tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_summary.csv")
    assert resolved.rdd_output_dir == str(tmp_path / "results" / "literature" / "hs300_rdd")
    assert resolved.figures_dir == str(tmp_path / "results" / "real_figures")
    assert resolved.tables_dir == str(tmp_path / "results" / "real_tables")
    assert resolved.results_manifest == str(tmp_path / "results" / "real_tables" / "results_manifest.csv")


def test_resolve_cli_args_falls_back_to_sample_profile_without_real_markers(tmp_path: Path) -> None:
    resolved = export_script._resolve_cli_args(_args(), root=tmp_path)

    assert resolved.profile == "sample"
    assert resolved.events == str(tmp_path / "data" / "processed" / "events_clean.csv")
    assert resolved.event_summary == str(tmp_path / "results" / "event_study" / "event_study_summary.csv")
    assert resolved.rdd_summary == ""
    assert resolved.rdd_output_dir == str(tmp_path / "results" / "literature" / "hs300_rdd")
    assert resolved.tables_dir == str(tmp_path / "results" / "tables")
    assert resolved.results_manifest == str(tmp_path / "results" / "tables" / "results_manifest.csv")


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


def test_cma_sensitivity_forest_rerender_is_cache_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sens_root = tmp_path / "results" / "sensitivity"
    _write_sensitivity_cache(
        sens_root / "threshold_0_10" / "cma_hypothesis_verdicts.csv"
    )
    figures_dir = tmp_path / "results" / "figures"
    calls: list[dict[str, object]] = []

    def _cache_only_renderer(**kwargs: object) -> Path:
        calls.append(kwargs)
        png_path = Path(kwargs["output_png_path"])  # type: ignore[arg-type]
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_bytes(b"png")
        pdf_path = Path(kwargs["output_pdf_path"])  # type: ignore[arg-type]
        pdf_path.write_bytes(b"%PDF")
        return png_path

    monkeypatch.setattr(
        export_script,
        "build_cma_sensitivity_forest_plot_from_cache",
        _cache_only_renderer,
    )

    export_script._maybe_build_cma_sensitivity_forest(
        figures_dir=figures_dir,
        sensitivity_root=sens_root,
    )

    assert calls
    assert calls[0]["sensitivity_root"] == sens_root
