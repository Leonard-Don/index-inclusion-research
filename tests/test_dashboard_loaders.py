from __future__ import annotations

from pathlib import Path

from index_inclusion_research import dashboard_loaders


ROOT = Path(__file__).resolve().parents[1]


def test_normalize_result_reads_summary_tables_and_figures(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("hello summary", encoding="utf-8")
    figure_path = tmp_path / "sample_event_timeline.png"
    figure_path.write_bytes(b"fake")

    result = dashboard_loaders.normalize_result(
        {
            "id": "demo",
            "title": "Title",
            "summary_path": summary_path,
            "tables": {"event_study_summary": [{"a": 1}]},
            "figures": [figure_path],
            "output_dir": tmp_path,
        },
        translate_label=lambda label: f"label:{label}",
        render_table=lambda frame: f"rendered:{len(frame)}",
        to_relative=lambda path: path.name,
        build_figure_caption=lambda path, **kwargs: f"caption:{path.name}",
    )

    assert result["summary_text"] == "hello summary"
    assert result["rendered_tables"] == [("label:event_study_summary", "rendered:1")]
    assert result["figure_paths"] == [{"path": "sample_event_timeline.png", "caption": "caption:sample_event_timeline.png"}]
    assert result["output_dir"] == tmp_path.name


def test_load_saved_tables_prefers_known_order_and_skips_rdd_status(tmp_path: Path) -> None:
    (tmp_path / "z_other.csv").write_text("x\n1\n", encoding="utf-8")
    (tmp_path / "rdd_status.csv").write_text("status\nmissing\n", encoding="utf-8")
    (tmp_path / "regression_coefficients.csv").write_text("x\n1\n", encoding="utf-8")
    (tmp_path / "event_study_summary.csv").write_text("x\n1\n", encoding="utf-8")

    tables = dashboard_loaders.load_saved_tables(
        tmp_path,
        translate_label=lambda label: label.upper(),
        render_table=lambda frame: f"rows:{len(frame)}",
    )

    assert [label for label, _ in tables] == ["EVENT_STUDY_SUMMARY", "REGRESSION_COEFFICIENTS", "Z_OTHER"]


def test_load_rdd_status_detects_demo_summary(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("这是 demo 伪排名数据，用于显式 `--demo` 模式。", encoding="utf-8")

    status = dashboard_loaders.load_rdd_status(ROOT, output_dir=tmp_path)

    assert status["mode"] == "demo"
    assert status["evidence_status"] == "方法展示"


def test_saved_output_dir_for_analysis_maps_known_modules() -> None:
    assert dashboard_loaders.saved_output_dir_for_analysis(ROOT, "price_pressure_track") == ROOT / "results" / "literature" / "harris_gurel"
    assert dashboard_loaders.saved_output_dir_for_analysis(ROOT, "demand_curve_track") == ROOT / "results" / "literature" / "shleifer"
    assert dashboard_loaders.saved_output_dir_for_analysis(ROOT, "unknown") is None
