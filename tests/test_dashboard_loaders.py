from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_loaders


ROOT = Path(__file__).resolve().parents[1]


def test_normalize_result_reads_summary_tables_and_figures(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("hello summary", encoding="utf-8")
    figure_path = tmp_path / "sample_event_timeline.png"
    figure_path.write_bytes(b"fake")
    figure_entry_path = tmp_path / "main_regression_coefficients.png"
    figure_entry_path.write_bytes(b"fake")
    caption_calls: list[dict[str, str | None]] = []

    result = dashboard_loaders.normalize_result(
        {
            "id": "demo",
            "title": "Title",
            "summary_path": summary_path,
            "tables": {"event_study_summary": pd.DataFrame([{"a": 1}])},
            "figures": [
                figure_path,
                {
                    "path": figure_entry_path,
                    "caption": "自定义图注",
                    "prefix": "补充",
                },
            ],
            "output_dir": tmp_path,
        },
        translate_label=lambda label: f"label:{label}",
        render_table=lambda frame: f"rendered:{len(frame)}",
        to_relative=lambda path: path.name,
        build_figure_caption=lambda path, **kwargs: (
            caption_calls.append(
                {
                    "path": path.name,
                    "custom_caption": kwargs.get("custom_caption"),
                    "prefix": kwargs.get("prefix"),
                }
            )
            or f"caption:{path.name}"
        ),
    )

    assert result["summary_text"] == "hello summary"
    assert result["rendered_tables"] == [("label:event_study_summary", "rendered:1")]
    assert result["figure_paths"] == [
        {"path": "sample_event_timeline.png", "caption": "caption:sample_event_timeline.png"},
        {"path": "main_regression_coefficients.png", "caption": "caption:main_regression_coefficients.png"},
    ]
    assert caption_calls == [
        {"path": "sample_event_timeline.png", "custom_caption": None, "prefix": None},
        {"path": "main_regression_coefficients.png", "custom_caption": "自定义图注", "prefix": "补充"},
    ]
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


def test_load_rdd_status_reads_status_csv_audit_and_validation_fields(tmp_path: Path) -> None:
    (tmp_path / "rdd_status.csv").write_text(
        "\n".join(
            [
                "status,evidence_status,message,note,input_file,audit_file,candidate_rows,candidate_batches,treated_rows,control_rows,crossing_batches,validation_error",
                "missing,待补正式样本,真实候选样本文件校验失败：running_variable 缺失,等待修复,data/raw/hs300_rdd_candidates.csv,results/literature/hs300_rdd/candidate_batch_audit.csv,2,1,1,1,1,running_variable 缺失",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    status = dashboard_loaders.load_rdd_status(ROOT, output_dir=tmp_path)

    assert status["mode"] == "missing"
    assert status["audit_file"] == "results/literature/hs300_rdd/candidate_batch_audit.csv"
    assert status["candidate_rows"] == 2
    assert status["candidate_batches"] == 1
    assert status["treated_rows"] == 1
    assert status["control_rows"] == 1
    assert status["crossing_batches"] == 1
    assert status["validation_error"] == "running_variable 缺失"


def test_saved_output_dir_for_analysis_maps_known_modules() -> None:
    assert dashboard_loaders.saved_output_dir_for_analysis(ROOT, "price_pressure_track") == ROOT / "results" / "literature" / "harris_gurel"
    assert dashboard_loaders.saved_output_dir_for_analysis(ROOT, "demand_curve_track") == ROOT / "results" / "literature" / "shleifer"
    assert dashboard_loaders.saved_output_dir_for_analysis(ROOT, "unknown") is None


def test_load_identification_china_saved_result_hides_formal_rdd_outputs_when_status_not_real(tmp_path: Path) -> None:
    style_dir = tmp_path / "results" / "literature" / "hs300_style"
    rdd_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    style_dir.mkdir(parents=True)
    rdd_dir.mkdir(parents=True)
    (style_dir / "summary.md").write_text("style summary", encoding="utf-8")
    (rdd_dir / "summary.md").write_text("rdd summary", encoding="utf-8")
    (style_dir / "sample_event_timeline.png").write_bytes(b"fake")
    (rdd_dir / "car_m1_p1_rdd_main.png").write_bytes(b"fake")

    calls: list[str] = []
    result = dashboard_loaders.load_identification_china_saved_result(
        tmp_path,
        {
            "identification_china_track": {
                "title": "制度识别与中国市场证据",
                "subtitle": "Identification",
                "description_zh": "desc",
                "project_module": "沪深300论文复现",
                "runner": lambda verbose=False: {},
            }
        },
        load_rdd_status=lambda: (
            calls.append("called")
            or {
                "mode": "missing",
                "evidence_status": "待补正式样本",
                "message": "missing",
                "note": "",
                "input_file": "",
                "audit_file": "",
                "candidate_rows": None,
                "candidate_batches": None,
                "treated_rows": None,
                "control_rows": None,
                "crossing_batches": None,
                "validation_error": "",
            }
        ),
        load_saved_tables=lambda output_dir: [("表格", output_dir.name)],
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
        build_figure_caption=lambda path, custom_caption=None, prefix=None: f"{prefix or 'none'}:{path.name}",
    )

    assert calls == ["called"]
    assert result["rendered_tables"] == [("风格识别：表格", "hs300_style")]
    assert result["figure_paths"] == [
        {
            "path": "results/literature/hs300_style/sample_event_timeline.png",
            "caption": "风格识别:sample_event_timeline.png",
        }
    ]


def test_load_identification_china_saved_result_keeps_formal_rdd_outputs_when_status_real(tmp_path: Path) -> None:
    style_dir = tmp_path / "results" / "literature" / "hs300_style"
    rdd_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    style_dir.mkdir(parents=True)
    rdd_dir.mkdir(parents=True)
    (style_dir / "summary.md").write_text("style summary", encoding="utf-8")
    (rdd_dir / "summary.md").write_text("rdd summary", encoding="utf-8")
    (style_dir / "sample_event_timeline.png").write_bytes(b"fake")
    (rdd_dir / "car_m1_p1_rdd_main.png").write_bytes(b"fake")

    calls: list[str] = []
    result = dashboard_loaders.load_identification_china_saved_result(
        tmp_path,
        {
            "identification_china_track": {
                "title": "制度识别与中国市场证据",
                "subtitle": "Identification",
                "description_zh": "desc",
                "project_module": "沪深300论文复现",
                "runner": lambda verbose=False: {},
            }
        },
        load_rdd_status=lambda: (
            calls.append("called")
            or {
                "mode": "real",
                "evidence_status": "正式边界样本",
                "message": "real",
                "note": "",
                "input_file": "",
                "audit_file": "",
                "candidate_rows": None,
                "candidate_batches": None,
                "treated_rows": None,
                "control_rows": None,
                "crossing_batches": None,
                "validation_error": "",
            }
        ),
        load_saved_tables=lambda output_dir: [("表格", output_dir.name)],
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
        build_figure_caption=lambda path, custom_caption=None, prefix=None: f"{prefix or 'none'}:{path.name}",
    )

    assert calls == ["called"]
    assert result["rendered_tables"] == [
        ("风格识别：表格", "hs300_style"),
        ("断点回归：表格", "hs300_rdd"),
    ]
    assert result["figure_paths"] == [
        {
            "path": "results/literature/hs300_style/sample_event_timeline.png",
            "caption": "风格识别:sample_event_timeline.png",
        },
        {
            "path": "results/literature/hs300_rdd/car_m1_p1_rdd_main.png",
            "caption": "断点回归:car_m1_p1_rdd_main.png",
        },
    ]
