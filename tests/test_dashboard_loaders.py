from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_loaders
from index_inclusion_research import dashboard_media
from index_inclusion_research.result_contract import build_results_manifest


ROOT = Path(__file__).resolve().parents[1]
TINY_PNG = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x02\x00\x00\x00\x01\x08\x02\x00\x00\x00"
    b"\x7b@\xe8\xdd"
    b"\x00\x00\x00\rIDATx\x9cc`\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00"
    b"\xc9\xfe\x92\xef"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


def test_normalize_result_reads_summary_tables_and_figures(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("hello summary", encoding="utf-8")
    figure_path = tmp_path / "sample_event_timeline.png"
    figure_path.write_bytes(TINY_PNG)
    figure_entry_path = tmp_path / "main_regression_coefficients.png"
    figure_entry_path.write_bytes(TINY_PNG)
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
        {
            "path": "sample_event_timeline.png",
            "caption": "caption:sample_event_timeline.png",
            "caption_lead": "caption:sample_event_timeline.png",
            "caption_focus": "",
            "width": 2,
            "height": 1,
        },
        {
            "path": "main_regression_coefficients.png",
            "caption": "caption:main_regression_coefficients.png",
            "caption_lead": "caption:main_regression_coefficients.png",
            "caption_focus": "",
            "width": 2,
            "height": 1,
        },
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
    assert status["evidence_tier"] == "L1"
    assert status["evidence_status"] == "方法展示"
    assert status["source_kind"] == "demo"
    assert status["source_label"] == "demo 伪排名样本"


def test_load_rdd_status_detects_reconstructed_summary(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("当前正在使用公开数据重建的候选样本文件：`hs300_rdd_candidates.reconstructed.csv`。", encoding="utf-8")

    status = dashboard_loaders.load_rdd_status(ROOT, output_dir=tmp_path)

    assert status["mode"] == "reconstructed"
    assert status["evidence_tier"] == "L2"
    assert status["evidence_status"] == "公开重建样本"
    assert status["source_kind"] == "reconstructed"
    assert status["input_file"] == "data/raw/hs300_rdd_candidates.reconstructed.csv"
    assert status["source_file"] == "data/raw/hs300_rdd_candidates.reconstructed.csv"


def test_load_rdd_status_defaults_missing_state_to_dual_input_paths(tmp_path: Path) -> None:
    status = dashboard_loaders.load_rdd_status(ROOT, output_dir=tmp_path)

    assert status["mode"] == "missing"
    assert status["evidence_tier"] == "L0"
    assert status["source_kind"] == "missing"
    assert status["source_label"] == "待补候选样本"
    assert "hs300_rdd_candidates.csv" in status["message"]
    assert "hs300_rdd_candidates.reconstructed.csv" in status["message"]
    assert "L2/L3" in status["note"]


def test_load_rdd_status_reads_status_csv_audit_and_validation_fields(tmp_path: Path) -> None:
    (tmp_path / "rdd_status.csv").write_text(
        "\n".join(
            [
                "status,evidence_tier,evidence_status,source_kind,source_label,source_file,generated_at,as_of_date,batch_label,coverage_note,message,note,input_file,audit_file,candidate_rows,candidate_batches,treated_rows,control_rows,crossing_batches,validation_error",
                "missing,L0,待补正式样本,missing,待补候选样本,data/raw/hs300_rdd_candidates.csv,2026-04-18T10:00:00+08:00,,,最近一次校验失败：running_variable 缺失。,真实候选样本文件校验失败：running_variable 缺失,等待修复,data/raw/hs300_rdd_candidates.csv,results/literature/hs300_rdd/candidate_batch_audit.csv,2,1,1,1,1,running_variable 缺失",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    status = dashboard_loaders.load_rdd_status(ROOT, output_dir=tmp_path)

    assert status["mode"] == "missing"
    assert status["evidence_tier"] == "L0"
    assert status["source_kind"] == "missing"
    assert status["source_file"] == "data/raw/hs300_rdd_candidates.csv"
    assert status["generated_at"] == "2026-04-18T10:00:00+08:00"
    assert status["coverage_note"] == "最近一次校验失败：running_variable 缺失。"
    assert status["audit_file"] == "results/literature/hs300_rdd/candidate_batch_audit.csv"
    assert status["candidate_rows"] == 2
    assert status["candidate_batches"] == 1
    assert status["treated_rows"] == 1
    assert status["control_rows"] == 1
    assert status["crossing_batches"] == 1
    assert status["validation_error"] == "running_variable 缺失"


def test_build_rdd_contract_check_detects_matching_manifest(tmp_path: Path) -> None:
    status = {
        "mode": "reconstructed",
        "evidence_tier": "L2",
        "evidence_status": "公开重建样本",
        "source_kind": "reconstructed",
        "source_label": "公开重建候选样本文件",
        "source_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
        "generated_at": "",
        "as_of_date": "",
        "batch_label": "",
        "coverage_note": "311 条候选；6 个批次；调入 6 / 对照 305。",
        "message": "当前正在使用公开数据重建的候选样本文件。",
        "note": "基于公开数据重建的边界样本，可进入公开数据版证据链。",
        "input_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
        "audit_file": "",
        "candidate_rows": 311,
        "candidate_batches": 6,
        "treated_rows": 6,
        "control_rows": 305,
        "crossing_batches": 6,
        "validation_error": "",
    }
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    build_results_manifest("real", status).to_csv(manifest_path, index=False)

    contract = dashboard_loaders.build_rdd_contract_check(
        ROOT,
        rdd_status=status,
        manifest_path=manifest_path,
    )

    assert contract["manifest_exists"] is True
    assert contract["manifest_profile"] == "real"
    assert contract["matches"] is True
    assert contract["mismatched_fields"] == []
    assert contract["manifest_path"].endswith("results/real_tables/results_manifest.csv")


def test_build_rdd_contract_check_reports_mismatched_fields(tmp_path: Path) -> None:
    status = {
        "mode": "reconstructed",
        "evidence_tier": "L2",
        "evidence_status": "公开重建样本",
        "source_kind": "reconstructed",
        "source_label": "公开重建候选样本文件",
        "source_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
        "generated_at": "",
        "as_of_date": "",
        "batch_label": "",
        "coverage_note": "311 条候选；6 个批次；调入 6 / 对照 305。",
        "message": "当前正在使用公开数据重建的候选样本文件。",
        "note": "基于公开数据重建的边界样本，可进入公开数据版证据链。",
        "input_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
        "audit_file": "",
        "candidate_rows": 311,
        "candidate_batches": 6,
        "treated_rows": 6,
        "control_rows": 305,
        "crossing_batches": 6,
        "validation_error": "",
    }
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    build_results_manifest(
        "real",
        {
            **status,
            "source_label": "待补候选样本",
            "coverage_note": "最近一次校验失败。",
        },
    ).to_csv(manifest_path, index=False)

    contract = dashboard_loaders.build_rdd_contract_check(
        ROOT,
        rdd_status=status,
        manifest_path=manifest_path,
    )

    assert contract["manifest_exists"] is True
    assert contract["matches"] is False
    assert contract["mismatched_fields"] == ["source_label", "coverage_note"]


def test_split_figure_caption_separates_lead_and_focus() -> None:
    lead, focus = dashboard_media.split_figure_caption(
        "中国样本 RDD 主图。图意：展示断点回归主图。阅读重点：观察 0 附近是否跳跃。"
    )

    assert lead == "中国样本 RDD 主图。展示断点回归主图。"
    assert focus == "观察 0 附近是否跳跃。"


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
            "caption_lead": "风格识别:sample_event_timeline.png",
            "caption_focus": "",
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
            "caption_lead": "风格识别:sample_event_timeline.png",
            "caption_focus": "",
        },
        {
            "path": "results/literature/hs300_rdd/car_m1_p1_rdd_main.png",
            "caption": "断点回归:car_m1_p1_rdd_main.png",
            "caption_lead": "断点回归:car_m1_p1_rdd_main.png",
            "caption_focus": "",
        },
    ]


def test_load_identification_china_saved_result_keeps_rdd_outputs_when_status_reconstructed(tmp_path: Path) -> None:
    style_dir = tmp_path / "results" / "literature" / "hs300_style"
    rdd_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    style_dir.mkdir(parents=True)
    rdd_dir.mkdir(parents=True)
    (style_dir / "summary.md").write_text("style summary", encoding="utf-8")
    (rdd_dir / "summary.md").write_text("rdd summary", encoding="utf-8")
    (style_dir / "sample_event_timeline.png").write_bytes(b"fake")
    (rdd_dir / "car_m1_p1_rdd_main.png").write_bytes(b"fake")

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
        load_rdd_status=lambda: {
            "mode": "reconstructed",
            "evidence_status": "公开重建样本",
            "message": "reconstructed",
            "note": "",
            "input_file": "",
            "audit_file": "",
            "candidate_rows": None,
            "candidate_batches": None,
            "treated_rows": None,
            "control_rows": None,
            "crossing_batches": None,
            "validation_error": "",
        },
        load_saved_tables=lambda output_dir: [("表格", output_dir.name)],
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
        build_figure_caption=lambda path, custom_caption=None, prefix=None: f"{prefix or 'none'}:{path.name}",
    )

    assert result["rendered_tables"] == [
        ("风格识别：表格", "hs300_style"),
        ("断点回归：表格", "hs300_rdd"),
    ]
    assert result["figure_paths"][-1]["caption"] == "断点回归:car_m1_p1_rdd_main.png"
