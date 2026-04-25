from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research import prepare_hs300_rdd_candidates as prepare_script
from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit,
    prepare_candidate_frame,
    summarize_candidate_audit,
    validate_candidate_frame,
)


def _raw_candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "证券代码": "686",
                "证券简称": "东北证券",
                "公告日期": "2024-11-29",
                "实施日": "2024-12-16",
                "候选排名": "300.22",
                "是否调入": "是",
                "备注": "manually collected",
                "无关字段": "drop-me",
            },
            {
                "证券代码": "1",
                "证券简称": "平安银行",
                "公告日期": "2024-11-29",
                "实施日": "2024-12-16",
                "候选排名": "299.91",
                "是否调入": "否",
                "备注": "manual transcription",
                "无关字段": "drop-me-too",
            },
        ]
    )


def test_prepare_candidate_frame_applies_aliases_defaults_and_derives_batch_id() -> None:
    prepared, metadata = prepare_candidate_frame(
        _raw_candidate_frame(),
        defaults={
            "market": "CN",
            "index_name": "CSI300",
            "cutoff": 300,
            "event_type": "inclusion_rdd",
            "source": "CSIndex",
        },
    )

    validated = validate_candidate_frame(prepared)

    assert validated["ticker"].tolist() == ["000686", "000001"]
    assert validated["inclusion"].tolist() == [1, 0]
    assert validated["market"].tolist() == ["CN", "CN"]
    assert validated["index_name"].tolist() == ["CSI300", "CSI300"]
    assert validated["cutoff"].tolist() == [300, 300]
    assert validated["batch_id"].tolist() == ["2024-11-29", "2024-11-29"]
    assert "batch_id" in metadata["derived_fields"]
    assert set(metadata["defaults_applied"]) >= {"market", "index_name", "cutoff", "event_type", "source"}
    assert "无关字段" in metadata["unused_columns"]


def test_l3_preflight_ready_when_default_output_and_provenance_complete() -> None:
    prepared, _ = prepare_candidate_frame(
        _raw_candidate_frame(),
        defaults={
            "market": "CN",
            "index_name": "CSI300",
            "cutoff": 300,
            "event_type": "inclusion_rdd",
            "source": "CSIndex",
            "source_url": "https://www.csindex.com.cn/",
        },
    )
    validated = validate_candidate_frame(prepared)
    audit = build_candidate_batch_audit(validated)
    report = prepare_script._build_l3_preflight_report(
        validated=validated,
        input_path=Path("raw_candidates.csv"),
        output_path=prepare_script.DEFAULT_OUTPUT,
        audit_summary=summarize_candidate_audit(audit),
        check_only=False,
    )

    assert report["status"] == "ready"
    assert report["status_label"] == "可接入 L3"
    assert {check["status"] for check in report["checks"]} == {"pass"}
    assert report["next_commands"] == [
        "index-inclusion-hs300-rdd",
        "index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma",
    ]


def test_l3_preflight_blocks_reconstructed_input_from_formal_promotion() -> None:
    prepared, _ = prepare_candidate_frame(
        _raw_candidate_frame(),
        defaults={
            "market": "CN",
            "index_name": "CSI300",
            "cutoff": 300,
            "event_type": "inclusion_rdd",
            "source": "public reconstruction",
            "source_url": "https://example.test/reconstructed",
        },
    )
    validated = validate_candidate_frame(prepared)
    audit = build_candidate_batch_audit(validated)
    report = prepare_script._build_l3_preflight_report(
        validated=validated,
        input_path=prepare_script.DEFAULT_RECONSTRUCTED_INPUT,
        output_path=prepare_script.DEFAULT_OUTPUT,
        audit_summary=summarize_candidate_audit(audit),
        check_only=True,
    )

    assert report["status"] == "blocked"
    assert report["status_label"] == "暂不可接入 L3"
    assert any(check["label"] == "来源层级" and check["status"] == "block" for check in report["checks"])
    joined_commands = "\n".join(report["next_commands"])
    assert "hs300_rdd_candidates.csv --force" not in joined_commands


def test_l3_preflight_blocks_copied_reconstructed_source_metadata() -> None:
    prepared, _ = prepare_candidate_frame(
        _raw_candidate_frame(),
        defaults={
            "market": "CN",
            "index_name": "CSI300",
            "cutoff": 300,
            "event_type": "inclusion_rdd",
            "source": "CSI300 constituent-union public reconstruction",
            "source_url": "https://www.csindex.com.cn/zh-CN/indices/index-detail/000300",
            "note": "Reconstructed from public market-cap proxies; not an official CSIndex reserve list.",
        },
    )
    validated = validate_candidate_frame(prepared)
    audit = build_candidate_batch_audit(validated)
    report = prepare_script._build_l3_preflight_report(
        validated=validated,
        input_path=Path("copied_candidates.csv"),
        output_path=prepare_script.DEFAULT_OUTPUT,
        audit_summary=summarize_candidate_audit(audit),
        check_only=True,
    )

    assert report["status"] == "blocked"
    source_check = next(check for check in report["checks"] if check["label"] == "来源层级")
    assert source_check["status"] == "block"
    assert "source/source_url/note" in source_check["copy"]


def test_prepare_script_refuses_to_promote_copied_reconstructed_metadata(tmp_path: Path) -> None:
    input_path = tmp_path / "copied_candidates.csv"
    _raw_candidate_frame().to_csv(input_path, index=False)

    with pytest.raises(SystemExit) as exc:
        prepare_script.main(
            [
                "--input",
                str(input_path),
                "--source",
                "CSI300 constituent-union public reconstruction",
                "--note",
                "Reconstructed from public market-cap proxies; not an official CSIndex reserve list.",
                "--output",
                str(prepare_script.DEFAULT_OUTPUT),
            ]
        )

    assert exc.value.code == 2


def test_prepare_script_check_only_validates_without_writing_outputs(tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "raw_candidates.csv"
    output_path = tmp_path / "hs300_rdd_candidates.csv"
    audit_path = tmp_path / "candidate_batch_audit.csv"
    summary_path = tmp_path / "import_summary.md"
    _raw_candidate_frame().to_csv(input_path, index=False)

    exit_code = prepare_script.main(
        [
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--audit-output",
            str(audit_path),
            "--summary-output",
            str(summary_path),
            "--check-only",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr().out
    assert "L3 preflight: 可接入但需补充" in captured
    assert "Check-only mode: no files were written." in captured
    assert "index-inclusion-prepare-hs300-rdd --input" in captured
    assert not output_path.exists()
    assert not audit_path.exists()
    assert not summary_path.exists()


def test_prepare_script_writes_standardized_candidate_and_audit_outputs(tmp_path: Path) -> None:
    input_path = tmp_path / "raw_candidates.csv"
    output_path = tmp_path / "hs300_rdd_candidates.csv"
    audit_path = tmp_path / "candidate_batch_audit.csv"
    summary_path = tmp_path / "import_summary.md"
    _raw_candidate_frame().to_csv(input_path, index=False)

    exit_code = prepare_script.main(
        [
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--audit-output",
            str(audit_path),
            "--summary-output",
            str(summary_path),
        ]
    )

    assert exit_code == 0
    assert output_path.exists()
    assert audit_path.exists()
    assert summary_path.exists()

    standardized = pd.read_csv(output_path, dtype={"ticker": str, "batch_id": str})
    audit = pd.read_csv(audit_path)
    summary = summary_path.read_text(encoding="utf-8")

    assert standardized["ticker"].tolist() == ["000686", "000001"]
    assert standardized["inclusion"].tolist() == [1, 0]
    assert audit.loc[0, "has_cutoff_crossing"] in (True, 1)
    assert "HS300 RDD 候选样本导入摘要" in summary
    assert "列映射" in summary
    assert "L3 导入预检" in summary
    assert "可接入但需补充" in summary
    assert "正式样本路径" in summary
    assert "下一步命令" in summary
