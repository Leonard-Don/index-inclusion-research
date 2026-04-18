from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SRC = ROOT / "src"
for path in [SCRIPTS, SRC]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import prepare_hs300_rdd_candidates as prepare_script
from index_inclusion_research.analysis.rdd_candidates import prepare_candidate_frame, validate_candidate_frame


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


def test_prepare_script_check_only_validates_without_writing_outputs(tmp_path: Path) -> None:
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
