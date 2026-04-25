from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import hs300_rdd_l3_collection as collection_cli


def _candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "batch_id": "2024-11-29",
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "000686",
                "security_name": "东北证券",
                "announce_date": "2024-11-29",
                "effective_date": "2024-12-16",
                "running_variable": 300.22,
                "cutoff": 300,
                "inclusion": 1,
                "event_type": "reconstructed_post_member",
                "source": "CSI300 constituent-union public reconstruction",
                "source_url": "https://www.csindex.com.cn/zh-CN/indices/index-detail/000300",
                "note": "not an official CSIndex reserve list",
            },
            {
                "batch_id": "2024-11-29",
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "000001",
                "security_name": "平安银行",
                "announce_date": "2024-11-29",
                "effective_date": "2024-12-16",
                "running_variable": 299.91,
                "cutoff": 300,
                "inclusion": 0,
                "event_type": "reconstructed_pre_only_member",
                "source": "CSI300 constituent-union public reconstruction",
                "source_url": "https://www.csindex.com.cn/zh-CN/indices/index-detail/000300",
                "note": "not an official CSIndex reserve list",
            },
        ]
    )


def test_build_batch_collection_checklist_summarizes_formal_requirements() -> None:
    checklist = collection_cli.build_batch_collection_checklist(_candidate_frame())

    assert checklist.loc[0, "batch_id"] == "2024-11-29"
    assert checklist.loc[0, "reconstructed_candidate_rows"] == 2
    assert checklist.loc[0, "reconstructed_included_rows"] == 1
    assert checklist.loc[0, "reconstructed_control_rows"] == 1
    assert checklist.loc[0, "has_cutoff_crossing"] in (True, 1)
    assert "batch_id" in checklist.loc[0, "required_fields"]
    assert "index-inclusion-prepare-hs300-rdd" in checklist.loc[0, "acceptance_command"]


def test_build_boundary_reference_keeps_nearest_names_on_each_side() -> None:
    boundary = collection_cli.build_boundary_reference(_candidate_frame(), window=1)

    assert boundary["ticker"].tolist() == ["000001", "000686"]
    assert set(boundary["boundary_side"]) == {"left_of_cutoff", "right_or_at_cutoff"}
    assert boundary["reference_warning"].str.contains("不能直接复制为 L3").all()


def test_write_collection_package_outputs_checklist_template_and_plan(tmp_path: Path) -> None:
    input_path = tmp_path / "reconstructed.csv"
    output_dir = tmp_path / "collection"
    _candidate_frame().to_csv(input_path, index=False)

    outputs = collection_cli.write_collection_package(
        input_path=input_path,
        output_dir=output_dir,
        force=False,
    )

    checklist_path = outputs["checklist_path"]
    template_path = outputs["template_path"]
    summary_path = outputs["summary_path"]
    assert isinstance(checklist_path, Path)
    assert isinstance(template_path, Path)
    assert isinstance(summary_path, Path)
    assert checklist_path.exists()
    assert template_path.exists()
    assert summary_path.exists()

    checklist = pd.read_csv(checklist_path)
    template = pd.read_csv(template_path)
    boundary = pd.read_csv(outputs["boundary_reference_path"], dtype={"ticker": str})
    summary = summary_path.read_text(encoding="utf-8")

    assert checklist.loc[0, "announce_date"] == "2024-11-29"
    assert template["collection_role"].tolist() == ["正式调入候选", "正式对照候选"]
    assert boundary["ticker"].tolist() == ["000001", "000686"]
    assert "HS300 RDD L3 正式候选样本采集包" in summary
    assert "不要复制公开重建排名口径" in summary
    assert "boundary_reference.csv" in summary
    assert outputs["candidate_batches"] == 1
    assert outputs["candidate_rows"] == 2
    assert outputs["boundary_reference_rows"] == 2
