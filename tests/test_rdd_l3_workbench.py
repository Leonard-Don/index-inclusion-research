from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import rdd_l3_workbench


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
            },
            {
                "证券代码": "1",
                "证券简称": "平安银行",
                "公告日期": "2024-11-29",
                "实施日": "2024-12-16",
                "候选排名": "299.91",
                "是否调入": "否",
            },
        ]
    )


def _standard_candidate_frame() -> pd.DataFrame:
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
            },
        ]
    )


def test_candidate_preflight_result_uses_prepare_script_contract(tmp_path: Path) -> None:
    input_path = tmp_path / "raw_candidates.csv"
    _raw_candidate_frame().to_csv(input_path, index=False)

    result = rdd_l3_workbench.build_candidate_preflight_result(
        input_path,
        defaults=rdd_l3_workbench._defaults(
            source="CSIndex",
            source_url="https://www.csindex.com.cn/",
        ),
    )

    assert result["preflight"]["status"] == "warning"
    assert result["preflight"]["status_label"] == "可接入但需补充"
    assert result["audit_summary"]["candidate_batches"] == 1
    assert result["candidate_preview"]["rows"][0]["ticker"] == "000686"


def test_candidate_preflight_result_blocks_reconstructed_metadata(tmp_path: Path) -> None:
    input_path = tmp_path / "raw_candidates.csv"
    _raw_candidate_frame().to_csv(input_path, index=False)

    result = rdd_l3_workbench.build_candidate_preflight_result(
        input_path,
        defaults=rdd_l3_workbench._defaults(
            source="public reconstruction",
            source_url="https://example.test/reconstructed",
        ),
    )

    assert result["preflight"]["status"] == "blocked"
    assert any(
        check["label"] == "来源层级" and check["status"] == "block"
        for check in result["preflight"]["checks"]
    )


def test_import_official_candidates_writes_standardized_outputs(tmp_path: Path) -> None:
    input_path = tmp_path / "raw_candidates.csv"
    output_path = tmp_path / "hs300_rdd_candidates.csv"
    audit_path = tmp_path / "candidate_batch_audit.csv"
    summary_path = tmp_path / "import_summary.md"
    _raw_candidate_frame().to_csv(input_path, index=False)

    result = rdd_l3_workbench.import_official_candidates(
        input_path,
        defaults=rdd_l3_workbench._defaults(
            source="CSIndex",
            source_url="https://www.csindex.com.cn/",
        ),
        output_path=output_path,
        audit_path=audit_path,
        summary_path=summary_path,
    )

    assert output_path.exists()
    assert audit_path.exists()
    assert summary_path.exists()
    assert result["preflight"]["status"] == "warning"
    assert pd.read_csv(output_path, dtype={"ticker": str})["ticker"].tolist() == [
        "000686",
        "000001",
    ]


def test_workbench_context_uses_supplied_root_paths(tmp_path: Path) -> None:
    context = rdd_l3_workbench.build_rdd_l3_workbench_context(root=tmp_path)

    assert context["status"]["mode"] == "missing"
    assert context["collection_status"]["status"] == "missing"
    assert context["collection_status"]["paths"][0]["label"].startswith("results/literature")
    assert context["collection_tables"][0]["key"] == "batch_collection_checklist"
    assert context["collection_tables"][0]["rows"] == []
    assert context["import_paths"][0]["label"] == "data/raw/hs300_rdd_candidates.csv"


def test_refresh_collection_package_builds_browser_workbench_artifacts(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw"
    input_dir.mkdir(parents=True)
    _standard_candidate_frame().to_csv(
        input_dir / "hs300_rdd_candidates.reconstructed.csv",
        index=False,
    )

    result = rdd_l3_workbench.refresh_collection_package(root=tmp_path, boundary_window=1)

    assert result["candidate_batches"] == 1
    assert result["boundary_reference_rows"] == 2
    assert result["status"]["status"] == "ready"
    written = {path["label"] for path in result["written_paths"]}
    assert "results/literature/hs300_rdd_l3_collection/collection_plan.md" in written
    assert "results/literature/hs300_rdd_l3_collection/formal_candidate_template.csv" in written

    context = rdd_l3_workbench.build_rdd_l3_workbench_context(root=tmp_path)
    tables = {table["key"]: table for table in context["collection_tables"]}
    assert tables["batch_collection_checklist"]["total_rows"] == 1
    assert tables["formal_candidate_template"]["total_rows"] == 2
    assert tables["boundary_reference"]["total_rows"] == 2
    assert tables["boundary_reference"]["rows"][0]["ticker"] in {"000686", "000001"}
