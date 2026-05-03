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
    assert context["online_collection_status"]["status"] == "missing"
    assert context["collection_status"]["paths"][0]["label"].startswith("results/literature")
    assert context["collection_tables"][0]["key"] == "batch_collection_checklist"
    assert context["online_collection_tables"][0]["key"] == "online_year_coverage"
    assert context["online_collection_tables"][1]["key"] == "online_source_audit"
    assert context["online_collection_tables"][2]["key"] == "online_manual_gap_worklist"
    assert context["online_collection_tables"][3]["key"] == "online_gap_source_hints"
    assert context["collection_tables"][0]["rows"] == []
    assert context["online_collection_tables"][0]["rows"] == []
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


def test_refresh_online_collection_uses_root_paths_and_extra_terms(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_collect(**kwargs):
        captured.update(kwargs)
        pd.DataFrame([{"ticker": "000001", "batch_id": "csi300-2023-05"}]).to_csv(kwargs["draft_output"], index=False)
        pd.DataFrame([{"source_kind": "official_rebalance_result_notice"}]).to_csv(kwargs["audit_output"], index=False)
        pd.DataFrame([{"search_term": "沪深300", "raw_rows": 1}]).to_csv(
            kwargs["search_diagnostics_output"],
            index=False,
        )
        pd.DataFrame([{"year": 2023, "status": "candidate_found"}]).to_csv(
            kwargs["year_coverage_output"],
            index=False,
        )
        pd.DataFrame([{"year": 2023, "priority": "P1"}]).to_csv(
            kwargs["manual_gap_worklist_output"],
            index=False,
        )
        pd.DataFrame([{"year": 2023, "source_kind": "web_search_csindex"}]).to_csv(
            kwargs["gap_source_hints_output"],
            index=False,
        )
        kwargs["report_output"].write_text("# report\n", encoding="utf-8")
        return {
            "draft_output": kwargs["draft_output"],
            "audit_output": kwargs["audit_output"],
            "search_diagnostics_output": kwargs["search_diagnostics_output"],
            "year_coverage_output": kwargs["year_coverage_output"],
            "manual_gap_worklist_output": kwargs["manual_gap_worklist_output"],
            "gap_source_hints_output": kwargs["gap_source_hints_output"],
            "report_output": kwargs["report_output"],
            "formal_output": None,
            "candidate_rows": 1,
            "source_rows": 1,
            "search_rows": 1,
            "year_rows": 1,
            "gap_rows": 1,
            "hint_rows": 1,
            "candidate_batches": 1,
            "status": "parsed",
        }

    monkeypatch.setattr(
        rdd_l3_workbench.hs300_rdd_online_sources,
        "collect_official_hs300_sources",
        fake_collect,
    )

    result = rdd_l3_workbench.refresh_online_collection(
        root=tmp_path,
        since="2020-01-01",
        until="2022-12-31",
        notice_rows=120,
        max_notices=6,
        extra_search_terms=("沪深300历史样本调整",),
    )

    collection_dir = tmp_path / "results" / "literature" / "hs300_rdd_l3_collection"
    assert captured["output_dir"] == collection_dir
    assert captured["draft_output"] == collection_dir / "official_candidate_draft.csv"
    assert captured["gap_source_hints_output"] == collection_dir / "online_gap_source_hints.csv"
    assert captured["since"] == "2020-01-01"
    assert captured["until"] == "2022-12-31"
    assert captured["notice_rows"] == 120
    assert captured["max_notices"] == 6
    assert captured["formal_output"] is None
    assert "沪深300历史样本调整" in captured["search_terms"]
    assert result["status"] == "parsed"
    assert result["hint_rows"] == 1
    assert result["online_status"]["status"] == "ready"
    assert any(path["label"].endswith("online_gap_source_hints.csv") for path in result["written_paths"])


def test_workbench_context_surfaces_online_collection_diagnostics(tmp_path: Path) -> None:
    collection_dir = tmp_path / "results" / "literature" / "hs300_rdd_l3_collection"
    collection_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"ticker": "000001", "batch_id": "csi300-2023-05"},
            {"ticker": "000002", "batch_id": "csi300-2023-05"},
        ]
    ).to_csv(collection_dir / "official_candidate_draft.csv", index=False)
    pd.DataFrame([{"status": "parsed"}, {"status": "detail_fetched"}]).to_csv(
        collection_dir / "online_source_audit.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {
                "search_term": "沪深300",
                "raw_rows": 4,
                "title_matched_rows": 3,
                "theme_matched_rows": 3,
                "matched_rows": 3,
                "date_filtered_matched_rows": 2,
                "matched_publish_dates": "2023-05-26",
                "reason": "",
            }
        ]
    ).to_csv(collection_dir / "online_search_diagnostics.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": 2022,
                "notice_rows": 1,
                "attachment_rows": 0,
                "usable_attachment_rows": 0,
                "candidate_rows": 0,
                "candidate_batches": 0,
                "status": "notice_only",
            },
            {
                "year": 2023,
                "notice_rows": 2,
                "attachment_rows": 1,
                "usable_attachment_rows": 1,
                "candidate_rows": 2,
                "candidate_batches": 1,
                "status": "candidate_found",
            },
        ]
    ).to_csv(collection_dir / "online_year_coverage.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": 2022,
                "priority": "P1",
                "gap_type": "parsed_additions_missing_controls",
                "publish_date": "2022-11-25",
                "title": "关于调整沪深300等指数样本的公告",
                "attachment_name": "指数样本调整名单.xlsx",
                "addition_rows": 1,
                "control_rows": 0,
                "missing_evidence": "official reserve/control list",
                "suggested_next_step": "补官方备选名单",
            }
        ]
    ).to_csv(collection_dir / "online_manual_gap_worklist.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": 2022,
                "priority": "P1",
                "gap_type": "parsed_additions_missing_controls",
                "source_kind": "official_attachment",
                "source_label": "中证官方附件",
                "source_url": "https://oss-ch.csindex.com.cn/notice/example.xlsx",
                "query": "",
                "expected_evidence": "official reserve/control list",
                "notes": "补官方备选名单",
            }
        ]
    ).to_csv(collection_dir / "online_gap_source_hints.csv", index=False)
    (collection_dir / "online_collection_report.md").write_text("# report\n", encoding="utf-8")

    context = rdd_l3_workbench.build_rdd_l3_workbench_context(root=tmp_path)

    status = context["online_collection_status"]
    assert status["status"] == "ready"
    assert status["candidate_rows"] == 2
    assert status["source_rows"] == 2
    assert status["search_rows"] == 1
    assert status["hint_rows"] == 1
    assert status["candidate_years"] == ["2023"]
    assert status["notice_only_years"] == ["2022"]
    tables = {table["key"]: table for table in context["online_collection_tables"]}
    assert tables["online_year_coverage"]["total_rows"] == 2
    assert tables["online_source_audit"]["total_rows"] == 2
    assert tables["online_source_audit"]["rows"][0]["status"] == "parsed"
    assert tables["online_manual_gap_worklist"]["total_rows"] == 1
    assert tables["online_manual_gap_worklist"]["rows"][0]["priority"] == "P1"
    assert tables["online_gap_source_hints"]["total_rows"] == 1
    assert tables["online_gap_source_hints"]["rows"][0]["source_kind"] == "official_attachment"
    assert tables["online_search_diagnostics"]["rows"][0]["search_term"] == "沪深300"
