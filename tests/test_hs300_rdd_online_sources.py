from pathlib import Path

import pandas as pd

from index_inclusion_research import hs300_rdd_online_sources as online_sources
from index_inclusion_research.prepare_hs300_rdd_candidates import (
    _reconstructed_source_reason,
)

SAMPLE_ATTACHMENT_TEXT = """
附件：部分指数样本调整名单

沪深 300 指数样本调整名单：
          调出名单                    调入名单
  证券代码         证券名称     证券代码             证券名称
   000800      一汽解放      002384          东山精密
   002129      TCL 中环    300251          光线传媒

中证 500 指数样本调整名单：
  证券代码          证券名称    证券代码             证券名称
   000066       中国长城     000088           盐田港

沪深 300 指数备选名单：
 排序     证券代码       证券名称   排序        证券代码       证券名称
  1      000988    华工科技     3        002294     信立泰
  2      688072    拓荆科技

中证 500 指数备选名单：
  排序     证券代码      证券名称
    1     688037    芯源微
"""


def _attachment_link() -> online_sources.AttachmentLink:
    return online_sources.AttachmentLink(
        notice_id=3006000,
        notice_title="关于沪深300、中证500、中证1000、中证A500等指数定期调整结果的公告",
        publish_date="2025-11-28",
        effective_date="2025-12-12",
        file_name="附件：部分指数样本调整名单.pdf",
        file_url="https://oss-ch.csindex.com.cn/notice/20251128165753-附件：部分指数样本调整名单.pdf",
        detail_url="https://www.csindex.com.cn/zh-CN/about/newsDetail?id=3006000",
    )


def test_parse_hs300_attachment_text_extracts_additions_and_reserves() -> None:
    parsed = online_sources.parse_hs300_attachment_text(SAMPLE_ATTACHMENT_TEXT)

    assert [row["ticker"] for row in parsed.additions] == ["002384", "300251"]
    assert [row["security_name"] for row in parsed.additions] == ["东山精密", "光线传媒"]
    assert [row["rank"] for row in parsed.reserves] == [1, 2, 3]
    assert [row["security_name"] for row in parsed.reserves] == ["华工科技", "拓荆科技", "信立泰"]
    assert parsed.usable_for_l3


def test_build_candidate_rows_maps_official_order_around_cutoff() -> None:
    parsed = online_sources.parse_hs300_attachment_text(SAMPLE_ATTACHMENT_TEXT)
    rows = online_sources.build_candidate_rows(_attachment_link(), parsed)
    frame = pd.DataFrame(rows)

    assert len(frame) == 5
    assert set(frame["batch_id"]) == {"csi300-2025-11"}
    assert frame.loc[frame["inclusion"] == 1, "running_variable"].min() > online_sources.CSI300_CUTOFF
    assert frame.loc[frame["inclusion"] == 0, "running_variable"].max() < online_sources.CSI300_CUTOFF
    assert set(frame["event_type"]) == {"official_adjustment_addition", "official_reserve_control"}

    validated = online_sources.validate_candidate_frame(frame)
    assert _reconstructed_source_reason(Path("official_candidate_draft.csv"), validated) == ""


def test_filter_notices_by_publish_date_window() -> None:
    notices = [
        {"id": 1, "publish_date": "2019-12-31"},
        {"id": 2, "publish_date": "2020-05-29"},
        {"id": 3, "publish_date": "2022-11-25"},
        {"id": 4, "publish_date": "2023-05-26"},
        {"id": 5, "publish_date": ""},
    ]

    filtered = online_sources._filter_notices_by_publish_date(
        notices,
        since="2020-01-01",
        until="2022-12-31",
    )

    assert [notice["id"] for notice in filtered] == [2, 3]


def test_query_rebalance_announcements_records_search_diagnostics_and_legacy_titles() -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "code": "200",
                "data": [
                    {
                        "id": 100,
                        "title": "关于调整沪深300指数样本股的公告",
                        "theme": "指数调样",
                        "publishDate": "2020-05-29",
                    },
                    {
                        "id": 101,
                        "title": "沪深300指数样本调整名单",
                        "theme": "",
                        "publishDate": "2020-11-27",
                    },
                    {
                        "id": 102,
                        "title": "沪深300指数市场表现回顾",
                        "theme": "市场资讯",
                        "publishDate": "2020-12-31",
                    },
                ],
            }

    class FakeSession:
        def post(self, *args, **kwargs) -> FakeResponse:
            return FakeResponse()

    diagnostics: list[dict[str, object]] = []
    notices = online_sources.query_rebalance_announcements(
        FakeSession(),
        search_terms=("沪深300 指数样本股 调整",),
        rows=3,
        search_diagnostics=diagnostics,
    )

    assert [notice["id"] for notice in notices] == [100, 101]
    assert diagnostics[0]["raw_rows"] == 3
    assert diagnostics[0]["hs300_title_rows"] == 3
    assert diagnostics[0]["title_matched_rows"] == 2
    assert diagnostics[0]["theme_matched_rows"] == 2
    assert diagnostics[0]["matched_rows"] == 2
    assert "100" in str(diagnostics[0]["matched_notice_ids"])
    assert "关于调整沪深300指数样本股的公告" in str(diagnostics[0]["sample_titles"])


def test_main_passes_date_window_to_collector(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {
            "draft_output": tmp_path / "draft.csv",
            "audit_output": tmp_path / "audit.csv",
            "search_diagnostics_output": tmp_path / "search.csv",
            "year_coverage_output": tmp_path / "year.csv",
            "report_output": tmp_path / "report.md",
            "formal_output": None,
            "candidate_rows": 0,
            "source_rows": 0,
            "search_rows": 1,
            "year_rows": 3,
            "candidate_batches": 0,
            "status": "no_candidates",
        }

    monkeypatch.setattr(online_sources, "collect_official_hs300_sources", fake_collect)

    rc = online_sources.main(
        [
            "--since",
            "2020-01-01",
            "--until",
            "2022-12-31",
            "--notice-rows",
            "120",
            "--max-notices",
            "6",
            "--search-term",
            "沪深300历史样本调整",
            "--force",
        ]
    )

    assert rc == 0
    assert captured["since"] == "2020-01-01"
    assert captured["until"] == "2022-12-31"
    assert captured["notice_rows"] == 120
    assert captured["max_notices"] == 6
    assert "沪深300历史样本调整" in captured["search_terms"]
    assert captured["search_diagnostics_output"] == online_sources.DEFAULT_SEARCH_DIAGNOSTICS_OUTPUT
    assert captured["year_coverage_output"] == online_sources.DEFAULT_YEAR_COVERAGE_OUTPUT


def test_collect_official_sources_writes_audit_when_no_candidates(monkeypatch, tmp_path: Path) -> None:
    def fake_query(
        session,
        *,
        search_terms=online_sources.SEARCH_TERMS,
        rows=online_sources.DEFAULT_NOTICE_ROWS,
        search_diagnostics=None,
    ):
        if search_diagnostics is not None:
            search_diagnostics.append(
                {
                    "search_term": search_terms[0],
                    "requested_rows": rows,
                    "api_code": "200",
                    "status": "ok",
                    "raw_rows": 1,
                    "matched_rows": 1,
                    "matched_notice_ids": "100",
                }
            )
        return [
            {
                "id": 100,
                "title": "关于沪深300、中证500、中证1000等指数定期调整结果的公告",
                "theme": "指数调样",
                "publish_date": "2022-11-25",
                "detail_url": "https://www.csindex.com.cn/zh-CN/about/newsDetail?id=100",
            }
        ]

    def fake_detail(session, notice_id: int):
        return {
            "id": notice_id,
            "title": "关于沪深300、中证500、中证1000等指数定期调整结果的公告",
            "publishDate": "2022-11-25",
            "content": "",
            "enclosureList": [],
        }

    monkeypatch.setattr(online_sources, "query_rebalance_announcements", fake_query)
    monkeypatch.setattr(online_sources, "fetch_notice_detail", fake_detail)

    outputs = online_sources.collect_official_hs300_sources(
        output_dir=tmp_path,
        draft_output=tmp_path / "official_candidate_draft.csv",
        audit_output=tmp_path / "online_source_audit.csv",
        search_diagnostics_output=tmp_path / "online_search_diagnostics.csv",
        year_coverage_output=tmp_path / "online_year_coverage.csv",
        report_output=tmp_path / "online_collection_report.md",
        attachment_dir=tmp_path / "official_attachments",
        since="2020-01-01",
        until="2022-12-31",
        force=True,
    )

    assert outputs["status"] == "no_candidates"
    assert outputs["candidate_rows"] == 0
    assert outputs["source_rows"] == 1
    assert outputs["search_rows"] == 1
    assert outputs["year_rows"] == 3
    assert (tmp_path / "official_candidate_draft.csv").exists()
    assert (tmp_path / "online_source_audit.csv").exists()
    assert (tmp_path / "online_search_diagnostics.csv").exists()
    assert (tmp_path / "online_year_coverage.csv").exists()
    report = (tmp_path / "online_collection_report.md").read_text(encoding="utf-8")
    assert "没有解析出" in report


def test_collect_official_sources_audits_notice_detail_errors(monkeypatch, tmp_path: Path) -> None:
    def fake_query(session, *, search_diagnostics=None, **kwargs):
        if search_diagnostics is not None:
            search_diagnostics.append(
                {
                    "search_term": "调整沪深300指数样本股",
                    "requested_rows": kwargs["rows"],
                    "api_code": "200",
                    "status": "ok",
                    "raw_rows": 1,
                    "matched_rows": 1,
                    "matched_notice_ids": "100",
                }
            )
        return [
            {
                "id": 100,
                "title": "关于调整沪深300指数样本股的公告",
                "theme": "指数调样",
                "publish_date": "2005-12-20",
                "detail_url": "https://www.csindex.com.cn/zh-CN/about/newsDetail?id=100",
            }
        ]

    def fake_detail(session, notice_id: int):
        raise ValueError("detail unavailable")

    monkeypatch.setattr(online_sources, "query_rebalance_announcements", fake_query)
    monkeypatch.setattr(online_sources, "fetch_notice_detail", fake_detail)

    outputs = online_sources.collect_official_hs300_sources(
        output_dir=tmp_path,
        draft_output=tmp_path / "official_candidate_draft.csv",
        audit_output=tmp_path / "online_source_audit.csv",
        search_diagnostics_output=tmp_path / "online_search_diagnostics.csv",
        year_coverage_output=tmp_path / "online_year_coverage.csv",
        report_output=tmp_path / "online_collection_report.md",
        attachment_dir=tmp_path / "official_attachments",
        force=True,
    )

    audit = pd.read_csv(tmp_path / "online_source_audit.csv")
    assert outputs["status"] == "no_candidates"
    assert outputs["source_rows"] == 1
    assert audit.loc[0, "status"] == "found"
    assert "detail unavailable" in audit.loc[0, "reason"]


def test_collect_official_sources_writes_diagnostics_and_years_when_no_notices(monkeypatch, tmp_path: Path) -> None:
    def fake_query(session, **kwargs):
        kwargs["search_diagnostics"].append(
            {
                "search_term": "沪深300 指数样本 调整",
                "requested_rows": kwargs["rows"],
                "api_code": "200",
                "status": "ok",
                "raw_rows": 0,
                "matched_rows": 0,
                "reason": "No rows matched the HS300 rebalance title/theme filters.",
            }
        )
        return []

    monkeypatch.setattr(online_sources, "query_rebalance_announcements", fake_query)

    outputs = online_sources.collect_official_hs300_sources(
        output_dir=tmp_path,
        draft_output=tmp_path / "official_candidate_draft.csv",
        audit_output=tmp_path / "online_source_audit.csv",
        search_diagnostics_output=tmp_path / "online_search_diagnostics.csv",
        year_coverage_output=tmp_path / "online_year_coverage.csv",
        report_output=tmp_path / "online_collection_report.md",
        attachment_dir=tmp_path / "official_attachments",
        since="2020-01-01",
        until="2022-12-31",
        force=True,
    )

    assert outputs["status"] == "no_candidates"
    assert outputs["search_rows"] == 1
    assert outputs["year_rows"] == 3
    audit_header = (tmp_path / "online_source_audit.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "announcement_id" in audit_header
    search_diagnostics = pd.read_csv(tmp_path / "online_search_diagnostics.csv")
    year_coverage = pd.read_csv(tmp_path / "online_year_coverage.csv")
    assert search_diagnostics.loc[0, "search_term"] == "沪深300 指数样本 调整"
    assert set(year_coverage["year"]) == {2020, 2021, 2022}
    assert set(year_coverage["status"]) == {"no_notice"}
    report = (tmp_path / "online_collection_report.md").read_text(encoding="utf-8")
    assert "搜索诊断" in report
    assert "年份覆盖" in report
    assert "没有匹配到中证官网定期调整结果公告" in report
