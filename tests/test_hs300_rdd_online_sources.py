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


def test_main_passes_date_window_to_collector(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {
            "draft_output": tmp_path / "draft.csv",
            "audit_output": tmp_path / "audit.csv",
            "report_output": tmp_path / "report.md",
            "formal_output": None,
            "candidate_rows": 0,
            "source_rows": 0,
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
            "--force",
        ]
    )

    assert rc == 0
    assert captured["since"] == "2020-01-01"
    assert captured["until"] == "2022-12-31"
    assert captured["notice_rows"] == 120
    assert captured["max_notices"] == 6


def test_collect_official_sources_writes_audit_when_no_candidates(monkeypatch, tmp_path: Path) -> None:
    def fake_query(session, *, search_terms=online_sources.SEARCH_TERMS, rows=online_sources.DEFAULT_NOTICE_ROWS):
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
        report_output=tmp_path / "online_collection_report.md",
        attachment_dir=tmp_path / "official_attachments",
        since="2020-01-01",
        until="2022-12-31",
        force=True,
    )

    assert outputs["status"] == "no_candidates"
    assert outputs["candidate_rows"] == 0
    assert outputs["source_rows"] == 1
    assert (tmp_path / "official_candidate_draft.csv").exists()
    assert (tmp_path / "online_source_audit.csv").exists()
    report = (tmp_path / "online_collection_report.md").read_text(encoding="utf-8")
    assert "没有解析出" in report


def test_collect_official_sources_writes_header_when_no_notices(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(online_sources, "query_rebalance_announcements", lambda session, **kwargs: [])

    outputs = online_sources.collect_official_hs300_sources(
        output_dir=tmp_path,
        draft_output=tmp_path / "official_candidate_draft.csv",
        audit_output=tmp_path / "online_source_audit.csv",
        report_output=tmp_path / "online_collection_report.md",
        attachment_dir=tmp_path / "official_attachments",
        since="2020-01-01",
        until="2022-12-31",
        force=True,
    )

    assert outputs["status"] == "no_candidates"
    audit_header = (tmp_path / "online_source_audit.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "announcement_id" in audit_header
    report = (tmp_path / "online_collection_report.md").read_text(encoding="utf-8")
    assert "没有匹配到中证官网定期调整结果公告" in report
