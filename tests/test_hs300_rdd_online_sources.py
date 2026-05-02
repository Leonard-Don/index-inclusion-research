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
