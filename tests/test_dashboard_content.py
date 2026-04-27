from __future__ import annotations

from index_inclusion_research import dashboard_content

PROJECT_MODULE_DISPLAY = {
    "短期价格压力": "短期价格压力与效应减弱",
    "需求曲线效应": "需求曲线与长期保留",
    "沪深300论文复现": "制度识别与中国市场证据",
}


def _render_table_stub(frame, compact: bool = False) -> str:
    return f"<table rows='{len(frame)}' compact='{int(compact)}'>{'|'.join(frame.columns)}</table>"


def test_compact_author_label_handles_single_pair_and_group_authors() -> None:
    assert dashboard_content.compact_author_label("Lawrence Harris") == "Harris"
    assert (
        dashboard_content.compact_author_label("Lawrence Harris; Eitan Gurel")
        == "Harris、Gurel"
    )
    assert (
        dashboard_content.compact_author_label("A Alpha; B Beta; C Gamma") == "Alpha 等"
    )


def test_paper_brief_title_uses_expected_record_keys() -> None:
    assert (
        dashboard_content.paper_brief_title(
            {"authors": "Lawrence Harris; Eitan Gurel", "year_label": "1986"}
        )
        == "Harris、Gurel（1986）"
    )


def test_load_literature_library_result_keeps_cards_and_docs_tables() -> None:
    result = dashboard_content.load_literature_library_result(
        render_table=_render_table_stub,
        library_card={
            "title": "16 篇文献库",
            "subtitle": "Literature Library",
            "description_zh": "反方、中性、正方三组文献与项目模块映射",
        },
    )

    assert result["id"] == "paper_library"
    assert result["output_dir"] == "docs"
    assert result["summary_cards"]
    assert [label for label, _ in result["rendered_tables"]] == [
        "文献分组统计",
        "文献目录",
    ]
    assert all("compact='1'" in html for _, html in result["rendered_tables"])


def test_load_supplement_result_keeps_expected_table_sequence() -> None:
    result = dashboard_content.load_supplement_result(
        render_table=_render_table_stub,
        supplement_card={
            "title": "机制与执行补充",
            "subtitle": "Mechanics & Execution",
            "description_zh": "事件时钟、机制链、冲击估算与表达框架，不进入文献库，仅作补充层",
        },
    )

    assert result["id"] == "project_supplement"
    assert [label for label, _ in result["rendered_tables"]] == [
        "事件时钟",
        "机制链",
        "冲击估算步骤",
        "冲击估算示例",
        "表达框架",
    ]


def test_load_paper_detail_result_builds_navigation_and_actions() -> None:
    result = dashboard_content.load_paper_detail_result(
        "harris_gurel_1986",
        render_table=_render_table_stub,
        project_module_display_map=PROJECT_MODULE_DISPLAY,
    )

    assert result is not None
    assert result["title"] == "Lawrence Harris 等（1986）"
    assert [label for label, _ in result["rendered_tables"]] == ["论文信息", "深度解读"]
    assert any(
        card["kicker"] == "当前这篇" and card["is_current"]
        for card in result["sequence_cards"]
    )
    assert any(card["kicker"] == "前一篇" for card in result["sequence_cards"])
    assert any(card["kicker"] == "后一篇" for card in result["sequence_cards"])
    prev_card = next(
        card for card in result["sequence_cards"] if card["kicker"] == "前一篇"
    )
    next_card = next(
        card for card in result["sequence_cards"] if card["kicker"] == "后一篇"
    )
    current_card = next(
        card for card in result["sequence_cards"] if card["kicker"] == "当前这篇"
    )
    assert result["recommended_cards"]
    assert all(
        card["href"].startswith("/paper/") for card in result["recommended_cards"]
    )
    assert [card["kicker"] for card in result["recommended_cards"]] == [
        "同主线延伸",
        "跨主线参照",
    ]
    assert [card["title"] for card in result["recommended_cards"]] == [
        "Denis 等（2003）",
        "Wurgler、Zhuravskaya（2002）",
    ]
    assert all(
        card["kicker"] in {"同阵营同主线", "同阵营延伸", "同主线延伸", "跨主线参照"}
        for card in result["recommended_cards"]
    )
    assert all(card["copy"].startswith("适合") for card in result["recommended_cards"])
    assert {card["title"] for card in result["recommended_cards"]}.isdisjoint(
        {card["title"] for card in result["sequence_cards"]}
    )
    assert [view["id"] for view in result["evolution_nav_views"]] == [
        "camp",
        "track",
        "stance",
    ]
    assert result["primary_actions"] == [
        {
            "label": "查看原文 PDF",
            "href": "/paper/harris_gurel_1986/pdf",
            "target": "_blank",
        }
    ]
    assert "·" in result["subtitle"]
    assert result["hero_aside_title"]
    assert any(item["label"] == "研究主线" for item in result["hero_meta_items"])
    assert any(card["title"] for card in result["summary_cards"])
    assert "公开 alpha" in result["summary_paragraphs"][2]
    assert "公开 alpha" in result["hero_aside_copy"]
    assert "价格发现" in result["summary_cards"][0]["copy"]
    assert "重新改写" in result["summary_cards"][1]["copy"]
    assert prev_card["copy"].startswith("如果你想回看")
    assert next_card["copy"].startswith("如果你想继续")
    assert current_card["copy"].startswith("你现在就位于这条争论链的当前节点。")
    assert prev_card["action_label"] == "回看上一环"
    assert next_card["action_label"] == "继续下一环"


def test_load_paper_detail_result_returns_none_for_unknown_paper() -> None:
    result = dashboard_content.load_paper_detail_result(
        "missing-paper-id",
        render_table=_render_table_stub,
        project_module_display_map=PROJECT_MODULE_DISPLAY,
    )

    assert result is None


def test_load_paper_detail_result_includes_verdict_citations_for_cited_paper() -> None:
    """Papers cited by H1+H3 should get a verdict_citations list with both."""
    result = dashboard_content.load_paper_detail_result(
        "harris_gurel_1986",
        render_table=_render_table_stub,
        project_module_display_map=PROJECT_MODULE_DISPLAY,
    )
    assert result is not None
    citations = result.get("verdict_citations") or []
    hids = sorted(c["hid"] for c in citations)
    assert hids == ["H1", "H3"]
    for c in citations:
        assert c["name_cn"]
        assert c["track_label"]


def test_load_paper_detail_result_includes_verdict_citations_for_h6_paper() -> None:
    """shleifer_1986 is cited only by H6."""
    result = dashboard_content.load_paper_detail_result(
        "shleifer_1986",
        render_table=_render_table_stub,
        project_module_display_map=PROJECT_MODULE_DISPLAY,
    )
    assert result is not None
    citations = result.get("verdict_citations") or []
    assert [c["hid"] for c in citations] == ["H6"]
