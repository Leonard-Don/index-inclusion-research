from __future__ import annotations

from index_inclusion_research import dashboard_presenters


def test_nav_sections_for_modes() -> None:
    brief = dashboard_presenters.nav_sections_for_mode("brief")
    demo = dashboard_presenters.nav_sections_for_mode("demo")
    full = dashboard_presenters.nav_sections_for_mode("full")

    assert [item["anchor"] for item in brief] == ["overview", "design", "tracks", "limits"]
    assert [item["anchor"] for item in demo] == [
        "overview",
        "design",
        "tracks",
        "framework",
        "supplement",
        "limits",
    ]
    assert [item["anchor"] for item in full] == [
        "overview",
        "design",
        "tracks",
        "framework",
        "supplement",
        "robustness",
        "limits",
    ]


def test_mode_tabs_for_mode_uses_url_builder_and_allowed_hashes() -> None:
    tabs = dashboard_presenters.mode_tabs_for_mode(
        "demo",
        lambda mode, anchor=None: f"/?mode={mode}" if anchor is None else f"/?mode={mode}#{anchor}",
    )

    assert [tab["mode"] for tab in tabs] == ["brief", "demo", "full"]
    assert tabs[1]["active"] is True
    assert tabs[1]["base_href"] == "/?mode=demo"
    assert tabs[1]["href"] == "/?mode=demo#overview"
    assert "#framework" in tabs[1]["allowed_hashes"]
    assert "#robustness" not in tabs[1]["allowed_hashes"]
    assert "#robustness" in tabs[2]["allowed_hashes"]


def test_prepare_track_display_splits_tables_and_keeps_demo_note() -> None:
    section = {
        "summary_text": "原始说明",
        "figure_paths": [{"path": "results/foo.png", "caption": "已有图"}],
        "support_papers": [{"citation": "Example"}],
    }

    display = dashboard_presenters.prepare_track_display(
        section,
        "price_pressure_track",
        True,
        fallback_summary="回退摘要",
        result_cards_by_analysis={"price_pressure_track": [{"label": "结果", "value": "1", "copy": "说明"}]},
        curated_tables_by_analysis={
            "price_pressure_track": [
                ("短窗口 CAR 摘要", "<table>primary</table>"),
                ("保留率与回吐", "<table>detail</table>"),
            ]
        },
        extra_figures_by_analysis={
            "price_pressure_track": [{"path": "results/extra.png", "caption": "额外图"}]
        },
    )

    assert display["result_cards"][0]["label"] == "结果"
    assert "详细稳健性结果见完整材料。" in display["display_summary"]
    assert len(display["display_figures"]) == 2
    assert display["badge"] == "核心结果"
    assert len(display["primary_tables"]) == 2
    assert display["detail_tables"] == []
    assert display["display_support_papers"] == section["support_papers"]
    assert display["track_view"]["meta"]["refresh_button_label"] == "只刷新本主线"
    assert display["track_view"]["primary"]["key"] == "demo-price_pressure_track-primary-tables"
    assert display["track_view"]["detail"]["demo_title"] == "展开 本主线 的补充细表（0 张）"
    assert display["track_view"]["support_demo_title"] == "支撑文献（1 篇）"


def test_prepare_track_display_updates_identification_copy_for_reconstructed_status() -> None:
    display = dashboard_presenters.prepare_track_display(
        {
            "summary_text": "原始说明",
            "notes": [
                {"name": "阅读顺序", "copy": "旧阅读顺序"},
                {"name": "样本特征", "copy": "旧样本特征"},
            ],
        },
        "identification_china_track",
        False,
        fallback_summary="回退摘要",
        result_cards_by_analysis={},
        curated_tables_by_analysis={},
        extra_figures_by_analysis={},
        status_panel={
            "kicker": "证据等级",
            "title": "公开重建样本",
            "tone": "reconstructed",
            "signal_label": "识别层级",
            "signal_value": "L2 · 公开重建样本",
            "signal_copy": "公开重建说明",
            "copy": "状态说明",
            "meta": [],
        },
    )

    assert "L2 · 公开重建样本" in display["display_summary"]
    assert "公开数据版证据链" in display["display_summary"]
    assert "公开重建口径" in display["takeaway"]
    assert display["notes"][0]["copy"] == "重点看中国样本的事件研究与匹配结果，再对照证据等级卡和断点回归（RDD）摘要表。"
    assert "L2 · 公开重建样本" in display["notes"][1]["copy"]
    assert "公开重建口径" in display["notes"][1]["copy"]
    assert display["track_view"]["primary"]["key"] == "demo-identification_china_track-primary-tables"
    assert display["track_view"]["support_demo_title"] == "支撑文献（0 篇）"


def test_prepare_framework_display_orders_tables() -> None:
    section = {
        "rendered_tables": [
            ("研究表达框架", "<table>framework</table>"),
            ("文献演进总表", "<table>timeline</table>"),
            ("五大阵营概览", "<table>camp</table>"),
        ]
    }

    display = dashboard_presenters.prepare_framework_display(
        section,
        summary_cards=[{"title": "卡片", "kicker": "摘要", "copy": "说明"}],
    )

    assert [item["label"] for item in display["display_tables"]] == [
        "文献演进总表",
        "五大阵营概览",
        "研究表达框架",
    ]
    assert display["summary_cards"][0]["title"] == "卡片"
    assert display["section_view"]["head"]["section_id"] == "framework"
    assert display["section_view"]["primary"]["key"] == "demo-framework-primary-tables"
    assert display["section_view"]["detail"]["demo_title"] == "展开文献框架补充表（2 张）"


def test_prepare_supplement_display_assigns_section_view_contract() -> None:
    section = {
        "rendered_tables": [
            ("事件时钟", "<table>clock</table>"),
            ("机制链", "<table>chain</table>"),
            ("冲击估算步骤", "<table>steps</table>"),
            ("冲击估算示例", "<table>example</table>"),
            ("表达框架", "<table>frame</table>"),
        ]
    }

    display = dashboard_presenters.prepare_supplement_display(
        section,
        summary_cards=[{"title": "卡片", "kicker": "摘要", "copy": "说明"}],
    )

    assert [item["label"] for item in display["primary_tables"]] == ["事件时钟", "机制链"]
    assert [item["label"] for item in display["detail_tables"]] == ["冲击估算步骤", "冲击估算示例", "表达框架"]
    assert display["section_view"]["head"]["section_id"] == "supplement"
    assert display["section_view"]["primary"]["key"] == "demo-supplement-primary-tables"
    assert display["section_view"]["detail"]["demo_title"] == "展开机制补充表（3 张）"
