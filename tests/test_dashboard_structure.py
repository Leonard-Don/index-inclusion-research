from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import start_literature_dashboard as dashboard


def test_dashboard_uses_three_track_entrypoints() -> None:
    assert set(dashboard.ANALYSES) == {
        "price_pressure_track",
        "demand_curve_track",
        "identification_china_track",
    }


def test_dashboard_does_not_expose_legacy_third_paper_copy() -> None:
    identification = dashboard.ANALYSES["identification_china_track"]
    assert identification["title"] == "制度识别与中国市场证据"
    assert "第三篇" not in identification["description_zh"]

    saved = dashboard._load_identification_china_saved_result()
    assert saved["id"] == "identification_china_track"
    assert "第三篇论文结果包" not in saved["summary_text"]


def test_dashboard_exposes_framework_page() -> None:
    assert dashboard.FRAMEWORK_CARD["title"] == "研究框架"
    framework = dashboard._load_literature_framework_result()
    assert framework["id"] == "paper_framework"
    assert "五大阵营" in framework["summary_text"]
    assert framework["summary_cards"]
    assert any(label == "五大阵营概览" for label, _ in framework["rendered_tables"])


def test_dashboard_exposes_supplement_page() -> None:
    assert dashboard.SUPPLEMENT_CARD["title"] == "机制与执行补充"
    supplement = dashboard._load_supplement_result()
    assert supplement["id"] == "project_supplement"
    assert "投研汇报" in supplement["summary_text"]
    assert any(label == "事件时钟" for label, _ in supplement["rendered_tables"])


def test_dashboard_exposes_review_and_library_deep_cards() -> None:
    review = dashboard._load_literature_review_result()
    library = dashboard._load_literature_library_result()
    assert review["summary_cards"]
    assert library["summary_cards"]


def test_home_dashboard_renders_single_frontend_sections() -> None:
    client = dashboard.app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "16 篇文献、真实样本与识别设计：指数纳入效应的综合证据" in html
    assert "三条主线，对应三类核心问题" in html
    assert "16 篇文献在同一条研究链条中的位置" in html
    assert "把事件研究结果放回交易机制与执行场景" in html
    assert "展示版" in html
    assert "支撑文献" in html
    assert "文献讲义" in html
    assert "识别对象" in html
    assert "挑战的假设" in html
    assert "争论推进" in html
    assert "<built-in method copy of dict object" not in html


def test_home_dashboard_supports_full_mode() -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=full")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "完整材料" in html


def test_paper_route_now_renders_brief_before_pdf() -> None:
    client = dashboard.app.test_client()
    response = client.get("/paper/harris_gurel_1986")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "<title>Lawrence Harris 等（1986）｜指数纳入效应研究界面</title>" in html
    assert "单篇文献讲义" in html
    assert "核心解读" in html
    assert "研究路径" in html
    assert "这篇论文在 16 篇链条中的位置" in html
    assert "结构化信息" in html
    assert "论文信息与深度解读" in html
    assert "返回文献库" in html
    assert "打开原文 PDF" in html
    assert "识别对象" in html
    assert "挑战的假设" in html
    assert "争论推进" in html
    assert "前一篇" in html
    assert "后一篇" in html
    assert "推荐下一篇" in html
    assert "完整文献演进导航" in html
    assert "01 ·" in html
    assert "按阵营" in html
    assert "按主线" in html
    assert "按立场" in html
    assert "短期价格压力与效应减弱" in html or "需求曲线与长期保留" in html or "制度识别与中国市场证据" in html
    assert "研究模块" not in html
    assert "文献页面" not in html

    pdf_response = client.get("/paper/harris_gurel_1986/pdf")
    assert pdf_response.status_code == 200


def test_framework_route_uses_specific_page_title() -> None:
    client = dashboard.app.test_client()
    response = client.get("/framework")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "<title>研究框架｜指数纳入效应研究界面</title>" in html
