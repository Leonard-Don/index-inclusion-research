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
    assert "稳健性检查" in html
    assert 'aria-current="page"' in html
    assert '>完整材料</a>' in html
    assert 'data-mode-link' in html


def test_home_dashboard_keeps_mode_tabs_and_refresh_anchor_logic(monkeypatch) -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=demo")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'data-section-link' in html
    assert 'data-section-key="framework"' in html
    assert 'data-section-key="supplement"' in html
    assert 'data-section-key="robustness"' not in html
    assert 'data-anchor-input' in html
    assert 'data-base-href="/?mode=brief"' in html
    assert 'data-base-href="/?mode=demo"' in html
    assert 'data-base-href="/?mode=full"' in html

    monkeypatch.setattr(dashboard, "_run_and_cache_all", lambda: None)
    refreshed = client.post("/refresh?mode=full", data={"anchor": "framework"})
    assert refreshed.status_code == 302
    assert refreshed.headers["Location"].endswith("/?mode=full#framework")

    brief_refreshed = client.post("/refresh?mode=brief", data={"anchor": "framework"})
    assert brief_refreshed.status_code == 302
    assert brief_refreshed.headers["Location"].endswith("/?mode=brief#tracks")


def test_home_dashboard_supports_three_minute_mode() -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=brief")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "3 分钟汇报" in html
    assert "3 分钟汇报模式下" in html
    assert "核心研究结论" in html
    assert "三条主线，对应三类核心问题" in html
    assert "支撑文献" not in html
    assert 'data-section-key="framework"' not in html
    assert 'data-section-key="supplement"' not in html
    assert 'data-allowed-hashes="#overview,#design,#tracks,#limits,#price_pressure_track,#demand_curve_track,#identification_china_track"' in html
    assert "页面将真实样本、三条研究主线与研究边界压缩为一套适合快速汇报的展示材料" in html
    assert "页面同步呈现主线结果、文献框架与机制补充" not in html
    assert "稳健性检查" not in html


def test_highlights_copy_stays_consistent_with_current_cn_effective_results() -> None:
    highlights = dashboard._build_highlights()
    discussion = next(item for item in highlights if item["label"] == "最值得讨论")
    assert "但统计上并不显著" in discussion["copy"]
    assert "[0,+120] 窗口下调入与调出的 CAR 差异达到" in discussion["copy"]
    assert "且统计显著。这说明 A 股市场不能机械套用美股的经典指数纳入叙事。" not in discussion["copy"]


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
    assert "<built-in method copy" not in html
    assert "把指数效应首先解释成短期价格压力" in html
    assert "研究模块" not in html
    assert "文献页面" not in html

    pdf_response = client.get("/paper/harris_gurel_1986/pdf")
    assert pdf_response.status_code == 200


def test_legacy_secondary_routes_redirect_to_single_frontend_anchors() -> None:
    client = dashboard.app.test_client()
    redirects = {
        "/library": "/#framework",
        "/review": "/#framework",
        "/framework": "/#framework",
        "/supplement": "/#supplement",
        "/analysis/price_pressure_track": "/#price_pressure_track",
        "/analysis/demand_curve_track": "/#demand_curve_track",
        "/analysis/identification_china_track": "/#identification_china_track",
    }
    for route, target in redirects.items():
        response = client.get(route)
        assert response.status_code == 302
        assert response.headers["Location"].endswith(target)


def test_old_app_template_has_been_removed() -> None:
    assert not hasattr(dashboard, "APP_TEMPLATE")
