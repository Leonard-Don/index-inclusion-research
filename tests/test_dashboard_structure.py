from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import index_inclusion_research.dashboard_app as dashboard
from index_inclusion_research.literature_dashboard import parse_dashboard_args


def _reset_refresh_state() -> None:
    with dashboard.REFRESH_LOCK:
        dashboard.REFRESH_STATE.update(
            {
                "status": "idle",
                "message": "页面已就绪，刷新完成后会自动同步本次更新。",
                "scope_label": "全部材料",
                "scope_key": "all",
                "started_at": "",
                "finished_at": "",
                "started_ts": 0.0,
                "finished_ts": 0.0,
                "error": "",
                "snapshot_label": "",
                "snapshot_copy": "",
                "snapshot_source_path": "",
                "snapshot_source_count": 0,
                "contract_status_label": "",
                "contract_status_copy": "",
                "artifact_summary_label": "",
                "artifact_summary_copy": "",
                "updated_artifacts": [],
                "baseline_artifact_mtimes": {},
            }
        )


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

    saved = dashboard.runtime.load_identification_china_saved_result()
    assert saved["id"] == "identification_china_track"
    assert "第三篇论文结果包" not in saved["summary_text"]


def test_dashboard_reads_live_rdd_status_and_exposes_outputs_consistently() -> None:
    status = dashboard.runtime.load_rdd_status()
    saved = dashboard.runtime.load_identification_china_saved_result()
    labels = [label for label, _ in saved["rendered_tables"]]
    if status["mode"] in {"real", "reconstructed"}:
        assert any(label.startswith("断点回归：") for label in labels)
        assert any("hs300_rdd" in figure["path"] for figure in saved["figure_paths"])
    else:
        assert all(not label.startswith("断点回归：") for label in labels)
        assert all(
            "hs300_rdd" not in figure["path"] for figure in saved["figure_paths"]
        )


def test_identification_summary_copy_stays_consistent_with_live_rdd_state() -> None:
    status = dashboard.runtime.load_rdd_status()
    saved = dashboard.runtime.load_identification_china_saved_result()

    if status["mode"] in {"real", "reconstructed"}:
        assert (
            "当前项目还没有纳入真实 RD 所需的候选样本排名 running variable。"
            not in saved["summary_text"]
        )
        assert f"证据状态：`{status['evidence_status']}`" in saved["summary_text"]


def test_exported_identification_scope_matches_live_rdd_status() -> None:
    status = dashboard.runtime.load_rdd_status()
    identification_scope = pd.read_csv(
        ROOT / "results" / "real_tables" / "identification_scope.csv"
    )
    rdd_row = identification_scope.loc[
        identification_scope["分析层"] == "中国 RDD 扩展"
    ].iloc[0]

    assert "证据等级" in identification_scope.columns
    assert "来源摘要" in identification_scope.columns
    assert identification_scope["来源摘要"].notna().all()
    assert rdd_row["证据状态"] == status["evidence_status"]
    assert rdd_row["证据等级"] == status["evidence_tier"]


def test_results_manifest_matches_live_rdd_status() -> None:
    contract = dashboard.runtime.load_rdd_contract_check()

    assert contract["manifest_exists"] is True
    assert contract["manifest_profile"] == "real"
    assert contract["matches"] is True
    assert contract["mismatched_fields"] == []


def test_literature_result_package_summaries_use_repo_relative_paths() -> None:
    summary_paths = [
        ROOT / "results" / "literature" / "harris_gurel" / "summary.md",
        ROOT / "results" / "literature" / "shleifer" / "summary.md",
        ROOT / "results" / "literature" / "hs300_style" / "summary.md",
    ]

    for path in summary_paths:
        assert str(ROOT) not in path.read_text(encoding="utf-8")


def test_dashboard_exposes_framework_page() -> None:
    assert dashboard.FRAMEWORK_CARD["title"] == "文献框架"
    framework = dashboard.runtime.load_literature_framework_result()
    assert framework["id"] == "paper_framework"
    assert "五大阵营" in framework["summary_text"]
    assert framework["summary_cards"]
    assert any(label == "五大阵营概览" for label, _ in framework["rendered_tables"])


def test_dashboard_exposes_supplement_page() -> None:
    assert dashboard.SUPPLEMENT_CARD["title"] == "机制与执行补充"
    supplement = dashboard.runtime.load_supplement_result()
    assert supplement["id"] == "project_supplement"
    assert "投研汇报" in supplement["summary_text"]
    assert any(label == "事件时钟" for label, _ in supplement["rendered_tables"])


def test_dashboard_cli_parser_accepts_host_and_port() -> None:
    args = parse_dashboard_args(["--host", "0.0.0.0", "--port", "5010"])
    assert args.host == "0.0.0.0"
    assert args.port == 5010


def test_dashboard_exposes_review_and_library_deep_cards() -> None:
    review = dashboard.runtime.load_literature_review_result()
    library = dashboard.runtime.load_literature_library_result()
    assert review["summary_cards"]
    assert library["summary_cards"]


def test_home_dashboard_renders_single_frontend_sections() -> None:
    client = dashboard.app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    status = dashboard.runtime.load_rdd_status()
    # Structural smoke for hero / tracks / mechanism sections — assert on
    # core noun phrases instead of full sentences so copy edits don't
    # routinely break this test.
    assert "16 篇文献" in html
    assert "效应被重估" in html
    assert "三条主线" in html
    assert "研究链" in html
    assert "交易机制" in html
    assert "展示版" in html
    assert "支撑文献" in html
    assert "查看这篇速读" in html
    assert "识别对象" in html
    assert "挑战的假设" in html
    assert "争论推进" in html
    assert "最近结果" in html
    assert "本次更新" in html
    assert "最近结果概览" in html
    assert "更新明细" in html
    assert "识别层级" in html
    assert "状态一致性" in html
    assert "结果状态已同步" in html
    assert dashboard.runtime.load_rdd_status()["evidence_status"] in html
    assert dashboard.runtime.load_rdd_status()["source_label"] in html
    assert "results/real_tables/results_manifest.csv" in html
    assert 'width="1600"' not in html
    assert 'height="1000"' not in html
    assert 'width="2684"' in html
    assert 'height="1056"' in html
    assert "中国 RDD" in html
    assert (
        f"L{3 if status['mode'] == 'real' else 2 if status['mode'] == 'reconstructed' else 1 if status['mode'] == 'demo' else 0}"
        in html
    )
    assert "built-in method copy of dict object" not in html


def test_verdict_redirect_route_lands_on_dashboard_anchor() -> None:
    """/verdict/<hid> for a valid hid should 302-redirect to the
    home-page verdict-card anchor."""
    client = dashboard.app.test_client()
    response = client.get("/verdict/H1", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith("#hypothesis-H1")
    assert "/?mode=full" in response.headers["Location"]


def test_verdict_redirect_route_returns_404_for_unknown_hid() -> None:
    client = dashboard.app.test_client()
    response = client.get("/verdict/H99", follow_redirects=False)
    assert response.status_code == 404


def test_home_dashboard_full_mode_renders_verdict_card_anchor_ids() -> None:
    """Each verdict card should carry id="hypothesis-{hid}" so the
    /verdict/<hid> redirect lands on the right card."""
    client = dashboard.app.test_client()
    response = client.get("/?mode=full")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7"):
        assert f'id="hypothesis-{hid}"' in html


def test_home_dashboard_full_mode_marks_cma_evidence_tiers() -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=full")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'data-filter-tier="core"' in html
    assert 'data-filter-tier="supplementary"' in html
    assert "正文主表层级" in html
    assert "附录层级" in html
    assert html.count('data-evidence-tier="core"') >= 6
    assert html.count('data-evidence-tier="supplementary"') >= 8
    for hid in ("H1", "H5", "H7"):
        marker = f'id="hypothesis-{hid}"'
        card_start = html.index(marker)
        card_end = html.index("</article>", card_start)
        assert 'data-evidence-tier="core"' in html[card_start:card_end]
        assert "正文可引用" in html[card_start:card_end]
    for hid in ("H2", "H3", "H4", "H6"):
        marker = f'id="hypothesis-{hid}"'
        card_start = html.index(marker)
        card_end = html.index("</article>", card_start)
        assert 'data-evidence-tier="supplementary"' in html[card_start:card_end]
        assert "附录/探索性" in html[card_start:card_end]


def test_home_dashboard_supports_full_mode() -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=full")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "完整材料" in html
    assert "稳健性检查" in html
    assert 'aria-current="page"' in html
    assert ">完整材料</a>" in html
    assert "data-mode-link" in html


def test_home_dashboard_demo_mode_collapses_secondary_material_and_marks_lazy_media() -> (
    None
):
    client = dashboard.app.test_client()
    response = client.get("/?mode=demo")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'href="/static/dashboard.css?v=' in html
    assert 'src="/static/dashboard.js?v=' in html
    assert 'type="module"' in html
    assert 'data-refresh-status-url="/refresh/status"' in html
    assert "data-refresh-form" in html
    assert "data-refresh-panel" in html
    assert "data-refresh-state-label" in html
    assert "data-refresh-scope-label" in html
    assert "data-refresh-artifact-list" in html
    assert "只刷新本主线" in html
    assert 'data-details-key="demo-design-detail-figures"' in html
    assert 'data-details-key="demo-framework-detail-tables"' in html
    assert "data-details-toggle" in html
    assert "data-waypoint-dock" in html
    assert "data-waypoint-menu" in html
    assert "data-waypoint-menu-toggle" in html
    assert "data-reading-progress" in html
    assert "data-waypoint-menu-link" in html
    assert 'data-waypoint-kind="track"' in html
    assert 'data-waypoint-parent="主线结果"' in html
    assert "data-waypoint-next" in html
    assert "data-waypoint-top" in html
    assert "waypoint-next-action" in html
    assert "waypoint-secondary-action" in html
    assert "样本设计补充表" in html
    assert "展开文献" in html
    assert 'loading="lazy"' in html
    assert 'fetchpriority="high"' in html


def test_dashboard_static_assets_are_served() -> None:
    client = dashboard.app.test_client()

    css_response = client.get("/static/dashboard.css")
    assert css_response.status_code == 200
    css = css_response.get_data(as_text=True)
    assert ".topbar" in css
    assert ".track-surface-meta" in css
    assert ".waypoint-dock" in css
    assert ".chapter-drawer" in css
    assert ".reading-progress" in css
    assert "#tracks" in css
    assert "scroll-margin-top: 84px;" in css
    assert "text-wrap: wrap;" in css
    assert "grid-template-columns: minmax(0, 0.94fr) minmax(340px, 1.06fr);" in css
    assert "#tracks > .section-head .section-side {" in css
    assert "#tracks > .section-head + .track {" in css
    assert "#tracks > .section-head + .track .track-meta {" in css
    assert "#tracks > .section-head + .track .track-takeaway {" in css
    assert "#tracks > .section-head + .track > .insight-strip .insight-card {" in css
    assert (
        "#tracks > .section-head + .track .track-surface-visual .figure-stack {" in css
    )
    assert "#tracks > .section-head + .track .thumb img {" in css
    assert ".track-surface-meta {\n        position: static;" in css
    assert "#design .details-panel summary {" in css
    assert "#design .details-panel .details-copy {" in css
    assert "#design .details-panel + .result-group {" in css
    assert "#design .details-panel .details-toggle {" in css

    js_response = client.get("/static/dashboard.js")
    assert js_response.status_code == 200
    js = js_response.get_data(as_text=True)
    assert 'from "./dashboard/bootstrap.js"' in js
    assert "bootstrapDashboard" in js

    bootstrap_response = client.get("/static/dashboard/bootstrap.js")
    assert bootstrap_response.status_code == 200
    bootstrap_js = bootstrap_response.get_data(as_text=True)
    assert 'from "./context.js"' in bootstrap_js
    assert 'from "./surface.js"' in bootstrap_js
    assert 'from "./navigation.js"' in bootstrap_js
    assert 'from "./refresh.js"' in bootstrap_js
    assert "bootstrapDashboard" in bootstrap_js

    context_response = client.get("/static/dashboard/context.js")
    assert context_response.status_code == 200
    context_js = context_response.get_data(as_text=True)
    assert 'from "./context_groups.js"' in context_js
    assert "createDashboardContext" in context_js

    context_groups_response = client.get("/static/dashboard/context_groups.js")
    assert context_groups_response.status_code == 200
    context_groups_js = context_groups_response.get_data(as_text=True)
    assert "createNavigationContext" in context_groups_js
    assert "createRefreshContext" in context_groups_js
    assert "createSurfaceContext" in context_groups_js
    assert "createRuntimeContext" in context_groups_js

    surface_response = client.get("/static/dashboard/surface.js")
    assert surface_response.status_code == 200
    surface_js = surface_response.get_data(as_text=True)
    assert 'from "./surface_details.js"' in surface_js
    assert 'from "./surface_tables.js"' in surface_js
    assert "createSurfaceController" in surface_js

    surface_details_response = client.get("/static/dashboard/surface_details.js")
    assert surface_details_response.status_code == 200
    surface_details_js = surface_details_response.get_data(as_text=True)
    assert "createDetailsSurface" in surface_details_js
    assert "shouldCarryDetailsState" in surface_details_js

    surface_tables_response = client.get("/static/dashboard/surface_tables.js")
    assert surface_tables_response.status_code == 200
    surface_tables_js = surface_tables_response.get_data(as_text=True)
    assert "createTableSurface" in surface_tables_js
    assert "syncAllTableWraps" in surface_tables_js

    navigation_response = client.get("/static/dashboard/navigation.js")
    assert navigation_response.status_code == 200
    navigation_js = navigation_response.get_data(as_text=True)
    assert 'from "./navigation_helpers.js"' in navigation_js
    assert 'from "./navigation_ui.js"' in navigation_js
    assert "createNavigationController" in navigation_js

    navigation_helpers_response = client.get("/static/dashboard/navigation_helpers.js")
    assert navigation_helpers_response.status_code == 200
    navigation_helpers_js = navigation_helpers_response.get_data(as_text=True)
    assert "collectWaypoints" in navigation_helpers_js
    assert "normalizeHashForAllowedSet" in navigation_helpers_js

    navigation_ui_response = client.get("/static/dashboard/navigation_ui.js")
    assert navigation_ui_response.status_code == 200
    navigation_ui_js = navigation_ui_response.get_data(as_text=True)
    assert "setWaypointMenuOpen" in navigation_ui_js
    assert "updateWaypointDock" in navigation_ui_js

    refresh_response = client.get("/static/dashboard/refresh.js")
    assert refresh_response.status_code == 200
    refresh_js = refresh_response.get_data(as_text=True)
    assert 'from "./refresh_requests.js"' in refresh_js
    assert 'from "./refresh_presenter.js"' in refresh_js
    assert "pollRefreshStatus" in refresh_js

    refresh_requests_response = client.get("/static/dashboard/refresh_requests.js")
    assert refresh_requests_response.status_code == 200
    refresh_requests_js = refresh_requests_response.get_data(as_text=True)
    assert "fetchRefreshStatus" in refresh_requests_js
    assert "postRefreshRequest" in refresh_requests_js

    refresh_presenter_response = client.get("/static/dashboard/refresh_presenter.js")
    assert refresh_presenter_response.status_code == 200
    refresh_presenter_js = refresh_presenter_response.get_data(as_text=True)
    assert "applyRefreshStateToDom" in refresh_presenter_js
    assert "renderRefreshArtifacts" in refresh_presenter_js


def test_dashboard_template_uses_shared_section_and_figure_macros() -> None:
    macros = (ROOT / "src" / "index_inclusion_research" / "web" / "templates" / "_dashboard_macros.html").read_text(
        encoding="utf-8"
    )
    shared_macros = (
        ROOT / "src" / "index_inclusion_research" / "web" / "templates" / "_dashboard_shared_macros.html"
    ).read_text(encoding="utf-8")
    overview_macros = (
        ROOT / "src" / "index_inclusion_research" / "web" / "templates" / "_dashboard_overview_macros.html"
    ).read_text(encoding="utf-8")
    content_macros = (
        ROOT / "src" / "index_inclusion_research" / "web" / "templates" / "_dashboard_content_macros.html"
    ).read_text(encoding="utf-8")
    dashboard_template = (ROOT / "src" / "index_inclusion_research" / "web" / "templates" / "dashboard.html").read_text(
        encoding="utf-8"
    )

    assert (
        '{% import "_dashboard_shared_macros.html" as shared with context %}' in macros
    )
    assert (
        '{% import "_dashboard_overview_macros.html" as overview with context %}'
        in macros
    )
    assert (
        '{% import "_dashboard_content_macros.html" as content with context %}'
        in macros
    )
    assert "macro render_summary_cards" in shared_macros
    assert "macro render_details_panel" in shared_macros
    assert "macro render_hero_section" in overview_macros
    assert "macro render_waypoint_navigation" in overview_macros
    assert "macro render_design_section" in content_macros
    assert "macro render_track_section" in content_macros
    assert "macro render_table_suite_section" in content_macros
    assert "macro render_topbar" in macros
    assert "macro render_section_head" in macros
    assert "macro render_abstract_panel" in macros
    assert "macro render_figure_card" in macros
    assert "macro render_figure_cards" in macros
    assert "macro render_hero_metrics" in macros
    assert "macro render_highlight_grid" in macros
    assert "macro render_refresh_form" in macros
    assert "macro render_refresh_status_panel" in macros
    assert "macro render_hero_section" in macros
    assert "macro render_utility_bar" in macros
    assert "macro render_core_findings_section" in macros
    assert "macro render_overview_context" in macros
    assert "macro render_design_section" in macros
    assert "macro render_framework_section" in macros
    assert "macro render_supplement_section" in macros
    assert "macro render_robustness_section" in macros
    assert "macro render_limits_section" in macros
    assert "macro render_paper_audit_section" in macros
    assert "macro render_insight_strip" in macros
    assert "macro render_status_panel" in macros
    assert "macro render_collapsible_figure_group" in macros
    assert "macro render_collapsible_table_group" in macros
    assert "macro render_collapsible_support_group" in macros
    assert "macro render_section_table_suite" in macros
    assert "macro render_table_suite_section" in macros
    assert "macro render_track_meta" in macros
    assert "macro render_track_section" in macros
    assert "macro render_track_panels" in macros
    assert "macro render_tracks_section" in macros
    assert "macro render_cta_strip" in macros
    assert "macro render_mode_hint" in macros
    assert "macro render_support_band" in macros
    assert "macro render_waypoint_navigation" in macros
    assert (
        '{% import "_dashboard_macros.html" as ui with context %}' in dashboard_template
    )
    assert "ui.render_topbar(" in dashboard_template
    assert "ui.render_hero_section(" in dashboard_template
    assert "ui.render_core_findings_section(" in dashboard_template
    assert "ui.render_utility_bar(" in dashboard_template
    assert "ui.render_overview_context(" in dashboard_template
    assert "ui.render_design_section(" in dashboard_template
    assert "ui.render_framework_section(" in dashboard_template
    assert "ui.render_supplement_section(" in dashboard_template
    assert "ui.render_robustness_section(" in dashboard_template
    assert "ui.render_limits_section(" in dashboard_template
    assert "ui.render_paper_audit_section(" in dashboard_template
    assert "ui.render_tracks_section(" in dashboard_template
    assert "ui.render_cta_strip(" in dashboard_template
    assert "ui.render_waypoint_navigation(" in dashboard_template


def test_home_dashboard_keeps_mode_tabs_and_refresh_anchor_logic(monkeypatch) -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=demo")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "data-section-link" in html
    assert 'data-section-key="framework"' in html
    assert 'data-section-key="supplement"' in html
    assert 'data-section-key="robustness"' not in html
    assert "data-anchor-input" in html
    assert 'data-base-href="/?mode=brief"' in html
    assert 'data-base-href="/?mode=demo"' in html
    assert 'data-base-href="/?mode=full"' in html

    monkeypatch.setattr(dashboard.services, "run_and_cache_all", lambda: None)
    refreshed = client.post("/refresh?mode=full", data={"anchor": "framework"})
    assert refreshed.status_code == 302
    assert refreshed.headers["Location"].endswith("/?mode=full#framework")

    brief_refreshed = client.post("/refresh?mode=brief", data={"anchor": "framework"})
    assert brief_refreshed.status_code == 302
    assert brief_refreshed.headers["Location"].endswith("/?mode=brief#tracks")


def test_home_dashboard_async_refresh_returns_json_status(monkeypatch) -> None:
    client = dashboard.app.test_client()
    _reset_refresh_state()
    monkeypatch.setattr(
        dashboard.services,
        "spawn_refresh_worker",
        lambda runner, scope_label, scope_key: None,
    )

    response = client.post(
        "/refresh?mode=demo",
        data={"anchor": "framework"},
        headers={"X-Requested-With": "fetch", "Accept": "application/json"},
    )

    payload = response.get_json()
    assert response.status_code == 202
    assert payload["accepted"] is True
    assert payload["status"] == "running"
    assert payload["redirect_url"] == ""
    assert payload["scope_key"] == "all"
    assert payload["started_ts"] > 0
    assert payload["duration_seconds"] == 0
    assert payload["poll_after_ms"] == 1200

    status_response = client.get("/refresh/status?mode=demo&anchor=framework")
    status_payload = status_response.get_json()
    assert status_response.status_code == 200
    assert status_payload["status"] == "running"
    assert status_payload["poll_after_ms"] == 1200
    _reset_refresh_state()


def test_track_level_async_refresh_returns_analysis_scope(monkeypatch) -> None:
    client = dashboard.app.test_client()
    _reset_refresh_state()
    monkeypatch.setattr(
        dashboard.services,
        "spawn_refresh_worker",
        lambda runner, scope_label, scope_key: None,
    )

    response = client.post(
        "/run/price_pressure_track?mode=demo",
        data={"anchor": "price_pressure_track"},
        headers={"X-Requested-With": "fetch", "Accept": "application/json"},
    )

    payload = response.get_json()
    assert response.status_code == 202
    assert payload["accepted"] is True
    assert payload["status"] == "running"
    assert payload["scope_label"] == "短期价格压力与效应减弱"
    assert payload["scope_key"] == "price_pressure_track"
    assert "短期价格压力与效应减弱" in payload["message"]
    _reset_refresh_state()


def test_track_level_refresh_fallback_redirects_to_track_anchor(monkeypatch) -> None:
    client = dashboard.app.test_client()
    monkeypatch.setattr(
        dashboard.services, "run_and_cache_analysis", lambda analysis_id: {}
    )

    response = client.post(
        "/run/demand_curve_track?mode=demo", data={"anchor": "demand_curve_track"}
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/?mode=demo#demand_curve_track")


def test_refresh_status_exposes_redirect_after_success() -> None:
    client = dashboard.app.test_client()
    with dashboard.REFRESH_LOCK:
        dashboard.REFRESH_STATE.update(
            {
                "status": "succeeded",
                "message": "刷新完成。",
                "scope_label": "全部材料",
                "scope_key": "all",
                "started_at": "2026-04-16 10:00",
                "finished_at": "2026-04-16 10:01",
                "started_ts": 100.0,
                "finished_ts": 160.0,
                "error": "",
                "snapshot_label": "2026-04-16 10:01",
                "snapshot_copy": "copy",
                "snapshot_source_path": "results/real_tables/event_study_summary.csv",
                "snapshot_source_count": 1,
                "contract_status_label": "",
                "contract_status_copy": "",
                "artifact_summary_label": "",
                "artifact_summary_copy": "",
                "updated_artifacts": [
                    {
                        "path": "results/real_tables/event_study_summary.csv",
                        "modified_at": "2026-04-16 10:01",
                    }
                ],
                "baseline_artifact_mtimes": {},
            }
        )

    response = client.get("/refresh/status?mode=demo&anchor=supplement")
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["status"] == "succeeded"
    assert payload["redirect_url"].endswith("/?mode=demo#supplement")
    assert payload["duration_seconds"] == 60
    assert (
        payload["updated_artifacts"][0]["path"]
        == "results/real_tables/event_study_summary.csv"
    )
    _reset_refresh_state()


def test_refresh_status_slows_polling_for_long_running_jobs(monkeypatch) -> None:
    client = dashboard.app.test_client()
    with dashboard.REFRESH_LOCK:
        dashboard.REFRESH_STATE.update(
            {
                "status": "running",
                "message": "正在刷新结果文件。",
                "scope_label": "全部材料",
                "scope_key": "all",
                "started_at": "2026-04-16 10:00",
                "finished_at": "",
                "started_ts": 100.0,
                "finished_ts": 0.0,
                "error": "",
                "snapshot_label": "2026-04-16 09:59",
                "snapshot_copy": "copy",
                "snapshot_source_path": "results/real_tables/event_study_summary.csv",
                "snapshot_source_count": 1,
                "contract_status_label": "",
                "contract_status_copy": "",
                "artifact_summary_label": "",
                "artifact_summary_copy": "",
                "updated_artifacts": [],
                "baseline_artifact_mtimes": {},
            }
        )

    monkeypatch.setattr(dashboard.time, "time", lambda: 160.0)
    response = client.get("/refresh/status?mode=demo&anchor=overview")
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["status"] == "running"
    assert payload["duration_seconds"] == 60
    assert payload["poll_after_ms"] == 5000
    _reset_refresh_state()


def test_home_dashboard_supports_three_minute_mode() -> None:
    client = dashboard.app.test_client()
    response = client.get("/?mode=brief")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "3 分钟汇报" in html
    assert "3 分钟汇报只保留样本摘要" in html
    assert "核心结论" in html
    assert "三条主线，分别回答三个核心问题" in html
    assert "支撑文献" not in html
    assert 'data-section-key="framework"' not in html
    assert 'data-section-key="supplement"' not in html
    assert (
        'data-allowed-hashes="#overview,#design,#tracks,#limits,#cross_market_asymmetry,#price_pressure_track,#demand_curve_track,#identification_china_track"'
        in html
    )
    assert "这一模式把真实样本、三条主线与研究边界压缩到一页里" in html
    assert (
        "页面同步呈现主线结果、文献框架与机制补充，便于在同一叙述里完成现象、机制与识别的说明"
        not in html
    )
    assert "稳健性检查" not in html


def test_highlights_copy_stays_consistent_with_current_cn_effective_results() -> None:
    highlights = dashboard.runtime.build_highlights()
    discussion = next(item for item in highlights if item["label"] == "最值得讨论")
    assert "但统计上并不显著" in discussion["copy"]
    assert "[0,+120] 窗口下调入与调出的 CAR 差异达到" in discussion["copy"]
    assert (
        "且统计显著。这说明 A 股市场不能机械套用美股的经典指数纳入叙事。"
        not in discussion["copy"]
    )
    assert "制度摩擦更强" in discussion["copy"]


def test_paper_route_now_renders_brief_before_pdf() -> None:
    client = dashboard.app.test_client()
    response = client.get("/paper/harris_gurel_1986")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'href="/static/paper.css"' in html
    assert 'src="/static/paper.js"' in html
    assert "<title>Lawrence Harris 等（1986）｜指数纳入效应研究界面</title>" in html
    assert "单篇文献速读" in html
    assert "核心解读" in html
    assert "文献链" in html
    assert "这篇论文在文献链中的位置" in html
    assert "结构化信息" in html
    assert "论文信息与深度解读" in html
    assert "首页总览" in html
    assert "返回文献框架" in html
    assert "查看原文 PDF" in html
    assert "识别对象" in html
    assert "挑战的假设" in html
    assert "争论推进" in html
    assert "前一篇" in html
    assert "后一篇" in html
    assert "当前这篇" in html
    assert "同主线延伸" in html or "同阵营延伸" in html or "跨主线参照" in html
    assert "回看上一环" in html
    assert "继续下一环" in html
    assert "查看相关论文" in html
    assert "文献链导航" in html
    assert "默认展开当前所在分组" in html
    assert "效应重估、价格发现或中国制度场景" in html
    assert "按阵营最适合看争论如何推进" in html
    assert "如果你想回看当前这条争论是从哪里起步的" in html
    assert '<details class="evolution-group"' in html
    assert "当前文献所在分组" in html
    assert "01 ·" in html
    assert "按阵营" in html
    assert "按主线" in html
    assert "按立场" in html
    assert (
        "短期价格压力与效应减弱" in html
        or "需求曲线与长期保留" in html
        or "制度识别与中国市场证据" in html
    )
    assert "built-in method copy" not in html
    assert "公告后股价立即上涨逾 3%" in html
    assert "研究模块" not in html
    assert "文献页面" not in html

    pdf_response = client.get("/paper/harris_gurel_1986/pdf")
    assert pdf_response.status_code == 200


def test_paper_static_assets_are_served() -> None:
    client = dashboard.app.test_client()

    css_response = client.get("/static/paper.css")
    assert css_response.status_code == 200
    css = css_response.get_data(as_text=True)
    assert ".hero-grid" in css
    assert ".evolution-nav-grid" in css

    js_response = client.get("/static/paper.js")
    assert js_response.status_code == 200
    js = js_response.get_data(as_text=True)
    assert 'document.querySelectorAll("[data-view-target]")' in js
    assert "panel.classList.toggle" in js


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


def test_old_underscore_compat_exports_have_been_removed() -> None:
    assert not hasattr(dashboard, "_load_identification_china_saved_result")
    assert not hasattr(dashboard, "_build_highlights")
    assert not hasattr(dashboard, "_build_dashboard_snapshot_meta")


def test_dashboard_snapshot_meta_uses_latest_available_file(
    tmp_path: Path, monkeypatch
) -> None:
    older = tmp_path / "older.csv"
    newer = tmp_path / "newer.md"
    older.write_text("older\n", encoding="utf-8")
    newer.write_text("newer\n", encoding="utf-8")
    os.utime(older, (1_700_000_000, 1_700_000_000))
    os.utime(newer, (1_800_000_000, 1_800_000_000))

    meta = dashboard.runtime.build_dashboard_snapshot_meta([older, newer])

    assert meta["source_count"] == 2
    assert meta["source_path"] == str(newer)
    assert meta["label"].startswith("2027-")
