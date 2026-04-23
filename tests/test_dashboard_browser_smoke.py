from __future__ import annotations

import http.client
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import pytest

pytestmark = pytest.mark.browser_smoke

if os.environ.get("RUN_BROWSER_SMOKE") != "1":
    pytest.skip(
        "Set RUN_BROWSER_SMOKE=1 to run browser smoke tests.", allow_module_level=True
    )

playwright_sync_api = pytest.importorskip("playwright.sync_api")


ROOT = Path(__file__).resolve().parents[1]


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_dashboard(
    url: str, proc: subprocess.Popen[str], timeout_seconds: float = 60.0
) -> None:
    target = urlsplit(url)
    path = target.path or "/"
    if target.query:
        path = f"{path}?{target.query}"
    host = target.hostname or "127.0.0.1"
    port = int(target.port or 80)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if proc.poll() is not None:
            stdout, _ = proc.communicate(timeout=1)
            raise RuntimeError(
                f"Dashboard server exited before becoming ready.\n{stdout}"
            )
        try:
            connection = http.client.HTTPConnection(host, port, timeout=10)
            connection.request("GET", path)
            response = connection.getresponse()
            response.read()
            connection.close()
            if response.status in {200, 204}:
                return
        except (TimeoutError, ConnectionError, OSError, http.client.HTTPException):
            time.sleep(0.5)
    proc.terminate()
    stdout, _ = proc.communicate(timeout=5)
    raise RuntimeError(f"Dashboard server did not become ready in time.\n{stdout}")


def _wheel_until_hash(
    page: playwright_sync_api.Page, expected_hash: str, deltas: list[int]
) -> None:
    for delta in deltas:
        if page.evaluate("window.location.hash") == expected_hash:
            return
        page.mouse.wheel(0, delta)
        page.wait_for_timeout(500)
    page.wait_for_function(f"() => window.location.hash === '{expected_hash}'")


def _wait_for_section_state(
    page: playwright_sync_api.Page,
    expected_hash: str,
    expected_section: str,
    expected_waypoint: str | None = None,
) -> None:
    page.wait_for_function(
        """
        ([hash, section, waypoint]) => {
            const activeLink = document.querySelector("[data-section-link][aria-current='location']");
            const waypointTitle = document.querySelector("[data-waypoint-title]");
            const target = document.querySelector(hash);
            return (
                window.location.hash === hash &&
                activeLink?.textContent?.trim() === section &&
                waypointTitle?.textContent?.trim() === waypoint &&
                (!target || target.getBoundingClientRect().top <= 180)
            );
        }
        """,
        arg=[expected_hash, expected_section, expected_waypoint or expected_section],
    )


@contextmanager
def _running_dashboard_server() -> str:
    port = _find_free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "index_inclusion_research.literature_dashboard", "--port", str(port)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://localhost:{port}"
    try:
        _wait_for_dashboard(f"{base_url}/favicon.ico", proc)
        yield base_url
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)


def test_dashboard_browser_smoke() -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 960})
        page.on(
            "console",
            lambda msg: (
                console_errors.append(msg.text) if msg.type == "error" else None
            ),
        )
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        page.goto(f"{base_url}/?mode=demo", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        assert "16 篇文献" in page.locator("h1").inner_text()
        assert page.locator("a.skip-link").get_attribute("href") == "#main-content"
        assert (
            page.locator("[data-refresh-state-label]").inner_text().strip() == "已就绪"
        )
        assert (
            page.locator("[data-refresh-scope-label]").inner_text().strip()
            == "全部材料"
        )
        assert "核心文件" in page.locator("[data-refresh-snapshot-source]").inner_text()
        topbar_metrics = page.evaluate(
            """
            () => {
                const topbar = document.querySelector(".topbar");
                const brand = document.querySelector(".brand");
                const nav = document.querySelector(".nav-sections");
                const topbarRect = topbar?.getBoundingClientRect();
                const brandRect = brand?.getBoundingClientRect();
                const navRect = nav?.getBoundingClientRect();
                return {
                    topbarHeight: topbarRect?.height ?? 0,
                    brandWidth: brandRect?.width ?? 0,
                    navWidth: navRect?.width ?? 0,
                };
            }
            """
        )
        assert topbar_metrics["topbarHeight"] <= 74
        assert topbar_metrics["brandWidth"] <= 236
        assert topbar_metrics["navWidth"] >= 1140
        hero_metrics = page.evaluate(
            """
            () => {
                const topbar = document.querySelector(".topbar");
                const hero = document.querySelector(".hero");
                const heroCopy = document.querySelector(".hero-copy");
                const h1 = document.querySelector(".hero h1");
                const heroSide = document.querySelector(".hero-side");
                const topbarRect = topbar?.getBoundingClientRect();
                const heroRect = hero?.getBoundingClientRect();
                const heroCopyRect = heroCopy?.getBoundingClientRect();
                const h1Rect = h1?.getBoundingClientRect();
                const heroSideRect = heroSide?.getBoundingClientRect();
                return {
                    heroHeight: heroRect?.height ?? 0,
                    heroBottom: heroRect?.bottom ?? 0,
                    heroSideHeight: heroSideRect?.height ?? 0,
                    gapTopbarToH1: topbarRect && h1Rect ? h1Rect.top - topbarRect.bottom : 0,
                    heroCopyWidth: heroCopyRect?.width ?? 0,
                    heroSideWidth: heroSideRect?.width ?? 0,
                    h1Width: h1Rect?.width ?? 0,
                    heroCopyUnusedWidth: heroCopyRect && h1Rect ? heroCopyRect.width - h1Rect.width : 0,
                };
            }
            """
        )
        assert hero_metrics["heroHeight"] <= 700
        assert hero_metrics["heroBottom"] <= 780
        assert hero_metrics["heroSideHeight"] <= 680
        assert hero_metrics["gapTopbarToH1"] <= 60
        assert hero_metrics["heroSideWidth"] >= hero_metrics["heroCopyWidth"]
        assert hero_metrics["h1Width"] >= 390
        assert hero_metrics["heroCopyUnusedWidth"] <= 260
        post_hero_metrics = page.evaluate(
            """
            () => {
                const hero = document.querySelector(".hero");
                const utility = document.querySelector(".utility-bar");
                const core = document.querySelector("main .section");
                const coreHead = core?.querySelector(".section-head");
                const heroRect = hero?.getBoundingClientRect();
                const utilityRect = utility?.getBoundingClientRect();
                const coreHeadRect = coreHead?.getBoundingClientRect();
                const visibleWithinViewport = (rect) =>
                    rect ? Math.max(0, Math.min(window.innerHeight, rect.bottom) - Math.max(0, rect.top)) : 0;
                return {
                    gapHeroToUtility: heroRect && utilityRect ? utilityRect.top - heroRect.bottom : 0,
                    utilityHeight: utilityRect?.height ?? 0,
                    coreHeadTop: coreHeadRect?.top ?? 0,
                    coreHeadVisibleWithinViewport: visibleWithinViewport(coreHeadRect),
                };
            }
            """
        )
        assert post_hero_metrics["gapHeroToUtility"] <= 14
        assert post_hero_metrics["utilityHeight"] <= 98
        assert post_hero_metrics["coreHeadTop"] <= 900
        assert post_hero_metrics["coreHeadVisibleWithinViewport"] >= 80
        core_findings_metrics = page.evaluate(
            """
            () => {
                const core = document.querySelector("main .section");
                const coreHead = core?.querySelector(".section-head");
                const abstractPanel = core?.querySelector(".abstract-panel");
                const highlightGrid = core?.querySelector(".highlight-grid");
                const firstHighlight = core?.querySelector(".highlight");
                const visibleWithinViewport = (rect) =>
                    rect ? Math.max(0, Math.min(window.innerHeight, rect.bottom) - Math.max(0, rect.top)) : 0;
                const coreHeadRect = coreHead?.getBoundingClientRect();
                const abstractRect = abstractPanel?.getBoundingClientRect();
                const highlightRect = highlightGrid?.getBoundingClientRect();
                return {
                    abstractTop: abstractRect?.top ?? 0,
                    abstractHeight: abstractRect?.height ?? 0,
                    highlightHeight: highlightRect?.height ?? 0,
                    gapHeadToAbstract: abstractRect && coreHeadRect ? abstractRect.top - coreHeadRect.bottom : 0,
                    abstractVisibleWithinViewport: visibleWithinViewport(abstractRect),
                    firstHighlightMinHeight: firstHighlight ? window.getComputedStyle(firstHighlight).minHeight : "",
                    firstHighlightAlignContent: firstHighlight ? window.getComputedStyle(firstHighlight).alignContent : "",
                    firstHeadlineFontSize: firstHighlight?.querySelector(".headline")
                        ? window.getComputedStyle(firstHighlight.querySelector(".headline")).fontSize
                        : "",
                    firstCopyFontSize: firstHighlight?.querySelector(".copy")
                        ? window.getComputedStyle(firstHighlight.querySelector(".copy")).fontSize
                        : "",
                };
            }
            """
        )
        assert core_findings_metrics["abstractTop"] <= 992
        assert core_findings_metrics["abstractHeight"] <= 308
        assert core_findings_metrics["highlightHeight"] <= 200
        assert core_findings_metrics["gapHeadToAbstract"] <= 14
        assert core_findings_metrics["abstractVisibleWithinViewport"] >= 0
        assert core_findings_metrics["firstHighlightMinHeight"] == "166px"
        assert core_findings_metrics["firstHighlightAlignContent"] == "start"
        assert core_findings_metrics["firstHeadlineFontSize"] == "22px"
        assert core_findings_metrics["firstCopyFontSize"] == "13px"
        design_metrics = page.evaluate(
            """
            () => {
                const design = document.querySelector("#design");
                const head = design?.querySelector(".section-head");
                const summary = design?.querySelector(".section-summary-grid");
                const figurePanels = design?.querySelector(".library-panels");
                const primaryGroup = design?.querySelector(".result-group");
                const groupHead = primaryGroup?.querySelector(".result-group-head");
                const firstResultCard = design?.querySelector(".result-card");
                const firstImageShell = design?.querySelector(".figure-media-shell");
                const firstImage = design?.querySelector(".result-figure img");
                const headRect = head?.getBoundingClientRect();
                const summaryRect = summary?.getBoundingClientRect();
                const figureRect = figurePanels?.getBoundingClientRect();
                const groupRect = primaryGroup?.getBoundingClientRect();
                const groupHeadRect = groupHead?.getBoundingClientRect();
                const designRect = design?.getBoundingClientRect();
                return {
                    designHeight: designRect?.height ?? 0,
                    headHeight: headRect?.height ?? 0,
                    summaryHeight: summaryRect?.height ?? 0,
                    figurePanelsHeight: figureRect?.height ?? 0,
                    primaryGroupHeight: groupRect?.height ?? 0,
                    groupHeadHeight: groupHeadRect?.height ?? 0,
                    gapHeadToSummary: summaryRect && headRect ? summaryRect.top - headRect.bottom : 0,
                    gapSummaryToFigurePanels: figureRect && summaryRect ? figureRect.top - summaryRect.bottom : 0,
                    firstCardPadding: firstResultCard ? window.getComputedStyle(firstResultCard).padding : "",
                    firstCardRadius: firstResultCard ? window.getComputedStyle(firstResultCard).borderRadius : "",
                    firstImageShellPadding: firstImageShell ? window.getComputedStyle(firstImageShell).padding : "",
                    firstImageHeight: firstImage?.getBoundingClientRect().height ?? 0,
                    firstImageRadius: firstImage ? window.getComputedStyle(firstImage).borderRadius : "",
                };
            }
            """
        )
        assert design_metrics["designHeight"] <= 1940
        assert design_metrics["headHeight"] <= 108
        assert design_metrics["summaryHeight"] <= 210
        assert design_metrics["figurePanelsHeight"] <= 640
        assert design_metrics["primaryGroupHeight"] <= 525
        assert design_metrics["groupHeadHeight"] <= 43
        assert design_metrics["gapHeadToSummary"] <= 14
        assert design_metrics["gapSummaryToFigurePanels"] <= 14
        assert design_metrics["firstCardPadding"] == "12px"
        assert design_metrics["firstCardRadius"] == "16px"
        assert design_metrics["firstImageShellPadding"] == "8px"
        assert design_metrics["firstImageHeight"] <= 480
        assert design_metrics["firstImageRadius"] == "12px"
        design_supplement_metrics = page.evaluate(
            """
            () => {
                const design = document.querySelector("#design");
                const figurePanels = design?.querySelector(".library-panels");
                const detailFigures = document.querySelector("[data-details-key='demo-design-detail-figures']");
                const primaryExtra = document.querySelector("[data-details-key='demo-design-primary-tables']");
                const detailTables = document.querySelector("[data-details-key='demo-design-detail-tables']");
                const primaryGroup = design?.querySelector(".result-group");
                const detailSummary = detailTables?.querySelector("summary");
                return {
                    detailFiguresHeight: detailFigures?.getBoundingClientRect().height ?? 0,
                    primaryExtraHeight: primaryExtra?.getBoundingClientRect().height ?? 0,
                    detailTablesHeight: detailTables?.getBoundingClientRect().height ?? 0,
                    detailSummaryHeight: detailSummary?.getBoundingClientRect().height ?? 0,
                    gapFigurePanelsToDetailFigures:
                        figurePanels && detailFigures
                            ? detailFigures.getBoundingClientRect().top - figurePanels.getBoundingClientRect().bottom
                            : 0,
                    gapDetailFiguresToPrimary:
                        detailFigures && primaryGroup
                            ? primaryGroup.getBoundingClientRect().top - detailFigures.getBoundingClientRect().bottom
                            : 0,
                    gapPrimaryToPrimaryExtra:
                        primaryGroup && primaryExtra
                            ? primaryExtra.getBoundingClientRect().top - primaryGroup.getBoundingClientRect().bottom
                            : 0,
                    gapPrimaryExtraToDetailTables:
                        primaryExtra && detailTables
                            ? detailTables.getBoundingClientRect().top - primaryExtra.getBoundingClientRect().bottom
                            : 0,
                    detailFiguresSummaryPadding: detailFigures
                        ? window.getComputedStyle(detailFigures.querySelector("summary")).padding
                        : "",
                    detailTablesSummaryPadding: detailTables
                        ? window.getComputedStyle(detailTables.querySelector("summary")).padding
                        : "",
                    detailFiguresRadius: detailFigures ? window.getComputedStyle(detailFigures).borderRadius : "",
                    detailTablesRadius: detailTables ? window.getComputedStyle(detailTables).borderRadius : "",
                    detailFiguresCopyFontSize: detailFigures
                        ? window.getComputedStyle(detailFigures.querySelector(".details-copy")).fontSize
                        : "",
                    detailTablesCopyFontSize: detailTables
                        ? window.getComputedStyle(detailTables.querySelector(".details-copy")).fontSize
                        : "",
                };
            }
            """
        )
        assert design_supplement_metrics["detailFiguresHeight"] <= 84
        assert design_supplement_metrics["primaryExtraHeight"] <= 84
        assert design_supplement_metrics["detailTablesHeight"] <= 84
        assert design_supplement_metrics["detailSummaryHeight"] <= 82
        assert design_supplement_metrics["gapFigurePanelsToDetailFigures"] <= 12
        assert design_supplement_metrics["gapDetailFiguresToPrimary"] <= 8
        assert design_supplement_metrics["gapPrimaryToPrimaryExtra"] <= 12
        assert design_supplement_metrics["gapPrimaryExtraToDetailTables"] <= 12
        assert design_supplement_metrics["detailFiguresSummaryPadding"] == "12px 14px"
        assert design_supplement_metrics["detailTablesSummaryPadding"] == "12px 14px"
        assert design_supplement_metrics["detailFiguresRadius"] == "18px"
        assert design_supplement_metrics["detailTablesRadius"] == "18px"
        assert design_supplement_metrics["detailFiguresCopyFontSize"] == "12px"
        assert design_supplement_metrics["detailTablesCopyFontSize"] == "12px"
        page.get_by_role("button", name="紧凑").click()
        assert page.locator("body").get_attribute("data-table-density") == "compact"
        design_details = page.locator("[data-details-key='demo-design-detail-tables']")
        design_details.locator("summary").click()
        page.wait_for_function(
            """
            () => {
                const panel = document.querySelector("[data-details-key='demo-design-detail-tables']");
                const url = new URL(window.location.href);
                return panel?.open && url.searchParams.get('open') === 'demo-design-detail-tables';
            }
            """
        )
        assert (
            page.locator("[data-open-input]").first.input_value()
            == "demo-design-detail-tables"
        )
        reloaded_url = page.url
        page.goto(reloaded_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        assert (
            page.locator("[data-details-key='demo-design-detail-tables']").evaluate(
                "node => node.open"
            )
            is True
        )

        page.get_by_role("link", name="主线结果").click()
        _wait_for_section_state(page, "#tracks", "主线结果")
        page.wait_for_function(
            """
            () => {
                const topbar = document.querySelector(".topbar");
                const tracks = document.querySelector("#tracks");
                const heading = document.querySelector("#tracks .section-head");
                const firstTrack = document.querySelector("#price_pressure_track");
                if (!topbar || !tracks || !heading || !firstTrack) {
                    return false;
                }
                const topbarRect = topbar.getBoundingClientRect();
                const tracksRect = tracks.getBoundingClientRect();
                const headingRect = heading.getBoundingClientRect();
                const firstTrackRect = firstTrack.getBoundingClientRect();
                return (
                    tracksRect.top - topbarRect.bottom <= 24 &&
                    headingRect.top - topbarRect.bottom <= 60 &&
                    firstTrackRect.top - headingRect.bottom < 20
                );
            }
            """
        )
        tracks_landing_metrics = page.evaluate(
            """
            () => {
                const topbar = document.querySelector(".topbar");
                const tracks = document.querySelector("#tracks");
                const heading = document.querySelector("#tracks .section-head");
                const intro = heading?.querySelector(".section-intro");
                const side = heading?.querySelector(".section-side");
                const sideCopy = heading?.querySelector(".section-side-copy");
                const firstTrack = document.querySelector("#price_pressure_track");
                const topbarRect = topbar?.getBoundingClientRect();
                const tracksRect = tracks?.getBoundingClientRect();
                const headingRect = heading?.getBoundingClientRect();
                const firstTrackRect = firstTrack?.getBoundingClientRect();
                return {
                    gapTopbarToSection: topbarRect && tracksRect ? tracksRect.top - topbarRect.bottom : 0,
                    gapTopbarToHeading: topbarRect && headingRect ? headingRect.top - topbarRect.bottom : 0,
                    gapHeadingToFirstTrack: headingRect && firstTrackRect ? firstTrackRect.top - headingRect.bottom : 0,
                    headingHeight: headingRect?.height ?? 0,
                    introHeight: intro?.getBoundingClientRect().height ?? 0,
                    sideHeight: side?.getBoundingClientRect().height ?? 0,
                    sidePadding: side ? window.getComputedStyle(side).padding : "",
                    sideRadius: side ? window.getComputedStyle(side).borderRadius : "",
                    sideCopyFontSize: sideCopy ? window.getComputedStyle(sideCopy).fontSize : "",
                    firstTrackPaddingTop: firstTrack ? window.getComputedStyle(firstTrack).paddingTop : "",
                    firstTrackGap: firstTrack ? window.getComputedStyle(firstTrack).gap : "",
                };
            }
            """
        )
        assert tracks_landing_metrics["gapTopbarToSection"] <= 24
        assert tracks_landing_metrics["gapTopbarToHeading"] <= 60
        assert tracks_landing_metrics["gapHeadingToFirstTrack"] <= 10
        assert tracks_landing_metrics["headingHeight"] <= 104
        assert tracks_landing_metrics["introHeight"] <= 22
        assert tracks_landing_metrics["sideHeight"] <= 68
        assert tracks_landing_metrics["sidePadding"] == "12px 14px"
        assert tracks_landing_metrics["sideRadius"] == "16px"
        assert tracks_landing_metrics["sideCopyFontSize"] == "13px"
        assert tracks_landing_metrics["firstTrackPaddingTop"] == "18px"
        assert tracks_landing_metrics["firstTrackGap"] == "16px"

        page.get_by_role("link", name="文献框架").click()
        _wait_for_section_state(page, "#framework", "文献框架")
        assert "open=demo-design-detail-tables" in page.url
        active_sections = [
            text.strip()
            for text in page.locator("[data-section-link].active").all_inner_texts()
        ]
        assert "文献框架" in active_sections
        assert (
            page.locator("[data-section-link][aria-current='location']")
            .first.inner_text()
            .strip()
            == "文献框架"
        )
        assert (
            page.locator("[data-waypoint-dock]").get_attribute("data-visible") == "true"
        )
        assert page.locator("[data-waypoint-title]").inner_text().strip() == "文献框架"

        page.get_by_role("button", name="下一节").click()
        _wait_for_section_state(page, "#supplement", "机制补充")
        assert page.locator("[data-waypoint-title]").inner_text().strip() == "机制补充"
        active_sections = [
            text.strip()
            for text in page.locator("[data-section-link].active").all_inner_texts()
        ]
        assert "机制补充" in active_sections

        page.goto(f"{base_url}/?mode=demo#tracks", wait_until="domcontentloaded")
        page.locator("#tracks").wait_for()
        page.wait_for_function(
            "() => ['#tracks', '#price_pressure_track', '#demand_curve_track', '#identification_china_track'].includes(window.location.hash)"
        )
        page.locator("#tracks").evaluate(
            "el => el.scrollIntoView({ block: 'start', behavior: 'instant' })"
        )
        _wheel_until_hash(page, "#price_pressure_track", [700, 700, 900, 1100, 1100])
        page.wait_for_function(
            """
            () => {
                const activeLink = document.querySelector("[data-section-link][aria-current='location']");
                const waypointTitle = document.querySelector("[data-waypoint-title]");
                return (
                    activeLink?.textContent?.trim() === '主线结果' &&
                    waypointTitle?.textContent?.trim() === '主线结果 / 短期价格压力与效应减弱'
                );
            }
            """
        )
        assert (
            page.locator("[data-waypoint-title]").inner_text().strip()
            == "主线结果 / 短期价格压力与效应减弱"
        )
        active_sections = [
            text.strip()
            for text in page.locator("[data-section-link].active").all_inner_texts()
        ]
        assert "主线结果" in active_sections
        layout_metrics = page.evaluate(
            """
            () => {
                const dock = document.querySelector("[data-waypoint-dock]");
                const meta = document.querySelector("#price_pressure_track .track-surface-meta");
                const metaBlock = document.querySelector("#price_pressure_track .track-meta");
                const insights = document.querySelector("#price_pressure_track .insight-strip");
                const panels = document.querySelector("#price_pressure_track .track-panels");
                const takeaway = document.querySelector("#price_pressure_track .track-takeaway");
                const summary = document.querySelector("#price_pressure_track .track-summary-block");
                const firstInsight = document.querySelector("#price_pressure_track .insight-card");
                const insightValue = document.querySelector("#price_pressure_track .insight-value");
                const insightCopy = document.querySelector("#price_pressure_track .insight-copy");
                const visual = document.querySelector("#price_pressure_track .track-surface-visual");
                const figureStack = document.querySelector("#price_pressure_track .track-surface-visual .figure-stack");
                const featureMedia = document.querySelector("#price_pressure_track .figure-media-shell-feature");
                const featureImage = document.querySelector("#price_pressure_track .figure-feature img");
                const featureCopy = document.querySelector("#price_pressure_track .figure-copy-panel-feature");
                const thumbs = document.querySelector("#price_pressure_track .track-surface-thumbs");
                const thumbImage = document.querySelector("#price_pressure_track .thumb img");
                const thumbCopy = document.querySelector("#price_pressure_track .thumb-copy");
                const thumbCaption = document.querySelector("#price_pressure_track .thumb-caption");
                const dockRect = dock?.getBoundingClientRect();
                const metaRect = metaBlock?.getBoundingClientRect();
                const insightsRect = insights?.getBoundingClientRect();
                const panelsRect = panels?.getBoundingClientRect();
                return {
                    dockWidth: dockRect?.width ?? 0,
                    dockHeight: dockRect?.height ?? 0,
                    dockArea: (dockRect?.width ?? 0) * (dockRect?.height ?? 0),
                    metaPosition: meta ? window.getComputedStyle(meta).position : "",
                    trackMetaHeight: metaRect?.height ?? 0,
                    gapMetaToInsights: metaRect && insightsRect ? insightsRect.top - metaRect.bottom : 0,
                    gapInsightsToPanels: insightsRect && panelsRect ? panelsRect.top - insightsRect.bottom : 0,
                    takeawayHeight: takeaway?.getBoundingClientRect().height ?? 0,
                    takeawayPadding: takeaway ? window.getComputedStyle(takeaway).padding : "",
                    summaryHeight: summary?.getBoundingClientRect().height ?? 0,
                    summaryPaddingTop: summary ? window.getComputedStyle(summary).paddingTop : "",
                    trackMetaGap: metaBlock ? window.getComputedStyle(metaBlock).gap : "",
                    trackMetaPaddingBottom: metaBlock ? window.getComputedStyle(metaBlock).paddingBottom : "",
                    insightHeight: insightsRect?.height ?? 0,
                    firstInsightHeight: firstInsight?.getBoundingClientRect().height ?? 0,
                    firstInsightPadding: firstInsight ? window.getComputedStyle(firstInsight).padding : "",
                    firstInsightRadius: firstInsight ? window.getComputedStyle(firstInsight).borderRadius : "",
                    firstInsightValueFontSize: insightValue ? window.getComputedStyle(insightValue).fontSize : "",
                    firstInsightCopyFontSize: insightCopy ? window.getComputedStyle(insightCopy).fontSize : "",
                    panelsHeight: panelsRect?.height ?? 0,
                    visualHeight: visual?.getBoundingClientRect().height ?? 0,
                    figureStackPadding: figureStack ? window.getComputedStyle(figureStack).padding : "",
                    featureImageHeight: featureImage?.getBoundingClientRect().height ?? 0,
                    featureMediaPadding: featureMedia ? window.getComputedStyle(featureMedia).padding : "",
                    featureCopyPadding: featureCopy ? window.getComputedStyle(featureCopy).padding : "",
                    thumbsHeight: thumbs?.getBoundingClientRect().height ?? 0,
                    thumbImageHeight: thumbImage?.getBoundingClientRect().height ?? 0,
                    thumbCopyPadding: thumbCopy ? window.getComputedStyle(thumbCopy).padding : "",
                    thumbCaptionFontSize: thumbCaption ? window.getComputedStyle(thumbCaption).fontSize : "",
                };
            }
            """
        )
        assert layout_metrics["dockWidth"] <= 208
        assert layout_metrics["dockArea"] < 24500
        assert layout_metrics["metaPosition"] == "static"
        assert layout_metrics["trackMetaHeight"] <= 221
        assert layout_metrics["gapMetaToInsights"] <= 16
        assert layout_metrics["gapInsightsToPanels"] <= 16
        assert layout_metrics["takeawayHeight"] <= 57
        assert layout_metrics["takeawayPadding"] == "9px 12px"
        assert layout_metrics["summaryHeight"] <= 100
        assert layout_metrics["summaryPaddingTop"] == "8px"
        assert layout_metrics["trackMetaGap"] == "12px"
        assert layout_metrics["trackMetaPaddingBottom"] == "6px"
        assert layout_metrics["insightHeight"] <= 83
        assert layout_metrics["firstInsightHeight"] <= 83
        assert layout_metrics["firstInsightPadding"] == "10px 12px"
        assert layout_metrics["firstInsightRadius"] == "16px"
        assert layout_metrics["firstInsightValueFontSize"] == "22px"
        assert layout_metrics["firstInsightCopyFontSize"] == "11px"
        assert layout_metrics["panelsHeight"] <= 911
        assert layout_metrics["visualHeight"] <= 445
        assert layout_metrics["figureStackPadding"] == "12px"
        assert layout_metrics["featureImageHeight"] <= 308
        assert layout_metrics["featureMediaPadding"] == "12px 12px 0px"
        assert layout_metrics["featureCopyPadding"] == "6px 10px 10px"
        assert layout_metrics["thumbsHeight"] <= 453
        assert layout_metrics["thumbImageHeight"] <= 356
        assert layout_metrics["thumbCopyPadding"] == "10px 12px 12px"
        assert layout_metrics["thumbCaptionFontSize"] == "12px"

        page.goto(
            f"{base_url}/paper/harris_gurel_1986?view=track",
            wait_until="domcontentloaded",
        )
        page.wait_for_load_state("networkidle")
        assert page.locator("a.skip-link").get_attribute("href") == "#main-content"
        assert "公开 alpha" in page.locator(".hero-summary").inner_text()
        assert (
            "按阵营最适合看争论如何推进"
            in page.locator(".evolution-nav-copy").inner_text()
        )
        assert page.get_by_role("link", name="回看上一环").is_visible()
        assert page.get_by_role("link", name="继续下一环").is_visible()
        assert (
            page.get_by_role("button", name="按主线").get_attribute("aria-pressed")
            == "true"
        )
        assert page.locator("#evolution-view-track").is_visible()
        assert page.locator("#evolution-view-camp").is_hidden()
        assert (
            page.locator("#evolution-view-track details.evolution-group[open]").count()
            == 1
        )
        assert (
            page.locator(
                "#evolution-view-track details.evolution-group[open] .evolution-link.current"
            ).count()
            == 1
        )
        assert (
            page.locator("#evolution-view-track .group-pill.current")
            .inner_text()
            .strip()
            == "当前文献所在分组"
        )
        assert (
            page.locator(
                "#evolution-view-track details.evolution-group:not([open])"
            ).count()
            >= 1
        )

        mobile_page = browser.new_page(viewport={"width": 390, "height": 844})
        mobile_page.goto(f"{base_url}/?mode=demo", wait_until="domcontentloaded")
        mobile_page.wait_for_load_state("networkidle")
        mobile_menu_toggle = mobile_page.locator(
            "[data-waypoint-menu-toggle].mobile-only"
        )
        mobile_menu_toggle.click()
        assert mobile_menu_toggle.get_attribute("aria-expanded") == "true"
        assert (
            mobile_page.locator("[data-waypoint-menu]").get_attribute("data-open")
            == "true"
        )
        mobile_page.locator(
            "[data-waypoint-menu-link][href='#identification_china_track']"
        ).click()
        mobile_page.wait_for_function(
            """
            () => {
                const title = document.querySelector("[data-waypoint-title]");
                const progress = document.querySelector("[data-reading-progress]");
                const dock = document.querySelector("[data-waypoint-dock]");
                return (
                    window.location.hash === '#identification_china_track' &&
                    title?.textContent?.trim() === '主线结果 / 制度识别与中国市场证据' &&
                    Number(progress?.getAttribute('aria-valuenow') || '0') > 0 &&
                    document.querySelector("[data-waypoint-menu]")?.getAttribute("data-open") === "false" &&
                    dock?.getAttribute("data-visible") === "true"
                );
            }
            """
        )
        assert (
            mobile_page.locator(
                "[data-waypoint-menu-toggle].mobile-only"
            ).get_attribute("aria-expanded")
            == "false"
        )
        assert (
            mobile_page.locator("[data-waypoint-dock]").get_attribute("data-visible")
            == "true"
        )
        mobile_page.close()

        browser.close()

    assert page_errors == []
    assert console_errors == []


def test_cross_market_section_renders_in_full_mode() -> None:
    """CMA section should render in full mode with quadrant table, figures,
    and hypothesis map."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = playwright.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("section#cross_market_asymmetry")
            assert section.count() == 1
            section.first.scroll_into_view_if_needed()
            assert (
                "美股 vs A股 公告—生效事件集中度差异"
                in section.locator("h2").first.inner_text()
            )

            quadrant_rows = section.locator("table.cma-quadrant-table tbody tr")
            assert quadrant_rows.count() == 4

            figures = section.locator("figure.cma-figure")
            assert figures.count() >= 3

            cma_images = section.locator("img[src*='cma_']")
            assert cma_images.count() >= 3

            hypothesis_rows = section.locator(".cma-hypothesis table tbody tr")
            assert hypothesis_rows.count() == 6
        finally:
            browser.close()


def test_cross_market_section_hides_figures_in_brief_mode() -> None:
    """Brief mode should render the CMA section header but not show figures
    or the hypothesis map."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = playwright.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=brief", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("section#cross_market_asymmetry")
            assert section.count() == 1
            assert section.locator("figure.cma-figure").count() == 0
            assert section.locator(".cma-hypothesis").count() == 0
        finally:
            browser.close()
