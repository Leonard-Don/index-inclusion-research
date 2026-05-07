from __future__ import annotations

import atexit
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

from index_inclusion_research.chart_data import CHART_BUILDERS

pytestmark = pytest.mark.browser_smoke

if os.environ.get("RUN_BROWSER_SMOKE") != "1":
    pytest.skip(
        "Set RUN_BROWSER_SMOKE=1 to run browser smoke tests.", allow_module_level=True
    )

playwright_sync_api = pytest.importorskip("playwright.sync_api")


ROOT = Path(__file__).resolve().parents[1]
_SHARED_DASHBOARD_SERVER: tuple[str, subprocess.Popen[str]] | None = None


def _cached_chromium_executable() -> Path | None:
    cache_roots = [
        Path.home() / "Library" / "Caches" / "ms-playwright",
        Path.home() / ".cache" / "ms-playwright",
    ]
    if os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        cache_roots.insert(0, Path(os.environ["PLAYWRIGHT_BROWSERS_PATH"]))
    patterns = [
        "chromium_headless_shell-*/chrome-headless-shell-mac-arm64/chrome-headless-shell",
        "chromium_headless_shell-*/chrome-headless-shell-linux/chrome-headless-shell",
        "chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium",
        "chromium-*/chrome-linux/chrome",
    ]
    candidates: list[Path] = []
    for cache_root in cache_roots:
        for pattern in patterns:
            candidates.extend(path for path in cache_root.glob(pattern) if path.is_file())
    return sorted(candidates, reverse=True)[0] if candidates else None


def _launch_chromium(playwright):
    try:
        return playwright.chromium.launch()
    except Exception as exc:
        if "Executable doesn't exist" not in str(exc):
            raise
        fallback = _cached_chromium_executable()
        if fallback is None:
            raise
        return playwright.chromium.launch(executable_path=str(fallback))


def _new_dashboard_page(browser, *, viewport: dict[str, int]) -> playwright_sync_api.Page:
    page = browser.new_page(viewport=viewport)
    navigation_timeout = 180_000 if os.environ.get("CI") else 45_000
    page.set_default_navigation_timeout(navigation_timeout)
    return page


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_dashboard(
    url: str,
    proc: subprocess.Popen[str],
    timeout_seconds: float = 180.0,
    request_timeout: float = 10.0,
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
            remaining = max(1.0, deadline - time.time())
            connection = http.client.HTTPConnection(
                host, port, timeout=min(request_timeout, remaining)
            )
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


def _warm_dashboard_pages(base_url: str, proc: subprocess.Popen[str]) -> None:
    for path in ["/?mode=demo", "/?mode=full", "/?mode=brief", "/rdd-l3"]:
        _wait_for_dashboard(
            f"{base_url}{path}",
            proc,
            timeout_seconds=240.0,
            request_timeout=240.0,
        )


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


def _stop_dashboard_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate(timeout=5)


def _ensure_dashboard_server() -> str:
    global _SHARED_DASHBOARD_SERVER
    if _SHARED_DASHBOARD_SERVER is not None:
        base_url, proc = _SHARED_DASHBOARD_SERVER
        if proc.poll() is None:
            return base_url

    port = _find_free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "index_inclusion_research.literature_dashboard", "--port", str(port)],
        cwd=ROOT,
        env={**os.environ, "DASHBOARD_ECHARTS_TEST_STUB": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    _wait_for_dashboard(f"{base_url}/favicon.ico", proc)
    _warm_dashboard_pages(base_url, proc)
    _SHARED_DASHBOARD_SERVER = (base_url, proc)
    atexit.register(_stop_dashboard_server, proc)
    return base_url


@contextmanager
def _running_dashboard_server() -> str:
    yield _ensure_dashboard_server()


def test_dashboard_browser_smoke() -> None:
    console_errors: list[str] = []
    page_errors: list[str] = []

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
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
        assert (
            page.locator("[data-refresh-health-summary]").inner_text().strip()
            == "结果健康良好"
        )
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
        assert topbar_metrics["topbarHeight"] <= 100
        assert topbar_metrics["brandWidth"] <= 320
        assert topbar_metrics["navWidth"] >= 1080
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
        assert hero_metrics["heroHeight"] <= 820
        assert hero_metrics["heroBottom"] <= 920
        assert hero_metrics["heroSideHeight"] <= 800
        assert hero_metrics["gapTopbarToH1"] >= 0
        assert hero_metrics["gapTopbarToH1"] <= 100
        assert hero_metrics["heroSideWidth"] >= hero_metrics["heroCopyWidth"]
        assert hero_metrics["h1Width"] >= 360
        assert hero_metrics["heroCopyUnusedWidth"] <= 320
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
        assert post_hero_metrics["gapHeroToUtility"] <= 24
        assert post_hero_metrics["utilityHeight"] <= 130
        assert post_hero_metrics["coreHeadTop"] <= 1080
        assert post_hero_metrics["coreHeadVisibleWithinViewport"] >= 60
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
        assert core_findings_metrics["abstractTop"] <= 1100
        assert core_findings_metrics["abstractHeight"] <= 360
        assert core_findings_metrics["highlightHeight"] <= 240
        assert core_findings_metrics["gapHeadToAbstract"] <= 24
        assert core_findings_metrics["abstractVisibleWithinViewport"] >= 0
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
        assert design_metrics["designHeight"] <= 2800
        assert design_metrics["headHeight"] <= 140
        assert design_metrics["summaryHeight"] <= 260
        assert design_metrics["figurePanelsHeight"] <= 1400
        assert design_metrics["primaryGroupHeight"] <= 600
        assert design_metrics["groupHeadHeight"] <= 60
        assert design_metrics["gapHeadToSummary"] <= 24
        assert design_metrics["gapSummaryToFigurePanels"] <= 24
        assert design_metrics["firstImageHeight"] <= 540
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
        assert design_supplement_metrics["gapFigurePanelsToDetailFigures"] <= 24
        assert design_supplement_metrics["gapDetailFiguresToPrimary"] <= 16
        assert design_supplement_metrics["gapPrimaryToPrimaryExtra"] <= 24
        assert design_supplement_metrics["gapPrimaryExtraToDetailTables"] <= 24
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
        assert tracks_landing_metrics["gapTopbarToSection"] <= 40
        assert tracks_landing_metrics["gapTopbarToHeading"] <= 80
        assert tracks_landing_metrics["gapHeadingToFirstTrack"] <= 20
        assert tracks_landing_metrics["headingHeight"] <= 140
        assert tracks_landing_metrics["introHeight"] <= 40
        assert tracks_landing_metrics["sideHeight"] <= 90

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
        assert layout_metrics["dockWidth"] <= 240
        assert layout_metrics["dockArea"] < 32000
        assert layout_metrics["metaPosition"] == "static"
        assert layout_metrics["trackMetaHeight"] <= 280
        assert layout_metrics["gapMetaToInsights"] <= 28
        assert layout_metrics["gapInsightsToPanels"] <= 28
        assert layout_metrics["takeawayHeight"] <= 90
        assert layout_metrics["summaryHeight"] <= 140
        assert layout_metrics["insightHeight"] <= 110
        assert layout_metrics["firstInsightHeight"] <= 110
        assert layout_metrics["panelsHeight"] <= 1080
        assert layout_metrics["visualHeight"] <= 540
        assert layout_metrics["featureImageHeight"] <= 360
        assert layout_metrics["thumbsHeight"] <= 540
        assert layout_metrics["thumbImageHeight"] <= 420

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

        mobile_page = _new_dashboard_page(browser, viewport={"width": 390, "height": 844})
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


def test_dashboard_bottom_scroll_does_not_snap_back_to_top() -> None:
    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=demo", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
            page.wait_for_function(
                """
                () => {
                    return (
                        window.scrollY > window.innerHeight * 2 &&
                        window.location.hash === '#cross_market_asymmetry'
                    );
                }
                """
            )
            page.wait_for_timeout(800)
            bottom_state = page.evaluate(
                """
                () => {
                    const root = document.documentElement;
                    const maxScroll = Math.max(0, root.scrollHeight - window.innerHeight);
                    return {
                        hash: window.location.hash,
                        stayedAwayFromTop: window.scrollY > window.innerHeight * 2,
                        scrollY: window.scrollY,
                        maxScroll,
                    };
                }
                """
            )

            assert bottom_state["hash"] == "#cross_market_asymmetry"
            assert bottom_state["stayedAwayFromTop"] is True, bottom_state
        finally:
            browser.close()


def test_cross_market_section_renders_in_full_mode() -> None:
    """CMA section should render in full mode with quadrant table, figures,
    hypothesis map, and all detail tables in collapsibles."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
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
            cma_image_count = cma_images.count()
            assert cma_image_count >= 3
            for image_index in range(cma_image_count):
                page.wait_for_function(
                    """
                    ({ selector, index }) => {
                        const img = document.querySelectorAll(selector)[index];
                        if (!img) {
                            return false;
                        }
                        img.loading = "eager";
                        img.scrollIntoView({ block: "center", inline: "nearest" });
                        return img.complete && img.naturalWidth > 0;
                    }
                    """,
                    arg={"selector": "section#cross_market_asymmetry img[src*='cma_']", "index": image_index},
                    timeout=8000,
                )
            cma_chart_metrics = page.evaluate(
                """
                () => Array.from(document.querySelectorAll(
                    "section#cross_market_asymmetry figure.cma-figure"
                )).map((figure) => {
                    const img = figure.querySelector("img[src*='cma_']");
                    const fallback = img?.closest(".echart-fallback");
                    const panel = figure.querySelector(".echart-panel");
                    const panelRect = panel?.getBoundingClientRect();
                    return {
                        caption: figure.querySelector("figcaption")?.textContent?.trim() || "",
                        naturalWidth: img?.naturalWidth ?? 0,
                        naturalHeight: img?.naturalHeight ?? 0,
                        fallbackHidden: fallback?.hasAttribute("hidden") ?? false,
                        panelHeight: panelRect?.height ?? 0,
                    };
                })
                """
            )
            assert len(cma_chart_metrics) >= 3
            for metric in cma_chart_metrics:
                assert metric["naturalWidth"] > 0, metric
                assert metric["naturalHeight"] > 0, metric
                if metric["fallbackHidden"]:
                    assert metric["panelHeight"] >= 160, metric

            page.evaluate(
                """
                () => Array.from(document.querySelectorAll(
                    "section#cross_market_asymmetry .echart-fallback"
                )).forEach((fallback) => { fallback.hidden = false; })
                """
            )
            cma_fallback_metrics = page.evaluate(
                """
                () => Array.from(document.querySelectorAll(
                    "section#cross_market_asymmetry figure.cma-figure"
                )).map((figure) => {
                    const img = figure.querySelector("img[src*='cma_']");
                    const figureRect = figure.getBoundingClientRect();
                    const imgRect = img?.getBoundingClientRect();
                    return {
                        caption: figure.querySelector("figcaption")?.textContent?.trim() || "",
                        figureWidth: figureRect.width,
                        imageWidth: imgRect?.width ?? 0,
                        imageHeight: imgRect?.height ?? 0,
                        naturalWidth: img?.naturalWidth ?? 0,
                        naturalHeight: img?.naturalHeight ?? 0,
                    };
                })
                """
            )
            assert len(cma_fallback_metrics) >= 3
            for metric in cma_fallback_metrics:
                assert metric["naturalWidth"] > 0, metric
                assert metric["naturalHeight"] > 0, metric
                assert metric["imageWidth"] <= metric["figureWidth"] + 1, metric
                assert metric["imageHeight"] >= 80, metric

            verdict_cards = section.locator(".cma-verdict-card")
            assert verdict_cards.count() == 7
            assert section.locator(".cma-verdict-card[data-evidence-tier='core']").count() == 3
            assert (
                section.locator(".cma-verdict-card[data-evidence-tier='supplementary']").count()
                == 4
            )
            evidence_tier_nav = section.locator(".cma-evidence-tier-filter")
            assert evidence_tier_nav.count() == 1
            evidence_tier_nav.locator(
                ".cma-verdict-filter-chip[data-filter-tier='supplementary']"
            ).click()
            page.wait_for_function(
                """
                () => {
                    const grid = document.querySelector(".cma-verdict-grid");
                    return grid?.getAttribute("data-filter-tier") === "supplementary";
                }
                """,
                timeout=5000,
            )
            assert (
                section.locator(
                    ".cma-verdict-card[data-evidence-tier='supplementary']:visible"
                ).count()
                == 4
            )
            evidence_tier_nav.locator(
                ".cma-verdict-filter-chip[data-filter-tier='all']"
            ).click()
            track_cards = section.locator(".cma-track-card")
            assert track_cards.count() == 3
            evidence_cards = section.locator(".cma-evidence-card")
            assert evidence_cards.count() >= 2
            h6_evidence = section.locator(
                ".cma-evidence-card:has-text('H6 权重变化')"
            )
            assert h6_evidence.count() == 1
            assert h6_evidence.first.get_attribute("href") == "/evidence/H6_weight_change"
            assert (
                section.locator(".cma-verdict-card:has-text('H3')").count()
                >= 1
            )

            hypothesis_rows = section.locator(".cma-hypothesis table tbody tr")
            assert hypothesis_rows.count() == 7

            collapsibles = section.locator("details.cma-collapsible")
            assert collapsibles.count() >= 12

            summaries = [
                "窗口摘要全集",
                "机制回归面板",
                "H6 权重解释层",
                "异质性 · 市值五分位",
                "异质性 · 流动性五分位",
                "异质性 · 行业",
                "H7 行业交互回归",
                "异质性 · 空窗期分桶",
                "滚动时序",
                "结构变点",
                "日度 AR 路径",
                "日度 CAR 路径",
            ]
            for label in summaries:
                summary = section.locator(
                    f"details.cma-collapsible summary:has-text('{label}')"
                )
                assert summary.count() >= 1, f"missing collapsible label: {label}"

            first_collapsible = collapsibles.first
            first_collapsible.evaluate("el => { el.open = true; }")
            first_collapsible.locator("table tbody tr").first.wait_for(
                state="visible", timeout=2000
            )
        finally:
            browser.close()


def test_evidence_detail_and_rdd_l3_workbench_pages_render() -> None:
    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})

            response = page.goto(
                f"{base_url}/api/evidence/H6_weight_change",
                wait_until="domcontentloaded",
            )
            assert response is not None
            assert response.status == 200
            assert "matched_weight_events" in page.locator("body").inner_text()

            page.goto(
                f"{base_url}/evidence/H6_weight_change",
                wait_until="domcontentloaded",
            )
            page.wait_for_load_state("networkidle")
            assert "H6 权重变化" in page.locator("h1").inner_text()
            assert page.locator(".evidence-detail-table:has-text('H6 权重解释层')").count() == 1
            assert page.locator("a[href='/rdd-l3']").count() >= 1

            page.goto(f"{base_url}/evidence/H7_cn_sector", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            assert "H7 A股行业覆盖" in page.locator("h1").inner_text()
            assert (
                page.locator(
                    ".evidence-detail-table:has-text('H7 行业交互回归')"
                ).count()
                == 1
            )

            page.goto(f"{base_url}/rdd-l3", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            assert "官方候选导入工作台" in page.locator("h1").inner_text()
            assert page.locator("form[action='/rdd-l3/check']").count() == 1
            assert page.locator("form[action='/rdd-l3/import']").count() == 1
            assert page.locator("form[action='/rdd-l3/collection']").count() == 1
            assert page.locator("form[action='/rdd-l3/online-collection']").count() == 1
            assert page.locator(".rdd-collection-status").count() >= 2
            assert page.locator(".result-card:has-text('线上采集诊断')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('线上年份覆盖诊断')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('线上来源审计预览')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('线上补录缺口清单')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('线上缺口来源查找入口')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('线上搜索诊断预览')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('批次采集清单预览')").count() == 1
            assert page.locator(".evidence-detail-table:has-text('边界参考预览')").count() == 1
        finally:
            browser.close()


def test_sensitivity_threshold_chip_flips_verdict_card_strips() -> None:
    """End-to-end check that the dashboard p-threshold chip rewires every
    p-gated verdict card's sensitivity strip in real time.

    Locks the wiring landed in 88fe435: SSR renders the 0.10 baseline,
    JS controller takes over on click. We verify both halves: the SSR
    initial state and a live click that flips strip text + data-sens.
    """
    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("section#cross_market_asymmetry")
            section.first.scroll_into_view_if_needed()

            # ── SSR baseline: nav rendered, default chip 0.10 active ──
            nav = section.locator(".cma-sensitivity-threshold-filter")
            assert nav.count() == 1
            chips = nav.locator(".cma-sensitivity-threshold-chip")
            assert chips.count() == 5
            active_chip = nav.locator(".cma-sensitivity-threshold-chip.is-active")
            assert active_chip.count() == 1
            assert active_chip.first.get_attribute("data-threshold") == "0.10"

            grid = section.locator(".cma-verdict-grid")
            assert grid.get_attribute("data-sensitivity-threshold") == "0.10"

            # The non-p hypotheses (H2/H3/H6/H7) are always na regardless of
            # threshold — the strip text shouldn't mention p<...
            h2_strip = section.locator(
                "#hypothesis-H2 .cma-verdict-sensitivity"
            )
            assert h2_strip.get_attribute("data-sensitivity") == "na"
            assert "不参与阈值切换" in h2_strip.inner_text()

            # ── Live click: pick 0.20, expect grid attr to flip ──
            chip_020 = nav.locator(
                ".cma-sensitivity-threshold-chip[data-threshold='0.20']"
            )
            chip_020.click()
            page.wait_for_function(
                """
                () => {
                    const grid = document.querySelector(".cma-verdict-grid");
                    return grid?.getAttribute("data-sensitivity-threshold") === "0.20";
                }
                """,
                timeout=5000,
            )
            # Active chip moved
            assert (
                section.locator(
                    ".cma-sensitivity-threshold-chip.is-active"
                ).first.get_attribute("data-threshold")
                == "0.20"
            )
            # H2 (non-p) still na — was never going to flip
            assert h2_strip.get_attribute("data-sensitivity") == "na"

            # H1 / H4 / H5 strip text contains the new threshold "0.20"
            for hid in ("H1", "H4", "H5"):
                card = section.locator(f"#hypothesis-{hid}")
                strip = card.locator(".cma-verdict-sensitivity")
                strip_text = strip.inner_text()
                assert "0.20" in strip_text, (
                    f"{hid} strip didn't pick up new threshold; got {strip_text!r}"
                )
                # data-p-value should be a parseable float — defensive check
                p_attr = card.get_attribute("data-p-value")
                assert p_attr and float(p_attr) >= 0.0
        finally:
            browser.close()


def test_l3_coverage_timeline_appears_in_identification_track() -> None:
    """The L3 coverage timeline figure should surface as a thumb in the
    identification china track once the candidates CSV exists."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("#identification_china_track")
            assert section.count() == 1
            section.first.scroll_into_view_if_needed()

            timeline_imgs = section.locator("img[src*='l3_coverage_timeline']")
            assert timeline_imgs.count() == 1, (
                "expected exactly one L3 coverage timeline image in the identification track"
            )
            alt = timeline_imgs.first.get_attribute("alt") or ""
            assert "L3 候选样本批次覆盖" in alt
            assert "20 个批次" in alt  # threshold target rendered into caption
        finally:
            browser.close()


def test_rdd_robustness_forest_plot_appears_in_identification_track() -> None:
    """The RDD robustness forest plot (main / donut / placebo / polynomial)
    should surface as the trailing thumb in the identification track,
    sitting alongside the L3 timeline + 3 secondary-outcome bin charts."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("#identification_china_track")
            assert section.count() == 1
            section.first.scroll_into_view_if_needed()

            forest_imgs = section.locator("img[src*='rdd_robustness_forest']")
            assert forest_imgs.count() == 1
            alt = forest_imgs.first.get_attribute("alt") or ""
            assert "稳健性面板" in alt
            # Headline τ + p surfaced in caption so reviewers see at a
            # glance whether main spec is the strongest.
            assert "main" in alt and "局部线性" in alt

            interactive_forest = section.locator('[data-echart="rdd_robustness"]')
            assert interactive_forest.count() == 1
            interactive_forest.scroll_into_view_if_needed()
            page.wait_for_function(
                """
                () => {
                    if (typeof echarts === "undefined") return false;
                    const el = document.querySelector('[data-echart="rdd_robustness"]');
                    if (!el) return false;
                    const inst = echarts.getInstanceByDom(el);
                    return inst != null && !el.classList.contains("echart-loading");
                }
                """,
                timeout=10_000,
            )

            # And the underlying chart_data endpoint should expose the same
            # 5 specs (forest plot rows) the PNG visualizes.
            api_response = page.request.get(f"{base_url}/api/chart/rdd_robustness")
            assert api_response.status == 200
            payload = api_response.json()
            spec_kinds = sorted({row["spec_kind"] for row in payload["rows"]})
            assert spec_kinds == ["donut", "main", "placebo", "polynomial"]
        finally:
            browser.close()


def test_full_dashboard_mounts_every_chart_api_builder() -> None:
    """Every backend chart-data builder should have a frontend mount point
    in full mode, otherwise the API exists without a visible dashboard
    consumer."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            frontend_chart_ids = set(
                page.locator("[data-echart]").evaluate_all(
                    "nodes => nodes.map(node => node.dataset.echart)"
                )
            )
            assert set(CHART_BUILDERS) <= frontend_chart_ids
        finally:
            browser.close()


def test_rdd_secondary_outcome_thumbs_render_in_identification_track() -> None:
    """The 3 RDD secondary outcome bin scatter figures (CAR[-3,+3] /
    turnover / volume) should surface alongside the L3 timeline as thumbs
    so reviewers can see RDD robustness across outcomes without leaving
    the home page."""

    expected_thumbs = [
        ("car_m3_p3_rdd_bins", "CAR[-3,+3]"),
        ("turnover_change_rdd_bins", "换手"),
        ("volume_change_rdd_bins", "成交量"),
    ]

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("#identification_china_track")
            assert section.count() == 1

            for filename_stem, caption_keyword in expected_thumbs:
                imgs = section.locator(f"img[src*='{filename_stem}']")
                assert imgs.count() == 1, (
                    f"expected exactly one '{filename_stem}' thumb in identification track"
                )
                alt = imgs.first.get_attribute("alt") or ""
                assert "RDD 稳健性" in alt
                assert caption_keyword in alt
        finally:
            browser.close()


def test_no_absolute_home_directory_path_leaks_in_dashboard_html() -> None:
    """End-to-end guard against pipeline scripts that write absolute paths
    into evidence-card text or dashboard tables. We saw this from the
    CMA evidence card's '... under {tables_dir}' string and from the
    data_sources.csv pipeline. Both are now relative; this smoke test
    catches future regressions that re-introduce contributor-home leaks
    into the dashboard HTML."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            html = page.content()
            assert "/Users/leonardodon/" not in html, (
                "dashboard HTML leaks an absolute home-directory path; "
                "rewrite the pipeline writer to use a project-relative form."
            )
            # Defensive: also check the evidence cards specifically since
            # that's where the most recent regression was caught.
            evidence_cards = page.locator(".cma-evidence-card")
            assert evidence_cards.count() >= 1
            for i in range(evidence_cards.count()):
                card_text = evidence_cards.nth(i).inner_text()
                assert "/Users/" not in card_text, (
                    f"evidence card #{i} leaks absolute path: {card_text!r}"
                )
        finally:
            browser.close()


def test_data_sources_citation_table_renders_in_limits_section() -> None:
    """The data_sources.csv citation table should be reachable from the
    limits section in full mode, with project-relative paths (no absolute
    home-directory leaks in the rendered cells)."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            html = page.content()
            assert "数据来源 · 引用清单" in html
            assert "原始输出全集" in html
            assert "results/real_event_study/event_level_metrics.csv" in html
            assert "data/raw/hs300_rdd_candidates.csv" in html
            assert "索引保留" in html
            assert "real_events_clean.csv" in html
            assert "real_prices.csv" in html
            assert "Yahoo Finance" in html

            # Ensure rendered citation table cells stay project-relative;
            # no absolute home-directory leak inside the citation table.
            citation_label_idx = html.find("数据来源 · 引用清单")
            assert citation_label_idx > 0
            table_open = html.find("<table", citation_label_idx)
            table_close = html.find("</table>", table_open)
            assert table_open > 0 and table_close > table_open
            citation_table_html = html[table_open:table_close]
            assert "real_events_clean.csv" in citation_table_html
            assert "/Users/" not in citation_table_html, (
                "citation table rendered an absolute home-dir path; "
                "expected project-relative (e.g. data/processed/real_events_clean.csv)"
            )
        finally:
            browser.close()


def test_pap_status_chip_renders_with_baseline_diff() -> None:
    """The PAP (pre-analysis plan) hero chip should surface the latest
    snapshot's drift status. Frozen state when current verdicts match the
    snapshot exactly; drift state when they differ."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            chip = page.locator("[data-pap-summary]")
            assert chip.count() == 1
            chip.first.scroll_into_view_if_needed()

            drift_state = chip.first.get_attribute("data-drift-state")
            assert drift_state in {"frozen", "drift", "missing"}

            headline = page.locator("[data-pap-headline]").inner_text().strip()
            assert headline.startswith("PAP 冻结")
            assert "当前 vs 基线" in headline

            snapshot_path = page.locator("[data-pap-snapshot-path]").inner_text().strip()
            assert snapshot_path.startswith("snapshots/pre-registration-")
            assert snapshot_path.endswith(".csv")

            summary_label = page.locator("[data-pap-summary-label]").inner_text().strip()
            assert summary_label  # non-empty
        finally:
            browser.close()


def test_rdd_chart_renders_bandwidth_sweep() -> None:
    """The HS300 RDD ECharts container should render with multiple bandwidth
    fit lines (legend acts as bandwidth selector) and a subtitle reporting
    τ/p/n at the default bandwidth."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=full", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            container = page.locator('[data-echart="rdd_scatter"]')
            assert container.count() == 1
            container.first.scroll_into_view_if_needed()

            # Wait until interactive_charts.js finishes initializing the
            # chart (echarts.getInstanceByDom returns a non-null instance).
            page.wait_for_function(
                """
                () => {
                    if (typeof echarts === "undefined") return false;
                    const el = document.querySelector('[data-echart="rdd_scatter"]');
                    if (!el) return false;
                    const inst = echarts.getInstanceByDom(el);
                    return inst != null && !el.classList.contains("echart-loading");
                }
                """,
                timeout=10_000,
            )

            chart_state = page.evaluate(
                """
                () => {
                    const el = document.querySelector('[data-echart="rdd_scatter"]');
                    const inst = echarts.getInstanceByDom(el);
                    const opt = inst.getOption();
                    return {
                        title_text: opt.title?.[0]?.text || "",
                        subtitle: opt.title?.[0]?.subtext || "",
                        series_count: opt.series.length,
                        series_types: opt.series.map(s => s.type),
                        legend_data: opt.legend?.[0]?.data || [],
                        legend_selected: opt.legend?.[0]?.selected || {},
                        x_axis_min: opt.xAxis?.[0]?.min,
                        x_axis_max: opt.xAxis?.[0]?.max,
                    };
                }
                """
            )

            assert "HS300 RDD" in chart_state["title_text"]
            # Subtitle records the default-bandwidth headline statistics.
            assert "默认 bandwidth=0.06" in chart_state["subtitle"]
            assert "τ=" in chart_state["subtitle"]
            assert "p=" in chart_state["subtitle"]
            assert "n=" in chart_state["subtitle"]
            assert float(chart_state["x_axis_min"]) > 299.0
            assert float(chart_state["x_axis_max"]) < 301.0

            line_series_count = sum(1 for t in chart_state["series_types"] if t == "line")
            scatter_series_count = sum(
                1 for t in chart_state["series_types"] if t == "scatter"
            )
            # 2 scatter (treated/control) + ≥6 line series (≥3 bandwidth fits
            # × 2 sides) + 1 cutoff marker line.
            assert scatter_series_count == 2
            assert line_series_count >= 4

            # Legend should expose bandwidth-labeled entries; each looks like
            # "bw=0.06 (τ=…%, p=…, n=…)".
            bandwidth_labels = [
                lbl for lbl in chart_state["legend_data"] if str(lbl).startswith("bw=")
            ]
            assert len(bandwidth_labels) >= 3, (
                f"expected ≥3 bandwidth legend entries, got {bandwidth_labels}"
            )

            # Default selection: scatter series visible, only the
            # default-bandwidth fit visible, other bandwidths off.
            default_bw_label = next(
                (lbl for lbl in bandwidth_labels if "bw=0.06" in str(lbl)),
                None,
            )
            assert default_bw_label is not None
            assert chart_state["legend_selected"][default_bw_label] is True
            other_bw_labels = [lbl for lbl in bandwidth_labels if lbl != default_bw_label]
            for lbl in other_bw_labels:
                assert chart_state["legend_selected"][lbl] is False, (
                    f"non-default bandwidth {lbl!r} should be hidden by default"
                )
        finally:
            browser.close()


def test_regression_forest_plots_use_coefficient_axis_extent() -> None:
    """Forest plots should scale to the coefficient/CI data, not the
    custom-series row indexes used to draw CI bars."""

    chart_thresholds = {
        "main_regression": 0.05,
        "mechanism_regression": 0.08,
    }

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(
                f"{base_url}/?mode=full&open=demo-design-detail-figures#design",
                wait_until="domcontentloaded",
            )
            page.wait_for_load_state("networkidle")

            for chart_id, max_abs_extent in chart_thresholds.items():
                container = page.locator(f'[data-echart="{chart_id}"]')
                assert container.count() == 1
                container.first.scroll_into_view_if_needed()
                page.wait_for_function(
                    """
                    (chartId) => {
                        if (typeof echarts === "undefined") return false;
                        const el = document.querySelector(`[data-echart="${chartId}"]`);
                        if (!el) return false;
                        const inst = echarts.getInstanceByDom(el);
                        return inst != null && !el.classList.contains("echart-loading");
                    }
                    """,
                    arg=chart_id,
                    timeout=10_000,
                )

                chart_state = page.evaluate(
                    """
                    (chartId) => {
                        const el = document.querySelector(`[data-echart="${chartId}"]`);
                        const opt = echarts.getInstanceByDom(el).getOption();
                        return {
                            x_axis_min: opt.xAxis?.[0]?.min,
                            x_axis_max: opt.xAxis?.[0]?.max,
                        };
                    }
                    """,
                    arg=chart_id,
                )
                assert abs(float(chart_state["x_axis_min"])) < max_abs_extent
                assert abs(float(chart_state["x_axis_max"])) < max_abs_extent
        finally:
            browser.close()


def test_cross_market_section_hides_figures_in_brief_mode() -> None:
    """Brief mode should render the CMA section header but not show figures
    or the hypothesis map."""

    with (
        _running_dashboard_server() as base_url,
        playwright_sync_api.sync_playwright() as playwright,
    ):
        browser = _launch_chromium(playwright)
        try:
            page = _new_dashboard_page(browser, viewport={"width": 1440, "height": 960})
            page.goto(f"{base_url}/?mode=brief", wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            section = page.locator("section#cross_market_asymmetry")
            assert section.count() == 1
            assert section.locator("figure.cma-figure").count() == 0
            assert section.locator(".cma-hypothesis").count() == 0
            assert section.locator(".cma-verdict-card").count() == 0
        finally:
            browser.close()
