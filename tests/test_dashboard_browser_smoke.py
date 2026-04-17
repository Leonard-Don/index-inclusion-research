from __future__ import annotations

from contextlib import contextmanager
import http.client
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

import pytest


pytestmark = pytest.mark.browser_smoke

if os.environ.get("RUN_BROWSER_SMOKE") != "1":
    pytest.skip("Set RUN_BROWSER_SMOKE=1 to run browser smoke tests.", allow_module_level=True)

playwright_sync_api = pytest.importorskip("playwright.sync_api")


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_SCRIPT = ROOT / "scripts" / "start_literature_dashboard.py"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_dashboard(url: str, proc: subprocess.Popen[str], timeout_seconds: float = 60.0) -> None:
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
            raise RuntimeError(f"Dashboard server exited before becoming ready.\n{stdout}")
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


def _wheel_until_hash(page: playwright_sync_api.Page, expected_hash: str, deltas: list[int]) -> None:
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
        [sys.executable, str(DASHBOARD_SCRIPT), "--port", str(port)],
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

    with _running_dashboard_server() as base_url, playwright_sync_api.sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 960})
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        page.goto(f"{base_url}/?mode=demo", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        assert "16 篇文献" in page.locator("h1").inner_text()
        assert page.locator("a.skip-link").get_attribute("href") == "#main-content"
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
        assert page.locator("[data-open-input]").first.input_value() == "demo-design-detail-tables"
        reloaded_url = page.url
        page.goto(reloaded_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        assert page.locator("[data-details-key='demo-design-detail-tables']").evaluate("node => node.open") is True

        page.get_by_role("link", name="文献框架").click()
        _wait_for_section_state(page, "#framework", "文献框架")
        assert "open=demo-design-detail-tables" in page.url
        active_sections = [text.strip() for text in page.locator("[data-section-link].active").all_inner_texts()]
        assert "文献框架" in active_sections
        assert page.locator("[data-section-link][aria-current='location']").first.inner_text().strip() == "文献框架"
        assert page.locator("[data-waypoint-dock]").get_attribute("data-visible") == "true"
        assert page.locator("[data-waypoint-title]").inner_text().strip() == "文献框架"

        page.get_by_role("button", name="下一节").click()
        _wait_for_section_state(page, "#supplement", "机制补充")
        assert page.locator("[data-waypoint-title]").inner_text().strip() == "机制补充"
        active_sections = [text.strip() for text in page.locator("[data-section-link].active").all_inner_texts()]
        assert "机制补充" in active_sections

        page.goto(f"{base_url}/?mode=demo#tracks", wait_until="domcontentloaded")
        page.locator("#tracks").wait_for()
        page.wait_for_function(
            "() => ['#tracks', '#price_pressure_track', '#demand_curve_track', '#identification_china_track'].includes(window.location.hash)"
        )
        page.locator("#tracks").evaluate("el => el.scrollIntoView({ block: 'start', behavior: 'instant' })")
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
        assert page.locator("[data-waypoint-title]").inner_text().strip() == "主线结果 / 短期价格压力与效应减弱"
        active_sections = [text.strip() for text in page.locator("[data-section-link].active").all_inner_texts()]
        assert "主线结果" in active_sections

        page.goto(f"{base_url}/paper/harris_gurel_1986?view=track", wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        assert page.locator("a.skip-link").get_attribute("href") == "#main-content"
        assert page.get_by_role("button", name="按主线").get_attribute("aria-pressed") == "true"
        assert page.locator("#evolution-view-track").is_visible()
        assert page.locator("#evolution-view-camp").is_hidden()

        mobile_page = browser.new_page(viewport={"width": 390, "height": 844})
        mobile_page.goto(f"{base_url}/?mode=demo", wait_until="domcontentloaded")
        mobile_page.wait_for_load_state("networkidle")
        mobile_menu_toggle = mobile_page.locator("[data-waypoint-menu-toggle].mobile-only")
        mobile_menu_toggle.click()
        assert mobile_menu_toggle.get_attribute("aria-expanded") == "true"
        assert mobile_page.locator("[data-waypoint-menu]").get_attribute("data-open") == "true"
        mobile_page.locator("[data-waypoint-menu-link][href='#identification_china_track']").click()
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
        assert mobile_page.locator("[data-waypoint-menu-toggle].mobile-only").get_attribute("aria-expanded") == "false"
        assert mobile_page.locator("[data-waypoint-dock]").get_attribute("data-visible") == "true"
        mobile_page.close()

        browser.close()

    assert page_errors == []
    assert console_errors == []
