from __future__ import annotations

from index_inclusion_research.dashboard_refresh_coordinator import DashboardRefreshCoordinator


def _build_coordinator() -> DashboardRefreshCoordinator:
    return DashboardRefreshCoordinator(
        details_query_param="open",
        allowed_keys=frozenset({"demo-design-detail-tables"}),
        track_anchors=frozenset({"price_pressure_track"}),
        build_dashboard_snapshot_meta=lambda: {"label": "snapshot", "copy": "copy"},
        nav_sections_for_mode=lambda mode: [{"anchor": "overview", "label": "总览"}],
    )


def test_refresh_coordinator_normalizes_open_panels_and_anchor() -> None:
    coordinator = _build_coordinator()

    assert coordinator.normalize_open_panels("demo-design-detail-tables,unknown") == "demo-design-detail-tables"
    assert coordinator.normalize_anchor_for_mode("demo", "price_pressure_track") == "price_pressure_track"
    assert coordinator.normalize_anchor_for_mode("demo", "unknown") == "overview"


def test_refresh_coordinator_builds_redirect_and_status_payload() -> None:
    coordinator = _build_coordinator()
    redirect = coordinator.refresh_redirect_url(
        "demo",
        "price_pressure_track",
        open_panels="demo-design-detail-tables",
        url_builder=lambda **kwargs: (
            f"/?mode={kwargs['mode']}"
            + (f"&open={kwargs['open']}" if "open" in kwargs else "")
            + (f"#{kwargs['_anchor']}" if "_anchor" in kwargs else "")
        ),
    )

    assert redirect == "/?mode=demo&open=demo-design-detail-tables#price_pressure_track"

    payload = coordinator.refresh_status_payload(
        "demo",
        "overview",
        open_panels=None,
        redirect_url_builder=lambda mode, anchor, open_panels: f"/?mode={mode}#{anchor}",
        now_ts=123.0,
    )

    assert payload["snapshot_label"] == "snapshot"
    assert payload["status"] == "idle"
    assert payload["redirect_url"] == ""


def test_refresh_coordinator_queue_and_mark_success() -> None:
    coordinator = _build_coordinator()
    spawned: list[tuple[str, str]] = []

    queued = coordinator.queue_refresh_job(
        runner=lambda: None,
        scope_label="全部材料",
        scope_key="all",
        started_at="2026-04-17 12:00:00",
        started_ts=100.0,
        spawn_refresh_worker=lambda runner, scope_label, scope_key: spawned.append((scope_label, scope_key)),
    )

    assert queued is True
    assert spawned == [("全部材料", "all")]

    coordinator.mark_refresh_succeeded(
        "全部材料",
        "all",
        finished_at="2026-04-17 12:00:05",
        finished_ts=105.0,
    )

    assert coordinator.state["status"] == "succeeded"
    assert coordinator.state["scope_key"] == "all"
