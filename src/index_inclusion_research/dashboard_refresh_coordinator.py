from __future__ import annotations

from collections.abc import Callable, Mapping
from threading import Lock

from index_inclusion_research import dashboard_refresh


class DashboardRefreshCoordinator:
    def __init__(
        self,
        *,
        details_query_param: str,
        allowed_keys: set[str] | frozenset[str],
        track_anchors: set[str] | frozenset[str],
        build_dashboard_snapshot_meta: Callable[[], dict[str, object]],
        nav_sections_for_mode: Callable[[str], list[dict[str, str]]],
    ) -> None:
        self.details_query_param = details_query_param
        self.allowed_keys = allowed_keys
        self.track_anchors = track_anchors
        self.build_dashboard_snapshot_meta = build_dashboard_snapshot_meta
        self.nav_sections_for_mode = nav_sections_for_mode
        self.lock = Lock()
        self.state: dict[str, object] = dashboard_refresh.default_refresh_state()

    def refresh_timestamp(self) -> str:
        return dashboard_refresh.refresh_timestamp()

    def resolve_dashboard_mode(self, raw_mode: str | None) -> str:
        return dashboard_refresh.resolve_dashboard_mode(raw_mode)

    def normalize_open_panels(self, raw: str | None) -> str:
        return dashboard_refresh.normalize_open_panels(raw, allowed_keys=self.allowed_keys)

    def normalize_anchor_for_mode(self, mode: str, anchor: str | None) -> str:
        return dashboard_refresh.normalize_anchor_for_mode(
            mode,
            anchor,
            nav_sections=self.nav_sections_for_mode(mode),
            track_anchors=self.track_anchors,
        )

    def refresh_redirect_url(
        self,
        mode: str,
        anchor: str,
        *,
        open_panels: str | None = None,
        url_builder: Callable[..., str],
    ) -> str:
        return dashboard_refresh.refresh_redirect_url(
            mode,
            anchor,
            open_panels=open_panels,
            details_query_param=self.details_query_param,
            url_builder=url_builder,
            normalize_anchor=self.normalize_anchor_for_mode,
            normalize_open_panels=self.normalize_open_panels,
        )

    def refresh_poll_after_ms(self, status: str, started_ts: float, *, now_ts: float) -> int:
        return dashboard_refresh.refresh_poll_after_ms(status, started_ts, now_ts=now_ts)

    def refresh_duration_seconds(
        self,
        started_ts: float,
        finished_ts: float,
        status: str,
        *,
        now_ts: float,
    ) -> int | None:
        return dashboard_refresh.refresh_duration_seconds(
            started_ts,
            finished_ts,
            status,
            now_ts=now_ts,
        )

    def refresh_status_payload(
        self,
        mode: str,
        anchor: str,
        *,
        open_panels: str | None = None,
        redirect_url_builder: Callable[[str, str, str | None], str],
        now_ts: float,
    ) -> dict[str, object]:
        with self.lock:
            state = dict(self.state)
        return dashboard_refresh.refresh_status_payload(
            state,
            mode=mode,
            anchor=anchor,
            open_panels=open_panels,
            snapshot_meta=self.build_dashboard_snapshot_meta(),
            redirect_url_builder=redirect_url_builder,
            now_ts=now_ts,
        )

    def mark_refresh_succeeded(
        self,
        scope_label: str,
        scope_key: str,
        *,
        finished_at: str,
        finished_ts: float,
    ) -> None:
        snapshot_meta = self.build_dashboard_snapshot_meta()
        dashboard_refresh.set_refresh_succeeded(
            self.lock,
            self.state,
            scope_label=scope_label,
            scope_key=scope_key,
            snapshot_label=str(snapshot_meta["label"]),
            finished_at=finished_at,
            finished_ts=finished_ts,
        )

    def mark_refresh_failed(
        self,
        scope_label: str,
        scope_key: str,
        exc: Exception,
        *,
        finished_at: str,
        finished_ts: float,
    ) -> None:
        dashboard_refresh.set_refresh_failed(
            self.lock,
            self.state,
            scope_label=scope_label,
            scope_key=scope_key,
            error=str(exc),
            finished_at=finished_at,
            finished_ts=finished_ts,
        )

    def run_refresh_job(
        self,
        runner,
        scope_label: str,
        scope_key: str,
        *,
        mark_refresh_succeeded,
        mark_refresh_failed,
    ) -> None:
        dashboard_refresh.run_refresh_job(
            runner,
            scope_label,
            scope_key,
            mark_refresh_succeeded=mark_refresh_succeeded,
            mark_refresh_failed=mark_refresh_failed,
        )

    def spawn_refresh_worker(
        self,
        runner,
        scope_label: str,
        scope_key: str,
        *,
        run_refresh_job,
    ) -> None:
        dashboard_refresh.spawn_refresh_worker(
            runner,
            scope_label,
            scope_key,
            run_refresh_job=run_refresh_job,
        )

    def queue_refresh_job(
        self,
        runner,
        scope_label: str,
        scope_key: str,
        *,
        started_at: str,
        started_ts: float,
        spawn_refresh_worker,
    ) -> bool:
        return dashboard_refresh.queue_refresh_job(
            self.lock,
            self.state,
            runner=runner,
            scope_label=scope_label,
            scope_key=scope_key,
            started_at=started_at,
            started_ts=started_ts,
            spawn_refresh_worker=spawn_refresh_worker,
        )

    def wants_async_refresh(self, headers: Mapping[str, str]) -> bool:
        return dashboard_refresh.wants_async_refresh(headers)
