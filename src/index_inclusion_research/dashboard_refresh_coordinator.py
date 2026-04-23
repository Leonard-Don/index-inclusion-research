from __future__ import annotations

from collections.abc import Mapping
from threading import Lock

from index_inclusion_research import dashboard_refresh
from index_inclusion_research.dashboard_types import (
    HomeUrlBuilder,
    ModeName,
    NavSectionsBuilder,
    RddContractCheckBuilder,
    RefreshFailureHandler,
    RefreshJobRunner,
    RefreshRedirectUrlBuilder,
    RefreshRunner,
    RefreshState,
    RefreshStatus,
    RefreshStatusPayload,
    RefreshSuccessHandler,
    RefreshWorkerSpawner,
    RelativePathBuilder,
    SnapshotMetaBuilder,
    SnapshotSourcesBuilder,
)


class DashboardRefreshCoordinator:
    def __init__(
        self,
        *,
        details_query_param: str,
        allowed_keys: set[str] | frozenset[str],
        track_anchors: set[str] | frozenset[str],
        dashboard_snapshot_sources: SnapshotSourcesBuilder,
        build_rdd_contract_check: RddContractCheckBuilder,
        to_relative: RelativePathBuilder,
        build_dashboard_snapshot_meta: SnapshotMetaBuilder,
        nav_sections_for_mode: NavSectionsBuilder,
    ) -> None:
        self.details_query_param = details_query_param
        self.allowed_keys = allowed_keys
        self.track_anchors = track_anchors
        self.dashboard_snapshot_sources = dashboard_snapshot_sources
        self.build_rdd_contract_check = build_rdd_contract_check
        self.to_relative = to_relative
        self.build_dashboard_snapshot_meta = build_dashboard_snapshot_meta
        self.nav_sections_for_mode = nav_sections_for_mode
        self.lock = Lock()
        self.state: RefreshState = dashboard_refresh.default_refresh_state()

    def refresh_timestamp(self) -> str:
        return dashboard_refresh.refresh_timestamp()

    def resolve_dashboard_mode(self, raw_mode: str | None) -> ModeName:
        return dashboard_refresh.resolve_dashboard_mode(raw_mode)

    def normalize_open_panels(self, raw: str | None) -> str:
        return dashboard_refresh.normalize_open_panels(raw, allowed_keys=self.allowed_keys)

    def normalize_anchor_for_mode(self, mode: ModeName, anchor: str | None) -> str:
        return dashboard_refresh.normalize_anchor_for_mode(
            mode,
            anchor,
            nav_sections=self.nav_sections_for_mode(mode),
            track_anchors=self.track_anchors,
        )

    def refresh_redirect_url(
        self,
        mode: ModeName,
        anchor: str,
        *,
        open_panels: str | None = None,
        url_builder: HomeUrlBuilder,
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

    def refresh_poll_after_ms(self, status: RefreshStatus, started_ts: float, *, now_ts: float) -> int:
        return dashboard_refresh.refresh_poll_after_ms(status, started_ts, now_ts=now_ts)

    def refresh_duration_seconds(
        self,
        started_ts: float,
        finished_ts: float,
        status: RefreshStatus,
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
        mode: ModeName,
        anchor: str,
        *,
        open_panels: str | None = None,
        redirect_url_builder: RefreshRedirectUrlBuilder,
        now_ts: float,
    ) -> RefreshStatusPayload:
        with self.lock:
            state: RefreshState = dict(self.state)  # type: ignore[assignment]
        return dashboard_refresh.refresh_status_payload(
            state,
            mode=mode,
            anchor=anchor,
            open_panels=open_panels,
            snapshot_meta=self.build_dashboard_snapshot_meta(),
            contract_check=self.build_rdd_contract_check(),
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
        with self.lock:
            baseline_artifact_mtimes = dict(self.state.get("baseline_artifact_mtimes", {}))
        snapshot_files = self.dashboard_snapshot_sources()
        snapshot_meta = self.build_dashboard_snapshot_meta(snapshot_files)
        contract_check = self.build_rdd_contract_check()
        updated_artifacts = dashboard_refresh.build_updated_artifacts(
            snapshot_files,
            baseline_artifact_mtimes=baseline_artifact_mtimes,
            to_relative=self.to_relative,
        )
        dashboard_refresh.set_refresh_succeeded(
            self.lock,
            self.state,
            scope_label=scope_label,
            scope_key=scope_key,
            snapshot_meta=snapshot_meta,
            contract_check=contract_check,
            updated_artifacts=updated_artifacts,
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
        runner: RefreshRunner,
        scope_label: str,
        scope_key: str,
        *,
        mark_refresh_succeeded: RefreshSuccessHandler,
        mark_refresh_failed: RefreshFailureHandler,
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
        runner: RefreshRunner,
        scope_label: str,
        scope_key: str,
        *,
        run_refresh_job: RefreshJobRunner,
    ) -> None:
        dashboard_refresh.spawn_refresh_worker(
            runner,
            scope_label,
            scope_key,
            run_refresh_job=run_refresh_job,
        )

    def queue_refresh_job(
        self,
        runner: RefreshRunner,
        scope_label: str,
        scope_key: str,
        *,
        started_at: str,
        started_ts: float,
        spawn_refresh_worker: RefreshWorkerSpawner,
    ) -> bool:
        snapshot_files = self.dashboard_snapshot_sources()
        snapshot_meta = self.build_dashboard_snapshot_meta(snapshot_files)
        baseline_artifact_mtimes = dashboard_refresh.snapshot_artifact_mtimes(
            snapshot_files,
            to_relative=self.to_relative,
        )
        return dashboard_refresh.queue_refresh_job(
            self.lock,
            self.state,
            runner=runner,
            scope_label=scope_label,
            scope_key=scope_key,
            started_at=started_at,
            started_ts=started_ts,
            snapshot_meta=snapshot_meta,
            baseline_artifact_mtimes=baseline_artifact_mtimes,
            spawn_refresh_worker=spawn_refresh_worker,
        )

    def wants_async_refresh(self, headers: Mapping[str, str]) -> bool:
        return dashboard_refresh.wants_async_refresh(headers)
