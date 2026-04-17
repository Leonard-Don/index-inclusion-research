from __future__ import annotations

from dataclasses import dataclass

from index_inclusion_research.dashboard_refresh_coordinator import DashboardRefreshCoordinator
from index_inclusion_research.dashboard_runtime import DashboardRuntime
from index_inclusion_research.dashboard_types import (
    AnalysisResult,
    HomeUrlBuilder,
    ModeTab,
    PaperDetailResult,
    RefreshStatusPayload,
    RequestProxyLike,
    TimeModuleLike,
)


@dataclass
class DashboardServices:
    runtime: DashboardRuntime
    refresh_coordinator: DashboardRefreshCoordinator
    request_proxy: RequestProxyLike
    time_module: TimeModuleLike
    details_query_param: str
    home_url_builder: HomeUrlBuilder

    def refresh_timestamp(self) -> str:
        return self.refresh_coordinator.refresh_timestamp()

    def dashboard_mode(self) -> str:
        return self.refresh_coordinator.resolve_dashboard_mode(self.request_proxy.args.get("mode", "demo"))

    def normalize_open_panels(self, raw: str | None) -> str:
        return self.refresh_coordinator.normalize_open_panels(raw)

    def normalize_anchor_for_mode(self, mode: str, anchor: str | None) -> str:
        return self.refresh_coordinator.normalize_anchor_for_mode(mode, anchor)

    def refresh_redirect_url(self, mode: str, anchor: str, open_panels: str | None = None) -> str:
        return self.refresh_coordinator.refresh_redirect_url(
            mode,
            anchor,
            open_panels=open_panels,
            url_builder=self.home_url_builder,
        )

    def refresh_poll_after_ms(self, status: str, started_ts: float) -> int:
        return self.refresh_coordinator.refresh_poll_after_ms(
            status,
            started_ts,
            now_ts=self.time_module.time(),
        )

    def refresh_duration_seconds(self, started_ts: float, finished_ts: float, status: str) -> int | None:
        return self.refresh_coordinator.refresh_duration_seconds(
            started_ts,
            finished_ts,
            status,
            now_ts=self.time_module.time(),
        )

    def refresh_status_payload(
        self,
        mode: str,
        anchor: str,
        open_panels: str | None = None,
    ) -> RefreshStatusPayload:
        return self.refresh_coordinator.refresh_status_payload(
            mode=mode,
            anchor=anchor,
            open_panels=open_panels,
            redirect_url_builder=self.refresh_redirect_url,
            now_ts=self.time_module.time(),
        )

    def mark_refresh_succeeded(self, scope_label: str, scope_key: str) -> None:
        self.refresh_coordinator.mark_refresh_succeeded(
            scope_label=scope_label,
            scope_key=scope_key,
            finished_at=self.refresh_timestamp(),
            finished_ts=self.time_module.time(),
        )

    def mark_refresh_failed(self, scope_label: str, scope_key: str, exc: Exception) -> None:
        self.refresh_coordinator.mark_refresh_failed(
            scope_label=scope_label,
            scope_key=scope_key,
            exc=exc,
            finished_at=self.refresh_timestamp(),
            finished_ts=self.time_module.time(),
        )

    def run_refresh_job(self, runner, scope_label: str, scope_key: str) -> None:
        self.refresh_coordinator.run_refresh_job(
            runner,
            scope_label,
            scope_key,
            mark_refresh_succeeded=self.mark_refresh_succeeded,
            mark_refresh_failed=self.mark_refresh_failed,
        )

    def spawn_refresh_worker(self, runner, scope_label: str, scope_key: str) -> None:
        self.refresh_coordinator.spawn_refresh_worker(
            runner,
            scope_label,
            scope_key,
            run_refresh_job=self.run_refresh_job,
        )

    def queue_refresh_job(self, runner, scope_label: str, scope_key: str) -> bool:
        return self.refresh_coordinator.queue_refresh_job(
            runner=runner,
            scope_label=scope_label,
            scope_key=scope_key,
            started_at=self.refresh_timestamp(),
            started_ts=self.time_module.time(),
            spawn_refresh_worker=self.spawn_refresh_worker,
        )

    def wants_async_refresh(self) -> bool:
        return self.refresh_coordinator.wants_async_refresh(self.request_proxy.headers)

    def mode_tabs_for_mode(self, mode: str, open_panels: str | None = None) -> list[ModeTab]:
        return self.runtime.mode_tabs_for_mode(
            mode,
            lambda tab_mode, anchor=None: (
                self.home_url_builder(
                    mode=tab_mode,
                    **(
                        {self.details_query_param: self.normalize_open_panels(open_panels)}
                        if open_panels is not None
                        else {}
                    ),
                )
                if anchor is None
                else self.home_url_builder(
                    mode=tab_mode,
                    _anchor=anchor,
                    **(
                        {self.details_query_param: self.normalize_open_panels(open_panels)}
                        if open_panels is not None
                        else {}
                    ),
                )
            ),
        )

    def run_and_cache_all(self) -> None:
        self.runtime.run_and_cache_all()

    def run_and_cache_analysis(self, analysis_id: str) -> AnalysisResult:
        return self.runtime.run_and_cache_analysis(analysis_id)

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None:
        return self.runtime.load_paper_detail_result(paper_id)


def build_dashboard_services(
    *,
    runtime: DashboardRuntime,
    refresh_coordinator: DashboardRefreshCoordinator,
    request_proxy: RequestProxyLike,
    time_module: TimeModuleLike,
    details_query_param: str,
    home_url_builder: HomeUrlBuilder,
) -> DashboardServices:
    return DashboardServices(
        runtime=runtime,
        refresh_coordinator=refresh_coordinator,
        request_proxy=request_proxy,
        time_module=time_module,
        details_query_param=details_query_param,
        home_url_builder=home_url_builder,
    )
