from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from index_inclusion_research import dashboard_routes
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    DashboardRuntimeLike,
    DashboardRouteRegistrationMap,
    EndpointUrlBuilder,
    HomeAnchorUrlBuilder,
    HomeUrlBuilder,
    LiteraturePaperLookup,
    ModeName,
    ModeTab,
    PaperDetailResult,
    RefreshRunner,
    RefreshStatusPayload,
    RefreshStatusUrlBuilder,
    RequestProxyLike,
    RouteView,
    TrackResult,
)


class DashboardRouteServices(Protocol):
    request_proxy: RequestProxyLike
    runtime: DashboardRuntimeLike

    def dashboard_mode(self) -> ModeName: ...

    def normalize_open_panels(self, raw: str | None) -> str: ...

    def mode_tabs_for_mode(self, mode: ModeName, open_panels: str | None = None) -> list[ModeTab]: ...

    def refresh_status_payload(self, mode: ModeName, anchor: str, open_panels: str | None = None) -> RefreshStatusPayload: ...

    def normalize_anchor_for_mode(self, mode: ModeName, anchor: str | None) -> str: ...

    def wants_async_refresh(self) -> bool: ...

    def queue_refresh_job(self, runner: RefreshRunner, scope_label: str, scope_key: str) -> bool: ...

    def run_and_cache_all(self) -> None: ...

    def run_and_cache_analysis(self, analysis_id: str) -> TrackResult: ...

    def refresh_redirect_url(self, mode: ModeName, anchor: str, open_panels: str | None = None) -> str: ...

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None: ...


@dataclass(frozen=True)
class DashboardRouteDependencies:
    request_proxy: RequestProxyLike
    runtime: DashboardRuntimeLike
    services: DashboardRouteServices

    def dashboard_mode(self) -> ModeName:
        return self.services.dashboard_mode()

    def normalize_open_panels(self, raw: str | None) -> str:
        return self.services.normalize_open_panels(raw)

    def mode_tabs_for_mode(self, mode: ModeName, open_panels: str | None = None) -> list[ModeTab]:
        return self.services.mode_tabs_for_mode(mode, open_panels)

    def refresh_status_payload(self, mode: ModeName, anchor: str, open_panels: str | None = None) -> RefreshStatusPayload:
        return self.services.refresh_status_payload(mode, anchor, open_panels)

    def normalize_anchor_for_mode(self, mode: ModeName, anchor: str | None) -> str:
        return self.services.normalize_anchor_for_mode(mode, anchor)

    def wants_async_refresh(self) -> bool:
        return self.services.wants_async_refresh()

    def queue_refresh_job(self, runner: RefreshRunner, scope_label: str, scope_key: str) -> bool:
        return self.services.queue_refresh_job(runner, scope_label, scope_key)

    def run_and_cache_all(self) -> None:
        self.services.run_and_cache_all()

    def run_and_cache_analysis(self, analysis_id: str) -> TrackResult:
        return self.services.run_and_cache_analysis(analysis_id)

    def refresh_redirect_url(self, mode: ModeName, anchor: str, open_panels: str | None = None) -> str:
        return self.services.refresh_redirect_url(mode, anchor, open_panels)

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None:
        return self.services.load_paper_detail_result(paper_id)


@dataclass(frozen=True)
class DashboardRouteViews:
    favicon: RouteView
    home: RouteView
    refresh_dashboard: RouteView
    refresh_status: RouteView
    run_analysis: RouteView
    show_library: RouteView
    show_review: RouteView
    show_framework: RouteView
    show_supplement: RouteView
    show_analysis: RouteView
    serve_result_file: RouteView
    show_paper_brief: RouteView
    serve_library_pdf: RouteView

    def route_namespace(self) -> dict[str, RouteView]:
        return {
            "favicon": self.favicon,
            "home": self.home,
            "refresh_dashboard": self.refresh_dashboard,
            "refresh_status": self.refresh_status,
            "run_analysis": self.run_analysis,
            "show_library": self.show_library,
            "show_review": self.show_review,
            "show_framework": self.show_framework,
            "show_supplement": self.show_supplement,
            "show_analysis": self.show_analysis,
            "serve_result_file": self.serve_result_file,
            "show_paper_brief": self.show_paper_brief,
            "serve_library_pdf": self.serve_library_pdf,
        }

    def registration_kwargs(self) -> DashboardRouteRegistrationMap:
        return {
            "favicon_view": self.favicon,
            "home_view": self.home,
            "refresh_dashboard_view": self.refresh_dashboard,
            "refresh_status_view": self.refresh_status,
            "run_analysis_view": self.run_analysis,
            "show_library_view": self.show_library,
            "show_review_view": self.show_review,
            "show_framework_view": self.show_framework,
            "show_supplement_view": self.show_supplement,
            "show_analysis_view": self.show_analysis,
            "serve_result_file_view": self.serve_result_file,
            "show_paper_brief_view": self.show_paper_brief,
            "serve_library_pdf_view": self.serve_library_pdf,
        }


@dataclass(frozen=True)
class DashboardRouteFactory:
    dependencies: DashboardRouteDependencies
    url_builder: EndpointUrlBuilder
    details_query_param: str
    analyses: AnalysesConfig
    root: Path
    get_literature_paper: LiteraturePaperLookup

    def build_views(self) -> DashboardRouteViews:
        home_anchor_url_builder = _build_home_anchor_url_builder(self.url_builder)
        refresh_status_url_builder = _build_refresh_status_url_builder(self.url_builder)
        dependencies = self.dependencies
        return DashboardRouteViews(
            favicon=empty_favicon,
            home=dashboard_routes.build_home_view(
                details_query_param=self.details_query_param,
                request_proxy=dependencies.request_proxy,
                dashboard_mode=dependencies.dashboard_mode,
                normalize_open_panels=dependencies.normalize_open_panels,
                runtime=dependencies.runtime,
                mode_tabs_for_mode=dependencies.mode_tabs_for_mode,
                refresh_status_payload=dependencies.refresh_status_payload,
                refresh_status_url_builder=refresh_status_url_builder,
            ),
            refresh_dashboard=dashboard_routes.build_refresh_dashboard_view(
                details_query_param=self.details_query_param,
                request_proxy=dependencies.request_proxy,
                dashboard_mode=dependencies.dashboard_mode,
                normalize_anchor_for_mode=dependencies.normalize_anchor_for_mode,
                normalize_open_panels=dependencies.normalize_open_panels,
                wants_async_refresh=dependencies.wants_async_refresh,
                queue_refresh_job=dependencies.queue_refresh_job,
                run_and_cache_all=dependencies.run_and_cache_all,
                refresh_status_payload=dependencies.refresh_status_payload,
                refresh_redirect_url=dependencies.refresh_redirect_url,
            ),
            refresh_status=dashboard_routes.build_refresh_status_view(
                details_query_param=self.details_query_param,
                request_proxy=dependencies.request_proxy,
                dashboard_mode=dependencies.dashboard_mode,
                normalize_anchor_for_mode=dependencies.normalize_anchor_for_mode,
                normalize_open_panels=dependencies.normalize_open_panels,
                refresh_status_payload=dependencies.refresh_status_payload,
            ),
            run_analysis=dashboard_routes.build_run_analysis_view(
                details_query_param=self.details_query_param,
                request_proxy=dependencies.request_proxy,
                analyses=self.analyses,
                dashboard_mode=dependencies.dashboard_mode,
                normalize_anchor_for_mode=dependencies.normalize_anchor_for_mode,
                normalize_open_panels=dependencies.normalize_open_panels,
                wants_async_refresh=dependencies.wants_async_refresh,
                queue_refresh_job=dependencies.queue_refresh_job,
                refresh_status_payload=dependencies.refresh_status_payload,
                run_and_cache_analysis=dependencies.run_and_cache_analysis,
                refresh_redirect_url=dependencies.refresh_redirect_url,
            ),
            show_library=dashboard_routes.build_anchor_redirect_view(
                "framework",
                home_url_builder=home_anchor_url_builder,
            ),
            show_review=dashboard_routes.build_anchor_redirect_view(
                "framework",
                home_url_builder=home_anchor_url_builder,
            ),
            show_framework=dashboard_routes.build_anchor_redirect_view(
                "framework",
                home_url_builder=home_anchor_url_builder,
            ),
            show_supplement=dashboard_routes.build_anchor_redirect_view(
                "supplement",
                home_url_builder=home_anchor_url_builder,
            ),
            show_analysis=dashboard_routes.build_analysis_redirect_view(
                analyses=self.analyses,
                home_url_builder=home_anchor_url_builder,
            ),
            serve_result_file=dashboard_routes.build_serve_result_file_view(self.root),
            show_paper_brief=dashboard_routes.build_show_paper_brief_view(
                request_proxy=dependencies.request_proxy,
                load_paper_detail_result=dependencies.load_paper_detail_result,
            ),
            serve_library_pdf=dashboard_routes.build_serve_library_pdf_view(
                get_literature_paper=self.get_literature_paper,
            ),
        )


def empty_favicon() -> tuple[str, int]:
    return ("", 204)


def build_home_url_builder(url_builder: EndpointUrlBuilder) -> HomeUrlBuilder:
    def home_url(**kwargs: Any) -> str:
        return url_builder("home", **kwargs)

    return home_url


def build_dashboard_route_dependencies(services: DashboardRouteServices) -> DashboardRouteDependencies:
    return DashboardRouteDependencies(
        request_proxy=services.request_proxy,
        runtime=services.runtime,
        services=services,
    )


def _build_home_anchor_url_builder(url_builder: EndpointUrlBuilder) -> HomeAnchorUrlBuilder:
    home_url = build_home_url_builder(url_builder)

    def home_anchor_url(anchor: str) -> str:
        return home_url(_anchor=anchor)

    return home_anchor_url


def _build_refresh_status_url_builder(url_builder: EndpointUrlBuilder) -> RefreshStatusUrlBuilder:
    def refresh_status_url() -> str:
        return url_builder("refresh_status")

    return refresh_status_url


def build_dashboard_route_views(
    *,
    services: DashboardRouteServices,
    url_builder: EndpointUrlBuilder,
    details_query_param: str,
    analyses: AnalysesConfig,
    root: Path,
    get_literature_paper: LiteraturePaperLookup,
) -> DashboardRouteViews:
    return DashboardRouteFactory(
        dependencies=build_dashboard_route_dependencies(services),
        url_builder=url_builder,
        details_query_param=details_query_param,
        analyses=analyses,
        root=root,
        get_literature_paper=get_literature_paper,
    ).build_views()


def bind_dashboard_route_views(namespace: dict[str, RouteView], route_views: DashboardRouteViews) -> None:
    namespace.update(route_views.route_namespace())
