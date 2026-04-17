from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol, TypeAlias, TypedDict

DashboardCard: TypeAlias = dict[str, str]
AnalysisResult: TypeAlias = dict[str, object]
ModeTab: TypeAlias = dict[str, object]
PaperDetailResult: TypeAlias = dict[str, object]
RefreshStatusPayload: TypeAlias = dict[str, object]
RouteHandler: TypeAlias = Callable[..., object]


class AnalysisRunner(Protocol):
    def __call__(self, verbose: bool = False) -> AnalysisResult: ...


class AnalysisDefinition(TypedDict):
    title: str
    subtitle: str
    description_zh: str
    project_module: str
    runner: AnalysisRunner


AnalysesConfig: TypeAlias = dict[str, AnalysisDefinition]


class RequestValuesLike(Protocol):
    def get(self, key: str, default: str | None = None) -> str | None: ...

    def __contains__(self, key: object) -> bool: ...


class RequestProxyLike(Protocol):
    args: RequestValuesLike
    form: RequestValuesLike
    headers: Mapping[str, str]


class TimeModuleLike(Protocol):
    def time(self) -> float: ...


class EndpointUrlBuilder(Protocol):
    def __call__(self, endpoint: str, **values: Any) -> str: ...


class HomeUrlBuilder(Protocol):
    def __call__(self, **values: Any) -> str: ...


class HomeAnchorUrlBuilder(Protocol):
    def __call__(self, anchor: str) -> str: ...


class RefreshStatusUrlBuilder(Protocol):
    def __call__(self) -> str: ...


class LiteraturePaperLookup(Protocol):
    def __call__(self, paper_id: str) -> Any: ...


class DashboardRouteRegistrationMap(TypedDict):
    favicon_view: RouteHandler
    home_view: RouteHandler
    refresh_dashboard_view: RouteHandler
    refresh_status_view: RouteHandler
    run_analysis_view: RouteHandler
    show_library_view: RouteHandler
    show_review_view: RouteHandler
    show_framework_view: RouteHandler
    show_supplement_view: RouteHandler
    show_analysis_view: RouteHandler
    serve_result_file_view: RouteHandler
    show_paper_brief_view: RouteHandler
    serve_library_pdf_view: RouteHandler
