from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from flask import abort, jsonify, redirect, render_template, send_file

from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisResult,
    ModeTab,
    RefreshStatusPayload,
    RequestProxyLike,
)


@dataclass(frozen=True)
class DashboardHomeRequestState:
    display_mode: str
    current_open_panels: str | None


@dataclass(frozen=True)
class DashboardRouteState:
    mode: str
    anchor: str
    open_panels: str | None


@dataclass(frozen=True)
class DashboardRequestAdapter:
    details_query_param: str
    request_proxy: RequestProxyLike
    dashboard_mode: Callable[[], str]
    normalize_open_panels: Callable[[str | None], str]
    normalize_anchor_for_mode: Callable[[str, str | None], str] | None = None

    def _normalized_open_panels(self, values) -> str | None:
        if self.details_query_param not in values:
            return None
        return self.normalize_open_panels(values.get(self.details_query_param))

    def home_state(self) -> DashboardHomeRequestState:
        return DashboardHomeRequestState(
            display_mode=self.dashboard_mode(),
            current_open_panels=self._normalized_open_panels(self.request_proxy.args),
        )

    def args_state(self, *, default_anchor: str) -> DashboardRouteState:
        mode = self.dashboard_mode()
        if self.normalize_anchor_for_mode is None:
            raise RuntimeError("normalize_anchor_for_mode is required for route state parsing")
        return DashboardRouteState(
            mode=mode,
            anchor=self.normalize_anchor_for_mode(mode, self.request_proxy.args.get("anchor", default_anchor)),
            open_panels=self._normalized_open_panels(self.request_proxy.args),
        )

    def form_state(self, *, default_anchor: str) -> DashboardRouteState:
        mode = self.dashboard_mode()
        if self.normalize_anchor_for_mode is None:
            raise RuntimeError("normalize_anchor_for_mode is required for route state parsing")
        return DashboardRouteState(
            mode=mode,
            anchor=self.normalize_anchor_for_mode(mode, self.request_proxy.form.get("anchor", default_anchor)),
            open_panels=self._normalized_open_panels(self.request_proxy.form),
        )


@dataclass(frozen=True)
class DashboardPaperRequestAdapter:
    request_proxy: RequestProxyLike

    def evolution_view(self) -> str:
        return self.request_proxy.args.get("view", "camp") or "camp"


@dataclass(frozen=True)
class DashboardHomeRenderer:
    runtime: object
    mode_tabs_for_mode: Callable[[str, str | None], list[ModeTab]]
    refresh_status_payload: Callable[[str, str, str | None], RefreshStatusPayload]
    refresh_status_url_builder: Callable[[], str]

    def render(self, request_state: DashboardHomeRequestState):
        return render_home(
            runtime=self.runtime,
            display_mode=request_state.display_mode,
            current_open_panels=request_state.current_open_panels,
            mode_tabs_for_mode=self.mode_tabs_for_mode,
            refresh_status_payload=self.refresh_status_payload,
            refresh_status_url=self.refresh_status_url_builder(),
        )


@dataclass(frozen=True)
class DashboardRefreshHandler:
    wants_async_refresh: Callable[[], bool]
    queue_refresh_job: Callable[[Callable[[], None], str, str], object]
    run_and_cache_all: Callable[[], None]
    refresh_status_payload: Callable[[str, str, str | None], RefreshStatusPayload]
    refresh_redirect_url: Callable[[str, str, str | None], str]

    def handle(self, request_state: DashboardRouteState):
        if self.wants_async_refresh():
            self.queue_refresh_job(self.run_and_cache_all, "全部材料", "all")
            return jsonify(
                self.refresh_status_payload(
                    request_state.mode,
                    request_state.anchor,
                    request_state.open_panels,
                )
            ), 202
        self.run_and_cache_all()
        return redirect(
            self.refresh_redirect_url(
                request_state.mode,
                request_state.anchor,
                request_state.open_panels,
            )
        )


@dataclass(frozen=True)
class DashboardRefreshStatusHandler:
    refresh_status_payload: Callable[[str, str, str | None], RefreshStatusPayload]

    def handle(self, request_state: DashboardRouteState):
        return jsonify(
            self.refresh_status_payload(
                request_state.mode,
                request_state.anchor,
                request_state.open_panels,
            )
        )


@dataclass(frozen=True)
class DashboardRunAnalysisHandler:
    analyses: AnalysesConfig
    wants_async_refresh: Callable[[], bool]
    queue_refresh_job: Callable[[Callable[[], None], str, str], object]
    refresh_status_payload: Callable[[str, str, str | None], RefreshStatusPayload]
    run_and_cache_analysis: Callable[[str], AnalysisResult]
    refresh_redirect_url: Callable[[str, str, str | None], str]

    def handle(self, analysis_id: str, request_state: DashboardRouteState):
        config = self.analyses.get(analysis_id)
        if not config:
            abort(404)
        if self.wants_async_refresh():
            self.queue_refresh_job(lambda: self.run_and_cache_analysis(analysis_id), config["title"], analysis_id)
            return jsonify(
                self.refresh_status_payload(
                    request_state.mode,
                    request_state.anchor,
                    request_state.open_panels,
                )
            ), 202
        self.run_and_cache_analysis(analysis_id)
        return redirect(
            self.refresh_redirect_url(
                request_state.mode,
                request_state.anchor,
                request_state.open_panels,
            )
        )


def render_home(
    *,
    runtime,
    display_mode: str,
    current_open_panels: str | None,
    mode_tabs_for_mode,
    refresh_status_payload,
    refresh_status_url: str,
):
    return render_template(
        "dashboard.html",
        **runtime.build_home_context(
            display_mode=display_mode,
            current_open_panels=current_open_panels,
            mode_tabs_for_mode=mode_tabs_for_mode,
            refresh_status_payload=refresh_status_payload,
            refresh_status_url=refresh_status_url,
        ),
    )


def handle_refresh(
    *,
    mode: str,
    anchor: str,
    open_panels: str | None,
    wants_async_refresh: bool,
    queue_refresh_job,
    run_and_cache_all,
    refresh_status_payload,
    refresh_redirect_url,
):
    return DashboardRefreshHandler(
        wants_async_refresh=lambda: wants_async_refresh,
        queue_refresh_job=queue_refresh_job,
        run_and_cache_all=run_and_cache_all,
        refresh_status_payload=refresh_status_payload,
        refresh_redirect_url=refresh_redirect_url,
    ).handle(
        DashboardRouteState(
            mode=mode,
            anchor=anchor,
            open_panels=open_panels,
        )
    )


def handle_refresh_status(
    *,
    mode: str,
    anchor: str,
    open_panels: str | None,
    refresh_status_payload,
):
    return DashboardRefreshStatusHandler(
        refresh_status_payload=refresh_status_payload,
    ).handle(
        DashboardRouteState(
            mode=mode,
            anchor=anchor,
            open_panels=open_panels,
        )
    )


def handle_run_analysis(
    analysis_id: str,
    *,
    analyses,
    mode: str,
    anchor: str,
    open_panels: str | None,
    wants_async_refresh: bool,
    queue_refresh_job,
    refresh_status_payload,
    run_and_cache_analysis,
    refresh_redirect_url,
):
    return DashboardRunAnalysisHandler(
        analyses=analyses,
        wants_async_refresh=lambda: wants_async_refresh,
        queue_refresh_job=queue_refresh_job,
        refresh_status_payload=refresh_status_payload,
        run_and_cache_analysis=run_and_cache_analysis,
        refresh_redirect_url=refresh_redirect_url,
    ).handle(
        analysis_id,
        DashboardRouteState(
            mode=mode,
            anchor=anchor,
            open_panels=open_panels,
        ),
    )


def redirect_home_anchor(anchor: str, *, home_url_builder):
    return redirect(home_url_builder(anchor))


def serve_result_file(root: Path, subpath: str):
    full_path = (root / subpath).resolve()
    resolved_root = root.resolve()
    if resolved_root not in full_path.parents and full_path != resolved_root:
        abort(404)
    if not full_path.exists() or not full_path.is_file():
        abort(404)
    return send_file(full_path)


def render_paper_brief(
    paper_id: str,
    *,
    load_paper_detail_result,
    evolution_view: str,
):
    current = load_paper_detail_result(paper_id)
    if current is None:
        abort(404)
    normalized_view = evolution_view if evolution_view in {"camp", "track", "stance"} else "camp"
    return render_template("paper.html", current=current, evolution_view=normalized_view)


def serve_library_pdf(paper_id: str, *, get_literature_paper):
    paper = get_literature_paper(paper_id)
    if paper is None or not paper.exists:
        abort(404)
    return send_file(paper.pdf_path)


def build_home_view(
    *,
    details_query_param: str,
    request_proxy,
    dashboard_mode,
    normalize_open_panels,
    runtime,
    mode_tabs_for_mode,
    refresh_status_payload,
    refresh_status_url_builder,
):
    request_adapter = DashboardRequestAdapter(
        details_query_param=details_query_param,
        request_proxy=request_proxy,
        dashboard_mode=dashboard_mode,
        normalize_open_panels=normalize_open_panels,
    )
    home_renderer = DashboardHomeRenderer(
        runtime=runtime,
        mode_tabs_for_mode=mode_tabs_for_mode,
        refresh_status_payload=refresh_status_payload,
        refresh_status_url_builder=refresh_status_url_builder,
    )

    def home():
        return home_renderer.render(request_adapter.home_state())

    home.__name__ = "home"
    return home


def build_refresh_dashboard_view(
    *,
    details_query_param: str,
    request_proxy,
    dashboard_mode,
    normalize_anchor_for_mode,
    normalize_open_panels,
    wants_async_refresh,
    queue_refresh_job,
    run_and_cache_all,
    refresh_status_payload,
    refresh_redirect_url,
):
    request_adapter = DashboardRequestAdapter(
        details_query_param=details_query_param,
        request_proxy=request_proxy,
        dashboard_mode=dashboard_mode,
        normalize_open_panels=normalize_open_panels,
        normalize_anchor_for_mode=normalize_anchor_for_mode,
    )
    refresh_handler = DashboardRefreshHandler(
        wants_async_refresh=wants_async_refresh,
        queue_refresh_job=queue_refresh_job,
        run_and_cache_all=run_and_cache_all,
        refresh_status_payload=refresh_status_payload,
        refresh_redirect_url=refresh_redirect_url,
    )

    def refresh_dashboard():
        return refresh_handler.handle(request_adapter.form_state(default_anchor=""))

    refresh_dashboard.__name__ = "refresh_dashboard"
    return refresh_dashboard


def build_refresh_status_view(
    *,
    details_query_param: str,
    request_proxy,
    dashboard_mode,
    normalize_anchor_for_mode,
    normalize_open_panels,
    refresh_status_payload,
):
    request_adapter = DashboardRequestAdapter(
        details_query_param=details_query_param,
        request_proxy=request_proxy,
        dashboard_mode=dashboard_mode,
        normalize_open_panels=normalize_open_panels,
        normalize_anchor_for_mode=normalize_anchor_for_mode,
    )
    refresh_status_handler = DashboardRefreshStatusHandler(
        refresh_status_payload=refresh_status_payload,
    )

    def refresh_status():
        return refresh_status_handler.handle(request_adapter.args_state(default_anchor=""))

    refresh_status.__name__ = "refresh_status"
    return refresh_status


def build_run_analysis_view(
    *,
    details_query_param: str,
    request_proxy,
    analyses,
    dashboard_mode,
    normalize_anchor_for_mode,
    normalize_open_panels,
    wants_async_refresh,
    queue_refresh_job,
    refresh_status_payload,
    run_and_cache_analysis,
    refresh_redirect_url,
):
    request_adapter = DashboardRequestAdapter(
        details_query_param=details_query_param,
        request_proxy=request_proxy,
        dashboard_mode=dashboard_mode,
        normalize_open_panels=normalize_open_panels,
        normalize_anchor_for_mode=normalize_anchor_for_mode,
    )
    analysis_handler = DashboardRunAnalysisHandler(
        analyses=analyses,
        wants_async_refresh=wants_async_refresh,
        queue_refresh_job=queue_refresh_job,
        refresh_status_payload=refresh_status_payload,
        run_and_cache_analysis=run_and_cache_analysis,
        refresh_redirect_url=refresh_redirect_url,
    )

    def run_analysis(analysis_id: str):
        return analysis_handler.handle(
            analysis_id,
            request_adapter.form_state(default_anchor=analysis_id),
        )

    run_analysis.__name__ = "run_analysis"
    return run_analysis


def build_anchor_redirect_view(anchor: str, *, home_url_builder):
    def redirect_view():
        return redirect_home_anchor(
            anchor,
            home_url_builder=home_url_builder,
        )

    redirect_view.__name__ = f"redirect_{anchor}"
    return redirect_view


def build_analysis_redirect_view(*, analyses, home_url_builder):
    def show_analysis(analysis_id: str):
        if analysis_id not in analyses:
            abort(404)
        return redirect_home_anchor(
            analysis_id,
            home_url_builder=home_url_builder,
        )

    show_analysis.__name__ = "show_analysis"
    return show_analysis


def build_serve_result_file_view(root: Path):
    def serve_result(subpath: str):
        return serve_result_file(root, subpath)

    serve_result.__name__ = "serve_result_file"
    return serve_result


def build_show_paper_brief_view(*, request_proxy, load_paper_detail_result):
    request_adapter = DashboardPaperRequestAdapter(request_proxy=request_proxy)

    def show_paper_brief(paper_id: str):
        return render_paper_brief(
            paper_id,
            load_paper_detail_result=load_paper_detail_result,
            evolution_view=request_adapter.evolution_view(),
        )

    show_paper_brief.__name__ = "show_paper_brief"
    return show_paper_brief


def build_serve_library_pdf_view(*, get_literature_paper):
    def serve_pdf(paper_id: str):
        return serve_library_pdf(
            paper_id,
            get_literature_paper=get_literature_paper,
        )

    serve_pdf.__name__ = "serve_library_pdf"
    return serve_pdf
