from __future__ import annotations

from collections.abc import Callable, Mapping
from flask.typing import ResponseReturnValue
from pathlib import Path
from typing import Any, Literal, Protocol, TypeAlias, TypedDict

import pandas as pd


ModeName: TypeAlias = Literal["brief", "demo", "full"]
RefreshStatus: TypeAlias = Literal["idle", "running", "succeeded", "failed"]
RddStatusMode: TypeAlias = Literal["real", "demo", "missing"]
DisplayTier: TypeAlias = Literal["primary", "detail"]


class DashboardCard(TypedDict):
    title: str
    subtitle: str
    description_zh: str


class NavSection(TypedDict):
    anchor: str
    label: str


class TrackNote(TypedDict):
    name: str
    copy: str


class OverviewMetric(TypedDict):
    value: str
    label: str


class NoteItem(TypedDict):
    title: str
    copy: str


class AbstractPoint(TypedDict):
    title: str
    copy: str


class HighlightItem(TypedDict):
    label: str
    headline: str
    copy: str


class SummaryCard(TypedDict, total=False):
    kicker: str
    title: str
    meta: str
    copy: str
    foot: str


class ResultCard(TypedDict):
    label: str
    value: str
    copy: str


class MetaItem(TypedDict):
    label: str
    value: str


class FigureEntry(TypedDict, total=False):
    path: str
    caption: str
    label: str
    layout_class: str


class RawFigureEntry(TypedDict, total=False):
    path: Path
    caption: str
    prefix: str


RawAnalysisTables: TypeAlias = Mapping[str, pd.DataFrame | None]
RelativePathBuilder: TypeAlias = Callable[[Path], str]


class RawAnalysisResult(TypedDict, total=False):
    id: str
    title: str
    description: str
    subtitle: str
    summary_text: str
    summary_path: Path
    tables: RawAnalysisTables
    figures: list[Path | RawFigureEntry]
    output_dir: Path | str


RenderedTable: TypeAlias = tuple[str, str]


class DisplayTable(TypedDict, total=False):
    label: str
    html: str
    layout_class: str
    tier: DisplayTier


class StatusPanel(TypedDict):
    kicker: str
    title: str
    copy: str
    meta: list[MetaItem]


class SnapshotMeta(TypedDict):
    label: str
    copy: str
    source_path: str
    source_count: int


class RefreshArtifact(TypedDict):
    path: str
    modified_at: str


class RefreshState(TypedDict):
    status: RefreshStatus
    message: str
    scope_label: str
    scope_key: str
    started_at: str
    finished_at: str
    started_ts: float
    finished_ts: float
    error: str
    snapshot_label: str
    snapshot_copy: str
    snapshot_source_path: str
    snapshot_source_count: int
    updated_artifacts: list[RefreshArtifact]
    baseline_artifact_mtimes: dict[str, float]


class RefreshStatusPayload(TypedDict):
    accepted: bool
    status: RefreshStatus
    message: str
    error: str
    scope_label: str
    scope_key: str
    started_at: str
    finished_at: str
    started_ts: float
    finished_ts: float
    duration_seconds: int | None
    poll_after_ms: int
    redirect_url: str
    snapshot_label: str
    snapshot_copy: str
    snapshot_source_path: str
    snapshot_source_count: int
    updated_artifacts: list[RefreshArtifact]


class ModeTab(TypedDict):
    mode: ModeName
    label: str
    base_href: str
    href: str
    default_hash: str
    allowed_hashes: list[str]
    active: bool


class SupportPaperRecord(TypedDict, total=False):
    citation: str
    title: str
    paper_id: str
    camp: str
    project_module: str
    method_focus: str
    practical_use: str
    deep_contribution: str
    one_line_role: str


class ActionLink(TypedDict, total=False):
    label: str
    href: str
    target: str


class PaperNavCard(TypedDict, total=False):
    kicker: str
    title: str
    year_label: str
    camp: str
    stance: str
    project_module: str
    track_label: str
    meta: str
    copy: str
    href: str
    is_current: bool


class EvolutionNavGroup(TypedDict):
    title: str
    meta: str
    cards: list[PaperNavCard]


class EvolutionNavView(TypedDict):
    id: str
    groups: list[EvolutionNavGroup]


class PaperCatalogRecord(TypedDict, total=False):
    paper_id: str
    authors: str
    year_label: str
    camp: str
    stance: str
    project_module: str
    method_focus: str
    deep_contribution: str
    practical_use: str
    one_line_role: str


PaperBriefRecord: TypeAlias = PaperCatalogRecord
PaperDashboardRecord = TypedDict(
    "PaperDashboardRecord",
    {
        "阵营": str,
        "立场": str,
        "市场 / 指数": str,
        "方法 / 关键词": str,
        "识别对象": str,
        "挑战的假设": str,
        "一句话定位": str,
        "争论推进": str,
        "研究中的作用": str,
    },
    total=False,
)


class BaseResult(TypedDict, total=False):
    id: str
    title: str
    description: str
    subtitle: str
    summary_text: str
    summary_cards: list[SummaryCard]
    rendered_tables: list[RenderedTable]
    figure_paths: list[FigureEntry]
    output_dir: str


class TrackResult(BaseResult, total=False):
    support_papers: list[SupportPaperRecord]


FrameworkResult: TypeAlias = TrackResult
SupplementResult: TypeAlias = BaseResult
AnalysisResult: TypeAlias = TrackResult | FrameworkResult | SupplementResult


class PaperDetailResult(BaseResult, total=False):
    hero_aside_title: str
    hero_meta_items: list[MetaItem]
    hero_aside_copy: str
    summary_paragraphs: list[str]
    sequence_cards: list[PaperNavCard]
    recommended_cards: list[PaperNavCard]
    evolution_nav_cards: list[PaperNavCard]
    evolution_nav_views: list[EvolutionNavView]
    primary_actions: list[ActionLink]


class RddStatus(TypedDict):
    mode: RddStatusMode
    evidence_status: str
    message: str
    note: str
    input_file: str
    audit_file: str
    candidate_rows: int | None
    candidate_batches: int | None
    treated_rows: int | None
    control_rows: int | None
    crossing_batches: int | None
    validation_error: str


class DashboardSection(TypedDict, total=False):
    summary: str
    display_summary: str
    summary_cards: list[SummaryCard]
    figures: list[FigureEntry]
    detail_figures: list[FigureEntry]
    display_figures: list[FigureEntry]
    tables: list[DisplayTable]
    display_tables: list[DisplayTable]
    primary_tables: list[DisplayTable]
    detail_tables: list[DisplayTable]


class TrackDisplaySection(TrackResult, DashboardSection, total=False):
    anchor: str
    notes: list[TrackNote]
    display_support_papers: list[SupportPaperRecord]
    result_cards: list[ResultCard]
    badge: str
    takeaway: str
    status_panel: StatusPanel | None


class SecondarySection(BaseResult, DashboardSection, total=False):
    pass


class RobustnessSection(DashboardSection, total=False):
    pass


class HomeContext(TypedDict):
    mode: ModeName
    nav_sections: list[NavSection]
    mode_tabs: list[ModeTab]
    snapshot_meta: SnapshotMeta
    refresh_meta: RefreshStatusPayload
    refresh_status_url: str
    current_open_panels: str | None
    overview_metrics: list[OverviewMetric]
    overview_notes: list[NoteItem]
    overview_summary: str
    cta_copy: str
    abstract_lead: str
    abstract_points: list[AbstractPoint]
    highlights: list[HighlightItem]
    design_section: DashboardSection
    track_sections: list[TrackDisplaySection]
    framework_section: SecondarySection
    supplement_section: SecondarySection
    robustness_section: RobustnessSection
    limits_section: DashboardSection


class AnalysisRunner(Protocol):
    def __call__(self, verbose: bool = False) -> RawAnalysisResult: ...


class AnalysisDefinition(TypedDict):
    title: str
    subtitle: str
    description_zh: str
    project_module: str
    runner: AnalysisRunner


AnalysesConfig: TypeAlias = dict[str, AnalysisDefinition]
CacheEntry: TypeAlias = TrackResult | SecondarySection | SupplementResult
AnalysisCache: TypeAlias = dict[str, CacheEntry]


RouteResponse: TypeAlias = ResponseReturnValue
TableRenderer: TypeAlias = Callable[..., str]
SnapshotMetaBuilder: TypeAlias = Callable[[], SnapshotMeta]
SnapshotSourcesBuilder: TypeAlias = Callable[[], list[Path]]
NavSectionsBuilder: TypeAlias = Callable[[ModeName], list[NavSection]]
OverviewNotesBuilder: TypeAlias = Callable[[ModeName], list[NoteItem]]
OverviewSummaryBuilder: TypeAlias = Callable[[ModeName], str]
CtaCopyBuilder: TypeAlias = Callable[[ModeName], str]
AbstractLeadBuilder: TypeAlias = Callable[[], str]
AbstractPointsBuilder: TypeAlias = Callable[[], list[AbstractPoint]]
DashboardModeResolver: TypeAlias = Callable[[], ModeName]
TrackSectionLoader: TypeAlias = Callable[[str], TrackResult]
TrackNotesBuilder: TypeAlias = Callable[[str], list[TrackNote]]
TrackDisplayPreparer: TypeAlias = Callable[[TrackDisplaySection, str, bool], TrackDisplaySection]
SecondarySectionLoader: TypeAlias = Callable[[], SecondarySection]
SecondarySectionPreparer: TypeAlias = Callable[[SecondarySection, bool], SecondarySection]
DemoModeSectionBuilder: TypeAlias = Callable[[bool], DashboardSection]
DashboardSectionBuilder: TypeAlias = Callable[[], DashboardSection]
RobustnessSectionBuilder: TypeAlias = Callable[[], RobustnessSection]
LabelTranslator: TypeAlias = Callable[[str], str]
TextCleaner: TypeAlias = Callable[[str], str]
SingleCsvLoader: TypeAlias = Callable[[Path, str], pd.DataFrame | None]
SavedTablesLoader: TypeAlias = Callable[[Path], list[RenderedTable]]
FigureCaptionBuilder: TypeAlias = Callable[[Path, str | None, str | None], str]
FigureEntriesBuilder: TypeAlias = Callable[[], list[FigureEntry]]
DisplayTableTiersAttacher: TypeAlias = Callable[[list[DisplayTable]], list[DisplayTable]]
DisplayTableTierSplitter: TypeAlias = Callable[[list[DisplayTable]], tuple[list[DisplayTable], list[DisplayTable]]]
FormatPValue: TypeAlias = Callable[[float], str]
FormatShare: TypeAlias = Callable[[float], str]
FormatPct: TypeAlias = Callable[[float], str]
CsvFrameReader: TypeAlias = Callable[[str | Path], pd.DataFrame]
IdentificationScopeUpdater: TypeAlias = Callable[[pd.DataFrame], pd.DataFrame]
ModeTabUrlBuilder: TypeAlias = Callable[[ModeName, str | None], str]
ModeTabsBuilder: TypeAlias = Callable[[ModeName, str | None], list[ModeTab]]
RefreshStatusPayloadBuilder: TypeAlias = Callable[[ModeName, str, str | None], RefreshStatusPayload]
RefreshRedirectUrlBuilder: TypeAlias = Callable[[ModeName, str, str | None], str]
OpenPanelsNormalizer: TypeAlias = Callable[[str | None], str]
AnchorNormalizer: TypeAlias = Callable[[ModeName, str | None], str]
AsyncRefreshChecker: TypeAlias = Callable[[], bool]
RunAllAnalyses: TypeAlias = Callable[[], None]
AnalysisInvoker: TypeAlias = Callable[[str], TrackResult]
PaperDetailLoader: TypeAlias = Callable[[str], PaperDetailResult | None]
TrackResultNormalizer: TypeAlias = Callable[[RawAnalysisResult], TrackResult]
TrackContextAttacher: TypeAlias = Callable[[TrackResult, AnalysisDefinition], TrackResult]
TrackAnalysisRunner: TypeAlias = Callable[[str], TrackResult]
TrackResultLoader: TypeAlias = Callable[[], TrackResult]
TrackLibraryResultLoader: TypeAlias = TrackResultLoader
TrackReviewResultLoader: TypeAlias = TrackResultLoader
FrameworkResultLoader: TypeAlias = Callable[[], FrameworkResult]
SupplementResultLoader: TypeAlias = Callable[[], SupplementResult]
TrackCardsBuilder: TypeAlias = Callable[[], list[ResultCard]]
TrackTablesBuilder: TypeAlias = Callable[[], list[RenderedTable]]
SavedTrackResultLoader: TypeAlias = Callable[[str, AnalysisDefinition], TrackResult | None]
RddStatusLoader: TypeAlias = Callable[[], RddStatus]


class RefreshRunner(Protocol):
    def __call__(self) -> None: ...


QueueRefreshJob: TypeAlias = Callable[[RefreshRunner, str, str], bool]
RefreshSuccessHandler: TypeAlias = Callable[[str, str], None]
RefreshFailureHandler: TypeAlias = Callable[[str, str, Exception], None]
RefreshJobRunner: TypeAlias = Callable[[RefreshRunner, str, str], None]
RefreshWorkerSpawner: TypeAlias = Callable[[RefreshRunner, str, str], None]


class RouteView(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> RouteResponse: ...


class DashboardRuntimeLike(Protocol):
    def dashboard_snapshot_sources(self) -> list[Path]: ...

    def safe_relative(self, path: Path) -> str: ...

    def build_dashboard_snapshot_meta(self, snapshot_files: list[Path] | None = None) -> SnapshotMeta: ...

    def load_identification_china_saved_result(self) -> TrackResult: ...

    def load_literature_library_result(self) -> TrackResult: ...

    def load_literature_review_result(self) -> TrackResult: ...

    def load_literature_framework_result(self) -> FrameworkResult: ...

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None: ...

    def load_supplement_result(self) -> SupplementResult: ...

    def run_and_cache_all(self) -> None: ...

    def run_and_cache_analysis(self, analysis_id: str) -> TrackResult: ...

    def nav_sections_for_mode(self, mode: ModeName) -> list[NavSection]: ...

    def mode_tabs_for_mode(self, mode: ModeName, url_builder: ModeTabUrlBuilder) -> list[ModeTab]: ...

    def build_home_context(
        self,
        *,
        display_mode: ModeName,
        current_open_panels: str | None,
        mode_tabs_for_mode: ModeTabsBuilder,
        refresh_status_payload: RefreshStatusPayloadBuilder,
        refresh_status_url: str,
    ) -> HomeContext: ...


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
    favicon_view: RouteView
    home_view: RouteView
    refresh_dashboard_view: RouteView
    refresh_status_view: RouteView
    run_analysis_view: RouteView
    show_library_view: RouteView
    show_review_view: RouteView
    show_framework_view: RouteView
    show_supplement_view: RouteView
    show_analysis_view: RouteView
    serve_result_file_view: RouteView
    show_paper_brief_view: RouteView
    serve_library_pdf_view: RouteView
