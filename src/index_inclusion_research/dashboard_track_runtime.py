from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from index_inclusion_research import (
    dashboard_content,
    dashboard_figures,
    dashboard_formatting,
    dashboard_loaders,
    dashboard_metrics,
    dashboard_presenters,
    dashboard_refresh,
    dashboard_tracks,
)
from index_inclusion_research.dashboard_cache import AnalysisCacheStore
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisCache,
    AnalysisDefinition,
    CacheEntry,
    DashboardCard,
    DisplayTable,
    FigureEntry,
    FrameworkResult,
    PaperDetailResult,
    RawAnalysisResult,
    RddContractCheck,
    RddStatus,
    RenderedTable,
    ResultHealth,
    SecondarySection,
    SnapshotMeta,
    SupplementResult,
    TrackDisplaySection,
    TrackResult,
)
from index_inclusion_research.literature_catalog import (
    build_project_track_frame,
    build_project_track_markdown,
    build_project_track_support_records,
)
from index_inclusion_research.rdd_evidence import (
    rdd_evidence_tier,
    rdd_evidence_tier_from_status,
    rdd_provenance_summary,
)


class DashboardTrackRuntime:
    def __init__(
        self,
        *,
        root: Path,
        analyses: AnalysesConfig,
        library_card: DashboardCard | Mapping[str, str],
        review_card: DashboardCard | Mapping[str, str],
        framework_card: DashboardCard | Mapping[str, str],
        supplement_card: DashboardCard | Mapping[str, str],
        project_module_display_map: Mapping[str, str],
    ) -> None:
        self.root = root
        self.analyses = analyses
        self.library_card = library_card
        self.review_card = review_card
        self.framework_card = framework_card
        self.supplement_card = supplement_card
        self.project_module_display_map = project_module_display_map
        self.run_cache: AnalysisCache = AnalysisCacheStore()

    # --- formatting / labels ----------------------------------------------------

    @property
    def table_labels(self) -> dict[str, str]:
        return dashboard_formatting.TABLE_LABELS

    @property
    def column_labels(self) -> dict[str, str]:
        return dashboard_formatting.COLUMN_LABELS

    @property
    def value_labels(self) -> dict[str, str]:
        return dashboard_formatting.VALUE_LABELS

    def safe_relative(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.root.resolve()).as_posix()
        except ValueError:
            return str(path)

    def render_table(self, frame, compact: bool = False) -> str:
        return dashboard_formatting.render_table(frame, compact=compact)

    def translate_label(self, label: str) -> str:
        return dashboard_formatting.translate_label(label)

    def format_figure_caption(self, path: Path) -> str:
        return dashboard_formatting.format_figure_caption(path)

    def build_figure_caption(
        self,
        path: Path,
        custom_caption: str | None = None,
        prefix: str | None = None,
    ) -> str:
        return dashboard_formatting.build_figure_caption(
            path,
            custom_caption=custom_caption,
            prefix=prefix,
        )

    def format_pct(self, value: float) -> str:
        return dashboard_formatting.format_pct(value)

    def format_p_value(self, value: float) -> str:
        return dashboard_formatting.format_p_value(value)

    def format_share(self, value: float) -> str:
        return dashboard_formatting.format_share(value)

    def strip_markdown_title(self, text: str) -> str:
        return dashboard_formatting.strip_markdown_title(text)

    def clean_display_text(self, text: str) -> str:
        return dashboard_formatting.clean_display_text(text)

    def table_layout_for_label(self, label: str) -> str:
        return dashboard_presenters.table_layout_for_label(label)

    def table_tier_for_label(self, label: str) -> str:
        return dashboard_presenters.table_tier_for_label(label)

    def decorate_display_tables(self, tables: list[RenderedTable]) -> list[DisplayTable]:
        return dashboard_presenters.decorate_display_tables(tables)

    def attach_display_tiers(self, items: list[DisplayTable]) -> list[DisplayTable]:
        return dashboard_presenters.attach_display_tiers(items)

    def split_items_by_tier(
        self,
        items: list[DisplayTable],
    ) -> tuple[list[DisplayTable], list[DisplayTable]]:
        return dashboard_presenters.split_items_by_tier(items)

    def dashboard_snapshot_sources(self) -> list[Path]:
        return dashboard_refresh.dashboard_snapshot_sources(self.root)

    def build_dashboard_snapshot_meta(self, snapshot_files: list[Path] | None = None) -> SnapshotMeta:
        return dashboard_refresh.build_dashboard_snapshot_meta(
            self.root,
            to_relative=self.safe_relative,
            snapshot_files=snapshot_files,
        )

    def build_result_health(self) -> ResultHealth:
        return dashboard_refresh.build_result_health(
            self.root,
            to_relative=self.safe_relative,
            contract_check=self.load_rdd_contract_check(),
        )

    # --- content loaders --------------------------------------------------------

    def normalize_result(self, raw: RawAnalysisResult) -> TrackResult:
        return dashboard_loaders.normalize_result(
            raw,
            translate_label=self.translate_label,
            render_table=self.render_table,
            to_relative=self.safe_relative,
            build_figure_caption=self.build_figure_caption,
        )

    def attach_project_track_context(
        self,
        current: TrackResult,
        config: AnalysisDefinition,
    ) -> TrackResult:
        project_module = config.get("project_module")
        if not project_module:
            return current
        track_summary = build_project_track_markdown(project_module).strip()
        if current.get("summary_text"):
            current["summary_text"] = f"{track_summary}\n\n---\n\n{current['summary_text']}"
        else:
            current["summary_text"] = track_summary
        support_table = ("支撑文献", self.render_table(build_project_track_frame(project_module), compact=True))
        current["rendered_tables"] = [support_table, *current.get("rendered_tables", [])]
        current["support_papers"] = build_project_track_support_records(project_module)
        return current

    def load_saved_tables(self, output_dir: Path) -> list[RenderedTable]:
        return dashboard_loaders.load_saved_tables(
            output_dir,
            translate_label=self.translate_label,
            render_table=self.render_table,
        )

    def load_single_csv(self, output_dir: Path, filename: str) -> pd.DataFrame | None:
        return dashboard_loaders.load_single_csv(output_dir, filename)

    def read_csv_if_exists(self, path: str | Path) -> pd.DataFrame:
        return dashboard_loaders.read_csv_if_exists(path)

    def rdd_output_dir(self) -> Path:
        return dashboard_loaders.rdd_output_dir(self.root)

    def load_rdd_status(self, output_dir: Path | None = None) -> RddStatus:
        return dashboard_loaders.load_rdd_status(
            self.root,
            output_dir=output_dir,
            read_csv_if_exists=self.read_csv_if_exists,
        )

    def load_rdd_contract_check(
        self,
        output_dir: Path | None = None,
        manifest_path: Path | None = None,
        rdd_status: RddStatus | None = None,
    ) -> RddContractCheck:
        return dashboard_loaders.build_rdd_contract_check(
            self.root,
            rdd_status=rdd_status,
            output_dir=output_dir,
            manifest_path=manifest_path,
            read_csv_if_exists=self.read_csv_if_exists,
        )

    def apply_live_rdd_status_to_identification_scope(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty or "分析层" not in frame.columns:
            return frame
        updated = frame.copy()
        mask = updated["分析层"] == "中国 RDD 扩展"
        if not mask.any():
            return updated
        status = self.load_rdd_status()
        tier = str(status.get("evidence_tier", "")) or rdd_evidence_tier(str(status["mode"]))
        if tier == "—":
            tier = rdd_evidence_tier_from_status(str(status["evidence_status"]))
        provenance = rdd_provenance_summary(status)
        if "证据等级" in updated.columns:
            updated.loc[mask, "证据等级"] = tier
        else:
            insert_at = updated.columns.get_loc("证据状态") if "证据状态" in updated.columns else len(updated.columns)
            updated.insert(insert_at, "证据等级", pd.NA)
            updated.loc[mask, "证据等级"] = tier
        updated.loc[mask, "证据状态"] = status["evidence_status"]
        updated.loc[mask, "当前口径"] = status["note"]
        if "来源摘要" in updated.columns:
            updated.loc[mask, "来源摘要"] = provenance
        else:
            insert_at = updated.columns.get_loc("当前口径") + 1 if "当前口径" in updated.columns else len(updated.columns)
            updated.insert(insert_at, "来源摘要", pd.NA)
            updated.loc[mask, "来源摘要"] = provenance
        return updated

    def load_identification_china_saved_result(self) -> TrackResult:
        return dashboard_loaders.load_identification_china_saved_result(
            self.root,
            self.analyses,
            load_rdd_status=lambda: self.load_rdd_status(self.rdd_output_dir()),
            load_saved_tables=self.load_saved_tables,
            to_relative=self.safe_relative,
            build_figure_caption=self.build_figure_caption,
        )

    def load_literature_library_result(self) -> TrackResult:
        return dashboard_content.load_literature_library_result(
            render_table=self.render_table,
            library_card=self.library_card,
        )

    def load_literature_review_result(self) -> TrackResult:
        return dashboard_content.load_literature_review_result(
            render_table=self.render_table,
            review_card=self.review_card,
        )

    def load_literature_framework_result(self) -> FrameworkResult:
        return dashboard_content.load_literature_framework_result(
            render_table=self.render_table,
            framework_card=self.framework_card,
        )

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None:
        return dashboard_content.load_paper_detail_result(
            paper_id,
            render_table=self.render_table,
            project_module_display_map=self.project_module_display_map,
        )

    def load_supplement_result(self) -> SupplementResult:
        return dashboard_content.load_supplement_result(
            render_table=self.render_table,
            supplement_card=self.supplement_card,
        )

    def load_saved_track_result(self, analysis_id: str, config: AnalysisDefinition) -> TrackResult | None:
        return dashboard_loaders.load_saved_track_result(
            self.root,
            analysis_id,
            config,
            load_identification_china_saved_result=self.load_identification_china_saved_result,
            attach_project_track_context=self.attach_project_track_context,
            load_saved_tables=self.load_saved_tables,
            to_relative=self.safe_relative,
            build_figure_caption=self.build_figure_caption,
        )

    # --- figures / display / run-and-cache --------------------------------------

    def _snapshot_run_cache(self) -> dict[str, CacheEntry]:
        if isinstance(self.run_cache, AnalysisCacheStore):
            return self.run_cache.snapshot()
        return dict(self.run_cache)

    def _replace_run_cache(self, next_cache: Mapping[str, CacheEntry]) -> None:
        if isinstance(self.run_cache, AnalysisCacheStore):
            self.run_cache.replace_all(next_cache)
            return
        self.run_cache.clear()
        self.run_cache.update(dict(next_cache))

    def create_price_pressure_figures(self) -> list[FigureEntry]:
        return dashboard_figures.create_price_pressure_figures(
            self.root,
            to_relative=self.safe_relative,
        )

    def create_identification_figures(self) -> list[FigureEntry]:
        return dashboard_figures.create_identification_figures(
            self.root,
            load_rdd_status=self.load_rdd_status,
            to_relative=self.safe_relative,
        )

    def run_and_cache_all(self) -> None:
        staged_cache = self._snapshot_run_cache()

        def _run_and_cache_analysis(analysis_id: str) -> TrackResult:
            return dashboard_tracks.run_and_cache_analysis(
                analysis_id,
                analyses=self.analyses,
                run_cache=staged_cache,
                normalize_result=self.normalize_result,
                attach_project_track_context=self.attach_project_track_context,
            )

        dashboard_tracks.run_and_cache_all(
            analyses=self.analyses,
            run_cache=staged_cache,
            run_and_cache_analysis=_run_and_cache_analysis,
            load_literature_library_result=self.load_literature_library_result,
            load_literature_review_result=self.load_literature_review_result,
            load_literature_framework_result=self.load_literature_framework_result,
            load_supplement_result=self.load_supplement_result,
        )
        self._replace_run_cache(staged_cache)

    def run_and_cache_analysis(self, analysis_id: str) -> TrackResult:
        staged_cache = self._snapshot_run_cache()
        current = dashboard_tracks.run_and_cache_analysis(
            analysis_id,
            analyses=self.analyses,
            run_cache=staged_cache,
            normalize_result=self.normalize_result,
            attach_project_track_context=self.attach_project_track_context,
        )
        self._replace_run_cache(staged_cache)
        return current

    def load_or_build_track_section(self, analysis_id: str) -> TrackResult:
        return dashboard_tracks.load_or_build_track_section(
            analysis_id,
            analyses=self.analyses,
            run_cache=self.run_cache,
            load_saved_track_result=self.load_saved_track_result,
            normalize_result=self.normalize_result,
            attach_project_track_context=self.attach_project_track_context,
        )

    def prepare_track_display(
        self,
        section: TrackDisplaySection,
        analysis_id: str,
        demo_mode: bool,
    ) -> TrackDisplaySection:
        return dashboard_tracks.prepare_track_display(
            self.root,
            section,
            analysis_id,
            demo_mode,
            load_rdd_status=self.load_rdd_status,
            load_rdd_contract_check=self.load_rdd_contract_check,
            clean_display_text=self.clean_display_text,
            render_table=self.render_table,
            format_pct=self.format_pct,
            format_p_value=self.format_p_value,
            create_price_pressure_figures=self.create_price_pressure_figures,
            create_identification_figures=self.create_identification_figures,
        )

    def prepare_framework_display(self, section: SecondarySection, demo_mode: bool) -> SecondarySection:
        del demo_mode
        return dashboard_presenters.prepare_framework_display(
            section,
            summary_cards=dashboard_metrics.build_framework_summary_cards(),
        )

    def prepare_supplement_display(self, section: SecondarySection, demo_mode: bool) -> SecondarySection:
        del demo_mode
        return dashboard_presenters.prepare_supplement_display(
            section,
            summary_cards=dashboard_metrics.build_supplement_summary_cards(),
        )
