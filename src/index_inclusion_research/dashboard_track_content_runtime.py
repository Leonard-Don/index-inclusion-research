from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_content
from index_inclusion_research import dashboard_loaders
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisDefinition,
    DashboardCard,
    FrameworkResult,
    PaperDetailResult,
    RawAnalysisResult,
    RenderedTable,
    RddStatus,
    SupplementResult,
    TrackResult,
)
from index_inclusion_research.rdd_evidence import (
    rdd_evidence_tier,
    rdd_evidence_tier_from_status,
    rdd_provenance_summary,
)
from index_inclusion_research.literature_catalog import (
    build_project_track_frame,
    build_project_track_markdown,
    build_project_track_support_records,
)

from index_inclusion_research.dashboard_track_support_runtime import DashboardTrackSupportRuntime


class DashboardTrackContentRuntime:
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
        support: DashboardTrackSupportRuntime,
    ) -> None:
        self.root = root
        self.analyses = analyses
        self.library_card = library_card
        self.review_card = review_card
        self.framework_card = framework_card
        self.supplement_card = supplement_card
        self.project_module_display_map = project_module_display_map
        self.support = support

    def normalize_result(self, raw: RawAnalysisResult) -> TrackResult:
        return dashboard_loaders.normalize_result(
            raw,
            translate_label=self.support.translate_label,
            render_table=self.support.render_table,
            to_relative=self.support.safe_relative,
            build_figure_caption=self.support.build_figure_caption,
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
        support_table = ("支撑文献", self.support.render_table(build_project_track_frame(project_module), compact=True))
        current["rendered_tables"] = [support_table, *current.get("rendered_tables", [])]
        current["support_papers"] = build_project_track_support_records(project_module)
        return current

    def load_saved_tables(self, output_dir: Path) -> list[RenderedTable]:
        return dashboard_loaders.load_saved_tables(
            output_dir,
            translate_label=self.support.translate_label,
            render_table=self.support.render_table,
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
            to_relative=self.support.safe_relative,
            build_figure_caption=self.support.build_figure_caption,
        )

    def load_literature_library_result(self) -> TrackResult:
        return dashboard_content.load_literature_library_result(
            render_table=self.support.render_table,
            library_card=self.library_card,
        )

    def load_literature_review_result(self) -> TrackResult:
        return dashboard_content.load_literature_review_result(
            render_table=self.support.render_table,
            review_card=self.review_card,
        )

    def load_literature_framework_result(self) -> FrameworkResult:
        return dashboard_content.load_literature_framework_result(
            render_table=self.support.render_table,
            framework_card=self.framework_card,
        )

    def load_paper_detail_result(self, paper_id: str) -> PaperDetailResult | None:
        return dashboard_content.load_paper_detail_result(
            paper_id,
            render_table=self.support.render_table,
            project_module_display_map=self.project_module_display_map,
        )

    def load_supplement_result(self) -> SupplementResult:
        return dashboard_content.load_supplement_result(
            render_table=self.support.render_table,
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
            to_relative=self.support.safe_relative,
            build_figure_caption=self.support.build_figure_caption,
        )
