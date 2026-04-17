from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from index_inclusion_research.dashboard_track_content_runtime import DashboardTrackContentRuntime
from index_inclusion_research.dashboard_track_display_runtime import DashboardTrackDisplayRuntime
from index_inclusion_research.dashboard_track_support_runtime import DashboardTrackSupportRuntime


class DashboardTrackRuntime:
    def __init__(
        self,
        *,
        root: Path,
        analyses: Mapping[str, Mapping[str, object]],
        library_card: Mapping[str, str],
        review_card: Mapping[str, str],
        framework_card: Mapping[str, str],
        supplement_card: Mapping[str, str],
        project_module_display_map: Mapping[str, str],
    ) -> None:
        self.root = root
        self.analyses = analyses
        self.library_card = library_card
        self.review_card = review_card
        self.framework_card = framework_card
        self.supplement_card = supplement_card
        self.project_module_display_map = project_module_display_map
        self.run_cache: dict[str, dict[str, object]] = {}

        self.support = DashboardTrackSupportRuntime(root=root)
        self.content = DashboardTrackContentRuntime(
            root=root,
            analyses=analyses,
            library_card=library_card,
            review_card=review_card,
            framework_card=framework_card,
            supplement_card=supplement_card,
            project_module_display_map=project_module_display_map,
            support=self.support,
        )
        self.display = DashboardTrackDisplayRuntime(
            root=root,
            analyses=analyses,
            run_cache=self.run_cache,
            support=self.support,
            content=self.content,
        )

    @property
    def table_labels(self) -> dict[str, str]:
        return self.support.table_labels

    @property
    def column_labels(self) -> dict[str, str]:
        return self.support.column_labels

    @property
    def value_labels(self) -> dict[str, str]:
        return self.support.value_labels

    def safe_relative(self, path: Path) -> str:
        return self.support.safe_relative(path)

    def dashboard_snapshot_sources(self) -> list[Path]:
        return self.support.dashboard_snapshot_sources()

    def build_dashboard_snapshot_meta(self, snapshot_files: list[Path] | None = None) -> dict[str, object]:
        return self.support.build_dashboard_snapshot_meta(snapshot_files)

    def render_table(self, frame, compact: bool = False) -> str:
        return self.support.render_table(frame, compact=compact)

    def translate_label(self, label: str) -> str:
        return self.support.translate_label(label)

    def format_figure_caption(self, path: Path) -> str:
        return self.support.format_figure_caption(path)

    def build_figure_caption(
        self,
        path: Path,
        custom_caption: str | None = None,
        prefix: str | None = None,
    ) -> str:
        return self.support.build_figure_caption(
            path,
            custom_caption=custom_caption,
            prefix=prefix,
        )

    def normalize_result(self, raw: dict[str, object]) -> dict[str, object]:
        return self.content.normalize_result(raw)

    def attach_project_track_context(
        self,
        current: dict[str, object],
        config: dict[str, object],
    ) -> dict[str, object]:
        return self.content.attach_project_track_context(current, config)

    def load_saved_tables(self, output_dir: Path) -> list[tuple[str, str]]:
        return self.content.load_saved_tables(output_dir)

    def load_single_csv(self, output_dir: Path, filename: str):
        return self.content.load_single_csv(output_dir, filename)

    def read_csv_if_exists(self, path: str | Path) -> pd.DataFrame:
        return self.content.read_csv_if_exists(path)

    def rdd_output_dir(self) -> Path:
        return self.content.rdd_output_dir()

    def load_rdd_status(self, output_dir: Path | None = None) -> dict[str, object]:
        return self.content.load_rdd_status(output_dir)

    def apply_live_rdd_status_to_identification_scope(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self.content.apply_live_rdd_status_to_identification_scope(frame)

    def format_pct(self, value: float) -> str:
        return self.support.format_pct(value)

    def format_p_value(self, value: float) -> str:
        return self.support.format_p_value(value)

    def format_share(self, value: float) -> str:
        return self.support.format_share(value)

    def table_layout_for_label(self, label: str) -> str:
        return self.support.table_layout_for_label(label)

    def table_tier_for_label(self, label: str) -> str:
        return self.support.table_tier_for_label(label)

    def decorate_display_tables(self, tables: list[tuple[str, str]]) -> list[dict[str, str]]:
        return self.support.decorate_display_tables(tables)

    def attach_display_tiers(self, items: list[dict[str, object]]) -> list[dict[str, object]]:
        return self.support.attach_display_tiers(items)

    def split_items_by_tier(
        self,
        items: list[dict[str, object]],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        return self.support.split_items_by_tier(items)

    def create_price_pressure_figures(self) -> list[dict[str, str]]:
        return self.display.create_price_pressure_figures()

    def create_identification_figures(self) -> list[dict[str, str]]:
        return self.display.create_identification_figures()

    def load_identification_china_saved_result(self) -> dict[str, object]:
        return self.content.load_identification_china_saved_result()

    def load_literature_library_result(self) -> dict[str, object]:
        return self.content.load_literature_library_result()

    def load_literature_review_result(self) -> dict[str, object]:
        return self.content.load_literature_review_result()

    def load_literature_framework_result(self) -> dict[str, object]:
        return self.content.load_literature_framework_result()

    def load_paper_detail_result(self, paper_id: str) -> dict[str, object] | None:
        return self.content.load_paper_detail_result(paper_id)

    def load_supplement_result(self) -> dict[str, object]:
        return self.content.load_supplement_result()

    def strip_markdown_title(self, text: str) -> str:
        return self.support.strip_markdown_title(text)

    def clean_display_text(self, text: str) -> str:
        return self.support.clean_display_text(text)

    def load_saved_track_result(self, analysis_id: str, config: dict[str, object]) -> dict[str, object] | None:
        return self.content.load_saved_track_result(analysis_id, config)

    def run_and_cache_all(self) -> None:
        self.display.run_and_cache_all()

    def run_and_cache_analysis(self, analysis_id: str) -> dict[str, object]:
        return self.display.run_and_cache_analysis(analysis_id)

    def load_or_build_track_section(self, analysis_id: str) -> dict[str, object]:
        return self.display.load_or_build_track_section(analysis_id)

    def prepare_track_display(
        self,
        section: dict[str, object],
        analysis_id: str,
        demo_mode: bool,
    ) -> dict[str, object]:
        return self.display.prepare_track_display(section, analysis_id, demo_mode)

    def prepare_framework_display(self, section: dict[str, object], demo_mode: bool) -> dict[str, object]:
        return self.display.prepare_framework_display(section, demo_mode)

    def prepare_supplement_display(self, section: dict[str, object], demo_mode: bool) -> dict[str, object]:
        return self.display.prepare_supplement_display(section, demo_mode)
