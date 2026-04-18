from __future__ import annotations

from pathlib import Path

from index_inclusion_research import dashboard_formatting
from index_inclusion_research import dashboard_presenters
from index_inclusion_research import dashboard_refresh
from index_inclusion_research.dashboard_types import DisplayTable, RenderedTable, SnapshotMeta


class DashboardTrackSupportRuntime:
    def __init__(self, *, root: Path) -> None:
        self.root = root

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

    def dashboard_snapshot_sources(self) -> list[Path]:
        return dashboard_refresh.dashboard_snapshot_sources(self.root)

    def build_dashboard_snapshot_meta(self, snapshot_files: list[Path] | None = None) -> SnapshotMeta:
        return dashboard_refresh.build_dashboard_snapshot_meta(
            self.root,
            to_relative=self.safe_relative,
            snapshot_files=snapshot_files,
        )

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

    def strip_markdown_title(self, text: str) -> str:
        return dashboard_formatting.strip_markdown_title(text)

    def clean_display_text(self, text: str) -> str:
        return dashboard_formatting.clean_display_text(text)
