from __future__ import annotations

from pathlib import Path

from index_inclusion_research import dashboard_metrics, dashboard_presenters
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisCache,
    AnalysisDefinition,
    FigureEntriesBuilder,
    FormatPct,
    FormatPValue,
    FrameworkResultLoader,
    RawAnalysisResult,
    RddContractCheckLoader,
    RddStatusLoader,
    SavedTrackResultLoader,
    SupplementResultLoader,
    TableRenderer,
    TextCleaner,
    TrackAnalysisRunner,
    TrackContextAttacher,
    TrackDisplaySection,
    TrackLibraryResultLoader,
    TrackResult,
    TrackResultNormalizer,
    TrackReviewResultLoader,
)


def finalize_track_result(
    raw: RawAnalysisResult,
    config: AnalysisDefinition,
    *,
    normalize_result: TrackResultNormalizer,
    attach_project_track_context: TrackContextAttacher,
    analysis_id: str,
) -> TrackResult:
    current = normalize_result(raw)
    current["id"] = config.get("project_module", analysis_id)
    current["title"] = config["title"]
    current["description"] = raw.get("description", config["description_zh"])
    current["subtitle"] = config["subtitle"]
    return attach_project_track_context(current, config)


def run_and_cache_analysis(
    analysis_id: str,
    *,
    analyses: AnalysesConfig,
    run_cache: AnalysisCache,
    normalize_result: TrackResultNormalizer,
    attach_project_track_context: TrackContextAttacher,
) -> TrackResult:
    config = analyses[analysis_id]
    raw = config["runner"](verbose=False)
    current = finalize_track_result(
        raw,
        config,
        normalize_result=normalize_result,
        attach_project_track_context=attach_project_track_context,
        analysis_id=analysis_id,
    )
    run_cache[analysis_id] = current
    return current


def load_or_build_track_section(
    analysis_id: str,
    *,
    analyses: AnalysesConfig,
    run_cache: AnalysisCache,
    load_saved_track_result: SavedTrackResultLoader,
    normalize_result: TrackResultNormalizer,
    attach_project_track_context: TrackContextAttacher,
) -> TrackResult:
    current = run_cache.get(analysis_id)
    config = analyses[analysis_id]
    if current is None:
        current = load_saved_track_result(analysis_id, config)
        if current is not None:
            run_cache[analysis_id] = current
    if current is None:
        raw = config["runner"](verbose=False)
        current = finalize_track_result(
            raw,
            config,
            normalize_result=normalize_result,
            attach_project_track_context=attach_project_track_context,
            analysis_id=analysis_id,
        )
        run_cache[analysis_id] = current
    return current


def run_and_cache_all(
    *,
    analyses: AnalysesConfig,
    run_cache: AnalysisCache,
    run_and_cache_analysis: TrackAnalysisRunner,
    load_literature_library_result: TrackLibraryResultLoader,
    load_literature_review_result: TrackReviewResultLoader,
    load_literature_framework_result: FrameworkResultLoader,
    load_supplement_result: SupplementResultLoader,
) -> None:
    for analysis_id in analyses:
        run_and_cache_analysis(analysis_id)
    run_cache["paper_library"] = load_literature_library_result()
    run_cache["paper_review"] = load_literature_review_result()
    run_cache["paper_framework"] = load_literature_framework_result()
    run_cache["project_supplement"] = load_supplement_result()


def prepare_track_display(
    root: Path,
    section: TrackDisplaySection,
    analysis_id: str,
    demo_mode: bool,
    *,
    load_rdd_status: RddStatusLoader,
    load_rdd_contract_check: RddContractCheckLoader | None = None,
    clean_display_text: TextCleaner,
    render_table: TableRenderer,
    format_pct: FormatPct,
    format_p_value: FormatPValue,
    create_price_pressure_figures: FigureEntriesBuilder,
    create_identification_figures: FigureEntriesBuilder,
) -> TrackDisplaySection:
    identification_status = load_rdd_status() if analysis_id == "identification_china_track" else None
    identification_contract = (
        load_rdd_contract_check()
        if analysis_id == "identification_china_track" and load_rdd_contract_check is not None
        else None
    )
    return dashboard_presenters.prepare_track_display(
        section,
        analysis_id,
        demo_mode,
        fallback_summary=clean_display_text(str(section.get("summary_text", ""))),
        result_cards_by_analysis={
            "price_pressure_track": dashboard_metrics.build_price_pressure_cards(
                root,
                format_pct=format_pct,
                format_p_value=format_p_value,
            ),
            "demand_curve_track": dashboard_metrics.build_demand_curve_cards(
                root,
                format_pct=format_pct,
                format_p_value=format_p_value,
            ),
            "identification_china_track": dashboard_metrics.build_identification_cards(
                root,
                format_pct=format_pct,
                format_p_value=format_p_value,
                rdd_status=identification_status,
            ),
        },
        curated_tables_by_analysis={
            "price_pressure_track": dashboard_metrics.build_price_pressure_tables(
                root,
                render_table=render_table,
            ),
            "demand_curve_track": dashboard_metrics.build_demand_curve_tables(
                root,
                render_table=render_table,
            ),
            "identification_china_track": dashboard_metrics.build_identification_tables(
                root,
                render_table=render_table,
                rdd_status=identification_status,
            ),
        },
        extra_figures_by_analysis={
            "price_pressure_track": create_price_pressure_figures(),
            "identification_china_track": create_identification_figures(),
        },
        status_panel=(
            dashboard_metrics.build_identification_status_panel(
                identification_status,
                contract_check=identification_contract,
            )
            if identification_status is not None
            else None
        ),
    )
