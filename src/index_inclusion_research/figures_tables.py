from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

from index_inclusion_research import paths
from index_inclusion_research.analysis import compute_event_study
from index_inclusion_research.literature import compute_retention_summary
from index_inclusion_research.loaders import (
    load_benchmarks,
    load_events,
    load_prices,
    save_dataframe,
)
from index_inclusion_research.outputs import (
    build_asymmetry_summary,
    build_cma_2d_robustness_heatmap_from_cache,
    build_cma_ar_engine_forest_plot_from_cache,
    build_cma_sensitivity_forest_plot_from_cache,
    build_cma_verdicts_forest_plot,
    build_data_source_table,
    build_event_counts_by_year_table,
    build_hs300_rdd_forest_plot,
    build_identification_scope_table,
    build_robustness_event_study_summary,
    build_robustness_regression_summary,
    build_robustness_retention_summary,
    build_sample_filter_summary,
    build_sample_scope_table,
    build_time_series_event_study_summary,
    export_descriptive_tables,
    export_latex_tables,
    plot_average_paths,
)
from index_inclusion_research.pipeline import build_event_panel
from index_inclusion_research.result_contract import (
    build_results_manifest,
    load_rdd_status,
)

ROOT = paths.project_root()


def _maybe_build_hs300_rdd_forest_plot(
    *,
    rdd_output_dir: Path | None,
    figures_dir: Path | None,
) -> None:
    """Refresh the HS300 RDD robustness forest plot when its source CSV
    is present. Mirrors the PNG into the literature dashboard's figure
    directory so the existing dashboard entry point picks it up without
    re-running the dashboard pipeline.
    """
    if rdd_output_dir is None:
        rdd_output_dir = ROOT / "results" / "literature" / "hs300_rdd"
    if figures_dir is None:
        figures_dir = ROOT / "results" / "figures"
    robust_csv = rdd_output_dir / "rdd_robustness.csv"
    if not robust_csv.exists():
        return
    png_path = figures_dir / "hs300_rdd_robustness_forest.png"
    pdf_path = figures_dir / "hs300_rdd_robustness_forest.pdf"
    try:
        build_hs300_rdd_forest_plot(
            robustness_csv_path=robust_csv,
            output_png_path=png_path,
            output_pdf_path=pdf_path,
        )
    except (ValueError, OSError) as exc:
        logger.warning("HS300 RDD forest plot skipped: %s", exc)
        return
    mirror_path = rdd_output_dir / "figures" / "rdd_robustness_forest.png"
    mirror_path.parent.mkdir(parents=True, exist_ok=True)
    mirror_path.write_bytes(png_path.read_bytes())


def _maybe_build_verdict_timeline(
    *,
    figures_dir: Path | None,
    repo_root: Path | None = None,
) -> None:
    """Refresh the H1..H7 verdict-evolution swimlane figure.

    Mirrors the other ``_maybe_*`` helpers: the renderer already
    tolerates an empty git history by emitting a placeholder PNG, so we
    just guard against an outright import / OSError and let
    figures_tables continue.
    """
    if figures_dir is None:
        figures_dir = ROOT / "results" / "figures"
    root = (repo_root or ROOT).resolve()
    if not (root / ".git").exists():
        # Not a git checkout — skip silently; downstream doctor checks
        # surface the missing artifact rather than this code path.
        return
    png_path = figures_dir / "verdict_timeline.png"
    pdf_path = figures_dir / "verdict_timeline.pdf"
    try:
        from index_inclusion_research.outputs import (
            build_verdict_timeline_from_git,
            render_verdict_timeline_plot,
        )

        timeline_df = build_verdict_timeline_from_git(root)
        render_verdict_timeline_plot(
            timeline_df,
            output_png_path=png_path,
            output_pdf_path=pdf_path,
        )
    except (ValueError, OSError) as exc:
        logger.warning("verdict timeline figure skipped: %s", exc)
        return


def _maybe_build_literature_timeline(
    *,
    repo_root: Path | None = None,
) -> None:
    """Refresh the 16-paper literature chronology figure.

    Mirrors the other ``_maybe_*`` helpers: the renderer tolerates a
    missing ``citation_centrality.csv`` (uniform marker sizes), so we
    just guard against an outright import / OSError so figures_tables
    keeps going on a corrupt install.
    """
    root = (repo_root or ROOT).resolve()
    try:
        from index_inclusion_research.outputs import (
            assemble_literature_timeline_papers,
            build_literature_timeline_plot,
            default_literature_timeline_centrality_csv_path,
            default_literature_timeline_pdf_path,
            default_literature_timeline_png_path,
        )

        centrality_csv = default_literature_timeline_centrality_csv_path(root)
        papers = assemble_literature_timeline_papers(
            centrality_csv_path=centrality_csv if centrality_csv.exists() else None
        )
        build_literature_timeline_plot(
            papers,
            output_png_path=default_literature_timeline_png_path(root),
            output_pdf_path=default_literature_timeline_pdf_path(root),
        )
    except (ValueError, OSError) as exc:
        logger.warning("literature timeline figure skipped: %s", exc)
        return


def _maybe_build_cma_verdicts_forest(
    *,
    tables_dir: Path | None,
    figures_dir: Path | None,
) -> None:
    """Refresh the cross-hypothesis CMA verdicts forest plot when its
    source CSV (``cma_hypothesis_verdicts.csv``) is present. Mirrors
    the HS300 RDD forest plot integration: silently skip if the source
    is missing, log a warning if rendering fails so the broader figures
    pipeline isn't blocked by a single visualisation regression.
    """
    if tables_dir is None:
        tables_dir = ROOT / "results" / "real_tables"
    if figures_dir is None:
        figures_dir = ROOT / "results" / "figures"
    verdicts_csv = tables_dir / "cma_hypothesis_verdicts.csv"
    if not verdicts_csv.exists():
        return
    png_path = figures_dir / "cma_verdicts_forest.png"
    pdf_path = figures_dir / "cma_verdicts_forest.pdf"
    try:
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=verdicts_csv,
            output_png_path=png_path,
            output_pdf_path=pdf_path,
        )
    except (ValueError, OSError) as exc:
        logger.warning("CMA verdicts forest plot skipped: %s", exc)
        return


def _maybe_build_cma_sensitivity_forest(
    *,
    figures_dir: Path | None,
    sensitivity_root: Path | None = None,
) -> None:
    """Refresh the sensitivity-aware CMA verdicts forest plot when the
    threshold-sweep cache is populated.

    Mirrors :func:`_maybe_build_cma_verdicts_forest`: silently skip if
    no per-threshold CSVs exist under ``results/sensitivity/`` (the
    user hasn't opted into the sweep yet), log a warning on render
    failure so the broader figures pipeline keeps going. Refreshing
    the sweep itself requires running the CMA pipeline four times so
    we don't trigger it automatically here — users opt in via
    ``index-inclusion-build-cma-sensitivity-forest``.
    """
    if figures_dir is None:
        figures_dir = ROOT / "results" / "figures"
    sens_root = sensitivity_root or (ROOT / "results" / "sensitivity")
    if not sens_root.exists():
        return
    # Only render if at least one cached threshold CSV exists.
    cached_csvs = list(sens_root.glob("threshold_*/cma_hypothesis_verdicts.csv"))
    if not cached_csvs:
        return
    png_path = figures_dir / "cma_verdicts_sensitivity.png"
    pdf_path = figures_dir / "cma_verdicts_sensitivity.pdf"
    try:
        build_cma_sensitivity_forest_plot_from_cache(
            output_png_path=png_path,
            output_pdf_path=pdf_path,
            sensitivity_root=sens_root,
        )
    except (ValueError, OSError) as exc:
        logger.warning("CMA verdicts sensitivity forest plot skipped: %s", exc)
        return


def _maybe_build_cma_2d_robustness_heatmap(
    *,
    figures_dir: Path | None,
    sensitivity_root: Path | None = None,
) -> None:
    """Refresh the 2D (threshold × AR engine) robustness heatmap when
    any of the sensitivity sub-caches is populated.

    Mirrors :func:`_maybe_build_cma_sensitivity_forest`: silently skip
    if neither dedicated grid caches nor single-axis fallbacks exist
    under ``results/sensitivity/`` (the user hasn't opted into the
    sweep yet); log a warning on render failure so the broader figures
    pipeline keeps going. Refreshing the sweep itself can require up
    to 8 CMA runs (3 of them market-engine), so we never trigger it
    automatically here — users opt in via
    ``index-inclusion-build-cma-2d-robustness-heatmap``.
    """
    if figures_dir is None:
        figures_dir = ROOT / "results" / "figures"
    sens_root = sensitivity_root or (ROOT / "results" / "sensitivity")
    if not sens_root.exists():
        return
    has_grid_cache = any(
        sens_root.glob("grid_*/cma_hypothesis_verdicts.csv")
    )
    has_threshold_fallback = any(
        sens_root.glob("threshold_*/cma_hypothesis_verdicts.csv")
    )
    has_ar_fallback = any(
        sens_root.glob("ar_*/cma_hypothesis_verdicts.csv")
    )
    if not (has_grid_cache or has_threshold_fallback or has_ar_fallback):
        return
    png_path = figures_dir / "cma_verdicts_2d_robustness.png"
    pdf_path = figures_dir / "cma_verdicts_2d_robustness.pdf"
    try:
        build_cma_2d_robustness_heatmap_from_cache(
            output_png_path=png_path,
            output_pdf_path=pdf_path,
            sensitivity_root=sens_root,
        )
    except (ValueError, OSError) as exc:
        logger.warning("CMA verdicts 2D robustness heatmap skipped: %s", exc)
        return


def _maybe_build_cma_ar_engine_forest(
    *,
    figures_dir: Path | None,
    sensitivity_root: Path | None = None,
) -> None:
    """Refresh the AR-engine-aware CMA verdicts forest plot when the
    engine-sweep cache is populated.

    Mirrors :func:`_maybe_build_cma_sensitivity_forest`: silently skip if
    no per-engine CSVs exist under ``results/sensitivity/`` (the user
    hasn't opted into the sweep yet), log a warning on render failure so
    the broader figures pipeline keeps going. Refreshing the sweep
    itself requires running the CMA pipeline once per engine (the
    ``market`` engine is materially slower because it materialises a
    market-model panel first), so we don't trigger it automatically
    here — users opt in via
    ``index-inclusion-build-cma-ar-engine-forest``.
    """
    if figures_dir is None:
        figures_dir = ROOT / "results" / "figures"
    sens_root = sensitivity_root or (ROOT / "results" / "sensitivity")
    if not sens_root.exists():
        return
    cached_csvs = list(sens_root.glob("ar_*/cma_hypothesis_verdicts.csv"))
    if not cached_csvs:
        return
    png_path = figures_dir / "cma_verdicts_ar_engine.png"
    pdf_path = figures_dir / "cma_verdicts_ar_engine.pdf"
    try:
        build_cma_ar_engine_forest_plot_from_cache(
            output_png_path=png_path,
            output_pdf_path=pdf_path,
            sensitivity_root=sens_root,
        )
    except (ValueError, OSError) as exc:
        logger.warning("CMA verdicts AR-engine forest plot skipped: %s", exc)
        return


def _read_csv_if_exists(path: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path, parse_dates=parse_dates, low_memory=False)


def _project_relative_label(path: str | Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _should_save_dataframe(frame: pd.DataFrame) -> bool:
    """Return true when a table has rows or a meaningful header-only schema."""
    return not frame.empty or len(frame.columns) > 0


def _profile_paths(profile: str, *, root: Path = ROOT) -> dict[str, str]:
    if profile == "real":
        return {
            "events": str(root / "data" / "processed" / "real_events_clean.csv"),
            "panel": str(root / "data" / "processed" / "real_event_panel.csv"),
            "prices": str(root / "data" / "raw" / "real_prices.csv"),
            "benchmarks": str(root / "data" / "raw" / "real_benchmarks.csv"),
            "metadata": str(root / "data" / "raw" / "real_metadata.csv"),
            "matched_panel": str(root / "data" / "processed" / "real_matched_event_panel.csv"),
            "average_paths": str(root / "results" / "real_event_study" / "average_paths.csv"),
            "event_summary": str(root / "results" / "real_event_study" / "event_study_summary.csv"),
            "regression_coefs": str(root / "results" / "real_regressions" / "regression_coefficients.csv"),
            "regression_models": str(root / "results" / "real_regressions" / "regression_models.csv"),
            "rdd_summary": str(root / "results" / "literature" / "hs300_rdd" / "rdd_summary.csv"),
            "rdd_output_dir": str(root / "results" / "literature" / "hs300_rdd"),
            "long_window_output_dir": str(root / "results" / "real_event_study"),
            "figures_dir": str(root / "results" / "real_figures"),
            "tables_dir": str(root / "results" / "real_tables"),
            "results_manifest": str(root / "results" / "real_tables" / "results_manifest.csv"),
        }
    return {
        "events": str(root / "data" / "processed" / "events_clean.csv"),
        "panel": str(root / "data" / "processed" / "event_panel.csv"),
        "prices": "",
        "benchmarks": "",
        "metadata": "",
        "matched_panel": "",
        "average_paths": str(root / "results" / "event_study" / "average_paths.csv"),
        "event_summary": str(root / "results" / "event_study" / "event_study_summary.csv"),
        "regression_coefs": str(root / "results" / "regressions" / "regression_coefficients.csv"),
        "regression_models": str(root / "results" / "regressions" / "regression_models.csv"),
        "rdd_summary": "",
        "rdd_output_dir": str(root / "results" / "literature" / "hs300_rdd"),
        "long_window_output_dir": "",
        "figures_dir": str(root / "results" / "figures"),
        "tables_dir": str(root / "results" / "tables"),
        "results_manifest": str(root / "results" / "tables" / "results_manifest.csv"),
    }


def _detect_profile(*, root: Path = ROOT) -> str:
    real_markers = [
        root / "data" / "processed" / "real_events_clean.csv",
        root / "data" / "processed" / "real_event_panel.csv",
        root / "results" / "real_event_study" / "event_study_summary.csv",
        root / "results" / "real_regressions" / "regression_coefficients.csv",
    ]
    return "real" if all(path.exists() for path in real_markers) else "sample"


def _resolve_cli_args(args: argparse.Namespace, *, root: Path = ROOT) -> argparse.Namespace:
    resolved = argparse.Namespace(**vars(args))
    requested_profile = getattr(resolved, "profile", "auto")
    profile = _detect_profile(root=root) if requested_profile == "auto" else requested_profile
    defaults = _profile_paths(profile, root=root)
    for key, value in defaults.items():
        if not getattr(resolved, key):
            setattr(resolved, key, value)
    resolved.profile = profile
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create paper-ready figures and tables.")
    parser.add_argument(
        "--profile",
        choices=["auto", "sample", "real"],
        default="auto",
        help="Export profile. Defaults to auto and prefers the real-data workflow when available.",
    )
    parser.add_argument("--events", default="", help="Events CSV.")
    parser.add_argument("--panel", default="", help="Event panel CSV.")
    parser.add_argument("--prices", default="", help="Raw prices CSV.")
    parser.add_argument("--benchmarks", default="", help="Raw benchmarks CSV.")
    parser.add_argument("--metadata", default="", help="Security metadata CSV.")
    parser.add_argument("--matched-panel", default="", help="Matched event panel CSV.")
    parser.add_argument("--average-paths", default="", help="Average paths CSV.")
    parser.add_argument("--event-summary", default="", help="Event-study summary CSV.")
    parser.add_argument("--regression-coefs", default="", help="Regression coefficients CSV.")
    parser.add_argument("--regression-models", default="", help="Regression model stats CSV.")
    parser.add_argument("--rdd-summary", default="", help="RDD summary CSV.")
    parser.add_argument("--rdd-output-dir", default="", help="RDD output directory with rdd_status.csv.")
    parser.add_argument(
        "--long-window-output-dir",
        default="",
        help="Optional directory for long-window event-study outputs. Defaults to the event-summary directory.",
    )
    parser.add_argument("--figures-dir", default="", help="Figure output directory.")
    parser.add_argument("--tables-dir", default="", help="Table output directory.")
    parser.add_argument("--results-manifest", default="", help="Structured result contract CSV output path.")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _resolve_cli_args(build_parser().parse_args(argv))

    events = load_events(args.events) if Path(args.events).exists() else pd.DataFrame()
    panel = _read_csv_if_exists(args.panel, parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"])
    prices = load_prices(args.prices) if args.prices and Path(args.prices).exists() else pd.DataFrame()
    benchmarks = load_benchmarks(args.benchmarks) if args.benchmarks and Path(args.benchmarks).exists() else pd.DataFrame()
    metadata = _read_csv_if_exists(args.metadata) if args.metadata else pd.DataFrame()
    matched_panel = (
        _read_csv_if_exists(args.matched_panel, parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"])
        if args.matched_panel
        else pd.DataFrame()
    )
    average_paths = _read_csv_if_exists(args.average_paths)
    event_summary = _read_csv_if_exists(args.event_summary)
    regression_coefs = _read_csv_if_exists(args.regression_coefs)
    regression_models = _read_csv_if_exists(args.regression_models)
    rdd_summary = _read_csv_if_exists(args.rdd_summary) if args.rdd_summary else pd.DataFrame()
    regression_dataset = _read_csv_if_exists(Path(args.regression_coefs).parent / "regression_dataset.csv")

    if not average_paths.empty:
        plot_average_paths(average_paths, args.figures_dir)

    # HS300 RDD robustness forest plot — produced from
    # results/literature/hs300_rdd/rdd_robustness.csv when present, so
    # the figure stays in lockstep with the main figure refresh cycle.
    _maybe_build_hs300_rdd_forest_plot(
        rdd_output_dir=Path(args.rdd_output_dir) if args.rdd_output_dir else None,
        figures_dir=Path(args.figures_dir) if args.figures_dir else None,
    )

    # CMA verdicts cross-hypothesis forest plot — paired with the HS300
    # RDD plot so the paper has both the single-spec robustness view
    # and the cross-hypothesis evidence-strength overview in lockstep.
    _maybe_build_cma_verdicts_forest(
        tables_dir=Path(args.tables_dir) if args.tables_dir else None,
        figures_dir=Path(args.figures_dir) if args.figures_dir else None,
    )

    # Sensitivity-aware version of the CMA forest — opt-in (renders only
    # when results/sensitivity/threshold_<T>/ caches exist). Building the
    # cache itself is the responsibility of
    # `index-inclusion-build-cma-sensitivity-forest`; here we only
    # re-render the figure from existing CSVs so make-figures-tables
    # never silently re-runs the full CMA pipeline 4×.
    _maybe_build_cma_sensitivity_forest(
        figures_dir=Path(args.figures_dir) if args.figures_dir else None,
    )

    # AR-engine-aware version of the CMA forest — opt-in (renders only
    # when results/sensitivity/ar_<engine>/ caches exist). Same cache-
    # only contract as the threshold variant above; refreshing the
    # caches is the explicit job of
    # `index-inclusion-build-cma-ar-engine-forest`.
    _maybe_build_cma_ar_engine_forest(
        figures_dir=Path(args.figures_dir) if args.figures_dir else None,
    )

    # 2D robustness heatmap that crosses both methodological axes
    # (4 thresholds × 2 AR engines = 8 cells per hypothesis). Cache-
    # only re-render here; the explicit refresh CLI is
    # `index-inclusion-build-cma-2d-robustness-heatmap`.
    _maybe_build_cma_2d_robustness_heatmap(
        figures_dir=Path(args.figures_dir) if args.figures_dir else None,
    )

    # CMA verdict-evolution timeline (40th CLI). Pulls verdict history
    # from the git log of cma_hypothesis_verdicts.csv and renders a
    # 7-swimlane figure. Skipped silently if the repo isn't a git
    # working tree or the CSV has no recorded history (the renderer
    # already emits a placeholder figure in that case).
    _maybe_build_verdict_timeline(
        figures_dir=Path(args.figures_dir) if args.figures_dir else None,
    )

    # Literature chronology timeline (47th CLI). Reads the static
    # PAPER_LIBRARY plus citation_centrality.csv (in-degree column) and
    # renders the 16-paper year × research-thread scatter. Never crashes
    # the figures pipeline — the renderer tolerates a missing centrality
    # CSV with uniform marker sizes.
    _maybe_build_literature_timeline()

    frames = {}
    long_event_level = pd.DataFrame()
    long_panel = pd.DataFrame()
    if not events.empty and not panel.empty:
        event_counts, panel_coverage = export_descriptive_tables(events, panel, args.tables_dir)
        frames["event_counts"] = event_counts
        frames["panel_coverage"] = panel_coverage

        if not prices.empty and not benchmarks.empty:
            long_windows = [(0, 5), (0, 20), (0, 60), (0, 120)]
            long_panel = build_event_panel(events, prices, benchmarks, window_pre=20, window_post=120)
            long_event_level, long_summary, _ = compute_event_study(long_panel, long_windows)
            retention_summary = compute_retention_summary(long_event_level)
            long_output_dir = Path(args.long_window_output_dir) if args.long_window_output_dir else Path(args.event_summary).parent
            save_dataframe(long_event_level, long_output_dir / "long_window_event_level_metrics.csv")
            save_dataframe(long_summary, Path(args.tables_dir) / "long_window_event_study_summary.csv")
            frames["long_window_event_study_summary"] = long_summary
            if not retention_summary.empty:
                save_dataframe(retention_summary, Path(args.tables_dir) / "retention_summary.csv")
                frames["retention_summary"] = retention_summary
            asymmetry_summary = build_asymmetry_summary(
                event_level=_read_csv_if_exists(long_output_dir / "event_level_metrics.csv"),
                long_event_level=long_event_level,
            )
            if _should_save_dataframe(asymmetry_summary):
                save_dataframe(asymmetry_summary, Path(args.tables_dir) / "asymmetry_summary.csv")
                frames["asymmetry_summary"] = asymmetry_summary
            robustness_retention_summary = build_robustness_retention_summary(long_event_level)
            if _should_save_dataframe(robustness_retention_summary):
                save_dataframe(robustness_retention_summary, Path(args.tables_dir) / "robustness_retention_summary.csv")
                frames["robustness_retention_summary"] = robustness_retention_summary

    if not event_summary.empty:
        save_dataframe(event_summary, Path(args.tables_dir) / "event_study_summary.csv")
        frames["event_study_summary"] = event_summary
    if not regression_coefs.empty:
        save_dataframe(regression_coefs, Path(args.tables_dir) / "regression_coefficients.csv")
        frames["regression_coefficients"] = regression_coefs
    if not regression_models.empty:
        save_dataframe(regression_models, Path(args.tables_dir) / "regression_models.csv")
        frames["regression_models"] = regression_models

    event_counts_by_year = build_event_counts_by_year_table(events)
    if _should_save_dataframe(event_counts_by_year):
        save_dataframe(event_counts_by_year, Path(args.tables_dir) / "event_counts_by_year.csv")
        frames["event_counts_by_year"] = event_counts_by_year

    if not events.empty:
        data_sources = build_data_source_table(
            events,
            prices=prices,
            benchmarks=benchmarks,
            metadata=metadata,
            panel=panel,
            matched_panel=matched_panel,
        )
        if _should_save_dataframe(data_sources):
            file_map = {
                "事件样本": _project_relative_label(args.events),
                "日频价格": _project_relative_label(args.prices),
                "基准收益": _project_relative_label(args.benchmarks),
                "证券元数据": _project_relative_label(args.metadata),
                "事件窗口面板": _project_relative_label(args.panel),
                "匹配回归面板": _project_relative_label(args.matched_panel),
            }
            data_sources.insert(1, "文件", data_sources["数据集"].map(file_map).fillna(""))
            save_dataframe(data_sources, Path(args.tables_dir) / "data_sources.csv")
            frames["data_sources"] = data_sources

        sample_scope = build_sample_scope_table(
            events,
            panel=panel,
            matched_panel=matched_panel,
            long_panel=long_panel,
            long_event_level=long_event_level,
        )
        if _should_save_dataframe(sample_scope):
            save_dataframe(sample_scope, Path(args.tables_dir) / "sample_scope.csv")
            frames["sample_scope"] = sample_scope

        if not panel.empty:
            short_event_level_path = Path(args.event_summary).parent / "event_level_metrics.csv"
            short_event_level = _read_csv_if_exists(short_event_level_path, parse_dates=["announce_date", "effective_date", "event_date"])
            if short_event_level.empty and args.panel:
                short_event_level, _, _ = compute_event_study(panel, [(-1, 1), (-3, 3), (-5, 5)])
            time_series_summary = build_time_series_event_study_summary(short_event_level)
            if _should_save_dataframe(time_series_summary):
                save_dataframe(time_series_summary, Path(args.tables_dir) / "time_series_event_study_summary.csv")
                frames["time_series_event_study_summary"] = time_series_summary
            if "asymmetry_summary" not in frames:
                asymmetry_summary = build_asymmetry_summary(short_event_level, long_event_level=long_event_level)
                if _should_save_dataframe(asymmetry_summary):
                    save_dataframe(asymmetry_summary, Path(args.tables_dir) / "asymmetry_summary.csv")
                    frames["asymmetry_summary"] = asymmetry_summary
            sample_filter_summary = build_sample_filter_summary(
                short_event_level,
                long_event_level=long_event_level,
                regression_dataset=regression_dataset,
            )
            if _should_save_dataframe(sample_filter_summary):
                save_dataframe(sample_filter_summary, Path(args.tables_dir) / "sample_filter_summary.csv")
                frames["sample_filter_summary"] = sample_filter_summary
            robustness_event_summary = build_robustness_event_study_summary(
                short_event_level,
                long_event_level=long_event_level,
            )
            if _should_save_dataframe(robustness_event_summary):
                save_dataframe(robustness_event_summary, Path(args.tables_dir) / "robustness_event_study_summary.csv")
                frames["robustness_event_study_summary"] = robustness_event_summary
            robustness_regression_summary = build_robustness_regression_summary(regression_dataset)
            if _should_save_dataframe(robustness_regression_summary):
                save_dataframe(robustness_regression_summary, Path(args.tables_dir) / "robustness_regression_summary.csv")
                frames["robustness_regression_summary"] = robustness_regression_summary

        rdd_status = load_rdd_status(
            ROOT,
            output_dir=Path(args.rdd_output_dir) if args.rdd_output_dir else None,
        )
        identification_scope = build_identification_scope_table(
            events,
            panel=panel,
            matched_panel=matched_panel,
            rdd_summary=rdd_summary,
            rdd_status=rdd_status,
        )
        save_dataframe(identification_scope, Path(args.tables_dir) / "identification_scope.csv")
        frames["identification_scope"] = identification_scope
        results_manifest = build_results_manifest(args.profile, rdd_status)
        save_dataframe(results_manifest, args.results_manifest)
        frames["results_manifest"] = results_manifest

    if "robustness_event_study_summary" not in frames:
        robustness_event_summary = build_robustness_event_study_summary(
            pd.DataFrame(),
            long_event_level=long_event_level,
        )
        if _should_save_dataframe(robustness_event_summary):
            save_dataframe(robustness_event_summary, Path(args.tables_dir) / "robustness_event_study_summary.csv")
            frames["robustness_event_study_summary"] = robustness_event_summary

    export_latex_tables(frames, args.tables_dir)

    try:
        from index_inclusion_research.analysis.cross_market_asymmetry import (
            orchestrator as _cma,
        )
        cma_csv = Path(args.tables_dir) / "cma_mechanism_panel.csv"
        if cma_csv.exists():
            _cma.regenerate_tex_only(tables_dir=Path(args.tables_dir))
    except (OSError, ImportError, ValueError) as exc:
        logger.warning("CMA tex regeneration skipped: %s", exc)

    print(f"Saved figures to {args.figures_dir} and tables to {args.tables_dir} (profile: {args.profile})")


if __name__ == "__main__":
    main()
