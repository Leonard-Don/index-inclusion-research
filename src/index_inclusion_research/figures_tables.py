from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

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
    build_data_source_table,
    build_event_counts_by_year_table,
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

ROOT = Path(__file__).resolve().parents[2]


def _read_csv_if_exists(path: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path, parse_dates=parse_dates, low_memory=False)


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
            if not asymmetry_summary.empty:
                save_dataframe(asymmetry_summary, Path(args.tables_dir) / "asymmetry_summary.csv")
                frames["asymmetry_summary"] = asymmetry_summary
            robustness_retention_summary = build_robustness_retention_summary(long_event_level)
            if not robustness_retention_summary.empty:
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

    if not events.empty:
        event_counts_by_year = build_event_counts_by_year_table(events)
        if not event_counts_by_year.empty:
            save_dataframe(event_counts_by_year, Path(args.tables_dir) / "event_counts_by_year.csv")
            frames["event_counts_by_year"] = event_counts_by_year

        data_sources = build_data_source_table(
            events,
            prices=prices,
            benchmarks=benchmarks,
            metadata=metadata,
            panel=panel,
            matched_panel=matched_panel,
        )
        if not data_sources.empty:
            file_map = {
                "事件样本": args.events,
                "日频价格": args.prices,
                "基准收益": args.benchmarks,
                "证券元数据": args.metadata,
                "事件窗口面板": args.panel,
                "匹配回归面板": args.matched_panel,
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
        if not sample_scope.empty:
            save_dataframe(sample_scope, Path(args.tables_dir) / "sample_scope.csv")
            frames["sample_scope"] = sample_scope

        if not panel.empty:
            short_event_level_path = Path(args.event_summary).parent / "event_level_metrics.csv"
            short_event_level = _read_csv_if_exists(short_event_level_path, parse_dates=["announce_date", "effective_date", "event_date"])
            if short_event_level.empty and args.panel:
                short_event_level, _, _ = compute_event_study(panel, [(-1, 1), (-3, 3), (-5, 5)])
            time_series_summary = build_time_series_event_study_summary(short_event_level)
            if not time_series_summary.empty:
                save_dataframe(time_series_summary, Path(args.tables_dir) / "time_series_event_study_summary.csv")
                frames["time_series_event_study_summary"] = time_series_summary
            if "asymmetry_summary" not in frames:
                asymmetry_summary = build_asymmetry_summary(short_event_level, long_event_level=long_event_level)
                if not asymmetry_summary.empty:
                    save_dataframe(asymmetry_summary, Path(args.tables_dir) / "asymmetry_summary.csv")
                    frames["asymmetry_summary"] = asymmetry_summary
            sample_filter_summary = build_sample_filter_summary(
                short_event_level,
                long_event_level=long_event_level,
                regression_dataset=regression_dataset,
            )
            if not sample_filter_summary.empty:
                save_dataframe(sample_filter_summary, Path(args.tables_dir) / "sample_filter_summary.csv")
                frames["sample_filter_summary"] = sample_filter_summary
            robustness_event_summary = build_robustness_event_study_summary(
                short_event_level,
                long_event_level=long_event_level,
            )
            if not robustness_event_summary.empty:
                save_dataframe(robustness_event_summary, Path(args.tables_dir) / "robustness_event_study_summary.csv")
                frames["robustness_event_study_summary"] = robustness_event_summary
            robustness_regression_summary = build_robustness_regression_summary(regression_dataset)
            if not robustness_regression_summary.empty:
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
