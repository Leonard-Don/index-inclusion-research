from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from index_inclusion_research.analysis import compute_event_study
from index_inclusion_research.literature import compute_retention_summary
from index_inclusion_research.loaders import load_benchmarks, load_events, load_prices, save_dataframe
from index_inclusion_research.outputs import (
    build_asymmetry_summary,
    build_data_source_table,
    build_event_counts_by_year_table,
    build_identification_scope_table,
    build_robustness_event_study_summary,
    build_robustness_regression_summary,
    build_robustness_retention_summary,
    build_sample_scope_table,
    build_sample_filter_summary,
    build_time_series_event_study_summary,
    export_descriptive_tables,
    export_latex_tables,
    plot_average_paths,
)
from index_inclusion_research.pipeline import build_event_panel


def _read_csv_if_exists(path: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path, parse_dates=parse_dates, low_memory=False)


def _infer_rdd_mode(summary_note_path: str | Path | None) -> str:
    if not summary_note_path:
        return "missing"

    note_path = Path(summary_note_path)
    status_path = note_path.parent / "rdd_status.csv"
    if status_path.exists():
        status_frame = _read_csv_if_exists(status_path)
        if not status_frame.empty and "status" in status_frame.columns:
            status = str(status_frame.iloc[0]["status"]).strip().lower()
            if status in {"real", "reconstructed", "demo", "missing"}:
                return status

    if note_path.exists():
        note_text = note_path.read_text(encoding="utf-8")
        if "显式 `--demo` 模式" in note_text or "demo 伪排名数据" in note_text:
            return "demo"
        if "当前正在使用公开数据重建的候选样本文件" in note_text:
            return "reconstructed"
        if "当前正在使用你提供的真实候选排名文件" in note_text:
            return "real"
        if "等待真实候选样本文件" in note_text:
            return "missing"
    return "missing"


def main() -> None:
    parser = argparse.ArgumentParser(description="Create paper-ready figures and tables.")
    parser.add_argument("--events", default="data/processed/events_clean.csv", help="Events CSV.")
    parser.add_argument("--panel", default="data/processed/event_panel.csv", help="Event panel CSV.")
    parser.add_argument("--prices", default="", help="Raw prices CSV.")
    parser.add_argument("--benchmarks", default="", help="Raw benchmarks CSV.")
    parser.add_argument("--metadata", default="", help="Security metadata CSV.")
    parser.add_argument("--matched-panel", default="", help="Matched event panel CSV.")
    parser.add_argument("--average-paths", default="results/event_study/average_paths.csv", help="Average paths CSV.")
    parser.add_argument("--event-summary", default="results/event_study/event_study_summary.csv", help="Event-study summary CSV.")
    parser.add_argument("--regression-coefs", default="results/regressions/regression_coefficients.csv", help="Regression coefficients CSV.")
    parser.add_argument("--regression-models", default="results/regressions/regression_models.csv", help="Regression model stats CSV.")
    parser.add_argument("--rdd-summary", default="", help="RDD summary CSV.")
    parser.add_argument("--rdd-summary-note", default="", help="RDD summary markdown path.")
    parser.add_argument(
        "--long-window-output-dir",
        default="",
        help="Optional directory for long-window event-study outputs. Defaults to the event-summary directory.",
    )
    parser.add_argument("--figures-dir", default="results/figures", help="Figure output directory.")
    parser.add_argument("--tables-dir", default="results/tables", help="Table output directory.")
    args = parser.parse_args()

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

        rdd_mode = _infer_rdd_mode(args.rdd_summary_note)
        identification_scope = build_identification_scope_table(
            events,
            panel=panel,
            matched_panel=matched_panel,
            rdd_summary=rdd_summary,
            rdd_mode=rdd_mode,
        )
        save_dataframe(identification_scope, Path(args.tables_dir) / "identification_scope.csv")
        frames["identification_scope"] = identification_scope

    export_latex_tables(frames, args.tables_dir)
    print(f"Saved figures to {args.figures_dir} and tables to {args.tables_dir}")


if __name__ == "__main__":
    main()
