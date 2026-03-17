from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.outputs import export_descriptive_tables, export_latex_tables, plot_average_paths


def _read_csv_if_exists(path: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path, parse_dates=parse_dates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create paper-ready figures and tables.")
    parser.add_argument("--events", default="data/processed/events_clean.csv", help="Events CSV.")
    parser.add_argument("--panel", default="data/processed/event_panel.csv", help="Event panel CSV.")
    parser.add_argument("--average-paths", default="results/event_study/average_paths.csv", help="Average paths CSV.")
    parser.add_argument("--event-summary", default="results/event_study/event_study_summary.csv", help="Event-study summary CSV.")
    parser.add_argument("--regression-coefs", default="results/regressions/regression_coefficients.csv", help="Regression coefficients CSV.")
    parser.add_argument("--regression-models", default="results/regressions/regression_models.csv", help="Regression model stats CSV.")
    parser.add_argument("--figures-dir", default="results/figures", help="Figure output directory.")
    parser.add_argument("--tables-dir", default="results/tables", help="Table output directory.")
    args = parser.parse_args()

    events = _read_csv_if_exists(args.events, parse_dates=["announce_date", "effective_date"])
    panel = _read_csv_if_exists(args.panel, parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"])
    average_paths = _read_csv_if_exists(args.average_paths)
    event_summary = _read_csv_if_exists(args.event_summary)
    regression_coefs = _read_csv_if_exists(args.regression_coefs)
    regression_models = _read_csv_if_exists(args.regression_models)

    if not average_paths.empty:
        plot_average_paths(average_paths, args.figures_dir)

    frames = {}
    if not events.empty and not panel.empty:
        event_counts, panel_coverage = export_descriptive_tables(events, panel, args.tables_dir)
        frames["event_counts"] = event_counts
        frames["panel_coverage"] = panel_coverage
    if not event_summary.empty:
        save_dataframe(event_summary, Path(args.tables_dir) / "event_study_summary.csv")
        frames["event_study_summary"] = event_summary
    if not regression_coefs.empty:
        save_dataframe(regression_coefs, Path(args.tables_dir) / "regression_coefficients.csv")
        frames["regression_coefficients"] = regression_coefs
    if not regression_models.empty:
        save_dataframe(regression_models, Path(args.tables_dir) / "regression_models.csv")
        frames["regression_models"] = regression_models

    export_latex_tables(frames, args.tables_dir)
    print(f"Saved figures to {args.figures_dir} and tables to {args.tables_dir}")


if __name__ == "__main__":
    main()
