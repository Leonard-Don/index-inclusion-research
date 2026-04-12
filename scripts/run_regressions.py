from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import load_project_config
from index_inclusion_research.analysis import build_regression_dataset, run_regressions
from index_inclusion_research.loaders import save_dataframe

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Run cross-market event-level regressions.")
    parser.add_argument("--panel", default="data/processed/matched_event_panel.csv", help="Matched event panel CSV.")
    parser.add_argument("--output-dir", default="results/regressions", help="Directory for regression outputs.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    args = parser.parse_args()

    config = load_project_config(args.config)
    panel = pd.read_csv(
        args.panel,
        parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"],
        low_memory=False,
    )
    dataset = build_regression_dataset(panel, config["defaults"]["car_windows"])
    coefficients, model_stats = run_regressions(dataset)
    output_dir = Path(args.output_dir)
    save_dataframe(dataset, output_dir / "regression_dataset.csv")
    save_dataframe(coefficients, output_dir / "regression_coefficients.csv")
    save_dataframe(model_stats, output_dir / "regression_models.csv")
    print(f"Saved regression outputs to {output_dir}")


if __name__ == "__main__":
    main()
