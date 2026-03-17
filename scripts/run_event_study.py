from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import load_project_config
from index_inclusion_research.analysis import compute_event_study
from index_inclusion_research.loaders import save_dataframe

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Run event-study summaries on an event panel.")
    parser.add_argument("--panel", default="data/processed/event_panel.csv", help="Event panel CSV.")
    parser.add_argument("--output-dir", default="results/event_study", help="Directory for event-study outputs.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    args = parser.parse_args()

    config = load_project_config(args.config)
    panel = pd.read_csv(args.panel, parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"])
    event_level, summary, average_paths = compute_event_study(panel, config["defaults"]["car_windows"])
    output_dir = Path(args.output_dir)
    save_dataframe(event_level, output_dir / "event_level_metrics.csv")
    save_dataframe(summary, output_dir / "event_study_summary.csv")
    save_dataframe(average_paths, output_dir / "average_paths.csv")
    print(f"Saved event-study outputs to {output_dir}")


if __name__ == "__main__":
    main()
