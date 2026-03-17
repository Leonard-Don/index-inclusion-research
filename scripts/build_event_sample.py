from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import load_project_config
from index_inclusion_research.loaders import load_events, save_dataframe
from index_inclusion_research.pipeline import build_event_sample


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and standardize index inclusion events.")
    parser.add_argument("--input", default="data/raw/sample_events.csv", help="Path to raw events CSV.")
    parser.add_argument("--output", default="data/processed/events_clean.csv", help="Path to save cleaned events.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    args = parser.parse_args()

    config = load_project_config(args.config)
    duplicate_window = config["defaults"]["matching"]["conflict_window_days"]
    events = load_events(args.input)
    cleaned = build_event_sample(events, duplicate_window_days=duplicate_window)
    save_dataframe(cleaned, args.output)
    print(f"Saved {len(cleaned)} cleaned events to {args.output}")


if __name__ == "__main__":
    main()
