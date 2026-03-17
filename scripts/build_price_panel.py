from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import load_project_config
from index_inclusion_research.loaders import load_benchmarks, load_events, load_prices, save_dataframe
from index_inclusion_research.pipeline import build_event_panel


def main() -> None:
    parser = argparse.ArgumentParser(description="Build event-window price panel.")
    parser.add_argument("--events", default="data/processed/events_clean.csv", help="Cleaned events CSV.")
    parser.add_argument("--prices", default="data/raw/sample_prices.csv", help="Daily prices CSV.")
    parser.add_argument("--benchmarks", default="data/raw/sample_benchmarks.csv", help="Benchmark returns CSV.")
    parser.add_argument("--output", default="data/processed/event_panel.csv", help="Panel output CSV.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    args = parser.parse_args()

    config = load_project_config(args.config)
    defaults = config["defaults"]
    events = load_events(args.events)
    prices = load_prices(args.prices)
    benchmarks = load_benchmarks(args.benchmarks)
    panel = build_event_panel(
        events=events,
        prices=prices,
        benchmarks=benchmarks,
        window_pre=defaults["event_window_pre"],
        window_post=defaults["event_window_post"],
    )
    save_dataframe(panel, args.output)
    print(f"Saved {len(panel)} panel rows to {args.output}")


if __name__ == "__main__":
    main()
