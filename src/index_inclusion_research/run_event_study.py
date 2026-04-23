from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from index_inclusion_research import load_project_config
from index_inclusion_research.analysis import compute_event_study
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.workflow_profiles import (
    add_profile_argument,
    resolve_profile_args,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run event-study summaries on an event panel.")
    add_profile_argument(parser)
    parser.add_argument("--panel", default="", help="Event panel CSV.")
    parser.add_argument("--output-dir", default="", help="Directory for event-study outputs.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = resolve_profile_args(parser.parse_args(argv), workflow="run_event_study")

    config = load_project_config(args.config)
    panel = pd.read_csv(args.panel, parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"])
    event_level, summary, average_paths = compute_event_study(panel, config["defaults"]["car_windows"])
    output_dir = Path(args.output_dir)
    save_dataframe(event_level, output_dir / "event_level_metrics.csv")
    save_dataframe(summary, output_dir / "event_study_summary.csv")
    save_dataframe(average_paths, output_dir / "average_paths.csv")
    print(f"Saved event-study outputs to {output_dir} (profile: {args.profile})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
