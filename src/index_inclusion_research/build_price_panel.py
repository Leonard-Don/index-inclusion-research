from __future__ import annotations

import argparse

from index_inclusion_research import load_project_config
from index_inclusion_research.loaders import (
    load_benchmarks,
    load_events,
    load_prices,
    save_dataframe,
)
from index_inclusion_research.pipeline import build_event_panel
from index_inclusion_research.workflow_profiles import (
    add_profile_argument,
    resolve_profile_args,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build event-window price panel.")
    add_profile_argument(parser)
    parser.add_argument("--events", default="", help="Cleaned events CSV.")
    parser.add_argument("--prices", default="", help="Daily prices CSV.")
    parser.add_argument("--benchmarks", default="", help="Benchmark returns CSV.")
    parser.add_argument("--output", default="", help="Panel output CSV.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = resolve_profile_args(parser.parse_args(argv), workflow="build_price_panel")

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
    print(f"Saved {len(panel)} panel rows to {args.output} (profile: {args.profile})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
