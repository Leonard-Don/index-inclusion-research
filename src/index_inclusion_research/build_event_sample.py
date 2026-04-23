from __future__ import annotations

import argparse

from index_inclusion_research import load_project_config
from index_inclusion_research.loaders import load_events, save_dataframe
from index_inclusion_research.pipeline import (
    build_event_sample as build_event_sample_frame,
)
from index_inclusion_research.workflow_profiles import (
    add_profile_argument,
    resolve_profile_args,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean and standardize index inclusion events.")
    add_profile_argument(parser)
    parser.add_argument("--input", default="", help="Path to raw events CSV.")
    parser.add_argument("--output", default="", help="Path to save cleaned events.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = resolve_profile_args(parser.parse_args(argv), workflow="build_event_sample")

    config = load_project_config(args.config)
    duplicate_window = config["defaults"]["matching"]["conflict_window_days"]
    events = load_events(args.input)
    cleaned = build_event_sample_frame(events, duplicate_window_days=duplicate_window)
    save_dataframe(cleaned, args.output)
    print(f"Saved {len(cleaned)} cleaned events to {args.output} (profile: {args.profile})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
