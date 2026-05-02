from __future__ import annotations

import argparse
from pathlib import Path

from index_inclusion_research import load_project_config
from index_inclusion_research.loaders import load_events, load_prices, save_dataframe
from index_inclusion_research.pipeline import (
    build_matched_sample,
    compute_covariate_balance,
)
from index_inclusion_research.workflow_profiles import (
    add_profile_argument,
    resolve_profile_args,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build matched-control pseudo-events.")
    add_profile_argument(parser)
    parser.add_argument("--events", default="", help="Cleaned events CSV.")
    parser.add_argument("--prices", default="", help="Daily prices CSV.")
    parser.add_argument("--output-events", default="", help="Output matched events CSV.")
    parser.add_argument("--output-diagnostics", default="", help="Match diagnostics CSV.")
    parser.add_argument(
        "--output-balance",
        default="",
        help="Optional covariate balance CSV. Defaults to <output-diagnostics>/match_balance.csv.",
    )
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    return parser


def _default_balance_path(diagnostics_path: str) -> str:
    if not diagnostics_path:
        return ""
    return str(Path(diagnostics_path).with_name("match_balance.csv"))


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = resolve_profile_args(parser.parse_args(argv), workflow="match_controls")

    config = load_project_config(args.config)
    matching = config["defaults"]["matching"]
    events = load_events(args.events)
    prices = load_prices(args.prices)
    matched_events, diagnostics = build_matched_sample(
        events=events,
        prices=prices,
        lookback_days=matching["lookback_days"],
        num_controls=matching["num_controls"],
        reference_date_column=matching["reference_date_column"],
        sector_filter_mode=matching.get("sector_filter_mode", "exact_when_available"),
        distance_weights=matching.get("distance_weights"),
        directional_penalties=matching.get("directional_penalties"),
    )
    balance = compute_covariate_balance(
        matched_events,
        prices,
        lookback_days=matching["lookback_days"],
        reference_date_column=matching["reference_date_column"],
        smd_threshold=matching.get("smd_threshold", 0.25),
    )
    save_dataframe(matched_events, args.output_events)
    save_dataframe(diagnostics, args.output_diagnostics)
    balance_path = args.output_balance or _default_balance_path(args.output_diagnostics)
    if balance_path:
        save_dataframe(balance, balance_path)
    print(f"Saved {len(matched_events)} matched events to {args.output_events} (profile: {args.profile})")
    if balance_path:
        print(f"Saved covariate balance to {balance_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
