from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from index_inclusion_research import load_project_config
from index_inclusion_research.analysis import build_regression_dataset, run_regressions as run_regressions_bundle
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.workflow_profiles import add_profile_argument, resolve_profile_args


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run cross-market event-level regressions.")
    add_profile_argument(parser)
    parser.add_argument("--panel", default="", help="Matched event panel CSV.")
    parser.add_argument("--output-dir", default="", help="Directory for regression outputs.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = resolve_profile_args(parser.parse_args(argv), workflow="run_regressions")

    config = load_project_config(args.config)
    panel = pd.read_csv(
        args.panel,
        parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"],
        low_memory=False,
    )
    dataset = build_regression_dataset(panel, config["defaults"]["car_windows"])
    coefficients, model_stats = run_regressions_bundle(dataset)
    output_dir = Path(args.output_dir)
    save_dataframe(dataset, output_dir / "regression_dataset.csv")
    save_dataframe(coefficients, output_dir / "regression_coefficients.csv")
    save_dataframe(model_stats, output_dir / "regression_models.csv")
    print(f"Saved regression outputs to {output_dir} (profile: {args.profile})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
