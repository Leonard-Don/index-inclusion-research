from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pandas as pd

from index_inclusion_research import load_project_config
from index_inclusion_research.analysis import (
    compute_event_study,
    compute_patell_bmp_summary,
)
from index_inclusion_research.analysis.event_study import (
    _MARKET_MODEL_MIN_ESTIMATION_OBS,
    compute_market_model_abnormal_returns,
)
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.workflow_profiles import (
    add_profile_argument,
    resolve_profile_args,
)

LOGGER = logging.getLogger(__name__)

_AR_MODEL_ADJUSTED = "adjusted"
_AR_MODEL_MARKET = "market"
_AR_MODEL_CHOICES = (_AR_MODEL_ADJUSTED, _AR_MODEL_MARKET)
_AR_MODEL_DEFAULT = _AR_MODEL_ADJUSTED

# Literature standard short-window estimation window: (-120, -10) trading
# days relative to the event. Exposed as `LOW,HIGH` positive ints on the CLI
# (signed internally) so the documented default reads naturally as the
# canonical "120-day estimation window ending 10 days before the event".
_DEFAULT_MARKET_ESTIMATION_WINDOW: tuple[int, int] = (-120, -10)
_DEFAULT_MARKET_ESTIMATION_WINDOW_CLI = "120,10"


def _parse_estimation_window(raw: str) -> tuple[int, int]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            "--estimation-window must be LOW,HIGH with two positive integers (e.g. 120,10)"
        )
    try:
        low = int(parts[0])
        high = int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--estimation-window components must be integers"
        ) from exc
    if low <= 0 or high <= 0:
        raise argparse.ArgumentTypeError(
            "--estimation-window LOW,HIGH must be positive (signs are added internally)"
        )
    if high >= low:
        raise argparse.ArgumentTypeError(
            "--estimation-window LOW,HIGH requires LOW > HIGH so the signed window "
            "(-LOW, -HIGH) is a proper pre-event interval"
        )
    return (-low, -high)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run event-study summaries on an event panel.")
    add_profile_argument(parser)
    parser.add_argument("--panel", default="", help="Event panel CSV.")
    parser.add_argument("--output-dir", default="", help="Directory for event-study outputs.")
    parser.add_argument("--config", default="config/markets.yml", help="Project config path.")
    parser.add_argument(
        "--ar-model",
        choices=_AR_MODEL_CHOICES,
        default=_AR_MODEL_DEFAULT,
        help=(
            "AR engine: 'adjusted' (default; ar = ret - benchmark_ret) keeps the "
            "main-pipeline output byte-identical to prior runs. 'market' switches "
            "to ar_market_model (OLS market model with alpha/beta estimated on the "
            "--estimation-window pre-event window). This is an additive option; "
            "Patell/BMP and the CMA hypothesis verdicts are unaffected."
        ),
    )
    parser.add_argument(
        "--estimation-window",
        type=_parse_estimation_window,
        default=_DEFAULT_MARKET_ESTIMATION_WINDOW,
        metavar="LOW,HIGH",
        help=(
            "Market-model estimation window as positive ints LOW,HIGH; the signed "
            "window is (-LOW, -HIGH). Default 120,10 matches the short-window "
            "event-study literature standard. Only consulted when --ar-model=market."
        ),
    )
    return parser


def _build_skipped_events_frame(panel: pd.DataFrame) -> pd.DataFrame:
    """Per event/phase rows whose market-model AR is NaN — sufficient-obs gate.

    These rows would otherwise contribute a silent 0 to CAR sums; we surface
    them as a sidecar so reviewers can audit estimation-window thinning vs
    genuine null findings.
    """
    required = {
        "event_id",
        "event_phase",
        "ar_market_model",
        "market_model_estimation_obs",
    }
    if panel.empty or not required.issubset(panel.columns):
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_phase",
                "market_model_estimation_obs",
                "minimum_estimation_obs",
                "reason",
            ]
        )

    per_event = (
        panel.groupby(["event_id", "event_phase"], dropna=False, sort=False)
        .agg(
            market_model_estimation_obs=("market_model_estimation_obs", "first"),
            ar_finite=("ar_market_model", lambda series: series.notna().any()),
        )
        .reset_index()
    )
    skipped = per_event.loc[~per_event["ar_finite"]].copy()
    if skipped.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_phase",
                "market_model_estimation_obs",
                "minimum_estimation_obs",
                "reason",
            ]
        )

    skipped["minimum_estimation_obs"] = _MARKET_MODEL_MIN_ESTIMATION_OBS
    skipped["reason"] = skipped["market_model_estimation_obs"].apply(
        lambda obs: (
            "insufficient_estimation_obs"
            if (pd.notna(obs) and int(obs) < _MARKET_MODEL_MIN_ESTIMATION_OBS)
            else "degenerate_benchmark_variance"
        )
    )
    return skipped[
        [
            "event_id",
            "event_phase",
            "market_model_estimation_obs",
            "minimum_estimation_obs",
            "reason",
        ]
    ]


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = resolve_profile_args(parser.parse_args(argv), workflow="run_event_study")

    config = load_project_config(args.config)
    panel = pd.read_csv(args.panel, parse_dates=["event_date_raw", "mapped_market_date", "event_date", "date"])
    car_windows = config["defaults"]["car_windows"]

    ar_model: str = args.ar_model
    estimation_window: tuple[int, int] = tuple(args.estimation_window)
    ar_column = "ar"

    if ar_model == _AR_MODEL_MARKET:
        panel = compute_market_model_abnormal_returns(
            panel, estimation_window=estimation_window
        )
        ar_column = "ar_market_model"

    event_level, summary, average_paths = compute_event_study(
        panel, car_windows, ar_column=ar_column
    )
    # Patell/BMP stays bound to the simple AR ('ar') by design: the assignment
    # 'do not promote Patell/BMP into the main CAR tables' applies, so we just
    # rerun the legacy summary path against the unchanged 'ar' column.
    patell_bmp = compute_patell_bmp_summary(panel, car_windows)

    output_dir = Path(args.output_dir)
    save_dataframe(event_level, output_dir / "event_level_metrics.csv")
    save_dataframe(summary, output_dir / "event_study_summary.csv")
    save_dataframe(average_paths, output_dir / "average_paths.csv")
    save_dataframe(patell_bmp, output_dir / "patell_bmp_summary.csv")

    meta: dict[str, object] = {
        "ar_model": ar_model,
        "ar_column": ar_column,
        "estimation_window": (
            list(estimation_window) if ar_model == _AR_MODEL_MARKET else None
        ),
        "profile": args.profile,
        "panel": str(args.panel),
        "car_windows": [list(window) for window in car_windows],
        "schema_version": 1,
    }

    if ar_model == _AR_MODEL_MARKET:
        skipped = _build_skipped_events_frame(panel)
        meta["n_events_skipped"] = int(len(skipped))
        save_dataframe(skipped, output_dir / "event_study_skipped_events.csv")
        if not skipped.empty:
            LOGGER.warning(
                "ar-model=market: %s event/phase rows have NaN ar_market_model "
                "(min estimation obs = %s); see event_study_skipped_events.csv",
                len(skipped),
                _MARKET_MODEL_MIN_ESTIMATION_OBS,
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "event_study_meta.json").write_text(
        json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
    )

    print(
        f"Saved event-study outputs to {output_dir} "
        f"(profile: {args.profile}, ar_model: {ar_model})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
