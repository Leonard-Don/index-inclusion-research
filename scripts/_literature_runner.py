from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
for path in [SRC, SCRIPTS]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import pandas as pd

from download_real_data import build_real_dataset
from index_inclusion_research.analysis import build_regression_dataset, compute_event_study, run_regressions
from index_inclusion_research.loaders import (
    load_benchmarks,
    load_events,
    load_prices,
    save_dataframe,
)
from index_inclusion_research.outputs import plot_average_paths
from index_inclusion_research.pipeline import build_event_panel, build_event_sample, build_matched_sample

RAW_REAL_PATHS = {
    "events": ROOT / "data" / "raw" / "real_events.csv",
    "prices": ROOT / "data" / "raw" / "real_prices.csv",
    "benchmarks": ROOT / "data" / "raw" / "real_benchmarks.csv",
    "metadata": ROOT / "data" / "raw" / "real_metadata.csv",
}


def ensure_real_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not all(path.exists() for path in RAW_REAL_PATHS.values()):
        events, prices, benchmarks, metadata = build_real_dataset(start="2024-01-01", end="2026-01-15")
        save_dataframe(events, RAW_REAL_PATHS["events"])
        save_dataframe(prices, RAW_REAL_PATHS["prices"])
        save_dataframe(benchmarks, RAW_REAL_PATHS["benchmarks"])
        save_dataframe(metadata, RAW_REAL_PATHS["metadata"])
    return (
        load_events(RAW_REAL_PATHS["events"]),
        load_prices(RAW_REAL_PATHS["prices"]),
        load_benchmarks(RAW_REAL_PATHS["benchmarks"]),
    )


def prepare_clean_events(events: pd.DataFrame) -> pd.DataFrame:
    return build_event_sample(events)


def filter_events(
    events: pd.DataFrame,
    markets: list[str] | None = None,
    index_name: str | None = None,
) -> pd.DataFrame:
    filtered = events.copy()
    if markets:
        filtered = filtered.loc[filtered["market"].isin(markets)].copy()
    if index_name:
        filtered = filtered.loc[filtered["index_name"] == index_name].copy()
    return filtered.reset_index(drop=True)


def prepare_panel(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    benchmarks: pd.DataFrame,
    window_pre: int,
    window_post: int,
) -> pd.DataFrame:
    return build_event_panel(
        events=events,
        prices=prices,
        benchmarks=benchmarks,
        window_pre=window_pre,
        window_post=window_post,
    )


def prepare_matched_panel(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    benchmarks: pd.DataFrame,
    window_pre: int,
    window_post: int,
    lookback_days: int = 20,
    num_controls: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    matched_events, diagnostics = build_matched_sample(
        events=events,
        prices=prices,
        lookback_days=lookback_days,
        num_controls=num_controls,
    )
    matched_panel = prepare_panel(
        events=matched_events,
        prices=prices,
        benchmarks=benchmarks,
        window_pre=window_pre,
        window_post=window_post,
    )
    return matched_panel, diagnostics


def run_event_study_bundle(
    panel: pd.DataFrame,
    car_windows: list[tuple[int, int]] | list[list[int]],
    output_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    event_level, summary, average_paths = compute_event_study(panel, car_windows)
    save_dataframe(event_level, output_dir / "event_level_metrics.csv")
    save_dataframe(summary, output_dir / "event_study_summary.csv")
    save_dataframe(average_paths, output_dir / "average_paths.csv")
    plot_average_paths(average_paths, output_dir / "figures")
    return event_level, summary, average_paths


def run_regression_bundle(
    panel: pd.DataFrame,
    car_windows: list[tuple[int, int]] | list[list[int]],
    output_dir: Path,
    main_car_slug: str = "m1_p1",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset = build_regression_dataset(panel, car_windows)
    coefficients, model_stats = run_regressions(dataset, main_car_slug=main_car_slug)
    save_dataframe(dataset, output_dir / "regression_dataset.csv")
    save_dataframe(coefficients, output_dir / "regression_coefficients.csv")
    save_dataframe(model_stats, output_dir / "regression_models.csv")
    return dataset, coefficients, model_stats


def write_markdown(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def print_frame(title: str, frame: pd.DataFrame, columns: list[str] | None = None, max_rows: int = 12) -> None:
    print(f"\n=== {title} ===")
    if frame.empty:
        print("No rows.")
        return
    display = frame.loc[:, columns] if columns else frame
    print(display.head(max_rows).to_string(index=False))
