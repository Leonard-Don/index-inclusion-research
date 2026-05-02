from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis import (
    build_regression_dataset,
    compute_event_study,
    filter_nonoverlap_event_windows,
    winsorize_event_level_metrics,
)
from index_inclusion_research.pipeline import (
    build_event_panel,
    build_event_sample,
    build_matched_sample,
    map_to_trading_date,
)


def _sample_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN01",
                "announce_date": "2024-01-06",
                "effective_date": "2024-01-08",
                "event_type": "addition",
                "sector": "Technology",
                "inclusion": 1,
                "treatment_group": 1,
            },
            {
                "market": "US",
                "index_name": "SP500",
                "ticker": "US01",
                "announce_date": "2024-01-06",
                "effective_date": "2024-01-08",
                "event_type": "deletion",
                "sector": "Technology",
                "inclusion": 0,
                "treatment_group": 1,
            },
        ]
    )


def _sample_prices_and_benchmarks() -> tuple[pd.DataFrame, pd.DataFrame]:
    cn_dates = pd.bdate_range("2024-01-02", periods=8)
    us_dates = pd.bdate_range("2024-01-02", periods=8)
    price_rows = []
    benchmark_rows = []
    for market, ticker, dates, benchmark_ret in [
        ("CN", "CN01", cn_dates, 0.01),
        ("CN", "CN99", cn_dates, 0.01),
        ("US", "US01", us_dates, 0.02),
        ("US", "US99", us_dates, 0.02),
    ]:
        for idx, date in enumerate(dates):
            ret = benchmark_ret + (0.01 if idx == 4 else 0.0) if ticker in {"CN01", "US01"} else benchmark_ret
            price_rows.append(
                {
                    "market": market,
                    "ticker": ticker,
                    "date": date,
                    "close": 100 + idx,
                    "ret": ret,
                    "volume": 1_000_000 + idx * 1_000,
                    "turnover": 0.02 + idx * 0.001,
                    "mkt_cap": 1e10 + idx * 1e8,
                    "sector": "Technology",
                }
            )
            benchmark_rows.append({"market": market, "date": date, "benchmark_ret": benchmark_ret})
    prices = pd.DataFrame(price_rows)
    benchmarks = pd.DataFrame(benchmark_rows).drop_duplicates(["market", "date"])
    return prices, benchmarks


def test_map_to_trading_date_uses_market_calendar() -> None:
    trading_dates = pd.bdate_range("2024-01-02", periods=5)
    mapped = map_to_trading_date(pd.Timestamp("2024-01-06"), trading_dates)
    assert mapped == pd.Timestamp("2024-01-08")


def test_build_event_sample_deduplicates_exact_rows() -> None:
    events = _sample_events()
    duplicated = pd.concat([events, events.iloc[[0]]], ignore_index=True)
    duplicated["announce_date"] = pd.to_datetime(duplicated["announce_date"])
    duplicated["effective_date"] = pd.to_datetime(duplicated["effective_date"])
    cleaned = build_event_sample(duplicated, duplicate_window_days=10)
    assert len(cleaned) == 2
    assert cleaned["is_exact_duplicate"].sum() >= 1


def test_event_panel_and_abnormal_returns_are_correct() -> None:
    events = _sample_events()
    events["announce_date"] = pd.to_datetime(events["announce_date"])
    events["effective_date"] = pd.to_datetime(events["effective_date"])
    cleaned = build_event_sample(events)
    prices, benchmarks = _sample_prices_and_benchmarks()
    panel = build_event_panel(cleaned, prices, benchmarks, window_pre=2, window_post=2)
    cn_announce = panel.loc[(panel["market"] == "CN") & (panel["event_phase"] == "announce")]
    event_day = cn_announce.loc[cn_announce["relative_day"] == 0].iloc[0]
    assert event_day["date"] == pd.Timestamp("2024-01-08")
    assert np.isclose(event_day["ar"], 0.01)


def test_markets_use_separate_benchmarks() -> None:
    events = _sample_events()
    events["announce_date"] = pd.to_datetime(events["announce_date"])
    events["effective_date"] = pd.to_datetime(events["effective_date"])
    cleaned = build_event_sample(events)
    prices, benchmarks = _sample_prices_and_benchmarks()
    panel = build_event_panel(cleaned, prices, benchmarks, window_pre=1, window_post=1)
    cn_ar = panel.loc[(panel["market"] == "CN") & (panel["relative_day"] == 0), "ar"].iloc[0]
    us_ar = panel.loc[(panel["market"] == "US") & (panel["relative_day"] == 0), "ar"].iloc[0]
    assert np.isclose(cn_ar, 0.01)
    assert np.isclose(us_ar, 0.01)


def test_matching_skips_missing_market_cap_candidates_without_failing() -> None:
    events = _sample_events().iloc[[0]].copy()
    events["announce_date"] = pd.to_datetime(events["announce_date"])
    events["effective_date"] = pd.to_datetime(events["effective_date"])
    cleaned = build_event_sample(events)
    prices, _ = _sample_prices_and_benchmarks()
    prices.loc[prices["ticker"] == "CN99", "mkt_cap"] = np.nan
    matched_events, diagnostics = build_matched_sample(cleaned, prices, lookback_days=3, num_controls=1)
    assert not matched_events.empty
    assert "status" in diagnostics.columns


def test_matching_controls_keep_event_direction_and_flip_treatment_group() -> None:
    events = _sample_events().iloc[[0]].copy()
    events["announce_date"] = pd.to_datetime(events["announce_date"])
    events["effective_date"] = pd.to_datetime(events["effective_date"])
    cleaned = build_event_sample(events)
    prices, _ = _sample_prices_and_benchmarks()
    matched_events, _ = build_matched_sample(cleaned, prices, lookback_days=3, num_controls=1)
    treated = matched_events.loc[matched_events["treatment_group"] == 1].iloc[0]
    control = matched_events.loc[matched_events["treatment_group"] == 0].iloc[0]
    assert treated["inclusion"] == 1
    assert control["inclusion"] == 1
    assert control["matched_to_event_id"] == treated["event_id"]


def test_balance_aware_matching_can_penalize_sector_instead_of_hard_filter() -> None:
    events = pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN01",
                "announce_date": pd.Timestamp("2024-01-08"),
                "effective_date": pd.Timestamp("2024-01-08"),
                "event_type": "addition",
                "sector": "Technology",
                "inclusion": 1,
                "treatment_group": 1,
                "event_id": "event-1",
            }
        ]
    )
    rows: list[dict[str, object]] = []
    for ticker, sector, mkt_cap in (
        ("CN01", "Technology", 1.0e9),
        ("CN02", "Technology", 1.8e9),
        ("CN03", "Industrials", 1.0e9),
    ):
        for idx, date in enumerate(pd.bdate_range("2024-01-02", periods=6)):
            rows.append(
                {
                    "market": "CN",
                    "ticker": ticker,
                    "date": date,
                    "close": 100 + idx,
                    "ret": 0.001,
                    "volume": 1_000_000,
                    "turnover": 0.01,
                    "mkt_cap": mkt_cap,
                    "sector": sector,
                }
            )
    prices = pd.DataFrame(rows)

    exact, _ = build_matched_sample(
        events,
        prices,
        lookback_days=3,
        num_controls=1,
        sector_filter_mode="exact_when_available",
    )
    penalized, diagnostics = build_matched_sample(
        events,
        prices,
        lookback_days=3,
        num_controls=1,
        sector_filter_mode="penalized",
        distance_weights={
            "size": 1.0,
            "pre_event_return": 1.0,
            "pre_event_volatility": 20.0,
            "sector_mismatch": 0.5,
        },
        directional_penalties={
            "larger_mkt_cap": 8.0,
            "lower_pre_event_volatility": 8.0,
        },
    )

    assert exact.loc[exact["treatment_group"] == 0, "ticker"].iloc[0] == "CN02"
    assert penalized.loc[penalized["treatment_group"] == 0, "ticker"].iloc[0] == "CN03"
    assert bool(diagnostics["sector_relaxed"].iloc[0]) is True


def test_event_study_and_regression_dataset_outputs() -> None:
    events = _sample_events()
    events["announce_date"] = pd.to_datetime(events["announce_date"])
    events["effective_date"] = pd.to_datetime(events["effective_date"])
    cleaned = build_event_sample(events)
    prices, benchmarks = _sample_prices_and_benchmarks()
    panel = build_event_panel(cleaned, prices, benchmarks, window_pre=5, window_post=5)
    event_level, summary, average_paths = compute_event_study(panel, [(-1, 1), (-3, 3)])
    dataset = build_regression_dataset(panel, [(-1, 1), (-3, 3)])
    assert {"car_m1_p1", "car_m3_p3"}.issubset(event_level.columns)
    assert not summary.empty
    assert not average_paths.empty
    assert {"se_car", "ci_low_95", "ci_high_95"}.issubset(summary.columns)
    assert {"se_car", "ci_low_95", "ci_high_95"}.issubset(average_paths.columns)
    assert "turnover_change" in dataset.columns
    assert "treatment_group" in dataset.columns


def test_filter_nonoverlap_event_windows_excludes_close_repeat_events() -> None:
    frame = pd.DataFrame(
        [
            {"event_id": "e1", "event_ticker": "AAA", "event_phase": "announce", "event_date": "2024-01-01"},
            {"event_id": "e2", "event_ticker": "AAA", "event_phase": "announce", "event_date": "2024-03-01"},
            {"event_id": "e3", "event_ticker": "AAA", "event_phase": "announce", "event_date": "2024-10-01"},
        ]
    )
    filtered = filter_nonoverlap_event_windows(frame, days=120)
    assert filtered["event_id"].tolist() == ["e3"]


def test_winsorize_event_level_metrics_clips_extreme_car_values() -> None:
    frame = pd.DataFrame(
        [
            {"market": "US", "event_phase": "announce", "inclusion": 1, "event_id": "e1", "car_m1_p1": 0.01},
            {"market": "US", "event_phase": "announce", "inclusion": 1, "event_id": "e2", "car_m1_p1": 0.02},
            {"market": "US", "event_phase": "announce", "inclusion": 1, "event_id": "e3", "car_m1_p1": 0.90},
        ]
    )
    winsorized = winsorize_event_level_metrics(frame, quantile=0.01)
    assert winsorized["car_m1_p1"].max() < frame["car_m1_p1"].max()
