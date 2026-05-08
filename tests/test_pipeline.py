from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis import (
    build_regression_dataset,
    compute_event_study,
    filter_nonoverlap_event_windows,
    summarize_market_model_estimation_obs,
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


def test_build_event_panel_can_opt_into_market_model_abnormal_returns() -> None:
    dates = pd.bdate_range("2024-01-02", periods=10)
    event_date = dates[6]
    alpha = 0.001
    beta = 1.4
    event_shock = 0.03
    benchmark_returns = np.linspace(-0.01, 0.012, len(dates))
    price_rows = []
    benchmark_rows = []
    for date, benchmark_ret in zip(dates, benchmark_returns, strict=True):
        ret = alpha + beta * benchmark_ret + (event_shock if date == event_date else 0.0)
        price_rows.append(
            {
                "market": "CN",
                "ticker": "CN01",
                "date": date,
                "close": 100.0,
                "ret": ret,
                "volume": 1_000_000,
                "turnover": 0.01,
                "mkt_cap": 1e9,
                "sector": "Technology",
            }
        )
        benchmark_rows.append({"market": "CN", "date": date, "benchmark_ret": benchmark_ret})

    events = pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN01",
                "announce_date": event_date,
                "effective_date": event_date,
                "event_type": "addition",
                "sector": "Technology",
                "inclusion": 1,
                "treatment_group": 1,
                "event_id": "event-1",
            }
        ]
    )
    prices = pd.DataFrame(price_rows)
    benchmarks = pd.DataFrame(benchmark_rows)

    default_panel = build_event_panel(events, prices, benchmarks, window_pre=6, window_post=1)
    assert "ar_market_model" not in default_panel.columns

    market_model_panel = build_event_panel(
        events,
        prices,
        benchmarks,
        window_pre=6,
        window_post=1,
        include_market_model_ar=True,
        market_model_estimation_window=(-6, -2),
    )

    assert {"ar_market_model", "market_model_alpha", "market_model_beta"}.issubset(
        market_model_panel.columns
    )
    announce = market_model_panel.loc[market_model_panel["event_phase"] == "announce"]
    event_day = announce.loc[announce["relative_day"] == 0].iloc[0]
    assert event_day["market_model_alpha"] == pytest.approx(alpha)
    assert event_day["market_model_beta"] == pytest.approx(beta)
    assert event_day["ar_market_model"] == pytest.approx(event_shock)
    assert event_day["ar"] != pytest.approx(event_shock)


def test_build_event_panel_preserves_market_model_columns_when_no_events_match() -> None:
    """When ``--include-market-model-ar`` is set but no event row has matching
    prices/benchmarks, the empty panel must still expose the four market-model
    columns (``ar_market_model`` / ``market_model_alpha`` / ``market_model_beta``
    / ``market_model_estimation_obs``).

    Why: commit ``cf3d29c`` already guaranteed those columns on an empty input
    inside ``compute_market_model_abnormal_returns``. ``build_event_panel``,
    however, short-circuits with ``pd.DataFrame()`` (zero columns) when no
    event row produces a window — *before* the helper is invoked. The
    cf3d29c contract therefore evaporates one layer up: the same
    ``--include-market-model-ar`` CLI run against a "no events match prices"
    input collapses the CSV schema to zero columns and forces every
    downstream consumer (paper bundle, dashboards,
    ``summarize_market_model_estimation_obs``) to branch on column presence
    or raise ``KeyError`` on a state that should be a deterministic
    "no events" rollup.

    Anchor the schema at the pipeline boundary: an opted-in empty panel must
    be interchangeable with an opted-in populated panel for downstream
    consumers, including the existing audit helper.
    """
    events = pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN_NO_PRICES",
                "announce_date": pd.Timestamp("2024-02-09"),
                "effective_date": pd.Timestamp("2024-02-09"),
                "event_type": "addition",
                "sector": "Technology",
                "inclusion": 1,
                "treatment_group": 1,
                "event_id": "missing",
            }
        ]
    )
    prices = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "CN_OTHER",
                "date": pd.Timestamp("2024-02-09"),
                "close": 100.0,
                "ret": 0.001,
                "volume": 1_000_000,
                "turnover": 0.01,
                "mkt_cap": 1e9,
                "sector": "Technology",
            }
        ]
    )
    benchmarks = pd.DataFrame(
        [{"market": "CN", "date": pd.Timestamp("2024-02-09"), "benchmark_ret": 0.0005}]
    )

    panel = build_event_panel(
        events,
        prices,
        benchmarks,
        window_pre=2,
        window_post=2,
        include_market_model_ar=True,
    )

    expected_columns = {
        "ar_market_model",
        "market_model_alpha",
        "market_model_beta",
        "market_model_estimation_obs",
    }
    assert len(panel) == 0
    assert expected_columns.issubset(panel.columns), (
        f"empty panel must still expose market-model columns, got {list(panel.columns)!r}"
    )

    summary = summarize_market_model_estimation_obs(panel)
    assert int(summary.iloc[0]["n_events_total"]) == 0
    assert int(summary.iloc[0]["minimum_estimation_obs"]) == 2


def test_build_event_panel_empty_market_model_panel_matches_populated_schema() -> None:
    """An empty event panel built with ``--include-market-model-ar`` must
    expose the same column SCHEMA (set and order) as a populated panel built
    with the same flag, so downstream CLI consumers can ``pd.read_csv`` it
    without branching on whether the run produced events.

    Why: ``run-event-study`` loads the panel with
    ``pd.read_csv(panel, parse_dates=["event_date_raw", "mapped_market_date",
    "event_date", "date"])``. If ``--include-market-model-ar`` is set but no
    event matches prices, ``build_event_panel`` currently returns a frame
    with *only* the four market-model columns — the four date columns
    ``run-event-study`` parses are gone, so loading the saved CSV raises
    ``ValueError: Missing column provided to 'parse_dates'`` *before* the
    downstream code can short-circuit on the "no events" state. That turns
    a clean empty-result pipeline path into a hard error.

    Commit ``cf3d29c`` anchored the four market-model columns at the empty-
    panel boundary; the natural next step is to anchor the full standard
    panel schema there too, so the empty and populated paths are
    interchangeable for every downstream consumer.
    """
    market_dates = pd.bdate_range("2024-01-02", periods=10)
    benchmarks = pd.DataFrame(
        [
            {"market": "CN", "date": date, "benchmark_ret": 0.0005}
            for date in market_dates
        ]
    )
    prices = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "CN01",
                "date": date,
                "close": 100.0 + idx,
                "ret": 0.001 * idx,
                "volume": 1_000_000,
                "turnover": 0.01,
                "mkt_cap": 1e9,
                "sector": "Technology",
            }
            for idx, date in enumerate(market_dates)
        ]
    )
    populated_events = pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN01",
                "announce_date": market_dates[5],
                "effective_date": market_dates[5],
                "event_type": "addition",
                "sector": "Technology",
                "inclusion": 1,
                "treatment_group": 1,
                "event_id": "populated",
            }
        ]
    )
    empty_events = populated_events.assign(
        ticker="CN_NO_PRICES", event_id="missing"
    )

    populated = build_event_panel(
        populated_events,
        prices,
        benchmarks,
        window_pre=2,
        window_post=2,
        include_market_model_ar=True,
    )
    empty = build_event_panel(
        empty_events,
        prices,
        benchmarks,
        window_pre=2,
        window_post=2,
        include_market_model_ar=True,
    )

    assert len(populated) > 0
    assert len(empty) == 0
    assert list(empty.columns) == list(populated.columns), (
        "empty panel column schema must match populated panel; "
        f"missing: {set(populated.columns) - set(empty.columns)}; "
        f"extra: {set(empty.columns) - set(populated.columns)}"
    )


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
