from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

from index_inclusion_research import match_controls as cli
from index_inclusion_research.pipeline import matching
from index_inclusion_research.pipeline.matching import compute_covariate_balance


def _build_prices_for_balance(
    treated_caps: list[float], control_caps: list[float]
) -> pd.DataFrame:
    rows = []
    base_date = pd.Timestamp("2024-01-01")
    tickers: list[tuple[str, float]] = []
    for i, cap in enumerate(treated_caps, start=1):
        tickers.append((f"CN_T{i}", cap))
    for i, cap in enumerate(control_caps, start=1):
        tickers.append((f"CN_C{i}", cap))
    for ticker, mkt_cap in tickers:
        for offset in range(0, 30):
            rows.append(
                {
                    "market": "CN",
                    "ticker": ticker,
                    "date": base_date + pd.Timedelta(days=offset),
                    "ret": 0.001,
                    "mkt_cap": mkt_cap,
                    "sector": "Finance",
                }
            )
    return pd.DataFrame(rows)


def _build_matched_for_balance(
    n_treated: int, n_control: int, announce: str = "2024-01-25"
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for i in range(1, n_treated + 1):
        rows.append(
            {
                "event_id": f"e{i}",
                "market": "CN",
                "ticker": f"CN_T{i}",
                "treatment_group": 1,
                "announce_date": pd.Timestamp(announce),
                "matched_to_event_id": f"e{i}",
            }
        )
    for i in range(1, n_control + 1):
        rows.append(
            {
                "event_id": f"e1-ctrl-{i:02d}",
                "market": "CN",
                "ticker": f"CN_C{i}",
                "treatment_group": 0,
                "announce_date": pd.Timestamp(announce),
                "matched_to_event_id": "e1",
            }
        )
    return pd.DataFrame(rows)


def test_compute_covariate_balance_schema():
    matched = _build_matched_for_balance(n_treated=4, n_control=4)
    prices = _build_prices_for_balance(
        treated_caps=[1.0e9, 1.05e9, 0.95e9, 1.02e9],
        control_caps=[1.0e9, 1.04e9, 0.96e9, 1.01e9],
    )
    out = compute_covariate_balance(matched, prices, lookback_days=10)
    expected = {
        "market",
        "covariate",
        "treated_mean",
        "control_mean",
        "treated_std",
        "control_std",
        "pooled_std",
        "smd",
        "balanced",
        "n_treated",
        "n_control",
    }
    assert expected.issubset(out.columns)
    assert {"mkt_cap_log", "pre_event_return", "pre_event_volatility"}.issubset(
        set(out["covariate"])
    )


def test_compute_covariate_balance_balanced_when_caps_match():
    matched = _build_matched_for_balance(n_treated=4, n_control=4)
    prices = _build_prices_for_balance(
        treated_caps=[1.0e9, 1.04e9, 0.96e9, 1.02e9],
        control_caps=[1.01e9, 1.03e9, 0.97e9, 1.005e9],
    )
    out = compute_covariate_balance(matched, prices, lookback_days=10)
    cap_row = out.loc[out["covariate"] == "mkt_cap_log"].iloc[0]
    assert cap_row["n_treated"] == 4
    assert cap_row["n_control"] == 4
    assert abs(cap_row["smd"]) < 0.10
    assert bool(cap_row["balanced"]) is True


def test_compute_covariate_balance_imbalanced_when_caps_diverge():
    matched = _build_matched_for_balance(n_treated=4, n_control=4)
    prices = _build_prices_for_balance(
        treated_caps=[1.0e9, 1.05e9, 0.97e9, 1.03e9],
        control_caps=[1e7, 1.5e7, 0.8e7, 1.1e7],
    )
    out = compute_covariate_balance(matched, prices, lookback_days=10)
    cap_row = out.loc[out["covariate"] == "mkt_cap_log"].iloc[0]
    assert math.isfinite(cap_row["smd"])
    assert abs(cap_row["smd"]) > 1.0
    assert bool(cap_row["balanced"]) is False


def test_compute_covariate_balance_threshold_applies():
    matched = _build_matched_for_balance(n_treated=4, n_control=4)
    prices = _build_prices_for_balance(
        treated_caps=[1.0e9, 1.05e9, 0.97e9, 1.03e9],
        control_caps=[1.5e9, 1.55e9, 1.48e9, 1.52e9],
    )
    strict = compute_covariate_balance(matched, prices, lookback_days=10, smd_threshold=0.05)
    lenient = compute_covariate_balance(matched, prices, lookback_days=10, smd_threshold=100.0)
    cap_row_strict = strict.loc[strict["covariate"] == "mkt_cap_log"].iloc[0]
    cap_row_lenient = lenient.loc[lenient["covariate"] == "mkt_cap_log"].iloc[0]
    assert bool(cap_row_lenient["balanced"]) is True
    assert cap_row_strict["smd"] == cap_row_lenient["smd"]
    assert bool(cap_row_strict["balanced"]) is False


def test_compute_covariate_balance_empty_matched_returns_empty_frame():
    empty = pd.DataFrame(columns=["market", "ticker", "treatment_group", "announce_date"])
    prices = _build_prices_for_balance(treated_caps=[1e9], control_caps=[1e9])
    out = compute_covariate_balance(empty, prices)
    assert out.empty
    assert {"market", "covariate", "smd", "balanced"}.issubset(out.columns)


def test_compute_covariate_balance_deterministic():
    matched = _build_matched_for_balance(n_treated=3, n_control=3)
    prices = _build_prices_for_balance(
        treated_caps=[1.0e9, 1.05e9, 0.95e9],
        control_caps=[1.01e9, 1.03e9, 0.97e9],
    )
    a = compute_covariate_balance(matched, prices, lookback_days=10)
    b = compute_covariate_balance(matched, prices, lookback_days=10)
    assert np.array_equal(a["smd"].fillna(-9).to_numpy(), b["smd"].fillna(-9).to_numpy())


def test_compute_covariate_balance_uses_grouped_price_history(monkeypatch):
    matched = _build_matched_for_balance(n_treated=1, n_control=1)
    prices = _build_prices_for_balance(treated_caps=[1.0e9], control_caps=[1.01e9])
    seen_lengths: list[int] = []
    original = matching._balance_snapshot

    def wrapped_balance_snapshot(*, prices, market, ticker, reference_date, lookback_days):
        seen_lengths.append(len(prices))
        return original(
            prices=prices,
            market=market,
            ticker=ticker,
            reference_date=reference_date,
            lookback_days=lookback_days,
        )

    monkeypatch.setattr(matching, "_balance_snapshot", wrapped_balance_snapshot)
    out = compute_covariate_balance(matched, prices, lookback_days=10)

    assert not out.empty
    assert seen_lengths
    assert max(seen_lengths) == 30


def test_main_writes_balance_csv_alongside_diagnostics(tmp_path: Path) -> None:
    events = tmp_path / "events.csv"
    pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN01",
                "announce_date": "2024-02-05",
                "effective_date": "2024-02-09",
                "event_type": "inclusion",
                "sector": "Industrials",
                "source": "test",
                "note": "",
            }
        ]
    ).to_csv(events, index=False)
    # Pipeline expects events with event_id; build via the CLI helper
    from index_inclusion_research import build_event_sample as bes_cli

    events_clean = tmp_path / "events_clean.csv"
    bes_cli.main(["--input", str(events), "--output", str(events_clean)])

    prices = tmp_path / "prices.csv"
    rows = []
    for ticker in ("CN01", "CN02", "CN03"):
        for offset in range(0, 30):
            date = pd.Timestamp("2024-02-01") + pd.Timedelta(days=offset)
            rows.append(
                {
                    "market": "CN",
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "close": 100.0 + offset,
                    "ret": 0.001 if offset > 0 else 0.0,
                    "volume": 1e6,
                    "turnover": 0.01,
                    "mkt_cap": 1e9,
                    "sector": "Industrials",
                }
            )
    pd.DataFrame(rows).to_csv(prices, index=False)

    matched_out = tmp_path / "matched.csv"
    diag_out = tmp_path / "match_diagnostics.csv"
    rc = cli.main(
        [
            "--events",
            str(events_clean),
            "--prices",
            str(prices),
            "--output-events",
            str(matched_out),
            "--output-diagnostics",
            str(diag_out),
        ]
    )
    assert rc == 0
    assert matched_out.exists()
    assert diag_out.exists()
    balance_path = tmp_path / "match_balance.csv"
    assert balance_path.exists()
    balance = pd.read_csv(balance_path)
    assert {"market", "covariate", "smd", "balanced"}.issubset(balance.columns)
