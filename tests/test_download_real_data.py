from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research import real_data
from index_inclusion_research.loaders import load_events
from index_inclusion_research.real_data import _build_price_rows, build_cn_events


def test_load_events_infers_inclusion_and_treatment_group(tmp_path: Path) -> None:
    path = tmp_path / "events.csv"
    pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "1",
                "announce_date": "2024-05-31",
                "effective_date": "2024-06-14",
                "event_type": "deletion",
            }
        ]
    ).to_csv(path, index=False)
    loaded = load_events(path)
    row = loaded.iloc[0]
    assert row["ticker"] == "000001"
    assert row["inclusion"] == 0
    assert row["treatment_group"] == 1


def test_build_cn_events_requires_file_driven_columns(tmp_path: Path) -> None:
    path = tmp_path / "cn_events.csv"
    pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "000001",
                "announce_date": "2024-05-31",
                "effective_date": "2024-06-14",
                "event_type": "addition",
                "inclusion": 1,
                "source": "manual",
                "source_url": "https://example.com",
                "security_name": "平安银行",
            }
        ]
    ).to_csv(path, index=False)
    with pytest.raises(ValueError):
        build_cn_events(path)


def test_build_price_rows_skips_obviously_corrupted_histories() -> None:
    security_frame = pd.DataFrame(
        [
            {"market": "US", "ticker": "GOOD", "yahoo_symbol": "GOOD", "sector": "Tech"},
            {"market": "US", "ticker": "BAD", "yahoo_symbol": "BAD", "sector": "Tech"},
        ]
    )
    history_map = {
        "GOOD": pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "Close": [100.0, 102.0, 101.0],
                "Adj Close": [100.0, 102.0, 101.0],
                "Volume": [1000, 1100, 1050],
            }
        ),
        "BAD": pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]),
                "Close": [1.0, 100.0, 2.0, 120.0],
                "Adj Close": [1.0, 100.0, 2.0, 120.0],
                "Volume": [100, 100, 100, 100],
            }
        ),
    }

    prices, metadata = _build_price_rows(security_frame, history_map)

    assert prices["ticker"].unique().tolist() == ["GOOD"]
    assert metadata["ticker"].unique().tolist() == ["GOOD"]


def test_build_real_dataset_can_use_tushare_for_cn_prices_and_benchmark(monkeypatch) -> None:
    us_events = pd.DataFrame(
        [
            {
                "market": "US",
                "index_name": "S&P500",
                "ticker": "AAPL",
                "announce_date": "2024-01-01",
                "effective_date": "2024-01-05",
                "event_type": "addition",
                "inclusion": 1,
                "sector": "Technology",
            }
        ]
    )
    us_constituents = pd.DataFrame(
        [{"Symbol": "MSFT", "GICS Sector": "Technology"}]
    )
    cn_events = pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "000001",
                "announce_date": "2024-01-01",
                "effective_date": "2024-01-05",
                "event_type": "addition",
                "inclusion": 1,
                "sector": "银行",
            }
        ]
    )
    cn_controls = pd.DataFrame([{"成分券代码": "000002"}])
    us_price_rows = pd.DataFrame(
        [
            {
                "market": "US",
                "ticker": "AAPL",
                "date": pd.Timestamp("2024-01-02"),
                "close": 100.0,
                "ret": 0.01,
                "volume": 1000.0,
                "turnover": 0.01,
                "mkt_cap": 1_000_000.0,
                "sector": "Technology",
            }
        ]
    )
    us_metadata = pd.DataFrame(
        [
            {
                "market": "US",
                "ticker": "AAPL",
                "yahoo_symbol": "AAPL",
                "sector": "Technology",
                "shares_outstanding": 10_000.0,
            }
        ]
    )
    cn_prices = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "000001",
                "date": pd.Timestamp("2024-01-02"),
                "close": 10.0,
                "ret": 0.02,
                "volume": 10000.0,
                "turnover": 0.02,
                "mkt_cap": 10_000_000.0,
                "sector": "银行",
            }
        ]
    )
    cn_metadata = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "000001",
                "yahoo_symbol": None,
                "sector": "银行",
                "shares_outstanding": np.nan,
                "data_source": "tushare",
            }
        ]
    )
    cn_benchmark = pd.DataFrame(
        [{"market": "CN", "date": pd.Timestamp("2024-01-02"), "benchmark_ret": 0.005}]
    )
    us_benchmark = pd.DataFrame(
        [{"market": "US", "date": pd.Timestamp("2024-01-02"), "benchmark_ret": 0.003}]
    )
    calls: dict[str, object] = {}

    monkeypatch.setattr(real_data, "build_us_events", lambda **_: (us_events, us_constituents))
    monkeypatch.setattr(real_data, "build_cn_events", lambda _: cn_events)
    monkeypatch.setattr(real_data, "_sample_us_controls", lambda *_, **__: pd.DataFrame())
    monkeypatch.setattr(real_data, "_sample_cn_controls", lambda *_, **__: cn_controls)
    monkeypatch.setattr(real_data, "_chunked_download_history", lambda *_, **__: {})
    monkeypatch.setattr(real_data, "_build_price_rows", lambda *_: (us_price_rows, us_metadata))

    def fake_download_benchmarks(*, start: str, end: str, markets=None):
        calls["benchmark_markets"] = markets
        return us_benchmark

    def fake_fetch_cn_prices(tickers, *, start, end, token, sectors):
        calls["tickers"] = list(tickers)
        calls["token"] = token
        calls["sectors"] = dict(sectors)
        return cn_prices, cn_metadata

    def fake_fetch_cn_benchmark(*, start, end, token, ts_code):
        calls["benchmark_token"] = token
        calls["benchmark_ts_code"] = ts_code
        return cn_benchmark

    monkeypatch.setattr(real_data, "_download_benchmarks", fake_download_benchmarks)
    monkeypatch.setattr(real_data.tushare_source, "fetch_cn_prices", fake_fetch_cn_prices)
    monkeypatch.setattr(real_data.tushare_source, "fetch_cn_benchmark", fake_fetch_cn_benchmark)

    events, prices, benchmarks, metadata = real_data.build_real_dataset(
        start="2024-01-01",
        end="2024-01-10",
        cn_price_source="tushare",
        tushare_token="token-123",
    )

    assert events["market"].tolist() == ["CN", "US"]
    assert prices["market"].tolist() == ["CN", "US"]
    assert benchmarks["market"].tolist() == ["CN", "US"]
    assert metadata["market"].tolist() == ["CN", "US"]
    assert calls["benchmark_markets"] == ["US"]
    assert calls["tickers"] == ["000001", "000002"]
    assert calls["token"] == "token-123"
    assert calls["sectors"] == {"000001": "银行"}
    assert calls["benchmark_token"] == "token-123"
