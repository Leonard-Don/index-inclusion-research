from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.tushare_source import (
    cn_ticker_to_ts_code,
    fetch_cn_prices,
    normalise_tushare_benchmark_frame,
    normalise_tushare_price_frame,
    resolve_tushare_token,
)


def test_cn_ticker_to_ts_code_uses_cn_exchange_suffixes() -> None:
    assert cn_ticker_to_ts_code("1") == "000001.SZ"
    assert cn_ticker_to_ts_code("600000") == "600000.SH"
    assert cn_ticker_to_ts_code("688981") == "688981.SH"


def test_resolve_tushare_token_accepts_env_fallback(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setenv("TS_TOKEN", "env-token")

    assert resolve_tushare_token(None) == "env-token"


def test_resolve_tushare_token_raises_clear_error_without_token(monkeypatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TS_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="Tushare token missing"):
        resolve_tushare_token(None)


def test_normalise_tushare_price_frame_maps_project_schema_and_units() -> None:
    daily = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240103",
                "close": 10.5,
                "pct_chg": 1.2,
                "vol": 123.0,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "close": 10.0,
                "pct_chg": -0.5,
                "vol": 100.0,
            },
        ]
    )
    daily_basic = pd.DataFrame(
        [
            {"trade_date": "20240102", "turnover_rate": 2.5, "total_mv": 1000.0},
            {"trade_date": "20240103", "turnover_rate": 3.0, "total_mv": 1100.0},
        ]
    )

    prices, metadata = normalise_tushare_price_frame(
        "1",
        daily,
        daily_basic=daily_basic,
        sector="银行",
    )

    assert prices["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert prices["market"].tolist() == ["CN", "CN"]
    assert prices["ticker"].tolist() == ["000001", "000001"]
    assert prices["ret"].round(3).tolist() == [-0.005, 0.012]
    assert prices["volume"].tolist() == [10000.0, 12300.0]
    assert prices["turnover"].round(3).tolist() == [0.025, 0.03]
    assert prices["mkt_cap"].tolist() == [10_000_000.0, 11_000_000.0]
    assert prices["sector"].tolist() == ["银行", "银行"]
    metadata_row = metadata.iloc[0]
    assert metadata_row["market"] == "CN"
    assert metadata_row["ticker"] == "000001"
    assert pd.isna(metadata_row["yahoo_symbol"])
    assert metadata_row["sector"] == "银行"
    assert pd.isna(metadata_row["shares_outstanding"])
    assert metadata_row["data_source"] == "tushare"


def test_normalise_tushare_benchmark_frame_maps_project_schema() -> None:
    index_daily = pd.DataFrame(
        [
            {"ts_code": "399300.SZ", "trade_date": "20240103", "pct_chg": 1.5, "close": 3300.0},
            {"ts_code": "399300.SZ", "trade_date": "20240102", "pct_chg": -0.2, "close": 3250.0},
        ]
    )

    benchmarks = normalise_tushare_benchmark_frame(index_daily)

    assert benchmarks["market"].tolist() == ["CN", "CN"]
    assert benchmarks["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert benchmarks["benchmark_ret"].round(3).tolist() == [-0.002, 0.015]


def test_fetch_cn_prices_passes_client_as_tushare_pro_bar_api_argument() -> None:
    daily = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "close": 10.0,
                "pct_chg": 0.5,
                "vol": 100.0,
            }
        ]
    )
    daily_basic = pd.DataFrame(
        [{"trade_date": "20240102", "turnover_rate": 1.5, "total_mv": 1000.0}]
    )
    calls: dict[str, object] = {}

    class FakePro:
        def daily_basic(self, **kwargs):
            calls["daily_basic"] = kwargs
            return daily_basic

    class FakeTushare:
        def pro_bar(self, **kwargs):
            calls["pro_bar"] = kwargs
            return daily

    pro = FakePro()

    prices, _ = fetch_cn_prices(
        ["000001"],
        start="2024-01-01",
        end="2024-01-03",
        token="token",
        sectors={},
        sleep_seconds=0,
        tushare_module=FakeTushare(),
        pro_api=pro,
    )

    assert prices["ticker"].tolist() == ["000001"]
    assert calls["pro_bar"]["api"] is pro
    assert "pro_api" not in calls["pro_bar"]
