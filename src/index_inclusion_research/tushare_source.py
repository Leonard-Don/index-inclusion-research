from __future__ import annotations

import os
import time
from typing import Any

import pandas as pd

DEFAULT_CSI300_TS_CODE = "399300.SZ"


def cn_ticker_to_ts_code(ticker: object) -> str:
    code = str(ticker).strip().split(".")[0].zfill(6)
    suffix = "SH" if code.startswith(("6", "9")) else "SZ"
    return f"{code}.{suffix}"


def _normalise_ticker(ticker: object) -> str:
    return str(ticker).strip().split(".")[0].zfill(6)


def _format_tushare_date(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y%m%d")


def _parse_trade_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str), format="%Y%m%d", errors="coerce").dt.normalize()


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def resolve_tushare_token(token: str | None) -> str:
    resolved = token or os.environ.get("TUSHARE_TOKEN") or os.environ.get("TS_TOKEN")
    if not resolved:
        raise RuntimeError("Tushare token missing: set TUSHARE_TOKEN or pass --tushare-token.")
    return resolved


def _load_tushare_module() -> Any:
    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError("tushare is not installed. Run `uv sync` before using --cn-price-source tushare.") from exc
    return ts


def _latest_numeric_value(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric_column(frame, column).dropna()
    if values.empty:
        return None
    return float(values.iloc[-1])


def normalise_tushare_price_frame(
    ticker: object,
    daily: pd.DataFrame,
    *,
    daily_basic: pd.DataFrame | None = None,
    sector: object = pd.NA,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ticker_code = _normalise_ticker(ticker)
    if daily is None or daily.empty:
        prices = pd.DataFrame(
            columns=["market", "ticker", "date", "close", "ret", "volume", "turnover", "mkt_cap", "sector"]
        )
        metadata = pd.DataFrame(
            [
                {
                    "market": "CN",
                    "ticker": ticker_code,
                    "yahoo_symbol": None,
                    "sector": sector,
                    "shares_outstanding": float("nan"),
                    "data_source": "tushare",
                }
            ]
        )
        return prices, metadata

    frame = daily.copy()
    frame["trade_date"] = frame["trade_date"].astype(str)
    if daily_basic is not None and not daily_basic.empty:
        basic = daily_basic.copy()
        basic["trade_date"] = basic["trade_date"].astype(str)
        keep_columns = [
            column
            for column in ("trade_date", "turnover_rate", "total_mv", "total_share")
            if column in basic.columns
        ]
        frame = frame.merge(basic.loc[:, keep_columns], on="trade_date", how="left")
    else:
        basic = pd.DataFrame()

    frame["date"] = _parse_trade_date(frame["trade_date"])
    frame["close"] = _numeric_column(frame, "close")
    frame = frame.loc[frame["date"].notna() & frame["close"].notna()].sort_values("date").reset_index(drop=True)
    ret = _numeric_column(frame, "pct_chg") / 100.0
    if ret.isna().all():
        ret = frame["close"].pct_change(fill_method=None)
    frame["ret"] = ret
    frame["volume"] = _numeric_column(frame, "vol") * 100.0
    frame["turnover"] = _numeric_column(frame, "turnover_rate") / 100.0
    frame["mkt_cap"] = _numeric_column(frame, "total_mv") * 10_000.0
    frame["market"] = "CN"
    frame["ticker"] = ticker_code
    frame["sector"] = sector
    prices = frame.loc[:, ["market", "ticker", "date", "close", "ret", "volume", "turnover", "mkt_cap", "sector"]]

    shares_outstanding: float = float("nan")
    if not basic.empty and "total_share" in basic.columns:
        latest_total_share = _latest_numeric_value(basic, "total_share")
        if latest_total_share is not None:
            shares_outstanding = latest_total_share * 10_000.0
    metadata = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": ticker_code,
                "yahoo_symbol": None,
                "sector": sector,
                "shares_outstanding": shares_outstanding,
                "data_source": "tushare",
            }
        ]
    )
    return prices.reset_index(drop=True), metadata


def normalise_tushare_benchmark_frame(index_daily: pd.DataFrame) -> pd.DataFrame:
    if index_daily is None or index_daily.empty:
        return pd.DataFrame(columns=["market", "date", "benchmark_ret"])
    frame = index_daily.copy()
    frame["date"] = _parse_trade_date(frame["trade_date"])
    frame["close"] = _numeric_column(frame, "close")
    frame = frame.loc[frame["date"].notna()].sort_values("date").reset_index(drop=True)
    ret = _numeric_column(frame, "pct_chg") / 100.0
    if ret.isna().all() and "close" in frame.columns:
        ret = frame["close"].pct_change(fill_method=None)
    frame["benchmark_ret"] = ret
    frame["market"] = "CN"
    return frame.loc[:, ["market", "date", "benchmark_ret"]].dropna(subset=["benchmark_ret"]).reset_index(drop=True)


def _pro_api(token: str | None, *, tushare_module: Any | None = None, pro_api: Any | None = None) -> tuple[Any, Any]:
    if pro_api is not None:
        return pro_api, tushare_module
    ts = tushare_module or _load_tushare_module()
    return ts.pro_api(resolve_tushare_token(token)), ts


def _download_tushare_daily(
    ts_code: str,
    *,
    start_date: str,
    end_date: str,
    pro_api: Any,
    tushare_module: Any | None,
    adj: str,
) -> pd.DataFrame:
    if tushare_module is not None and hasattr(tushare_module, "pro_bar"):
        frame = tushare_module.pro_bar(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            adj=adj,
            asset="E",
            api=pro_api,
        )
        if frame is not None and not frame.empty:
            return frame
    if hasattr(pro_api, "daily"):
        return pro_api.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return pd.DataFrame()


def fetch_cn_prices(
    tickers: list[str],
    *,
    start: str,
    end: str,
    token: str | None = None,
    sectors: dict[str, object] | None = None,
    adj: str = "qfq",
    sleep_seconds: float = 0.2,
    tushare_module: Any | None = None,
    pro_api: Any | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pro, ts = _pro_api(token, tushare_module=tushare_module, pro_api=pro_api)
    start_date = _format_tushare_date(start)
    end_date = _format_tushare_date(end)
    sectors = sectors or {}
    price_frames: list[pd.DataFrame] = []
    metadata_frames: list[pd.DataFrame] = []
    unique_tickers = sorted({_normalise_ticker(ticker) for ticker in tickers})

    for ticker in unique_tickers:
        ts_code = cn_ticker_to_ts_code(ticker)
        daily = _download_tushare_daily(
            ts_code,
            start_date=start_date,
            end_date=end_date,
            pro_api=pro,
            tushare_module=ts,
            adj=adj,
        )
        daily_basic = pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,turnover_rate,total_mv,total_share",
        )
        prices, metadata = normalise_tushare_price_frame(
            ticker,
            daily,
            daily_basic=daily_basic,
            sector=sectors.get(ticker, pd.NA),
        )
        if not prices.empty:
            price_frames.append(prices)
        metadata_frames.append(metadata)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    prices_out = pd.concat(price_frames, ignore_index=True) if price_frames else pd.DataFrame()
    metadata_out = pd.concat(metadata_frames, ignore_index=True) if metadata_frames else pd.DataFrame()
    return prices_out, metadata_out


def fetch_cn_benchmark(
    *,
    start: str,
    end: str,
    token: str | None = None,
    ts_code: str = DEFAULT_CSI300_TS_CODE,
    tushare_module: Any | None = None,
    pro_api: Any | None = None,
) -> pd.DataFrame:
    pro, _ = _pro_api(token, tushare_module=tushare_module, pro_api=pro_api)
    frame = pro.index_daily(
        ts_code=ts_code,
        start_date=_format_tushare_date(start),
        end_date=_format_tushare_date(end),
    )
    return normalise_tushare_benchmark_frame(frame)
