from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.loaders import save_dataframe

ROOT = paths.project_root()


def _market_tickers(prefix: str) -> list[str]:
    return [f"{prefix}{idx:02d}" for idx in range(1, 9)]


def main(argv: list[str] | None = None) -> None:
    del argv

    rng = np.random.default_rng(42)
    raw_dir = ROOT / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    market_specs = {
        "CN": {
            "index_name": "CSI300",
            "tickers": _market_tickers("CN"),
            "benchmark_mu": 0.0004,
            "benchmark_sigma": 0.007,
            "dates": pd.bdate_range("2024-01-02", "2024-05-31"),
            "event_rows": [
                ("CN01", "2024-02-03", "2024-02-12", "Industrials"),
                ("CN02", "2024-03-09", "2024-03-18", "Technology"),
                ("CN03", "2024-04-20", "2024-04-29", "Consumer"),
            ],
        },
        "US": {
            "index_name": "SP500",
            "tickers": _market_tickers("US"),
            "benchmark_mu": 0.0005,
            "benchmark_sigma": 0.008,
            "dates": pd.bdate_range("2024-01-02", "2024-05-31"),
            "event_rows": [
                ("US01", "2024-02-10", "2024-02-20", "Industrials"),
                ("US02", "2024-03-16", "2024-03-25", "Technology"),
                ("US03", "2024-04-06", "2024-04-15", "Consumer"),
            ],
        },
    }

    benchmark_rows: list[dict[str, object]] = []
    price_rows: list[dict[str, object]] = []
    event_rows: list[dict[str, object]] = []

    for market, spec in market_specs.items():
        dates = spec["dates"]
        benchmark_ret = rng.normal(spec["benchmark_mu"], spec["benchmark_sigma"], len(dates))
        benchmark_rows.extend(
            {"market": market, "date": date, "benchmark_ret": ret}
            for date, ret in zip(dates, benchmark_ret, strict=True)
        )
        benchmark_series = pd.Series(benchmark_ret, index=dates)

        sector_map = {
            ticker: sector
            for ticker, sector in zip(
                spec["tickers"],
                ["Industrials", "Technology", "Consumer", "Healthcare", "Finance", "Energy", "Materials", "Utilities"],
                strict=True,
            )
        }
        base_caps = {ticker: rng.uniform(8e9, 35e9) for ticker in spec["tickers"]}

        treatment_effects: dict[str, dict[str, pd.Timestamp]] = {}
        for ticker, announce_date, effective_date, sector in spec["event_rows"]:
            event_rows.append(
                {
                    "market": market,
                    "index_name": spec["index_name"],
                    "ticker": ticker,
                    "announce_date": announce_date,
                    "effective_date": effective_date,
                    "event_type": "inclusion",
                    "sector": sector,
                    "source": "synthetic_sample",
                    "note": "Synthetic event for pipeline testing",
                }
            )
            treatment_effects[ticker] = {
                "announce": pd.Timestamp(announce_date),
                "effective": pd.Timestamp(effective_date),
            }

        for ticker in spec["tickers"]:
            close = 100.0 + rng.normal(0, 2)
            market_beta = rng.uniform(0.85, 1.15)
            idiosyncratic_noise = rng.normal(0, 0.012, len(dates))
            rets = benchmark_series.to_numpy() * market_beta + idiosyncratic_noise
            volumes = rng.normal(1.5e7, 1.2e6, len(dates)).clip(min=6e6)
            turnovers = rng.normal(0.028, 0.004, len(dates)).clip(min=0.008)
            mkt_caps = np.full(len(dates), base_caps[ticker]) * np.cumprod(1 + rets)

            effects = treatment_effects.get(ticker)
            if effects:
                for phase_name, date_value in effects.items():
                    mapped_date = dates[dates.searchsorted(date_value, side="left")]
                    center = dates.get_loc(mapped_date)
                    for offset in range(-1, 2):
                        loc = center + offset
                        if 0 <= loc < len(dates):
                            rets[loc] += 0.012 if phase_name == "announce" else 0.018
                    for offset in range(0, 6):
                        loc = center + offset
                        if 0 <= loc < len(dates):
                            volumes[loc] *= 1.22 if phase_name == "effective" else 1.12
                            turnovers[loc] *= 1.18 if phase_name == "effective" else 1.08

            for idx, date in enumerate(dates):
                close *= 1 + rets[idx]
                price_rows.append(
                    {
                        "market": market,
                        "ticker": ticker,
                        "date": date,
                        "close": close,
                        "ret": rets[idx],
                        "volume": volumes[idx],
                        "turnover": turnovers[idx],
                        "mkt_cap": mkt_caps[idx],
                        "sector": sector_map[ticker],
                    }
                )

    save_dataframe(pd.DataFrame(event_rows), raw_dir / "sample_events.csv")
    save_dataframe(pd.DataFrame(price_rows), raw_dir / "sample_prices.csv")
    save_dataframe(pd.DataFrame(benchmark_rows), raw_dir / "sample_benchmarks.csv")
    print(f"Sample files written to {raw_dir}")


if __name__ == "__main__":
    main()
