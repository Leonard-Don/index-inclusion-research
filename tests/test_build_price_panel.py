from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import build_event_sample as build_events_cli
from index_inclusion_research import build_price_panel as cli


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    raw_events = tmp_path / "events.csv"
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
    ).to_csv(raw_events, index=False)
    cleaned = tmp_path / "events_clean.csv"
    build_events_cli.main(["--input", str(raw_events), "--output", str(cleaned)])

    prices = tmp_path / "prices.csv"
    rows = []
    for offset in range(0, 30):
        date = pd.Timestamp("2024-02-01") + pd.Timedelta(days=offset)
        rows.append(
            {
                "market": "CN",
                "ticker": "CN01",
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

    bench = tmp_path / "benchmarks.csv"
    pd.DataFrame(
        [
            {
                "market": "CN",
                "date": (pd.Timestamp("2024-02-01") + pd.Timedelta(days=i)).date().isoformat(),
                "benchmark_ret": 0.0005,
            }
            for i in range(0, 30)
        ]
    ).to_csv(bench, index=False)

    out = tmp_path / "panel.csv"
    return cleaned, prices, bench, out


def test_main_writes_panel_csv(tmp_path: Path) -> None:
    events, prices, bench, out = _write_inputs(tmp_path)
    rc = cli.main(
        [
            "--events",
            str(events),
            "--prices",
            str(prices),
            "--benchmarks",
            str(bench),
            "--output",
            str(out),
        ]
    )
    assert rc == 0
    assert out.exists()
    df = pd.read_csv(out)
    expected_cols = {
        "event_id",
        "market",
        "event_ticker",
        "relative_day",
        "ar",
        "event_phase",
    }
    assert expected_cols.issubset(df.columns)
    assert len(df) > 0
