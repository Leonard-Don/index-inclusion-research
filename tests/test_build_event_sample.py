from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import build_event_sample as cli


def _write_min_events(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN01",
                "announce_date": "2024-02-03",
                "effective_date": "2024-02-12",
                "event_type": "inclusion",
                "sector": "Industrials",
                "source": "test",
                "note": "",
            },
            {
                "market": "US",
                "index_name": "SP500",
                "ticker": "US01",
                "announce_date": "2024-02-10",
                "effective_date": "2024-02-20",
                "event_type": "inclusion",
                "sector": "Industrials",
                "source": "test",
                "note": "",
            },
        ]
    ).to_csv(path, index=False)


def test_main_writes_cleaned_events_csv(tmp_path: Path) -> None:
    in_path = tmp_path / "events.csv"
    out_path = tmp_path / "events_clean.csv"
    _write_min_events(in_path)
    rc = cli.main(
        [
            "--profile",
            "sample",
            "--input",
            str(in_path),
            "--output",
            str(out_path),
        ]
    )
    assert rc == 0
    assert out_path.exists()
    df = pd.read_csv(out_path)
    assert {"market", "ticker", "announce_date", "effective_date"}.issubset(df.columns)
    assert len(df) == 2


def test_main_assigns_unique_event_ids(tmp_path: Path) -> None:
    in_path = tmp_path / "events.csv"
    out_path = tmp_path / "events_clean.csv"
    _write_min_events(in_path)
    rc = cli.main(["--input", str(in_path), "--output", str(out_path)])
    assert rc == 0
    df = pd.read_csv(out_path)
    assert "event_id" in df.columns
    assert df["event_id"].is_unique
