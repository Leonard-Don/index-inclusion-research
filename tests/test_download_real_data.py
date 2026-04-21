from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

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
