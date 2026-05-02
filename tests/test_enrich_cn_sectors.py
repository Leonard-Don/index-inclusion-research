from __future__ import annotations

import pandas as pd

from index_inclusion_research.enrich_cn_sectors import (
    enrich_sector_frames,
    fill_missing_cn_sectors,
)


def test_fill_missing_cn_sectors_only_updates_cn_missing_values() -> None:
    frame = pd.DataFrame(
        [
            {"market": "CN", "ticker": "1", "sector": None},
            {"market": "CN", "ticker": "000002", "sector": "Industrials"},
            {"market": "US", "ticker": "ABC", "sector": None},
        ]
    )

    enriched, count = fill_missing_cn_sectors(frame, {"000001": "Financial Services"})

    assert count == 1
    assert enriched.loc[0, "ticker"] == "000001"
    assert enriched.loc[0, "sector"] == "Financial Services"
    assert enriched.loc[1, "sector"] == "Industrials"
    assert pd.isna(enriched.loc[2, "sector"])


def test_enrich_sector_frames_reuses_one_fetched_map_for_events_and_metadata() -> None:
    events = pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "sector": None},
            {"market": "US", "ticker": "ABC", "sector": "Technology"},
        ]
    )
    metadata = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "1",
                "yahoo_symbol": "000001.SZ",
                "sector": "Unknown",
            },
        ]
    )
    calls: list[tuple[str, str]] = []

    def fetcher(ticker: str, yahoo_symbol: str) -> str | None:
        calls.append((ticker, yahoo_symbol))
        return {"000001": "Financial Services"}.get(ticker)

    enriched_events, enriched_metadata, summary = enrich_sector_frames(
        events, metadata, fetcher=fetcher, sleep_seconds=0
    )

    assert calls == [("000001", "000001.SZ")]
    assert enriched_events.loc[0, "sector"] == "Financial Services"
    assert enriched_metadata.loc[0, "sector"] == "Financial Services"
    assert summary["events_filled"] == 1
    assert summary["metadata_filled"] == 1
    assert summary["fetched_sectors"] == 1


def test_enrich_sector_frames_preserves_existing_cn_sector_without_fetch() -> None:
    events = pd.DataFrame(
        [{"market": "CN", "ticker": "000001", "sector": "Financial Services"}]
    )
    metadata = pd.DataFrame(
        [{"market": "CN", "ticker": "000001", "sector": None}]
    )

    def fetcher(_ticker: str, _yahoo_symbol: str) -> str | None:
        raise AssertionError("existing event sector should be reused")

    enriched_events, enriched_metadata, summary = enrich_sector_frames(
        events, metadata, fetcher=fetcher, sleep_seconds=0
    )

    assert enriched_events.loc[0, "sector"] == "Financial Services"
    assert enriched_metadata.loc[0, "sector"] == "Financial Services"
    assert summary["existing_sectors"] == 1
    assert summary["fetched_sectors"] == 0
