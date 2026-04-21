from __future__ import annotations

from index_inclusion_research.dashboard_cache import AnalysisCacheStore


def test_analysis_cache_store_snapshot_and_replace_all_are_isolated() -> None:
    store = AnalysisCacheStore(
        {
            "price_pressure_track": {"id": "live", "summary_text": "old"},
        }
    )

    snapshot = store.snapshot()
    snapshot["price_pressure_track"] = {"id": "snapshot", "summary_text": "new"}
    store["paper_framework"] = {"id": "paper_framework"}

    assert store["price_pressure_track"]["id"] == "live"
    assert store["paper_framework"]["id"] == "paper_framework"

    store.replace_all(snapshot)

    assert dict(store) == {
        "price_pressure_track": {"id": "snapshot", "summary_text": "new"},
    }
