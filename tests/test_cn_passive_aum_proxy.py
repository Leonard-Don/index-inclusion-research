"""Schema / provenance tests for ``data/raw/cn_passive_aum_proxy.csv``.

These tests are intentionally schema-focused: they verify that the file
on disk has the structure H2 expects, without requiring akshare to be
reachable at test time (the CSV is checked in).

If the CSV does not exist, the entire module is skipped — useful for
fresh clones that haven't yet run the downloader.
"""

from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research import paths

PROXY_PATH = paths.project_root() / "data" / "raw" / "cn_passive_aum_proxy.csv"

REQUIRED_COLUMNS: tuple[str, ...] = (
    "index_name",
    "snapshot_date",
    "total_tna_cny_billions",
    "etf_count",
    "source",
    "note",
)

EXPECTED_INDICES: frozenset[str] = frozenset({"CSI300", "CSI500"})
MIN_SNAPSHOTS_PER_INDEX = 5


@pytest.fixture(scope="module")
def proxy_frame() -> pd.DataFrame:
    if not PROXY_PATH.exists():
        pytest.skip(f"{PROXY_PATH} not present; run download_cn_passive_aum_proxy first.")
    return pd.read_csv(PROXY_PATH)


def test_proxy_has_required_columns(proxy_frame: pd.DataFrame) -> None:
    assert set(REQUIRED_COLUMNS).issubset(proxy_frame.columns), (
        f"missing columns: expected {REQUIRED_COLUMNS}, got {list(proxy_frame.columns)}"
    )


def test_proxy_has_indices_covered(proxy_frame: pd.DataFrame) -> None:
    indices = set(proxy_frame["index_name"].astype(str))
    assert EXPECTED_INDICES.issubset(indices), (
        f"missing index_name values: expected ⊇ {EXPECTED_INDICES}, got {indices}"
    )


def test_proxy_has_min_snapshots_per_index(proxy_frame: pd.DataFrame) -> None:
    for index in EXPECTED_INDICES:
        sub = proxy_frame.loc[proxy_frame["index_name"] == index]
        assert len(sub) >= MIN_SNAPSHOTS_PER_INDEX, (
            f"{index} has only {len(sub)} snapshots; need ≥ {MIN_SNAPSHOTS_PER_INDEX}"
        )


def test_proxy_snapshot_dates_parse_and_are_unique_per_index(
    proxy_frame: pd.DataFrame,
) -> None:
    parsed = pd.to_datetime(proxy_frame["snapshot_date"], errors="coerce")
    assert parsed.notna().all(), "snapshot_date must be ISO-parseable"
    for index in EXPECTED_INDICES:
        sub = proxy_frame.loc[proxy_frame["index_name"] == index]
        dates = pd.to_datetime(sub["snapshot_date"])
        assert dates.is_unique, f"{index} has duplicate snapshot_date entries"


def test_proxy_snapshot_dates_monotonic_per_index(proxy_frame: pd.DataFrame) -> None:
    for index in EXPECTED_INDICES:
        sub = proxy_frame.loc[proxy_frame["index_name"] == index].copy()
        sub["snapshot_date"] = pd.to_datetime(sub["snapshot_date"])
        sub = sub.sort_index()  # preserve file order; check that file order is sorted
        assert sub["snapshot_date"].is_monotonic_increasing, (
            f"{index} snapshot_date should be monotonic increasing in file order"
        )


def test_proxy_tna_values_are_positive_floats(proxy_frame: pd.DataFrame) -> None:
    tna = pd.to_numeric(proxy_frame["total_tna_cny_billions"], errors="coerce")
    assert tna.notna().all(), "total_tna_cny_billions must be numeric"
    assert (tna > 0).all(), (
        "total_tna_cny_billions must be > 0; an empty snapshot suggests AKShare "
        "returned empty results — re-run download_cn_passive_aum_proxy."
    )


def test_proxy_etf_count_is_positive_int(proxy_frame: pd.DataFrame) -> None:
    counts = pd.to_numeric(proxy_frame["etf_count"], errors="coerce")
    assert counts.notna().all(), "etf_count must be numeric"
    assert (counts >= 1).all(), "every row must aggregate at least one ETF"
    # ints not floats with fractional parts
    assert (counts.astype(int) == counts).all(), "etf_count should be integer"


def test_proxy_source_column_non_null(proxy_frame: pd.DataFrame) -> None:
    sources = proxy_frame["source"].astype(str)
    assert sources.notna().all(), "source must be set for every row"
    assert (sources.str.len() > 0).all(), "source must be non-empty for every row"
    # All current rows must reference the akshare aggregation tag so we can
    # tell provenance apart if other sources are added later.
    assert (sources.str.startswith("akshare:")).all(), (
        "source rows currently expected to start with 'akshare:' "
        "(introduce a new prefix if you add a manual / alternate source)"
    )


def test_proxy_note_column_mentions_proxy_caveat(proxy_frame: pd.DataFrame) -> None:
    notes = proxy_frame["note"].astype(str)
    # Every row must carry the proxy-disclosure caveat so downstream
    # readers don't mistake it for direct AUM disclosure.
    assert notes.str.contains("Proxy").all() or notes.str.contains("proxy").all(), (
        "note column should mark every row as a proxy aggregate"
    )
