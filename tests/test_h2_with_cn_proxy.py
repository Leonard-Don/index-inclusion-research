"""Behavioural tests for H2 once the CN ETF-TNA proxy is plumbed in.

These tests cover:

* The orchestrator helper that merges the proxy into the AUM frame.
* The ``_h2`` verdict logic when both US and CN series are present.
* The combined-n driven evidence_tier promotion (``supplementary`` →
  ``core``) once the threshold is crossed.

We avoid hitting akshare; the tests synthesize tiny in-memory frames
and exercise the pure-Python code paths.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.orchestrator import (
    _cn_proxy_to_aum_rows,
    _merge_aum_with_cn_proxy,
)
from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
    build_hypothesis_verdicts,
)


def _proxy_rows() -> pd.DataFrame:
    """Two-index, three-snapshot synthetic CN proxy table."""
    return pd.DataFrame(
        [
            {
                "index_name": "CSI300",
                "snapshot_date": "2020-12-31",
                "total_tna_cny_billions": 100.0,
                "etf_count": 10,
                "source": "akshare:test",
                "note": "Proxy synthetic",
            },
            {
                "index_name": "CSI300",
                "snapshot_date": "2022-12-31",
                "total_tna_cny_billions": 200.0,
                "etf_count": 12,
                "source": "akshare:test",
                "note": "Proxy synthetic",
            },
            {
                "index_name": "CSI300",
                "snapshot_date": "2024-12-31",
                "total_tna_cny_billions": 800.0,
                "etf_count": 15,
                "source": "akshare:test",
                "note": "Proxy synthetic",
            },
            {
                "index_name": "CSI500",
                "snapshot_date": "2020-12-31",
                "total_tna_cny_billions": 50.0,
                "etf_count": 5,
                "source": "akshare:test",
                "note": "Proxy synthetic",
            },
            {
                "index_name": "CSI500",
                "snapshot_date": "2022-12-31",
                "total_tna_cny_billions": 80.0,
                "etf_count": 7,
                "source": "akshare:test",
                "note": "Proxy synthetic",
            },
            {
                "index_name": "CSI500",
                "snapshot_date": "2024-12-31",
                "total_tna_cny_billions": 150.0,
                "etf_count": 9,
                "source": "akshare:test",
                "note": "Proxy synthetic",
            },
        ]
    )


def _us_only_aum() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"market": "US", "year": 2014, "aum_trillion": 2.0},
            {"market": "US", "year": 2020, "aum_trillion": 5.0},
            {"market": "US", "year": 2024, "aum_trillion": 10.0},
        ]
    )


def _rolling_both_markets() -> pd.DataFrame:
    """Rolling CAR with multiple years for both markets, enough to satisfy
    H2's ≥2 obs per market check and to cross the combined-n promotion floor.
    """
    rows: list[dict[str, object]] = []
    # US: 12 years 2014..2025, effective CAR drifts down then back up
    for i, year in enumerate(range(2014, 2026)):
        rows.append(
            {
                "market": "US",
                "event_phase": "effective",
                "window_end_year": year,
                "car_mean": 0.004 - 0.0002 * i,
            }
        )
    # CN: 5 years 2021..2025, effective CAR drifts down (H2 pattern)
    for i, year in enumerate(range(2021, 2026)):
        rows.append(
            {
                "market": "CN",
                "event_phase": "effective",
                "window_end_year": year,
                "car_mean": 0.006 - 0.0003 * i,
            }
        )
    return pd.DataFrame(rows)


def test_cn_proxy_to_aum_rows_aggregates_by_year() -> None:
    proxy = _proxy_rows()
    aum = _cn_proxy_to_aum_rows(proxy)
    assert list(aum.columns) == ["market", "year", "aum_trillion"]
    assert set(aum["year"]) == {2020, 2022, 2024}
    # 2024: (800 + 150) billion = 950 billion = 0.950 trillion
    aum_2024 = float(aum.loc[aum["year"] == 2024, "aum_trillion"].iloc[0])
    assert aum_2024 == pytest.approx(0.950)
    # 2020: (100 + 50) billion = 0.150 trillion
    aum_2020 = float(aum.loc[aum["year"] == 2020, "aum_trillion"].iloc[0])
    assert aum_2020 == pytest.approx(0.150)
    assert (aum["market"] == "CN").all()


def test_cn_proxy_to_aum_rows_handles_empty_frame() -> None:
    empty = pd.DataFrame(columns=["index_name", "snapshot_date", "total_tna_cny_billions"])
    aum = _cn_proxy_to_aum_rows(empty)
    assert aum.empty
    assert list(aum.columns) == ["market", "year", "aum_trillion"]


def test_merge_aum_with_cn_proxy_replaces_existing_cn(tmp_path: Path) -> None:
    proxy_path = tmp_path / "cn_passive_aum_proxy.csv"
    _proxy_rows().to_csv(proxy_path, index=False)
    base = pd.DataFrame(
        [
            {"market": "CN", "year": 2021, "aum_trillion": 2.85},  # old top-down value
            {"market": "CN", "year": 2024, "aum_trillion": 3.58},
            {"market": "US", "year": 2024, "aum_trillion": 10.0},
        ]
    )
    merged = _merge_aum_with_cn_proxy(base, proxy_path)
    cn_rows = merged.loc[merged["market"] == "CN"]
    # Old CN rows (2021, 2024 from top-down) replaced by proxy rows (2020, 2022, 2024)
    assert set(cn_rows["year"]) == {2020, 2022, 2024}
    # US row untouched
    us_rows = merged.loc[merged["market"] == "US"]
    assert len(us_rows) == 1
    assert float(us_rows["aum_trillion"].iloc[0]) == pytest.approx(10.0)


def test_merge_aum_with_cn_proxy_returns_unchanged_when_proxy_missing(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "does_not_exist.csv"
    base = pd.DataFrame(
        [
            {"market": "CN", "year": 2021, "aum_trillion": 2.85},
            {"market": "US", "year": 2024, "aum_trillion": 10.0},
        ]
    )
    out = _merge_aum_with_cn_proxy(base, missing_path)
    pd.testing.assert_frame_equal(out, base)


def test_h2_with_both_markets_loads_without_crashing() -> None:
    """H2 must run when both US and CN series have ≥2 observations."""
    aum_us = _us_only_aum()
    aum_cn = pd.DataFrame(
        [
            {"market": "CN", "year": 2020, "aum_trillion": 0.15},
            {"market": "CN", "year": 2024, "aum_trillion": 0.95},
        ]
    )
    aum = pd.concat([aum_us, aum_cn], ignore_index=True)
    rolling = _rolling_both_markets()

    verdicts = build_hypothesis_verdicts(
        gap_summary=pd.DataFrame(),
        mechanism_panel=pd.DataFrame(),
        heterogeneity_size=pd.DataFrame(),
        time_series_rolling=rolling,
        aum_frame=aum,
    )
    h2 = verdicts.set_index("hid").loc["H2"]
    # The verdict shouldn't be "待补数据" anymore — both markets present.
    assert h2["verdict"] != "待补数据"
    # Metric snapshot must reference both markets so a reviewer can audit.
    assert "US AUM" in h2["metric_snapshot"]
    assert "CN AUM" in h2["metric_snapshot"]


def test_h2_combined_n_promotes_evidence_tier_to_core() -> None:
    """With both markets present, combined n crosses the promotion floor
    and H2 should be re-tiered to ``core``."""
    aum_us = _us_only_aum()
    aum_cn = pd.DataFrame(
        [
            {"market": "CN", "year": 2020, "aum_trillion": 0.15},
            {"market": "CN", "year": 2024, "aum_trillion": 0.95},
        ]
    )
    aum = pd.concat([aum_us, aum_cn], ignore_index=True)
    rolling = _rolling_both_markets()

    verdicts = build_hypothesis_verdicts(
        gap_summary=pd.DataFrame(),
        mechanism_panel=pd.DataFrame(),
        heterogeneity_size=pd.DataFrame(),
        time_series_rolling=rolling,
        aum_frame=aum,
    )
    h2 = verdicts.set_index("hid").loc["H2"]
    # 12 US rolling years + 5 CN rolling years = 17 ≥ 15 floor → promoted.
    assert int(h2["n_obs"]) == 17
    assert h2["evidence_tier"] == "core", (
        f"expected H2 promoted to core when combined-n >= 15, got tier={h2['evidence_tier']}"
    )


def test_h2_stays_supplementary_when_only_us_series_present() -> None:
    """Without CN data we fall back to single-market n=12 < 15 → stays supplementary."""
    aum = _us_only_aum()
    # Strip CN rolling rows
    rolling = _rolling_both_markets()
    rolling_us_only = rolling.loc[rolling["market"] == "US"].copy()

    verdicts = build_hypothesis_verdicts(
        gap_summary=pd.DataFrame(),
        mechanism_panel=pd.DataFrame(),
        heterogeneity_size=pd.DataFrame(),
        time_series_rolling=rolling_us_only,
        aum_frame=aum,
    )
    h2 = verdicts.set_index("hid").loc["H2"]
    # 12 US rolling years only, no CN → 12 < 15 → stays supplementary.
    assert int(h2["n_obs"]) == 12
    assert h2["evidence_tier"] == "supplementary"


def test_h2_verdict_csv_reflects_pipeline_output() -> None:
    """Smoke test: the on-disk verdict CSV should agree with the new H2 logic
    after the orchestrator has been run (asserts the value rather than
    its specific text, so future legitimate verdict changes don't break
    this guardrail)."""
    from index_inclusion_research import paths

    verdict_path = paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"
    if not verdict_path.exists():
        pytest.skip("verdict CSV not present; run `index-inclusion-cma` first.")
    frame = pd.read_csv(verdict_path)
    h2_rows = frame.loc[frame["hid"] == "H2"]
    assert not h2_rows.empty
    h2 = h2_rows.iloc[0]
    # After plumbing the proxy in: H2 should reference both markets in
    # the metric snapshot, and the verdict should not be "待补数据".
    assert h2["verdict"] != "待补数据"
    assert "CN AUM" in str(h2["metric_snapshot"])
    # combined-n should be > 12 (the old US-only n) since CN rolling
    # observations are now included.
    assert int(h2["n_obs"]) > 12
