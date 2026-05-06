"""Tests for ``index_inclusion_research.download_passive_aum_cn``.

The module hits AKShare endpoints at runtime; tests use small synthetic
DataFrames passed to the pure helper functions and avoid network I/O.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research.download_passive_aum_cn import (
    build_cn_yearly_passive_aum,
    fetch_index_fund_share,
    fetch_total_aum_quarterly,
    merge_into_csv,
)


def _quarterly(rows: list[tuple[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "value_rmb"]).assign(
        date=lambda d: pd.to_datetime(d["date"])
    )


def test_build_cn_yearly_passive_aum_collapses_to_year_end_with_share_scaling() -> None:
    quarterly = _quarterly(
        [
            ("2021-06-30", 2.0e13),
            ("2021-12-31", 2.5e13),
            ("2022-06-30", 2.6e13),
            ("2022-12-31", 2.8e13),
        ]
    )
    out = build_cn_yearly_passive_aum(quarterly, index_share=0.10)
    assert list(out.columns) == ["market", "year", "aum_trillion"]
    assert (out["market"] == "CN").all()
    # 2021 picks 12-31 (2.5e13 * 0.10 / 1e12 = 2.5)
    # 2022 picks 12-31 (2.8e13 * 0.10 / 1e12 = 2.8)
    assert out.loc[out["year"] == 2021, "aum_trillion"].iloc[0] == pytest.approx(2.5)
    assert out.loc[out["year"] == 2022, "aum_trillion"].iloc[0] == pytest.approx(2.8)


def test_build_cn_yearly_falls_back_to_latest_when_no_year_end() -> None:
    quarterly = _quarterly(
        [
            ("2021-06-30", 2.0e13),
            ("2021-09-30", 2.2e13),  # no Q4 row
        ]
    )
    out = build_cn_yearly_passive_aum(quarterly, index_share=0.10)
    assert len(out) == 1
    assert out["year"].iloc[0] == 2021
    # 2.2e13 RMB * 0.10 share / 1e12 = 2.2 RMB trillion
    assert out["aum_trillion"].iloc[0] == pytest.approx(2.2)


def test_merge_into_csv_replaces_existing_cn_by_default(tmp_path: Path) -> None:
    csv = tmp_path / "passive_aum.csv"
    pd.DataFrame(
        [
            {"market": "US", "year": 2020, "aum_trillion": 5.0},
            {"market": "US", "year": 2021, "aum_trillion": 7.0},
            {"market": "CN", "year": 2020, "aum_trillion": 1.0},
        ]
    ).to_csv(csv, index=False)

    new_cn = pd.DataFrame(
        [
            {"market": "CN", "year": 2021, "aum_trillion": 2.5},
            {"market": "CN", "year": 2022, "aum_trillion": 3.0},
        ]
    )
    merged = merge_into_csv(new_cn, csv, overwrite_cn=True)
    cn_rows = merged.loc[merged["market"] == "CN"]
    assert list(cn_rows["year"]) == [2021, 2022]
    # US untouched
    us_rows = merged.loc[merged["market"] == "US"]
    assert list(us_rows["year"]) == [2020, 2021]


def test_merge_into_csv_keeps_existing_cn_when_requested(tmp_path: Path) -> None:
    csv = tmp_path / "passive_aum.csv"
    pd.DataFrame(
        [{"market": "CN", "year": 2020, "aum_trillion": 1.0}]
    ).to_csv(csv, index=False)
    new_cn = pd.DataFrame([{"market": "CN", "year": 2021, "aum_trillion": 2.0}])
    merged = merge_into_csv(new_cn, csv, overwrite_cn=False)
    assert sorted(merged["year"].tolist()) == [2020, 2021]


def _install_fake_akshare(monkeypatch: pytest.MonkeyPatch, **endpoints: object) -> None:
    """Stub ``import akshare`` for the duration of one test."""
    import sys
    import types

    fake = types.SimpleNamespace(**endpoints)
    monkeypatch.setitem(sys.modules, "akshare", fake)


def test_fetch_index_fund_share_uses_akshare(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_breakdown = pd.DataFrame(
        {
            "总规模": [150_000.0, 50_000.0],  # 200_000 亿 RMB ≈ 20 万亿 (above threshold)
            "指数型": [20_000.0, 5_000.0],  # 25_000 / 200_000 = 0.125
        }
    )
    _install_fake_akshare(monkeypatch, fund_aum_hist_em=lambda: fake_breakdown)
    share = fetch_index_fund_share()
    assert share == pytest.approx(25_000.0 / 200_000.0, rel=1e-6)


def test_fetch_index_fund_share_rejects_truncated_total(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pd.DataFrame({"总规模": [10.0], "指数型": [5.0]})
    _install_fake_akshare(monkeypatch, fund_aum_hist_em=lambda: fake)
    with pytest.raises(RuntimeError, match="below sanity threshold"):
        fetch_index_fund_share()


def test_fetch_total_aum_quarterly_normalizes_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pd.DataFrame(
        {
            "date": ["2021-06-30", "2021-09-30", "bad"],
            "value": [2.0e13, 2.2e13, 2.4e13],
        }
    )
    _install_fake_akshare(monkeypatch, fund_aum_trend_em=lambda: fake)
    out = fetch_total_aum_quarterly()
    # bad date dropped, 2 rows remain, sorted ascending
    assert list(out.columns) == ["date", "value_rmb"]
    assert len(out) == 2
    assert out["date"].is_monotonic_increasing
