"""Tests for ``index_inclusion_research.compute_h6_weight_change``.

Network calls (akshare / yfinance) are NOT exercised — production
fetchers are abstracted behind ``MarketCapFetcher`` so all tests inject
synthetic closures.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.compute_h6_weight_change import (
    attach_market_caps,
    compute_weight_change_table,
    compute_weight_proxy,
    export_weight_change_table,
    main,
)


def _candidates_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"batch_id": "B1", "market": "CN", "ticker": "000001", "announce_date": "2022-05-27", "effective_date": "2022-06-10", "running_variable": 600, "cutoff": 300, "inclusion": 1},
            {"batch_id": "B1", "market": "CN", "ticker": "000002", "announce_date": "2022-05-27", "effective_date": "2022-06-10", "running_variable": 599, "cutoff": 300, "inclusion": 1},
            {"batch_id": "B1", "market": "CN", "ticker": "000003", "announce_date": "2022-05-27", "effective_date": "2022-06-10", "running_variable": 598, "cutoff": 300, "inclusion": 1},
            {"batch_id": "B1", "market": "CN", "ticker": "999999", "announce_date": "2022-05-27", "effective_date": "2022-06-10", "running_variable": 100, "cutoff": 300, "inclusion": 0},
        ]
    )


def _fixture_fetcher(mapping: dict[tuple[str, str], float]):
    def _fetch(market: str, ticker: str, _date: str) -> float | None:
        return mapping.get((market, ticker))
    return _fetch


# ── attach_market_caps ───────────────────────────────────────────────


def test_attach_market_caps_calls_fetcher_for_inclusion_only() -> None:
    cands = _candidates_fixture()
    calls: list[tuple[str, str, str]] = []

    def fetcher(market: str, ticker: str, date: str) -> float | None:
        calls.append((market, ticker, date))
        return 1.0

    out = attach_market_caps(cands, fetcher=fetcher)
    # 3 inclusion=1 rows fetched, 1 inclusion=0 skipped
    assert len(calls) == 3
    assert out.loc[out["ticker"] == "999999", "mkt_cap_proxy"].iloc[0] is None or pd.isna(
        out.loc[out["ticker"] == "999999", "mkt_cap_proxy"].iloc[0]
    )


def test_attach_market_caps_handles_failed_fetcher() -> None:
    cands = _candidates_fixture()

    def fetcher(*_args, **_kwargs):
        raise OSError("synthetic network failure")

    out = attach_market_caps(cands, fetcher=fetcher)
    assert out["mkt_cap_proxy"].isna().all()


def test_attach_market_caps_drops_non_positive_values() -> None:
    cands = _candidates_fixture()
    fetcher = _fixture_fetcher({
        ("CN", "000001"): 100.0,
        ("CN", "000002"): -5.0,
        ("CN", "000003"): 0.0,
    })
    out = attach_market_caps(cands, fetcher=fetcher)
    valid = out.loc[out["mkt_cap_proxy"].notna() & (out["mkt_cap_proxy"] > 0)]
    assert len(valid) == 1
    assert valid["ticker"].iloc[0] == "000001"


# ── compute_weight_proxy ─────────────────────────────────────────────


def test_compute_weight_proxy_normalises_within_batch() -> None:
    cands = _candidates_fixture().assign(mkt_cap_proxy=[60.0, 30.0, 10.0, None])
    out = compute_weight_proxy(cands)
    assert len(out) == 3
    assert abs(out["weight_proxy"].sum() - 1.0) < 1e-9
    by_ticker = out.set_index("ticker")
    assert abs(by_ticker.loc["000001", "weight_proxy"] - 0.6) < 1e-9
    assert abs(by_ticker.loc["000002", "weight_proxy"] - 0.3) < 1e-9
    assert abs(by_ticker.loc["000003", "weight_proxy"] - 0.1) < 1e-9


def test_compute_weight_proxy_skips_inclusion_zero_and_nan() -> None:
    cands = _candidates_fixture().assign(mkt_cap_proxy=[60.0, 30.0, None, 5.0])
    out = compute_weight_proxy(cands)
    # Only inclusion=1 rows with non-NaN cap survive
    assert len(out) == 2
    assert "999999" not in out["ticker"].values
    assert "000003" not in out["ticker"].values


def test_compute_weight_proxy_empty_input() -> None:
    out = compute_weight_proxy(pd.DataFrame())
    assert out.empty
    assert "weight_proxy" in out.columns


# ── compute_weight_change_table (full pipeline) ──────────────────────


def test_compute_weight_change_table_end_to_end() -> None:
    cands = _candidates_fixture()
    fetcher = _fixture_fetcher({
        ("CN", "000001"): 60.0,
        ("CN", "000002"): 30.0,
        ("CN", "000003"): 10.0,
    })
    out = compute_weight_change_table(cands, fetcher=fetcher)
    assert len(out) == 3
    assert {"weight_proxy", "batch_total_mkt_cap", "mkt_cap_proxy"}.issubset(out.columns)
    assert abs(out["weight_proxy"].sum() - 1.0) < 1e-9


# ── export ───────────────────────────────────────────────────────────


def test_export_weight_change_table_writes_csv(tmp_path: Path) -> None:
    cands = _candidates_fixture()
    fetcher = _fixture_fetcher({
        ("CN", "000001"): 60.0,
        ("CN", "000002"): 30.0,
        ("CN", "000003"): 10.0,
    })
    frame = compute_weight_change_table(cands, fetcher=fetcher)
    out = tmp_path / "hs300_weight_change.csv"
    written = export_weight_change_table(frame, output_path=out)
    assert written == out
    df = pd.read_csv(out)
    assert len(df) == 3


# ── CLI ──────────────────────────────────────────────────────────────


def test_main_returns_1_when_input_missing(tmp_path: Path) -> None:
    rc = main([
        "--input", str(tmp_path / "missing.csv"),
        "--output", str(tmp_path / "out.csv"),
    ])
    assert rc == 1


def test_main_refuses_overwrite_without_force(tmp_path: Path) -> None:
    input_path = tmp_path / "candidates.csv"
    _candidates_fixture().to_csv(input_path, index=False)
    output_path = tmp_path / "out.csv"
    output_path.write_text("existing\n")
    rc = main(["--input", str(input_path), "--output", str(output_path)])
    assert rc == 1
    assert "existing" in output_path.read_text()
