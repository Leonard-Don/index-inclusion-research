from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.match_robustness import (
    build_match_robustness_grid,
    main,
    select_control_ratio,
)


def _matched_events_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for group, treated_ticker, control_tickers in (
        ("event-a", "000001", ("000002", "000003", "000004")),
        ("event-b", "000005", ("000006", "000007", "000008")),
    ):
        rows.append(
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": treated_ticker,
                "announce_date": "2024-02-01",
                "effective_date": "2024-02-05",
                "batch_id": "fixture",
                "security_name": treated_ticker,
                "event_type": "addition",
                "inclusion": 1,
                "source": "fixture",
                "source_url": "",
                "note": "",
                "sector": "Tech",
                "treatment_group": 1,
                "matched_to_event_id": group,
                "event_id": group,
            }
        )
        for rank, ticker in enumerate(control_tickers, start=1):
            rows.append(
                {
                    "market": "CN",
                    "index_name": "CSI300",
                    "ticker": ticker,
                    "announce_date": "2024-02-01",
                    "effective_date": "2024-02-05",
                    "batch_id": "fixture",
                    "security_name": ticker,
                    "event_type": "addition",
                    "inclusion": 1,
                    "source": "fixture",
                    "source_url": "",
                    "note": "",
                    "sector": "Tech",
                    "treatment_group": 0,
                    "matched_to_event_id": group,
                    "event_id": f"{group}-ctrl-{rank:02d}",
                }
            )
    frame = pd.DataFrame(rows)
    frame["announce_date"] = pd.to_datetime(frame["announce_date"])
    frame["effective_date"] = pd.to_datetime(frame["effective_date"])
    return frame


def _prices_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ticker_num in range(1, 9):
        ticker = f"{ticker_num:06d}"
        for day_idx, date in enumerate(pd.date_range("2024-01-01", "2024-02-10")):
            ret = ((day_idx % 5) - 2) * 0.0005 * (1 + ticker_num / 10)
            rows.append(
                {
                    "market": "CN",
                    "ticker": ticker,
                    "date": date,
                    "close": 10 + ticker_num + day_idx * 0.01,
                    "ret": ret,
                    "volume": 1000 + ticker_num,
                    "turnover": 10000 + ticker_num,
                    "mkt_cap": 1_000_000 + ticker_num * 50_000 + day_idx * 100,
                    "sector": "Tech",
                }
            )
    return pd.DataFrame(rows)


def test_select_control_ratio_keeps_first_k_controls() -> None:
    matched = _matched_events_fixture()

    selected = select_control_ratio(matched, 2)

    assert len(selected) == 6
    assert selected["treatment_group"].sum() == 2
    assert not selected["event_id"].astype(str).str.endswith("-ctrl-03").any()


def test_build_match_robustness_grid_compares_reference_dates_and_ratios() -> None:
    grid, balance = build_match_robustness_grid(
        matched_events=_matched_events_fixture(),
        prices=_prices_fixture(),
        control_ratios=[1, 2],
        reference_date_columns=["announce_date", "effective_date"],
        default_control_ratio=2,
    )

    assert set(grid["spec_id"]) == {
        "announce_1to1",
        "announce_1to2",
        "effective_1to1",
        "effective_1to2",
    }
    assert bool(grid.loc[grid["spec_id"] == "announce_1to2", "is_default"].iloc[0])
    assert set(balance["spec_id"]) == set(grid["spec_id"])
    assert {"market", "covariate", "smd"}.issubset(balance.columns)


def test_match_robustness_cli_writes_grid_balance_and_summary(tmp_path: Path) -> None:
    matched_events_path = tmp_path / "matched_events.csv"
    prices_path = tmp_path / "prices.csv"
    grid_path = tmp_path / "grid.csv"
    balance_path = tmp_path / "balance.csv"
    summary_path = tmp_path / "summary.md"
    _matched_events_fixture().to_csv(matched_events_path, index=False)
    _prices_fixture().to_csv(prices_path, index=False)

    rc = main(
        [
            "--matched-events",
            str(matched_events_path),
            "--prices",
            str(prices_path),
            "--output-grid",
            str(grid_path),
            "--output-balance",
            str(balance_path),
            "--output-summary",
            str(summary_path),
            "--control-ratios",
            "1",
            "3",
            "--reference-date-columns",
            "announce_date",
            "effective_date",
        ]
    )

    assert rc == 0
    assert len(pd.read_csv(grid_path)) == 4
    assert not pd.read_csv(balance_path).empty
    assert "does not download or scrape web data" in summary_path.read_text()
