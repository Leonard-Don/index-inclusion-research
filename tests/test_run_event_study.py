from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import run_event_study as cli


def _write_panel(path: Path) -> None:
    """Build a tiny event panel that compute_event_study can chew on."""
    rows = []
    for event_id, market, ticker, index_name in [
        (1, "CN", "CN01", "CSI300"),
        (2, "US", "US01", "SP500"),
    ]:
        for phase in ("announce", "effective"):
            for rel in range(-5, 6):
                rows.append(
                    {
                        "event_id": event_id,
                        "matched_to_event_id": event_id,
                        "market": market,
                        "index_name": index_name,
                        "event_ticker": ticker,
                        "ticker": ticker,
                        "security_name": ticker,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.005 if rel == 0 else 0.001,
                        "ret": 0.001,
                        "benchmark_ret": 0.0005,
                        "event_date": "2024-02-05",
                        "event_date_raw": "2024-02-05",
                        "mapped_market_date": "2024-02-05",
                        "date": (pd.Timestamp("2024-02-05") + pd.Timedelta(days=rel)).date().isoformat(),
                        "treatment_group": 1,
                        "inclusion": 1,
                        "sector": "Industrials",
                        "mkt_cap": 1e9,
                        "turnover": 0.02,
                        "volume": 1e6,
                        "close": 100.0,
                    }
                )
    pd.DataFrame(rows).to_csv(path, index=False)


def test_main_writes_event_study_outputs(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.csv"
    out_dir = tmp_path / "out"
    _write_panel(panel_path)
    rc = cli.main(
        [
            "--panel",
            str(panel_path),
            "--output-dir",
            str(out_dir),
        ]
    )
    assert rc == 0
    for name in ("event_level_metrics.csv", "event_study_summary.csv", "average_paths.csv"):
        assert (out_dir / name).exists(), f"missing output {name}"
    summary = pd.read_csv(out_dir / "event_study_summary.csv")
    assert not summary.empty
