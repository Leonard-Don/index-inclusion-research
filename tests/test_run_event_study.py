from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import run_event_study as cli
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.pipeline import build_event_panel


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


def test_main_handles_market_model_panel_with_no_matched_events(tmp_path: Path) -> None:
    """End-to-end CLI guard: ``run-event-study`` must complete cleanly when its
    input panel was emitted by ``build-price-panel --include-market-model-ar``
    against an event set that produced no matched price rows.

    Why: ``build_event_panel(..., include_market_model_ar=True)`` now anchors
    the full standard-plus-market-model schema on its empty path (commits
    ``cf3d29c``/``26c6344``/``d87e79b``). The pipeline-level guard already
    locks that schema in memory, but the CLI hop —
    ``save_dataframe(panel, ...)`` followed by
    ``pd.read_csv(panel, parse_dates=[...])`` inside
    ``run_event_study.main`` — is what an actual research run hits and is the
    path that previously raised
    ``ValueError: Missing column provided to 'parse_dates'`` when the empty
    panel collapsed its columns. This test exercises the CSV roundtrip and
    pins down ``rc == 0`` plus the expected output files, so any future
    regression that drops the date columns or the market-model columns at the
    pipeline boundary fails here instead of in a downstream pipeline run.
    """
    events = pd.DataFrame(
        [
            {
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "CN_NO_PRICES",
                "announce_date": pd.Timestamp("2024-02-09"),
                "effective_date": pd.Timestamp("2024-02-09"),
                "event_type": "addition",
                "sector": "Technology",
                "inclusion": 1,
                "treatment_group": 1,
                "event_id": "missing",
            }
        ]
    )
    prices = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "CN_OTHER",
                "date": pd.Timestamp("2024-02-09"),
                "close": 100.0,
                "ret": 0.001,
                "volume": 1_000_000,
                "turnover": 0.01,
                "mkt_cap": 1e9,
                "sector": "Technology",
            }
        ]
    )
    benchmarks = pd.DataFrame(
        [{"market": "CN", "date": pd.Timestamp("2024-02-09"), "benchmark_ret": 0.0005}]
    )

    panel = build_event_panel(
        events,
        prices,
        benchmarks,
        window_pre=2,
        window_post=2,
        include_market_model_ar=True,
    )
    assert len(panel) == 0

    panel_path = tmp_path / "empty_market_model_panel.csv"
    out_dir = tmp_path / "out"
    save_dataframe(panel, panel_path)

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

    reloaded = pd.read_csv(panel_path)
    expected_columns = {
        "event_date_raw",
        "mapped_market_date",
        "event_date",
        "date",
        "ar_market_model",
        "market_model_alpha",
        "market_model_beta",
        "market_model_estimation_obs",
    }
    assert expected_columns.issubset(reloaded.columns), (
        "saved empty panel must preserve date + market-model columns; "
        f"missing: {expected_columns - set(reloaded.columns)}"
    )
