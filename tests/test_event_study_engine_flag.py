"""CLI engine-flag guard for ``index-inclusion-run-event-study``.

These tests pin down the additive ``--ar-model`` / ``--estimation-window``
flags on the event-study entry script. They use a synthetic fixture (no real
data) so the file stays fast and self-contained.

The most important guarantee is the **bit-for-bit identical** clause: the
default-flag run MUST produce the same CSV bytes as the no-flag run. The
assignment explicitly forbids changing the default behavior — the new flags
exist purely to expose existing market-model machinery, not to retune the
main pipeline. The byte-equality test catches accidental coupling regressions
(e.g. someone routing the ``adjusted`` engine through a slightly different
aggregation path).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research import run_event_study as cli

_EVENT_DATE = pd.Timestamp("2024-02-05")


def _build_synthetic_panel(
    *,
    pre_days: int = 130,
    post_days: int = 20,
    event_ids: tuple[tuple[int, str, str, str], ...] = (
        (1, "CN", "CN01", "CSI300"),
        (2, "US", "US01", "SP500"),
    ),
) -> pd.DataFrame:
    """Build a deterministic event panel rich enough for the (-120, -10) window.

    The benchmark return varies day-by-day (sin wave) so the market-model OLS
    sees non-degenerate variance; the event-day stock return is bumped up so
    CARs are visibly non-zero under either engine.
    """
    rng = np.random.default_rng(42)
    rows: list[dict[str, object]] = []
    for event_id, market, ticker, index_name in event_ids:
        for phase in ("announce", "effective"):
            for rel in range(-pre_days, post_days + 1):
                # Deterministic benchmark + idiosyncratic noise. Beta ~ 1.2.
                bench = 0.001 * np.sin(rel / 7.0)
                idio = float(rng.normal(loc=0.0, scale=0.002))
                event_bump = 0.01 if rel == 0 else 0.0
                ret = 1.2 * bench + idio + event_bump
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
                        "ar": ret - bench,
                        "ret": ret,
                        "benchmark_ret": bench,
                        "event_date": _EVENT_DATE.date().isoformat(),
                        "event_date_raw": _EVENT_DATE.date().isoformat(),
                        "mapped_market_date": _EVENT_DATE.date().isoformat(),
                        "date": (_EVENT_DATE + pd.Timedelta(days=rel)).date().isoformat(),
                        "treatment_group": 1,
                        "inclusion": 1,
                        "sector": "Industrials",
                        "mkt_cap": 1e9,
                        "turnover": 0.02,
                        "volume": 1e6,
                        "close": 100.0,
                    }
                )
    return pd.DataFrame(rows)


def _write_panel(path: Path, panel: pd.DataFrame) -> None:
    panel.to_csv(path, index=False)


def _read_event_study_outputs(out_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        name: pd.read_csv(out_dir / name)
        for name in (
            "event_level_metrics.csv",
            "event_study_summary.csv",
            "average_paths.csv",
            "patell_bmp_summary.csv",
        )
    }


def test_default_flag_run_is_bit_for_bit_identical_to_no_flag_run(tmp_path: Path) -> None:
    """Regression guard: ``--ar-model adjusted`` must equal no-flag invocation.

    Reads each output CSV byte-for-byte so any accidental aggregation reroute
    fails here. The new flag is strictly additive — the documented contract is
    that PAP / CMA baselines remain pinned to the simple market-adjusted AR.
    """
    panel_path = tmp_path / "panel.csv"
    out_no_flag = tmp_path / "out_no_flag"
    out_adjusted = tmp_path / "out_adjusted"
    _write_panel(panel_path, _build_synthetic_panel())

    assert cli.main(["--panel", str(panel_path), "--output-dir", str(out_no_flag)]) == 0
    assert (
        cli.main(
            [
                "--panel",
                str(panel_path),
                "--output-dir",
                str(out_adjusted),
                "--ar-model",
                "adjusted",
            ]
        )
        == 0
    )

    for name in (
        "event_level_metrics.csv",
        "event_study_summary.csv",
        "average_paths.csv",
        "patell_bmp_summary.csv",
    ):
        assert (out_no_flag / name).read_bytes() == (out_adjusted / name).read_bytes(), (
            f"output {name} must be bit-for-bit identical for adjusted/no-flag runs"
        )

    # In adjusted mode the skipped-events sidecar must NOT exist (market only).
    assert not (out_no_flag / "event_study_skipped_events.csv").exists()
    assert not (out_adjusted / "event_study_skipped_events.csv").exists()

    # Meta sidecar exists in both modes and records the engine.
    meta_no_flag = json.loads((out_no_flag / "event_study_meta.json").read_text())
    meta_adjusted = json.loads((out_adjusted / "event_study_meta.json").read_text())
    assert meta_no_flag["ar_model"] == "adjusted"
    assert meta_no_flag["ar_column"] == "ar"
    assert meta_no_flag["estimation_window"] is None
    assert meta_adjusted == meta_no_flag


def test_market_mode_populates_ar_market_model_when_estimation_window_is_sufficient(
    tmp_path: Path,
) -> None:
    """``--ar-model market`` writes non-NaN CARs and records the engine in meta."""
    panel_path = tmp_path / "panel.csv"
    out_dir = tmp_path / "out_market"
    _write_panel(panel_path, _build_synthetic_panel())

    rc = cli.main(
        [
            "--panel",
            str(panel_path),
            "--output-dir",
            str(out_dir),
            "--ar-model",
            "market",
        ]
    )
    assert rc == 0

    meta = json.loads((out_dir / "event_study_meta.json").read_text())
    assert meta["ar_model"] == "market"
    assert meta["ar_column"] == "ar_market_model"
    assert meta["estimation_window"] == [-120, -10]
    assert meta["n_events_skipped"] == 0

    event_level = pd.read_csv(out_dir / "event_level_metrics.csv")
    assert not event_level.empty
    car_cols = [col for col in event_level.columns if col.startswith("car_")]
    assert car_cols, "expected at least one car_<slug> column"
    # Every event/phase row should have at least one finite CAR — the
    # estimation window is comfortably populated for the synthetic fixture.
    finite_per_row = event_level[car_cols].notna().any(axis=1)
    assert finite_per_row.all(), (
        "all events should have finite market-model CARs with default window"
    )
    # And the realised CARs should differ from the simple-AR baseline so we
    # know the engine actually swapped (β ≠ 1 in the synthetic fixture).
    out_adjusted = tmp_path / "out_adjusted"
    cli.main(["--panel", str(panel_path), "--output-dir", str(out_adjusted)])
    baseline = pd.read_csv(out_adjusted / "event_level_metrics.csv")
    merged = event_level.merge(
        baseline,
        on=["event_id", "event_phase"],
        suffixes=("_market", "_adjusted"),
    )
    main_car_slug = car_cols[0]
    assert not np.allclose(
        merged[f"{main_car_slug}_market"].to_numpy(),
        merged[f"{main_car_slug}_adjusted"].to_numpy(),
        atol=1e-9,
    ), "market-model CARs should differ from simple market-adjusted CARs"

    # Skipped-events sidecar is written (empty) so reviewers always have a place
    # to look.
    skipped = pd.read_csv(out_dir / "event_study_skipped_events.csv")
    assert skipped.empty or skipped["reason"].notna().all()


def test_estimation_window_override_is_respected(tmp_path: Path) -> None:
    """A custom ``--estimation-window`` flag is threaded into the market model."""
    panel_path = tmp_path / "panel.csv"
    out_dir = tmp_path / "out_custom"
    _write_panel(panel_path, _build_synthetic_panel())

    rc = cli.main(
        [
            "--panel",
            str(panel_path),
            "--output-dir",
            str(out_dir),
            "--ar-model",
            "market",
            "--estimation-window",
            "60,5",
        ]
    )
    assert rc == 0

    meta = json.loads((out_dir / "event_study_meta.json").read_text())
    assert meta["estimation_window"] == [-60, -5]


def test_estimation_window_rejects_malformed_values(tmp_path: Path) -> None:
    """``--estimation-window`` requires LOW > HIGH positive ints."""
    panel_path = tmp_path / "panel.csv"
    out_dir = tmp_path / "out_bad"
    _write_panel(panel_path, _build_synthetic_panel(pre_days=10, post_days=5))

    for bad_value in ("not-a-tuple", "0,0", "10,20", "-120,-10"):
        with pytest.raises(SystemExit):
            cli.main(
                [
                    "--panel",
                    str(panel_path),
                    "--output-dir",
                    str(out_dir),
                    "--ar-model",
                    "market",
                    "--estimation-window",
                    bad_value,
                ]
            )


def test_market_mode_logs_events_with_too_few_estimation_obs(tmp_path: Path) -> None:
    """Events that cannot estimate β must be logged as skipped (not silent 0).

    Build a panel whose estimation window has only a single non-NaN paired
    observation: the OLS gate requires ≥ 2 paired obs, so the AR must come out
    NaN. The CLI is supposed to write the affected event/phase rows into
    ``event_study_skipped_events.csv`` AND surface them in the meta sidecar.
    """
    rng = np.random.default_rng(7)
    rows: list[dict[str, object]] = []
    # Single event/phase with 5 days: only day -1 lives inside (-120, -10)? No,
    # we instead use a tight estimation window so we can hand-engineer the
    # number of paired observations. We'll override --estimation-window=3,2 so
    # the estimation rows are relative_day == -3 and -2, then null out
    # benchmark_ret on -2 to leave a single paired observation.
    for rel in range(-5, 6):
        bench = float("nan") if rel == -2 else 0.001 * np.sin(rel / 3.0)
        ret = float(rng.normal(0.0, 0.002)) if not np.isnan(bench) else float("nan")
        # Force a single paired estimation observation only on day -3.
        rows.append(
            {
                "event_id": 99,
                "matched_to_event_id": 99,
                "market": "CN",
                "index_name": "CSI300",
                "event_ticker": "CN99",
                "ticker": "CN99",
                "security_name": "CN99",
                "event_type": "addition",
                "event_phase": "announce",
                "relative_day": rel,
                "ar": (ret - bench) if (not np.isnan(ret) and not np.isnan(bench)) else float("nan"),
                "ret": ret,
                "benchmark_ret": bench,
                "event_date": _EVENT_DATE.date().isoformat(),
                "event_date_raw": _EVENT_DATE.date().isoformat(),
                "mapped_market_date": _EVENT_DATE.date().isoformat(),
                "date": (_EVENT_DATE + pd.Timedelta(days=rel)).date().isoformat(),
                "treatment_group": 1,
                "inclusion": 1,
                "sector": "Industrials",
                "mkt_cap": 1e9,
                "turnover": 0.02,
                "volume": 1e6,
                "close": 100.0,
            }
        )
    panel_path = tmp_path / "panel.csv"
    out_dir = tmp_path / "out_thin"
    pd.DataFrame(rows).to_csv(panel_path, index=False)

    rc = cli.main(
        [
            "--panel",
            str(panel_path),
            "--output-dir",
            str(out_dir),
            "--ar-model",
            "market",
            "--estimation-window",
            "3,2",
        ]
    )
    assert rc == 0

    skipped = pd.read_csv(out_dir / "event_study_skipped_events.csv")
    assert not skipped.empty, "thin estimation window must surface in skipped sidecar"
    assert (skipped["event_id"].astype(int) == 99).all()
    assert set(skipped["reason"].unique()).issubset(
        {"insufficient_estimation_obs", "degenerate_benchmark_variance"}
    )

    meta = json.loads((out_dir / "event_study_meta.json").read_text())
    assert meta["n_events_skipped"] >= 1

    # Critical: the affected event must have NaN CARs (not silently 0). At
    # least one car_<slug> column must be NaN for the skipped event.
    event_level = pd.read_csv(out_dir / "event_level_metrics.csv")
    skipped_rows = event_level.loc[event_level["event_id"].astype(int) == 99]
    car_cols = [col for col in event_level.columns if col.startswith("car_")]
    assert car_cols
    assert skipped_rows[car_cols].isna().any(axis=None), (
        "skipped event must show NaN CARs, never silent 0"
    )
