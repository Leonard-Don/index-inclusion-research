from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research import cross_market_asymmetry as cma_cli


def _make_min_event_panel():
    rows = []
    for event_id in (1, 2, 3, 4):
        market = "CN" if event_id <= 2 else "US"
        for phase in ("announce", "effective"):
            for rel in range(-20, 21):
                rows.append(
                    {
                        "event_id": event_id,
                        "market": market,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 if rel == 0 else 0.001,
                        "ret": 0.005 if rel == 0 else 0.0005,
                        "turnover": 0.02 if rel >= 0 else 0.015,
                        "volume": 110 if rel >= 0 else 100,
                        "mkt_cap": 1e9 * event_id,
                        "treatment_group": 1 if event_id in (1, 3) else 0,
                        "sector": "Tech",
                        "event_date": "2020-06-01",
                    }
                )
    return pd.DataFrame(rows)


def _make_min_events():
    return pd.DataFrame(
        [
            {
                "event_id": i,
                "market": "CN" if i <= 2 else "US",
                "ticker": f"T{i}",
                "event_type": "addition",
                "announce_date": "2020-05-15",
                "effective_date": "2020-06-01",
            }
            for i in (1, 2, 3, 4)
        ]
    )


def test_cli_main_fails_when_inputs_missing(tmp_path):
    fake = tmp_path / "nope.csv"
    with pytest.raises(FileNotFoundError):
        cma_cli.main(
            [
                "--event-panel",
                str(fake),
                "--matched-panel",
                str(fake),
                "--events",
                str(fake),
                "--tables-dir",
                str(tmp_path / "t"),
                "--figures-dir",
                str(tmp_path / "f"),
            ]
        )


def test_cli_main_runs_with_valid_inputs(tmp_path):
    event_panel_path = tmp_path / "event_panel.csv"
    matched_path = tmp_path / "matched.csv"
    events_path = tmp_path / "events.csv"
    _make_min_event_panel().to_csv(event_panel_path, index=False)
    _make_min_event_panel().to_csv(matched_path, index=False)
    _make_min_events().to_csv(events_path, index=False)
    summary_path = tmp_path / "summary.md"

    cma_cli.main(
        [
            "--event-panel",
            str(event_panel_path),
            "--matched-panel",
            str(matched_path),
            "--events",
            str(events_path),
            "--tables-dir",
            str(tmp_path / "tables"),
            "--figures-dir",
            str(tmp_path / "figures"),
            "--research-summary",
            str(summary_path),
        ]
    )
    assert (tmp_path / "tables" / "cma_ar_path.csv").exists()
    assert summary_path.exists()


def test_cli_tex_only_regenerates(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "outcome": "car_1_1",
                "spec": "no_fe",
                "coef": 0.01,
                "se": 0.002,
                "t": 5.0,
                "p_value": 0.0,
                "n_obs": 100,
                "r_squared": 0.1,
            }
        ]
    ).to_csv(tables / "cma_mechanism_panel.csv", index=False)
    cma_cli.main(["--tex-only", "--tables-dir", str(tables)])
    assert (tables / "cma_mechanism_panel.tex").exists()


def test_cli_py_exports_run_cma_main():
    from index_inclusion_research import cli

    assert callable(cli.run_cma_main)


def test_cli_supports_aum_argument(tmp_path):
    aum = pd.DataFrame(
        {
            "market": ["CN", "US"],
            "year": [2020, 2020],
            "aum_trillion": [2.0, 5.0],
        }
    )
    aum_path = tmp_path / "aum.csv"
    aum.to_csv(aum_path, index=False)

    event_panel_path = tmp_path / "event_panel.csv"
    matched_path = tmp_path / "matched.csv"
    events_path = tmp_path / "events.csv"
    _make_min_event_panel().to_csv(event_panel_path, index=False)
    _make_min_event_panel().to_csv(matched_path, index=False)
    _make_min_events().to_csv(events_path, index=False)

    cma_cli.main(
        [
            "--event-panel",
            str(event_panel_path),
            "--matched-panel",
            str(matched_path),
            "--events",
            str(events_path),
            "--tables-dir",
            str(tmp_path / "tables"),
            "--figures-dir",
            str(tmp_path / "figures"),
            "--research-summary",
            str(tmp_path / "summary.md"),
            "--aum",
            str(aum_path),
        ]
    )
    # Figure should still exist
    assert (tmp_path / "figures" / "cma_time_series_rolling.png").exists()
