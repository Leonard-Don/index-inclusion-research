"""Wiring tests for the descriptive event-study robustness artifacts that
``figures_tables`` now refreshes via ``_maybe_build_robustness_event_study``.

The analysis functions themselves are covered by
``test_robustness_event_study.py``; here we only assert the figures_tables
helper (a) writes the four ``robustness_*`` / ``parallel_trends_*`` outputs
when handed a valid matched daily panel + event-level frame, and (b) skips
cleanly (no outputs, no raise) when the inputs are empty — the contract the
demo profile and the existing ``figures_tables`` main tests rely on.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.figures_tables import (
    _maybe_build_robustness_event_study,
)


def _matched_daily_panel() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    rows: list[dict[str, object]] = []
    for market in ("CN", "US"):
        for phase in ("announce", "effective"):
            for event in range(12):
                event_id = f"{market}_{phase}_{event}"
                treat = 1 if event % 2 == 0 else 0
                for day in range(-12, 13):
                    ar = float(rng.normal(0.0, 0.01))
                    if treat and phase == "announce" and -1 <= day <= 1:
                        ar += 0.02
                    rows.append(
                        {
                            "market": market,
                            "event_phase": phase,
                            "event_id": event_id,
                            "treatment_group": treat,
                            "relative_day": day,
                            "ar": ar,
                            "event_date": f"2024-0{1 + event % 9}-01",
                        }
                    )
    return pd.DataFrame(rows)


def _event_level() -> pd.DataFrame:
    rng = np.random.default_rng(11)
    rows: list[dict[str, object]] = []
    for market in ("CN", "US"):
        for phase in ("announce", "effective"):
            for event in range(20):
                base = 0.02 if (phase == "announce") else 0.0
                rows.append(
                    {
                        "market": market,
                        "event_phase": phase,
                        "event_id": f"{market}_{phase}_{event}",
                        "treatment_group": 1,
                        "event_date": f"2024-0{1 + event % 9}-01",
                        "car_m1_p1": float(rng.normal(base, 0.01)),
                    }
                )
    return pd.DataFrame(rows)


def test_helper_writes_robustness_artifacts(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    tables.mkdir()
    figures.mkdir()

    _maybe_build_robustness_event_study(
        matched_panel=_matched_daily_panel(),
        event_level=_event_level(),
        tables_dir=tables,
        figures_dir=figures,
    )

    assert (tables / "robustness_parallel_trends_aar.csv").exists()
    assert (tables / "robustness_placebo_car.csv").exists()
    assert (tables / "robustness_car_permutation.csv").exists()
    assert (tables / "robustness_car_clustered_se.csv").exists()
    # at least one parallel-trends figure rendered
    assert list(figures.glob("parallel_trends_aar_*.png"))


def test_helper_skips_cleanly_on_empty_inputs(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    tables.mkdir()
    figures.mkdir()

    _maybe_build_robustness_event_study(
        matched_panel=pd.DataFrame(),
        event_level=pd.DataFrame(),
        tables_dir=tables,
        figures_dir=figures,
    )

    assert not list(tables.glob("robustness_*.csv"))
    assert not list(figures.glob("parallel_trends_*.png"))
