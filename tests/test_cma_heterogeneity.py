from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.heterogeneity import (
    build_heterogeneity_panel,
    compute_cell_statistics,
    compute_h7_sector_interaction,
    export_h7_sector_interaction,
    export_heterogeneity_tables,
    render_heterogeneity_matrix,
)


def _make_panel():
    rng = np.random.default_rng(42)
    rows = []
    for event_id in range(1, 11):
        market = "CN" if event_id <= 5 else "US"
        sector = "Tech" if event_id % 2 == 0 else "Fin"
        for phase in ("announce", "effective"):
            for rel in (-3, -1, 0, 1, 3, 10):
                rows.append(
                    {
                        "event_id": event_id,
                        "market": market,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 * rng.standard_normal(),
                        "mkt_cap": 1e9 * event_id,
                        "turnover": 0.01 * event_id,
                        "sector": sector,
                    }
                )
    return pd.DataFrame(rows)


def test_build_heterogeneity_panel_size_adds_bucket():
    panel = _make_panel()
    out = build_heterogeneity_panel(panel, dim="size")
    assert "bucket" in out.columns
    assert out["bucket"].notna().all()
    cn_counts = out.loc[out["market"] == "CN", "bucket"].value_counts()
    assert len(cn_counts) <= 5


def test_build_heterogeneity_panel_sector_uses_sector_values():
    panel = _make_panel()
    out = build_heterogeneity_panel(panel, dim="sector")
    assert set(out["bucket"].unique()) == {"Tech", "Fin"}


def test_build_heterogeneity_panel_unknown_dim_raises():
    panel = _make_panel()
    with pytest.raises(ValueError):
        build_heterogeneity_panel(panel, dim="unknown")


def test_compute_cell_statistics_asymmetry_index():
    panel = _make_panel()
    gap = pd.DataFrame(
        {"event_id": list(range(1, 11)), "gap_length_days": [5, 15, 25, 5, 15, 25, 5, 15, 25, 5]}
    )
    buckets = build_heterogeneity_panel(panel, dim="sector")
    stats = compute_cell_statistics(panel, buckets, gap_frame=gap)
    expected = {
        "market",
        "dim",
        "bucket",
        "announce_car",
        "effective_car",
        "gap_drift",
        "asymmetry_index",
        "n_events",
    }
    assert expected.issubset(stats.columns)


def test_render_heterogeneity_matrix_writes_png(tmp_path):
    df = pd.DataFrame(
        [
            {
                "market": "CN",
                "dim": "size",
                "bucket": f"Q{i}",
                "asymmetry_index": 0.1 * i,
                "n_events": 10,
            }
            for i in range(1, 6)
        ]
        + [
            {
                "market": "US",
                "dim": "size",
                "bucket": f"Q{i}",
                "asymmetry_index": -0.1 * i,
                "n_events": 10,
            }
            for i in range(1, 6)
        ]
    )
    out = render_heterogeneity_matrix(df, dim="size", output_dir=tmp_path)
    assert out.exists()


def test_export_heterogeneity_tables_writes_csvs(tmp_path):
    df = pd.DataFrame(
        {
            "market": ["CN"],
            "dim": ["size"],
            "bucket": ["Q1"],
            "asymmetry_index": [0.5],
        }
    )
    paths = export_heterogeneity_tables({"size": df}, output_dir=tmp_path)
    assert paths["size"].exists()


def _make_h7_mechanism_panel():
    rng = np.random.default_rng(7)
    rows = []
    for market in ("CN", "US"):
        for sector in ("Tech", "Finance", "Energy"):
            for phase in ("announce", "effective"):
                for treatment in (0, 1):
                    for i in range(8):
                        sector_effect = 0.0
                        if market == "US" and sector == "Tech" and treatment == 1:
                            sector_effect = 0.05
                        rows.append(
                            {
                                "market": market,
                                "event_phase": phase,
                                "treatment_group": treatment,
                                "sector": sector,
                                "car_1_1": (
                                    0.01 * treatment
                                    + sector_effect
                                    + 0.002 * rng.standard_normal()
                                    + 0.001 * i
                                ),
                            }
                        )
    return pd.DataFrame(rows)


def test_compute_h7_sector_interaction_estimates_market_rows():
    out = compute_h7_sector_interaction(
        _make_h7_mechanism_panel(), min_obs_per_sector=6
    )
    expected = {
        "market",
        "status",
        "signal",
        "n_obs",
        "sector_count",
        "joint_p_value",
        "top_term",
    }
    assert expected.issubset(out.columns)
    assert set(out["market"]) == {"CN", "US"}
    assert (out["status"] == "pass").all()
    us = out.set_index("market").loc["US"]
    assert us["signal"] == "support"
    assert float(us["joint_p_value"]) < 0.10


def test_export_h7_sector_interaction_writes_csv(tmp_path):
    out = compute_h7_sector_interaction(
        _make_h7_mechanism_panel(), min_obs_per_sector=6
    )
    path = export_h7_sector_interaction(out, output_dir=tmp_path)
    assert path.exists()
    saved = pd.read_csv(path)
    assert "joint_p_value" in saved.columns
