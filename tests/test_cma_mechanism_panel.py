from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
    assemble_mechanism_comparison_table,
    build_mechanism_panel,
    estimate_quadrant_regression,
    export_mechanism_tables,
    render_mechanism_heatmap,
)


def _make_matched_panel():
    rows = []
    rng = np.random.default_rng(0)
    for event_id in (1, 2, 3, 4):
        for phase in ("announce", "effective"):
            for rel in range(-20, 21):
                rows.append(
                    {
                        "event_id": event_id,
                        "market": "CN",
                        "event_type": "addition",
                        "treatment_group": 1 if event_id in (1, 3) else 0,
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 if rel in (-1, 0, 1) else 0.0,
                        "turnover": 0.02 + (0.01 if rel >= 0 else 0.0),
                        "volume": 100 + (20 if rel >= 0 else 0),
                        "ret": 0.01 * rng.standard_normal(),
                        "mkt_cap": 1.0e9,
                        "sector": "Tech",
                    }
                )
    return pd.DataFrame(rows)


def test_build_mechanism_panel_schema():
    raw = _make_matched_panel()
    out = build_mechanism_panel(raw)
    expected = {
        "event_id",
        "market",
        "event_phase",
        "treatment_group",
        "car_1_1",
        "turnover_change",
        "volume_change",
        "volatility_change",
        "price_limit_hit_share",
        "log_mktcap_pre",
        "pre_turnover",
        "sector",
    }
    assert expected.issubset(out.columns)
    # 4 events × 2 phases = 8 rows
    assert len(out) == 8


def test_build_mechanism_panel_car_1_1_correct():
    raw = _make_matched_panel()
    out = build_mechanism_panel(raw)
    assert np.allclose(out["car_1_1"], 0.03, atol=1e-9)


def test_estimate_quadrant_regression_returns_coefficient():
    raw = _make_matched_panel()
    panel = build_mechanism_panel(raw)
    # Also duplicate panel as US market, same treatment distribution
    panel_us = panel.copy()
    panel_us["market"] = "US"
    all_panel = pd.concat([panel, panel_us], ignore_index=True)
    result = estimate_quadrant_regression(
        all_panel,
        market="CN",
        event_phase="announce",
        outcome="car_1_1",
        spec="no_fe",
    )
    assert result["outcome"] == "car_1_1"
    assert result["market"] == "CN"
    assert result["event_phase"] == "announce"
    assert result["spec"] == "no_fe"
    assert "coef" in result and "se" in result and "t" in result
    assert result["n_obs"] >= 1


def test_assemble_mechanism_comparison_table_schema():
    raw = _make_matched_panel()
    panel = build_mechanism_panel(raw)
    panel_us = panel.copy()
    panel_us["market"] = "US"
    all_panel = pd.concat([panel, panel_us], ignore_index=True)
    tbl = assemble_mechanism_comparison_table(all_panel)
    expected = {
        "market",
        "event_phase",
        "outcome",
        "spec",
        "coef",
        "se",
        "t",
        "p_value",
        "n_obs",
        "r_squared",
    }
    assert expected.issubset(tbl.columns)
    # 2 markets × 2 phases × 5 outcomes × 3 specs = 60 rows
    assert len(tbl) == 60


def test_render_mechanism_heatmap_writes_png(tmp_path):
    df = pd.DataFrame(
        [
            {
                "market": m,
                "event_phase": p,
                "outcome": o,
                "spec": "no_fe",
                "t": 1.0,
            }
            for m in ("CN", "US")
            for p in ("announce", "effective")
            for o in ("car_1_1", "turnover_change")
        ]
    )
    out = render_mechanism_heatmap(df, output_dir=tmp_path)
    assert out.exists()


def test_export_mechanism_tables_writes_csv_and_tex(tmp_path):
    df = pd.DataFrame(
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
    )
    out = export_mechanism_tables(df, output_dir=tmp_path)
    assert out["csv"].exists()
    assert out["tex"].exists()
