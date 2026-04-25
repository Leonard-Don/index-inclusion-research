from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

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


def test_render_mechanism_heatmap_handles_all_nan_pivot(tmp_path):
    df = pd.DataFrame(
        [
            {
                "market": m,
                "event_phase": p,
                "outcome": o,
                "spec": "no_fe",
                "t": float("nan"),
            }
            for m in ("CN", "US")
            for p in ("announce", "effective")
            for o in ("car_1_1", "turnover_change")
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        out = render_mechanism_heatmap(df, output_dir=tmp_path)
    assert out.exists()


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


def test_estimate_quadrant_regression_returns_empty_when_underdetermined():
    panel = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "treatment_group": 1,
                "car_1_1": 0.01,
                "log_mktcap_pre": 20.0,
                "pre_turnover": 0.02,
            },
            {
                "market": "CN",
                "event_phase": "announce",
                "treatment_group": 0,
                "car_1_1": 0.02,
                "log_mktcap_pre": 20.5,
                "pre_turnover": 0.03,
            },
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = estimate_quadrant_regression(
            panel,
            market="CN",
            event_phase="announce",
            outcome="car_1_1",
            spec="controls",
        )
    assert np.isnan(result["coef"])
    assert np.isnan(result["se"])
    assert result["n_obs"] == 2


def test_estimate_quadrant_regression_drops_rows_with_nan_in_controls():
    # Mix treatment so kept rows still have both groups present.
    treatments = [1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1]
    panel = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "treatment_group": treatments[i],
                "car_1_1": 0.01 + 0.005 * i,
                "log_mktcap_pre": float("nan") if i % 2 == 0 else 20.0 + 0.1 * i,
                "pre_turnover": 0.02 + 0.001 * i,
            }
            for i in range(len(treatments))
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = estimate_quadrant_regression(
            panel,
            market="CN",
            event_phase="announce",
            outcome="car_1_1",
            spec="controls",
        )
    # Should fit successfully on the 6 rows with finite controls
    assert not np.isnan(result["coef"])
    assert result["n_obs"] == 6


def test_estimate_quadrant_regression_drops_rows_with_inf_in_controls():
    panel = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "treatment_group": t,
                "car_1_1": 0.01 + 0.005 * i,
                "log_mktcap_pre": float("inf") if i == 0 else 20.0 + 0.1 * i,
                "pre_turnover": 0.02 + 0.001 * i,
            }
            for i, t in enumerate([1, 0, 1, 0, 1, 0, 1, 0])
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = estimate_quadrant_regression(
            panel,
            market="CN",
            event_phase="announce",
            outcome="car_1_1",
            spec="controls",
        )
    assert not np.isnan(result["coef"])
    assert result["n_obs"] == 7


def test_estimate_quadrant_regression_skips_constant_outcome():
    panel = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "treatment_group": t,
                "car_1_1": 0.01,
                "log_mktcap_pre": 20.0 + 0.1 * i,
                "pre_turnover": 0.02 + 0.001 * i,
            }
            for i, t in enumerate([1, 0, 1, 0, 1, 0])
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = estimate_quadrant_regression(
            panel,
            market="CN",
            event_phase="announce",
            outcome="car_1_1",
            spec="no_fe",
        )
    assert np.isnan(result["coef"])
    assert result["n_obs"] == 6


def test_assemble_mechanism_comparison_table_emits_no_runtime_warnings():
    raw = _make_matched_panel()
    panel = build_mechanism_panel(raw)
    panel_us = panel.copy()
    panel_us["market"] = "US"
    all_panel = pd.concat([panel, panel_us], ignore_index=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        tbl = assemble_mechanism_comparison_table(all_panel)
    assert len(tbl) == 60


def test_compute_channel_concentration_table_combines_turnover_and_volume():
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_channel_concentration_table,
    )
    panel = pd.DataFrame(
        [
            # CN announce: turnover sig, volume not sig
            {"market": "CN", "event_phase": "announce", "outcome": "turnover_change",
             "spec": "no_fe", "coef": 0.001, "t": 2.5, "p_value": 0.012, "n_obs": 470},
            {"market": "CN", "event_phase": "announce", "outcome": "volume_change",
             "spec": "no_fe", "coef": 1e7, "t": 1.0, "p_value": 0.32, "n_obs": 470},
            # CN effective: both sig
            {"market": "CN", "event_phase": "effective", "outcome": "turnover_change",
             "spec": "no_fe", "coef": 0.0014, "t": 2.7, "p_value": 0.007, "n_obs": 470},
            {"market": "CN", "event_phase": "effective", "outcome": "volume_change",
             "spec": "no_fe", "coef": 5e6, "t": 2.1, "p_value": 0.036, "n_obs": 470},
            # US announce: both sig
            {"market": "US", "event_phase": "announce", "outcome": "turnover_change",
             "spec": "no_fe", "coef": 0.03, "t": 20.0, "p_value": 0.0, "n_obs": 980},
            {"market": "US", "event_phase": "announce", "outcome": "volume_change",
             "spec": "no_fe", "coef": 8e6, "t": 8.0, "p_value": 0.0, "n_obs": 980},
            # US effective: neither sig
            {"market": "US", "event_phase": "effective", "outcome": "turnover_change",
             "spec": "no_fe", "coef": 0.002, "t": 1.5, "p_value": 0.13, "n_obs": 980},
            {"market": "US", "event_phase": "effective", "outcome": "volume_change",
             "spec": "no_fe", "coef": 3e5, "t": 0.5, "p_value": 0.62, "n_obs": 980},
        ]
    )
    table = compute_channel_concentration_table(panel)
    assert {"market", "event_phase", "turnover_coef", "turnover_p", "volume_coef",
            "volume_p", "turnover_sig", "volume_sig", "both_channels_sig"}.issubset(
        table.columns
    )
    assert len(table) == 4
    by_quad = {(r["market"], r["event_phase"]): r for _, r in table.iterrows()}
    assert by_quad[("CN", "effective")]["both_channels_sig"]
    assert by_quad[("US", "announce")]["both_channels_sig"]
    assert not by_quad[("CN", "announce")]["both_channels_sig"]
    assert not by_quad[("US", "effective")]["both_channels_sig"]


def test_compute_channel_concentration_table_handles_missing_outcomes():
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_channel_concentration_table,
    )
    panel = pd.DataFrame(
        [
            {"market": "CN", "event_phase": "announce", "outcome": "turnover_change",
             "spec": "no_fe", "coef": 0.001, "t": 2.5, "p_value": 0.012, "n_obs": 470},
        ]
    )
    table = compute_channel_concentration_table(panel)
    row = table.iloc[0]
    assert row["turnover_sig"]
    assert not row["volume_sig"]
    import math
    assert math.isnan(row["volume_coef"])


def test_compute_h5_limit_predictive_regression_finds_clear_effect():
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_h5_limit_predictive_regression,
    )
    rng = np.random.default_rng(0)
    rows = []
    for i in range(80):
        limit_share = float(rng.uniform(0, 0.5))
        # Strong positive relationship: car ≈ 0.05 * limit_share + noise
        car_1_1 = 0.05 * limit_share + float(rng.normal(0, 0.005))
        rows.append({
            "event_id": f"cn-{i}",
            "market": "CN",
            "event_phase": "effective",
            "car_1_1": car_1_1,
            "price_limit_hit_share": limit_share,
            "log_mktcap_pre": 22.0 + float(rng.normal(0, 1)),
        })
    panel = pd.DataFrame(rows)
    result = compute_h5_limit_predictive_regression(panel, market="CN")
    assert result["limit_coef"] > 0.02
    assert result["limit_p_value"] < 0.001
    assert result["n_obs"] == 80


def test_compute_h5_limit_predictive_regression_returns_high_p_for_no_relationship():
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_h5_limit_predictive_regression,
    )
    rng = np.random.default_rng(11)
    rows = []
    for i in range(80):
        rows.append({
            "event_id": f"cn-{i}",
            "market": "CN",
            "event_phase": "effective",
            "car_1_1": float(rng.normal(0, 0.02)),
            "price_limit_hit_share": float(rng.uniform(0, 0.5)),
            "log_mktcap_pre": 22.0,
        })
    panel = pd.DataFrame(rows)
    result = compute_h5_limit_predictive_regression(panel, market="CN")
    assert result["limit_p_value"] > 0.10


def test_compute_h5_limit_predictive_regression_handles_small_sample():
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_h5_limit_predictive_regression,
    )
    panel = pd.DataFrame(
        [
            {"event_id": "cn-1", "market": "CN", "event_phase": "effective",
             "car_1_1": 0.01, "price_limit_hit_share": 0.1, "log_mktcap_pre": 22.0},
            {"event_id": "cn-2", "market": "CN", "event_phase": "effective",
             "car_1_1": 0.02, "price_limit_hit_share": 0.0, "log_mktcap_pre": 22.0},
        ]
    )
    import math

    result = compute_h5_limit_predictive_regression(panel, market="CN")
    assert math.isnan(result["limit_coef"])
    assert result["n_obs"] == 2


def test_export_h5_limit_predictive_regression_writes_csv(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_h5_limit_predictive_regression,
        export_h5_limit_predictive_regression_table,
    )
    rng = np.random.default_rng(2)
    rows = []
    for i in range(40):
        rows.append({
            "event_id": f"cn-{i}",
            "market": "CN",
            "event_phase": "effective",
            "car_1_1": 0.04 * float(rng.uniform(0, 0.5)) + float(rng.normal(0, 0.005)),
            "price_limit_hit_share": float(rng.uniform(0, 0.5)),
            "log_mktcap_pre": 22.0,
        })
    panel = pd.DataFrame(rows)
    result = compute_h5_limit_predictive_regression(panel, market="CN")
    out = export_h5_limit_predictive_regression_table(result, output_dir=tmp_path)
    assert out.name == "cma_h5_limit_predictive_regression.csv"
    written = pd.read_csv(out)
    assert {"limit_coef", "limit_p_value", "n_obs", "r_squared"}.issubset(written.columns)
    assert len(written) == 1


def test_export_channel_concentration_table_writes_csv(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.mechanism_panel import (
        compute_channel_concentration_table,
        export_channel_concentration_table,
    )
    panel = pd.DataFrame(
        [
            {"market": "CN", "event_phase": "announce", "outcome": "turnover_change",
             "spec": "no_fe", "coef": 0.001, "t": 2.5, "p_value": 0.012, "n_obs": 470},
            {"market": "CN", "event_phase": "announce", "outcome": "volume_change",
             "spec": "no_fe", "coef": 1e7, "t": 1.0, "p_value": 0.32, "n_obs": 470},
        ]
    )
    table = compute_channel_concentration_table(panel)
    out = export_channel_concentration_table(table, output_dir=tmp_path)
    assert out.name == "cma_h3_channel_concentration.csv"
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
