from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
    compute_gap_metrics,
)


def _build_events_and_panel():
    events = pd.DataFrame(
        [
            {
                "event_id": 1,
                "market": "CN",
                "ticker": "000001",
                "event_type": "addition",
                "announce_date": "2024-01-01",
                "effective_date": "2024-01-15",
            },
            {
                "event_id": 2,
                "market": "US",
                "ticker": "AAPL",
                "event_type": "addition",
                "announce_date": "2024-02-01",
                "effective_date": "2024-02-10",
            },
        ]
    )
    rows = []

    def _append(event_id, market, phase, relative_day, ar):
        rows.append(
            {
                "event_id": event_id,
                "market": market,
                "event_phase": phase,
                "event_type": "addition",
                "relative_day": relative_day,
                "ar": ar,
            }
        )

    for rel in range(-20, 21):
        _append(1, "CN", "announce", rel, 0.01 if rel == 0 else 0.002)
        _append(1, "CN", "effective", rel, 0.005 if rel == 0 else 0.001)
        _append(2, "US", "announce", rel, 0.015 if rel == 0 else 0.001)
        _append(2, "US", "effective", rel, 0.0 if rel == 0 else 0.001)
    panel = pd.DataFrame(rows)
    return events, panel


def test_compute_gap_metrics_schema():
    events, panel = _build_events_and_panel()
    out = compute_gap_metrics(events, panel)
    expected = {
        "event_id",
        "market",
        "ticker",
        "announce_date",
        "effective_date",
        "sector",
        "batch_id",
        "gap_length_days",
        "pre_announce_runup",
        "announce_jump",
        "gap_drift",
        "effective_jump",
        "post_effective_reversal",
    }
    assert expected.issubset(out.columns)


def test_compute_gap_metrics_carries_sector_and_batch_id():
    events, panel = _build_events_and_panel()
    events.loc[events["event_id"] == 1, "sector"] = "Finance"
    events.loc[events["event_id"] == 1, "batch_id"] = "2024H1"
    out = compute_gap_metrics(events, panel)
    cn = out.loc[out["event_id"] == 1].iloc[0]
    assert cn["sector"] == "Finance"
    assert cn["batch_id"] == "2024H1"


def test_compute_gap_metrics_computes_gap_length():
    events, panel = _build_events_and_panel()
    out = compute_gap_metrics(events, panel)
    cn = out.loc[out["event_id"] == 1].iloc[0]
    assert cn["gap_length_days"] == 14


def test_compute_gap_metrics_announce_jump_uses_announce_phase():
    events, panel = _build_events_and_panel()
    out = compute_gap_metrics(events, panel)
    cn = out.loc[out["event_id"] == 1].iloc[0]
    # announce_jump = sum AR over [-1, 1] around announce: 0.002 + 0.01 + 0.002 = 0.014
    assert cn["announce_jump"] == pytest.approx(0.014, abs=1e-9)


def test_compute_gap_metrics_filters_addition_only():
    events, panel = _build_events_and_panel()
    events_with_del = pd.concat(
        [
            events,
            pd.DataFrame(
                [
                    {
                        "event_id": 3,
                        "market": "CN",
                        "ticker": "000002",
                        "event_type": "deletion",
                        "announce_date": "2024-03-01",
                        "effective_date": "2024-03-15",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    out = compute_gap_metrics(events_with_del, panel)
    assert (out["event_id"] != 3).all()


def test_summarize_gap_metrics_schema():
    events, panel = _build_events_and_panel()
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        summarize_gap_metrics,
    )
    gap = compute_gap_metrics(events, panel)
    summary = summarize_gap_metrics(gap)
    expected = {"market", "metric", "mean", "median", "se", "t", "p_value", "n_events"}
    assert expected.issubset(summary.columns)
    assert len(summary) == 12


def test_render_gap_figures_writes_png(tmp_path):
    events, panel = _build_events_and_panel()
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        render_gap_figures,
        summarize_gap_metrics,
    )
    gap = compute_gap_metrics(events, panel)
    summary = summarize_gap_metrics(gap)
    outs = render_gap_figures(gap, summary, output_dir=tmp_path)
    for key in ("gap_distribution", "gap_decomposition"):
        assert outs[key].exists()


def _gap_event_level(cn_runups, us_runups):
    rows = []
    for i, value in enumerate(cn_runups):
        rows.append({"market": "CN", "pre_announce_runup": value, "event_id": f"cn-{i}"})
    for i, value in enumerate(us_runups):
        rows.append({"market": "US", "pre_announce_runup": value, "event_id": f"us-{i}"})
    return pd.DataFrame(rows)


def test_compute_pre_runup_bootstrap_returns_significant_p_for_clear_difference():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    cn_runups = [0.05] * 60
    us_runups = [0.00] * 60
    result = compute_pre_runup_bootstrap_test(
        _gap_event_level(cn_runups, us_runups), n_boot=2000, seed=0
    )
    assert result["n_cn"] == 60
    assert result["n_us"] == 60
    assert result["diff_mean"] == pytest.approx(0.05, abs=1e-9)
    assert result["boot_p_value"] < 0.01
    assert result["boot_ci_low"] > 0


def test_compute_pre_runup_bootstrap_returns_high_p_when_no_difference():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    rng = __import__("numpy").random.default_rng(123)
    cn_runups = list(rng.normal(0.0, 0.02, size=80))
    us_runups = list(rng.normal(0.0, 0.02, size=80))
    result = compute_pre_runup_bootstrap_test(
        _gap_event_level(cn_runups, us_runups), n_boot=2000, seed=0
    )
    assert result["boot_p_value"] > 0.20
    assert result["boot_ci_low"] < 0 < result["boot_ci_high"]


def test_compute_pre_runup_bootstrap_handles_small_sample():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    result = compute_pre_runup_bootstrap_test(
        _gap_event_level([0.01], [0.02]), n_boot=2000, seed=0
    )
    import math

    assert result["n_cn"] == 1
    assert result["n_us"] == 1
    assert math.isnan(result["boot_p_value"])
    assert result["n_boot"] == 0


def _gap_event_level_with_clusters(cn_clusters, us_clusters):
    """Each cluster is a list of values that share an announce_date."""
    rows = []
    for c_idx, values in enumerate(cn_clusters):
        for i, v in enumerate(values):
            rows.append(
                {
                    "market": "CN",
                    "pre_announce_runup": v,
                    "event_id": f"cn-{c_idx}-{i}",
                    "announce_date": f"2024-01-{c_idx + 1:02d}",
                }
            )
    for c_idx, values in enumerate(us_clusters):
        for i, v in enumerate(values):
            rows.append(
                {
                    "market": "US",
                    "pre_announce_runup": v,
                    "event_id": f"us-{c_idx}-{i}",
                    "announce_date": f"2024-02-{c_idx + 1:02d}",
                }
            )
    return pd.DataFrame(rows)


def test_compute_pre_runup_bootstrap_block_returns_cluster_method_and_counts():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    cn_clusters = [[0.05, 0.05, 0.05]] * 20
    us_clusters = [[0.0, 0.0]] * 20
    panel = _gap_event_level_with_clusters(cn_clusters, us_clusters)
    result = compute_pre_runup_bootstrap_test(
        panel, n_boot=500, seed=0, block_by="announce_date"
    )
    assert result["cluster_method"] == "announce_date"
    assert result["n_cn_clusters"] == 20
    assert result["n_us_clusters"] == 20
    assert result["n_cn"] == 60
    assert result["n_us"] == 40
    assert result["boot_p_value"] < 0.05


def test_compute_pre_runup_bootstrap_block_widens_ci_when_within_cluster_correlated():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    rng = __import__("numpy").random.default_rng(11)
    # 10 perfectly-correlated clusters of size 5 (all events in a cluster share a value)
    cn_cluster_means = list(rng.normal(0.0, 0.04, size=10))
    us_cluster_means = list(rng.normal(0.0, 0.04, size=10))
    cn_clusters = [[m] * 5 for m in cn_cluster_means]
    us_clusters = [[m] * 5 for m in us_cluster_means]
    panel = _gap_event_level_with_clusters(cn_clusters, us_clusters)
    iid = compute_pre_runup_bootstrap_test(panel, n_boot=2000, seed=0)
    block = compute_pre_runup_bootstrap_test(
        panel, n_boot=2000, seed=0, block_by="announce_date"
    )
    iid_width = iid["boot_ci_high"] - iid["boot_ci_low"]
    block_width = block["boot_ci_high"] - block["boot_ci_low"]
    # Block bootstrap should produce a wider CI when within-cluster values are
    # perfectly correlated, because effective sample size is # clusters not # events.
    assert block_width > iid_width


def test_compute_pre_runup_bootstrap_block_falls_back_when_too_few_clusters():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    panel = _gap_event_level_with_clusters([[0.01, 0.02, 0.03]], [[0.0, 0.01]])
    result = compute_pre_runup_bootstrap_test(
        panel, n_boot=500, seed=0, block_by="announce_date"
    )
    import math

    assert result["n_cn_clusters"] == 1
    assert result["n_us_clusters"] == 1
    assert math.isnan(result["boot_p_value"])
    assert result["n_boot"] == 0


def test_compute_pre_runup_bootstrap_seed_reproducible():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
    )
    cn = [0.03, 0.04, -0.01, 0.02, 0.05, 0.01]
    us = [0.00, 0.01, -0.02, 0.02, -0.01, 0.00]
    a = compute_pre_runup_bootstrap_test(_gap_event_level(cn, us), n_boot=1000, seed=42)
    b = compute_pre_runup_bootstrap_test(_gap_event_level(cn, us), n_boot=1000, seed=42)
    assert a["boot_p_value"] == b["boot_p_value"]
    assert a["boot_ci_low"] == b["boot_ci_low"]
    assert a["boot_ci_high"] == b["boot_ci_high"]


def test_compute_gap_drift_cross_market_regression_finds_clear_cn_effect():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_drift_cross_market_regression,
    )
    rows = []
    for i in range(80):
        rows.append({"market": "CN", "gap_drift": 0.04 + 0.001 * i, "gap_length_days": 14})
    for i in range(80):
        rows.append({"market": "US", "gap_drift": -0.001 + 0.0005 * i, "gap_length_days": 7})
    panel = pd.DataFrame(rows)
    result = compute_gap_drift_cross_market_regression(panel)
    assert result["cn_coef"] > 0.02
    assert result["cn_p_value"] < 0.001
    assert result["n_obs"] == 160


def test_compute_gap_drift_cross_market_regression_returns_high_p_for_no_difference():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_drift_cross_market_regression,
    )
    np_mod = __import__("numpy")
    rng = np_mod.random.default_rng(7)
    rows = []
    for _ in range(120):
        rows.append({"market": "CN", "gap_drift": float(rng.normal(0, 0.05)), "gap_length_days": 14})
    for _ in range(120):
        rows.append({"market": "US", "gap_drift": float(rng.normal(0, 0.05)), "gap_length_days": float(rng.integers(0, 21))})
    panel = pd.DataFrame(rows)
    result = compute_gap_drift_cross_market_regression(panel)
    assert result["cn_p_value"] > 0.10
    assert result["n_obs"] == 240


def test_compute_gap_drift_cross_market_regression_handles_small_sample():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_drift_cross_market_regression,
    )
    panel = pd.DataFrame(
        [
            {"market": "CN", "gap_drift": 0.01, "gap_length_days": 14},
            {"market": "US", "gap_drift": 0.0, "gap_length_days": 7},
        ]
    )
    import math

    result = compute_gap_drift_cross_market_regression(panel)
    assert math.isnan(result["cn_coef"])
    assert result["n_obs"] == 2


def test_compute_gap_drift_cross_market_regression_drops_nan_rows():
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_drift_cross_market_regression,
    )
    rows = []
    for i in range(40):
        rows.append({"market": "CN", "gap_drift": 0.03 if i % 3 else float("nan"), "gap_length_days": 14})
        rows.append({"market": "US", "gap_drift": 0.0 if i % 3 else 0.01, "gap_length_days": float("nan") if i % 5 == 0 else 7})
    panel = pd.DataFrame(rows)
    result = compute_gap_drift_cross_market_regression(panel)
    # Each input has NaN drops; n_obs should be < 80 but > 0 and finite coef
    assert result["n_obs"] > 0
    assert result["n_obs"] < 80
    import math

    assert not math.isnan(result["cn_coef"])


def test_export_gap_drift_cross_market_regression_table_writes_csv(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_gap_drift_cross_market_regression,
        export_gap_drift_cross_market_regression_table,
    )
    rows = []
    for _ in range(50):
        rows.append({"market": "CN", "gap_drift": 0.03, "gap_length_days": 14})
        rows.append({"market": "US", "gap_drift": 0.0, "gap_length_days": 7})
    panel = pd.DataFrame(rows)
    result = compute_gap_drift_cross_market_regression(panel)
    out = export_gap_drift_cross_market_regression_table(result, output_dir=tmp_path)
    assert out.exists()
    assert out.name == "cma_gap_drift_market_regression.csv"
    written = pd.read_csv(out)
    assert {"cn_coef", "cn_p_value", "gap_length_coef", "n_obs", "r_squared"}.issubset(
        written.columns
    )
    assert len(written) == 1


def test_export_pre_runup_bootstrap_table_writes_csv(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        compute_pre_runup_bootstrap_test,
        export_pre_runup_bootstrap_table,
    )
    result = compute_pre_runup_bootstrap_test(
        _gap_event_level([0.05] * 30, [0.0] * 30), n_boot=500, seed=0
    )
    out = export_pre_runup_bootstrap_table(result, output_dir=tmp_path)
    assert out.exists()
    written = pd.read_csv(out)
    assert {"cn_mean", "us_mean", "diff_mean", "boot_p_value", "n_cn", "n_us"}.issubset(
        written.columns
    )
    assert len(written) == 1


def test_export_gap_tables_writes_csvs(tmp_path):
    events, panel = _build_events_and_panel()
    from index_inclusion_research.analysis.cross_market_asymmetry.gap_period import (
        export_gap_tables,
        summarize_gap_metrics,
    )
    gap = compute_gap_metrics(events, panel)
    summary = summarize_gap_metrics(gap)
    paths = export_gap_tables(gap, summary, output_dir=tmp_path)
    assert paths["event_level"].exists()
    assert paths["summary"].exists()
