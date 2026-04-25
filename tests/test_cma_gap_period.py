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
        "gap_length_days",
        "pre_announce_runup",
        "announce_jump",
        "gap_drift",
        "effective_jump",
        "post_effective_reversal",
    }
    assert expected.issubset(out.columns)


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
