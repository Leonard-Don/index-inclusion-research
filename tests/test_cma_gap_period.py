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
