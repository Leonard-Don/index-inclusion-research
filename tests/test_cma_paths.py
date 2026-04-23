from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry.paths import (
    build_daily_ar_panel,
)


def _make_panel_frame() -> pd.DataFrame:
    rows = []
    for market in ("CN", "US"):
        for event_phase in ("announce", "effective"):
            for event_id, ar_scale in ((1, 0.01), (2, 0.02)):
                for rel in range(-3, 4):
                    rows.append(
                        {
                            "event_id": event_id,
                            "market": market,
                            "event_phase": event_phase,
                            "event_type": "addition",
                            "relative_day": rel,
                            "ar": ar_scale * (1 if rel == 0 else 0.5),
                        }
                    )
    rows.append(
        {
            "event_id": 99,
            "market": "US",
            "event_phase": "announce",
            "event_type": "deletion",
            "relative_day": 0,
            "ar": 0.9,
        }
    )
    return pd.DataFrame(rows)


def test_build_daily_ar_panel_filters_additions_only():
    raw = _make_panel_frame()
    out = build_daily_ar_panel(raw)
    assert (out["event_type"] == "addition").all()
    assert "relative_day" in out.columns
    assert set(out["market"].unique()) == {"CN", "US"}
    assert set(out["event_phase"].unique()) == {"announce", "effective"}


def test_build_daily_ar_panel_adds_cumulative_car():
    raw = _make_panel_frame()
    out = build_daily_ar_panel(raw)
    assert "car" in out.columns
    first = (
        out.sort_values(["event_id", "market", "event_phase", "relative_day"])
        .groupby(["event_id", "market", "event_phase"], as_index=False)
        .head(1)
    )
    pd.testing.assert_series_equal(
        first["ar"].reset_index(drop=True),
        first["car"].reset_index(drop=True),
        check_names=False,
    )


def test_build_daily_ar_panel_requires_columns():
    with pytest.raises(ValueError, match="missing columns"):
        build_daily_ar_panel(pd.DataFrame({"ar": [0.0]}))
