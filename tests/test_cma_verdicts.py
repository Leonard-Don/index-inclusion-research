from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
    build_hypothesis_verdicts,
    export_hypothesis_verdicts,
)


def _gap_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market": "CN",
                "metric": "pre_announce_runup",
                "mean": 0.035,
                "t": 3.1,
                "p_value": 0.002,
            },
            {
                "market": "US",
                "metric": "pre_announce_runup",
                "mean": 0.020,
                "t": 2.5,
                "p_value": 0.012,
            },
            {
                "market": "CN",
                "metric": "gap_drift",
                "mean": 0.010,
                "t": 1.1,
                "p_value": 0.27,
            },
            {
                "market": "US",
                "metric": "gap_drift",
                "mean": -0.003,
                "t": -0.6,
                "p_value": 0.55,
            },
        ]
    )


def _mechanism_panel() -> pd.DataFrame:
    rows = []
    for market, phase, outcome, coef, t, p in [
        ("CN", "effective", "turnover_change", 0.002, 2.7, 0.007),
        ("US", "announce", "turnover_change", 0.030, 20.0, 0.000),
        ("US", "effective", "turnover_change", 0.002, 1.8, 0.070),
        ("CN", "effective", "volume_change", 5.0, 1.5, 0.130),
        ("CN", "announce", "price_limit_hit_share", 0.002, 0.8, 0.400),
        ("CN", "effective", "price_limit_hit_share", 0.001, 0.5, 0.620),
    ]:
        rows.append(
            {
                "market": market,
                "event_phase": phase,
                "outcome": outcome,
                "spec": "no_fe",
                "coef": coef,
                "t": t,
                "p_value": p,
            }
        )
    return pd.DataFrame(rows)


def _heterogeneity_size() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"market": "CN", "bucket": "Q1", "asymmetry_index": 1.4},
            {"market": "CN", "bucket": "Q2", "asymmetry_index": 1.2},
            {"market": "CN", "bucket": "Q4", "asymmetry_index": 0.2},
            {"market": "CN", "bucket": "Q5", "asymmetry_index": -0.1},
        ]
    )


def _rolling() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market": "US",
                "event_phase": "effective",
                "window_end_year": 2014,
                "car_mean": 0.004,
            },
            {
                "market": "US",
                "event_phase": "effective",
                "window_end_year": 2020,
                "car_mean": -0.002,
            },
        ]
    )


def test_build_hypothesis_verdicts_returns_six_ordered_rows() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )

    assert list(verdicts["hid"]) == ["H1", "H2", "H3", "H4", "H5", "H6"]
    assert {
        "hid",
        "name_cn",
        "verdict",
        "confidence",
        "evidence_summary",
        "metric_snapshot",
        "next_step",
        "evidence_refs",
    }.issubset(verdicts.columns)
    by_hid = verdicts.set_index("hid")
    assert by_hid.loc["H1", "verdict"] == "部分支持"
    assert by_hid.loc["H2", "verdict"] == "待补数据"
    assert by_hid.loc["H3", "verdict"] == "支持"
    assert by_hid.loc["H4", "verdict"] == "部分支持"
    assert by_hid.loc["H5", "verdict"] == "证据不足"
    assert by_hid.loc["H6", "verdict"] == "部分支持"


def test_h2_uses_aum_when_available() -> None:
    aum = pd.DataFrame(
        {
            "market": ["US", "US"],
            "year": [2014, 2020],
            "aum_trillion": [2.0, 7.0],
        }
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        aum_frame=aum,
    )

    h2 = verdicts.set_index("hid").loc["H2"]
    assert h2["verdict"] == "部分支持"
    assert "US AUM" in h2["metric_snapshot"]


def test_export_hypothesis_verdicts_writes_csv(tmp_path) -> None:
    out = export_hypothesis_verdicts(
        output_dir=tmp_path,
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )

    assert out.name == "cma_hypothesis_verdicts.csv"
    exported = pd.read_csv(out)
    assert len(exported) == 6
    assert exported.loc[exported["hid"] == "H3", "verdict"].iloc[0] == "支持"
