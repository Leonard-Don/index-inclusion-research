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


def test_build_hypothesis_verdicts_returns_seven_ordered_rows() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )

    assert list(verdicts["hid"]) == ["H1", "H2", "H3", "H4", "H5", "H6", "H7"]
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
    # H7 with no sector frame → 待补数据
    assert by_hid.loc["H7", "verdict"] == "待补数据"


def test_h6_uses_weight_change_when_provided() -> None:
    weight_change = pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "weight_proxy": 0.05},
            {"market": "CN", "ticker": "000002", "weight_proxy": 0.04},
            {"market": "CN", "ticker": "000003", "weight_proxy": 0.03},
            {"market": "CN", "ticker": "000004", "weight_proxy": 0.01},
            {"market": "CN", "ticker": "000005", "weight_proxy": 0.005},
            {"market": "CN", "ticker": "000006", "weight_proxy": 0.001},
        ]
    )
    gap_event_level = pd.DataFrame(
        [
            # heavy weight events (above median): announce_jump = 0.03
            {"market": "CN", "ticker": "000001", "announce_jump": 0.03, "gap_drift": 0.01},
            {"market": "CN", "ticker": "000002", "announce_jump": 0.03, "gap_drift": 0.01},
            {"market": "CN", "ticker": "000003", "announce_jump": 0.03, "gap_drift": 0.01},
            # light weight events (below/at median): announce_jump = 0.005
            {"market": "CN", "ticker": "000004", "announce_jump": 0.005, "gap_drift": 0.0},
            {"market": "CN", "ticker": "000005", "announce_jump": 0.005, "gap_drift": 0.0},
            {"market": "CN", "ticker": "000006", "announce_jump": 0.005, "gap_drift": 0.0},
        ]
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        weight_change=weight_change,
        gap_event_level=gap_event_level,
    )
    h6 = verdicts.set_index("hid").loc["H6"]
    # heavy 0.03 vs light 0.005 → spread positive, both>0 → 支持
    assert h6["verdict"] == "支持"
    assert "heavy" in h6["metric_snapshot"] or "重权重" in h6["evidence_summary"]
    assert h6["key_label"] == "heavy−light spread"


def test_h6_falls_back_to_size_proxy_when_no_weight_change() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    h6 = verdicts.set_index("hid").loc["H6"]
    assert h6["key_label"] == "Q1Q2−Q4Q5 spread"


def test_h7_uses_us_sector_spread_when_provided() -> None:
    sector = pd.DataFrame(
        [
            {"market": "US", "bucket": "Tech", "asymmetry_index": 1.5, "n_events": 40},
            {"market": "US", "bucket": "Energy", "asymmetry_index": -0.3, "n_events": 25},
            {"market": "US", "bucket": "Health", "asymmetry_index": 0.6, "n_events": 30},
            {"market": "CN", "bucket": "Unknown", "asymmetry_index": 0.7, "n_events": 100},
        ]
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        heterogeneity_sector=sector,
    )
    h7 = verdicts.set_index("hid").loc["H7"]
    # spread = 1.5 - (-0.3) = 1.8 > 1.5 → 部分支持(因 CN 仍是 Unknown)
    assert h7["verdict"] == "部分支持"
    assert "Tech" in h7["metric_snapshot"]
    assert "Energy" in h7["metric_snapshot"]
    assert h7["key_label"] == "US sector spread"
    assert abs(float(h7["key_value"]) - 1.8) < 1e-6


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


def test_verdict_rows_carry_paper_ids_from_hypothesis_registry() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    assert {"paper_ids", "paper_count"}.issubset(verdicts.columns)
    by_hid = verdicts.set_index("hid")
    # H1 should have 4 supporting papers per registry
    assert int(by_hid.loc["H1", "paper_count"]) == 4
    h1_papers = str(by_hid.loc["H1", "paper_ids"]).split(" | ")
    assert "harris_gurel_1986" in h1_papers
    assert "denis_et_al_2003" in h1_papers
    # H6 should have 5 (its registry entry is the longest)
    assert int(by_hid.loc["H6", "paper_count"]) == 5


def test_verdict_rows_carry_structured_key_fields() -> None:
    bootstrap = {
        "cn_mean": 0.035,
        "us_mean": 0.020,
        "diff_mean": 0.015,
        "boot_p_value": 0.012,
        "boot_ci_low": 0.004,
        "boot_ci_high": 0.026,
        "n_cn": 130,
        "n_us": 310,
        "n_boot": 5000,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        pre_runup_bootstrap=bootstrap,
    )
    assert {"key_label", "key_value", "n_obs"}.issubset(verdicts.columns)
    h1 = verdicts.set_index("hid").loc["H1"]
    assert h1["key_label"] == "bootstrap p"
    assert abs(float(h1["key_value"]) - 0.012) < 1e-9
    assert int(h1["n_obs"]) == 440


def test_p_value_column_populated_for_p_gated_hypotheses() -> None:
    """The verdict CSV exposes a structured p_value column.

    H1 (bootstrap), H4 (gap_drift regression), H5 (limit_regression) are
    gated by a single p, so their p_value column is non-NaN and equals
    the gating p exactly. H2 / H3 / H6 / H7 (and any fallback path that
    isn't decided by a single p) keep p_value NaN. Sensitivity sweeps
    can therefore re-threshold without parsing ``evidence_summary``.
    """
    bootstrap = {
        "cn_mean": 0.035,
        "us_mean": 0.020,
        "diff_mean": 0.015,
        "boot_p_value": 0.012,
        "boot_ci_low": 0.004,
        "boot_ci_high": 0.026,
        "n_cn": 130,
        "n_us": 310,
        "n_boot": 5000,
    }
    gap_drift_regression = {
        "cn_coef": 0.025,
        "cn_se": 0.008,
        "cn_t": 3.1,
        "cn_p_value": 0.002,
        "gap_length_coef": 0.0001,
        "gap_length_p_value": 0.45,
        "n_obs": 430,
        "r_squared": 0.04,
    }
    limit_regression = {
        "limit_coef": 0.05,
        "limit_t": 4.1,
        "limit_p_value": 0.0002,
        "r_squared": 0.06,
        "n_obs": 118,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        pre_runup_bootstrap=bootstrap,
        gap_drift_regression=gap_drift_regression,
        limit_regression=limit_regression,
    )

    assert "p_value" in verdicts.columns

    by_hid = verdicts.set_index("hid")

    # p-gated paths: column equals the gating p exactly.
    assert abs(float(by_hid.loc["H1", "p_value"]) - 0.012) < 1e-12
    assert abs(float(by_hid.loc["H4", "p_value"]) - 0.002) < 1e-12
    assert abs(float(by_hid.loc["H5", "p_value"]) - 0.0002) < 1e-12

    # Spread / share / pending paths: p_value is NaN.
    for hid in ("H2", "H3", "H6", "H7"):
        assert pd.isna(by_hid.loc[hid, "p_value"]), (
            f"{hid} should have NaN p_value (its headline metric isn't a p)"
        )


def test_p_value_is_nan_for_h1_h4_fallback_when_no_regression_provided() -> None:
    """Without bootstrap / regression inputs the H1/H4 fallback paths
    pick a verdict from mean direction + t, not a single p, so
    p_value stays NaN — the column reflects "decided by a p" not
    "has any p anywhere"."""
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    by_hid = verdicts.set_index("hid")
    assert pd.isna(by_hid.loc["H1", "p_value"])
    assert pd.isna(by_hid.loc["H4", "p_value"])


def test_h1_upgrades_to_full_support_when_bootstrap_significant() -> None:
    bootstrap = {
        "cn_mean": 0.035,
        "us_mean": 0.020,
        "diff_mean": 0.015,
        "boot_p_value": 0.012,
        "boot_ci_low": 0.004,
        "boot_ci_high": 0.026,
        "n_cn": 130,
        "n_us": 310,
        "n_boot": 5000,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        pre_runup_bootstrap=bootstrap,
    )
    h1 = verdicts.set_index("hid").loc["H1"]
    assert h1["verdict"] == "支持"
    assert h1["confidence"] == "高"
    assert "bootstrap" in h1["metric_snapshot"]
    assert "0.012" in h1["metric_snapshot"]


def test_h1_downgrades_to_证据不足_when_bootstrap_p_above_threshold() -> None:
    bootstrap = {
        "cn_mean": 0.035,
        "us_mean": 0.020,
        "diff_mean": 0.015,
        "boot_p_value": 0.42,
        "boot_ci_low": -0.010,
        "boot_ci_high": 0.040,
        "n_cn": 130,
        "n_us": 310,
        "n_boot": 5000,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        pre_runup_bootstrap=bootstrap,
    )
    h1 = verdicts.set_index("hid").loc["H1"]
    assert h1["verdict"] == "证据不足"
    assert "0.42" in h1["metric_snapshot"]


def test_h1_falls_back_to_single_market_significance_when_bootstrap_missing() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    h1 = verdicts.set_index("hid").loc["H1"]
    # Fixture has CN > US directional + CN sig (t=3.1), legacy logic = 部分支持/中
    assert h1["verdict"] == "部分支持"
    assert h1["confidence"] == "中"
    assert "bootstrap" not in h1["metric_snapshot"]


def test_h4_upgrades_to_full_support_when_regression_significant() -> None:
    regression = {
        "cn_coef": 0.025,
        "cn_se": 0.008,
        "cn_t": 3.1,
        "cn_p_value": 0.002,
        "gap_length_coef": 0.0001,
        "gap_length_p_value": 0.45,
        "n_obs": 430,
        "r_squared": 0.04,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        gap_drift_regression=regression,
    )
    h4 = verdicts.set_index("hid").loc["H4"]
    assert h4["verdict"] == "支持"
    assert h4["confidence"] == "高"
    assert "0.002" in h4["metric_snapshot"]
    assert "regression" in h4["metric_snapshot"]


def test_h4_downgrades_to_证据不足_when_regression_insignificant() -> None:
    regression = {
        "cn_coef": 0.005,
        "cn_se": 0.012,
        "cn_t": 0.4,
        "cn_p_value": 0.69,
        "gap_length_coef": 0.0,
        "gap_length_p_value": 0.92,
        "n_obs": 400,
        "r_squared": 0.001,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        gap_drift_regression=regression,
    )
    h4 = verdicts.set_index("hid").loc["H4"]
    assert h4["verdict"] == "证据不足"
    assert "0.690" in h4["metric_snapshot"] or "0.69" in h4["metric_snapshot"]


def test_h4_falls_back_to_summary_logic_when_regression_missing() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    h4 = verdicts.set_index("hid").loc["H4"]
    # Fixture has CN > 0, US < 0, CN t=1.1 (not sig at 0.10) -> 部分支持/低
    assert h4["verdict"] == "部分支持"
    assert "regression" not in h4["metric_snapshot"]


def test_h3_requires_both_channels_when_table_provided() -> None:
    channel = pd.DataFrame(
        [
            # US announce: both sig
            {"market": "US", "event_phase": "announce", "turnover_coef": 0.03,
             "turnover_p": 0.0, "volume_coef": 8e6, "volume_p": 0.0,
             "turnover_sig": True, "volume_sig": True, "both_channels_sig": True},
            # CN effective: both sig
            {"market": "CN", "event_phase": "effective", "turnover_coef": 0.0014,
             "turnover_p": 0.007, "volume_coef": 5e6, "volume_p": 0.036,
             "turnover_sig": True, "volume_sig": True, "both_channels_sig": True},
            # CN announce: turnover only
            {"market": "CN", "event_phase": "announce", "turnover_coef": 0.001,
             "turnover_p": 0.03, "volume_coef": 1e7, "volume_p": 0.32,
             "turnover_sig": True, "volume_sig": False, "both_channels_sig": False},
            # US effective: neither
            {"market": "US", "event_phase": "effective", "turnover_coef": 0.002,
             "turnover_p": 0.13, "volume_coef": 3e5, "volume_p": 0.62,
             "turnover_sig": False, "volume_sig": False, "both_channels_sig": False},
        ]
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        channel_concentration=channel,
    )
    h3 = verdicts.set_index("hid").loc["H3"]
    assert h3["verdict"] == "支持"
    assert h3["confidence"] == "高"
    assert "2/4" in h3["metric_snapshot"]


def test_h3_partial_when_only_one_quadrant_has_both_channels() -> None:
    channel = pd.DataFrame(
        [
            {"market": "US", "event_phase": "announce", "turnover_coef": 0.03,
             "turnover_p": 0.0, "volume_coef": 8e6, "volume_p": 0.0,
             "turnover_sig": True, "volume_sig": True, "both_channels_sig": True},
            {"market": "CN", "event_phase": "effective", "turnover_coef": 0.0014,
             "turnover_p": 0.007, "volume_coef": 5e6, "volume_p": 0.13,
             "turnover_sig": True, "volume_sig": False, "both_channels_sig": False},
            {"market": "CN", "event_phase": "announce", "turnover_coef": 0.001,
             "turnover_p": 0.03, "volume_coef": 1e7, "volume_p": 0.32,
             "turnover_sig": True, "volume_sig": False, "both_channels_sig": False},
            {"market": "US", "event_phase": "effective", "turnover_coef": 0.002,
             "turnover_p": 0.13, "volume_coef": 3e5, "volume_p": 0.62,
             "turnover_sig": False, "volume_sig": False, "both_channels_sig": False},
        ]
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        channel_concentration=channel,
    )
    h3 = verdicts.set_index("hid").loc["H3"]
    assert h3["verdict"] == "部分支持"
    assert "1/4" in h3["metric_snapshot"]


def test_h5_upgrades_to_full_support_when_limit_regression_significant() -> None:
    limit_regression = {
        "limit_coef": 0.034,
        "limit_se": 0.009,
        "limit_t": 3.8,
        "limit_p_value": 0.0002,
        "n_obs": 118,
        "r_squared": 0.12,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        limit_regression=limit_regression,
    )
    h5 = verdicts.set_index("hid").loc["H5"]
    assert h5["verdict"] == "支持"
    assert h5["confidence"] == "高"
    assert "limit_coef" in h5["metric_snapshot"]


def test_h5_证据不足_when_limit_regression_insignificant() -> None:
    limit_regression = {
        "limit_coef": 0.005,
        "limit_se": 0.012,
        "limit_t": 0.4,
        "limit_p_value": 0.62,
        "n_obs": 118,
        "r_squared": 0.001,
    }
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        limit_regression=limit_regression,
    )
    h5 = verdicts.set_index("hid").loc["H5"]
    assert h5["verdict"] == "证据不足"
    assert "0.62" in h5["metric_snapshot"]


def test_h5_falls_back_to_summary_logic_when_regression_missing() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    h5 = verdicts.set_index("hid").loc["H5"]
    # legacy logic with fixture: 证据不足
    assert h5["verdict"] == "证据不足"
    assert "limit_coef" not in h5["metric_snapshot"]


def test_h3_falls_back_to_single_channel_logic_when_table_missing() -> None:
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    h3 = verdicts.set_index("hid").loc["H3"]
    # Original fixture-driven verdict
    assert h3["verdict"] == "支持"
    assert "channel" not in h3["metric_snapshot"]


def test_render_paper_verdict_section_summarises_aggregates_and_each_hid(tmp_path) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        render_paper_verdict_section,
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    text = render_paper_verdict_section(verdicts)
    assert text.startswith("## 假说裁决叙述")
    # aggregate header mentions verdict counts
    assert "支持" in text or "部分支持" in text
    # each hid surfaces as a section heading
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7"):
        assert f"### {hid}" in text


def test_export_paper_verdict_section_writes_markdown(tmp_path) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        export_paper_verdict_section,
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    out = tmp_path / "verdict.md"
    result = export_paper_verdict_section(verdicts, output_path=out)
    assert result == out
    content = out.read_text()
    assert "## 假说裁决叙述" in content
    assert "H1" in content and "H7" in content


def test_render_paper_verdict_section_includes_sample_and_methods_when_event_counts_provided(
    tmp_path,
) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        render_paper_verdict_section,
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    event_counts = pd.DataFrame(
        [
            {"market": "CN", "announce_year": 2020, "inclusion": 1, "n_events": 21},
            {"market": "CN", "announce_year": 2024, "inclusion": 1, "n_events": 28},
            {"market": "US", "announce_year": 2020, "inclusion": 1, "n_events": 30},
            {"market": "US", "announce_year": 2024, "inclusion": 1, "n_events": 19},
        ]
    )
    text = render_paper_verdict_section(verdicts, event_counts=event_counts)
    # sample summary preamble before verdict-by-verdict block
    assert "样本概述" in text
    assert "方法概述" in text
    assert "限制与稳健性" in text
    # H2 is 待补数据 in fixture → should appear in limitations list
    assert "H2" in text


def test_render_paper_verdict_section_skips_extras_without_event_counts() -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        render_paper_verdict_section,
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    text = render_paper_verdict_section(verdicts)
    assert "样本概述" not in text
    assert "限制与稳健性" not in text


def test_render_paper_verdict_section_handles_empty_input() -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        render_paper_verdict_section,
    )
    text = render_paper_verdict_section(pd.DataFrame())
    assert "暂无 verdict 数据" in text


def test_export_hypothesis_verdicts_tex_writes_booktabs_table(tmp_path) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        export_hypothesis_verdicts_tex,
    )
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
    )
    out = export_hypothesis_verdicts_tex(verdicts, output_dir=tmp_path)
    assert out.name == "cma_hypothesis_verdicts.tex"
    content = out.read_text()
    assert r"\begin{tabular}" in content
    assert r"\toprule" in content
    assert r"\bottomrule" in content
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6"):
        assert hid in content


def test_export_hypothesis_verdicts_tex_escapes_latex_metacharacters(tmp_path) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry.verdicts import (
        export_hypothesis_verdicts_tex,
    )
    # craft a verdict with special chars in key_label
    verdicts = pd.DataFrame(
        [
            {
                "hid": "H99",
                "name_cn": "Probe & test",
                "verdict": "支持",
                "confidence": "高",
                "evidence_summary": "",
                "metric_snapshot": "",
                "next_step": "",
                "evidence_refs": "",
                "key_label": "p_value & R²",
                "key_value": 0.05,
                "n_obs": 100,
            }
        ]
    )
    out = export_hypothesis_verdicts_tex(verdicts, output_dir=tmp_path)
    text = out.read_text()
    # & escaped
    assert r"Probe \& test" in text
    # _ escaped
    assert r"p\_value" in text


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
    assert len(exported) == 7
    assert exported.loc[exported["hid"] == "H3", "verdict"].iloc[0] == "支持"


# ── significance_level parameterization (CMA --threshold) ──────────────


def _h4_boundary_regression(p_value: float) -> dict[str, object]:
    """Helper: build a gap_drift_regression input with the given p_value
    so we can probe how H4 verdict shifts as significance_level moves."""
    return {
        "cn_coef": 0.025,
        "cn_se": 0.008,
        "cn_t": 3.1,
        "cn_p_value": p_value,
        "gap_length_coef": 0.0001,
        "gap_length_p_value": 0.45,
        "n_obs": 430,
        "r_squared": 0.04,
    }


def test_default_significance_level_matches_pre_parameterized_behavior() -> None:
    """Sanity: not passing significance_level reproduces the historical
    0.05/0.10 double-tier behavior. p=0.07 → 部分支持 (was sig at 0.10
    only; flipped between strict 0.05 and default 0.10)."""
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        gap_drift_regression=_h4_boundary_regression(0.07),
    )
    h4 = verdicts.set_index("hid").loc["H4"]
    assert h4["verdict"] == "部分支持"


def test_strict_threshold_flips_boundary_p_to_evidence_insufficient() -> None:
    """level=0.05 makes p=0.07 fail the boundary check entirely (since
    0.07 >= 0.05) → '证据不足'. The same input at default 0.10 yielded
    '部分支持' above; this is exactly the kind of flip a referee asks
    about."""
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        gap_drift_regression=_h4_boundary_regression(0.07),
        significance_level=0.05,
    )
    h4 = verdicts.set_index("hid").loc["H4"]
    assert h4["verdict"] == "证据不足"


def test_loose_threshold_promotes_p_007_to_full_support() -> None:
    """level=0.20 → strict cutoff is 0.10, so p=0.07 < 0.10 ⇒ '支持'."""
    verdicts = build_hypothesis_verdicts(
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        gap_drift_regression=_h4_boundary_regression(0.07),
        significance_level=0.20,
    )
    h4 = verdicts.set_index("hid").loc["H4"]
    assert h4["verdict"] == "支持"
    assert h4["confidence"] == "高"


def test_h2_h3_h6_h7_unaffected_by_significance_level_change() -> None:
    """Spread / share / direction-driven hypotheses keep their verdict
    regardless of significance_level — locks the documented contract."""
    bootstrap = {
        "cn_mean": 0.035,
        "us_mean": 0.020,
        "diff_mean": 0.015,
        "boot_p_value": 0.012,
        "boot_ci_low": 0.004,
        "boot_ci_high": 0.026,
        "n_cn": 130,
        "n_us": 310,
        "n_boot": 5000,
    }
    common = {
        "gap_summary": _gap_summary(),
        "mechanism_panel": _mechanism_panel(),
        "heterogeneity_size": _heterogeneity_size(),
        "time_series_rolling": _rolling(),
        "pre_runup_bootstrap": bootstrap,
    }
    default = build_hypothesis_verdicts(**common).set_index("hid")
    strict = build_hypothesis_verdicts(**common, significance_level=0.01).set_index("hid")
    loose = build_hypothesis_verdicts(**common, significance_level=0.30).set_index("hid")
    for hid in ("H2", "H3", "H6", "H7"):
        assert default.loc[hid, "verdict"] == strict.loc[hid, "verdict"], (
            f"{hid} should be invariant to threshold (default vs strict 0.01)"
        )
        assert default.loc[hid, "verdict"] == loose.loc[hid, "verdict"], (
            f"{hid} should be invariant to threshold (default vs loose 0.30)"
        )


def test_export_hypothesis_verdicts_threads_significance_level_through(tmp_path) -> None:
    """The CLI-visible export wrapper also honours significance_level
    so `index-inclusion-cma --threshold 0.05` regenerates the CSV at
    that cutoff."""
    out = export_hypothesis_verdicts(
        output_dir=tmp_path,
        gap_summary=_gap_summary(),
        mechanism_panel=_mechanism_panel(),
        heterogeneity_size=_heterogeneity_size(),
        time_series_rolling=_rolling(),
        gap_drift_regression=_h4_boundary_regression(0.07),
        significance_level=0.05,
    )
    csv = pd.read_csv(out)
    assert csv.loc[csv["hid"] == "H4", "verdict"].iloc[0] == "证据不足"
