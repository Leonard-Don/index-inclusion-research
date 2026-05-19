"""Unit tests for the post-hoc power analysis module + CLI.

Covers:

- Standard sanity (n=20 p=0.6 vs 0.5, n=30 d=0.5).
- Edge cases (effect size 0 → power = α; n=1 / n=2 boundaries).
- MDE inversion round-trip (power at returned MDE matches target).
- H3 specific (n=4 p=0.75 vs 0.5 → power < 0.50 documented limit;
  exact binomial returns 0 since no α=0.05 rejection region exists).
- H6 specific (n=67 d=0.2 → power in expected 0.30-0.40 band).
- Bootstrap convergence (300-bootstrap stable around analytical power).
- CLI smoke test (writes CSV + markdown twin to tmp_path).
- Public re-exports load through the analysis package.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from index_inclusion_research import power_analysis as cli_module
from index_inclusion_research.analysis import power_analysis as pa

# ---------------------------------------------------------------------------
# Standard sanity tests
# ---------------------------------------------------------------------------


def test_proportion_power_standard_case() -> None:
    """n=20, p0=0.5, p1=0.6: roughly 14% two-sided power, 22% one-sided.

    Anchor values come from the same normal-approx formula textbooks
    use; we pin the numbers to 3 decimals to detect arithmetic drift.
    """
    r2 = pa.binomial_proportion_power(20, 0.5, 0.6, alternative="two-sided")
    r1 = pa.binomial_proportion_power(20, 0.5, 0.6, alternative="greater")
    assert r2.power == pytest.approx(0.1402, abs=0.005)
    assert r1.power == pytest.approx(0.2219, abs=0.005)


def test_t_test_power_standard_case() -> None:
    """n=30, d=0.5, α=0.05 two-sided → ~75% power (Cohen 1988 §2.3 row)."""
    r = pa.t_test_power(30, 0.5)
    assert r.power == pytest.approx(0.754, abs=0.005)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_zero_effect_power_equals_alpha_t() -> None:
    """t-test with d=0 must return exactly α (correct null behaviour)."""
    for n in (10, 50, 200):
        r = pa.t_test_power(n, 0.0, alpha=0.05)
        assert r.power == pytest.approx(0.05, abs=1e-6)


def test_zero_effect_power_equals_alpha_proportion() -> None:
    """Proportion z-test with p1=p0 must return α (correct null behaviour)."""
    r = pa.binomial_proportion_power(100, 0.5, 0.5, alpha=0.05)
    assert r.power == pytest.approx(0.05, abs=1e-6)


def test_n_2_is_minimum_for_t_test() -> None:
    """t-test requires n>=2 (df>=1); n=1 raises ValueError."""
    with pytest.raises(ValueError):
        pa.t_test_power(1, 0.5)


def test_n_1_proportion_returns_alpha_or_zero() -> None:
    """n=1 proportion power should not crash; effect=0 → power=α."""
    r = pa.binomial_proportion_power(1, 0.5, 0.5)
    assert 0.0 <= r.power <= 1.0
    assert r.power == pytest.approx(0.05, abs=1e-6)


# ---------------------------------------------------------------------------
# MDE inversion round-trip
# ---------------------------------------------------------------------------


def test_mde_at_power_round_trip_t() -> None:
    """At returned MDE, power should equal the target within tolerance."""
    for n in (10, 30, 67):
        d_star = pa.mde_at_power(n, test="t", target_power=0.80)
        actual = pa.t_test_power(n, d_star).power
        assert actual == pytest.approx(0.80, abs=0.01)


def test_mde_at_power_round_trip_proportion() -> None:
    """Same round-trip for the proportion test (one-sided, against p0=0.5)."""
    n = 50
    gap = pa.mde_at_power(
        n, test="proportion", target_power=0.80, alternative="greater"
    )
    p1 = 0.5 + gap
    actual = pa.binomial_proportion_power(
        n, 0.5, p1, alternative="greater"
    ).power
    assert actual == pytest.approx(0.80, abs=0.01)


# ---------------------------------------------------------------------------
# H3 specific
# ---------------------------------------------------------------------------


def test_h3_observed_power_below_50_percent() -> None:
    """The whole point: n=4, observed 75% hit rate → power < 0.50."""
    report = pa.compute_h3_power(observed_hit_rate=0.75, n=4)
    assert report.power_at_observed < 0.50
    # Normal-approx-power lands around 13-14% — pin to a band.
    assert 0.10 < report.power_at_observed < 0.20


def test_h3_exact_binomial_no_rejection_region_at_alpha_05() -> None:
    """At n=4, α=0.05 two-sided exact binomial cannot reject — power=0."""
    r = pa.exact_binomial_power(4, 0.5, 0.75, alpha=0.05, alternative="two-sided")
    assert r.power == 0.0
    assert "no rejection region" in r.detail


def test_h3_bayes_posterior_above_one_half() -> None:
    """3/4 hits + uniform prior → P(p > 0.6) sits around 0.66 (Beta tail)."""
    p_post = pa.beta_posterior_probability_above(3, 4, 0.60)
    assert 0.60 < p_post < 0.75


def test_h3_interpretation_flags_underpowered() -> None:
    """The interpretation string must say so when power < 0.30."""
    report = pa.compute_h3_power()
    assert "欠功效" in report.interpretation
    assert "supplementary" in report.interpretation


# ---------------------------------------------------------------------------
# H6 specific
# ---------------------------------------------------------------------------


def test_h6_small_effect_power_in_band() -> None:
    """n=67, d=0.20 (small) → power around 0.36 — pin the band."""
    r = pa.t_test_power(67, 0.20)
    assert 0.30 < r.power < 0.42


def test_h6_large_effect_power_near_1() -> None:
    """n=67, d=0.80 (large) → power ≈ 1.0 (no rounding to 1.0 exactly)."""
    r = pa.t_test_power(67, 0.80)
    assert r.power > 0.99


def test_h6_report_observes_negative_d_with_full_bucket_inputs() -> None:
    """When real bucket means + SD are supplied, the report flags
    the direction mismatch flag in the interpretation."""
    report = pa.compute_h6_power(
        observed_spread=-0.023,
        heavy_jump_mean=0.011,
        light_jump_mean=0.034,
        bucket_sd=0.032,
        n=67,
    )
    assert report.observed_effect < 0
    assert "方向" in report.interpretation


# ---------------------------------------------------------------------------
# Bootstrap convergence
# ---------------------------------------------------------------------------


def test_bootstrap_observed_power_converges() -> None:
    """Bootstrap power on data with known signal converges near the
    analytical power for the same effect size."""
    rng = np.random.default_rng(0)
    # Generate data with mean 0.5, SD 1.0, n=30 (Cohen's d ≈ 0.5)
    data = rng.normal(loc=0.5, scale=1.0, size=30)
    bs = pa.bootstrap_observed_power(
        data, null_value=0.0, alpha=0.05, n_bootstrap=600, seed=42
    )
    # The analytical power at d=0.5, n=30 is ~0.75; bootstrap on this
    # sample varies but should sit in [0.55, 0.95].
    assert 0.55 < bs.rejection_rate < 0.95


def test_bootstrap_handles_n1_gracefully() -> None:
    """Bootstrap on single observation returns NaN, not a crash."""
    bs = pa.bootstrap_observed_power([1.0], null_value=0.0, n_bootstrap=50)
    assert math.isnan(bs.rejection_rate)


# ---------------------------------------------------------------------------
# Public re-exports
# ---------------------------------------------------------------------------


def test_analysis_package_reexports() -> None:
    """The public analysis package surfaces the new symbols so other
    modules can ``from index_inclusion_research.analysis import ...`` them."""
    from index_inclusion_research import analysis

    for symbol in (
        "binomial_proportion_power",
        "t_test_power",
        "mde_at_power",
        "bootstrap_observed_power",
        "compute_h3_power",
        "compute_h6_power",
        "PowerResult",
        "HypothesisPowerReport",
        "BootstrapPowerResult",
    ):
        assert hasattr(analysis, symbol), symbol
        assert symbol in analysis.__all__, symbol


# ---------------------------------------------------------------------------
# Engine diagnostics / edge-case regressions
# ---------------------------------------------------------------------------


def _engine_report(
    *,
    hid: str = "H1",
    engine: str,
    power: float,
    observed_effect: float = 0.2,
    bootstrap_p_value: float | None = None,
    cohens_d: float | None = None,
) -> pa.HypothesisPowerReport:
    extras: dict[str, float] = {}
    if bootstrap_p_value is not None:
        extras["bootstrap_p_value"] = bootstrap_p_value
    if cohens_d is not None:
        extras["cohens_d"] = cohens_d
    return pa.HypothesisPowerReport(
        hid=hid,
        name_cn="测试假说",
        n_obs=10,
        test_family="test",
        observed_effect=observed_effect,
        observed_effect_label="effect",
        alpha=0.05,
        power_at_observed=power,
        mde_at_80_power=0.5,
        mde_label="mde",
        interpretation="test",
        extras=extras,
        engine=engine,
    )


def test_h1_engine_bootstrap_missing_counts_uses_default_n(tmp_path: Path) -> None:
    """Partial H1 bootstrap CSVs should not turn missing counts into n=0."""
    path = tmp_path / "bootstrap.csv"
    pd.DataFrame(
        [
            {
                "diff_mean": 0.02,
                "boot_ci_low": -0.01,
                "boot_ci_high": 0.05,
                "boot_p_value": 0.04,
                # n_cn / n_us intentionally absent: stripped checkouts may
                # carry old CSVs before the count columns existed.
            }
        ]
    ).to_csv(path, index=False)

    inputs = cli_module._h1_inputs_from_engine_bootstrap("adjusted", bootstrap_csv=path)
    assert inputs is not None
    assert "n_total" not in inputs
    report = pa.compute_h1_power_per_engine({"adjusted": inputs})["adjusted"]
    assert report.n_obs == 436

    partial_path = tmp_path / "partial_bootstrap.csv"
    pd.DataFrame(
        [
            {
                "diff_mean": 0.02,
                "boot_ci_low": -0.01,
                "boot_ci_high": 0.05,
                "boot_p_value": 0.04,
                "n_cn": 123,
            }
        ]
    ).to_csv(partial_path, index=False)
    partial = cli_module._h1_inputs_from_engine_bootstrap(
        "adjusted", bootstrap_csv=partial_path
    )
    assert partial is not None
    assert "n_total" not in partial


def test_h2_rolling_n_combined_uses_delta_count(tmp_path: Path) -> None:
    """H2 power is based on year-over-year deltas, not finite level rows."""
    path = tmp_path / "rolling.csv"
    pd.DataFrame(
        [
            {"market": "US", "event_phase": "effective", "window_end_year": 2020, "car_mean": 0.03},
            {"market": "US", "event_phase": "effective", "window_end_year": 2021, "car_mean": 0.02},
            {"market": "US", "event_phase": "effective", "window_end_year": 2022, "car_mean": 0.01},
            {"market": "CN", "event_phase": "effective", "window_end_year": 2021, "car_mean": 0.04},
            {"market": "CN", "event_phase": "effective", "window_end_year": 2022, "car_mean": 0.03},
        ]
    ).to_csv(path, index=False)

    inputs = cli_module._h2_inputs_from_engine_rolling("market", rolling_csv=path)
    assert inputs is not None
    assert inputs["n_combined"] == 3.0
    assert len(inputs["deltas"]) == 3
    report = pa.compute_h2_power_per_engine({"market": {"deltas": inputs["deltas"]}})[
        "market"
    ]
    assert report.n_obs == 3


def test_build_engine_flip_diagnoses_skips_single_engine() -> None:
    """A one-engine row is missing comparison data, not a flip diagnosis."""
    reports = [_engine_report(engine="adjusted", power=0.9, bootstrap_p_value=0.01)]
    assert cli_module.build_engine_flip_diagnoses(reports) == []


def test_diagnose_engine_flip_no_flip_when_both_engines_agree() -> None:
    """High power alone must not fabricate a methodology-driven flip."""
    diag = pa.diagnose_engine_flip(
        "H1",
        {
            "adjusted": _engine_report(engine="adjusted", power=0.9, bootstrap_p_value=0.01),
            "market": _engine_report(engine="market", power=0.92, bootstrap_p_value=0.02),
        },
    )
    assert diag.classification == "NO_FLIP"
    assert "no adjusted-vs-market verdict flip" in diag.narrative


def test_compare_h1_h2_accepts_delta_vector_inputs() -> None:
    """Typed callers can pass H2's supported deltas list without casts."""
    comparison = pa.compare_h1_h2_across_engines(
        {
            "adjusted": {
                "diff_mean": 0.02,
                "boot_ci_low": -0.01,
                "boot_ci_high": 0.05,
                "boot_p_value": 0.04,
            },
            "market": {
                "diff_mean": 0.01,
                "boot_ci_low": -0.02,
                "boot_ci_high": 0.04,
                "boot_p_value": 0.40,
            },
        },
        {
            "adjusted": {"deltas": [0.03, -0.01, 0.02, -0.02]},
            "market": {"deltas": [-0.04, -0.02, -0.03, -0.01]},
        },
    )
    assert set(comparison.h2_per_engine) == {"adjusted", "market"}
    assert comparison.h2_per_engine["adjusted"].n_obs == 4


# ---------------------------------------------------------------------------
# CLI smoke
# ---------------------------------------------------------------------------


def _write_verdicts_csv(path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "hid": "H3",
                "name_cn": "散户 vs 机构结构",
                "key_value": 0.75,
                "n_obs": 4,
            },
            {
                "hid": "H6",
                "name_cn": "指数权重可预测性",
                "key_value": -0.019,
                "n_obs": 67,
            },
        ]
    )
    df.to_csv(path, index=False)


def test_cli_writes_csv_and_markdown(tmp_path: Path, monkeypatch) -> None:
    """`main()` writes both a CSV and a markdown twin to disk."""
    # Build a minimal verdicts CSV that the CLI will read.
    fake_root = tmp_path / "root"
    real_tables = fake_root / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _write_verdicts_csv(real_tables / "cma_hypothesis_verdicts.csv")
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fake_root))

    rc = cli_module.main([])
    assert rc == 0
    csv_path = real_tables / "power_analysis_report.csv"
    md_path = real_tables / "power_analysis_report.md"
    assert csv_path.exists()
    assert md_path.exists()
    df = pd.read_csv(csv_path)
    assert set(df["hid"].tolist()) == {"H3", "H6"}
    h3 = df.loc[df["hid"] == "H3"].iloc[0]
    h6 = df.loc[df["hid"] == "H6"].iloc[0]
    assert 0.10 < h3["power_at_observed"] < 0.20
    # H6 falls back to r²=0.033 derivation when panel CSVs are absent.
    assert 0.0 < h6["power_at_observed"] < 1.0
    markdown = md_path.read_text(encoding="utf-8")
    assert "## 1. 功效一览表" in markdown
    assert "H3" in markdown and "H6" in markdown
