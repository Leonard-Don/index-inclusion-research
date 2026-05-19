"""Unit tests for the H4 / H5 power-analysis extensions.

Covers:

- Regression-coef power matches statsmodels' ``TTestPower`` gold standard.
- MDE inversion round-trip (power at returned MDE matches target).
- Degenerate edge cases — non-positive SE / very small n / very large n.
- H4 specific (n=436, coef≈+0.006 → power ~0.10, MDE ≈ 4-5× coef).
- H5 specific (n=936, coef≈+0.155, p=0.008 → power in 0.7-0.8 band,
  MDE roughly equal to the observed coef).
- CLI smoke (CSV writes new H4 / H5 rows; the verdicts-CSV fallback
  path is silently skipped when the regression CSVs are missing AND
  the verdicts CSV lacks a per-row SE column).
- Analysis package re-exports the new symbols.

The "synthetic n with known effect → power matches scipy/statsmodels"
contract is tested by anchoring our :func:`regression_coef_power` to
:class:`statsmodels.stats.power.TTestPower` at three sample sizes —
identical answers (to 3 decimals) to detect arithmetic drift across
SciPy / statsmodels versions.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest
from statsmodels.stats.power import TTestPower

from index_inclusion_research import power_analysis as cli_module
from index_inclusion_research.analysis import power_analysis as pa

# ---------------------------------------------------------------------------
# Regression-coef power: agreement with statsmodels gold standard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "n, coef, se, n_covariates",
    [
        # H4 actual numbers
        (436, 0.006111113589099461, 0.00989025033185132, 2),
        # H5 actual numbers
        (936, 0.15489137456100996, 0.05862470933497476, 1),
        # Larger n, mid-sized effect
        (200, 0.10, 0.05, 1),
        # Boundary: very small n
        (10, 0.20, 0.10, 1),
    ],
)
def test_regression_coef_power_matches_statsmodels(
    n: int, coef: float, se: float, n_covariates: int
) -> None:
    """Power from the regression-coef formula matches statsmodels.

    To compare apples-to-apples with statsmodels' ``TTestPower`` (which
    assumes df = n - 1), drive our function with the same effective
    df by setting ``n_covariates=0`` — the resulting non-central t
    answer must then match to 4 decimals. For the actual paper
    ``n_covariates`` (the regression's real covariate count, k), the
    difference is < 0.005 for n in the hundreds (paper regime).
    """
    # Apples-to-apples: same df ⇒ same answer to floating-point.
    actual_same_df = pa.regression_coef_power(
        coef, se, n=n, n_covariates=0, alpha=0.05
    ).power
    ncp = coef / se
    d_effective = ncp / math.sqrt(n)
    gold = TTestPower().power(
        effect_size=d_effective, nobs=n, alpha=0.05, alternative="two-sided"
    )
    assert actual_same_df == pytest.approx(gold, abs=1e-6)

    # Paper-realistic: our k covariates → tiny df shrinkage. The gap
    # is < 0.015 at n=10, < 0.001 by n=200 and ≈ 0 by n=400.
    actual_paper = pa.regression_coef_power(
        coef, se, n=n, n_covariates=n_covariates, alpha=0.05
    ).power
    tol = 0.015 if n < 20 else 0.005
    assert actual_paper == pytest.approx(gold, abs=tol)


def test_regression_coef_power_zero_effect_equals_alpha() -> None:
    """coef=0 must return power = α exactly (correct null behaviour)."""
    r = pa.regression_coef_power(0.0, 0.05, n=200, alpha=0.05)
    assert r.power == pytest.approx(0.05, abs=1e-6)


# ---------------------------------------------------------------------------
# Regression-coef power: degenerate-input handling
# ---------------------------------------------------------------------------


def test_regression_coef_power_non_positive_se_returns_nan() -> None:
    """SE ≤ 0 ⇒ ``nan`` power with descriptive ``detail`` string."""
    r = pa.regression_coef_power(0.1, 0.0, n=200)
    assert math.isnan(r.power)
    assert "non-positive" in r.detail.lower() or "non-finite" in r.detail.lower()

    r_neg = pa.regression_coef_power(0.1, -0.01, n=200)
    assert math.isnan(r_neg.power)


def test_regression_coef_power_n_minimum() -> None:
    """``n < 2`` must raise (need df >= 1)."""
    with pytest.raises(ValueError):
        pa.regression_coef_power(0.1, 0.05, n=1)


# ---------------------------------------------------------------------------
# MDE inversion: round-trip + degenerate cases
# ---------------------------------------------------------------------------


def test_mde_regression_coef_round_trip() -> None:
    """At returned MDE, power should equal the target within tolerance."""
    for n, se in ((50, 0.05), (200, 0.02), (1000, 0.01)):
        mde = pa.mde_regression_coef(se, n=n, target_power=0.80, alpha=0.05)
        actual = pa.regression_coef_power(mde, se, n=n, alpha=0.05).power
        assert actual == pytest.approx(0.80, abs=0.01)


def test_mde_regression_coef_degenerate_n4() -> None:
    """n=4 round-trip: even at n=4 the inversion must converge cleanly
    (no NaN, no infinite loop) and produce a large but finite MDE."""
    mde = pa.mde_regression_coef(0.05, n=4, target_power=0.80, alpha=0.05)
    assert math.isfinite(mde)
    assert mde > 0
    # n=4 has tiny df; MDE for 80% power must be large relative to SE.
    assert mde >= 0.10  # at least 2× SE


def test_mde_regression_coef_degenerate_n1000() -> None:
    """n=1000: with huge df the MDE collapses to ~ (z_{1-α/2}+z_β)·SE."""
    se = 0.01
    mde = pa.mde_regression_coef(se, n=1000, target_power=0.80, alpha=0.05)
    # Closed-form normal-approx: (1.96 + 0.8416) * 0.01 ≈ 0.0280
    assert mde == pytest.approx(0.0280, abs=0.001)


def test_mde_regression_coef_non_finite_se_returns_nan() -> None:
    """SE ≤ 0 ⇒ ``nan`` MDE (matches the power-side behaviour)."""
    assert math.isnan(pa.mde_regression_coef(0.0, n=200, target_power=0.80))
    assert math.isnan(pa.mde_regression_coef(-0.05, n=200, target_power=0.80))
    assert math.isnan(
        pa.mde_regression_coef(float("nan"), n=200, target_power=0.80)
    )


# ---------------------------------------------------------------------------
# H4 specific
# ---------------------------------------------------------------------------


def test_h4_observed_power_severely_underpowered() -> None:
    """The whole point: H4's observed coef is only ~0.6 SE → power < 0.20.

    n=436 sounds 'large' but observed coef = +0.0061 vs SE = 0.0099
    ⇒ t = +0.62 — far from significance. Post-hoc power must reflect
    that, not the nominal n.
    """
    report = pa.compute_h4_power()
    assert report.hid == "H4"
    assert report.n_obs == 436
    assert 0.05 < report.power_at_observed < 0.20


def test_h4_interpretation_flags_underpowered() -> None:
    """The interpretation must say so when power < 0.30."""
    report = pa.compute_h4_power()
    assert "欠功效" in report.interpretation
    assert "supplementary" in report.interpretation


def test_h4_mde_is_multiple_of_observed_coef() -> None:
    """MDE@80% should be several× the observed coef (4-5×)."""
    report = pa.compute_h4_power()
    coef = report.extras["coef_observed"]
    mde = report.mde_at_80_power
    assert mde > 4.0 * abs(coef)
    assert mde < 6.0 * abs(coef)


def test_h4_extras_carry_full_regression_payload() -> None:
    """Extras must round-trip coef / SE / t / p for paper integrity."""
    report = pa.compute_h4_power(
        coef=0.006, se=0.01, p_value=0.55, n=436
    )
    assert report.extras["coef_observed"] == pytest.approx(0.006)
    assert report.extras["se_observed"] == pytest.approx(0.01)
    assert report.extras["t_observed"] == pytest.approx(0.6)
    assert report.extras["p_value_observed"] == pytest.approx(0.55)


# ---------------------------------------------------------------------------
# H5 specific
# ---------------------------------------------------------------------------


def test_h5_observed_power_in_moderate_band() -> None:
    """H5 (n=936, coef=+0.155, p=0.008) ⇒ post-hoc power ~0.75 — moderate.

    The verdict 'supportive' is anchored on the p-value, but the
    post-hoc power lands *just below* the conventional 0.80 threshold,
    so the report must surface that ambiguity rather than rounding up
    to 'powered'.
    """
    report = pa.compute_h5_power()
    assert report.hid == "H5"
    assert report.n_obs == 936
    assert 0.70 < report.power_at_observed < 0.80


def test_h5_mde_near_observed_coef() -> None:
    """H5's observed coef sits near the MDE, by construction (p=0.008)."""
    report = pa.compute_h5_power()
    coef = report.extras["coef_observed"]
    mde = report.mde_at_80_power
    # MDE/coef should be in [1.0, 1.3] for H5's numbers
    ratio = mde / abs(coef)
    assert 1.0 < ratio < 1.3


def test_h5_extras_carry_regression_payload() -> None:
    """H5 extras: coef / se / t / p / n_covariates all round-tripped."""
    report = pa.compute_h5_power(
        coef=0.15, se=0.06, p_value=0.012, n=936
    )
    assert report.extras["coef_observed"] == pytest.approx(0.15)
    assert report.extras["se_observed"] == pytest.approx(0.06)
    assert report.extras["t_observed"] == pytest.approx(2.5)
    assert report.extras["p_value_observed"] == pytest.approx(0.012)
    assert report.extras["n_covariates"] == pytest.approx(1.0)


def test_h5_synthetic_large_coef_powers_above_80() -> None:
    """Synthetic: doubling the H5 coef should push power past 0.99."""
    report = pa.compute_h5_power(
        coef=0.30, se=0.06, p_value=0.0001, n=936
    )
    assert report.power_at_observed > 0.99


# ---------------------------------------------------------------------------
# CLI input loaders
# ---------------------------------------------------------------------------


def test_h4_input_loader_reads_regression_csv(tmp_path: Path) -> None:
    """``_h4_inputs_from_regression`` parses the headline row from the CSV."""
    csv = tmp_path / "h4.csv"
    pd.DataFrame(
        [
            {
                "cn_coef": 0.0061,
                "cn_se": 0.0099,
                "cn_t": 0.62,
                "cn_p_value": 0.5366,
                "gap_length_coef": 0.0007,
                "gap_length_p_value": 0.318,
                "n_obs": 436,
                "r_squared": 0.0094,
            }
        ]
    ).to_csv(csv, index=False)
    inputs = cli_module._h4_inputs_from_regression(regression_csv=csv)
    assert inputs is not None
    assert inputs["coef"] == pytest.approx(0.0061)
    assert inputs["se"] == pytest.approx(0.0099)
    assert inputs["p_value"] == pytest.approx(0.5366)
    assert inputs["n"] == 436


def test_h5_input_loader_reads_regression_csv(tmp_path: Path) -> None:
    """``_h5_inputs_from_regression`` parses the headline row from the CSV."""
    csv = tmp_path / "h5.csv"
    pd.DataFrame(
        [
            {
                "limit_coef": 0.1549,
                "limit_se": 0.0586,
                "limit_t": 2.64,
                "limit_p_value": 0.0082,
                "n_obs": 936,
                "r_squared": 0.011,
            }
        ]
    ).to_csv(csv, index=False)
    inputs = cli_module._h5_inputs_from_regression(regression_csv=csv)
    assert inputs is not None
    assert inputs["coef"] == pytest.approx(0.1549)
    assert inputs["se"] == pytest.approx(0.0586)
    assert inputs["n"] == 936


def test_h4_input_loader_returns_none_for_missing_csv(tmp_path: Path) -> None:
    """A missing regression CSV AND missing verdicts CSV ⇒ ``None``."""
    missing = tmp_path / "does_not_exist.csv"
    assert (
        cli_module._h4_inputs_from_regression(regression_csv=missing) is None
    )


# ---------------------------------------------------------------------------
# CLI smoke (write CSV + markdown with H4 / H5 rows present)
# ---------------------------------------------------------------------------


def _write_h4_h5_csvs(tables_dir: Path) -> None:
    pd.DataFrame(
        [
            {
                "cn_coef": 0.006111113589099461,
                "cn_se": 0.00989025033185132,
                "cn_t": 0.6178927109073025,
                "cn_p_value": 0.5366460641588349,
                "gap_length_coef": 0.0006931751241707541,
                "gap_length_p_value": 0.3181884479009437,
                "n_obs": 436,
                "r_squared": 0.009372863700574108,
            }
        ]
    ).to_csv(tables_dir / "cma_gap_drift_market_regression.csv", index=False)
    pd.DataFrame(
        [
            {
                "limit_coef": 0.15489137456100996,
                "limit_se": 0.05862470933497476,
                "limit_t": 2.6420834545375516,
                "limit_p_value": 0.008239775013213406,
                "n_obs": 936,
                "r_squared": 0.010981707013614561,
            }
        ]
    ).to_csv(
        tables_dir / "cma_h5_limit_predictive_regression.csv", index=False
    )


def _write_verdicts_csv(path: Path) -> None:
    pd.DataFrame(
        [
            {"hid": "H3", "key_value": 0.75, "n_obs": 4},
            {"hid": "H6", "key_value": -0.019, "n_obs": 67},
        ]
    ).to_csv(path, index=False)


def test_cli_writes_h4_h5_rows(tmp_path: Path, monkeypatch) -> None:
    """``main()`` now writes H3 / H4 / H5 / H6 rows when CSVs are present."""
    fake_root = tmp_path / "root"
    real_tables = fake_root / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _write_verdicts_csv(real_tables / "cma_hypothesis_verdicts.csv")
    _write_h4_h5_csvs(real_tables)
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fake_root))

    rc = cli_module.main([])
    assert rc == 0
    csv_path = real_tables / "power_analysis_report.csv"
    assert csv_path.exists()
    df = pd.read_csv(csv_path)
    assert {"H3", "H4", "H5", "H6"}.issubset(set(df["hid"].tolist()))

    h4 = df.loc[df["hid"] == "H4"].iloc[0]
    h5 = df.loc[df["hid"] == "H5"].iloc[0]
    assert h4["n_obs"] == 436
    assert h5["n_obs"] == 936
    # H4: severely underpowered
    assert 0.05 < float(h4["power_at_observed"]) < 0.20
    # H5: moderate, below the 0.80 threshold
    assert 0.70 < float(h5["power_at_observed"]) < 0.80
    # MDE labels match the regression-coef family
    assert h4["mde_label"] == "coef_at_target_power"
    assert h5["mde_label"] == "coef_at_target_power"


def test_cli_skips_h4_h5_when_csvs_missing(tmp_path: Path, monkeypatch) -> None:
    """Missing regression CSVs ⇒ silent skip (H3 + H6 still write)."""
    fake_root = tmp_path / "root"
    real_tables = fake_root / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _write_verdicts_csv(real_tables / "cma_hypothesis_verdicts.csv")
    # NB: no H4 / H5 regression CSVs.
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fake_root))
    rc = cli_module.main([])
    assert rc == 0
    df = pd.read_csv(real_tables / "power_analysis_report.csv")
    hids = set(df["hid"].tolist())
    assert {"H3", "H6"}.issubset(hids)
    # H4 / H5 silently absent.
    assert "H4" not in hids
    assert "H5" not in hids


# ---------------------------------------------------------------------------
# Public re-exports
# ---------------------------------------------------------------------------


def test_h4_h5_public_reexports() -> None:
    """The analysis package re-exports the new symbols."""
    from index_inclusion_research import analysis

    for symbol in (
        "compute_h4_power",
        "compute_h5_power",
        "regression_coef_power",
        "mde_regression_coef",
    ):
        assert hasattr(analysis, symbol), symbol
        assert symbol in analysis.__all__, symbol
