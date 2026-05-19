"""Post-hoc statistical power analysis for low-n CMA hypotheses.

Reviewers reading the CMA verdicts table will reasonably ask: *"with
n=4 (H3) or n=67 (H6), could you actually detect a real effect, or are
you reporting noise?"*. This module turns that question into numbers.

Two families of post-hoc power are implemented because the two
hypotheses use different test families:

- **Binomial proportion** (H3): the dual-channel hit rate is 3 / 4
  quadrants — a Bernoulli-trial outcome. Power is computed under the
  normal approximation z-test for a one-sample proportion, plus an
  exact-binomial cross-check. (At n=4 the exact binomial cannot reject
  ``H0: p=0.5`` at α=0.05 two-sided regardless of the data — *the
  rejection region is empty* — which is itself an honest statement of
  the limit.)
- **One-sample t-test** (H6): the heavy-vs-light weight bucket spread
  is a continuous outcome. Power is the standard non-central t formula
  parameterised by Cohen's *d* (mean / SD) and degrees of freedom
  ``n - 1``.

For both families we expose:

1. ``binomial_proportion_power`` / ``t_test_power`` — power at a stated
   ``(n, effect, α)``.
2. ``mde_at_power`` — the inverse: the minimum-detectable effect at a
   given target power (default 80%), useful for the
   "with n_obs as-is, what's the smallest effect we could have found?"
   reviewer question.
3. ``bootstrap_observed_power`` — resampling-based observed power for
   when you have actual data, not just a summary statistic.

Plus per-hypothesis helpers:

- ``compute_h3_power`` — wraps the binomial machinery for H3's 3/4
  observed hit rate against H0: p=0.5, plus a Beta-prior posterior
  ``P(p > 0.6 | data)`` cross-check. The prior is documented (uniform
  Beta(1, 1)) — we do NOT silently inject an informative prior.
- ``compute_h6_power`` — derives the observed Cohen's *d* from the
  observed spread + implied SD (heavy / light buckets), then runs the
  same battery at *d* = 0.2 / 0.5 / 0.8 / *d_observed*.

All math is from :mod:`scipy.stats`; we don't reinvent statistical
distributions. The CLI wrapper in :mod:`index_inclusion_research.power_analysis`
writes the rendered table to ``results/real_tables/power_analysis_report.csv``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
from scipy import stats

# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------


Alternative = Literal["two-sided", "greater", "less"]


@dataclass(frozen=True)
class PowerResult:
    """One row of post-hoc power: probability of rejecting ``H0`` under ``H1``."""

    test: str
    n: int
    effect_size: float
    alpha: float
    alternative: str
    power: float
    detail: str = ""


@dataclass(frozen=True)
class BootstrapPowerResult:
    """Observed-power via re-sampling the actual observations."""

    n: int
    n_bootstrap: int
    alpha: float
    rejection_rate: float
    mean_p_value: float
    detail: str = ""


@dataclass(frozen=True)
class HypothesisPowerReport:
    """Per-hypothesis post-hoc power table — fits one row of a CSV."""

    hid: str
    name_cn: str
    n_obs: int
    test_family: str
    observed_effect: float
    observed_effect_label: str
    alpha: float
    power_at_observed: float
    mde_at_80_power: float
    mde_label: str
    interpretation: str
    extras: dict[str, float] = field(default_factory=dict)
    engine: str = ""


@dataclass(frozen=True)
class EngineFlipDiagnosis:
    """Per-hypothesis verdict-flip diagnosis across two AR engines.

    Classifies whether a verdict flip between ``adjusted`` and ``market``
    AR engines is a **methodology-driven** choice (both engines have
    adequate power, so the flip is a genuine sensitivity to AR-model
    specification) or **power-limited** (at least one engine has low
    power, so the flip might be a statistical artifact rather than a
    real methodological disagreement).

    Fields
    ------
    hid:
        Hypothesis identifier (H1 or H2).
    adjusted_power, market_power:
        Per-engine power at the observed effect size.
    adjusted_p_or_effect, market_p_or_effect:
        Headline statistic per engine (p-value for H1, magnitude proxy
        for H2). Smaller p-value or larger magnitude ⇒ more "support".
    adjusted_confidence, market_confidence:
        Composite ``power × significance_indicator`` confidence score in
        ``[0, 1]``; higher = more defensible verdict per engine. The
        significance indicator is ``1.0`` when the engine's headline p
        is below ``alpha`` (or effect magnitude meets the magnitude
        floor for H2), else ``0.0``.
    engine_choice_impact:
        Ratio ``max(power_adjusted, power_market) / max(min(...), eps)``
        — how much the power differs across engines (1.0 = identical,
        >>1.0 = one engine dramatically better powered than the other).
    classification:
        ``"METHODOLOGY_DRIVEN"`` (both powers >= 0.80, real flip) ·
        ``"POWER_LIMITED"`` (at least one power < 0.50, flip artifact) ·
        ``"MIXED"`` (intermediate — flip is partly real, partly power).
    narrative:
        One-sentence English summary fit for paper §5.
    """

    hid: str
    adjusted_power: float
    market_power: float
    adjusted_p_or_effect: float
    market_p_or_effect: float
    adjusted_confidence: float
    market_confidence: float
    engine_choice_impact: float
    classification: str
    narrative: str


@dataclass(frozen=True)
class AcrossEnginePowerComparison:
    """Per-engine power reports + flip diagnoses for H1 and H2.

    Convenience container so downstream code can iterate over a single
    object instead of juggling two ``dict[engine, HypothesisPowerReport]``
    pairs and two diagnoses.
    """

    h1_per_engine: dict[str, HypothesisPowerReport]
    h2_per_engine: dict[str, HypothesisPowerReport]
    h1_diagnosis: EngineFlipDiagnosis
    h2_diagnosis: EngineFlipDiagnosis


# ---------------------------------------------------------------------------
# Binomial-proportion power (normal-approximation z-test)
# ---------------------------------------------------------------------------


def _validate_proportion(p: float, name: str) -> None:
    if not (0.0 <= p <= 1.0):
        raise ValueError(f"{name}={p!r} is not a valid probability in [0, 1]")


def binomial_proportion_power(
    n: int,
    p0: float,
    p1: float,
    *,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> PowerResult:
    """Power of a one-sample proportion z-test.

    Uses the standard normal approximation (Wald-style critical region)
    so power is well-defined for *every* ``n``, including the n=4
    regime where the exact binomial test has no α=0.05 rejection
    region. The exact-binomial test is provided separately via
    :func:`exact_binomial_power` for cross-checks.

    Parameters
    ----------
    n:
        Sample size (positive int).
    p0:
        Null-hypothesis proportion (e.g. ``0.5`` for fair coin / chance).
    p1:
        True proportion under the alternative (the "real effect" you
        want to detect).
    alpha:
        Significance level. Default 0.05.
    alternative:
        ``"two-sided"`` / ``"greater"`` / ``"less"``.

    Returns
    -------
    PowerResult
        ``power`` in ``[0, 1]`` plus the inputs as metadata.
    """
    if n < 1:
        raise ValueError(f"n={n} must be >= 1")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")
    _validate_proportion(p0, "p0")
    _validate_proportion(p1, "p1")

    se0 = math.sqrt(p0 * (1.0 - p0) / n)
    se1 = math.sqrt(p1 * (1.0 - p1) / n)

    # Handle degenerate boundary p1 ∈ {0, 1}: SE1 = 0 → indicator on
    # whether p1 lies inside the H0 acceptance region.
    if se1 == 0.0:
        if alternative == "two-sided":
            z = stats.norm.ppf(1.0 - alpha / 2.0)
            inside = (p0 - z * se0) <= p1 <= (p0 + z * se0)
            return PowerResult(
                test="binomial_proportion_z_two_sided",
                n=n,
                effect_size=p1 - p0,
                alpha=alpha,
                alternative=alternative,
                power=0.0 if inside else 1.0,
                detail="degenerate SE1=0 (p1∈{0,1}); power is indicator.",
            )
        if alternative == "greater":
            z = stats.norm.ppf(1.0 - alpha)
            return PowerResult(
                test="binomial_proportion_z_greater",
                n=n,
                effect_size=p1 - p0,
                alpha=alpha,
                alternative=alternative,
                power=1.0 if p1 > p0 + z * se0 else 0.0,
                detail="degenerate SE1=0; power is indicator.",
            )
        z = stats.norm.ppf(1.0 - alpha)
        return PowerResult(
            test="binomial_proportion_z_less",
            n=n,
            effect_size=p1 - p0,
            alpha=alpha,
            alternative=alternative,
            power=1.0 if p1 < p0 - z * se0 else 0.0,
            detail="degenerate SE1=0; power is indicator.",
        )

    if alternative == "two-sided":
        z = stats.norm.ppf(1.0 - alpha / 2.0)
        upper = 1.0 - stats.norm.cdf((p0 + z * se0 - p1) / se1)
        lower = stats.norm.cdf((p0 - z * se0 - p1) / se1)
        power = float(upper + lower)
    elif alternative == "greater":
        z = stats.norm.ppf(1.0 - alpha)
        power = float(1.0 - stats.norm.cdf((p0 + z * se0 - p1) / se1))
    elif alternative == "less":
        z = stats.norm.ppf(1.0 - alpha)
        power = float(stats.norm.cdf((p0 - z * se0 - p1) / se1))
    else:  # pragma: no cover - guarded by Literal
        raise ValueError(f"alternative={alternative!r} not recognised")

    power = max(0.0, min(1.0, power))
    return PowerResult(
        test=f"binomial_proportion_z_{alternative.replace('-', '_')}",
        n=n,
        effect_size=p1 - p0,
        alpha=alpha,
        alternative=alternative,
        power=power,
        detail=f"normal approximation; SE(p0)={se0:.4f}, SE(p1)={se1:.4f}",
    )


def exact_binomial_power(
    n: int,
    p0: float,
    p1: float,
    *,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> PowerResult:
    """Power of an EXACT binomial test (no normal approximation).

    Useful for very small n where the normal approximation can over-
    state power. When *no* rejection region exists at the requested α
    (e.g. n=4, p0=0.5, α=0.05 two-sided), the returned ``power`` is
    ``0.0`` with ``detail`` flagging "no rejection region" — that's the
    honest answer.
    """
    if n < 1:
        raise ValueError(f"n={n} must be >= 1")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")
    _validate_proportion(p0, "p0")
    _validate_proportion(p1, "p1")

    binom_null = stats.binom(n, p0)
    binom_alt = stats.binom(n, p1)

    if alternative == "greater":
        critical: int | None = None
        for k in range(n + 1):
            tail = 1.0 - binom_null.cdf(k - 1) if k > 0 else 1.0
            if tail <= alpha:
                critical = k
                break
        if critical is None:
            return PowerResult(
                test="binomial_exact_greater",
                n=n,
                effect_size=p1 - p0,
                alpha=alpha,
                alternative=alternative,
                power=0.0,
                detail=(
                    "no rejection region at this α (smallest upper tail "
                    f"= {float(1.0 - binom_null.cdf(n - 1)):.4f})"
                ),
            )
        power = float(1.0 - binom_alt.cdf(critical - 1))
        return PowerResult(
            test="binomial_exact_greater",
            n=n,
            effect_size=p1 - p0,
            alpha=alpha,
            alternative=alternative,
            power=power,
            detail=f"reject if k>={critical}",
        )

    if alternative == "less":
        critical = None
        for k in range(n + 1):
            tail = float(binom_null.cdf(k))
            if tail <= alpha:
                critical = k
        if critical is None:
            return PowerResult(
                test="binomial_exact_less",
                n=n,
                effect_size=p1 - p0,
                alpha=alpha,
                alternative=alternative,
                power=0.0,
                detail="no rejection region at this α",
            )
        power = float(binom_alt.cdf(critical))
        return PowerResult(
            test="binomial_exact_less",
            n=n,
            effect_size=p1 - p0,
            alpha=alpha,
            alternative=alternative,
            power=power,
            detail=f"reject if k<={critical}",
        )

    # two-sided: union of both tails, each at α/2.
    upper_crit: int | None = None
    for k in range(n + 1):
        tail = 1.0 - binom_null.cdf(k - 1) if k > 0 else 1.0
        if tail <= alpha / 2.0:
            upper_crit = k
            break
    lower_crit: int | None = None
    for k in range(n + 1):
        tail = float(binom_null.cdf(k))
        if tail <= alpha / 2.0:
            lower_crit = k
    if upper_crit is None and lower_crit is None:
        return PowerResult(
            test="binomial_exact_two_sided",
            n=n,
            effect_size=p1 - p0,
            alpha=alpha,
            alternative=alternative,
            power=0.0,
            detail="no rejection region at this α (both tails exceed α/2)",
        )
    power_upper = (
        float(1.0 - binom_alt.cdf(upper_crit - 1))
        if upper_crit is not None
        else 0.0
    )
    power_lower = (
        float(binom_alt.cdf(lower_crit)) if lower_crit is not None else 0.0
    )
    parts = []
    if upper_crit is not None:
        parts.append(f"k>={upper_crit}")
    if lower_crit is not None:
        parts.append(f"k<={lower_crit}")
    return PowerResult(
        test="binomial_exact_two_sided",
        n=n,
        effect_size=p1 - p0,
        alpha=alpha,
        alternative=alternative,
        power=float(power_upper + power_lower),
        detail="reject if " + " or ".join(parts),
    )


# ---------------------------------------------------------------------------
# One-sample t-test power (Cohen's d, non-central t)
# ---------------------------------------------------------------------------


def t_test_power(
    n: int,
    effect_size: float,
    *,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> PowerResult:
    """Power of a one-sample t-test parameterised by Cohen's *d*.

    Uses the non-central t distribution (the textbook formula). Returns
    α exactly when ``effect_size == 0`` (correct null behaviour).

    Parameters
    ----------
    n:
        Sample size (positive int; degrees of freedom = ``n - 1``).
    effect_size:
        Cohen's *d* = mean / SD. Sign matters for one-sided tests.
    alpha:
        Significance level.
    alternative:
        ``"two-sided"`` / ``"greater"`` / ``"less"``.
    """
    if n < 2:
        raise ValueError(f"n={n} must be >= 2 (t-test needs >=1 df)")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")

    df = n - 1
    ncp = effect_size * math.sqrt(n)

    if alternative == "two-sided":
        tcrit = stats.t.ppf(1.0 - alpha / 2.0, df)
        upper = 1.0 - stats.nct.cdf(tcrit, df, ncp)
        lower = stats.nct.cdf(-tcrit, df, ncp)
        power = float(upper + lower)
    elif alternative == "greater":
        tcrit = stats.t.ppf(1.0 - alpha, df)
        power = float(1.0 - stats.nct.cdf(tcrit, df, ncp))
    elif alternative == "less":
        tcrit = stats.t.ppf(alpha, df)
        power = float(stats.nct.cdf(tcrit, df, ncp))
    else:  # pragma: no cover - guarded by Literal
        raise ValueError(f"alternative={alternative!r} not recognised")

    power = max(0.0, min(1.0, power))
    return PowerResult(
        test=f"one_sample_t_{alternative.replace('-', '_')}",
        n=n,
        effect_size=effect_size,
        alpha=alpha,
        alternative=alternative,
        power=power,
        detail=f"df={df}, ncp={ncp:.4f}",
    )


# ---------------------------------------------------------------------------
# Minimum detectable effect (MDE) at target power
# ---------------------------------------------------------------------------


def mde_at_power(
    n: int,
    *,
    test: Literal["proportion", "t"] = "t",
    target_power: float = 0.80,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
    p0: float = 0.5,
) -> float:
    """Smallest effect-size detectable at ``target_power``.

    For ``test='t'``: returns Cohen's *d* such that
    :func:`t_test_power` at that *d* equals ``target_power``.

    For ``test='proportion'``: returns the *one-sided* proportion gap
    ``p1 − p0`` (positive for ``alternative='greater'`` / two-sided)
    such that :func:`binomial_proportion_power` equals ``target_power``.

    Uses a bisection in ``[1e-4, 5.0]`` (Cohen's d) or
    ``[1e-4, 1 − p0]`` (proportion). When even the maximum effect on
    that grid doesn't reach ``target_power`` (n is too small), the
    upper bound is returned with a ``+`` annotation in caller code.
    """
    if not (0.0 < target_power < 1.0):
        raise ValueError(f"target_power={target_power} must be in (0, 1)")

    if test == "t":
        def power_at(d: float) -> float:
            return t_test_power(
                n, d, alpha=alpha, alternative=alternative
            ).power

        lo, hi = 1e-4, 5.0
        if power_at(hi) < target_power:
            return float(hi)
        if power_at(lo) >= target_power:
            return float(lo)
        for _ in range(80):  # binary search to high precision
            mid = (lo + hi) / 2.0
            if power_at(mid) < target_power:
                lo = mid
            else:
                hi = mid
        return float((lo + hi) / 2.0)

    if test == "proportion":
        _validate_proportion(p0, "p0")
        if alternative == "less":
            def power_at_p(p1: float) -> float:
                return binomial_proportion_power(
                    n, p0, p1, alpha=alpha, alternative=alternative
                ).power

            lo, hi = max(1e-6, 0.0), p0 - 1e-6
            if power_at_p(lo) < target_power:
                return float(p0 - lo)
            if power_at_p(hi) >= target_power:
                return float(p0 - hi)
            for _ in range(80):
                mid = (lo + hi) / 2.0
                if power_at_p(mid) < target_power:
                    hi = mid
                else:
                    lo = mid
            return float(p0 - (lo + hi) / 2.0)

        def power_at_p(p1: float) -> float:
            return binomial_proportion_power(
                n, p0, p1, alpha=alpha, alternative=alternative
            ).power

        lo, hi = p0 + 1e-6, 1.0 - 1e-6
        if power_at_p(hi) < target_power:
            return float(hi - p0)
        if power_at_p(lo) >= target_power:
            return float(lo - p0)
        for _ in range(80):
            mid = (lo + hi) / 2.0
            if power_at_p(mid) < target_power:
                lo = mid
            else:
                hi = mid
        return float((lo + hi) / 2.0 - p0)

    raise ValueError(f"test={test!r} not recognised (need 'proportion' or 't')")


# ---------------------------------------------------------------------------
# Bootstrap observed power
# ---------------------------------------------------------------------------


def bootstrap_observed_power(
    observed_data: np.ndarray | list[float],
    *,
    null_value: float = 0.0,
    alpha: float = 0.05,
    n_bootstrap: int = 1000,
    alternative: Alternative = "two-sided",
    seed: int | None = 0,
) -> BootstrapPowerResult:
    """Observed power from re-sampling the actual data.

    For each of ``n_bootstrap`` resamples (with replacement) of size
    ``n = len(observed_data)``, run a one-sample t-test against
    ``null_value`` and tally the fraction that reject at ``alpha``.

    Returns a :class:`BootstrapPowerResult` with the rejection rate
    (observed power) and the mean p-value across resamples.

    Notes
    -----
    "Observed power" is a controversial post-hoc construct (see Hoenig
    & Heisey 2001). We provide it because reviewers ask for it; the
    headline numbers in this module are the *prospective* analytical
    power (``t_test_power`` / ``binomial_proportion_power``), not the
    bootstrap variant.
    """
    arr = np.asarray(observed_data, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n < 2:
        return BootstrapPowerResult(
            n=n,
            n_bootstrap=n_bootstrap,
            alpha=alpha,
            rejection_rate=float("nan"),
            mean_p_value=float("nan"),
            detail="need n>=2 finite observations",
        )
    if n_bootstrap < 1:
        raise ValueError(f"n_bootstrap={n_bootstrap} must be >= 1")

    rng = np.random.default_rng(seed)
    rejections = 0
    p_values: list[float] = []
    for _ in range(n_bootstrap):
        sample = rng.choice(arr, size=n, replace=True)
        result = stats.ttest_1samp(sample, popmean=null_value)
        # SciPy 1.6+: result.pvalue is two-sided. Adjust for one-sided.
        p_two = float(result.pvalue)
        statistic = float(result.statistic)
        if alternative == "two-sided":
            p = p_two
        elif alternative == "greater":
            p = p_two / 2.0 if statistic > 0 else 1.0 - p_two / 2.0
        else:  # less
            p = p_two / 2.0 if statistic < 0 else 1.0 - p_two / 2.0
        p_values.append(p)
        if p < alpha:
            rejections += 1
    rate = rejections / n_bootstrap
    mean_p = float(np.mean(p_values)) if p_values else float("nan")
    return BootstrapPowerResult(
        n=n,
        n_bootstrap=n_bootstrap,
        alpha=alpha,
        rejection_rate=float(rate),
        mean_p_value=mean_p,
        detail=f"one-sample t-test vs μ={null_value}, alt={alternative}",
    )


# ---------------------------------------------------------------------------
# Bayesian helper (H3 posterior with documented prior)
# ---------------------------------------------------------------------------


def beta_posterior_probability_above(
    successes: int,
    n: int,
    threshold: float,
    *,
    prior_alpha: float = 1.0,
    prior_beta: float = 1.0,
) -> float:
    """``P(p > threshold | data)`` under a Beta(prior_alpha, prior_beta) prior.

    Default prior is Beta(1, 1) — uniform on [0, 1], the standard
    weakly-informative choice. *Do not* silently change the prior; it
    is the most-contested input to a Bayesian post-hoc statement.
    """
    if successes < 0 or successes > n:
        raise ValueError(f"successes={successes} not in [0, {n}]")
    if not (0.0 <= threshold <= 1.0):
        raise ValueError(f"threshold={threshold} not in [0, 1]")
    a = prior_alpha + successes
    b = prior_beta + (n - successes)
    return float(1.0 - stats.beta.cdf(threshold, a, b))


# ---------------------------------------------------------------------------
# Per-hypothesis reports
# ---------------------------------------------------------------------------


def compute_h3_power(
    observed_hit_rate: float = 0.75,
    n: int = 4,
    *,
    null_rate: float = 0.5,
    alpha: float = 0.05,
    target_power: float = 0.80,
    bayes_threshold: float = 0.60,
) -> HypothesisPowerReport:
    """Build the H3 (散户 vs 机构 dual-channel hit rate) power report.

    H3's evidence is a 3/4 dual-channel hit rate (``observed_hit_rate
    = 0.75``) across n=4 (CN announce, CN effective, US announce, US
    effective) quadrants. Reviewers will object that n=4 is too small
    to detect anything, so we make the limit explicit.

    The Bayesian tail probability ``P(true p > 0.60 | 3/4 hits, uniform
    prior)`` complements the frequentist power because that prior
    choice is documented (not hidden), and 0.60 is a sensible "real
    edge" threshold that the conventional "weak supplementary"
    classification implicitly assumes.
    """
    _validate_proportion(observed_hit_rate, "observed_hit_rate")
    _validate_proportion(null_rate, "null_rate")

    power_at_obs = binomial_proportion_power(
        n, null_rate, observed_hit_rate, alpha=alpha, alternative="two-sided"
    )
    mde = mde_at_power(
        n,
        test="proportion",
        target_power=target_power,
        alpha=alpha,
        alternative="two-sided",
        p0=null_rate,
    )
    exact = exact_binomial_power(
        n, null_rate, observed_hit_rate, alpha=alpha, alternative="two-sided"
    )
    successes = int(round(observed_hit_rate * n))
    posterior = beta_posterior_probability_above(
        successes, n, bayes_threshold
    )

    if power_at_obs.power < 0.30:
        verdict = (
            f"严重欠功效 (power={power_at_obs.power:.2f} < 0.30): n=4 "
            f"无法在 α={alpha} 检出真实命中率 {observed_hit_rate:.0%}；"
            "结果按 supplementary 处理是合理的。"
        )
    elif power_at_obs.power < 0.50:
        verdict = (
            f"功效偏低 (power={power_at_obs.power:.2f} < 0.50): "
            "可作为指向性证据保留，但不应作为强结论。"
        )
    elif power_at_obs.power < 0.80:
        verdict = (
            f"功效中等 (power={power_at_obs.power:.2f}): 趋势可读但仍"
            "建议补样本至 n>=20 才能升级为 core 证据。"
        )
    else:
        verdict = (
            f"功效充足 (power={power_at_obs.power:.2f} >= 0.80): "
            "n 足以稳健检出该效应。"
        )

    extras = {
        "exact_power": float(exact.power),
        f"bayes_p_gt_{bayes_threshold:.2f}": posterior,
        "successes": float(successes),
    }
    interpretation = (
        f"normal-approx power={power_at_obs.power:.3f} · "
        f"exact-binomial power={exact.power:.3f} · "
        f"posterior P(p>{bayes_threshold:.2f}|data)={posterior:.3f} "
        f"(Beta(1,1) uniform prior). MDE@{int(target_power * 100)}%="
        f"+{mde:.3f} 概率差（p1≈{null_rate + mde:.3f}）。{verdict}"
    )
    return HypothesisPowerReport(
        hid="H3",
        name_cn="散户 vs 机构结构",
        n_obs=n,
        test_family="binomial_proportion_z_two_sided",
        observed_effect=observed_hit_rate - null_rate,
        observed_effect_label="hit_rate − 0.5",
        alpha=alpha,
        power_at_observed=float(power_at_obs.power),
        mde_at_80_power=float(mde),
        mde_label="proportion_gap_p1_minus_p0",
        interpretation=interpretation,
        extras=extras,
    )


def compute_h6_power(
    observed_spread: float = -0.019,
    observed_sd: float | None = None,
    n: int = 67,
    *,
    alpha: float = 0.05,
    target_power: float = 0.80,
    heavy_jump_mean: float | None = None,
    light_jump_mean: float | None = None,
    bucket_sd: float | None = None,
) -> HypothesisPowerReport:
    """Build the H6 (指数权重可预测性) power report.

    H6 contrasts heavy-bucket ``announce_jump`` against light-bucket.
    The published heavy−light spread is roughly −1.90 pp (the value
    stored as ``key_value`` in the verdicts CSV; equivalent to the
    OLS-style standardized weight coefficient). When ``observed_sd``
    is not supplied, we fall back to a defensible default derived from
    the within-sample ``announce_jump`` standard deviation; this fall-
    back is documented in the ``detail`` field.

    Two power computations are surfaced:

    1. ``power_at_observed`` — power of the one-sample t-test to detect
       the observed effect (Cohen's *d* derived from spread / SD).
       *This answers the question "could we re-detect the same effect
       if we re-ran the study?"*
    2. ``mde_at_80_power`` — Cohen's *d* the test could detect at 80 %
       power. *This answers "what's the smallest real effect we could
       have caught with n_obs we have?"*

    The headline test is a two-sided one-sample t (the same family
    statsmodels / scipy use for ``ttest_1samp``); a two-sample test
    parameterised by the same Cohen's *d* is closely equivalent in
    this regime.

    Note on H6's "证据不足" verdict
    -----------------------------
    With the observed Cohen's *d* of ≈ −0.7 the test has near-1 power
    to reject the null. The hypothesis is marked "证据不足" not because
    of low power but because the **direction** contradicts H6's
    prediction (heavy > light expected, light > heavy observed). The
    power report makes this clear by surfacing the sign of *d*.
    """
    if observed_sd is not None and observed_sd > 0:
        d_observed = observed_spread / observed_sd
        sd_source = f"caller-supplied SD={observed_sd:.4f}"
    elif (
        heavy_jump_mean is not None
        and light_jump_mean is not None
        and bucket_sd is not None
        and bucket_sd > 0
    ):
        d_observed = (heavy_jump_mean - light_jump_mean) / bucket_sd
        sd_source = f"bucket-SD={bucket_sd:.4f}"
    else:
        # Back-derive from r² = 0.033 reported in the H6 OLS-HC3
        # robustness row. r² = d² / (d² + 4) under equal-group asymp;
        # solve for d. This is approximate, not fabricated, and is
        # called out in the ``detail`` so reviewers can see it.
        r2 = 0.033
        d_observed = math.copysign(
            2.0 * math.sqrt(r2 / (1.0 - r2)), observed_spread
        )
        sd_source = f"derived from r²={r2} (no SD supplied)"

    power_at_obs = t_test_power(
        n, abs(d_observed), alpha=alpha, alternative="two-sided"
    )
    mde = mde_at_power(
        n,
        test="t",
        target_power=target_power,
        alpha=alpha,
        alternative="two-sided",
    )
    pw_d020 = t_test_power(n, 0.20, alpha=alpha)
    pw_d050 = t_test_power(n, 0.50, alpha=alpha)
    pw_d080 = t_test_power(n, 0.80, alpha=alpha)

    # H6 verdict text: distinguish low-power *direction-mismatch* from
    # low-power *noise*. d_observed sign carries the directional flag.
    if power_at_obs.power < 0.30:
        verdict = (
            f"严重欠功效 (power={power_at_obs.power:.2f} < 0.30): n={n} "
            "无法稳健检出该规模的效应。"
        )
    elif power_at_obs.power < 0.50:
        verdict = (
            f"功效偏低 (power={power_at_obs.power:.2f} < 0.50): "
            "可作为方向性证据保留，但需扩样本（≥150）才能升级。"
        )
    elif power_at_obs.power < 0.80:
        verdict = (
            f"功效中等 (power={power_at_obs.power:.2f}): 趋势可读但"
            "仍存在错过真实小效应的风险。"
        )
    else:
        if d_observed < 0:
            verdict = (
                f"功效充足 (power={power_at_obs.power:.2f} >= 0.80) — "
                "n 足以检出该效应，但观测方向 (heavy<light) "
                "与 H6 预测 (heavy>light) 相反，所以 verdict='证据不足' "
                "并不是 n 不够，而是方向不符。"
            )
        else:
            verdict = (
                f"功效充足 (power={power_at_obs.power:.2f} >= 0.80): "
                "n 足以稳健检出该效应。"
            )

    extras = {
        "cohens_d_observed": float(d_observed),
        "power_at_d_0.20": float(pw_d020.power),
        "power_at_d_0.50": float(pw_d050.power),
        "power_at_d_0.80": float(pw_d080.power),
    }
    interpretation = (
        f"Cohen's d (observed) ≈ {d_observed:+.3f} ({sd_source}); "
        f"two-sided t-test power={power_at_obs.power:.3f}. "
        f"对比小/中/大效应 (d=0.2/0.5/0.8) 的功效 = "
        f"{pw_d020.power:.2f} / {pw_d050.power:.2f} / {pw_d080.power:.2f}. "
        f"MDE@{int(target_power * 100)}% = |d|={mde:.3f}. {verdict}"
    )
    return HypothesisPowerReport(
        hid="H6",
        name_cn="指数权重可预测性",
        n_obs=n,
        test_family="one_sample_t_two_sided",
        observed_effect=float(d_observed),
        observed_effect_label="cohens_d",
        alpha=alpha,
        power_at_observed=float(power_at_obs.power),
        mde_at_80_power=float(mde),
        mde_label="cohens_d_at_target_power",
        interpretation=interpretation,
        extras=extras,
    )


# ---------------------------------------------------------------------------
# H1 + H2 per-engine power (AR-engine sensitivity)
# ---------------------------------------------------------------------------


# Canonical engine labels used across the project (matches
# results/sensitivity/ar_{adjusted,market}/ directory naming).
ENGINE_LABELS: tuple[str, ...] = ("adjusted", "market")


def _bootstrap_se_from_ci(
    ci_low: float, ci_high: float, *, confidence: float = 0.95
) -> float:
    """Recover an approximate bootstrap SE from a symmetric (1−α) CI.

    For a 95 % bootstrap CI under normal-approximation symmetry, the SE
    is ``(CI_high − CI_low) / (2 · z_{1−α/2})`` with ``z_{0.975} ≈ 1.96``.
    The recovered SE is a *normal-approximation* read of the bootstrap
    spread; it agrees with the empirical SE up to bootstrap noise when
    the bootstrap distribution is symmetric, which the diff-of-means
    statistic always is asymptotically.
    """
    if not (0.0 < confidence < 1.0):
        raise ValueError(f"confidence={confidence} must be in (0, 1)")
    half_width = (float(ci_high) - float(ci_low)) / 2.0
    if half_width <= 0:
        return float("nan")
    z = stats.norm.ppf(0.5 + confidence / 2.0)
    return float(half_width / z)


def bootstrap_test_power(
    observed_diff: float,
    bootstrap_se: float,
    *,
    n_total: int,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> PowerResult:
    """Power of a bootstrap-based two-sample diff-of-means test.

    Uses the normal-approximation power formula on the observed test
    statistic ``Z = diff / SE``. This is exactly what the bootstrap
    p-value is approximating in the limit, so post-hoc power computed
    this way is the natural counterpart of the bootstrap p in the
    verdict CSV.

    Parameters
    ----------
    observed_diff:
        The point estimate (CN_mean − US_mean for H1).
    bootstrap_se:
        Bootstrap standard error of ``observed_diff`` (typically
        :func:`_bootstrap_se_from_ci` of the published CI).
    n_total:
        Combined sample size (n_cn + n_us); recorded on the
        :class:`PowerResult` for transparency. The SE already encodes
        the sample-size effect, so it is *not* used to rescale the
        non-centrality.
    alpha, alternative:
        Standard arguments; default two-sided.
    """
    if n_total < 1:
        raise ValueError(f"n_total={n_total} must be >= 1")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")
    if not math.isfinite(bootstrap_se) or bootstrap_se <= 0:
        return PowerResult(
            test=f"bootstrap_diff_{alternative.replace('-', '_')}",
            n=n_total,
            effect_size=float(observed_diff),
            alpha=alpha,
            alternative=alternative,
            power=float("nan"),
            detail="non-positive or non-finite bootstrap SE",
        )
    ncp = float(observed_diff) / float(bootstrap_se)
    if alternative == "two-sided":
        z = stats.norm.ppf(1.0 - alpha / 2.0)
        # P(|Z| > z) under H1 ~ N(ncp, 1)
        upper = float(1.0 - stats.norm.cdf(z - ncp))
        lower = float(stats.norm.cdf(-z - ncp))
        power = upper + lower
    elif alternative == "greater":
        z = stats.norm.ppf(1.0 - alpha)
        power = float(1.0 - stats.norm.cdf(z - ncp))
    elif alternative == "less":
        z = stats.norm.ppf(1.0 - alpha)
        power = float(stats.norm.cdf(-z - ncp))
    else:  # pragma: no cover - guarded by Literal
        raise ValueError(f"alternative={alternative!r} not recognised")
    power = max(0.0, min(1.0, power))
    return PowerResult(
        test=f"bootstrap_diff_{alternative.replace('-', '_')}",
        n=n_total,
        effect_size=float(observed_diff),
        alpha=alpha,
        alternative=alternative,
        power=power,
        detail=f"normal-approx via Z={ncp:.4f}; bootstrap SE={bootstrap_se:.4f}",
    )


def mde_bootstrap_test(
    bootstrap_se: float,
    *,
    target_power: float = 0.80,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> float:
    """Minimum-detectable diff for a bootstrap test at ``target_power``.

    For the normal-approximation form, the analytic MDE is
    ``(z_{1-α/2} + z_{target_power}) · SE`` two-sided (or the obvious
    one-sided variants). Returns ``nan`` for non-finite / non-positive
    SE.
    """
    if not math.isfinite(bootstrap_se) or bootstrap_se <= 0:
        return float("nan")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")
    if not (0.0 < target_power < 1.0):
        raise ValueError(f"target_power={target_power} must be in (0, 1)")
    z_beta = stats.norm.ppf(target_power)
    if alternative == "two-sided":
        z_alpha = stats.norm.ppf(1.0 - alpha / 2.0)
    else:
        z_alpha = stats.norm.ppf(1.0 - alpha)
    return float((z_alpha + z_beta) * bootstrap_se)


def regression_coef_power(
    coef: float,
    se: float,
    *,
    n: int,
    n_covariates: int = 1,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> PowerResult:
    """Power of a single-coefficient t-test in an OLS / robust-SE regression.

    H4 and H5's headline tests are exactly this shape: a published
    ``(coef, se, n)`` triple from an HC3 (heteroskedasticity-robust)
    regression. Under the standard asymptotic Gaussian approximation,
    the test statistic is ``t = coef / se`` against
    ``H0: coef = 0`` with degrees of freedom ``df = n − n_covariates − 1``.
    Post-hoc power is the non-central t survival probability at the
    critical t-value, with non-centrality ``ncp = coef / se``.

    Parameters
    ----------
    coef:
        Observed coefficient (point estimate).
    se:
        Robust (HC3) standard error of ``coef``.
    n:
        Regression sample size (rows used to fit the model).
    n_covariates:
        Number of regressors *excluding* the intercept. The
        ``cma_gap_drift_market_regression`` table fits ``announce_jump
        ~ cn_dummy + gap_length_days`` so ``n_covariates = 2``; the H5
        ``cma_h5_limit_predictive_regression`` table fits the lone
        ``limit_hit_rate`` predictor so ``n_covariates = 1``. With
        ``n`` in the hundreds the choice of ``df`` only matters at the
        third decimal; we still pass it through so callers stay honest.
    alpha, alternative:
        Standard arguments.

    Returns
    -------
    PowerResult
        ``power`` in ``[0, 1]`` with ``df`` and ``ncp`` in the detail
        string. Returns ``nan`` power when ``se`` is non-positive /
        non-finite — the regression was rank-deficient or the upstream
        CSV column was missing.
    """
    if n < 2:
        raise ValueError(f"n={n} must be >= 2 (regression needs df >= 1)")
    if n_covariates < 0:
        raise ValueError(f"n_covariates={n_covariates} must be >= 0")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")
    if not math.isfinite(se) or se <= 0:
        return PowerResult(
            test=f"regression_coef_t_{alternative.replace('-', '_')}",
            n=n,
            effect_size=float(coef),
            alpha=alpha,
            alternative=alternative,
            power=float("nan"),
            detail="non-positive or non-finite SE; regression rank-deficient?",
        )
    df = max(n - n_covariates - 1, 1)
    ncp = float(coef) / float(se)
    if alternative == "two-sided":
        tcrit = stats.t.ppf(1.0 - alpha / 2.0, df)
        upper = float(1.0 - stats.nct.cdf(tcrit, df, ncp))
        lower = float(stats.nct.cdf(-tcrit, df, ncp))
        power = upper + lower
    elif alternative == "greater":
        tcrit = stats.t.ppf(1.0 - alpha, df)
        power = float(1.0 - stats.nct.cdf(tcrit, df, ncp))
    elif alternative == "less":
        tcrit = stats.t.ppf(alpha, df)
        power = float(stats.nct.cdf(tcrit, df, ncp))
    else:  # pragma: no cover - guarded by Literal
        raise ValueError(f"alternative={alternative!r} not recognised")
    power = max(0.0, min(1.0, power))
    return PowerResult(
        test=f"regression_coef_t_{alternative.replace('-', '_')}",
        n=n,
        effect_size=float(coef),
        alpha=alpha,
        alternative=alternative,
        power=power,
        detail=f"df={df}, ncp={ncp:.4f}, se={se:.4g}",
    )


def mde_regression_coef(
    se: float,
    *,
    n: int,
    n_covariates: int = 1,
    target_power: float = 0.80,
    alpha: float = 0.05,
    alternative: Alternative = "two-sided",
) -> float:
    """Minimum-detectable regression coefficient at ``target_power``.

    Two equivalent formulations are common: the standard (normal-approx)
    closed form ``(z_{1-α/2} + z_{target_power}) · SE`` and the exact
    non-central t inversion (slightly larger at very small df). We use
    the **non-central t inversion** — bisection over the same range as
    :func:`mde_at_power` — so the returned MDE matches the actual
    :func:`regression_coef_power` at that effect.

    Returns ``nan`` for non-finite / non-positive SE.
    """
    if not math.isfinite(se) or se <= 0:
        return float("nan")
    if not (0.0 < alpha < 1.0):
        raise ValueError(f"alpha={alpha} must be in (0, 1)")
    if not (0.0 < target_power < 1.0):
        raise ValueError(f"target_power={target_power} must be in (0, 1)")
    if n < 2:
        raise ValueError(f"n={n} must be >= 2")

    def power_at(coef: float) -> float:
        return regression_coef_power(
            coef,
            se,
            n=n,
            n_covariates=n_covariates,
            alpha=alpha,
            alternative=alternative,
        ).power

    # Bisection across a generous absolute-coef range, scaled by SE so
    # very small or very large SEs both converge.
    lo, hi = 1e-12, max(se * 50.0, 1.0)
    if power_at(hi) < target_power:
        return float(hi)
    if power_at(lo) >= target_power:
        return float(lo)
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if power_at(mid) < target_power:
            lo = mid
        else:
            hi = mid
    return float((lo + hi) / 2.0)


def compute_h4_power(
    coef: float = 0.006111113589099461,
    se: float = 0.00989025033185132,
    p_value: float = 0.5366460641588349,
    n: int = 436,
    *,
    n_covariates: int = 2,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> HypothesisPowerReport:
    """Build the H4 (short-sale constraint / 卖空约束) power report.

    H4's headline test is the t-test on the ``cn_dummy`` coefficient in
    the HC3 regression of ``gap_drift`` on ``cn_dummy + gap_length_days``.
    Verdict source: ``results/real_tables/cma_gap_drift_market_regression.csv``
    — ``cn_coef``, ``cn_se``, ``cn_p_value``, ``n_obs``.

    The expected sign under H4 is **positive** (CN should drift more
    than US during the gap window because short-sale constraints
    prevent arbitrageurs from leaning against the inclusion-driven
    over-pricing). The observed coefficient is positive but with a wide
    SE — exactly the scenario where post-hoc power matters: we need to
    quantify how small an effect we could realistically have detected
    with n=436.

    Parameters
    ----------
    coef, se, p_value:
        Headline statistics from the cn_coef row of the gap-drift
        regression CSV. Defaults match the frozen verdicts (HC3 SE).
    n:
        Regression sample size.
    n_covariates:
        Regressors excluding the intercept (default 2: cn_dummy +
        gap_length_days).
    """
    if n < 2:
        raise ValueError(f"n={n} must be >= 2")

    power_at_obs = regression_coef_power(
        coef,
        se,
        n=n,
        n_covariates=n_covariates,
        alpha=alpha,
        alternative="two-sided",
    )
    mde = mde_regression_coef(
        se,
        n=n,
        n_covariates=n_covariates,
        target_power=target_power,
        alpha=alpha,
        alternative="two-sided",
    )
    t_observed = coef / se if math.isfinite(se) and se > 0 else float("nan")

    if not math.isfinite(power_at_obs.power):
        verdict = (
            "无法计算 (SE 不可用)，请检查 cma_gap_drift_market_regression.csv "
            "的 cn_se 列是否完整。"
        )
    elif power_at_obs.power < 0.30:
        verdict = (
            f"严重欠功效 (power={power_at_obs.power:.2f} < 0.30): n={n} 下"
            f"观测系数 {coef:+.4f} 太小 (相对 SE={se:.4f})，"
            "无法在 α=0.05 下稳健检出。证据不足的判定是 n 不够大，"
            "不是 H4 一定错，因此保留为 supplementary 是合理的。"
        )
    elif power_at_obs.power < 0.50:
        verdict = (
            f"功效偏低 (power={power_at_obs.power:.2f} < 0.50): "
            f"n={n} 下可见方向性证据，但需扩样本(或加更强协变量)才能升级。"
        )
    elif power_at_obs.power < 0.80:
        verdict = (
            f"功效中等 (power={power_at_obs.power:.2f}): n={n} 已能稳健"
            "区分中等以上效应，但当前观测效应仍存在错过真实小效应的风险。"
        )
    else:
        verdict = (
            f"功效充足 (power={power_at_obs.power:.2f} >= 0.80): "
            f"n={n} 下已能稳健检出 |coef|≈{abs(coef):.4f} 的效应。"
        )

    extras = {
        "coef_observed": float(coef),
        "se_observed": float(se),
        "t_observed": float(t_observed) if math.isfinite(t_observed) else float("nan"),
        "p_value_observed": float(p_value),
        "n_covariates": float(n_covariates),
    }
    power_val = (
        float(power_at_obs.power)
        if math.isfinite(power_at_obs.power)
        else float("nan")
    )
    interpretation = (
        f"HC3 regression coef={coef:+.4f} (SE={se:.4f}, t={t_observed:+.3f}, "
        f"p={p_value:.4f}), df≈{n - n_covariates - 1}; "
        f"two-sided t-test power={power_val:.3f}. "
        f"MDE@{int(target_power * 100)}% = |coef|≈{mde:.4f} "
        f"(≈ {mde / max(abs(coef), 1e-12):.1f}× the observed coefficient). "
        f"{verdict}"
    )
    return HypothesisPowerReport(
        hid="H4",
        name_cn="卖空约束",
        n_obs=n,
        test_family="regression_coef_t_two_sided",
        observed_effect=float(coef),
        observed_effect_label="cn_coef_gap_drift",
        alpha=alpha,
        power_at_observed=power_val,
        mde_at_80_power=float(mde),
        mde_label="coef_at_target_power",
        interpretation=interpretation,
        extras=extras,
    )


def compute_h5_power(
    coef: float = 0.15489137456100996,
    se: float = 0.05862470933497476,
    p_value: float = 0.008239775013213406,
    n: int = 936,
    *,
    n_covariates: int = 1,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> HypothesisPowerReport:
    """Build the H5 (涨跌停限制 / limit-up-down constraint) power report.

    H5's headline test is the t-test on the ``limit_coef`` coefficient
    in the regression of ``announce-day CAR`` on the limit-hit rate.
    Verdict source: ``results/real_tables/cma_h5_limit_predictive_regression.csv``
    — ``limit_coef``, ``limit_se``, ``limit_p_value``, ``n_obs``.

    The expected sign under H5 is **positive**: CN-style limit-up
    days truncate the price-discovery process, so heavier limit
    exposure → larger announce-day CAR. The observed coefficient is
    +0.1549 (significant at α=0.05), so we expect post-hoc power near 1
    — but we report the number explicitly rather than asserting it, and
    we surface the MDE so reviewers can see how small an effect H5's
    n=936 could *have* detected.

    Parameters
    ----------
    coef, se, p_value:
        Headline statistics from the limit-predictive regression CSV.
        Defaults match the frozen verdicts.
    n:
        Regression sample size (event-level CN sample, n=936).
    n_covariates:
        Regressors excluding the intercept (default 1: limit_hit_rate).
    """
    if n < 2:
        raise ValueError(f"n={n} must be >= 2")

    power_at_obs = regression_coef_power(
        coef,
        se,
        n=n,
        n_covariates=n_covariates,
        alpha=alpha,
        alternative="two-sided",
    )
    mde = mde_regression_coef(
        se,
        n=n,
        n_covariates=n_covariates,
        target_power=target_power,
        alpha=alpha,
        alternative="two-sided",
    )
    t_observed = coef / se if math.isfinite(se) and se > 0 else float("nan")

    if not math.isfinite(power_at_obs.power):
        verdict = (
            "无法计算 (SE 不可用)，请检查 cma_h5_limit_predictive_regression.csv "
            "的 limit_se 列是否完整。"
        )
    elif power_at_obs.power < 0.30:
        verdict = (
            f"严重欠功效 (power={power_at_obs.power:.2f} < 0.30): n={n} "
            "下仍无法稳健检出该效应。"
        )
    elif power_at_obs.power < 0.50:
        verdict = (
            f"功效偏低 (power={power_at_obs.power:.2f} < 0.50): "
            "可作为方向性证据保留，但扩样本会带来明显增益。"
        )
    elif power_at_obs.power < 0.80:
        verdict = (
            f"功效中等 (power={power_at_obs.power:.2f}): 趋势可读但仍"
            "存在错过真实小效应的风险。"
        )
    else:
        verdict = (
            f"功效充足 (power={power_at_obs.power:.2f} >= 0.80): "
            f"n={n} 足以稳健检出 |coef|≈{abs(coef):.4f} 的效应；"
            "H5 的 'supportive' 裁决在统计功效层面是站得住脚的。"
        )

    extras = {
        "coef_observed": float(coef),
        "se_observed": float(se),
        "t_observed": float(t_observed) if math.isfinite(t_observed) else float("nan"),
        "p_value_observed": float(p_value),
        "n_covariates": float(n_covariates),
    }
    power_val = (
        float(power_at_obs.power)
        if math.isfinite(power_at_obs.power)
        else float("nan")
    )
    interpretation = (
        f"HC3 regression coef={coef:+.4f} (SE={se:.4f}, t={t_observed:+.3f}, "
        f"p={p_value:.4f}), df≈{n - n_covariates - 1}; "
        f"two-sided t-test power={power_val:.3f}. "
        f"MDE@{int(target_power * 100)}% = |coef|≈{mde:.4f} "
        f"(≈ {mde / max(abs(coef), 1e-12):.2f}× the observed coefficient). "
        f"{verdict}"
    )
    return HypothesisPowerReport(
        hid="H5",
        name_cn="涨跌停限制",
        n_obs=n,
        test_family="regression_coef_t_two_sided",
        observed_effect=float(coef),
        observed_effect_label="limit_coef_announce_car",
        alpha=alpha,
        power_at_observed=power_val,
        mde_at_80_power=float(mde),
        mde_label="coef_at_target_power",
        interpretation=interpretation,
        extras=extras,
    )


def compute_h1_power_per_engine(
    engine_inputs: dict[str, dict[str, float]],
    *,
    n: int = 436,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> dict[str, HypothesisPowerReport]:
    """Per-engine H1 (pre-runup) power reports.

    H1's headline test is the bootstrap-distribution of the cross-market
    diff-of-means ``CN_pre_runup − US_pre_runup``. With both engines'
    bootstrap CSVs we can compute the post-hoc power *per engine* and
    detect whether the verdict flip reflects a real methodological
    choice or low power under one engine.

    Parameters
    ----------
    engine_inputs:
        Mapping ``engine_label → dict`` with at minimum the keys
        ``diff_mean``, ``boot_ci_low``, ``boot_ci_high``,
        ``boot_p_value``. Missing entries return ``nan`` power.
    n:
        Combined sample size (default 436 = n_cn + n_us in the headline
        run).
    """
    out: dict[str, HypothesisPowerReport] = {}
    for engine, row in engine_inputs.items():
        diff = float(row.get("diff_mean", float("nan")))
        ci_low = float(row.get("boot_ci_low", float("nan")))
        ci_high = float(row.get("boot_ci_high", float("nan")))
        p_value = float(row.get("boot_p_value", float("nan")))
        n_engine = int(row.get("n_total", n))
        se = _bootstrap_se_from_ci(ci_low, ci_high)
        power_res = bootstrap_test_power(
            diff, se, n_total=n_engine, alpha=alpha
        )
        mde = mde_bootstrap_test(se, target_power=target_power, alpha=alpha)
        if not math.isfinite(power_res.power):
            verdict = (
                "无法计算 (bootstrap SE 不可用)，请确认 cma_pre_runup_bootstrap.csv "
                "存在并含 boot_ci_low/high 列。"
            )
        elif power_res.power >= 0.80:
            verdict = (
                f"功效充足 (power={power_res.power:.2f} >= 0.80): "
                f"{engine} 引擎下 n={n_engine} 足以检出 |diff|≈{abs(diff):.4f} "
                "的真实效应。"
            )
        elif power_res.power >= 0.50:
            verdict = (
                f"功效中等 (power={power_res.power:.2f}): {engine} 引擎下 "
                "可见趋势但仍存在错过真实小效应的风险。"
            )
        else:
            verdict = (
                f"功效偏低 (power={power_res.power:.2f} < 0.50): "
                f"{engine} 引擎下 bootstrap SE={se:.4f} 太宽，"
                "差异需达到 |diff|≈" + (f"{mde:.4f}" if math.isfinite(mde) else "—")
                + " 才能在 80% 功效下被检出。"
            )
        interpretation = (
            f"engine={engine}: diff={diff:+.4f}, bootstrap SE≈{se:.4f}, "
            f"bootstrap p={p_value:.4f}; 在该 SE 与 n={n_engine} 下，"
            f"两侧 z-test 功效={power_res.power:.3f}; "
            f"MDE@{int(target_power * 100)}%≈|diff|={mde:.4f}. {verdict}"
        )
        extras = {
            "bootstrap_se": float(se) if math.isfinite(se) else float("nan"),
            "bootstrap_p_value": p_value,
            "ci_low": ci_low,
            "ci_high": ci_high,
        }
        out[engine] = HypothesisPowerReport(
            hid="H1",
            name_cn="信息泄露与预运行",
            n_obs=n_engine,
            test_family="bootstrap_diff_two_sided",
            observed_effect=diff,
            observed_effect_label="cn_minus_us_pre_runup",
            alpha=alpha,
            power_at_observed=float(power_res.power)
            if math.isfinite(power_res.power)
            else float("nan"),
            mde_at_80_power=float(mde),
            mde_label="diff_at_target_power",
            interpretation=interpretation,
            extras=extras,
            engine=engine,
        )
    return out


def compute_h2_power_per_engine(
    engine_inputs: dict[str, dict[str, Any]],
    *,
    n_combined: int = 17,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> dict[str, HypothesisPowerReport]:
    """Per-engine H2 (passive-AUM AUM up + effective CAR decline) power.

    H2's verdict is a direction-match test on the rolling effective-day
    CAR time-series (does CAR decline as AUM rises?). We treat the
    sequence of year-over-year ``car_effective`` deltas as the data
    vector and run a one-sample t-test against ``H0: mean change = 0``
    (the per-engine direction). Cohen's *d* is ``mean / SD`` of the
    deltas, ``n = n_combined`` for the matched US+CN pooled series.

    Parameters
    ----------
    engine_inputs:
        Mapping ``engine_label → dict`` with optional keys ``deltas``
        (an iterable of year-over-year changes in ``car_effective``)
        and ``aum_ratio`` (the US AUM ratio headline). When ``deltas``
        is missing we fall back to a derived effect size built from
        ``car_first``, ``car_last``, ``n_roll`` plus the magnitude
        proxy ``|car_last − car_first| / max(|first|, |last|, 1e-3)``.
    n_combined:
        Total rolling-CAR observations (US n_roll + CN n_roll). The
        published "n=17" comes from US 12 + CN 5.
    """
    out: dict[str, HypothesisPowerReport] = {}
    for engine, row in engine_inputs.items():
        n_engine = int(row.get("n_combined", n_combined))
        if "deltas" in row and row["deltas"] is not None:
            deltas = np.asarray(row["deltas"], dtype=float)
            deltas = deltas[np.isfinite(deltas)]
            n_engine = int(deltas.size)
            if deltas.size < 2:
                d_observed = float("nan")
                sd = float("nan")
                source = "deltas insufficient"
            else:
                mean = float(deltas.mean())
                sd = float(deltas.std(ddof=1))
                d_observed = mean / sd if sd > 0 else float("nan")
                source = f"empirical deltas (n_used={deltas.size}, SD={sd:.4f})"
        else:
            # Fallback: derive |d| from the trend magnitude.
            car_first = float(row.get("car_first", float("nan")))
            car_last = float(row.get("car_last", float("nan")))
            trend_sd = float(row.get("trend_sd", float("nan")))
            if (
                math.isfinite(car_first)
                and math.isfinite(car_last)
                and math.isfinite(trend_sd)
                and trend_sd > 0
            ):
                d_observed = (car_last - car_first) / trend_sd
                sd = trend_sd
                source = (
                    f"trend slope (car_first={car_first:+.4f} → "
                    f"car_last={car_last:+.4f}, sd={trend_sd:.4f})"
                )
            else:
                d_observed = float("nan")
                sd = float("nan")
                source = "no deltas + trend SD missing"
        if math.isfinite(d_observed) and n_engine >= 2:
            power_res = t_test_power(
                n_engine, abs(d_observed), alpha=alpha, alternative="two-sided"
            )
            mde = mde_at_power(
                n_engine, test="t", target_power=target_power, alpha=alpha
            )
            power_val = float(power_res.power)
        else:
            power_val = float("nan")
            mde = float("nan")
        if not math.isfinite(power_val):
            verdict = (
                "无法计算 (deltas / trend SD 不可用)，"
                "请确认 cma_time_series_rolling.csv 完整。"
            )
        elif power_val >= 0.80:
            verdict = (
                f"功效充足 (power={power_val:.2f} >= 0.80): {engine} 引擎下 "
                f"n_combined={n_engine} 足以检出 |d|≈{abs(d_observed):.2f} "
                "的真实方向效应。"
            )
        elif power_val >= 0.50:
            verdict = (
                f"功效中等 (power={power_val:.2f}): {engine} 引擎下 "
                "趋势可读，但 n=17 仅能稳健检出中等以上效应。"
            )
        else:
            verdict = (
                f"功效偏低 (power={power_val:.2f} < 0.50): {engine} 引擎下 "
                f"样本 n={n_engine} 太小，"
                f"|d| 必须 ≥{mde:.3f} 才能在 80% 功效下检出。"
            )
        interpretation = (
            f"engine={engine}: Cohen's d≈{d_observed:+.3f} ({source}); "
            f"n_combined={n_engine}, two-sided t-test power="
            f"{power_val:.3f}, MDE@{int(target_power * 100)}%=|d|={mde:.3f}. "
            f"{verdict}"
        )
        extras = {
            "cohens_d": float(d_observed)
            if math.isfinite(d_observed)
            else float("nan"),
            "trend_sd": float(sd) if math.isfinite(sd) else float("nan"),
        }
        out[engine] = HypothesisPowerReport(
            hid="H2",
            name_cn="被动基金 AUM 差异",
            n_obs=n_engine,
            test_family="one_sample_t_two_sided",
            observed_effect=float(d_observed)
            if math.isfinite(d_observed)
            else float("nan"),
            observed_effect_label="cohens_d_car_delta",
            alpha=alpha,
            power_at_observed=power_val,
            mde_at_80_power=float(mde),
            mde_label="cohens_d_at_target_power",
            interpretation=interpretation,
            extras=extras,
            engine=engine,
        )
    return out


def diagnose_engine_flip(
    hid: str,
    per_engine: dict[str, HypothesisPowerReport],
    *,
    alpha: float = 0.05,
    h2_magnitude_floor: float = 0.50,
) -> EngineFlipDiagnosis:
    """Classify an engine flip as METHODOLOGY-DRIVEN, POWER-LIMITED, or MIXED.

    Rules
    -----
    - **NO_FLIP**: both engines are present but their verdict indicators
      do not disagree, so no flip should be diagnosed.
    - **METHODOLOGY_DRIVEN**: both engines have power >= 0.80, the
      engines disagree, AND at least one engine clearly rejects
      (p < α / magnitude exceeds the floor). Flip is a real, defensible
      methodological-choice signal.
    - **POWER_LIMITED**: at least one engine has power < 0.50 AND the
      ratio ``max_power / min_power`` >= 2.0 (clear power asymmetry).
      Flip is most likely an artifact of low power under one engine.
    - **MIXED**: anything else (one engine 0.50-0.80, or both engines
      moderate power, or both have power >=0.80 but neither rejects).

    H1 uses ``bootstrap_p_value`` as significance indicator (< α). H2
    uses |Cohen's d| >= ``h2_magnitude_floor`` because the H2 verdict
    is direction-based rather than p-gated.
    """
    if not per_engine or not all(engine in per_engine for engine in ENGINE_LABELS):
        return EngineFlipDiagnosis(
            hid=hid,
            adjusted_power=float("nan"),
            market_power=float("nan"),
            adjusted_p_or_effect=float("nan"),
            market_p_or_effect=float("nan"),
            adjusted_confidence=float("nan"),
            market_confidence=float("nan"),
            engine_choice_impact=float("nan"),
            classification="MISSING",
            narrative=(
                f"{hid}: both engine inputs are required — cannot diagnose engine flip."
            ),
        )
    adj = per_engine.get("adjusted")
    mkt = per_engine.get("market")
    adj_power = (
        float(adj.power_at_observed)
        if adj is not None and math.isfinite(adj.power_at_observed)
        else float("nan")
    )
    mkt_power = (
        float(mkt.power_at_observed)
        if mkt is not None and math.isfinite(mkt.power_at_observed)
        else float("nan")
    )

    def _significance(report: HypothesisPowerReport | None) -> tuple[float, float]:
        if report is None:
            return (float("nan"), float("nan"))
        if hid == "H1":
            p = float(report.extras.get("bootstrap_p_value", float("nan")))
            indicator = 1.0 if math.isfinite(p) and p < alpha else 0.0
            return (p, indicator)
        # H2: magnitude-based — use |Cohen's d|.
        d = float(report.extras.get("cohens_d", float("nan")))
        if not math.isfinite(d):
            d = float(report.observed_effect)
        magnitude = abs(d) if math.isfinite(d) else float("nan")
        indicator = (
            1.0 if math.isfinite(magnitude) and magnitude >= h2_magnitude_floor
            else 0.0
        )
        return (magnitude, indicator)

    adj_stat, adj_ind = _significance(adj)
    mkt_stat, mkt_ind = _significance(mkt)
    has_signal = bool(adj_ind > 0.0 or mkt_ind > 0.0)

    if hid == "H2":
        adj_effect = float(adj.observed_effect) if adj is not None else float("nan")
        mkt_effect = float(mkt.observed_effect) if mkt is not None else float("nan")
        has_disagreement = (
            math.isfinite(adj_effect)
            and math.isfinite(mkt_effect)
            and adj_effect != 0.0
            and mkt_effect != 0.0
            and math.copysign(1.0, adj_effect) != math.copysign(1.0, mkt_effect)
        )
    else:
        has_disagreement = bool(adj_ind != mkt_ind)

    adj_conf = (
        adj_power * adj_ind if math.isfinite(adj_power * adj_ind) else float("nan")
    )
    mkt_conf = (
        mkt_power * mkt_ind if math.isfinite(mkt_power * mkt_ind) else float("nan")
    )

    if math.isfinite(adj_power) and math.isfinite(mkt_power):
        eps = 1e-6
        max_pow = max(adj_power, mkt_power)
        min_pow = max(min(adj_power, mkt_power), eps)
        impact = float(max_pow / min_pow)
    else:
        impact = float("nan")

    if not has_disagreement:
        classification = "NO_FLIP"
        narrative = (
            f"{hid} shows no adjusted-vs-market verdict flip: "
            f"adjusted_indicator={adj_ind:.0f}, market_indicator={mkt_ind:.0f}. "
            "Do not interpret the engine comparison as a methodological flip."
        )
    elif (
        math.isfinite(adj_power)
        and math.isfinite(mkt_power)
        and adj_power >= 0.80
        and mkt_power >= 0.80
        and has_signal
    ):
        classification = "METHODOLOGY_DRIVEN"
        narrative = (
            f"{hid} flip is METHODOLOGY-DRIVEN: both engines have "
            f"power ≥ 0.80 (adjusted={adj_power:.2f}, "
            f"market={mkt_power:.2f}). The verdict difference is a "
            "real methodological-choice signal, not a power artifact; "
            "paper §5 should report both engines transparently."
        )
    elif (
        math.isfinite(adj_power)
        and math.isfinite(mkt_power)
        and (adj_power < 0.50 or mkt_power < 0.50)
        and impact >= 2.0
    ):
        classification = "POWER_LIMITED"
        narrative = (
            f"{hid} flip is POWER-LIMITED: at least one engine has "
            f"power < 0.50 (adjusted={adj_power:.2f}, "
            f"market={mkt_power:.2f}); engine_choice_impact={impact:.1f}× "
            "indicates the flip likely reflects the under-powered "
            "engine missing the effect, not a real methodological "
            "disagreement. Paper §5 should default to the better-"
            "powered engine."
        )
    else:
        classification = "MIXED"
        narrative = (
            f"{hid} flip is MIXED: power split (adjusted={adj_power:.2f}, "
            f"market={mkt_power:.2f}) sits between strong methodological "
            "and pure power regimes; report both engines + power numbers "
            "in paper §5."
        )
    return EngineFlipDiagnosis(
        hid=hid,
        adjusted_power=adj_power,
        market_power=mkt_power,
        adjusted_p_or_effect=adj_stat,
        market_p_or_effect=mkt_stat,
        adjusted_confidence=float(adj_conf)
        if math.isfinite(adj_conf)
        else float("nan"),
        market_confidence=float(mkt_conf)
        if math.isfinite(mkt_conf)
        else float("nan"),
        engine_choice_impact=impact,
        classification=classification,
        narrative=narrative,
    )


def compare_h1_h2_across_engines(
    h1_inputs: dict[str, dict[str, float]],
    h2_inputs: dict[str, dict[str, Any]],
    *,
    h1_n: int = 436,
    h2_n_combined: int = 17,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> AcrossEnginePowerComparison:
    """Build the cross-engine power-comparison container for H1 + H2.

    Convenience wrapper over :func:`compute_h1_power_per_engine`,
    :func:`compute_h2_power_per_engine`, and
    :func:`diagnose_engine_flip`. Intended as the headline entry-point
    for the CLI and for tests.
    """
    h1_per = compute_h1_power_per_engine(
        h1_inputs, n=h1_n, alpha=alpha, target_power=target_power
    )
    h2_per = compute_h2_power_per_engine(
        h2_inputs,
        n_combined=h2_n_combined,
        alpha=alpha,
        target_power=target_power,
    )
    h1_diag = diagnose_engine_flip("H1", h1_per, alpha=alpha)
    h2_diag = diagnose_engine_flip("H2", h2_per, alpha=alpha)
    return AcrossEnginePowerComparison(
        h1_per_engine=h1_per,
        h2_per_engine=h2_per,
        h1_diagnosis=h1_diag,
        h2_diagnosis=h2_diag,
    )


__all__ = [
    "AcrossEnginePowerComparison",
    "Alternative",
    "BootstrapPowerResult",
    "ENGINE_LABELS",
    "EngineFlipDiagnosis",
    "HypothesisPowerReport",
    "PowerResult",
    "beta_posterior_probability_above",
    "binomial_proportion_power",
    "bootstrap_observed_power",
    "bootstrap_test_power",
    "compare_h1_h2_across_engines",
    "compute_h1_power_per_engine",
    "compute_h2_power_per_engine",
    "compute_h3_power",
    "compute_h4_power",
    "compute_h5_power",
    "compute_h6_power",
    "diagnose_engine_flip",
    "exact_binomial_power",
    "mde_at_power",
    "mde_bootstrap_test",
    "mde_regression_coef",
    "regression_coef_power",
    "t_test_power",
]
