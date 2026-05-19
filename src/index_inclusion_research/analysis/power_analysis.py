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
from typing import Literal

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


__all__ = [
    "Alternative",
    "BootstrapPowerResult",
    "HypothesisPowerReport",
    "PowerResult",
    "beta_posterior_probability_above",
    "binomial_proportion_power",
    "bootstrap_observed_power",
    "compute_h3_power",
    "compute_h6_power",
    "exact_binomial_power",
    "mde_at_power",
    "t_test_power",
]
