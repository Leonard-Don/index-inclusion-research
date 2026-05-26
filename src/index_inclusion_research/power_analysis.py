"""CLI for the post-hoc power analysis report (``index-inclusion-power-analysis``).

Renders ``results/real_tables/power_analysis_report.csv`` (machine-
readable) plus an optional markdown twin (``power_analysis_report.md``)
that callers can paste into the paper § 5 limitations or
``docs/limitations.md``.

Coverage:

- **H3** (双通道命中率, n=4) — :func:`pa.compute_h3_power` from the
  verdicts CSV.
- **H4** (卖空约束 / gap-drift cn_coef, n=436) — :func:`pa.compute_h4_power`
  from ``cma_gap_drift_market_regression.csv``.
- **H5** (涨跌停限制 / limit-predictive coef, n=936) —
  :func:`pa.compute_h5_power` from
  ``cma_h5_limit_predictive_regression.csv``.
- **H6** (heavy − light spread, n=67) — :func:`pa.compute_h6_power`
  with bucket-SD reconstructed from the on-disk weight panel when
  available, otherwise falling back to the r²-derived |d|≈0.18.
- **H1, H2** per AR engine — bootstrap / rolling-CAR CSVs under
  ``results/sensitivity/ar_{adjusted,market}/``; flip diagnoses appended
  to ``power_analysis_engine_flip.csv``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.analysis import power_analysis as pa

logger = logging.getLogger(__name__)


CSV_COLUMNS: tuple[str, ...] = (
    "hid",
    "name_cn",
    "engine",
    "n_obs",
    "test_family",
    "observed_effect",
    "observed_effect_label",
    "alpha",
    "power_at_observed",
    "mde_at_80_power",
    "mde_label",
    "interpretation",
    "cohens_d_observed",
    "power_at_d_0.20",
    "power_at_d_0.50",
    "power_at_d_0.80",
    "exact_power",
    "bayes_p_gt_0.60",
    "bootstrap_se",
    "bootstrap_p_value",
    "trend_sd",
    "coef_observed",
    "se_observed",
    "t_observed",
    "p_value_observed",
)


# Cross-engine summary CSV columns — appended after the per-hypothesis
# per-engine rows so a single file holds both the detail and the
# diagnosis.
CROSS_ENGINE_COLUMNS: tuple[str, ...] = (
    "hid",
    "engine",
    "n_obs",
    "test_family",
    "observed_effect",
    "observed_effect_label",
    "alpha",
    "power_at_observed",
    "mde_at_80_power",
    "mde_label",
    "interpretation",
)


# Flip-diagnosis CSV (one row per flipping hypothesis).
FLIP_DIAGNOSIS_COLUMNS: tuple[str, ...] = (
    "hid",
    "adjusted_power",
    "market_power",
    "adjusted_p_or_effect",
    "market_p_or_effect",
    "adjusted_confidence",
    "market_confidence",
    "engine_choice_impact",
    "classification",
    "narrative",
)


# ---------------------------------------------------------------------------
# Chinese glosses for ``extras`` keys rendered under ``**额外指标**``.
#
# The raw English keys stay visible (academic readers grep source code by
# these identifiers); we add the gloss in parentheses for the Chinese
# audience. Append-only — if you add an extras key elsewhere in the
# module, add the Chinese label here too, otherwise the renderer will
# tag it with a TODO comment and ship the line without the gloss.
# ---------------------------------------------------------------------------


_EXTRA_INDICATOR_LABELS_ZH: dict[str, str] = {
    # H3 (binomial proportion, exact-binomial + Bayesian extras)
    "exact_power": "精确二项功效",
    "bayes_p_gt_0.60": "后验 P(p>0.60)",
    "bayes_p_gt_0.5": "后验 P(p>0.5)",
    "successes": "成功次数",
    # H4 / H5 (regression-coef t-test extras)
    "coef_observed": "观测系数",
    "se_observed": "系数标准误",
    "t_observed": "t 统计量",
    "p_value_observed": "p 值",
    "n_covariates": "协变量数",
    # H6 (one-sample t-test extras)
    "cohens_d_observed": "Cohen d",
    "power_at_d_0.20": "d=0.20 功效",
    "power_at_d_0.50": "d=0.50 功效",
    "power_at_d_0.80": "d=0.80 功效",
    # H1 (bootstrap-diff extras)
    "bootstrap_se": "Bootstrap 标准误",
    "bootstrap_p_value": "Bootstrap p 值",
    "ci_low": "CI 下界",
    "ci_high": "CI 上界",
    # H2 (rolling-delta t-test extras)
    "cohens_d": "Cohen d",
    "trend_sd": "趋势标准差",
    # Misc — defensive defaults for keys not currently emitted.
    "normal_power": "正态近似功效",
    "effect_size": "效应大小",
}


# ---------------------------------------------------------------------------
# Default file locations
# ---------------------------------------------------------------------------


def _default_verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_h6_weight_csv() -> Path:
    return paths.processed_data_dir() / "hs300_weight_change.csv"


def _default_gap_event_csv() -> Path:
    return paths.real_tables_dir() / "cma_gap_event_level.csv"


def _default_h4_regression_csv() -> Path:
    return paths.real_tables_dir() / "cma_gap_drift_market_regression.csv"


def _default_h5_regression_csv() -> Path:
    return paths.real_tables_dir() / "cma_h5_limit_predictive_regression.csv"


def _default_csv_output() -> Path:
    return paths.real_tables_dir() / "power_analysis_report.csv"


def _default_md_output() -> Path:
    return paths.real_tables_dir() / "power_analysis_report.md"


def _default_engine_bootstrap_csv(engine: str) -> Path:
    """Per-engine ``cma_pre_runup_bootstrap.csv`` under the sensitivity cache."""
    return paths.sensitivity_dir(f"ar_{engine}") / "cma_pre_runup_bootstrap.csv"


def _default_engine_rolling_csv(engine: str) -> Path:
    """Per-engine ``cma_time_series_rolling.csv`` under the sensitivity cache."""
    return paths.sensitivity_dir(f"ar_{engine}") / "cma_time_series_rolling.csv"


def _default_flip_diagnosis_csv() -> Path:
    return paths.real_tables_dir() / "power_analysis_engine_flip.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except (OSError, ValueError) as exc:
        logger.warning("Failed to read %s: %s", path, exc)
        return None


def _h3_inputs_from_verdicts(
    verdicts_df: pd.DataFrame | None,
) -> tuple[float, int]:
    """Return ``(observed_hit_rate, n_obs)`` for H3 with sensible defaults."""
    if verdicts_df is None or verdicts_df.empty:
        return (0.75, 4)
    h3 = verdicts_df.loc[verdicts_df["hid"] == "H3"]
    if h3.empty:
        return (0.75, 4)
    row = h3.iloc[0]
    try:
        hit_rate = float(row["key_value"])
    except (TypeError, ValueError, KeyError):
        hit_rate = 0.75
    try:
        n = int(row["n_obs"])
    except (TypeError, ValueError, KeyError):
        n = 4
    return (hit_rate, n)


def _h6_inputs_from_verdicts(
    verdicts_df: pd.DataFrame | None,
) -> tuple[float, int]:
    """Return ``(observed_spread, n_obs)`` for H6 with sensible defaults."""
    if verdicts_df is None or verdicts_df.empty:
        return (-0.019, 67)
    h6 = verdicts_df.loc[verdicts_df["hid"] == "H6"]
    if h6.empty:
        return (-0.019, 67)
    row = h6.iloc[0]
    try:
        spread = float(row["key_value"])
    except (TypeError, ValueError, KeyError):
        spread = -0.019
    try:
        n = int(row["n_obs"])
    except (TypeError, ValueError, KeyError):
        n = 67
    return (spread, n)


def _h4_inputs_from_regression(
    *,
    regression_csv: Path | None = None,
    verdicts_df: pd.DataFrame | None = None,
) -> dict[str, float] | None:
    """Read H4 gap-drift regression CSV → inputs for :func:`pa.compute_h4_power`.

    Pulls the headline ``cn_coef`` row from
    ``results/real_tables/cma_gap_drift_market_regression.csv``. Falls
    back to the cn_p_value column in the verdicts CSV when the
    regression CSV is missing — but only the p-value carries across, so
    callers should treat the regression CSV as the canonical source.

    Returns ``None`` when neither source is available; the CLI then
    silently skips H4 (matching the H1 / H2 pattern in stripped checkouts).
    """
    csv = regression_csv or _default_h4_regression_csv()
    df = _safe_read_csv(csv)
    if df is not None and not df.empty:
        needed = ("cn_coef", "cn_se", "cn_p_value", "n_obs")
        if all(col in df.columns for col in needed):
            row = df.iloc[0]
            try:
                return {
                    "coef": float(row["cn_coef"]),
                    "se": float(row["cn_se"]),
                    "p_value": float(row["cn_p_value"]),
                    "n": float(row["n_obs"]),
                }
            except (TypeError, ValueError):
                pass
    # Fallback to verdicts CSV (only p_value + n; coef + se are not
    # surfaced there, so we cannot compute power without the regression
    # CSV).
    if verdicts_df is not None and not verdicts_df.empty:
        h4 = verdicts_df.loc[verdicts_df["hid"] == "H4"]
        if not h4.empty:
            row = h4.iloc[0]
            try:
                p_value = float(row["p_value"])
                n = int(row["n_obs"])
            except (TypeError, ValueError, KeyError):
                return None
            return {"p_value": p_value, "n": float(n)}
    return None


def _h5_inputs_from_regression(
    *,
    regression_csv: Path | None = None,
    verdicts_df: pd.DataFrame | None = None,
) -> dict[str, float] | None:
    """Read H5 limit-predictive regression CSV → inputs for
    :func:`pa.compute_h5_power`.

    Pulls the ``limit_coef`` / ``limit_se`` / ``limit_p_value`` / ``n_obs``
    row from ``results/real_tables/cma_h5_limit_predictive_regression.csv``.
    Falls back to the verdicts CSV's ``p_value`` + ``n_obs`` when the
    regression CSV is missing; like H4 we cannot compute power without
    the SE, so the fallback yields a partial payload that callers must
    treat as missing.
    """
    csv = regression_csv or _default_h5_regression_csv()
    df = _safe_read_csv(csv)
    if df is not None and not df.empty:
        needed = ("limit_coef", "limit_se", "limit_p_value", "n_obs")
        if all(col in df.columns for col in needed):
            row = df.iloc[0]
            try:
                return {
                    "coef": float(row["limit_coef"]),
                    "se": float(row["limit_se"]),
                    "p_value": float(row["limit_p_value"]),
                    "n": float(row["n_obs"]),
                }
            except (TypeError, ValueError):
                pass
    if verdicts_df is not None and not verdicts_df.empty:
        h5 = verdicts_df.loc[verdicts_df["hid"] == "H5"]
        if not h5.empty:
            row = h5.iloc[0]
            try:
                p_value = float(row["p_value"])
                n = int(row["n_obs"])
            except (TypeError, ValueError, KeyError):
                return None
            return {"p_value": p_value, "n": float(n)}
    return None


def _h1_inputs_from_engine_bootstrap(
    engine: str,
    *,
    bootstrap_csv: Path | None = None,
) -> dict[str, float] | None:
    """Read per-engine pre-runup bootstrap CSV → inputs for
    :func:`pa.compute_h1_power_per_engine`.

    Returns ``None`` when the CSV is missing or required columns absent.
    """
    csv = bootstrap_csv or _default_engine_bootstrap_csv(engine)
    df = _safe_read_csv(csv)
    if df is None or df.empty:
        return None
    row = df.iloc[0]
    needed = ("diff_mean", "boot_ci_low", "boot_ci_high", "boot_p_value")
    if not all(col in df.columns for col in needed):
        return None
    try:
        diff = float(row["diff_mean"])
        ci_low = float(row["boot_ci_low"])
        ci_high = float(row["boot_ci_high"])
        p = float(row["boot_p_value"])
    except (TypeError, ValueError):
        return None
    sample_counts: list[int] = []
    for col in ("n_cn", "n_us"):
        raw_n = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
        if pd.notna(raw_n):
            sample_counts.append(int(raw_n))
    out = {
        "diff_mean": diff,
        "boot_ci_low": ci_low,
        "boot_ci_high": ci_high,
        "boot_p_value": p,
    }
    if len(sample_counts) == 2 and sum(sample_counts) > 0:
        out["n_total"] = float(sum(sample_counts))
    return out


def _h2_inputs_from_engine_rolling(
    engine: str,
    *,
    rolling_csv: Path | None = None,
) -> dict[str, Any] | None:
    """Read per-engine rolling CSV → inputs for
    :func:`pa.compute_h2_power_per_engine`.

    Builds the year-over-year *effective*-phase ``car_mean`` delta
    vector for US + CN markets pooled. ``n_combined`` is the total
    number of pooled year-over-year deltas (≈ n_roll_US + n_roll_CN − 2).
    """
    csv = rolling_csv or _default_engine_rolling_csv(engine)
    df = _safe_read_csv(csv)
    if df is None or df.empty:
        return None
    needed = ("market", "event_phase", "window_end_year", "car_mean")
    if not all(col in df.columns for col in needed):
        return None
    eff = df.loc[df["event_phase"] == "effective"].copy()
    if eff.empty:
        return None
    eff["car_mean"] = pd.to_numeric(eff["car_mean"], errors="coerce")
    deltas: list[float] = []
    n_total_deltas = 0
    for _market, group in eff.groupby("market"):
        ordered = group.sort_values("window_end_year")
        vals = ordered["car_mean"].to_numpy(dtype=float)
        if vals.size < 2:
            continue
        d = np.diff(vals)
        d = d[np.isfinite(d)]
        n_total_deltas += int(d.size)
        deltas.extend(float(x) for x in d.tolist())
    if len(deltas) < 2:
        return None
    return {
        "deltas": deltas,
        "n_combined": float(n_total_deltas),
    }


def _h6_bucket_stats_from_panel(
    weight_csv: Path,
    events_csv: Path,
) -> dict[str, float] | None:
    """Reconstruct heavy/light bucket means + pooled SD from the on-disk panel.

    Returns ``None`` if either input is missing; callers fall back to
    the r²-derived default and surface that fact in the interpretation
    text. The split is median-of-``weight_proxy`` (matches the verdict
    CSV's "heavy / light" terminology — n=33+34 = 67 events).
    """
    weights = _safe_read_csv(weight_csv)
    events = _safe_read_csv(events_csv)
    if weights is None or events is None:
        return None
    if not {"market", "ticker", "announce_date", "weight_proxy"}.issubset(
        weights.columns
    ):
        return None
    if not {"market", "ticker", "announce_date", "announce_jump"}.issubset(
        events.columns
    ):
        return None
    w = weights.copy()
    e = events.copy()
    w["market"] = w["market"].astype(str).str.upper()
    e["market"] = e["market"].astype(str).str.upper()
    w["ticker"] = w["ticker"].astype(str).str.zfill(6)
    e["ticker"] = e["ticker"].astype(str).str.zfill(6)
    w["announce_date"] = pd.to_datetime(
        w["announce_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    e["announce_date"] = pd.to_datetime(
        e["announce_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    w = w.loc[w["market"] == "CN"]
    e = e.loc[e["market"] == "CN", ["market", "ticker", "announce_date", "announce_jump"]]
    merged = w.merge(
        e, on=["market", "ticker", "announce_date"], how="inner"
    ).dropna(subset=["weight_proxy", "announce_jump"])
    if merged.empty:
        return None
    med = float(merged["weight_proxy"].median())
    heavy = merged.loc[merged["weight_proxy"] >= med, "announce_jump"].astype(float)
    light = merged.loc[merged["weight_proxy"] < med, "announce_jump"].astype(float)
    if len(heavy) < 2 or len(light) < 2:
        return None
    nh = len(heavy)
    nl = len(light)
    # Pooled SD for two-sample t (equal variance).
    heavy_var = float(cast(Any, heavy.var(ddof=1)))
    light_var = float(cast(Any, light.var(ddof=1)))
    pooled_var = (
        (nh - 1) * heavy_var
        + (nl - 1) * light_var
    ) / max(nh + nl - 2, 1)
    pooled_sd = float(np.sqrt(pooled_var))
    return {
        "n_heavy": float(nh),
        "n_light": float(nl),
        "heavy_mean": float(heavy.mean()),
        "light_mean": float(light.mean()),
        "pooled_sd": pooled_sd,
        "spread_observed": float(heavy.mean() - light.mean()),
        "announce_jump_sd": float(
            merged["announce_jump"].astype(float).std(ddof=1)
        ),
    }


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------


def build_power_report_rows(
    *,
    verdicts_csv: Path | None = None,
    h6_weight_csv: Path | None = None,
    h6_events_csv: Path | None = None,
    h4_regression_csv: Path | None = None,
    h5_regression_csv: Path | None = None,
    h1_engine_bootstraps: dict[str, Path] | None = None,
    h2_engine_rollings: dict[str, Path] | None = None,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> list[pa.HypothesisPowerReport]:
    """Compute the per-hypothesis power report list.

    Layout (matches the published CSV row order):

    1. H3 (single row, no engine split) — :func:`pa.compute_h3_power`.
    2. H4 (single row, regression-coef t-test) — :func:`pa.compute_h4_power`.
    3. H5 (single row, regression-coef t-test) — :func:`pa.compute_h5_power`.
    4. H6 (single row, no engine split) — :func:`pa.compute_h6_power`.
    5. H1 per engine (one row per ``adjusted`` / ``market``) —
       :func:`pa.compute_h1_power_per_engine`. Each row has the
       ``engine`` field populated.
    6. H2 per engine (one row per ``adjusted`` / ``market``).

    Rows 5-6 are silently skipped when the sensitivity CSVs are absent
    so the CLI keeps working in stripped checkouts; the markdown
    renderer surfaces "(engine data missing)" in that case. Rows 2-3
    (H4 / H5) are skipped when the regression CSVs are missing AND the
    verdicts CSV does not carry a fallback p-value + n; otherwise the
    function returns the row anyway with ``NaN`` power so reviewers see
    that the hypothesis was at least looked at.
    """
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    h6_weight_csv = h6_weight_csv or _default_h6_weight_csv()
    h6_events_csv = h6_events_csv or _default_gap_event_csv()

    verdicts_df = _safe_read_csv(verdicts_csv)

    h3_hit, h3_n = _h3_inputs_from_verdicts(verdicts_df)
    h6_spread, h6_n = _h6_inputs_from_verdicts(verdicts_df)

    h3_report = pa.compute_h3_power(
        observed_hit_rate=h3_hit,
        n=h3_n,
        alpha=alpha,
        target_power=target_power,
    )

    h6_bucket = _h6_bucket_stats_from_panel(h6_weight_csv, h6_events_csv)
    if h6_bucket is not None:
        h6_report = pa.compute_h6_power(
            observed_spread=h6_bucket["spread_observed"],
            heavy_jump_mean=h6_bucket["heavy_mean"],
            light_jump_mean=h6_bucket["light_mean"],
            bucket_sd=h6_bucket["pooled_sd"],
            n=h6_n,
            alpha=alpha,
            target_power=target_power,
        )
    else:
        h6_report = pa.compute_h6_power(
            observed_spread=h6_spread,
            n=h6_n,
            alpha=alpha,
            target_power=target_power,
        )

    reports: list[pa.HypothesisPowerReport] = [h3_report]

    # ── H4 (gap-drift cn_coef t-test) ─────────────────────────────
    h4_inputs = _h4_inputs_from_regression(
        regression_csv=h4_regression_csv, verdicts_df=verdicts_df
    )
    if h4_inputs is not None and "coef" in h4_inputs and "se" in h4_inputs:
        reports.append(
            pa.compute_h4_power(
                coef=h4_inputs["coef"],
                se=h4_inputs["se"],
                p_value=h4_inputs["p_value"],
                n=int(h4_inputs["n"]),
                alpha=alpha,
                target_power=target_power,
            )
        )

    # ── H5 (limit-predictive coef t-test) ─────────────────────────
    h5_inputs = _h5_inputs_from_regression(
        regression_csv=h5_regression_csv, verdicts_df=verdicts_df
    )
    if h5_inputs is not None and "coef" in h5_inputs and "se" in h5_inputs:
        reports.append(
            pa.compute_h5_power(
                coef=h5_inputs["coef"],
                se=h5_inputs["se"],
                p_value=h5_inputs["p_value"],
                n=int(h5_inputs["n"]),
                alpha=alpha,
                target_power=target_power,
            )
        )

    reports.append(h6_report)

    # ── H1 per engine ────────────────────────────────────────────
    h1_inputs: dict[str, dict[str, float]] = {}
    for engine in pa.ENGINE_LABELS:
        path = (
            h1_engine_bootstraps.get(engine)
            if h1_engine_bootstraps
            else None
        )
        inputs = _h1_inputs_from_engine_bootstrap(engine, bootstrap_csv=path)
        if inputs is not None:
            h1_inputs[engine] = inputs
    if h1_inputs:
        h1_per_engine = pa.compute_h1_power_per_engine(
            h1_inputs, alpha=alpha, target_power=target_power
        )
        for engine in pa.ENGINE_LABELS:
            if engine in h1_per_engine:
                reports.append(h1_per_engine[engine])

    # ── H2 per engine ────────────────────────────────────────────
    h2_inputs: dict[str, dict[str, Any]] = {}
    for engine in pa.ENGINE_LABELS:
        path = (
            h2_engine_rollings.get(engine) if h2_engine_rollings else None
        )
        inputs = _h2_inputs_from_engine_rolling(engine, rolling_csv=path)
        if inputs is not None:
            h2_inputs[engine] = inputs
    if h2_inputs:
        h2_per_engine = pa.compute_h2_power_per_engine(
            h2_inputs, alpha=alpha, target_power=target_power
        )
        for engine in pa.ENGINE_LABELS:
            if engine in h2_per_engine:
                reports.append(h2_per_engine[engine])

    return reports


def build_engine_flip_diagnoses(
    reports: Sequence[pa.HypothesisPowerReport],
    *,
    alpha: float = 0.05,
) -> list[pa.EngineFlipDiagnosis]:
    """Aggregate per-engine reports → per-hypothesis flip diagnoses.

    Inputs: the per-hypothesis report list (output of
    :func:`build_power_report_rows`). Output: one diagnosis per
    flipping hypothesis (H1, H2) only when both canonical engine rows are
    present. A single-engine row is not a flip and is skipped so reports
    do not imply a comparison that was not actually computed.
    """
    out: list[pa.EngineFlipDiagnosis] = []
    for hid in ("H1", "H2"):
        per_engine = {
            r.engine: r
            for r in reports
            if r.hid == hid and r.engine
        }
        if not all(engine in per_engine for engine in pa.ENGINE_LABELS):
            continue
        out.append(pa.diagnose_engine_flip(hid, per_engine, alpha=alpha))
    return out


def reports_to_dataframe(
    reports: Sequence[pa.HypothesisPowerReport],
) -> pd.DataFrame:
    """Render the list of per-hypothesis reports into a tidy CSV frame.

    Column layout (see :data:`CSV_COLUMNS`):

    - Identity (hid, name_cn, engine, n_obs)
    - Headline numbers (test_family, observed_effect, alpha,
      power_at_observed, mde_at_80_power, mde_label, interpretation)
    - H3/H6-specific extras (cohens_d_observed, power_at_d_*,
      exact_power, bayes_p_gt_0.60)
    - H1/H2-specific extras (bootstrap_se, bootstrap_p_value, trend_sd)

    Per-engine H1/H2 rows leave H3/H6 columns NaN and vice versa, so
    pandas readers can ``groupby("hid")`` cleanly.
    """
    rows: list[dict[str, object]] = []
    for r in reports:
        d = asdict(r)
        extras = d.pop("extras") or {}
        # promote known extras into top-level columns for CSV
        row: dict[str, object] = {
            "hid": d["hid"],
            "name_cn": d["name_cn"],
            "engine": d.get("engine", ""),
            "n_obs": d["n_obs"],
            "test_family": d["test_family"],
            "observed_effect": d["observed_effect"],
            "observed_effect_label": d["observed_effect_label"],
            "alpha": d["alpha"],
            "power_at_observed": d["power_at_observed"],
            "mde_at_80_power": d["mde_at_80_power"],
            "mde_label": d["mde_label"],
            "interpretation": d["interpretation"],
        }
        for k in (
            "cohens_d_observed",
            "power_at_d_0.20",
            "power_at_d_0.50",
            "power_at_d_0.80",
            "exact_power",
            "bayes_p_gt_0.60",
            "bootstrap_se",
            "bootstrap_p_value",
            "trend_sd",
            "coef_observed",
            "se_observed",
            "t_observed",
            "p_value_observed",
        ):
            row[k] = extras.get(k, float("nan"))
        rows.append(row)
    df = pd.DataFrame(rows, columns=list(CSV_COLUMNS))
    return df


def diagnoses_to_dataframe(
    diagnoses: Sequence[pa.EngineFlipDiagnosis],
) -> pd.DataFrame:
    """Render engine-flip diagnoses into a tidy CSV frame.

    See :data:`FLIP_DIAGNOSIS_COLUMNS` for the column layout. One row
    per flipping hypothesis; ``classification`` ∈ {METHODOLOGY_DRIVEN,
    POWER_LIMITED, MIXED, MISSING, NO_FLIP}.
    """
    rows: list[dict[str, object]] = []
    for d in diagnoses:
        rows.append(
            {
                "hid": d.hid,
                "adjusted_power": d.adjusted_power,
                "market_power": d.market_power,
                "adjusted_p_or_effect": d.adjusted_p_or_effect,
                "market_p_or_effect": d.market_p_or_effect,
                "adjusted_confidence": d.adjusted_confidence,
                "market_confidence": d.market_confidence,
                "engine_choice_impact": d.engine_choice_impact,
                "classification": d.classification,
                "narrative": d.narrative,
            }
        )
    return pd.DataFrame(rows, columns=list(FLIP_DIAGNOSIS_COLUMNS))


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def render_markdown(
    reports: Sequence[pa.HypothesisPowerReport],
    *,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> str:
    """Render a paper-ready markdown twin of the CSV.

    The markdown is designed to drop straight into ``docs/limitations.md``
    § 7 or the paper § 5 limitations: one summary table, one
    per-hypothesis paragraph with the interpretation text.
    """
    lines: list[str] = []
    lines.append("# 假说后验统计功效分析")
    lines.append("")
    lines.append(
        f"对各假说做 post-hoc 功效计算 (H3 / H4 / H5 / H6 单口径 + H1 / H2 "
        f"分引擎)，α={alpha}, target power = {int(target_power * 100)}%。"
    )
    lines.append("")
    lines.append("## 1. 功效一览表")
    lines.append("")
    lines.append(
        "| 假说 | 名称 | n | 测试族 | 观测效应 | "
        "在观测效应下的功效 | 80% 功效下的 MDE |"
    )
    lines.append("|---|---|---:|---|---:|---:|---:|")
    for r in reports:
        eff_label = r.observed_effect_label
        eff = r.observed_effect
        mde = r.mde_at_80_power
        lines.append(
            f"| {r.hid} | {r.name_cn} | {r.n_obs} | "
            f"{r.test_family} | {eff:+.3f} ({eff_label}) | "
            f"{r.power_at_observed:.3f} | "
            f"{mde:.3f} ({r.mde_label}) |"
        )
    lines.append("")
    lines.append("## 2. 逐假说释义")
    lines.append("")
    for r in reports:
        lines.append(f"### {r.hid} · {r.name_cn} (n={r.n_obs})")
        lines.append("")
        lines.append(r.interpretation)
        lines.append("")
        if r.extras:
            lines.append("**额外指标**:")
            lines.append("")
            for k, v in r.extras.items():
                gloss = _EXTRA_INDICATOR_LABELS_ZH.get(k)
                # TODO(power-analysis-extras): if ``gloss is None``, add a
                # Chinese label for the key to ``_EXTRA_INDICATOR_LABELS_ZH``
                # so the **额外指标** bullet ships with both the raw key
                # and a Chinese gloss for non-academic readers. The dict
                # is append-only.
                suffix = f" ({gloss})" if gloss else ""
                if isinstance(v, float) and not np.isnan(v):
                    lines.append(f"- `{k}`{suffix} = {v:.4f}")
                elif not isinstance(v, float):
                    lines.append(f"- `{k}`{suffix} = {v}")
            lines.append("")
    lines.append("## 3. 方法学说明")
    lines.append("")
    lines.append(
        "- H3 (n=4) 使用比例 z-test（正态近似）；同时提供 exact-binomial 对照。"
        "因为正态近似在小样本下偏乐观，**只有当两个计算给出相近结论**时才能"
        "把 H3 的判断扣在 normal-approx 上。"
    )
    lines.append(
        "- H4 (n=436) 与 H5 (n=936) 使用 HC3 回归单系数 t-test：观测 "
        "``coef/SE`` 作为非中心 t 的 ncp，``df = n − k − 1`` (k=协变量数)。"
        "MDE 是 ``80% 功效下能检出的最小 |coef|``，由非中心 t 反演的二分搜索"
        "给出；它和闭式 ``(z_{1-α/2}+z_{power})·SE`` 在 n 足够大时一致。"
    )
    lines.append(
        "- H6 (n=67) 使用单样本 t-test，Cohen's *d* = mean / SD。"
        "在面板可用时以中位数 weight 切重/轻 bucket 并算 pooled SD；"
        "面板缺失时，回退到 H6 OLS-HC3 r²=0.033 反推的 |d|≈0.18。"
    )
    lines.append(
        "- 80% 功效下的 MDE 由二分搜索求解；当 n 太小，returned MDE 可能"
        "超过实际可能的效应上界（H3 即如此：MDE≈0.50 意味着只有 p1≈1.0 才能在 80% 功效下检出）。"
    )
    lines.append(
        "- Bayesian 后验 (H3) 默认采用 uniform Beta(1,1) 先验；先验是 Bayesian "
        "陈述里最有争议的输入，更换需明确说明。"
    )
    lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index-inclusion-power-analysis",
        description=(
            "Post-hoc statistical power analysis for the CMA hypotheses "
            "(H3 n=4, H4 n=436, H5 n=936, H6 n=67; plus H1 / H2 per AR "
            "engine). Writes results/real_tables/power_analysis_report.csv, "
            "power_analysis_engine_flip.csv, and a markdown twin; reads the "
            "published regression / verdict CSVs for observed effect-size "
            "and SE; falls back to documented defaults if a regression CSV "
            "is missing."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Destination CSV path "
            "(default: results/real_tables/power_analysis_report.csv)."
        ),
    )
    parser.add_argument(
        "--md-output",
        type=Path,
        default=None,
        help=(
            "Destination markdown twin "
            "(default: results/real_tables/power_analysis_report.md). "
            "Pass an empty string to skip writing the markdown."
        ),
    )
    parser.add_argument(
        "--flip-output",
        type=Path,
        default=None,
        help=(
            "Destination engine-flip diagnostic CSV "
            "(default: results/real_tables/power_analysis_engine_flip.csv)."
        ),
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.05,
        help="Significance level (default 0.05).",
    )
    parser.add_argument(
        "--target-power",
        type=float,
        default=0.80,
        help="Target power for MDE inversion (default 0.80).",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help=(
            "Deprecated no-op: the markdown body now prints to stdout by "
            "default. The flag is retained for backward compatibility "
            "with cron jobs that pass it explicitly. Pass ``--quiet`` to "
            "suppress the stdout body."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help=(
            "Suppress the markdown body on stdout. The ``INFO`` log "
            "lines for generated artifact paths are still emitted so cron "
            "logs show where the CSV, markdown, and engine-flip files landed."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    args = _build_arg_parser().parse_args(argv)

    reports = build_power_report_rows(
        alpha=args.alpha,
        target_power=args.target_power,
    )
    df = reports_to_dataframe(reports)
    markdown = render_markdown(
        reports, alpha=args.alpha, target_power=args.target_power
    )

    csv_path = args.output or _default_csv_output()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info("Wrote power analysis CSV to %s (%d rows)", csv_path, len(df))

    diagnoses = build_engine_flip_diagnoses(reports, alpha=args.alpha)
    flip_df = diagnoses_to_dataframe(diagnoses)
    flip_path = args.flip_output or _default_flip_diagnosis_csv()
    flip_path.parent.mkdir(parents=True, exist_ok=True)
    flip_df.to_csv(flip_path, index=False)
    logger.info(
        "Wrote engine-flip diagnostics CSV to %s (%d rows)",
        flip_path,
        len(flip_df),
    )

    if args.md_output is None or str(args.md_output) != "":
        md_path = args.md_output or _default_md_output()
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(markdown, encoding="utf-8")
        logger.info(
            "Wrote power analysis markdown to %s (%d bytes)",
            md_path,
            md_path.stat().st_size,
        )

    # Default behaviour: also print the markdown body to stdout so a no-
    # args invocation surfaces the report without forcing the user to
    # ``cat`` the file. ``--quiet`` suppresses this for cron jobs; the
    # legacy ``--print`` flag is a no-op (kept for backward compat).
    if not args.quiet:
        sys.stdout.write(markdown)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
