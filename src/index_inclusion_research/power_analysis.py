"""CLI for the post-hoc power analysis report (``index-inclusion-power-analysis``).

Renders ``results/real_tables/power_analysis_report.csv`` (machine-
readable) plus an optional markdown twin (``power_analysis_report.md``)
that callers can paste into the paper § 5 limitations or
``docs/limitations.md``.

The CLI reads the published verdicts CSV to recover ``n_obs`` and the
``key_value`` for the two low-n hypotheses (H3 and H6) and then calls
:func:`index_inclusion_research.analysis.power_analysis.compute_h3_power`
and :func:`compute_h6_power` to render the per-hypothesis table. When
the H6 weight-event panel is available on disk, the CLI also derives a
proper heavy-vs-light bucket SD to replace the back-derivation from
r²; otherwise it falls back to a documented default (see the analysis
module docstring).
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
# Default file locations
# ---------------------------------------------------------------------------


def _default_verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_h6_weight_csv() -> Path:
    return paths.processed_data_dir() / "hs300_weight_change.csv"


def _default_gap_event_csv() -> Path:
    return paths.real_tables_dir() / "cma_gap_event_level.csv"


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
    h1_engine_bootstraps: dict[str, Path] | None = None,
    h2_engine_rollings: dict[str, Path] | None = None,
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> list[pa.HypothesisPowerReport]:
    """Compute the per-hypothesis power report list.

    Layout (matches the published CSV row order):

    1. H3 (single row, no engine split) — :func:`pa.compute_h3_power`.
    2. H6 (single row, no engine split) — :func:`pa.compute_h6_power`.
    3. H1 per engine (one row per ``adjusted`` / ``market``) —
       :func:`pa.compute_h1_power_per_engine`. Each row has the
       ``engine`` field populated.
    4. H2 per engine (one row per ``adjusted`` / ``market``).

    Rows 3-4 are silently skipped when the sensitivity CSVs are absent
    so the CLI keeps working in stripped checkouts; the markdown
    renderer surfaces "(engine data missing)" in that case.
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

    reports: list[pa.HypothesisPowerReport] = [h3_report, h6_report]

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
        f"对低-n 假说 (H3, H6) 做 post-hoc 功效计算，α={alpha}, "
        f"target power = {int(target_power * 100)}%。"
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
                if isinstance(v, float) and not np.isnan(v):
                    lines.append(f"- `{k}` = {v:.4f}")
                elif not isinstance(v, float):
                    lines.append(f"- `{k}` = {v}")
            lines.append("")
    lines.append("## 3. 方法学说明")
    lines.append("")
    lines.append(
        "- H3 (n=4) 使用比例 z-test（正态近似）；同时提供 exact-binomial 对照。"
        "因为正态近似在小样本下偏乐观，**只有当两个计算给出相近结论**时才能"
        "把 H3 的判断扣在 normal-approx 上。"
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
            "Post-hoc statistical power analysis for the low-n CMA "
            "hypotheses (H3 n=4, H6 n=67). Writes "
            "results/real_tables/power_analysis_report.csv + markdown "
            "twin; reads cma_hypothesis_verdicts.csv for n_obs and "
            "observed effect-size; falls back to documented defaults "
            "if the verdicts CSV is missing."
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
        help="Print the markdown report to stdout instead of writing to disk.",
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

    if args.print:
        sys.stdout.write(markdown)
        return 0

    csv_path = args.output or _default_csv_output()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info("Wrote power analysis CSV to %s (%d rows)", csv_path, len(df))

    if args.md_output is None or str(args.md_output) != "":
        md_path = args.md_output or _default_md_output()
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(markdown, encoding="utf-8")
        logger.info(
            "Wrote power analysis markdown to %s (%d bytes)",
            md_path,
            md_path.stat().st_size,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
