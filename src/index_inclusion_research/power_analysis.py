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

import numpy as np
import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.analysis import power_analysis as pa

logger = logging.getLogger(__name__)


CSV_COLUMNS: tuple[str, ...] = (
    "hid",
    "name_cn",
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
    pooled_var = (
        (nh - 1) * float(heavy.var(ddof=1))
        + (nl - 1) * float(light.var(ddof=1))
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
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> list[pa.HypothesisPowerReport]:
    """Compute H3 + H6 power rows from the on-disk artifacts.

    Returns a list (H3 first, H6 second). Both reports are always
    computed; if inputs are unavailable they fall back to the documented
    defaults (0.75 hit rate / -0.019 spread).
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

    return [h3_report, h6_report]


def reports_to_dataframe(
    reports: Sequence[pa.HypothesisPowerReport],
) -> pd.DataFrame:
    """Render the list of per-hypothesis reports into a tidy CSV frame."""
    rows: list[dict[str, object]] = []
    for r in reports:
        d = asdict(r)
        extras = d.pop("extras") or {}
        # promote known extras into top-level columns for CSV
        row: dict[str, object] = {
            "hid": d["hid"],
            "name_cn": d["name_cn"],
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
        ):
            row[k] = extras.get(k, float("nan"))
        rows.append(row)
    df = pd.DataFrame(rows, columns=list(CSV_COLUMNS))
    return df


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
