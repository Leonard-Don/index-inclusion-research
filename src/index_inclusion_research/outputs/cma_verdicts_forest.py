"""Publication-quality forest plot of the 7 CMA hypothesis verdicts.

The CMA pipeline writes ``results/real_tables/cma_hypothesis_verdicts.csv``
with 7 heterogeneous rows (H1-H7). Each hypothesis uses a different test
statistic (bootstrap p, regression coefficient, ratio difference, dual
channel hit rate, sector spread), so the rows cannot share a literal
x-axis. To make the seven verdicts visually comparable on a single
figure, this module derives a presentation-only "support-strength score"
in [0, 1] from ``verdict`` + ``confidence`` and renders the seven
hypotheses on the y-axis with strength on the x-axis.

The score is **purely presentational**. It is not used to derive new
statistical claims — the underlying verdict logic is unchanged. The
figure caption documents the derivation so reviewers can re-trace it.

Aesthetics mirror the HS300 RDD robustness forest plot
(:mod:`index_inclusion_research.outputs.hs300_rdd_forest`):
Songti SC font stack, axis-relative monospace annotation column on the
right margin, dashed reference line for the "0% / 50% / 100%" strength
ticks, fig-level subtitle and provenance footer.
"""

from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

# Canonical hypothesis ordering — matches the PAP / verdict CSV (H1-H7).
# Anchors the eye at H1 (top) and walks down to H7 so the figure reads
# in the same order as the paper text.
_HYPOTHESIS_ORDER: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")

# Score lookup table for (verdict, confidence) → support strength.
# Documented in the figure caption. Anything not in this table maps to
# 0.0 with a warning logged on the row annotation.
_STRENGTH_TABLE: dict[tuple[str, str], float] = {
    ("支持", "高"): 1.0,
    ("支持", "中"): 0.7,
    ("支持", "低"): 0.55,
    ("部分支持", "高"): 0.6,
    ("部分支持", "中"): 0.5,
    ("部分支持", "低"): 0.4,
    ("证据不足", "高"): 0.35,
    ("证据不足", "中"): 0.3,
    ("证据不足", "低"): 0.0,
}

# Evidence-tier colour coding. core = paper main text (saturated blue),
# supplementary = appendix (cool gray). Matches the HS300 forest palette
# family so the two figures sit comfortably side-by-side in the paper.
_TIER_COLORS: dict[str, str] = {
    "core": "#0f5c6e",
    "supplementary": "#5c6b77",
}
_TIER_LABELS: dict[str, str] = {
    "core": "正文 core",
    "supplementary": "附录 supplementary",
}

# Sample-period label kept generic — the verdicts CSV spans multiple
# data sources (event-level CN/US, time-series rolling 5/12, sector
# heterogeneity) so a single window string would be misleading.
_SUBTITLE = "证据强度 = f(verdict, confidence)；置信区间宽度 ∝ 1/√n（样本越小区间越宽）"


def classify_strength(verdict: str, confidence: str) -> float:
    """Return the [0, 1] support strength for a (verdict, confidence) pair.

    Public helper so tests and downstream callers can verify a known
    cell without rebuilding the entire plot pipeline.
    """
    key = (str(verdict).strip(), str(confidence).strip())
    return _STRENGTH_TABLE.get(key, 0.0)


def _tier_color(tier: str) -> str:
    return _TIER_COLORS.get(str(tier).strip(), _TIER_COLORS["supplementary"])


def _tier_label(tier: str) -> str:
    return _TIER_LABELS.get(str(tier).strip(), str(tier).strip() or "supplementary")


def _ci_half_width(n: float | int, *, scale: float = 0.18) -> float:
    """Return half-width of the CI whisker as a function of sample size.

    The whisker is intentionally a **visual uncertainty proxy**, not a
    statistical CI. Width shrinks as ``1/sqrt(max(n, 1))`` so degenerate
    n=0 doesn't crash and small-n rows visibly stretch wider than
    large-n rows. The ``scale`` constant is calibrated so n=4 (H3
    smallest) renders a clearly visible whisker while n=936 (H5
    largest) renders a near-point.
    """
    try:
        n_val = float(n)
    except (TypeError, ValueError):
        return scale
    if not (n_val > 0):
        return scale
    return scale / (n_val**0.5)


# Map Chinese verdict / confidence labels to ASCII tokens so the right
# margin annotation column can use a monospace font (matching the HS300
# forest aesthetic) without tripping CJK glyph warnings.
_VERDICT_ASCII: dict[str, str] = {
    "支持": "support",
    "部分支持": "partial",
    "证据不足": "insufficient",
}
_CONFIDENCE_ASCII: dict[str, str] = {
    "高": "hi",
    "中": "mid",
    "低": "lo",
}


def _format_headline(row: pd.Series) -> str:
    """Build the right-margin annotation: 'n=N | tier | verdict/conf'.

    The annotation column is rendered in monospace to match the HS300
    forest plot. Chinese verdict / confidence values are mapped to
    ASCII tokens so the monospace font (no CJK coverage) doesn't emit
    glyph-missing warnings; the y-axis label and figure caption carry
    the Chinese-language context.
    """
    n_raw = row.get("n_obs", float("nan"))
    try:
        n_str = f"n={int(float(n_raw))}"
    except (TypeError, ValueError):
        n_str = "n=NA"
    tier = str(row.get("evidence_tier", "")).strip() or "supp"
    tier_short = "core" if tier == "core" else "supp"
    verdict = str(row.get("verdict", "")).strip()
    verdict_ascii = _VERDICT_ASCII.get(verdict, "unknown")
    conf = str(row.get("confidence", "")).strip()
    conf_ascii = _CONFIDENCE_ASCII.get(conf, conf if conf.isascii() else "?")
    conf_str = f"/{conf_ascii}" if conf else ""
    return f"{n_str} | tier={tier_short} | {verdict_ascii}{conf_str}"


def _prepare_panel(df: pd.DataFrame) -> pd.DataFrame:
    """Validate columns and normalize the panel to a fixed H1-H7 order.

    Missing hypotheses are surfaced as a ``ValueError`` so the figure
    refresh fails loudly rather than silently skipping rows.
    """
    required = {"verdict", "confidence", "evidence_tier", "n_obs"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"verdicts CSV missing required columns: {sorted(missing)}; "
            "regenerate via index-inclusion-cma"
        )

    # Allow either ``hid`` (CMA verdicts CSV) or ``hypothesis`` (older
    # fixtures). Normalise to a single column so the rest of the
    # pipeline is column-agnostic.
    hid_col = "hid" if "hid" in df.columns else ("hypothesis" if "hypothesis" in df.columns else None)
    if hid_col is None:
        raise ValueError(
            "verdicts CSV missing hypothesis identifier column: expected 'hid' or 'hypothesis'"
        )

    panel = df.copy()
    panel["_hid"] = panel[hid_col].astype(str).str.strip().str.upper()
    # Surface missing hypotheses as a finding — don't silently skip.
    present = set(panel["_hid"].tolist())
    expected = set(_HYPOTHESIS_ORDER)
    missing_hids = expected - present
    if missing_hids:
        raise ValueError(
            f"verdicts CSV missing hypotheses: {sorted(missing_hids)}; "
            "expected H1-H7 (rerun index-inclusion-cma to regenerate)"
        )

    panel["_strength"] = panel.apply(
        lambda r: classify_strength(r.get("verdict", ""), r.get("confidence", "")),
        axis=1,
    )
    panel["_color"] = panel["evidence_tier"].apply(_tier_color)
    panel["_tier_label"] = panel["evidence_tier"].apply(_tier_label)
    panel["_annotation"] = panel.apply(_format_headline, axis=1)

    # Lookup name from either ``name_cn`` (canonical) or ``name``
    # (fixture-friendly fallback) so test fixtures don't need both.
    name_col = "name_cn" if "name_cn" in panel.columns else ("name" if "name" in panel.columns else None)
    if name_col is None:
        panel["_label"] = panel["_hid"]
    else:
        panel["_label"] = panel.apply(
            lambda r: f"{r['_hid']} · {str(r[name_col]).strip()}" if str(r[name_col]).strip() else r["_hid"],
            axis=1,
        )

    # Sort to the canonical H1-H7 order (top of figure = H1).
    order_lookup = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}
    panel["_order"] = panel["_hid"].map(order_lookup)
    panel = panel.sort_values("_order", kind="mergesort").reset_index(drop=True)
    return panel


def build_cma_verdicts_forest_plot(
    verdicts_csv_path: str | Path,
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the cross-hypothesis CMA verdicts forest plot.

    Parameters
    ----------
    verdicts_csv_path:
        Path to ``results/real_tables/cma_hypothesis_verdicts.csv`` (or
        a synthetic fixture in tests). Must contain all 7 hypotheses
        H1-H7; a missing hypothesis is treated as a hard failure rather
        than a silent skip.
    output_png_path:
        Destination PNG. Parent directory is created if missing.
    output_pdf_path:
        Optional companion PDF (paper-ready vector). When None, only
        the PNG is written.
    title:
        Headline text. Defaults to the audit-aligned Chinese title.
    generated_on:
        Override for the "生成日期" footer. Defaults to today; tests
        pass a fixed date to keep the rendered figure deterministic.

    Returns
    -------
    Path
        The PNG path written. PDF (if requested) is written alongside.

    Raises
    ------
    FileNotFoundError
        When the verdicts CSV does not exist.
    ValueError
        When the CSV is empty, missing required columns, or missing
        any of the H1-H7 hypothesis rows.
    """
    csv_path = Path(verdicts_csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"verdicts CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        raise ValueError(f"verdicts CSV is empty: {csv_path}")

    panel = _prepare_panel(df)
    if panel.empty:
        raise ValueError(f"verdicts CSV has no rows after parsing: {csv_path}")

    png_path = Path(output_png_path)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path: Path | None = Path(output_pdf_path) if output_pdf_path else None
    if pdf_path is not None:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)

    n_rows = len(panel)
    # Match the HS300 forest height heuristic so the 7-row CMA figure
    # has comparable visual density to the 4-row HS300 panel.
    fig_height = max(4.8, 0.95 * n_rows + 1.8)
    fig, ax = plt.subplots(figsize=(11.5, fig_height))

    y_positions = list(range(n_rows))
    strengths = panel["_strength"].astype(float).to_numpy()
    n_obs = panel["n_obs"].astype(float).fillna(0.0).to_numpy()
    colors = panel["_color"].tolist()

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        for y_pos, strength, n_val, color in zip(
            y_positions, strengths, n_obs, colors, strict=True
        ):
            half = _ci_half_width(n_val)
            # Clamp the whisker to [0, 1] so very-small-n rows don't
            # spill past the visual frame.
            left = max(0.0, strength - half)
            right = min(1.0, strength + half)
            ax.errorbar(
                [strength],
                [y_pos],
                xerr=[[strength - left], [right - strength]],
                fmt="none",
                ecolor=color,
                elinewidth=1.6,
                capsize=5,
                alpha=0.85,
                zorder=2,
            )

        ax.scatter(
            strengths,
            y_positions,
            s=110,
            c=colors,
            edgecolors="#18212b",
            linewidths=0.9,
            zorder=3,
        )

        # Reference lines: 0.5 (neutral / borderline) and 1.0 (max
        # support). Mirror the HS300 axvline-at-zero idiom.
        ax.axvline(0.5, color="#9ba3ad", linestyle="--", linewidth=1.0, zorder=1)
        ax.axvline(1.0, color="#9ba3ad", linestyle=":", linewidth=0.8, zorder=1)

        ax.set_yticks(y_positions)
        ax.set_yticklabels(panel["_label"].tolist(), fontsize=11)
        ax.invert_yaxis()  # H1 at top, H7 at bottom

        ax.set_xlabel("证据强度 score (0 = 无支持，1 = 强支持)", fontsize=11)
        plot_title = title or "CMA 跨市场不对称 — 7 条假说证据强度对比"
        ax.set_title(plot_title, fontsize=15, pad=14, fontweight="bold")

        # x-axis grid + spines: match HS300 idiom.
        ax.grid(axis="x", alpha=0.22, linestyle=":")
        ax.tick_params(axis="x", labelsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Fix x-axis to [0, 1.25] so we have a stable annotation column.
        ax.set_xlim(0.0, 1.25)

        annotation_x = 1.03
        for y_pos, (_idx, row) in zip(y_positions, panel.iterrows(), strict=True):
            ax.text(
                annotation_x,
                y_pos,
                str(row["_annotation"]),
                fontsize=10,
                va="center",
                ha="left",
                color="#30424f",
                family="monospace",
            )

        # Evidence-tier legend: only show tiers actually present in the
        # panel (parallels HS300's significance-band dedup).
        present_tiers: list[str] = []
        for tier in panel["evidence_tier"].astype(str).str.strip().tolist():
            if tier not in present_tiers:
                present_tiers.append(tier)
        legend_handles = []
        for tier in ("core", "supplementary"):
            if tier in present_tiers:
                legend_handles.append(
                    plt.Line2D(
                        [0],
                        [0],
                        marker="o",
                        linestyle="",
                        markerfacecolor=_tier_color(tier),
                        markeredgecolor="#18212b",
                        markersize=10,
                        label=_tier_label(tier),
                    )
                )
        if legend_handles:
            ax.legend(
                handles=legend_handles,
                loc="lower right",
                frameon=True,
                fontsize=10,
                title="evidence tier",
                title_fontsize=10,
            )

        # Subtitle (top, slightly inset) + score derivation footnote +
        # provenance footer. Three text blocks so the figure stays
        # self-explanatory in slide / paper export.
        gen_date = (generated_on or date.today()).isoformat()
        fig.text(0.01, 0.965, _SUBTITLE, fontsize=10, color="#30424f")
        fig.text(
            0.01,
            0.045,
            "评分对照：(支持·高)=1.0  (支持·中)=0.7  (部分支持·高)=0.6  (部分支持·中)=0.5  "
            "(证据不足·中)=0.3  (证据不足·低)=0.0",
            fontsize=9,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.013,
            f"数据来源：{csv_path.name}    ·    生成日期：{gen_date}    "
            "·    评分仅用于可视化对比，不构成新的统计推断",
            fontsize=9,
            color="#5c6b77",
        )

        fig.tight_layout(rect=(0.0, 0.07, 1.0, 0.94))
        fig.savefig(png_path, dpi=220)
        if pdf_path is not None:
            fig.savefig(pdf_path)
        plt.close(fig)

    return png_path
