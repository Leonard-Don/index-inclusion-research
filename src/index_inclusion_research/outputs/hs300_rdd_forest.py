"""Publication-quality forest plot of the HS300 RDD robustness panel.

The HS300 RDD `car_m1_p1` main result (τ=3.92%, p=0.048, n=120) is
reported alongside donut / placebo±0.05 / polynomial specifications in
``results/literature/hs300_rdd/rdd_robustness.csv``. The audit
(``docs/limitations.md``) flags that the paper must report the full
panel rather than only the significant main spec, so this forest plot is
the user-facing artifact that makes the panel transparent.

The figure is intentionally self-contained: a single call to
:func:`build_hs300_rdd_forest_plot` reads the robustness CSV and writes
PNG (and optionally PDF) outputs with significance-coded markers, 95% CI
whiskers, per-row n/p annotations, and provenance footer (source CSV,
generation date, sample period).
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

# Sort order matches the audit narrative: main first, then donut /
# placebo / polynomial. Specs with unknown kinds drop to the bottom.
_SPEC_KIND_ORDER: tuple[str, ...] = ("main", "donut", "placebo", "polynomial")

# Bilingual labels keep the figure readable for both Chinese-language
# review and English-language paper export.
_SPEC_KIND_LABELS: dict[str, str] = {
    "main": "main · 局部线性",
    "donut": "donut · 剔除断点邻域",
    "placebo": "placebo · 安慰剂断点",
    "polynomial": "polynomial · 多项式设定",
}

# Significance bands mirror the convention used elsewhere in the project
# (econ-style 5% / 10% / NS) so reviewers can compare visuals across
# tracks without recalibrating.
_SIG_LEVELS: tuple[tuple[str, float, str, str], ...] = (
    ("p<0.05", 0.05, "#0f5c6e", "显著 (p<0.05)"),
    ("p<0.10", 0.10, "#d97706", "边缘显著 (p<0.10)"),
    ("p≥0.10", 1.01, "#5c6b77", "不显著 (p≥0.10)"),
)

# Audit-confirmed sample window. Surface it in the subtitle so reviewers
# don't have to cross-reference the limitations doc when reading the
# figure standalone (e.g. as a slide).
_SAMPLE_PERIOD_LABEL = "样本区间：2020-11 — 2025-11 (HS300 L3 候选 11 批次)"


def _classify_significance(p_value: float) -> tuple[str, str]:
    """Return (color, legend_label) for a given p-value."""
    if pd.isna(p_value):
        return "#5c6b77", "不显著 (p≥0.10)"
    for _key, threshold, color, legend_label in _SIG_LEVELS:
        if float(p_value) < threshold:
            return color, legend_label
    return "#5c6b77", "不显著 (p≥0.10)"


def _spec_label(row: pd.Series) -> str:
    """Build a row label preferring the published spec string but
    falling back to the kind so unknown specs still render readably."""
    raw_spec = str(row.get("spec", "")).strip()
    if raw_spec:
        return raw_spec
    kind = str(row.get("spec_kind", "")).strip()
    return _SPEC_KIND_LABELS.get(kind, kind or "未知规格")


def _kind_rank(kind: str) -> int:
    try:
        return _SPEC_KIND_ORDER.index(kind)
    except ValueError:
        return len(_SPEC_KIND_ORDER)


def _prepare_panel(df: pd.DataFrame) -> pd.DataFrame:
    """Validate columns and sort rows for the forest plot.

    Sort order:
      - main first (visually anchors the eye on the headline result)
      - then donut / placebo / polynomial, in their canonical order
      - within a kind, ascending τ keeps placebo±0.05 grouped together.
    """
    required = {"spec", "spec_kind", "tau", "std_error", "p_value", "n_obs"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"robustness CSV missing required columns: {sorted(missing)}; "
            "regenerate via index-inclusion-hs300-rdd"
        )

    panel = df.copy()
    panel["_kind_rank"] = panel["spec_kind"].astype(str).map(_kind_rank)
    panel["_spec_label"] = panel.apply(_spec_label, axis=1)
    panel = panel.sort_values(["_kind_rank", "tau"], ascending=[True, True], kind="mergesort")
    panel = panel.reset_index(drop=True)
    return panel


def _format_annotation(row: pd.Series) -> str:
    n = row.get("n_obs", float("nan"))
    p = row.get("p_value", float("nan"))
    try:
        n_str = f"n={int(n)}"
    except (TypeError, ValueError):
        n_str = "n=NA"
    try:
        p_str = f"p={float(p):.3f}"
    except (TypeError, ValueError):
        p_str = "p=NA"
    return f"{n_str}, {p_str}"


def build_hs300_rdd_forest_plot(
    robustness_csv_path: str | Path,
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    title: str = "HS300 RDD 稳健性面板 (car_m1_p1)",
    generated_on: date | None = None,
) -> Path:
    """Render the HS300 RDD robustness forest plot.

    Parameters
    ----------
    robustness_csv_path:
        Path to ``results/literature/hs300_rdd/rdd_robustness.csv`` (or a
        synthetic fixture in tests).
    output_png_path:
        Destination PNG. Parent directory is created if missing.
    output_pdf_path:
        Optional companion PDF (paper-ready vector). Parent directory is
        created if missing. When None, only the PNG is written.
    title:
        Headline text. Defaults to the audit-aligned Chinese title.
    generated_on:
        Override for the "生成日期" footer. Defaults to today; tests pass
        a fixed date to keep the rendered figure deterministic.

    Returns
    -------
    Path
        The PNG path that was written. PDF (if requested) is written
        alongside and is not returned separately — both paths are
        derived from the inputs.

    Raises
    ------
    FileNotFoundError
        When the robustness CSV does not exist.
    ValueError
        When the CSV is empty or missing required columns.
    """
    robust_path = Path(robustness_csv_path)
    if not robust_path.exists():
        raise FileNotFoundError(f"robustness CSV not found: {robust_path}")

    df = pd.read_csv(robust_path)
    if df.empty:
        raise ValueError(f"robustness CSV is empty: {robust_path}")

    panel = _prepare_panel(df)
    if panel.empty:
        raise ValueError(f"robustness CSV has no rows after parsing: {robust_path}")

    png_path = Path(output_png_path)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path: Path | None = Path(output_pdf_path) if output_pdf_path else None
    if pdf_path is not None:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)

    n_rows = len(panel)
    # Height scales with row count so 4-row and 8-row panels stay
    # visually consistent (no cramped rows, no excessive whitespace).
    fig_height = max(4.4, 1.0 * n_rows + 1.6)
    fig, ax = plt.subplots(figsize=(11.0, fig_height))

    # Forest rows are plotted bottom-to-top (matplotlib's y-axis grows
    # upward) but conceptually the "main" spec should sit at the top of
    # the page. Invert the y-axis after plotting to achieve that without
    # rebuilding indices.
    y_positions = list(range(n_rows))
    taus = panel["tau"].astype(float).to_numpy()
    ses = panel["std_error"].astype(float).fillna(0.0).to_numpy()
    p_values = panel["p_value"].astype(float).to_numpy()

    # Per-row significance color drives both the marker fill and the CI
    # whisker — single coding channel keeps the legend simple.
    colors = [_classify_significance(p)[0] for p in p_values]
    legend_labels = [_classify_significance(p)[1] for p in p_values]

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        for y_pos, tau, se, color in zip(y_positions, taus, ses, colors, strict=True):
            ax.errorbar(
                [tau],
                [y_pos],
                xerr=[[1.96 * se], [1.96 * se]],
                fmt="none",
                ecolor=color,
                elinewidth=1.6,
                capsize=5,
                alpha=0.85,
                zorder=2,
            )
        ax.scatter(
            taus,
            y_positions,
            s=90,
            c=colors,
            edgecolors="#18212b",
            linewidths=0.9,
            zorder=3,
        )

        # τ=0 reference: anchors the eye on "no effect" and makes the
        # placebo-near-zero check immediate.
        ax.axvline(0.0, color="#9ba3ad", linestyle="--", linewidth=1.1, zorder=1)

        ax.set_yticks(y_positions)
        ax.set_yticklabels(panel["_spec_label"].tolist(), fontsize=11)
        ax.invert_yaxis()  # main spec at top

        ax.set_xlabel("τ (RDD 处理效应，CAR[-1,+1])", fontsize=11)
        ax.set_title(title, fontsize=15, pad=14, fontweight="bold")

        ax.grid(axis="x", alpha=0.22, linestyle=":")
        ax.tick_params(axis="x", labelsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Per-row annotation: n + p value at the right margin. Use an
        # axes-relative x-coordinate so annotations don't overlap the CI
        # whiskers no matter how the x-limits land.
        x_min, x_max = ax.get_xlim()
        x_span = x_max - x_min
        x_max_padded = x_max + 0.18 * x_span
        ax.set_xlim(x_min, x_max_padded)
        annotation_x = x_max + 0.02 * x_span
        for y_pos, (_idx, row) in zip(y_positions, panel.iterrows(), strict=True):
            ax.text(
                annotation_x,
                y_pos,
                _format_annotation(row),
                fontsize=10,
                va="center",
                ha="left",
                color="#30424f",
                family="monospace",
            )

        # Significance legend: build deduplicated entries in the
        # canonical 5% / 10% / NS order regardless of which rows are
        # present in the panel.
        seen: set[str] = set()
        legend_handles = []
        for _key, _threshold, color, label in _SIG_LEVELS:
            if label not in seen and label in legend_labels:
                legend_handles.append(
                    plt.Line2D(
                        [0],
                        [0],
                        marker="o",
                        linestyle="",
                        markerfacecolor=color,
                        markeredgecolor="#18212b",
                        markersize=10,
                        label=label,
                    )
                )
                seen.add(label)
        if legend_handles:
            ax.legend(
                handles=legend_handles,
                loc="lower right",
                frameon=True,
                fontsize=10,
                title="显著性水平",
                title_fontsize=10,
            )

        # Subtitle + provenance footer. Keep it on the figure (not the
        # axes) so it survives axes-level tight_layout adjustments.
        gen_date = (generated_on or date.today()).isoformat()
        source_label = robust_path.name
        fig.text(
            0.01,
            0.965,
            _SAMPLE_PERIOD_LABEL,
            fontsize=10,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.015,
            f"数据来源：{source_label}    ·    生成日期：{gen_date}",
            fontsize=9,
            color="#5c6b77",
        )

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Glyph .* missing from font\(s\) .+",
                category=UserWarning,
            )
            fig.tight_layout(rect=(0.0, 0.04, 1.0, 0.94))
            fig.savefig(png_path, dpi=220)
            if pdf_path is not None:
                fig.savefig(pdf_path)
        plt.close(fig)

    return png_path
