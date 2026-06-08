"""Parallel-trends AAR figure: treated vs matched controls.

Renders the per-relative-day average abnormal return (AAR) path for treated
inclusion events against their covariate-balanced matched controls. The visual
story: the two paths overlap (quasi-parallel) across the pre-event window and
diverge only inside the event window — the descriptive parallel-trends sanity
check that supports the matched event-study design.

Self-contained like the other ``outputs/*`` builders: a single call to
:func:`build_parallel_trends_plot` consumes the AAR table produced by
``analysis.robustness_event_study.compute_parallel_trends_aar`` and writes one
PNG (and optional PDF) per ``(market, event_phase)`` cell into the figures
directory (``results/real_figures`` in the real profile). It never mutates an
existing figure or reuses an existing file name.
"""

from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from index_inclusion_research.plot_style import configure_matplotlib_cjk

configure_matplotlib_cjk(plt)

_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "market",
        "event_phase",
        "relative_day",
        "treated_aar",
        "control_aar",
    }
)

_MARKET_LABELS: dict[str, str] = {"CN": "A股 (CSI300)", "US": "美股 (S&P500)"}
_PHASE_LABELS: dict[str, str] = {"announce": "公告", "effective": "生效"}

_TREATED_COLOR = "#0f5c6e"
_CONTROL_COLOR = "#d97706"
_PRE_WINDOW_COLOR = "#9ba3ad"


def _market_label(market: object) -> str:
    return _MARKET_LABELS.get(str(market), str(market))


def _phase_label(phase: object) -> str:
    return _PHASE_LABELS.get(str(phase), str(phase))


def build_parallel_trends_plot(
    aar_table: pd.DataFrame,
    output_dir: str | Path,
    *,
    pre_event_window: tuple[int, int] = (-20, -2),
    write_pdf: bool = True,
    generated_on: date | None = None,
) -> list[Path]:
    """Render parallel-trends AAR figures (one per market × phase cell).

    Parameters
    ----------
    aar_table:
        Output of
        :func:`analysis.robustness_event_study.compute_parallel_trends_aar`.
        Must carry ``market``, ``event_phase``, ``relative_day``,
        ``treated_aar`` and ``control_aar``; optional ``*_se`` columns add a
        shaded ±1 SE band.
    output_dir:
        Destination directory (created if missing). Each cell writes
        ``parallel_trends_aar_<market>_<phase>.png``.
    pre_event_window:
        Inclusive ``(start, end)`` of the pre-event window highlighted as the
        quasi-parallel region. Defaults to ``[-20, -2]``.
    write_pdf:
        Also write a vector PDF companion alongside each PNG.
    generated_on:
        Override the provenance footer date (tests pass a fixed date for
        deterministic output).

    Returns
    -------
    list[Path]
        The PNG paths written (one per cell). Empty when the table is empty or
        lacks the required columns.
    """
    out_dir = Path(output_dir)
    if aar_table.empty or not _REQUIRED_COLUMNS.issubset(aar_table.columns):
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    pre_lo, pre_hi = pre_event_window
    gen_date = (generated_on or date.today()).isoformat()
    written: list[Path] = []

    for (market, phase), cell in aar_table.groupby(["market", "event_phase"], dropna=False):
        cell = cell.sort_values("relative_day")
        rel_days = cell["relative_day"].astype(int).to_numpy()
        treated = pd.to_numeric(cell["treated_aar"], errors="coerce").to_numpy()
        control = pd.to_numeric(cell["control_aar"], errors="coerce").to_numpy()
        if not np.isfinite(treated).any() and not np.isfinite(control).any():
            continue

        fig, ax = plt.subplots(figsize=(10.5, 6.0))

        # Pre-event window shading anchors the "should-overlap" region.
        ax.axvspan(
            pre_lo,
            pre_hi,
            color=_PRE_WINDOW_COLOR,
            alpha=0.12,
            zorder=0,
            label=f"事前平行窗 [{pre_lo},{pre_hi}]",
        )
        ax.axvline(0.0, color="#5c6b77", linestyle="--", linewidth=1.1, zorder=1)
        ax.axhline(0.0, color="#c4ccd4", linestyle=":", linewidth=1.0, zorder=1)

        _plot_series(
            ax,
            rel_days,
            treated,
            cell.get("treated_aar_se"),
            color=_TREATED_COLOR,
            label="处理组 (纳入事件)",
        )
        _plot_series(
            ax,
            rel_days,
            control,
            cell.get("control_aar_se"),
            color=_CONTROL_COLOR,
            label="匹配控制组",
        )

        market_label = _market_label(market)
        phase_label = _phase_label(phase)
        ax.set_title(
            f"{market_label} · {phase_label}事件：逐日平均异常收益 (AAR) 平行趋势",
            fontsize=14,
            pad=12,
            fontweight="bold",
        )
        ax.set_xlabel("相对交易日", fontsize=11)
        ax.set_ylabel("平均异常收益 (AAR)", fontsize=11)
        ax.grid(alpha=0.22, linestyle=":")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.legend(frameon=False, fontsize=10, loc="best")

        fig.text(
            0.01,
            0.015,
            f"数据来源：robustness_parallel_trends_aar    ·    生成日期：{gen_date}",
            fontsize=9,
            color="#5c6b77",
        )

        slug = f"{str(market).lower()}_{str(phase).lower()}"
        png_path = out_dir / f"parallel_trends_aar_{slug}.png"
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Glyph .* missing from font\(s\) .+",
                category=UserWarning,
            )
            fig.tight_layout(rect=(0.0, 0.03, 1.0, 1.0))
            fig.savefig(png_path, dpi=200)
            if write_pdf:
                fig.savefig(out_dir / f"parallel_trends_aar_{slug}.pdf")
        plt.close(fig)
        written.append(png_path)

    return written


def _plot_series(
    ax: matplotlib.axes.Axes,
    rel_days: np.ndarray,
    values: np.ndarray,
    se: pd.Series | None,
    *,
    color: str,
    label: str,
) -> None:
    finite = np.isfinite(values)
    if not finite.any():
        return
    ax.plot(
        rel_days[finite],
        values[finite],
        marker="o",
        markersize=3.5,
        linewidth=1.8,
        color=color,
        alpha=0.95,
        label=label,
        zorder=3,
    )
    if se is not None:
        se_arr = pd.to_numeric(se, errors="coerce").to_numpy()
        band = np.isfinite(values) & np.isfinite(se_arr)
        if band.any():
            ax.fill_between(
                rel_days[band],
                (values - se_arr)[band],
                (values + se_arr)[band],
                color=color,
                alpha=0.13,
                linewidth=0,
                zorder=2,
            )
