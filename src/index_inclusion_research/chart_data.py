"""Build ECharts-compatible JSON data for interactive dashboard charts.

Each ``build_*`` function reads the relevant CSV result files and returns
a plain dict that can be serialised to JSON and consumed by the ECharts
frontend layer.  The dicts follow the ECharts *option* shape wherever
practical, so the JS rendering code can use them with minimal
transformation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ── colour palette (shared with Matplotlib backend) ──────────────────

MARKET_COLORS = {"CN": "#a63b28", "US": "#0f5c6e"}
MARKET_LABELS = {"CN": "中国 A 股", "US": "美国"}
PHASE_LABELS = {"announce": "公告日", "effective": "生效日"}


def _significance_stars(p_value: float) -> str:
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


# ── 1. CMA AR / CAR path chart ──────────────────────────────────────

def build_car_path_chart_data(root: Path) -> dict:
    """Build ECharts data for the CMA AR/CAR path comparison chart.

    Returns a dict with ``series`` (list of line-series dicts) and
    ``days`` (x-axis values).
    """
    ar_path = root / "results" / "real_tables" / "cma_ar_path.csv"
    car_path = root / "results" / "real_tables" / "cma_car_path.csv"

    if not ar_path.exists() or not car_path.exists():
        return {"series": [], "days": []}

    ar = pd.read_csv(ar_path)
    car = pd.read_csv(car_path)

    series: list[dict] = []

    for df, metric in [(ar, "AR"), (car, "CAR")]:
        # gap_period exports use ``relative_day`` but legacy fixtures may
        # supply ``day``; accept either to keep tests + production both
        # working.
        day_col = "relative_day" if "relative_day" in df.columns else "day"
        required = {"market", "event_phase", day_col}
        value_col = "ar_mean" if metric == "AR" else "car_mean"
        if not required.issubset(df.columns) or value_col not in df.columns:
            continue
        for (market, phase), group in df.groupby(["market", "event_phase"], dropna=False):
            group = group.sort_values(day_col)
            label = f"{MARKET_LABELS.get(market, market)} {PHASE_LABELS.get(phase, phase)}"
            series.append({
                "name": f"{label} ({metric})",
                "type": "line",
                "data": [
                    [int(r[day_col]), round(float(r[value_col]), 6)]
                    for _, r in group.iterrows()
                ],
                "color": MARKET_COLORS.get(market, "#30424f"),
                "lineStyle": {
                    "type": "solid" if phase == "announce" else "dashed",
                    "width": 2,
                },
                "symbol": "none",
                "metric": metric,
                "market": market,
                "phase": phase,
            })

    days = sorted({int(d) for s in series for d, _ in s["data"]})
    return {"series": series, "days": days}


# ── 2. Price pressure time-series chart ──────────────────────────────

def build_price_pressure_chart_data(root: Path) -> dict:
    """Build ECharts data for the short-window CAR time-series chart.

    Returns a dict with ``series`` and ``years``.
    """
    summary_path = root / "results" / "real_tables" / "time_series_event_study_summary.csv"
    if not summary_path.exists():
        return {"series": [], "years": []}

    summary = pd.read_csv(summary_path)
    summary = summary.loc[summary["inclusion"] == 1].copy()
    if summary.empty:
        return {"series": [], "years": []}

    series: list[dict] = []
    for (market, phase), group in summary.groupby(["market", "event_phase"], dropna=False):
        group = group.sort_values("announce_year")
        label = f"{MARKET_LABELS.get(market, market)} {PHASE_LABELS.get(phase, phase)}"

        data = []
        ci_low: list[list] = []
        ci_high: list[list] = []
        for _, r in group.iterrows():
            year = int(r["announce_year"])
            car = round(float(r["mean_car_m1_p1"]), 6)
            data.append([year, car])
            if {"ci_low_95_car_m1_p1", "ci_high_95_car_m1_p1"}.issubset(group.columns):
                ci_low.append([year, round(float(r["ci_low_95_car_m1_p1"]), 6)])
                ci_high.append([year, round(float(r["ci_high_95_car_m1_p1"]), 6)])

        series.append({
            "name": label,
            "type": "line",
            "data": data,
            "ci_low": ci_low,
            "ci_high": ci_high,
            "color": MARKET_COLORS.get(market, "#30424f"),
            "lineStyle": {
                "type": "solid" if phase == "announce" else "dashed",
                "width": 2.2,
            },
            "symbol": "circle",
            "symbolSize": 7,
            "market": market,
            "phase": phase,
        })

    years = sorted({int(y) for s in series for y, _ in s["data"]})
    return {"series": series, "years": years}


# ── 3. CAR heatmap chart ─────────────────────────────────────────────

def build_car_heatmap_chart_data(root: Path) -> dict:
    """Build ECharts data for the short-window CAR heatmap.

    Returns a dict with ``data`` (list of [col_idx, row_idx, value]),
    ``row_labels``, ``col_labels``, ``annotations`` and value ranges.
    """
    summary_path = root / "results" / "real_tables" / "event_study_summary.csv"
    if not summary_path.exists():
        return {"data": [], "row_labels": [], "col_labels": [], "annotations": []}

    event = pd.read_csv(summary_path)
    heat = event.loc[event["inclusion"] == 1].copy()
    if heat.empty:
        return {"data": [], "row_labels": [], "col_labels": [], "annotations": []}

    row_order = ["中国 A 股 · 公告日", "中国 A 股 · 生效日", "美国 · 公告日", "美国 · 生效日"]
    col_order = ["[-1,+1]", "[-3,+3]", "[-5,+5]"]

    heat["row_label"] = heat["market"].map(MARKET_LABELS) + " · " + heat["event_phase"].map(PHASE_LABELS)
    heat_matrix = (
        heat.pivot_table(index="row_label", columns="window", values="mean_car", aggfunc="first")
        .reindex(index=row_order, columns=col_order)
    )
    p_matrix = (
        heat.pivot_table(index="row_label", columns="window", values="p_value", aggfunc="first")
        .reindex(index=row_order, columns=col_order)
    )

    data: list[list] = []
    annotations: list[dict] = []
    for i, row_label in enumerate(row_order):
        for j, window in enumerate(col_order):
            car = float(heat_matrix.loc[row_label, window])
            p = float(p_matrix.loc[row_label, window])
            data.append([j, i, round(car, 6)])
            annotations.append({
                "col": j,
                "row": i,
                "car": round(car, 6),
                "car_pct": f"{car:.2%}",
                "p_value": round(p, 4),
                "stars": _significance_stars(p),
            })

    vmax = max(abs(v) for _, _, v in data) if data else 0.01

    return {
        "data": data,
        "row_labels": row_order,
        "col_labels": col_order,
        "annotations": annotations,
        "vmax": round(vmax, 6),
    }


# ── Registry ─────────────────────────────────────────────────────────

CHART_BUILDERS: dict[str, callable] = {
    "car_path": build_car_path_chart_data,
    "price_pressure": build_price_pressure_chart_data,
    "car_heatmap": build_car_heatmap_chart_data,
}


def build_chart_data(chart_id: str, root: Path) -> dict | None:
    """Dispatch to the appropriate chart-data builder, or return *None*
    for unknown chart IDs."""
    builder = CHART_BUILDERS.get(chart_id)
    if builder is None:
        return None
    try:
        return builder(root)
    except Exception:
        logger.exception("Failed to build chart data for %s", chart_id)
        return {"error": f"Failed to build chart data for {chart_id}"}
