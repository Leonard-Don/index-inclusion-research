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


# ── 4. Gap decomposition chart ───────────────────────────────────────

GAP_SEGMENTS: tuple[str, ...] = (
    "announce_jump",
    "gap_drift",
    "effective_jump",
    "post_effective_reversal",
)
GAP_SEGMENT_LABELS = {
    "announce_jump": "公告跳",
    "gap_drift": "空窗漂移",
    "effective_jump": "生效跳",
    "post_effective_reversal": "生效后反转",
}


def build_gap_decomposition_chart_data(root: Path) -> dict:
    """Build ECharts data for the announce → gap → effective decomposition.

    Returns a grouped-bar payload keyed on market with 4 segments per market.
    """
    summary_path = root / "results" / "real_tables" / "cma_gap_summary.csv"
    if not summary_path.exists():
        return {"markets": [], "segments": [], "series": []}

    summary = pd.read_csv(summary_path)
    summary = summary.loc[summary["metric"].isin(GAP_SEGMENTS)].copy()
    if summary.empty:
        return {"markets": [], "segments": [], "series": []}

    pivot = summary.pivot_table(
        index="market", columns="metric", values="mean", aggfunc="first"
    ).reindex(columns=list(GAP_SEGMENTS))
    markets = list(pivot.index)
    segments = list(GAP_SEGMENTS)

    series = []
    for segment in segments:
        if segment not in pivot.columns:
            continue
        data = []
        for market in markets:
            value = pivot.loc[market, segment]
            data.append(round(float(value), 6) if pd.notna(value) else 0.0)
        series.append(
            {
                "name": GAP_SEGMENT_LABELS.get(segment, segment),
                "type": "bar",
                "data": data,
                "segment": segment,
            }
        )

    return {
        "markets": [MARKET_LABELS.get(m, m) for m in markets],
        "raw_markets": markets,
        "segments": [GAP_SEGMENT_LABELS.get(s, s) for s in segments],
        "series": series,
    }


# ── 5. Heterogeneity (size buckets) ──────────────────────────────────


def build_heterogeneity_size_chart_data(root: Path) -> dict:
    """Build ECharts data for the size-bucket asymmetry-index matrix.

    Returns one series per market with bucket-sorted asymmetry values.
    """
    path = root / "results" / "real_tables" / "cma_heterogeneity_size.csv"
    if not path.exists():
        return {"buckets": [], "series": []}

    df = pd.read_csv(path)
    if "asymmetry_index" not in df.columns:
        return {"buckets": [], "series": []}

    buckets = sorted(df["bucket"].dropna().unique().tolist())
    series = []
    for market, group in df.groupby("market", dropna=False):
        ordered = group.set_index("bucket").reindex(buckets)
        data = [
            round(float(v), 4) if pd.notna(v) else None
            for v in ordered["asymmetry_index"]
        ]
        n_events_col = ordered.get("n_events")
        n_events = (
            [int(v) if pd.notna(v) else 0 for v in n_events_col]
            if n_events_col is not None
            else [0] * len(buckets)
        )
        series.append(
            {
                "name": MARKET_LABELS.get(market, market),
                "type": "bar",
                "data": data,
                "n_events": n_events,
                "color": MARKET_COLORS.get(market, "#30424f"),
                "market": market,
            }
        )

    return {"buckets": buckets, "series": series}


# ── 6. Time-series rolling CAR ───────────────────────────────────────


def build_time_series_rolling_chart_data(root: Path) -> dict:
    """Build ECharts data for rolling CAR by market × phase over years."""
    path = root / "results" / "real_tables" / "cma_time_series_rolling.csv"
    if not path.exists():
        return {"series": [], "years": []}

    df = pd.read_csv(path)
    required = {"market", "event_phase", "car_mean", "window_end_year"}
    if not required.issubset(df.columns):
        return {"series": [], "years": []}

    series = []
    for (market, phase), group in df.groupby(["market", "event_phase"], dropna=False):
        group = group.sort_values("window_end_year")
        label = f"{MARKET_LABELS.get(market, market)} {PHASE_LABELS.get(phase, phase)}"
        data = []
        for _, r in group.iterrows():
            year = int(r["window_end_year"])
            car = round(float(r["car_mean"]), 6)
            data.append([year, car])
        series.append(
            {
                "name": label,
                "type": "line",
                "data": data,
                "color": MARKET_COLORS.get(market, "#30424f"),
                "lineStyle": {
                    "type": "solid" if phase == "announce" else "dashed",
                    "width": 2.2,
                },
                "symbol": "circle",
                "symbolSize": 7,
                "market": market,
                "phase": phase,
            }
        )

    years = sorted({int(y) for s in series for y, _ in s["data"]})
    return {"series": series, "years": years}


# ── 7. Main regression forest plot ───────────────────────────────────


_QUADRANT_ORDER: dict[tuple[str, str], int] = {
    ("CN", "announce"): 0,
    ("CN", "effective"): 1,
    ("US", "announce"): 2,
    ("US", "effective"): 3,
}


def _treatment_forest_rows(
    df: pd.DataFrame,
    *,
    specification: str,
) -> list[dict]:
    sub = df.loc[
        (df["specification"] == specification)
        & (df["parameter"] == "treatment_group")
    ]
    rows: list[dict] = []
    for _, r in sub.iterrows():
        coef = float(r["coefficient"])
        se = float(r["std_error"])
        rows.append(
            {
                "label": (
                    f"{MARKET_LABELS.get(r['market'], r['market'])} "
                    f"{PHASE_LABELS.get(r['event_phase'], r['event_phase'])}"
                ),
                "market": r["market"],
                "phase": r["event_phase"],
                "specification": r["specification"],
                "coef": round(coef, 6),
                "ci_lo": round(coef - 1.96 * se, 6),
                "ci_hi": round(coef + 1.96 * se, 6),
                "se": round(se, 6),
                "p_value": round(float(r["p_value"]), 6),
                "stars": _significance_stars(float(r["p_value"])),
                "color": MARKET_COLORS.get(r["market"], "#30424f"),
            }
        )
    rows.sort(key=lambda r: _QUADRANT_ORDER.get((r["market"], r["phase"]), 99))
    return rows


def build_main_regression_chart_data(root: Path) -> dict:
    """Forest-plot payload for the main CAR regression treatment coefficient.

    Filters regression_coefficients.csv to ``specification == "main_car"``
    and ``parameter == "treatment_group"`` and emits one row per
    (market, event_phase) quadrant with coefficient, ±1.96·SE CI bounds
    and HC3 p-value. Suitable for an ECharts custom forest plot.
    """
    path = root / "results" / "real_tables" / "regression_coefficients.csv"
    if not path.exists():
        return {"rows": []}

    df = pd.read_csv(path)
    rows = _treatment_forest_rows(df, specification="main_car")
    return {"rows": rows}


def build_mechanism_regression_chart_data(root: Path) -> dict:
    """Forest-plot payload for the turnover-mechanism regression treatment coef.

    Same shape as ``build_main_regression_chart_data`` but filtered to
    ``specification == "turnover_mechanism"`` so the dashboard can show
    the channel-concentration evidence right next to the main CAR
    forest plot.
    """
    path = root / "results" / "real_tables" / "regression_coefficients.csv"
    if not path.exists():
        return {"rows": []}

    df = pd.read_csv(path)
    rows = _treatment_forest_rows(df, specification="turnover_mechanism")
    return {"rows": rows}


# ── Registry ─────────────────────────────────────────────────────────

CHART_BUILDERS: dict[str, callable] = {
    "car_path": build_car_path_chart_data,
    "price_pressure": build_price_pressure_chart_data,
    "car_heatmap": build_car_heatmap_chart_data,
    "gap_decomposition": build_gap_decomposition_chart_data,
    "heterogeneity_size": build_heterogeneity_size_chart_data,
    "time_series_rolling": build_time_series_rolling_chart_data,
    "main_regression": build_main_regression_chart_data,
    "mechanism_regression": build_mechanism_regression_chart_data,
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
