"""Build ECharts-compatible JSON data for interactive dashboard charts.

Each ``build_*`` function reads the relevant CSV result files and returns
a plain dict that can be serialised to JSON and consumed by the ECharts
frontend layer.  The dicts follow the ECharts *option* shape wherever
practical, so the JS rendering code can use them with minimal
transformation.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

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


def _float_or_none(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return None


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


def build_rdd_robustness_chart_data(root: Path) -> dict:
    """Forest-plot payload for the RDD robustness panel.

    Reads ``results/literature/hs300_rdd/rdd_robustness.csv`` produced by
    ``run_rdd_robustness`` and emits one row per spec (main, donut,
    placebo±, polynomial) with τ + ±1.96·SE CI bounds + p-value, ordered
    main-first then by spec_kind. Suitable for an ECharts forest plot
    sharing the same option builder as main_regression.
    """
    path = root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
    if not path.exists():
        return {"rows": []}

    df = pd.read_csv(path)
    if df.empty or "spec" not in df.columns or "tau" not in df.columns:
        return {"rows": []}

    color_map = {
        "main": "#0f5c6e",
        "donut": "#5d4f8a",
        "placebo": "#5c6b77",
        "polynomial": "#a63b28",
    }
    spec_order = ["main", "donut", "placebo", "polynomial"]
    rows: list[dict] = []
    for _, r in df.iterrows():
        tau = float(r["tau"]) if pd.notna(r["tau"]) else float("nan")
        se = float(r["std_error"]) if pd.notna(r["std_error"]) else float("nan")
        p_value = float(r["p_value"]) if pd.notna(r["p_value"]) else float("nan")
        if pd.isna(tau) or pd.isna(se):
            continue
        spec_kind = str(r.get("spec_kind", ""))
        # Key names mirror _treatment_forest_rows so the JS option builder
        # for main_regression can be reused without changes.
        rows.append(
            {
                "label": str(r["spec"]),
                "spec_kind": spec_kind,
                "coef": round(tau, 6),
                "ci_lo": round(tau - 1.96 * se, 6),
                "ci_hi": round(tau + 1.96 * se, 6),
                "se": round(se, 6),
                "p_value": (
                    round(p_value, 6) if not pd.isna(p_value) else None
                ),
                "stars": _significance_stars(p_value) if not pd.isna(p_value) else "",
                "n_obs": (
                    int(r["n_obs"]) if pd.notna(r["n_obs"]) else 0
                ),
                "color": color_map.get(spec_kind, "#30424f"),
            }
        )
    rows.sort(key=lambda r: (spec_order.index(r["spec_kind"]) if r["spec_kind"] in spec_order else 99, r["label"]))
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


# ── 9. Event counts by year ──────────────────────────────────────────


def build_event_counts_chart_data(root: Path) -> dict:
    """Bar-chart payload of treated event counts per market × announce_year.

    Reads ``event_counts_by_year.csv`` and emits one series per market
    (filtered to ``inclusion == 1`` — treated events only) with a value
    for every year in the union range. Suitable for an ECharts grouped
    bar chart that pairs with the static sample-event-timeline figure.
    """
    path = root / "results" / "real_tables" / "event_counts_by_year.csv"
    if not path.exists():
        return {"series": [], "years": []}

    df = pd.read_csv(path)
    df = df.loc[df["inclusion"] == 1].copy()
    if df.empty:
        return {"series": [], "years": []}

    years = sorted({int(y) for y in df["announce_year"].dropna().unique()})
    series = []
    for market, group in df.groupby("market", dropna=False):
        by_year = group.set_index("announce_year")["n_events"].to_dict()
        data = [int(by_year.get(year, 0)) for year in years]
        series.append(
            {
                "name": MARKET_LABELS.get(market, market),
                "type": "bar",
                "data": data,
                "color": MARKET_COLORS.get(market, "#30424f"),
                "market": market,
            }
        )
    return {"series": series, "years": years}


# ── 10. CMA mechanism heatmap (t-values by quadrant × outcome) ───────


_MECHANISM_OUTCOME_LABELS = {
    "car_1_1": "CAR[-1,+1]",
    "turnover_change": "Turnover Δ",
    "volume_change": "Volume Δ",
    "volatility_change": "Volatility Δ",
    "price_limit_hit_share": "涨跌停命中率",
}


def build_cma_mechanism_heatmap_chart_data(root: Path) -> dict:
    """Heatmap of treatment-group t-values across CMA mechanism panel.

    Reads ``cma_mechanism_panel.csv`` (no_fe spec only) and pivots into
    a quadrant × outcome heatmap with ECharts-friendly cell triples.
    """
    path = root / "results" / "real_tables" / "cma_mechanism_panel.csv"
    if not path.exists():
        return {"data": [], "row_labels": [], "col_labels": [], "annotations": []}

    df = pd.read_csv(path)
    sub = df.loc[df["spec"] == "no_fe"].copy()
    if sub.empty:
        return {"data": [], "row_labels": [], "col_labels": [], "annotations": []}

    sub["row_label"] = (
        sub["market"].map(MARKET_LABELS).fillna(sub["market"])
        + " · "
        + sub["event_phase"].map(PHASE_LABELS).fillna(sub["event_phase"])
    )
    sub["col_label"] = sub["outcome"].map(_MECHANISM_OUTCOME_LABELS).fillna(sub["outcome"])

    row_order = [
        f"{MARKET_LABELS[m]} · {PHASE_LABELS[p]}"
        for m in ("CN", "US")
        for p in ("announce", "effective")
    ]
    col_order = [
        _MECHANISM_OUTCOME_LABELS[o]
        for o in (
            "car_1_1",
            "turnover_change",
            "volume_change",
            "volatility_change",
            "price_limit_hit_share",
        )
    ]

    pivot_t = sub.pivot_table(index="row_label", columns="col_label", values="t", aggfunc="first")
    pivot_p = sub.pivot_table(index="row_label", columns="col_label", values="p_value", aggfunc="first")

    data: list[list] = []
    annotations: list[dict] = []
    for i, row in enumerate(row_order):
        for j, col in enumerate(col_order):
            t_val = pivot_t.loc[row, col] if row in pivot_t.index and col in pivot_t.columns else None
            p_val = pivot_p.loc[row, col] if row in pivot_p.index and col in pivot_p.columns else None
            if t_val is None or pd.isna(t_val):
                continue
            p_float = _float_or_none(p_val)
            data.append([j, i, round(float(t_val), 3)])
            annotations.append(
                {
                    "col": j,
                    "row": i,
                    "t": round(float(t_val), 3),
                    "p_value": round(p_float, 4) if p_float is not None else None,
                    "stars": _significance_stars(p_float) if p_float is not None else "",
                }
            )

    vmax = max((abs(v) for _, _, v in data), default=2.0)
    return {
        "data": data,
        "row_labels": row_order,
        "col_labels": col_order,
        "annotations": annotations,
        "vmax": round(vmax, 3),
    }


# ── 11. CMA gap-length distribution ──────────────────────────────────


def build_cma_gap_length_distribution_chart_data(root: Path) -> dict:
    """Bar-chart payload: count of events per ``gap_length_days`` per market.

    Reads ``cma_gap_event_level.csv`` and groups by integer gap_length_days
    so the chart shows the announce → effective window-length distribution
    (CN is fixed at 14, US varies 0..26).
    """
    path = root / "results" / "real_tables" / "cma_gap_event_level.csv"
    if not path.exists():
        return {"series": [], "lengths": []}

    df = pd.read_csv(path)
    df = df.dropna(subset=["gap_length_days"])
    if df.empty:
        return {"series": [], "lengths": []}

    df["gap_length_days"] = df["gap_length_days"].astype(int)
    lengths = sorted(df["gap_length_days"].unique().tolist())
    series = []
    for market, group in df.groupby("market", dropna=False):
        counts = group["gap_length_days"].value_counts().to_dict()
        data = [int(counts.get(length, 0)) for length in lengths]
        series.append(
            {
                "name": MARKET_LABELS.get(market, market),
                "type": "bar",
                "data": data,
                "color": MARKET_COLORS.get(market, "#30424f"),
                "market": market,
            }
        )
    return {"series": series, "lengths": lengths}


# ── 12. RDD scatter ──────────────────────────────────────────────────


RDD_BANDWIDTH_SWEEP: tuple[float, ...] = (0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.15, 0.20)
RDD_DEFAULT_BANDWIDTH: float = 0.06


def _pick_first_column(df: pd.DataFrame, options: list[str]) -> str | None:
    for col in options:
        if col in df.columns:
            return col
    return None


def _rdd_bandwidth_fits(df: pd.DataFrame, *, cutoff_value: float) -> list[dict]:
    """For each candidate bandwidth, fit a local-linear RDD and emit the
    line endpoints (left of cutoff, right of cutoff) in (running_variable,
    car_m1_p1) space the client can plot directly.

    HS300 running_variable is built from the official adjustment list order
    via ``cutoff + (n - order + 1)/100``, so distance_to_cutoff carries
    floating-point artifacts (e.g. ``0.0600000000000022``). To keep the
    sweep's "bw=0.06" cell consistent with rdd_summary.csv (which uses the
    auto-chosen bandwidth that lands on the same artifact), we widen each
    requested bandwidth by ``bw * 1e-9`` internally; the display label
    keeps the rounded value.
    """
    from index_inclusion_research.analysis.rdd import fit_local_linear_rdd

    fits: list[dict] = []
    for bw in RDD_BANDWIDTH_SWEEP:
        result = fit_local_linear_rdd(
            df,
            outcome_col="car_m1_p1",
            running_col="distance_to_cutoff",
            treatment_col="inclusion",
            bandwidth=bw * (1.0 + 1e-9),
        )
        n_obs = int(result.get("n_obs") or 0)
        intercept = result.get("intercept")
        running_slope = result.get("running_slope")
        interaction_slope = result.get("interaction_slope")
        tau = result.get("tau")
        if (
            n_obs < 10
            or intercept is None
            or running_slope is None
            or interaction_slope is None
            or tau is None
            or pd.isna(intercept)
            or pd.isna(running_slope)
            or pd.isna(interaction_slope)
            or pd.isna(tau)
        ):
            continue
        # Effective bandwidth used (may have been widened when local sample
        # empty). For display we keep the rounded request value; the line
        # endpoints anchor at that label, not the float-artifact internal value.
        effective_bw = float(bw)
        # Left segment: treatment=0, running in [-effective_bw, 0]
        y_left_outer = float(intercept) + float(running_slope) * (-effective_bw)
        y_left_inner = float(intercept)
        # Right segment: treatment=1, running in [0, +effective_bw]
        y_right_inner = float(intercept) + float(tau)
        y_right_outer = (
            float(intercept) + float(tau)
            + (float(running_slope) + float(interaction_slope)) * effective_bw
        )
        fits.append(
            {
                "bandwidth": round(float(bw), 4),
                "effective_bandwidth": round(effective_bw, 4),
                "tau": round(float(tau), 6),
                "p_value": round(float(result["p_value"]), 6) if not pd.isna(result["p_value"]) else None,
                "n_obs": n_obs,
                "n_left": int(result.get("n_left") or 0),
                "n_right": int(result.get("n_right") or 0),
                "line_left": [
                    [round(cutoff_value - effective_bw, 4), round(y_left_outer, 6)],
                    [round(cutoff_value, 4), round(y_left_inner, 6)],
                ],
                "line_right": [
                    [round(cutoff_value, 4), round(y_right_inner, 6)],
                    [round(cutoff_value + effective_bw, 4), round(y_right_outer, 6)],
                ],
            }
        )
    return fits


def build_rdd_scatter_chart_data(root: Path) -> dict:
    """RDD bin scatter for HS300 inclusion at the cutoff with bandwidth sweep.

    Reads ``results/literature/hs300_rdd/event_level_with_running.csv`` and
    emits two scatter series — control (inclusion=0) and treated
    (inclusion=1) — in (running_variable, car_m1_p1) space, plus per-point
    metadata (batch_id / ticker / security_name) for hover tooltips, plus a
    bandwidth sweep of local-linear RDD fits the client renders as
    multiple selectable fit lines.
    """
    path = root / "results" / "literature" / "hs300_rdd" / "event_level_with_running.csv"
    empty_payload: dict[str, Any] = {
        "series": [],
        "cutoff": None,
        "outcome": "car_m1_p1",
        "fits": [],
        "default_bandwidth": RDD_DEFAULT_BANDWIDTH,
    }
    if not path.exists():
        return empty_payload

    df = pd.read_csv(path)
    required = {"running_variable", "car_m1_p1", "inclusion"}
    if not required.issubset(df.columns):
        return empty_payload

    df = df.dropna(subset=["running_variable", "car_m1_p1"])
    if df.empty:
        return empty_payload

    cutoff_value = (
        float(df["cutoff"].iloc[0]) if "cutoff" in df.columns and pd.notna(df["cutoff"].iloc[0]) else 300.0
    )
    if "distance_to_cutoff" not in df.columns:
        df = df.assign(distance_to_cutoff=df["running_variable"].astype(float) - cutoff_value)

    batch_col = _pick_first_column(df, ["batch_id", "batch_id_x", "batch_id_y"])
    ticker_col = _pick_first_column(df, ["candidate_ticker", "ticker", "event_ticker"])
    name_col = _pick_first_column(df, ["security_name", "security_name_x", "security_name_y"])

    series = []
    label_map = {0: "对照(inclusion=0)", 1: "处理(inclusion=1)"}
    color_map = {0: "#5c6b77", 1: "#a63b28"}
    for inclusion_value, group in df.groupby("inclusion", dropna=False):
        try:
            inc = int(inclusion_value)
        except (TypeError, ValueError):
            continue
        points: list[dict] = []
        for _, r in group.iterrows():
            point: dict = {
                "value": [
                    round(float(r["running_variable"]), 4),
                    round(float(r["car_m1_p1"]), 6),
                ],
            }
            if batch_col is not None and pd.notna(r.get(batch_col)):
                point["batch_id"] = str(r[batch_col])
            if ticker_col is not None and pd.notna(r.get(ticker_col)):
                point["ticker"] = str(r[ticker_col])
            if name_col is not None and pd.notna(r.get(name_col)):
                point["security_name"] = str(r[name_col])
            points.append(point)
        series.append(
            {
                "name": label_map.get(inc, f"inclusion={inc}"),
                "type": "scatter",
                "data": points,
                "color": color_map.get(inc, "#30424f"),
                "inclusion": inc,
            }
        )

    fits = _rdd_bandwidth_fits(df, cutoff_value=cutoff_value)
    available_bandwidths = [fit["bandwidth"] for fit in fits]
    if RDD_DEFAULT_BANDWIDTH in available_bandwidths:
        default_bw: float | None = RDD_DEFAULT_BANDWIDTH
    elif available_bandwidths:
        default_bw = available_bandwidths[len(available_bandwidths) // 2]
    else:
        default_bw = None

    return {
        "series": series,
        "cutoff": cutoff_value,
        "outcome": "car_m1_p1",
        "fits": fits,
        "default_bandwidth": default_bw,
    }


# ── Registry ─────────────────────────────────────────────────────────

CHART_BUILDERS: dict[str, Callable[[Path], dict[str, Any]]] = {
    "car_path": build_car_path_chart_data,
    "price_pressure": build_price_pressure_chart_data,
    "car_heatmap": build_car_heatmap_chart_data,
    "gap_decomposition": build_gap_decomposition_chart_data,
    "heterogeneity_size": build_heterogeneity_size_chart_data,
    "time_series_rolling": build_time_series_rolling_chart_data,
    "main_regression": build_main_regression_chart_data,
    "rdd_robustness": build_rdd_robustness_chart_data,
    "mechanism_regression": build_mechanism_regression_chart_data,
    "event_counts": build_event_counts_chart_data,
    "cma_mechanism_heatmap": build_cma_mechanism_heatmap_chart_data,
    "cma_gap_length_distribution": build_cma_gap_length_distribution_chart_data,
    "rdd_scatter": build_rdd_scatter_chart_data,
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
