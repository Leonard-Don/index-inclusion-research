"""Low-cost robustness add-ons for the descriptive event study.

This module is strictly *additive*: it never mutates an existing estimator,
result CSV, or baseline. Three independent robustness checks live here, each a
pure, typed, seeded function that downstream callers (figures pipeline, a
future console entry) can opt into:

1. :func:`compute_parallel_trends_aar` — per-relative-day average abnormal
   return (AAR) paths for treated vs matched controls. The pre-event window
   (e.g. ``[-20, -2]``) should overlap (quasi-parallel) and the two paths
   should diverge only inside the event window. Feeds a plotting builder.

2. :func:`compute_placebo_car_distribution` — for each inclusion event draw
   several *non-event* pseudo-days, recompute CAR on those windows, and build
   a placebo null distribution. Reports where the real event-window CAR sits
   in that distribution (empirical p-value). Fully seeded.

3. :func:`compute_main_car_permutation_test` and
   :func:`compute_event_clustered_car_se` — a permutation test for the main
   event-study CAR (sign-flip / label-shuffle style, mirroring
   ``cross_market_asymmetry.h6_robustness``) plus an event/batch-clustered
   CRV1 standard-error table promoted from the announcement-day smoke to a
   reportable artifact.

Conventions mirrored from ``analysis/event_study.py`` and
``cross_market_asymmetry/h6_robustness.py``: ``from __future__`` annotations,
NumPy/pandas/statsmodels/scipy stack, ``np.random.default_rng(seed)`` for every
random draw, and Monte-Carlo p-values use the ``(extreme + 1) / (B + 1)``
convention so a finite permutation budget never reports ``p == 0``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

# --------------------------------------------------------------------------- #
# Shared column schemas (anchor empty-frame round-trips, mirroring the
# event_study.py ``_empty_*_frame`` pattern so a "no data" run still writes a
# CSV that ``pd.read_csv`` accepts).
# --------------------------------------------------------------------------- #

PARALLEL_TRENDS_COLUMNS: tuple[str, ...] = (
    "market",
    "event_phase",
    "relative_day",
    "treated_aar",
    "control_aar",
    "treated_aar_se",
    "control_aar_se",
    "aar_gap",
    "n_treated",
    "n_control",
)

PLACEBO_CAR_COLUMNS: tuple[str, ...] = (
    "market",
    "event_phase",
    "window",
    "window_slug",
    "real_mean_car",
    "placebo_mean",
    "placebo_std",
    "placebo_q025",
    "placebo_q975",
    "empirical_p_value",
    "n_events",
    "n_events_effective",
    "n_placebo_draws",
    "seed",
)

PERMUTATION_CAR_COLUMNS: tuple[str, ...] = (
    "market",
    "event_phase",
    "window",
    "window_slug",
    "observed_mean_car",
    "permutation_mean",
    "permutation_std",
    "empirical_p_value",
    "n_events",
    "n_permutations",
    "seed",
)

CLUSTERED_CAR_COLUMNS: tuple[str, ...] = (
    "market",
    "event_phase",
    "window",
    "window_slug",
    "cluster_col",
    "mean_car",
    "se_iid",
    "se_clustered",
    "t_iid",
    "t_clustered",
    "p_iid",
    "p_clustered",
    "n_events",
    "n_clusters",
)

# A CRV1 clustered SE needs at least two distinct clusters to be defined.
_MIN_CLUSTERS = 2
# Permutation / placebo draws need enough events to have any cross-sectional
# null variation worth reporting.
_MIN_EVENTS = 4


def _window_slug(start: int, end: int) -> str:
    return f"m{abs(start)}_p{end}" if start < 0 else f"p{start}_p{end}"


def _window_label(start: int, end: int) -> str:
    return f"[{start},+{end}]" if end >= 0 else f"[{start},{end}]"


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalise_windows(
    car_windows: list[list[int]] | list[tuple[int, int]],
) -> list[tuple[int, int]]:
    normalised: list[tuple[int, int]] = []
    for start, end in car_windows:
        start_i, end_i = int(start), int(end)
        if end_i < start_i:
            raise ValueError(f"window ({start_i}, {end_i}) must have start <= end")
        normalised.append((start_i, end_i))
    return normalised


# --------------------------------------------------------------------------- #
# 1. Parallel-trends AAR paths (treated vs matched control)
# --------------------------------------------------------------------------- #


def compute_parallel_trends_aar(
    matched_panel: pd.DataFrame,
    *,
    ar_column: str = "ar",
    treatment_col: str = "treatment_group",
    relative_day_col: str = "relative_day",
) -> pd.DataFrame:
    """Per-relative-day AAR paths for treated vs matched controls.

    For each ``(market, event_phase, relative_day)`` cell the function averages
    the abnormal return across treated events (``treatment_group == 1``) and,
    separately, across matched controls (``treatment_group == 0``). The
    treated-minus-control gap (``aar_gap``) should hover near zero across the
    pre-event window and open up only inside the event window — the visual
    parallel-trends check.

    The matched control group is already covariate-balanced upstream
    (``pipeline.matching.build_matched_sample`` / ``compute_covariate_balance``,
    SMD < 0.25), so this is a descriptive overlay rather than a new estimator.
    The input panel and its ``ar`` column are read but never mutated.

    Returns one row per ``(market, event_phase, relative_day)`` with both AAR
    levels, their cross-sectional standard errors, the gap, and per-group
    counts. Returns an empty (schema-anchored) frame when the panel is empty or
    lacks the required columns.
    """
    required = {"market", "event_phase", treatment_col, relative_day_col, ar_column}
    if matched_panel.empty or not required.issubset(matched_panel.columns):
        return _empty_frame(PARALLEL_TRENDS_COLUMNS)

    work = matched_panel[list(required)].copy()
    work[ar_column] = pd.to_numeric(work[ar_column], errors="coerce")
    work[treatment_col] = pd.to_numeric(work[treatment_col], errors="coerce")
    work = work.dropna(subset=[ar_column, treatment_col, relative_day_col])
    if work.empty:
        return _empty_frame(PARALLEL_TRENDS_COLUMNS)
    work[relative_day_col] = work[relative_day_col].astype(int)

    rows: list[dict[str, object]] = []
    group_keys = ["market", "event_phase", relative_day_col]
    for (market, event_phase, rel_day), group in work.groupby(group_keys, dropna=False):
        treated = group.loc[group[treatment_col] == 1, ar_column]
        control = group.loc[group[treatment_col] == 0, ar_column]
        treated_mean = float(treated.mean()) if not treated.empty else np.nan
        control_mean = float(control.mean()) if not control.empty else np.nan
        treated_se = (
            float(treated.std(ddof=1) / np.sqrt(len(treated))) if len(treated) > 1 else np.nan
        )
        control_se = (
            float(control.std(ddof=1) / np.sqrt(len(control))) if len(control) > 1 else np.nan
        )
        gap = (
            treated_mean - control_mean
            if np.isfinite(treated_mean) and np.isfinite(control_mean)
            else np.nan
        )
        rows.append(
            {
                "market": market,
                "event_phase": event_phase,
                "relative_day": int(rel_day),
                "treated_aar": treated_mean,
                "control_aar": control_mean,
                "treated_aar_se": treated_se,
                "control_aar_se": control_se,
                "aar_gap": gap,
                "n_treated": int(len(treated)),
                "n_control": int(len(control)),
            }
        )

    frame = pd.DataFrame(rows, columns=list(PARALLEL_TRENDS_COLUMNS))
    return frame.sort_values(["market", "event_phase", "relative_day"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 2. Placebo pseudo-event-day CAR distribution
# --------------------------------------------------------------------------- #


def _per_event_window_car(
    event_panel: pd.DataFrame,
    *,
    start: int,
    end: int,
    ar_column: str,
    relative_day_col: str,
) -> float:
    mask = event_panel[relative_day_col].between(start, end, inclusive="both")
    window_ar = event_panel.loc[mask, ar_column]
    return float(window_ar.sum(min_count=1))


def _placebo_window_car(
    event_panel: pd.DataFrame,
    *,
    placebo_center: int,
    span: int,
    ar_column: str,
    relative_day_col: str,
) -> float:
    """CAR over a span-length window centred on a pseudo (non-event) day."""
    start = placebo_center
    end = placebo_center + span - 1
    mask = event_panel[relative_day_col].between(start, end, inclusive="both")
    window_ar = event_panel.loc[mask, ar_column]
    if window_ar.dropna().empty:
        return np.nan
    return float(window_ar.sum(min_count=1))


def compute_placebo_car_distribution(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
    *,
    ar_column: str = "ar",
    event_id_col: str = "event_id",
    phase_col: str = "event_phase",
    relative_day_col: str = "relative_day",
    treatment_col: str = "treatment_group",
    n_placebo_draws: int = 200,
    event_window_buffer: int = 2,
    seed: int = 20260607,
) -> pd.DataFrame:
    """Pseudo-event-day placebo CAR distribution with an empirical p-value.

    For each ``(market, event_phase)`` cell and each requested CAR window the
    real cross-sectional mean CAR is computed on the true event window. Then,
    for ``n_placebo_draws`` Monte-Carlo iterations, every event is re-anchored
    on a randomly chosen *non-event* relative day (a placebo center at least
    ``event_window_buffer`` days away from day 0 and far enough from the panel
    edges to fit the window). The mean CAR over those placebo windows builds a
    null distribution; the two-sided empirical p-value is the fraction of
    placebo means whose absolute value is at least the real ``|mean CAR|``,
    using the ``(extreme + 1) / (draws + 1)`` Monte-Carlo convention.

    Only treated events (``treatment_group == 1``) drive the distribution, so
    the check mirrors the main event study. Every draw goes through a single
    ``np.random.default_rng(seed)`` so the table is bit-reproducible.

    Returns one row per ``(market, event_phase, window)``; an empty
    schema-anchored frame when the panel is empty / lacks columns / has too few
    events. ``n_events`` is the raw event-id group count for the cell, while
    ``n_events_effective`` is the number of those events that contribute a
    finite window CAR to ``real_mean_car`` — the two differ whenever an event
    lacks a usable abnormal return in the window (e.g. delisted / acquired
    tickers with no price), and ``n_events_effective`` is the N to compare
    against the permutation / clustered-SE tables.
    """
    windows = _normalise_windows(car_windows)
    required = {"market", phase_col, event_id_col, relative_day_col, ar_column}
    if panel.empty or not required.issubset(panel.columns) or not windows:
        return _empty_frame(PLACEBO_CAR_COLUMNS)

    work = panel.copy()
    if treatment_col in work.columns:
        work = work.loc[pd.to_numeric(work[treatment_col], errors="coerce") == 1]
    if work.empty:
        return _empty_frame(PLACEBO_CAR_COLUMNS)
    work[ar_column] = pd.to_numeric(work[ar_column], errors="coerce")
    work[relative_day_col] = pd.to_numeric(work[relative_day_col], errors="coerce")
    work = work.dropna(subset=[relative_day_col])
    work[relative_day_col] = work[relative_day_col].astype(int)
    if work.empty:
        return _empty_frame(PLACEBO_CAR_COLUMNS)

    day_min = int(work[relative_day_col].min())
    day_max = int(work[relative_day_col].max())

    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []

    for (market, event_phase), cell in work.groupby(["market", phase_col], dropna=False):
        event_groups = {
            event_id: grp for event_id, grp in cell.groupby(event_id_col, dropna=False)
        }
        n_events = len(event_groups)

        for start, end in windows:
            span = end - start + 1
            slug = _window_slug(start, end)
            label = _window_label(start, end)

            real_cars = [
                _per_event_window_car(
                    grp,
                    start=start,
                    end=end,
                    ar_column=ar_column,
                    relative_day_col=relative_day_col,
                )
                for grp in event_groups.values()
            ]
            real_series = pd.Series(real_cars, dtype=float).dropna()
            real_mean = float(real_series.mean()) if not real_series.empty else np.nan
            # ``n_events`` is the raw (market, phase) cell group count; the real
            # mean CAR is averaged only over events with a finite window sum
            # (delisted / acquired tickers carry all-NaN window AR), so report
            # the *effective* N beside it — matching the N the permutation and
            # clustered-SE tables report and avoiding a CSV that looks
            # internally inconsistent to a reviewer cross-checking the columns.
            n_events_effective = int(real_series.size)

            # Candidate placebo centers: any window of length ``span`` that
            # fits inside the panel and whose own span never overlaps the real
            # event window's buffered neighbourhood [-buffer, +buffer].
            candidate_centers = [
                c
                for c in range(day_min, day_max - span + 2)
                # placebo window [c, c+span-1] must clear the buffered event zone
                if (c + span - 1) < -event_window_buffer or c > event_window_buffer
            ]

            if (
                n_events < _MIN_EVENTS
                or not np.isfinite(real_mean)
                or not candidate_centers
            ):
                rows.append(
                    {
                        "market": market,
                        "event_phase": event_phase,
                        "window": label,
                        "window_slug": slug,
                        "real_mean_car": real_mean,
                        "placebo_mean": np.nan,
                        "placebo_std": np.nan,
                        "placebo_q025": np.nan,
                        "placebo_q975": np.nan,
                        "empirical_p_value": np.nan,
                        "n_events": int(n_events),
                        "n_events_effective": n_events_effective,
                        "n_placebo_draws": 0,
                        "seed": int(seed),
                    }
                )
                continue

            placebo_means: list[float] = []
            centers_arr = np.asarray(candidate_centers, dtype=int)
            for _ in range(n_placebo_draws):
                # Each event independently picks a pseudo-day, mirroring how
                # the real study anchors each event on its own day 0.
                draw_cars: list[float] = []
                chosen = rng.integers(0, len(centers_arr), size=n_events)
                for grp, center_idx in zip(event_groups.values(), chosen, strict=True):
                    car = _placebo_window_car(
                        grp,
                        placebo_center=int(centers_arr[center_idx]),
                        span=span,
                        ar_column=ar_column,
                        relative_day_col=relative_day_col,
                    )
                    if np.isfinite(car):
                        draw_cars.append(car)
                if draw_cars:
                    placebo_means.append(float(np.mean(draw_cars)))

            placebo_arr = np.asarray(placebo_means, dtype=float)
            if placebo_arr.size == 0:
                rows.append(
                    {
                        "market": market,
                        "event_phase": event_phase,
                        "window": label,
                        "window_slug": slug,
                        "real_mean_car": real_mean,
                        "placebo_mean": np.nan,
                        "placebo_std": np.nan,
                        "placebo_q025": np.nan,
                        "placebo_q975": np.nan,
                        "empirical_p_value": np.nan,
                        "n_events": int(n_events),
                        "n_events_effective": n_events_effective,
                        "n_placebo_draws": 0,
                        "seed": int(seed),
                    }
                )
                continue

            extreme = int(np.sum(np.abs(placebo_arr) >= abs(real_mean)))
            p_value = (extreme + 1) / (placebo_arr.size + 1)
            rows.append(
                {
                    "market": market,
                    "event_phase": event_phase,
                    "window": label,
                    "window_slug": slug,
                    "real_mean_car": real_mean,
                    "placebo_mean": float(np.mean(placebo_arr)),
                    "placebo_std": float(np.std(placebo_arr, ddof=1))
                    if placebo_arr.size > 1
                    else np.nan,
                    "placebo_q025": float(np.quantile(placebo_arr, 0.025)),
                    "placebo_q975": float(np.quantile(placebo_arr, 0.975)),
                    "empirical_p_value": float(p_value),
                    "n_events": int(n_events),
                    "n_events_effective": n_events_effective,
                    "n_placebo_draws": int(placebo_arr.size),
                    "seed": int(seed),
                }
            )

    if not rows:
        return _empty_frame(PLACEBO_CAR_COLUMNS)
    return pd.DataFrame(rows, columns=list(PLACEBO_CAR_COLUMNS))


def export_placebo_car_distribution(frame: pd.DataFrame, *, output_dir: Path | str) -> Path:
    """Write the placebo CAR table to ``results/real_tables`` (or any dir)."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "robustness_placebo_car.csv"
    frame.to_csv(out_path, index=False)
    return out_path


# --------------------------------------------------------------------------- #
# 3a. Permutation test for the main event-study CAR
# --------------------------------------------------------------------------- #


def compute_main_car_permutation_test(
    event_level: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]] | None = None,
    *,
    treatment_col: str = "treatment_group",
    n_permutations: int = 5_000,
    seed: int = 20260607,
) -> pd.DataFrame:
    """Sign-flip permutation test for the main event-study mean CAR.

    Mirrors the permutation style in
    ``cross_market_asymmetry.h6_robustness._permutation_quartile_spread``: a
    distribution-free complement to the parametric one-sample t-test that backs
    the main CAR. Under the sharp null "the index-inclusion event has no effect
    on CAR", each event's CAR sign is exchangeable, so we randomly flip signs
    ``n_permutations`` times and record the permuted mean. The two-sided
    empirical p-value is the fraction of permuted means whose absolute value is
    at least the observed ``|mean CAR|`` (``(extreme + 1) / (B + 1)``).

    Reads the per-event CAR columns (``car_<slug>``) that
    ``compute_event_level_metrics`` already wrote; only treated events
    (``treatment_group == 1``) are used. Seeded via ``np.random.default_rng``.

    Returns one row per ``(market, event_phase, window)``; empty schema-anchored
    frame for empty / column-less / too-few-event inputs.
    """
    if event_level.empty:
        return _empty_frame(PERMUTATION_CAR_COLUMNS)

    work = event_level.copy()
    if treatment_col in work.columns:
        work = work.loc[pd.to_numeric(work[treatment_col], errors="coerce") == 1]
    if work.empty or "market" not in work.columns or "event_phase" not in work.columns:
        return _empty_frame(PERMUTATION_CAR_COLUMNS)

    if car_windows is not None:
        windows = _normalise_windows(car_windows)
        slugs = [(_window_slug(s, e), _window_label(s, e)) for s, e in windows]
    else:
        slugs = []
        for column in work.columns:
            if not column.startswith("car_"):
                continue
            slug = column.removeprefix("car_")
            try:
                start, end = _slug_to_window(slug)
            except ValueError:
                continue
            slugs.append((slug, _window_label(start, end)))
        slugs.sort()

    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []

    for (market, event_phase), cell in work.groupby(["market", "event_phase"], dropna=False):
        for slug, label in slugs:
            column = f"car_{slug}"
            if column not in cell.columns:
                continue
            values = pd.to_numeric(cell[column], errors="coerce").dropna().to_numpy()
            n_events = int(values.size)
            if n_events < _MIN_EVENTS:
                rows.append(
                    _permutation_nan_row(market, event_phase, label, slug, n_events, n_permutations, seed)
                )
                continue

            observed_mean = float(values.mean())
            # Vectorised sign-flip: signs ∈ {-1, +1}, shape (B, n_events).
            signs = rng.choice(np.array([-1.0, 1.0]), size=(n_permutations, n_events))
            permuted_means = (signs * values).mean(axis=1)
            extreme = int(np.sum(np.abs(permuted_means) >= abs(observed_mean)))
            p_value = (extreme + 1) / (n_permutations + 1)
            rows.append(
                {
                    "market": market,
                    "event_phase": event_phase,
                    "window": label,
                    "window_slug": slug,
                    "observed_mean_car": observed_mean,
                    "permutation_mean": float(permuted_means.mean()),
                    "permutation_std": float(permuted_means.std(ddof=1)),
                    "empirical_p_value": float(p_value),
                    "n_events": n_events,
                    "n_permutations": int(n_permutations),
                    "seed": int(seed),
                }
            )

    if not rows:
        return _empty_frame(PERMUTATION_CAR_COLUMNS)
    return pd.DataFrame(rows, columns=list(PERMUTATION_CAR_COLUMNS))


def _permutation_nan_row(
    market: object,
    event_phase: object,
    label: str,
    slug: str,
    n_events: int,
    n_permutations: int,
    seed: int,
) -> dict[str, object]:
    return {
        "market": market,
        "event_phase": event_phase,
        "window": label,
        "window_slug": slug,
        "observed_mean_car": np.nan,
        "permutation_mean": np.nan,
        "permutation_std": np.nan,
        "empirical_p_value": np.nan,
        "n_events": int(n_events),
        "n_permutations": 0,
        "seed": int(seed),
    }


def _slug_to_window(slug: str) -> tuple[int, int]:
    if slug.startswith("m"):
        start_part, end_part = slug.split("_p", maxsplit=1)
        return -int(start_part[1:]), int(end_part)
    if slug.startswith("p"):
        start_part, end_part = slug.split("_p", maxsplit=1)
        return int(start_part[1:]), int(end_part)
    raise ValueError(f"unsupported CAR window slug: {slug}")


def export_main_car_permutation_test(frame: pd.DataFrame, *, output_dir: Path | str) -> Path:
    """Write the permutation-test table to ``results/real_tables`` (or any dir)."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "robustness_car_permutation.csv"
    frame.to_csv(out_path, index=False)
    return out_path


# --------------------------------------------------------------------------- #
# 3b. Event/batch-clustered CRV1 standard errors (reportable table)
# --------------------------------------------------------------------------- #


def compute_event_clustered_car_se(
    event_level: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]] | None = None,
    *,
    cluster_col: str = "event_date",
    treatment_col: str = "treatment_group",
) -> pd.DataFrame:
    """Promote CRV1 event/batch-clustered SE to a reportable CAR table.

    For each ``(market, event_phase, window)`` cell the per-event CAR is
    regressed on a constant; the constant's coefficient is the mean CAR. Two
    SE flavours sit side by side: the iid OLS SE (PRIMARY, unchanged from the
    one-sample-t convention used elsewhere) and the cluster-robust (CRV1) SE
    clustered on ``cluster_col`` (default ``event_date``; pass ``batch_id`` to
    cluster on inclusion batches). Events that share a date / batch are not
    independent, so the clustered SE typically widens relative to iid.

    This reuses ``statsmodels`` ``cov_type="cluster"`` — a core dependency — so
    it runs unconditionally without the optional ``pyfixest`` methods extra,
    unlike ``pyfixest_cluster.estimate_announcement_day_cluster_se`` (kept as a
    parallel cross-engine smoke). Clustered columns are NaN when a cell has
    fewer than two distinct clusters.

    Returns one row per ``(market, event_phase, window)``; empty schema-anchored
    frame for empty / column-less inputs.
    """
    if event_level.empty:
        return _empty_frame(CLUSTERED_CAR_COLUMNS)

    work = event_level.copy()
    if treatment_col in work.columns:
        work = work.loc[pd.to_numeric(work[treatment_col], errors="coerce") == 1]
    required = {"market", "event_phase", cluster_col}
    if work.empty or not required.issubset(work.columns):
        return _empty_frame(CLUSTERED_CAR_COLUMNS)

    if car_windows is not None:
        windows = _normalise_windows(car_windows)
        slugs = [(_window_slug(s, e), _window_label(s, e)) for s, e in windows]
    else:
        slugs = []
        for column in work.columns:
            if not column.startswith("car_"):
                continue
            slug = column.removeprefix("car_")
            try:
                start, end = _slug_to_window(slug)
            except ValueError:
                continue
            slugs.append((slug, _window_label(start, end)))
        slugs.sort()

    rows: list[dict[str, object]] = []
    for (market, event_phase), cell in work.groupby(["market", "event_phase"], dropna=False):
        for slug, label in slugs:
            column = f"car_{slug}"
            if column not in cell.columns:
                continue
            paired = pd.DataFrame(
                {
                    "car": pd.to_numeric(cell[column], errors="coerce"),
                    "cluster": cell[cluster_col],
                }
            ).dropna()
            n_events = int(len(paired))
            n_clusters = int(paired["cluster"].nunique()) if n_events else 0
            stats_row = _clustered_regression_stats(paired)
            rows.append(
                {
                    "market": market,
                    "event_phase": event_phase,
                    "window": label,
                    "window_slug": slug,
                    "cluster_col": cluster_col,
                    "mean_car": stats_row["mean_car"],
                    "se_iid": stats_row["se_iid"],
                    "se_clustered": stats_row["se_clustered"],
                    "t_iid": stats_row["t_iid"],
                    "t_clustered": stats_row["t_clustered"],
                    "p_iid": stats_row["p_iid"],
                    "p_clustered": stats_row["p_clustered"],
                    "n_events": n_events,
                    "n_clusters": n_clusters,
                }
            )

    if not rows:
        return _empty_frame(CLUSTERED_CAR_COLUMNS)
    return pd.DataFrame(rows, columns=list(CLUSTERED_CAR_COLUMNS))


def _clustered_regression_stats(paired: pd.DataFrame) -> dict[str, float]:
    """iid and CRV1-clustered intercept stats for a single CAR cell."""
    nan_stats = {
        "mean_car": np.nan,
        "se_iid": np.nan,
        "se_clustered": np.nan,
        "t_iid": np.nan,
        "t_clustered": np.nan,
        "p_iid": np.nan,
        "p_clustered": np.nan,
    }
    if len(paired) < 2:
        if len(paired) == 1:
            nan_stats["mean_car"] = float(paired["car"].iloc[0])
        return nan_stats

    outcome = paired["car"].to_numpy(dtype=float)
    design = np.ones((len(outcome), 1), dtype=float)
    mean_car = float(outcome.mean())

    try:
        iid_model = sm.OLS(outcome, design).fit()
        se_iid = float(iid_model.bse[0])
        t_iid = float(iid_model.tvalues[0])
        p_iid = float(iid_model.pvalues[0])
    except (ValueError, np.linalg.LinAlgError):
        se_iid = t_iid = p_iid = np.nan

    se_clustered = t_clustered = p_clustered = np.nan
    if paired["cluster"].nunique() >= _MIN_CLUSTERS:
        groups = pd.factorize(paired["cluster"])[0]
        try:
            clustered_model = sm.OLS(outcome, design).fit(
                cov_type="cluster", cov_kwds={"groups": groups}
            )
            se_clustered = float(clustered_model.bse[0])
            t_clustered = float(clustered_model.tvalues[0])
            p_clustered = float(clustered_model.pvalues[0])
            if not np.isfinite(se_clustered):
                se_clustered = t_clustered = p_clustered = np.nan
        except (ValueError, np.linalg.LinAlgError):
            se_clustered = t_clustered = p_clustered = np.nan

    return {
        "mean_car": mean_car,
        "se_iid": se_iid,
        "se_clustered": se_clustered,
        "t_iid": t_iid,
        "t_clustered": t_clustered,
        "p_iid": p_iid,
        "p_clustered": p_clustered,
    }


def export_event_clustered_car_se(frame: pd.DataFrame, *, output_dir: Path | str) -> Path:
    """Write the clustered-SE table to ``results/real_tables`` (or any dir)."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "robustness_car_clustered_se.csv"
    frame.to_csv(out_path, index=False)
    return out_path


def export_parallel_trends_aar(frame: pd.DataFrame, *, output_dir: Path | str) -> Path:
    """Write the parallel-trends AAR path table (companion to the figure)."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "robustness_parallel_trends_aar.csv"
    frame.to_csv(out_path, index=False)
    return out_path
