from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats


@dataclass(frozen=True)
class WindowDefinition:
    start: int
    end: int

    @property
    def label(self) -> str:
        return f"[{self.start},+{self.end}]" if self.end >= 0 else f"[{self.start},{self.end}]"

    @property
    def slug(self) -> str:
        return f"m{abs(self.start)}_p{self.end}" if self.start < 0 else f"p{self.start}_p{self.end}"


def _normalise_windows(car_windows: list[list[int]] | list[tuple[int, int]]) -> list[WindowDefinition]:
    return [WindowDefinition(int(start), int(end)) for start, end in car_windows]


def _window_definition_from_slug(slug: str) -> WindowDefinition:
    if slug.startswith("m"):
        start_part, end_part = slug.split("_p", maxsplit=1)
        return WindowDefinition(-int(start_part[1:]), int(end_part))
    if slug.startswith("p"):
        start_part, end_part = slug.split("_p", maxsplit=1)
        return WindowDefinition(int(start_part[1:]), int(end_part))
    raise ValueError(f"Unsupported CAR window slug: {slug}")


def _summarise_values(values: pd.Series) -> dict[str, float | int]:
    clean = values.dropna().astype(float)
    n_obs = int(clean.count())
    mean_value = clean.mean() if not clean.empty else np.nan
    std_value = clean.std(ddof=1) if len(clean) > 1 else np.nan
    se_value = std_value / np.sqrt(n_obs) if len(clean) > 1 else np.nan
    ci_low = mean_value - 1.96 * se_value if pd.notna(se_value) else np.nan
    ci_high = mean_value + 1.96 * se_value if pd.notna(se_value) else np.nan
    t_stat = np.nan
    p_value = np.nan
    if len(clean) > 1:
        t_stat, p_value = stats.ttest_1samp(clean, popmean=0.0, nan_policy="omit")
    return {
        "n_events": n_obs,
        "mean_car": mean_value,
        "std_car": std_value,
        "se_car": se_value,
        "ci_low_95": ci_low,
        "ci_high_95": ci_high,
        "t_stat": t_stat,
        "p_value": p_value,
    }


def summarize_event_level_metrics(
    event_level: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]] | None = None,
    sample_filter: str | None = None,
) -> pd.DataFrame:
    if event_level.empty:
        return pd.DataFrame()

    work = event_level.copy()
    if "treatment_group" in work.columns:
        work = work.loc[work["treatment_group"] == 1].copy()
    if work.empty:
        return pd.DataFrame()

    windows = _normalise_windows(car_windows) if car_windows is not None else [
        _window_definition_from_slug(column.removeprefix("car_"))
        for column in work.columns
        if column.startswith("car_")
    ]
    windows = sorted(windows, key=lambda window: (window.start, window.end))

    summary_rows: list[dict[str, object]] = []
    for (market, event_phase, inclusion), group in work.groupby(["market", "event_phase", "inclusion"], dropna=False):
        for window in windows:
            column = f"car_{window.slug}"
            if column not in group.columns:
                continue
            row: dict[str, object] = {
                "market": market,
                "event_phase": event_phase,
                "inclusion": inclusion,
                "window": window.label,
                "window_slug": window.slug,
            }
            if sample_filter is not None:
                row["sample_filter"] = sample_filter
            row.update(_summarise_values(group[column]))
            summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def filter_nonoverlap_event_windows(
    frame: pd.DataFrame,
    *,
    days: int = 120,
    event_id_col: str = "event_id",
) -> pd.DataFrame:
    required = {"event_ticker", "event_phase", "event_date", event_id_col}
    if frame.empty or not required.issubset(frame.columns):
        return frame.copy()

    work = frame.copy()
    work["event_date"] = pd.to_datetime(work["event_date"], errors="coerce")
    key_frame = (
        work.loc[:, [event_id_col, "event_ticker", "event_phase", "event_date"]]
        .dropna(subset=["event_date"])
        .drop_duplicates()
        .sort_values(["event_ticker", "event_phase", "event_date", event_id_col])
        .copy()
    )
    if key_frame.empty:
        return work

    key_frame["prev_gap_days"] = key_frame.groupby(["event_ticker", "event_phase"], dropna=False)["event_date"].diff().dt.days
    key_frame["next_gap_days"] = (
        key_frame.groupby(["event_ticker", "event_phase"], dropna=False)["event_date"].diff(-1).abs().dt.days
    )
    prev_overlap = key_frame["prev_gap_days"].le(days).fillna(False)
    next_overlap = key_frame["next_gap_days"].le(days).fillna(False)
    key_frame["overlap_flag"] = prev_overlap | next_overlap
    valid_ids = key_frame.loc[~key_frame["overlap_flag"], event_id_col].astype(str)
    return work.loc[work[event_id_col].astype(str).isin(valid_ids)].copy()


def winsorize_event_level_metrics(
    event_level: pd.DataFrame,
    *,
    quantile: float = 0.01,
) -> pd.DataFrame:
    if event_level.empty:
        return event_level.copy()

    work = event_level.copy()
    car_columns = [column for column in work.columns if column.startswith("car_")]
    if not car_columns:
        return work

    group_columns = [column for column in ["market", "event_phase", "inclusion"] if column in work.columns]
    for column in car_columns:
        if group_columns:
            lower = work.groupby(group_columns, dropna=False)[column].transform(lambda series: series.quantile(quantile))
            upper = work.groupby(group_columns, dropna=False)[column].transform(lambda series: series.quantile(1 - quantile))
        else:
            lower = pd.Series(work[column].quantile(quantile), index=work.index)
            upper = pd.Series(work[column].quantile(1 - quantile), index=work.index)
        work[column] = work[column].clip(lower=lower, upper=upper)
    return work


def compute_event_level_metrics(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
) -> pd.DataFrame:
    windows = _normalise_windows(car_windows)
    rows: list[dict[str, object]] = []
    grouping = ["event_id", "event_phase"]
    for (_, _), group in panel.groupby(grouping, dropna=False):
        group = group.sort_values("relative_day").copy()
        first = group.iloc[0]
        metrics: dict[str, object] = {
            "event_id": first["event_id"],
            "matched_to_event_id": first.get("matched_to_event_id", pd.NA),
            "market": first["market"],
            "index_name": first["index_name"],
            "event_ticker": first["event_ticker"],
            "security_name": first.get("security_name", pd.NA),
            "event_phase": first["event_phase"],
            "event_type": first["event_type"],
            "inclusion": first["inclusion"],
            "treatment_group": first.get("treatment_group", 1),
            "batch_id": first.get("batch_id", pd.NA),
            "announce_date": first.get("announce_date", pd.NaT),
            "effective_date": first.get("effective_date", pd.NaT),
            "event_date": first["event_date"],
            "sector": first.get("sector", pd.NA),
        }
        for window in windows:
            mask = (group["relative_day"] >= window.start) & (group["relative_day"] <= window.end)
            metrics[f"car_{window.slug}"] = group.loc[mask, "ar"].sum()

        pre_group = group.loc[(group["relative_day"] >= -5) & (group["relative_day"] <= -1)]
        post_group = group.loc[(group["relative_day"] >= 0) & (group["relative_day"] <= 5)]
        pre_return_group = group.loc[(group["relative_day"] >= -20) & (group["relative_day"] <= -1)]
        metrics["pre_event_return"] = (
            (1.0 + pre_return_group["ret"].fillna(0.0)).prod() - 1.0 if not pre_return_group.empty else 0.0
        )
        reference_mkt_cap = group.loc[group["relative_day"] == -1, "mkt_cap"]
        if reference_mkt_cap.empty:
            reference_mkt_cap = group.loc[group["relative_day"] == 0, "mkt_cap"]
        metrics["log_mkt_cap"] = np.log(reference_mkt_cap.iloc[0]) if not reference_mkt_cap.empty and reference_mkt_cap.iloc[0] > 0 else np.nan
        metrics["turnover_change"] = post_group["turnover"].mean() - pre_group["turnover"].mean()
        metrics["volume_change"] = np.log1p(post_group["volume"].mean()) - np.log1p(pre_group["volume"].mean())
        metrics["volatility_change"] = post_group["ret"].std(ddof=0) - pre_group["ret"].std(ddof=0)
        rows.append(metrics)
    return pd.DataFrame(rows)


def compute_patell_bmp_summary(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
    *,
    estimation_window: tuple[int, int] = (-20, -2),
    group_columns: tuple[str, ...] = ("market", "event_phase", "inclusion"),
) -> pd.DataFrame:
    """Patell (1976) and BMP (1991) standardized event-study tests.

    Each AR_it is standardized by sigma_i estimated from a pre-event window;
    Patell Z assumes SCAR ~ N(0, 1) under H0; BMP t uses cross-sectional
    variance of SCARs (robust to event-induced variance increase).

    The default estimation window is [-20, -2] (in-panel proxy, ~18 days).
    The literature standard is [-250, -21]; see docs/limitations.md.
    """
    if panel.empty or "ar" not in panel.columns or "relative_day" not in panel.columns:
        return pd.DataFrame()

    work = panel.copy()
    if "treatment_group" in work.columns:
        work = work.loc[work["treatment_group"] == 1]
    if work.empty:
        return pd.DataFrame()

    est_lo, est_hi = estimation_window
    if est_hi < est_lo:
        raise ValueError("estimation_window must be (low, high) with low <= high")

    est_mask = work["relative_day"].between(est_lo, est_hi, inclusive="both")
    est_window = work.loc[est_mask, ["event_id", "event_phase", "ar"]].dropna(subset=["ar"])
    if est_window.empty:
        return pd.DataFrame()

    sigma_per_event = (
        est_window.groupby(["event_id", "event_phase"], dropna=False)["ar"]
        .std(ddof=1)
        .rename("sigma_estimation")
        .reset_index()
    )
    n_per_event = (
        est_window.groupby(["event_id", "event_phase"], dropna=False)["ar"]
        .count()
        .rename("n_estimation")
        .reset_index()
    )

    summary_rows: list[dict[str, object]] = []
    windows = _normalise_windows(car_windows)
    group_cols_list = list(group_columns)

    for window in windows:
        win_mask = work["relative_day"].between(window.start, window.end, inclusive="both")
        window_frame = work.loc[win_mask].copy()
        if window_frame.empty:
            continue

        agg_spec: dict[str, tuple[str, str]] = {
            "car": ("ar", "sum"),
            "window_obs": ("ar", "count"),
        }
        for col in group_cols_list:
            if col in window_frame.columns and col not in {"event_id", "event_phase"}:
                agg_spec[col] = (col, "first")

        car_per_event = (
            window_frame.groupby(["event_id", "event_phase"], dropna=False)
            .agg(**agg_spec)
            .reset_index()
        )

        merged = car_per_event.merge(
            sigma_per_event, on=["event_id", "event_phase"], how="left"
        ).merge(n_per_event, on=["event_id", "event_phase"], how="left")
        valid = merged.dropna(subset=["sigma_estimation"]).copy()
        valid = valid.loc[valid["sigma_estimation"] > 0]

        window_length = max(window.end - window.start + 1, 1)
        valid["scar"] = valid["car"] / (valid["sigma_estimation"] * np.sqrt(window_length))

        existing_group_cols = [col for col in group_cols_list if col in valid.columns]
        if not existing_group_cols:
            existing_group_cols = ["event_phase"]

        for keys, group in valid.groupby(existing_group_cols, dropna=False):
            n = len(group)
            if not isinstance(keys, tuple):
                keys = (keys,)
            row: dict[str, object] = dict(zip(existing_group_cols, keys, strict=False))
            row["window"] = window.label
            row["window_slug"] = window.slug
            row["n_events"] = n

            if n < 1:
                row.update(
                    {
                        "patell_z": np.nan,
                        "patell_p": np.nan,
                        "bmp_t": np.nan,
                        "bmp_p": np.nan,
                        "mean_scar": np.nan,
                        "std_scar": np.nan,
                    }
                )
                summary_rows.append(row)
                continue

            scar_values = group["scar"].to_numpy()
            patell_z = float(scar_values.sum() / np.sqrt(n))
            patell_p = float(2.0 * stats.norm.sf(abs(patell_z)))

            if n > 1:
                bmp_t_stat, bmp_p_val = stats.ttest_1samp(
                    scar_values, popmean=0.0, nan_policy="omit"
                )
                bmp_t = float(bmp_t_stat)
                bmp_p = float(bmp_p_val)
                std_scar = float(np.std(scar_values, ddof=1))
            else:
                bmp_t = np.nan
                bmp_p = np.nan
                std_scar = np.nan

            row.update(
                {
                    "patell_z": patell_z,
                    "patell_p": patell_p,
                    "bmp_t": bmp_t,
                    "bmp_p": bmp_p,
                    "mean_scar": float(np.mean(scar_values)),
                    "std_scar": std_scar,
                }
            )
            summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def compute_event_study(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    event_level = compute_event_level_metrics(panel, car_windows)

    path_frame = panel.sort_values(["event_id", "event_phase", "relative_day"]).copy()
    path_frame["car_path"] = path_frame.groupby(["event_id", "event_phase"], dropna=False)["ar"].cumsum()
    average_paths = (
        path_frame.groupby(["market", "event_phase", "inclusion", "relative_day"], dropna=False)
        .agg(
            mean_ar=("ar", "mean"),
            std_ar=("ar", lambda series: series.std(ddof=1)),
            mean_car=("car_path", "mean"),
            std_car=("car_path", lambda series: series.std(ddof=1)),
            n_obs=("ar", "size"),
        )
        .reset_index()
    )
    average_paths["se_ar"] = average_paths["std_ar"] / np.sqrt(average_paths["n_obs"].where(average_paths["n_obs"] > 1))
    average_paths["se_car"] = average_paths["std_car"] / np.sqrt(average_paths["n_obs"].where(average_paths["n_obs"] > 1))
    average_paths["ci_low_95"] = average_paths["mean_car"] - 1.96 * average_paths["se_car"]
    average_paths["ci_high_95"] = average_paths["mean_car"] + 1.96 * average_paths["se_car"]

    summary = summarize_event_level_metrics(event_level, car_windows)
    return event_level, summary, average_paths
