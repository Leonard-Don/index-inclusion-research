from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .panel import map_to_trading_date


def _compute_security_snapshot(
    prices: pd.DataFrame,
    market: str,
    ticker: str,
    reference_date: pd.Timestamp,
    lookback_days: int,
) -> dict[str, object] | None:
    history = prices.loc[(prices["market"] == market) & (prices["ticker"] == ticker)].sort_values("date")
    if history.empty:
        return None
    mapped_reference = map_to_trading_date(reference_date, history["date"].tolist())
    if pd.isna(mapped_reference):
        return None

    history = history.loc[history["date"] <= mapped_reference].copy()
    if history.empty:
        return None
    window = history.tail(lookback_days + 1).copy()
    if window.empty:
        return None
    latest = window.iloc[-1]
    pre_window = window.iloc[:-1]
    pre_return = (1.0 + pre_window["ret"].fillna(0.0)).prod() - 1.0 if not pre_window.empty else 0.0
    pre_volatility = pre_window["ret"].std(ddof=0) if len(pre_window) > 1 else 0.0
    return {
        "market": market,
        "ticker": ticker,
        "reference_date": mapped_reference,
        "mkt_cap": latest.get("mkt_cap", np.nan),
        "sector": latest.get("sector", pd.NA),
        "pre_event_return": pre_return,
        "pre_event_volatility": pre_volatility,
    }


def _distance_score(
    target: dict[str, object],
    candidate: dict[str, object],
    *,
    size_weight: float = 1.0,
    return_weight: float = 1.0,
    volatility_weight: float = 0.5,
    sector_mismatch_penalty: float = 0.25,
    larger_mkt_cap_penalty: float = 1.0,
    lower_volatility_penalty: float = 1.0,
) -> float:
    target_cap = target.get("mkt_cap")
    candidate_cap = candidate.get("mkt_cap")
    if pd.isna(target_cap) or pd.isna(candidate_cap) or target_cap <= 0 or candidate_cap <= 0:
        return math.inf
    target_log_cap = np.log(target_cap)
    candidate_log_cap = np.log(candidate_cap)
    size_distance = abs(target_log_cap - candidate_log_cap)
    if candidate_log_cap > target_log_cap:
        size_distance *= larger_mkt_cap_penalty
    return_distance = abs(float(target["pre_event_return"]) - float(candidate["pre_event_return"]))
    target_volatility = float(target["pre_event_volatility"])
    candidate_volatility = float(candidate["pre_event_volatility"])
    vol_distance = abs(target_volatility - candidate_volatility)
    if candidate_volatility < target_volatility:
        vol_distance *= lower_volatility_penalty
    sector_distance = 0.0 if target.get("sector") == candidate.get("sector") else sector_mismatch_penalty
    return float(
        size_weight * size_distance
        + return_weight * return_distance
        + volatility_weight * vol_distance
        + sector_distance
    )


def build_matched_sample(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    lookback_days: int = 20,
    num_controls: int = 3,
    reference_date_column: str = "announce_date",
    sector_filter_mode: str = "exact_when_available",
    distance_weights: dict[str, float] | None = None,
    directional_penalties: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    distance_weights = distance_weights or {}
    directional_penalties = directional_penalties or {}
    size_weight = float(distance_weights.get("size", 1.0))
    return_weight = float(distance_weights.get("pre_event_return", 1.0))
    volatility_weight = float(distance_weights.get("pre_event_volatility", 0.5))
    sector_mismatch_penalty = float(distance_weights.get("sector_mismatch", 0.25))
    larger_mkt_cap_penalty = float(directional_penalties.get("larger_mkt_cap", 1.0))
    lower_volatility_penalty = float(directional_penalties.get("lower_pre_event_volatility", 1.0))
    valid_sector_modes = {"exact_when_available", "penalized"}
    if sector_filter_mode not in valid_sector_modes:
        raise ValueError(f"sector_filter_mode must be one of {sorted(valid_sector_modes)}")

    treated = events.copy()
    if "treatment_group" not in treated.columns:
        treated["treatment_group"] = 1
    treated["treatment_group"] = treated["treatment_group"].fillna(1).astype(int)
    treated = treated.loc[treated["treatment_group"] == 1].copy()
    treated_tickers = set(treated["ticker"].astype(str))
    history_by_key = {
        (market, ticker): group.sort_values("date").reset_index(drop=True)
        for (market, ticker), group in prices.groupby(["market", "ticker"], dropna=False)
    }
    tickers_by_market = {
        market: sorted(group["ticker"].astype(str).unique().tolist())
        for market, group in prices.groupby("market")
    }
    snapshot_cache: dict[tuple[str, str, str, int], dict[str, object] | None] = {}

    def get_snapshot(market: str, ticker: str, reference_date: pd.Timestamp) -> dict[str, object] | None:
        cache_key = (market, str(ticker), pd.Timestamp(reference_date).date().isoformat(), lookback_days)
        if cache_key in snapshot_cache:
            return snapshot_cache[cache_key]
        history = history_by_key.get((market, ticker))
        if history is None:
            snapshot_cache[cache_key] = None
            return None
        mapped_snapshot = _compute_security_snapshot(
            prices=history,
            market=market,
            ticker=ticker,
            reference_date=reference_date,
            lookback_days=lookback_days,
        )
        snapshot_cache[cache_key] = mapped_snapshot
        return mapped_snapshot

    matched_rows: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    for event in treated.itertuples(index=False):
        reference_date = getattr(event, reference_date_column)
        target_snapshot = get_snapshot(
            market=event.market,
            ticker=event.ticker,
            reference_date=reference_date,
        )
        if target_snapshot is None:
            diagnostics.append(
                {
                    "event_id": event.event_id,
                    "status": "skipped_missing_target_snapshot",
                    "selected_controls": 0,
                }
            )
            continue

        candidates: list[dict[str, object]] = []
        for candidate_ticker in tickers_by_market.get(event.market, []):
            if candidate_ticker == event.ticker or str(candidate_ticker) in treated_tickers:
                continue
            candidate_snapshot = get_snapshot(
                market=event.market,
                ticker=candidate_ticker,
                reference_date=reference_date,
            )
            if candidate_snapshot is None:
                continue
            candidates.append(candidate_snapshot)

        if not candidates:
            diagnostics.append(
                {
                    "event_id": event.event_id,
                    "status": "skipped_no_candidates",
                    "selected_controls": 0,
                }
            )
            continue

        candidate_frame = pd.DataFrame(candidates)
        if (
            sector_filter_mode == "exact_when_available"
            and pd.notna(target_snapshot.get("sector"))
            and "sector" in candidate_frame
        ):
            sector_candidates = candidate_frame.loc[candidate_frame["sector"] == target_snapshot["sector"]].copy()
        else:
            sector_candidates = candidate_frame.iloc[0:0].copy()
        relaxed_sector = sector_candidates.empty
        if relaxed_sector:
            sector_candidates = candidate_frame.copy()

        target = target_snapshot
        sector_candidates["distance"] = sector_candidates.apply(
            lambda row, target=target: _distance_score(
                target,
                row.to_dict(),
                size_weight=size_weight,
                return_weight=return_weight,
                volatility_weight=volatility_weight,
                sector_mismatch_penalty=sector_mismatch_penalty,
                larger_mkt_cap_penalty=larger_mkt_cap_penalty,
                lower_volatility_penalty=lower_volatility_penalty,
            ),
            axis=1,
        )
        sector_candidates = sector_candidates.replace([np.inf, -np.inf], np.nan).dropna(subset=["distance"])
        selected = sector_candidates.nsmallest(num_controls, "distance")
        if sector_filter_mode == "penalized" and pd.notna(target_snapshot.get("sector")) and not selected.empty:
            relaxed_sector = bool((selected["sector"] != target_snapshot["sector"]).any())

        diagnostics.append(
            {
                "event_id": event.event_id,
                "status": "matched" if not selected.empty else "skipped_no_valid_match",
                "selected_controls": int(len(selected)),
                "sector_relaxed": relaxed_sector,
                "sector_filter_mode": sector_filter_mode,
            }
        )
        for rank, candidate in enumerate(selected.itertuples(index=False), start=1):
            matched_rows.append(
                {
                    **event._asdict(),
                    "event_id": f"{event.event_id}-ctrl-{rank:02d}",
                    "ticker": candidate.ticker,
                    "treatment_group": 0,
                    "matched_to_event_id": event.event_id,
                    "note": f"Matched control {rank} for {event.event_id}",
                }
            )

    treated_rows = treated.copy()
    treated_rows["matched_to_event_id"] = treated_rows["event_id"]
    matched_events = pd.concat([treated_rows, pd.DataFrame(matched_rows)], ignore_index=True, sort=False)
    if matched_events.empty:
        matched_events = treated_rows
    matched_events = matched_events.sort_values(
        ["market", "matched_to_event_id", "treatment_group", "inclusion"],
        ascending=[True, True, False, False],
    )
    return matched_events.reset_index(drop=True), pd.DataFrame(diagnostics)


_BALANCE_COVARIATES: tuple[str, ...] = (
    "mkt_cap_log",
    "pre_event_return",
    "pre_event_volatility",
)


def _balance_snapshot(
    prices: pd.DataFrame,
    market: str,
    ticker: str,
    reference_date: pd.Timestamp,
    lookback_days: int,
) -> dict[str, object] | None:
    snap = _compute_security_snapshot(
        prices=prices,
        market=market,
        ticker=ticker,
        reference_date=reference_date,
        lookback_days=lookback_days,
    )
    if snap is None:
        return None
    cap = snap.get("mkt_cap")
    cap_log = float(np.log(cap)) if cap is not None and not pd.isna(cap) and cap > 0 else float("nan")
    return {
        "mkt_cap_log": cap_log,
        "pre_event_return": float(snap.get("pre_event_return", float("nan"))),
        "pre_event_volatility": float(snap.get("pre_event_volatility", float("nan"))),
        "sector": snap.get("sector"),
    }


def compute_covariate_balance(
    matched_events: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    lookback_days: int = 20,
    reference_date_column: str = "announce_date",
    smd_threshold: float = 0.10,
) -> pd.DataFrame:
    """Stuart (2010) covariate balance table for a matched sample.

    Returns one row per (market, covariate) with treated/control means, pooled
    standard deviation, standardised mean difference (SMD), and a boolean
    ``balanced`` flag indicating |SMD| < ``smd_threshold``. ``treatment_group``
    is read from ``matched_events`` (1 = treated, 0 = control).
    """
    if matched_events.empty or "treatment_group" not in matched_events.columns:
        return pd.DataFrame(
            columns=[
                "market",
                "covariate",
                "treated_mean",
                "control_mean",
                "treated_std",
                "control_std",
                "pooled_std",
                "smd",
                "balanced",
                "n_treated",
                "n_control",
            ]
        )

    if "matched_to_event_id" in matched_events.columns:
        has_treated = matched_events.groupby("matched_to_event_id")["treatment_group"].transform(
            lambda values: (pd.to_numeric(values, errors="coerce") == 1).any()
        )
        has_control = matched_events.groupby("matched_to_event_id")["treatment_group"].transform(
            lambda values: (pd.to_numeric(values, errors="coerce") == 0).any()
        )
        matched_events = matched_events.loc[has_treated & has_control].copy()

    history_by_key = {
        (str(market), str(ticker)): group.sort_values("date").reset_index(drop=True)
        for (market, ticker), group in prices.groupby(["market", "ticker"], dropna=False)
    }
    snapshot_cache: dict[tuple[str, str, str, int], dict[str, object] | None] = {}

    def get_balance_snapshot(
        market: str,
        ticker: str,
        reference_date: pd.Timestamp,
    ) -> dict[str, object] | None:
        cache_key = (
            str(market),
            str(ticker),
            pd.Timestamp(reference_date).date().isoformat(),
            lookback_days,
        )
        if cache_key in snapshot_cache:
            return snapshot_cache[cache_key]
        history = history_by_key.get((str(market), str(ticker)))
        if history is None:
            snapshot_cache[cache_key] = None
            return None
        snap = _balance_snapshot(
            prices=history,
            market=str(market),
            ticker=str(ticker),
            reference_date=reference_date,
            lookback_days=lookback_days,
        )
        snapshot_cache[cache_key] = snap
        return snap

    rows_with_snap: list[dict[str, object]] = []
    for event in matched_events.itertuples(index=False):
        reference_date = getattr(event, reference_date_column, None)
        if reference_date is None or pd.isna(reference_date):
            continue
        snap = get_balance_snapshot(
            market=str(event.market),
            ticker=str(event.ticker),
            reference_date=pd.Timestamp(reference_date),
        )
        if snap is None:
            continue
        rows_with_snap.append(
            {
                "market": event.market,
                "treatment_group": int(getattr(event, "treatment_group", 1)),
                **snap,
            }
        )
    snap_frame = pd.DataFrame(rows_with_snap)
    if snap_frame.empty:
        return pd.DataFrame(
            columns=[
                "market",
                "covariate",
                "treated_mean",
                "control_mean",
                "treated_std",
                "control_std",
                "pooled_std",
                "smd",
                "balanced",
                "n_treated",
                "n_control",
            ]
        )

    rows: list[dict[str, object]] = []
    for market, sub in snap_frame.groupby("market"):
        treated = sub.loc[sub["treatment_group"] == 1]
        control = sub.loc[sub["treatment_group"] == 0]
        n_t = int(len(treated))
        n_c = int(len(control))
        for covariate in _BALANCE_COVARIATES:
            t_vals = treated[covariate].dropna()
            c_vals = control[covariate].dropna()
            t_mean = float(t_vals.mean()) if not t_vals.empty else float("nan")
            c_mean = float(c_vals.mean()) if not c_vals.empty else float("nan")
            t_std = float(t_vals.std(ddof=1)) if len(t_vals) > 1 else float("nan")
            c_std = float(c_vals.std(ddof=1)) if len(c_vals) > 1 else float("nan")
            if not np.isfinite(t_std) and not np.isfinite(c_std):
                pooled = float("nan")
            else:
                t_var = t_std**2 if np.isfinite(t_std) else 0.0
                c_var = c_std**2 if np.isfinite(c_std) else 0.0
                pooled = float(math.sqrt((t_var + c_var) / 2.0))
            if pooled and pooled > 0 and np.isfinite(t_mean) and np.isfinite(c_mean):
                smd = float((t_mean - c_mean) / pooled)
            else:
                smd = float("nan")
            balanced = bool(np.isfinite(smd) and abs(smd) < smd_threshold)
            rows.append(
                {
                    "market": market,
                    "covariate": covariate,
                    "treated_mean": t_mean,
                    "control_mean": c_mean,
                    "treated_std": t_std,
                    "control_std": c_std,
                    "pooled_std": pooled,
                    "smd": smd,
                    "balanced": balanced,
                    "n_treated": n_t,
                    "n_control": n_c,
                }
            )
    return pd.DataFrame(rows)
