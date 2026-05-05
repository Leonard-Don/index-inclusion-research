from __future__ import annotations

import warnings
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

ROBUSTNESS_COLUMNS: tuple[str, ...] = (
    "test",
    "status",
    "coefficient",
    "p_value",
    "n_obs",
    "detail",
)
EXPLANATION_COLUMNS: tuple[str, ...] = (
    "topic",
    "status",
    "headline",
    "detail",
    "metric",
    "value",
)

Status = Literal["pass", "warn", "missing"]


def _result_row(
    test: str,
    *,
    status: Status,
    coefficient: float = float("nan"),
    p_value: float = float("nan"),
    n_obs: int = 0,
    detail: str = "",
) -> dict[str, object]:
    return {
        "test": test,
        "status": status,
        "coefficient": coefficient,
        "p_value": p_value,
        "n_obs": int(n_obs),
        "detail": detail,
    }


def _normalise_cn_join_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["market"] = out["market"].astype(str).str.strip().str.upper()
    out = out.loc[out["market"] == "CN"].copy()
    out["ticker"] = out["ticker"].astype(str).str.strip().str.zfill(6)
    if "announce_date" in out.columns:
        out["announce_date"] = (
            pd.to_datetime(out["announce_date"], errors="coerce")
            .dt.normalize()
            .dt.strftime("%Y-%m-%d")
        )
    return out


def build_h6_weight_joined_frame(
    weight_change: pd.DataFrame | None,
    gap_event_level: pd.DataFrame | None,
) -> pd.DataFrame:
    """Join CN ``weight_proxy`` with event-level ``announce_jump`` evidence."""
    if weight_change is None or gap_event_level is None:
        return pd.DataFrame()
    required_weights = {"market", "ticker", "weight_proxy"}
    required_gap = {"market", "ticker", "announce_jump"}
    if weight_change.empty or gap_event_level.empty:
        return pd.DataFrame()
    if not required_weights.issubset(weight_change.columns):
        return pd.DataFrame()
    if not required_gap.issubset(gap_event_level.columns):
        return pd.DataFrame()

    weights = _normalise_cn_join_frame(weight_change)
    events = _normalise_cn_join_frame(gap_event_level)
    weights["weight_proxy"] = pd.to_numeric(weights["weight_proxy"], errors="coerce")
    events["announce_jump"] = pd.to_numeric(events["announce_jump"], errors="coerce")

    join_cols = ["ticker"]
    if "announce_date" in weights.columns and "announce_date" in events.columns:
        join_cols.append("announce_date")

    weight_cols = [*join_cols, "weight_proxy"]
    if "batch_id" in weights.columns and "batch_id" not in events.columns:
        weight_cols.append("batch_id")
    event_cols = [*join_cols, "announce_jump"]
    for col in ("event_id", "sector", "batch_id", "gap_length_days"):
        if col in events.columns and col not in event_cols:
            event_cols.append(col)

    weights_for_join = weights[weight_cols].dropna(subset=["weight_proxy"])
    weights_for_join = weights_for_join.drop_duplicates(subset=join_cols, keep="last")
    events_for_join = events[event_cols].dropna(subset=["announce_jump"])
    merged = events_for_join.merge(weights_for_join, on=join_cols, how="inner")
    merged = merged.dropna(subset=["weight_proxy", "announce_jump"])
    if "sector" in merged.columns:
        merged["sector"] = merged["sector"].astype(str).str.strip()
        merged.loc[
            merged["sector"].str.lower().isin({"", "nan", "none", "<na>", "unknown"}),
            "sector",
        ] = pd.NA
    return merged.reset_index(drop=True)


def _standardized_weight(joined: pd.DataFrame) -> pd.Series | None:
    weights = pd.to_numeric(joined["weight_proxy"], errors="coerce")
    std = float(weights.std(ddof=0))
    if not np.isfinite(std) or std <= 0:
        return None
    return (weights - float(weights.mean())) / std


def _fit_ols(
    joined: pd.DataFrame,
    *,
    with_sector_fe: bool = False,
) -> dict[str, object]:
    work = joined.copy()
    z = _standardized_weight(work)
    if z is None:
        return _result_row(
            "sector_fe_weight" if with_sector_fe else "ols_weight",
            status="warn",
            n_obs=len(work),
            detail="weight_proxy has no cross-event variation",
        )
    y = pd.to_numeric(work["announce_jump"], errors="coerce")
    x_parts = [pd.DataFrame({"weight_proxy_z": z.astype(float)})]
    if with_sector_fe:
        if "sector" not in work.columns:
            return _result_row(
                "sector_fe_weight",
                status="warn",
                n_obs=len(work),
                detail="sector column unavailable",
            )
        sector = work["sector"].dropna()
        if sector.nunique() < 2:
            return _result_row(
                "sector_fe_weight",
                status="warn",
                n_obs=len(work),
                detail="fewer than two known sectors",
            )
        dummies = pd.get_dummies(work["sector"], prefix="sector", drop_first=True)
        dummies = dummies.loc[:, dummies.sum() > 0].astype(float)
        x_parts.append(dummies)
    x = pd.concat(x_parts, axis=1)
    valid = pd.concat([y.rename("announce_jump"), x], axis=1).dropna()
    n_obs = int(len(valid))
    if n_obs < max(6, len(x.columns) + 3):
        return _result_row(
            "sector_fe_weight" if with_sector_fe else "ols_weight",
            status="warn",
            n_obs=n_obs,
            detail="not enough matched rows for regression degrees of freedom",
        )
    model = sm.OLS(valid["announce_jump"].astype(float), sm.add_constant(valid.drop(columns=["announce_jump"]).astype(float)))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        result = model.fit(cov_type="HC3")
    return _result_row(
        "sector_fe_weight" if with_sector_fe else "ols_weight",
        status="pass",
        coefficient=float(result.params.get("weight_proxy_z", float("nan"))),
        p_value=float(result.pvalues.get("weight_proxy_z", float("nan"))),
        n_obs=n_obs,
        detail=f"HC3 OLS on standardized weight_proxy; r2={float(result.rsquared):.3f}",
    )


def _fit_quantile(joined: pd.DataFrame) -> dict[str, object]:
    z = _standardized_weight(joined)
    if z is None:
        return _result_row(
            "median_quantreg_weight",
            status="warn",
            n_obs=len(joined),
            detail="weight_proxy has no cross-event variation",
        )
    valid = pd.DataFrame(
        {
            "announce_jump": pd.to_numeric(joined["announce_jump"], errors="coerce"),
            "weight_proxy_z": z,
        }
    ).dropna()
    n_obs = int(len(valid))
    if n_obs < 8:
        return _result_row(
            "median_quantreg_weight",
            status="warn",
            n_obs=n_obs,
            detail="not enough matched rows for median quantile regression",
        )
    try:
        model = sm.QuantReg(
            valid["announce_jump"].astype(float),
            sm.add_constant(valid[["weight_proxy_z"]].astype(float)),
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            result = model.fit(q=0.5, max_iter=1000)
    except Exception as exc:  # noqa: BLE001
        return _result_row(
            "median_quantreg_weight",
            status="warn",
            n_obs=n_obs,
            detail=f"quantile regression failed: {type(exc).__name__}",
        )
    return _result_row(
        "median_quantreg_weight",
        status="pass",
        coefficient=float(result.params.get("weight_proxy_z", float("nan"))),
        p_value=float(result.pvalues.get("weight_proxy_z", float("nan"))),
        n_obs=n_obs,
        detail="median QuantReg on standardized weight_proxy",
    )


def _permutation_quartile_spread(
    joined: pd.DataFrame,
    *,
    n_permutations: int = 5_000,
    seed: int = 42,
) -> dict[str, object]:
    """Two-sided permutation test on the heavy-vs-light quartile spread.

    Shuffles the assignment of announce_jump values across events many
    times to generate a null distribution for the (Q4 mean − Q1 mean)
    spread, then reports the two-sided p-value (fraction of permuted
    spreads whose absolute value ≥ observed). At n=67 this is the
    distribution-free complement to the OLS / quantile / sector-FE specs:
    if the OLS p=0.001 in the wrong direction is real signal, permutation
    should agree; if it's small-sample / outlier-driven noise, permutation
    will land near 0.5 and the H6 'evidence insufficient' verdict gains
    a non-parametric backbone.
    """
    n_obs = int(len(joined))
    if n_obs < 8 or joined["weight_proxy"].nunique(dropna=True) < 4:
        return _result_row(
            "permutation_quartile_spread",
            status="warn",
            n_obs=n_obs,
            detail="not enough weight variation for quartile permutation",
        )
    work = joined.copy()
    try:
        work["weight_quartile"] = pd.qcut(
            work["weight_proxy"],
            q=4,
            labels=["Q1", "Q2", "Q3", "Q4"],
            duplicates="drop",
        )
    except ValueError as exc:
        return _result_row(
            "permutation_quartile_spread",
            status="warn",
            n_obs=n_obs,
            detail=f"qcut failed: {exc}",
        )
    low_mask = work["weight_quartile"] == "Q1"
    high_mask = work["weight_quartile"] == "Q4"
    low = work.loc[low_mask, "announce_jump"].dropna().to_numpy()
    high = work.loc[high_mask, "announce_jump"].dropna().to_numpy()
    if low.size == 0 or high.size == 0:
        return _result_row(
            "permutation_quartile_spread",
            status="warn",
            n_obs=n_obs,
            detail="Q1 or Q4 is empty after qcut",
        )
    pooled = np.concatenate([low, high])
    n_low, n_high = low.size, high.size
    observed_spread = float(high.mean() - low.mean())

    rng = np.random.default_rng(seed)
    extreme_count = 0
    for _ in range(n_permutations):
        rng.shuffle(pooled)
        sample_low = pooled[:n_low]
        sample_high = pooled[n_low : n_low + n_high]
        spread = float(sample_high.mean() - sample_low.mean())
        if abs(spread) >= abs(observed_spread):
            extreme_count += 1
    # +1 in numerator and denominator avoids reporting p=0 from a finite
    # permutation budget; standard convention for Monte Carlo p-values.
    p_value = (extreme_count + 1) / (n_permutations + 1)
    return _result_row(
        "permutation_quartile_spread",
        status="pass",
        coefficient=observed_spread,
        p_value=float(p_value),
        n_obs=n_obs,
        detail=(
            f"two-sided permutation (B={n_permutations}); "
            f"|observed spread|={abs(observed_spread):.3%}; "
            f"extreme={extreme_count}/{n_permutations}"
        ),
    )


def _quartile_spread(joined: pd.DataFrame) -> dict[str, object]:
    n_obs = int(len(joined))
    if n_obs < 8 or joined["weight_proxy"].nunique(dropna=True) < 4:
        return _result_row(
            "quartile_spread",
            status="warn",
            n_obs=n_obs,
            detail="not enough weight variation for quartile split",
        )
    work = joined.copy()
    try:
        work["weight_quartile"] = pd.qcut(
            work["weight_proxy"],
            q=4,
            labels=["Q1", "Q2", "Q3", "Q4"],
            duplicates="drop",
        )
    except ValueError as exc:
        return _result_row(
            "quartile_spread",
            status="warn",
            n_obs=n_obs,
            detail=f"qcut failed: {exc}",
        )
    low = work.loc[work["weight_quartile"] == "Q1", "announce_jump"].dropna()
    high = work.loc[work["weight_quartile"] == "Q4", "announce_jump"].dropna()
    if low.empty or high.empty:
        return _result_row(
            "quartile_spread",
            status="warn",
            n_obs=n_obs,
            detail="Q1 or Q4 is empty after qcut",
        )
    p_value = float(stats.ttest_ind(high, low, equal_var=False, nan_policy="omit").pvalue)
    spread = float(high.mean() - low.mean())
    return _result_row(
        "quartile_spread",
        status="pass",
        coefficient=spread,
        p_value=p_value,
        n_obs=n_obs,
        detail=f"Q4 mean={float(high.mean()):+.3%}; Q1 mean={float(low.mean()):+.3%}",
    )


def compute_h6_weight_robustness(
    weight_change: pd.DataFrame | None,
    gap_event_level: pd.DataFrame | None,
) -> pd.DataFrame:
    joined = build_h6_weight_joined_frame(weight_change, gap_event_level)
    n_obs = int(len(joined))
    if joined.empty:
        return pd.DataFrame(
            [
                _result_row(
                    "coverage",
                    status="missing",
                    n_obs=0,
                    detail="no CN events matched on ticker/date with weight_proxy and announce_jump",
                )
            ],
            columns=ROBUSTNESS_COLUMNS,
        )

    rows: list[dict[str, object]] = [
        _result_row(
            "coverage",
            status="pass" if n_obs >= 20 else "warn",
            n_obs=n_obs,
            detail=(
                f"matched events={n_obs}; "
                f"unique tickers={joined['ticker'].nunique() if 'ticker' in joined else 'NA'}"
            ),
        ),
        _quartile_spread(joined),
        _fit_ols(joined, with_sector_fe=False),
        _fit_ols(joined, with_sector_fe=True),
        _fit_quantile(joined),
        _permutation_quartile_spread(joined),
    ]
    return pd.DataFrame(rows, columns=ROBUSTNESS_COLUMNS)


def export_h6_weight_robustness(
    frame: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_h6_weight_robustness.csv"
    frame.to_csv(out_path, index=False)
    return out_path


def _lookup_row(frame: pd.DataFrame, test: str) -> pd.Series | None:
    if frame.empty or "test" not in frame.columns:
        return None
    match = frame.loc[frame["test"].astype(str) == test]
    if match.empty:
        return None
    return match.iloc[0]


def _as_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return number


def _as_int(value: object) -> int:
    number = _as_float(value)
    if not np.isfinite(number):
        return 0
    return int(number)


def _format_number(value: object, *, percent: bool = False) -> str:
    number = _as_float(value)
    if not np.isfinite(number):
        return "NA"
    if percent:
        return f"{number:+.2%}"
    return f"{number:.4f}"


def _explanation_row(
    topic: str,
    *,
    status: Status,
    headline: str,
    detail: str,
    metric: str = "",
    value: object = "",
) -> dict[str, object]:
    return {
        "topic": topic,
        "status": status,
        "headline": headline,
        "detail": detail,
        "metric": metric,
        "value": value,
    }


def build_h6_weight_explanation(
    *,
    h6_verdict: pd.Series | dict[str, object] | None = None,
    robustness: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a plain-language diagnostic table for the H6 weight evidence."""
    table = robustness.copy() if robustness is not None else pd.DataFrame()
    rows: list[dict[str, object]] = []

    coverage = _lookup_row(table, "coverage")
    matched = _as_int(coverage.get("n_obs")) if coverage is not None else 0
    coverage_status: Status = "pass" if matched >= 20 else "warn" if matched else "missing"
    rows.append(
        _explanation_row(
            "sample_coverage",
            status=coverage_status,
            headline=f"{matched} 个 CN 事件同时匹配 weight_proxy 与 announce_jump",
            detail=(
                str(coverage.get("detail", ""))
                if coverage is not None
                else "未能从 cma_h6_weight_robustness.csv 读取 coverage 行。"
            ),
            metric="matched_events",
            value=matched,
        )
    )

    computed = table.loc[
        table.get("test", pd.Series(dtype=str)).astype(str).ne("coverage")
    ].copy() if not table.empty else pd.DataFrame()
    pass_rows = computed.loc[computed.get("status", pd.Series(dtype=str)).astype(str) == "pass"].copy()
    coefficients = (
        pd.to_numeric(pass_rows.get("coefficient", pd.Series(dtype=float)), errors="coerce")
        if not pass_rows.empty
        else pd.Series(dtype=float)
    )
    finite_coefficients = coefficients.dropna()
    negative = int((finite_coefficients < 0).sum())
    positive = int((finite_coefficients > 0).sum())
    neutral = int((finite_coefficients == 0).sum())
    if finite_coefficients.empty:
        direction_status: Status = "missing"
        direction_headline = "没有可解释的权重方向检验"
        direction_detail = "稳健性规格未产生有效系数，H6 仍只能停留在证据不足。"
    elif negative > positive:
        direction_status = "warn"
        direction_headline = "权重越高未显示更强公告日跳涨"
        direction_detail = (
            f"有效规格中负向 {negative} 个、正向 {positive} 个、零值 {neutral} 个；"
            "方向与“大权重调入带来更强 announce jump”的 H6 直觉不一致。"
        )
    elif positive > negative:
        direction_status = "pass"
        direction_headline = "多数权重规格方向为正"
        direction_detail = (
            f"有效规格中正向 {positive} 个、负向 {negative} 个、零值 {neutral} 个；"
            "仍需结合显著性与样本覆盖判断强度。"
        )
    else:
        direction_status = "warn"
        direction_headline = "权重方向没有形成一致信号"
        direction_detail = (
            f"有效规格中正向 {positive} 个、负向 {negative} 个、零值 {neutral} 个。"
        )
    rows.append(
        _explanation_row(
            "direction",
            status=direction_status,
            headline=direction_headline,
            detail=direction_detail,
            metric="negative_vs_positive",
            value=f"{negative}/{positive}",
        )
    )

    specs = {
        "quartile_spread": ("quartile", "Q4-Q1 公告日跳涨差"),
        "ols_weight": ("ols_hc3", "标准化权重 OLS-HC3"),
        "sector_fe_weight": ("sector_fe", "加入行业固定效应"),
        "median_quantreg_weight": ("median_quantreg", "中位数分位数回归"),
        "permutation_quartile_spread": (
            "permutation",
            "Q4-Q1 跳涨差 · permutation 检验",
        ),
    }
    for test, (topic, label) in specs.items():
        row = _lookup_row(table, test)
        if row is None:
            rows.append(
                _explanation_row(
                    topic,
                    status="missing",
                    headline=f"{label} 未生成",
                    detail="缺少对应稳健性规格。",
                    metric=test,
                )
            )
            continue
        status = str(row.get("status", "warn"))
        coefficient = row.get("coefficient")
        p_value = row.get("p_value")
        formatted_coef = _format_number(coefficient, percent=test == "quartile_spread")
        formatted_p = _format_number(p_value)
        spec_status: Status = "pass" if status == "pass" else "warn"
        if status == "pass":
            headline = f"{label}: 系数 {formatted_coef}, p={formatted_p}"
        else:
            headline = f"{label}: {status}"
        detail = str(row.get("detail", ""))
        if test == "sector_fe_weight" and _as_float(p_value) >= 0.99:
            detail = (
                f"{detail}；行业固定效应后的 p 接近 1，说明该规格更像高杠杆/吸收变异诊断，"
                "不应单独当作强证据。"
            )
        rows.append(
            _explanation_row(
                topic,
                status=spec_status,
                headline=headline,
                detail=detail,
                metric="coefficient",
                value=coefficient,
            )
        )

    verdict_label = ""
    confidence = ""
    evidence_summary = ""
    if h6_verdict is not None:
        getter = h6_verdict.get  # type: ignore[assignment]
        verdict_label = str(getter("verdict", "") or "")
        confidence = str(getter("confidence", "") or "")
        evidence_summary = str(getter("evidence_summary", "") or "")
    final_status: Status = "pass" if verdict_label in {"支持", "部分支持"} else "warn"
    if not verdict_label:
        final_status = "missing"
        verdict_label = "未生成"
    rows.append(
        _explanation_row(
            "final_read",
            status=final_status,
            headline=f"当前 H6 裁决: {verdict_label}",
            detail=evidence_summary or "等待 cma_hypothesis_verdicts.csv 中的 H6 行。",
            metric="confidence",
            value=confidence,
        )
    )

    return pd.DataFrame(rows, columns=EXPLANATION_COLUMNS)


def export_h6_weight_explanation(
    frame: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_h6_weight_explanation.csv"
    frame.to_csv(out_path, index=False)
    return out_path
