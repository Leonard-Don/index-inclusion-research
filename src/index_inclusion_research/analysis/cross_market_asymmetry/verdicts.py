from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from .hypotheses import HYPOTHESES, StructuralHypothesis

SIGNIFICANCE_LEVEL = 0.10


def _row(
    frame: pd.DataFrame,
    *,
    market: str,
    metric: str | None = None,
    event_phase: str | None = None,
    outcome: str | None = None,
    spec: str | None = None,
) -> pd.Series | None:
    if frame.empty:
        return None
    sub = frame.copy()
    filters = {
        "market": market,
        "metric": metric,
        "event_phase": event_phase,
        "outcome": outcome,
        "spec": spec,
    }
    for column, value in filters.items():
        if value is None:
            continue
        if column not in sub.columns:
            return None
        sub = sub.loc[sub[column] == value]
    if sub.empty:
        return None
    return sub.iloc[0]


def _num(row: pd.Series | None, column: str) -> float | None:
    if row is None or column not in row:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.2%}"


def _fmt_num(value: float | None, digits: int = 2) -> str:
    return "NA" if value is None else f"{value:.{digits}f}"


def _sig(row: pd.Series | None) -> bool:
    p_value = _num(row, "p_value")
    return p_value is not None and p_value < SIGNIFICANCE_LEVEL


def _make_verdict(
    hypothesis: StructuralHypothesis,
    *,
    verdict: str,
    confidence: str,
    evidence_summary: str,
    metric_snapshot: str,
    next_step: str,
    key_label: str = "",
    key_value: float | None = None,
    n_obs: int | None = None,
) -> dict[str, object]:
    """Build one verdict row.

    The ``key_label`` / ``key_value`` / ``n_obs`` triple is the
    machine-readable headline (e.g. ``"bootstrap p" 0.640 n=436``).
    Downstream consumers — dashboard verdict cards, research_summary
    markdown table — can render the headline number prominently next
    to the human-readable ``metric_snapshot``.
    """
    return {
        "hid": hypothesis.hid,
        "name_cn": hypothesis.name_cn,
        "verdict": verdict,
        "confidence": confidence,
        "evidence_summary": evidence_summary,
        "metric_snapshot": metric_snapshot,
        "next_step": next_step,
        "evidence_refs": " | ".join(hypothesis.evidence_refs),
        "key_label": key_label,
        "key_value": float(key_value) if key_value is not None else float("nan"),
        "n_obs": int(n_obs) if n_obs is not None else 0,
        "paper_ids": " | ".join(hypothesis.paper_ids),
        "paper_count": len(hypothesis.paper_ids),
        "track": hypothesis.track,
    }


def _pending(hypothesis: StructuralHypothesis, reason: str, next_step: str) -> dict[str, object]:
    return _make_verdict(
        hypothesis,
        verdict="待补数据",
        confidence="低",
        evidence_summary=reason,
        metric_snapshot="NA",
        next_step=next_step,
    )


def _h1(
    hypothesis: StructuralHypothesis,
    gap_summary: pd.DataFrame,
    *,
    bootstrap: Mapping[str, object] | None = None,
) -> dict[str, object]:
    cn = _row(gap_summary, market="CN", metric="pre_announce_runup")
    us = _row(gap_summary, market="US", metric="pre_announce_runup")
    cn_mean = _num(cn, "mean")
    us_mean = _num(us, "mean")
    if cn_mean is None or us_mean is None:
        return _pending(
            hypothesis,
            "缺少 CN/US 公告前漂移汇总，暂时无法比较信息预运行。",
            "重跑 CMA M2 空窗期分析，生成 cma_gap_summary.csv。",
        )
    boot_p = _bootstrap_p(bootstrap)
    if boot_p is not None:
        return _h1_from_bootstrap(
            hypothesis, cn_mean=cn_mean, us_mean=us_mean, bootstrap=bootstrap, boot_p=boot_p
        )
    directional = cn_mean > us_mean
    if directional and _sig(cn):
        verdict = "部分支持"
        confidence = "中"
        summary = "CN 公告前漂移高于 US，且 CN 自身显著；但当前还没有跨市场差异检验。"
    elif directional:
        verdict = "证据不足"
        confidence = "低"
        summary = "CN 公告前漂移方向高于 US，但显著性不足，不能直接归因为信息泄露。"
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = "CN 公告前漂移没有高于 US，当前口径不支持更强的信息预运行解释。"
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=(
            f"CN pre-runup={_fmt_pct(cn_mean)}, t={_fmt_num(_num(cn, 't'))}; "
            f"US pre-runup={_fmt_pct(us_mean)}, t={_fmt_num(_num(us, 't'))}"
        ),
        next_step="加入 CN-US pre-runup 差异的 bootstrap 或回归检验，避免只比较两个均值。",
    )


def _bootstrap_p(bootstrap: Mapping[str, object] | None) -> float | None:
    if not bootstrap:
        return None
    raw = bootstrap.get("boot_p_value")
    if raw is None:
        return None
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if value != value:  # NaN guard
        return None
    return value


def _h1_from_bootstrap(
    hypothesis: StructuralHypothesis,
    *,
    cn_mean: float,
    us_mean: float,
    bootstrap: Mapping[str, object],
    boot_p: float,
) -> dict[str, object]:
    diff_raw = bootstrap.get("diff_mean", cn_mean - us_mean)
    diff = float(diff_raw) if diff_raw is not None else cn_mean - us_mean
    ci_low = float(bootstrap.get("boot_ci_low", float("nan")))  # type: ignore[arg-type]
    ci_high = float(bootstrap.get("boot_ci_high", float("nan")))  # type: ignore[arg-type]
    if diff > 0 and boot_p < 0.05:
        verdict = "支持"
        confidence = "高"
        summary = (
            f"CN-US pre-runup 差异 {_fmt_pct(diff)} 在 bootstrap 下显著 (p={boot_p:.3f}, "
            f"CI95=[{_fmt_pct(ci_low)}, {_fmt_pct(ci_high)}])，支持 H1 信息预运行。"
        )
    elif diff > 0 and boot_p < 0.10:
        verdict = "部分支持"
        confidence = "中"
        summary = (
            f"CN-US pre-runup 差异 {_fmt_pct(diff)} 边际显著 (p={boot_p:.3f}, "
            f"CI95=[{_fmt_pct(ci_low)}, {_fmt_pct(ci_high)}])，弱支持 H1。"
        )
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = (
            f"CN-US pre-runup 差异 {_fmt_pct(diff)} 在 bootstrap 下不显著 (p={boot_p:.3f}, "
            f"CI95=[{_fmt_pct(ci_low)}, {_fmt_pct(ci_high)}])，方向偏 CN 但跨市场差异口径无法归因为信息泄露。"
        )
    metric_snapshot = (
        f"CN pre-runup={_fmt_pct(cn_mean)}; US pre-runup={_fmt_pct(us_mean)}; "
        f"diff={_fmt_pct(diff)}, bootstrap p={boot_p:.3f}, "
        f"CI95=[{_fmt_pct(ci_low)}, {_fmt_pct(ci_high)}]"
    )
    n_total = int(bootstrap.get("n_cn", 0) or 0) + int(bootstrap.get("n_us", 0) or 0)  # type: ignore[arg-type]
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=metric_snapshot,
        next_step="如需更强结论,可叠加事件级回归并控制 gap_length_days / sector / size 等协变量。",
        key_label="bootstrap p",
        key_value=boot_p,
        n_obs=n_total,
    )


def _h2(
    hypothesis: StructuralHypothesis,
    time_series_rolling: pd.DataFrame,
    *,
    aum_frame: pd.DataFrame | None,
) -> dict[str, object]:
    if aum_frame is None or aum_frame.empty:
        return _pending(
            hypothesis,
            "当前有 rolling CAR，但缺少被动 AUM 年度数据，不能检验 AUM 上升与生效日效应衰减的关系。",
            "补充 data/raw/passive_aum.csv（market, year, aum_trillion）后重跑 CMA。",
        )
    required = {"market", "year", "aum_trillion"}
    if not required.issubset(aum_frame.columns):
        return _pending(
            hypothesis,
            "AUM 数据列不完整，无法和 rolling CAR 按年份对齐。",
            "确保 AUM 文件包含 market, year, aum_trillion 三列。",
        )
    rolling_required = {"market", "event_phase", "window_end_year", "car_mean"}
    if not rolling_required.issubset(time_series_rolling.columns):
        return _pending(
            hypothesis,
            "rolling CAR 输出列不完整，无法和 AUM 年度序列对齐。",
            "重跑 CMA M5 时序模块，生成 cma_time_series_rolling.csv。",
        )
    us_roll = time_series_rolling.loc[
        (time_series_rolling["market"] == "US")
        & (time_series_rolling["event_phase"] == "effective")
    ].sort_values("window_end_year")
    us_aum = aum_frame.loc[aum_frame["market"] == "US"].sort_values("year")
    if len(us_roll) < 2 or len(us_aum) < 2:
        return _pending(
            hypothesis,
            "US rolling CAR 或 US AUM 年度序列不足，无法判断趋势。",
            "至少准备两个年份以上的 US 被动 AUM，并保留 rolling CAR 输出。",
        )
    aum_up = float(us_aum["aum_trillion"].iloc[-1]) > float(us_aum["aum_trillion"].iloc[0])
    effect_down = float(us_roll["car_mean"].iloc[-1]) < float(us_roll["car_mean"].iloc[0])
    if aum_up and effect_down:
        verdict = "部分支持"
        confidence = "中"
        summary = "US 被动 AUM 上升且 US 生效日 rolling CAR 走弱，方向符合 H2。"
    else:
        verdict = "证据不足"
        confidence = "低"
        summary = "AUM 与 US 生效日 rolling CAR 的方向关系不稳定，当前不支持 H2。"
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=(
            f"US AUM {float(us_aum['aum_trillion'].iloc[0]):.2f}→{float(us_aum['aum_trillion'].iloc[-1]):.2f}; "
            f"US effective rolling CAR {_fmt_pct(float(us_roll['car_mean'].iloc[0]))}→{_fmt_pct(float(us_roll['car_mean'].iloc[-1]))}"
        ),
        next_step="用年度面板回归替代趋势首尾比较，并加入 CN AUM 作为对照。",
    )


def _h3(
    hypothesis: StructuralHypothesis,
    mechanism_panel: pd.DataFrame,
    *,
    channel_concentration: pd.DataFrame | None = None,
) -> dict[str, object]:
    if channel_concentration is not None and not channel_concentration.empty:
        return _h3_from_channel_table(hypothesis, channel_concentration)
    cn_eff_turnover = _row(
        mechanism_panel,
        market="CN",
        event_phase="effective",
        outcome="turnover_change",
        spec="no_fe",
    )
    us_ann_turnover = _row(
        mechanism_panel,
        market="US",
        event_phase="announce",
        outcome="turnover_change",
        spec="no_fe",
    )
    us_eff_turnover = _row(
        mechanism_panel,
        market="US",
        event_phase="effective",
        outcome="turnover_change",
        spec="no_fe",
    )
    cn_eff_volume = _row(
        mechanism_panel,
        market="CN",
        event_phase="effective",
        outcome="volume_change",
        spec="no_fe",
    )
    required = [cn_eff_turnover, us_ann_turnover, us_eff_turnover, cn_eff_volume]
    if any(item is None for item in required):
        return _pending(
            hypothesis,
            "缺少 turnover/volume 机制回归四象限结果，暂时不能裁决量能集中机制。",
            "重跑 CMA M3 机制面板，生成完整 cma_mechanism_panel.csv。",
        )
    cn_turnover_ok = (_num(cn_eff_turnover, "coef") or 0.0) > 0 and _sig(cn_eff_turnover)
    us_concentrated_announce = (
        (_num(us_ann_turnover, "coef") or 0.0)
        > (_num(us_eff_turnover, "coef") or 0.0)
        and _sig(us_ann_turnover)
    )
    cn_volume_positive = (_num(cn_eff_volume, "coef") or 0.0) > 0
    if cn_turnover_ok and us_concentrated_announce and cn_volume_positive:
        verdict = "支持"
        confidence = "中"
        summary = "CN 生效日换手显著为正，US 换手更集中在公告日，量能集中机制成立。"
    elif cn_turnover_ok or us_concentrated_announce:
        verdict = "部分支持"
        confidence = "中"
        summary = "至少一个量能通道方向成立，但 volume 或四象限对照仍不完整。"
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = "当前机制面板没有显示清晰的 CN 生效日量能集中。"
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=(
            f"CN effective turnover coef={_fmt_num(_num(cn_eff_turnover, 'coef'), 4)}, "
            f"t={_fmt_num(_num(cn_eff_turnover, 't'))}; "
            f"US announce/effective turnover coef="
            f"{_fmt_num(_num(us_ann_turnover, 'coef'), 4)}/{_fmt_num(_num(us_eff_turnover, 'coef'), 4)}"
        ),
        next_step="把 turnover 与 volume 做成同一张四象限差异检验表，避免单通道过度解释。",
    )


def _h3_from_channel_table(
    hypothesis: StructuralHypothesis,
    channel: pd.DataFrame,
) -> dict[str, object]:
    total = len(channel)
    both_sig_count = int(channel["both_channels_sig"].astype(bool).sum())
    expected_quadrants = {("US", "announce"), ("CN", "effective")}
    expected_hits: list[str] = []
    for _, row in channel.iterrows():
        if not bool(row["both_channels_sig"]):
            continue
        if (row["market"], row["event_phase"]) in expected_quadrants:
            expected_hits.append(f"{row['market']} {row['event_phase']}")
    expected_hit_count = len(expected_hits)
    if expected_hit_count == 2:
        verdict = "支持"
        confidence = "高"
        summary = (
            f"US announce 与 CN effective 两条预期量能集中四象限均双通道显著 "
            f"(turnover + volume p<0.10), 共 {both_sig_count}/{total} 个象限通过双通道判据。"
        )
    elif expected_hit_count == 1:
        verdict = "部分支持"
        confidence = "中"
        summary = (
            f"仅 {expected_hits[0]} 一个预期象限双通道显著, 共 {both_sig_count}/{total} 个象限通过双通道判据,"
            "另一条预期象限只有单通道显著, 不能完全确认 H3。"
        )
    elif both_sig_count >= 1:
        verdict = "部分支持"
        confidence = "低"
        summary = (
            f"仅 {both_sig_count}/{total} 个象限双通道显著, 但都不在 US announce / CN effective 预期位置上,"
            "方向不完全符合 H3。"
        )
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = (
            f"四象限内没有任何象限同时通过 turnover + volume 显著性 (共 {both_sig_count}/{total}),"
            "单通道证据不足以支持 H3 量能集中机制。"
        )

    snapshot_parts: list[str] = []
    for _, row in channel.iterrows():
        flag = "✓" if bool(row["both_channels_sig"]) else (
            "T" if bool(row["turnover_sig"]) else "V" if bool(row["volume_sig"]) else "·"
        )
        snapshot_parts.append(
            f"{row['market']} {row['event_phase']}={flag}"
        )
    metric_snapshot = (
        f"channel concentration {both_sig_count}/{total} both-sig: "
        + ", ".join(snapshot_parts)
    )
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=metric_snapshot,
        next_step="可叠加 volatility 通道或 sector heterogeneity 进一步剖分量能集中来源。",
        key_label="双通道命中率",
        key_value=(both_sig_count / total) if total else float("nan"),
        n_obs=total,
    )


def _h4(
    hypothesis: StructuralHypothesis,
    gap_summary: pd.DataFrame,
    *,
    regression: Mapping[str, object] | None = None,
) -> dict[str, object]:
    cn = _row(gap_summary, market="CN", metric="gap_drift")
    us = _row(gap_summary, market="US", metric="gap_drift")
    cn_mean = _num(cn, "mean")
    us_mean = _num(us, "mean")
    if cn_mean is None or us_mean is None:
        return _pending(
            hypothesis,
            "缺少 CN/US gap_drift 汇总，无法判断公告到生效之间是否被套利压平。",
            "重跑 CMA M2 空窗期分析，保留 gap_drift 指标。",
        )
    reg_p = _regression_p(regression)
    if reg_p is not None:
        return _h4_from_regression(
            hypothesis,
            cn_mean=cn_mean,
            us_mean=us_mean,
            regression=regression,
            reg_p=reg_p,
        )
    if cn_mean > 0 and us_mean <= 0 and _sig(cn):
        verdict = "支持"
        confidence = "中"
        summary = "CN 空窗期漂移显著为正且 US 接近或低于 0，符合套利约束解释。"
    elif cn_mean > 0 and us_mean <= 0:
        verdict = "部分支持"
        confidence = "低"
        summary = "CN/US 方向符合 H4，但 CN gap_drift 当前不显著。"
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = "gap_drift 方向没有形成 CN 正、US 零或负的稳定对照。"
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=(
            f"CN gap_drift={_fmt_pct(cn_mean)}, t={_fmt_num(_num(cn, 't'))}; "
            f"US gap_drift={_fmt_pct(us_mean)}, t={_fmt_num(_num(us, 't'))}"
        ),
        next_step="增加 event-level gap_drift 的跨市场差异回归，并控制 gap_length_days。",
    )


def _regression_p(regression: Mapping[str, object] | None) -> float | None:
    if not regression:
        return None
    raw = regression.get("cn_p_value")
    if raw is None:
        return None
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value


def _h4_from_regression(
    hypothesis: StructuralHypothesis,
    *,
    cn_mean: float,
    us_mean: float,
    regression: Mapping[str, object],
    reg_p: float,
) -> dict[str, object]:
    cn_coef = float(regression.get("cn_coef", 0.0))  # type: ignore[arg-type]
    n_obs = int(regression.get("n_obs", 0))  # type: ignore[arg-type]
    if cn_coef > 0 and reg_p < 0.05:
        verdict = "支持"
        confidence = "高"
        summary = (
            f"控制 gap_length_days 后 CN gap_drift 比 US 高 {_fmt_pct(cn_coef)} "
            f"(回归 p={reg_p:.3f})，跨市场差异稳健，符合 H4 套利约束解释。"
        )
    elif cn_coef > 0 and reg_p < 0.10:
        verdict = "部分支持"
        confidence = "中"
        summary = (
            f"控制 gap_length_days 后 CN gap_drift 比 US 高 {_fmt_pct(cn_coef)} "
            f"边际显著 (p={reg_p:.3f})，方向支持 H4 但稳健性仍需观察。"
        )
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = (
            f"控制 gap_length_days 后 CN-US gap_drift 差异 {_fmt_pct(cn_coef)} 不显著 "
            f"(p={reg_p:.3f})，跨市场差异口径无法支持 H4 套利约束解释。"
        )
    metric_snapshot = (
        f"CN gap_drift={_fmt_pct(cn_mean)}; US gap_drift={_fmt_pct(us_mean)}; "
        f"regression cn_coef={_fmt_pct(cn_coef)}, p={reg_p:.3f}, n={n_obs}"
    )
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=metric_snapshot,
        next_step="可继续叠加 sector / size 等协变量,或检验非线性 gap_length 项的稳健性。",
        key_label="regression p",
        key_value=reg_p,
        n_obs=n_obs,
    )


def _h5(
    hypothesis: StructuralHypothesis,
    mechanism_panel: pd.DataFrame,
    *,
    limit_regression: Mapping[str, object] | None = None,
) -> dict[str, object]:
    limit_p = _limit_p(limit_regression)
    if limit_p is not None:
        return _h5_from_regression(hypothesis, regression=limit_regression, limit_p=limit_p)
    cn_ann = _row(
        mechanism_panel,
        market="CN",
        event_phase="announce",
        outcome="price_limit_hit_share",
        spec="no_fe",
    )
    cn_eff = _row(
        mechanism_panel,
        market="CN",
        event_phase="effective",
        outcome="price_limit_hit_share",
        spec="no_fe",
    )
    if cn_ann is None or cn_eff is None:
        return _pending(
            hypothesis,
            "缺少 CN price_limit_hit_share 机制回归，不能评估涨跌停截断。",
            "重跑 CMA M3，并确认事件窗口内 ret 字段可用于计算涨跌停命中率。",
        )
    ann_positive = (_num(cn_ann, "coef") or 0.0) > 0
    eff_positive = (_num(cn_eff, "coef") or 0.0) > 0
    if ann_positive and eff_positive and _sig(cn_ann) and _sig(cn_eff):
        verdict = "支持"
        confidence = "中"
        summary = "CN 公告日与生效日 price_limit_hit_share 均显著为正，支持涨跌停截断机制。"
    elif ann_positive and eff_positive:
        verdict = "证据不足"
        confidence = "低"
        summary = "CN price_limit_hit_share 方向为正，但显著性不足，不能支撑 H5。"
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = "CN price_limit_hit_share 没有形成公告/生效双正向信号。"
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=(
            f"CN announce limit coef={_fmt_num(_num(cn_ann, 'coef'), 4)}, "
            f"t={_fmt_num(_num(cn_ann, 't'))}; "
            f"CN effective limit coef={_fmt_num(_num(cn_eff, 'coef'), 4)}, "
            f"t={_fmt_num(_num(cn_eff, 't'))}"
        ),
        next_step="把涨跌停命中率改成 event-level 暴露，并检验它对 effective_jump 的预测力。",
    )


def _limit_p(regression: Mapping[str, object] | None) -> float | None:
    if not regression:
        return None
    raw = regression.get("limit_p_value")
    if raw is None:
        return None
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value


def _h5_from_regression(
    hypothesis: StructuralHypothesis,
    *,
    regression: Mapping[str, object],
    limit_p: float,
) -> dict[str, object]:
    coef = float(regression.get("limit_coef", 0.0))  # type: ignore[arg-type]
    n_obs = int(regression.get("n_obs", 0))  # type: ignore[arg-type]
    r2 = float(regression.get("r_squared", 0.0))  # type: ignore[arg-type]
    if coef > 0 and limit_p < 0.05:
        verdict = "支持"
        confidence = "高"
        summary = (
            f"CN 事件级涨跌停命中率正向预测 announce-day CAR (limit_coef={_fmt_num(coef, 4)}, "
            f"p={limit_p:.3f}, R²={r2:.3f}, n={n_obs})，支持 H5 涨跌停截断机制。"
        )
    elif coef > 0 and limit_p < 0.10:
        verdict = "部分支持"
        confidence = "中"
        summary = (
            f"CN 事件级涨跌停命中率方向正、边际显著 (limit_coef={_fmt_num(coef, 4)}, "
            f"p={limit_p:.3f}, n={n_obs})，弱支持 H5。"
        )
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = (
            f"CN 事件级涨跌停命中率对 announce-day CAR 不具显著预测力 "
            f"(limit_coef={_fmt_num(coef, 4)}, p={limit_p:.3f}, n={n_obs})，H5 缺乏支持。"
        )
    metric_snapshot = (
        f"CN limit_coef={_fmt_num(coef, 4)}, p={limit_p:.3f}, R²={r2:.3f}, n={n_obs}"
    )
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=metric_snapshot,
        next_step="可加入 sector / size 协变量稳健性检验,或区分 limit_up vs limit_down 暴露。",
        key_label="limit_coef p",
        key_value=limit_p,
        n_obs=n_obs,
    )


def _h6(
    hypothesis: StructuralHypothesis,
    heterogeneity_size: pd.DataFrame,
    *,
    weight_change: pd.DataFrame | None = None,
    gap_event_level: pd.DataFrame | None = None,
) -> dict[str, object]:
    if (
        weight_change is not None
        and not weight_change.empty
        and gap_event_level is not None
        and not gap_event_level.empty
    ):
        return _h6_from_weight_change(
            hypothesis, weight_change=weight_change, gap_event_level=gap_event_level
        )
    required = {"market", "bucket", "asymmetry_index"}
    if heterogeneity_size.empty or not required.issubset(heterogeneity_size.columns):
        return _pending(
            hypothesis,
            "缺少市值异质性矩阵，暂时不能用小市值 proxy 观察权重可预测性。",
            "重跑 CMA M4 size 异质性，或补充真实 weight_change 字段。",
        )
    cn = heterogeneity_size.loc[heterogeneity_size["market"] == "CN"].copy()
    cn = cn.loc[cn["bucket"].notna()]
    small = cn.loc[cn["bucket"].isin(["Q1", "Q2"]), "asymmetry_index"].mean()
    large = cn.loc[cn["bucket"].isin(["Q4", "Q5"]), "asymmetry_index"].mean()
    if pd.isna(small) or pd.isna(large):
        return _pending(
            hypothesis,
            "市值分组不足，无法比较小市值与大市值 asymmetry_index。",
            "确认每个市场内至少能形成 Q1-Q5 分组。",
        )
    if float(small) > float(large) and float(small) > 1.0:
        verdict = "部分支持"
        confidence = "低"
        summary = "CN 小市值 cell 的不对称指数高于大市值，方向符合权重难预判的 proxy 解释。"
    else:
        verdict = "证据不足"
        confidence = "低"
        summary = "市值异质性没有显示小市值更强的不对称，当前 proxy 不支持 H6。"
    spread = float(small) - float(large)
    cn_n_events = int(cn["n_events"].sum()) if "n_events" in cn.columns else len(cn)
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=f"CN size Q1-Q2 avg={float(small):.2f}; Q4-Q5 avg={float(large):.2f}",
        next_step=(
            "补充真实或重建的 weight_change 后,用 weight_change 直接替代 size proxy。"
            "注意当前 hs300_rdd_candidates.reconstructed.csv 的 running_variable 仅是"
            "rank 映射(顶=600..尾=1),不能直接当真实流通市值用——需要重新抓取每批次"
            "公告日的成分股流通股本 × 收盘价才能算出可信的 weight_change。"
        ),
        key_label="Q1Q2−Q4Q5 spread",
        key_value=spread,
        n_obs=cn_n_events,
    )


def build_hypothesis_verdicts(
    *,
    gap_summary: pd.DataFrame,
    mechanism_panel: pd.DataFrame,
    heterogeneity_size: pd.DataFrame,
    time_series_rolling: pd.DataFrame,
    aum_frame: pd.DataFrame | None = None,
    pre_runup_bootstrap: Mapping[str, object] | None = None,
    gap_drift_regression: Mapping[str, object] | None = None,
    channel_concentration: pd.DataFrame | None = None,
    limit_regression: Mapping[str, object] | None = None,
    heterogeneity_sector: pd.DataFrame | None = None,
    weight_change: pd.DataFrame | None = None,
    gap_event_level: pd.DataFrame | None = None,
) -> pd.DataFrame:
    hypotheses = {h.hid: h for h in HYPOTHESES}
    sector_frame = (
        heterogeneity_sector
        if heterogeneity_sector is not None
        else pd.DataFrame()
    )
    rows = [
        _h1(hypotheses["H1"], gap_summary, bootstrap=pre_runup_bootstrap),
        _h2(hypotheses["H2"], time_series_rolling, aum_frame=aum_frame),
        _h3(hypotheses["H3"], mechanism_panel, channel_concentration=channel_concentration),
        _h4(hypotheses["H4"], gap_summary, regression=gap_drift_regression),
        _h5(hypotheses["H5"], mechanism_panel, limit_regression=limit_regression),
        _h6(
            hypotheses["H6"],
            heterogeneity_size,
            weight_change=weight_change,
            gap_event_level=gap_event_level,
        ),
        _h7(hypotheses["H7"], sector_frame),
    ]
    return pd.DataFrame(rows)


_H6_WEIGHT_QUANTILES = (0.0, 0.25, 0.50, 0.75, 1.0)


def _h6_from_weight_change(
    hypothesis: StructuralHypothesis,
    *,
    weight_change: pd.DataFrame,
    gap_event_level: pd.DataFrame,
) -> dict[str, object]:
    """H6 verdict path that uses real weight_change instead of size proxy.

    Joins the per-event ``weight_proxy`` (from compute_h6_weight_change)
    with the gap event panel on (market, ticker), splits by the median
    weight_proxy within CN events, and compares the mean
    ``announce_jump`` between the heavy-weight and light-weight groups.
    H6 predicts heavy-weight events should show LARGER announce_jump
    because their inclusion is more predictable and arbitrageurs front-run.
    """
    if "ticker" not in weight_change.columns or "ticker" not in gap_event_level.columns:
        return _pending(
            hypothesis,
            "weight_change / gap_event_level 缺少 ticker 列,无法 join。",
            "确认 hs300_weight_change.csv 与 cma_gap_event_level.csv 都按 ticker 标识。",
        )
    cn_weights = weight_change.loc[weight_change["market"] == "CN"].copy()
    cn_events = gap_event_level.loc[gap_event_level["market"] == "CN"].copy()
    if cn_weights.empty or cn_events.empty:
        return _pending(
            hypothesis,
            "CN 没有 weight_change 或 gap_event_level 行,无法用真实权重替代 size proxy。",
            "重跑 index-inclusion-compute-h6-weight-change 取 CN 流通市值后再做。",
        )
    merged = cn_events.merge(
        cn_weights[["ticker", "weight_proxy"]], on="ticker", how="inner"
    ).dropna(subset=["weight_proxy", "announce_jump"])
    if len(merged) < 6:
        return _make_verdict(
            hypothesis,
            verdict="证据不足",
            confidence="低",
            evidence_summary=(
                f"CN 实际匹配上 weight_proxy + announce_jump 的事件只有 {len(merged)} 条,"
                "样本太小,无法分组比较。"
            ),
            metric_snapshot=f"matched events={len(merged)}",
            next_step="扩大 weight_change 样本或允许跨批次 ticker 复用。",
            key_label="weight_proxy split",
            key_value=float("nan"),
            n_obs=int(len(merged)),
        )
    median_weight = float(merged["weight_proxy"].median())
    heavy = merged.loc[merged["weight_proxy"] > median_weight]
    light = merged.loc[merged["weight_proxy"] <= median_weight]
    heavy_mean = float(heavy["announce_jump"].mean())
    light_mean = float(light["announce_jump"].mean())
    spread = heavy_mean - light_mean
    if spread > 0 and heavy_mean > 0:
        verdict = "支持"
        confidence = "中"
        summary = (
            f"CN 重权重事件(weight>median={median_weight:.4f})announce_jump 均值"
            f" {heavy_mean:+.2%} 高于轻权重 {light_mean:+.2%},spread={spread:+.2%},"
            f" 方向符合 H6 权重可预判预言。"
        )
    elif spread > 0:
        verdict = "部分支持"
        confidence = "低"
        summary = (
            f"CN 重权重 announce_jump 高于轻权重 ({heavy_mean:+.2%} vs {light_mean:+.2%}),"
            "但绝对量级小,方向支持 H6 但不强。"
        )
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = (
            f"CN 重权重 announce_jump 并不明显高于轻权重 ({heavy_mean:+.2%} vs"
            f" {light_mean:+.2%},spread={spread:+.2%}),H6 不被支持。"
        )
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=(
            f"matched={len(merged)}, median weight={median_weight:.4f},"
            f" heavy announce_jump={heavy_mean:+.2%}, light={light_mean:+.2%},"
            f" spread={spread:+.2%}"
        ),
        next_step="可以再按 sector × weight 交互或多分位回归检验稳健性。",
        key_label="heavy−light spread",
        key_value=spread,
        n_obs=int(len(merged)),
    )


_H7_MIN_EVENTS_PER_SECTOR = 10


def _h7(
    hypothesis: StructuralHypothesis,
    heterogeneity_sector: pd.DataFrame,
) -> dict[str, object]:
    required = {"market", "bucket", "asymmetry_index", "n_events"}
    if heterogeneity_sector.empty or not required.issubset(heterogeneity_sector.columns):
        return _pending(
            hypothesis,
            "缺少 sector 异质性表,无法测试行业结构差异。",
            "重跑 CMA M4 sector 维度异质性。",
        )
    cn = heterogeneity_sector.loc[heterogeneity_sector["market"] == "CN"].copy()
    us = heterogeneity_sector.loc[heterogeneity_sector["market"] == "US"].copy()
    cn_buckets = sorted(b for b in cn["bucket"].dropna().unique() if str(b) != "Unknown")
    cn_status = "已分行业" if cn_buckets else "待补 sector"

    us_eligible = us.loc[
        (us["n_events"] >= _H7_MIN_EVENTS_PER_SECTOR)
        & (us["bucket"].astype(str) != "Unknown")
    ].copy()
    if us_eligible.empty or len(us_eligible) < 2:
        return _make_verdict(
            hypothesis,
            verdict="证据不足",
            confidence="低",
            evidence_summary=(
                f"US sector 桶内 n>={_H7_MIN_EVENTS_PER_SECTOR} 的行业不足 2 个,"
                f"无法计算跨行业 asymmetry spread。CN 状态:{cn_status}。"
            ),
            metric_snapshot=(
                f"US eligible sectors={len(us_eligible)}; CN sector 状态={cn_status}"
            ),
            next_step="将 sector_min_events 阈值降低,或确保 sector 字段被填充。",
            key_label="US sector spread",
            key_value=float("nan"),
            n_obs=int(us_eligible["n_events"].sum()) if not us_eligible.empty else 0,
        )
    us_max = float(us_eligible["asymmetry_index"].max())
    us_min = float(us_eligible["asymmetry_index"].min())
    spread = us_max - us_min
    sector_top_row = us_eligible.loc[us_eligible["asymmetry_index"].idxmax()]
    sector_bot_row = us_eligible.loc[us_eligible["asymmetry_index"].idxmin()]
    us_n = int(us_eligible["n_events"].sum())
    if spread > 1.5:
        verdict = "支持" if cn_buckets else "部分支持"
        confidence = "中" if cn_buckets else "低"
        summary = (
            f"US 行业间 asymmetry_index spread = {spread:.2f}({sector_top_row['bucket']}"
            f" {us_max:+.2f} vs {sector_bot_row['bucket']} {us_min:+.2f}),行业结构在 inclusion"
            f" 效应中显著起作用。CN 状态:{cn_status}。"
        )
    elif spread > 0.5:
        verdict = "部分支持"
        confidence = "低"
        summary = (
            f"US 行业 asymmetry_index spread = {spread:.2f},方向上行业差异存在但分化不强。"
            f" CN 状态:{cn_status}。"
        )
    else:
        verdict = "证据不足"
        confidence = "中"
        summary = (
            f"US 行业 asymmetry_index spread = {spread:.2f}(<0.5),行业维度的 inclusion 效应分化"
            f" 微弱。CN 状态:{cn_status}。"
        )
    metric_snapshot = (
        f"US eligible sectors={len(us_eligible)}, max={us_max:+.2f}({sector_top_row['bucket']}),"
        f" min={us_min:+.2f}({sector_bot_row['bucket']}), spread={spread:.2f}, n={us_n}"
    )
    if cn_buckets:
        metric_snapshot += f"; CN sectors={len(cn_buckets)}"
    else:
        metric_snapshot += "; CN sector 未填充"
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=metric_snapshot,
        next_step=(
            "若 CN sector 字段补齐,可做 CN-US 行业 × 阶段交互回归;否则限定为美股结论。"
        ),
        key_label="US sector spread",
        key_value=spread,
        n_obs=us_n,
    )


_LATEX_VERDICT_HEADER = r"""\begin{tabular}{llllrrl}
\toprule
HID & 名称 & 裁决 & 可信度 & 头条指标 & 值 & n \\
\midrule
"""

_LATEX_VERDICT_FOOTER = "\\bottomrule\n\\end{tabular}\n"


def _latex_escape(text: str) -> str:
    """Minimal LaTeX-safe escaping for the verdict snapshot."""
    if text is None:
        return ""
    return (
        str(text)
        .replace("\\", r"\textbackslash{}")
        .replace("&", r"\&")
        .replace("%", r"\%")
        .replace("_", r"\_")
        .replace("#", r"\#")
        .replace("$", r"\$")
        .replace("{", r"\{")
        .replace("}", r"\}")
    )


def export_hypothesis_verdicts_tex(
    verdicts: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    """Render the verdict frame as a booktabs LaTeX table for paper insertion.

    The output schema matches the markdown verdict block in research_summary
    so the paper can cite either format. The 关键证据 column is omitted from
    the LaTeX version to keep it printable on a single page.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_hypothesis_verdicts.tex"
    lines: list[str] = ["% auto-generated CMA hypothesis verdict table", _LATEX_VERDICT_HEADER]
    for _, row in verdicts.iterrows():
        label = str(row.get("key_label", "") or "")
        value_raw = row.get("key_value")
        try:
            value_f = float(value_raw) if value_raw is not None else float("nan")
        except (TypeError, ValueError):
            value_f = float("nan")
        value_text = f"{value_f:.3f}" if value_f == value_f else "—"
        n_obs_raw = row.get("n_obs")
        try:
            n_obs_int = int(n_obs_raw) if n_obs_raw is not None else 0
        except (TypeError, ValueError):
            n_obs_int = 0
        n_text = str(n_obs_int) if n_obs_int > 0 else "—"
        lines.append(
            f"{_latex_escape(row['hid'])} & "
            f"{_latex_escape(row['name_cn'])} & "
            f"{_latex_escape(row['verdict'])} & "
            f"{_latex_escape(row['confidence'])} & "
            f"{_latex_escape(label) if label else '—'} & "
            f"{value_text} & "
            f"{n_text} \\\\"
        )
    lines.append(_LATEX_VERDICT_FOOTER)
    out_path.write_text("\n".join(lines))
    return out_path


def _sample_summary_block(event_counts: pd.DataFrame | None) -> list[str]:
    """Render the sample-description preamble from event_counts_by_year.csv."""
    if event_counts is None or event_counts.empty:
        return []
    by_market = (
        event_counts.loc[event_counts["inclusion"] == 1]
        .groupby("market")["n_events"].sum()
    )
    if by_market.empty:
        return []
    cn_n = int(by_market.get("CN", 0))
    us_n = int(by_market.get("US", 0))
    years = (
        event_counts["announce_year"].astype("Int64").dropna()
        if "announce_year" in event_counts.columns
        else pd.Series(dtype="Int64")
    )
    year_lo = int(years.min()) if not years.empty else 0
    year_hi = int(years.max()) if not years.empty else 0
    lines = ["### 样本概述", ""]
    lines.append(
        f"真实样本覆盖 {year_lo}–{year_hi} 年间 CSI300(CN)与 S&P500(US)"
        f"的指数纳入事件: 共 **CN {cn_n} 起 / US {us_n} 起 inclusion 事件**(`inclusion=1`)。"
        " 每个事件采用 [-20, +20] 交易日的事件窗口,匹配对照组按 sector × 同期市值 quintile 抽取。"
    )
    lines.append("")
    return lines


def _methods_block() -> list[str]:
    return [
        "### 方法概述",
        "",
        "- **事件研究**: 公告日 (`announce`) 与生效日 (`effective`) 双窗口,",
        "  CAR 窗口包括 `[-1,+1]` / `[-3,+3]` / `[-5,+5]`,长窗口取 `[0,+20]` / `[0,+60]` / `[0,+120]`。",
        "- **匹配回归**: `treatment_group` 二值变量 + sector / 对数市值 / 事件前收益作为协变量,",
        "  使用 HC3 异方差稳健标准误。",
        "- **CMA 7 条机制假说**: 见下方逐项裁决,每条假说都有自动产出的 verdict + 头条指标。",
        "  完整 metric pipeline 见 `index-inclusion-cma`(`results/real_tables/cma_*.csv`)。",
        "- **HS300 RDD**: cutoff=300 的运行变量断点,公告日 `[-1,+1]` 主结果。",
        "",
    ]


def _limitations_block(verdicts: pd.DataFrame) -> list[str]:
    """List 待补数据 hypotheses + general caveats."""
    pending = verdicts.loc[verdicts["verdict"] == "待补数据"] if not verdicts.empty else pd.DataFrame()
    lines = ["### 限制与稳健性补强方向", ""]
    if not pending.empty:
        lines.append("**当前 verdicts 标注 待补数据 的项**(下游研究可补齐再重跑):")
        lines.append("")
        for _, row in pending.iterrows():
            lines.append(
                f"- {row['hid']} {row['name_cn']}: {str(row.get('next_step') or '').strip()}"
            )
        lines.append("")
    lines.extend(
        [
            "**通用稳健性补强**:",
            "",
            "- HS300 RDD 当前 `running_variable` 是公开重建排名(顶=600..尾=1),不等同于真实流通市值;",
            "  正式批次候选样本(L3)上线前,RDD 结论限定为公开重建口径,不可表述为中证官方历史候选排名。",
            "- 跨市场比较默认按事件汇总(announce vs effective × CN vs US 4 象限),后续可叠加事件级",
            "  bootstrap / permutation 检验,以及 sector × size 的交互检验,进一步压低单通道误判风险。",
            "- 长窗口(>120 日)的 retention ratio 在样本量收缩时会跳到 NA,",
            "  当前 demand_curve 主线主要靠 `[0,+60]` / `[0,+120]` 给出方向性结论。",
            "",
        ]
    )
    return lines


def render_paper_verdict_section(
    verdicts: pd.DataFrame,
    *,
    event_counts: pd.DataFrame | None = None,
) -> str:
    """Render verdicts as a paper-ready markdown section.

    Output starts with an aggregate summary line (X 支持 / Y 部分支持 /
    Z 证据不足 / W 待补数据), followed by one paragraph per hypothesis
    that combines name, verdict, key metric, and the human-readable
    evidence_summary. When ``event_counts`` is supplied, a sample
    summary preamble + a methods block sit before the verdict
    paragraphs and a limitations block sits after — yielding a draft
    that can be pasted into ``docs/paper_outline.md`` or fed into a
    LaTeX paper template.
    """
    if verdicts is None or verdicts.empty:
        return "## 假说裁决叙述\n\n(暂无 verdict 数据。先跑 `index-inclusion-cma`。)\n"

    counts: dict[str, int] = {}
    for _, row in verdicts.iterrows():
        verdict_label = str(row["verdict"])
        counts[verdict_label] = counts.get(verdict_label, 0) + 1

    aggregate_parts = []
    for label in ("支持", "部分支持", "证据不足", "待补数据"):
        if counts.get(label, 0) > 0:
            aggregate_parts.append(f"{counts[label]} 项{label}")

    lines: list[str] = []
    lines.append("## 假说裁决叙述")
    lines.append("")
    lines.append(
        f"基于跨市场不对称(CMA)pipeline 自动产出,7 条机制假说的当前裁决分布:"
        f" **{' / '.join(aggregate_parts) if aggregate_parts else '无可裁决项'}**。"
        " 详见 `results/real_tables/cma_hypothesis_verdicts.csv`。"
    )
    lines.append("")
    sample_lines = _sample_summary_block(event_counts)
    if sample_lines:
        lines.extend(sample_lines)
        lines.extend(_methods_block())
    for _, row in verdicts.iterrows():
        hid = str(row["hid"])
        name = str(row["name_cn"])
        verdict_label = str(row["verdict"])
        confidence = str(row["confidence"])
        evidence = str(row.get("evidence_summary", "")).strip()
        metric_snapshot = str(row.get("metric_snapshot", "")).strip()
        key_label = str(row.get("key_label", "") or "").strip()
        key_value = row.get("key_value")
        try:
            key_value_f = float(key_value) if key_value is not None else float("nan")
        except (TypeError, ValueError):
            key_value_f = float("nan")
        n_obs_raw = row.get("n_obs")
        try:
            n_obs_int = int(n_obs_raw) if n_obs_raw is not None else 0
        except (TypeError, ValueError):
            n_obs_int = 0

        head = f"### {hid} · {name} —— {verdict_label}(可信度:{confidence})"
        lines.append(head)
        if key_label and key_value_f == key_value_f:
            tail = f"**{key_label} = {key_value_f:.3f}**"
            if n_obs_int > 0:
                tail += f", n = {n_obs_int}"
            lines.append(tail)
        if evidence:
            lines.append(evidence)
        if metric_snapshot:
            lines.append(f"_细节_: {metric_snapshot}")
        lines.append("")
    if sample_lines:
        lines.extend(_limitations_block(verdicts))
    return "\n".join(lines).rstrip() + "\n"


def export_paper_verdict_section(
    verdicts: pd.DataFrame,
    *,
    output_path: Path,
    event_counts: pd.DataFrame | None = None,
) -> Path:
    """Write the paper-ready verdict section to ``output_path`` (a .md file)."""
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_paper_verdict_section(verdicts, event_counts=event_counts))
    return out_path


def export_hypothesis_verdicts(
    *,
    output_dir: Path,
    gap_summary: pd.DataFrame,
    mechanism_panel: pd.DataFrame,
    heterogeneity_size: pd.DataFrame,
    time_series_rolling: pd.DataFrame,
    aum_frame: pd.DataFrame | None = None,
    pre_runup_bootstrap: Mapping[str, object] | None = None,
    gap_drift_regression: Mapping[str, object] | None = None,
    channel_concentration: pd.DataFrame | None = None,
    limit_regression: Mapping[str, object] | None = None,
    heterogeneity_sector: pd.DataFrame | None = None,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_hypothesis_verdicts.csv"
    verdicts = build_hypothesis_verdicts(
        gap_summary=gap_summary,
        mechanism_panel=mechanism_panel,
        heterogeneity_size=heterogeneity_size,
        time_series_rolling=time_series_rolling,
        aum_frame=aum_frame,
        pre_runup_bootstrap=pre_runup_bootstrap,
        gap_drift_regression=gap_drift_regression,
        channel_concentration=channel_concentration,
        limit_regression=limit_regression,
        heterogeneity_sector=heterogeneity_sector,
    )
    verdicts.to_csv(out_path, index=False)
    return out_path
