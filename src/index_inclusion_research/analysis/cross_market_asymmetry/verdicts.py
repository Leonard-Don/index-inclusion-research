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
) -> dict[str, object]:
    return {
        "hid": hypothesis.hid,
        "name_cn": hypothesis.name_cn,
        "verdict": verdict,
        "confidence": confidence,
        "evidence_summary": evidence_summary,
        "metric_snapshot": metric_snapshot,
        "next_step": next_step,
        "evidence_refs": " | ".join(hypothesis.evidence_refs),
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
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=metric_snapshot,
        next_step="如需更强结论,可叠加事件级回归并控制 gap_length_days / sector / size 等协变量。",
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


def _h3(hypothesis: StructuralHypothesis, mechanism_panel: pd.DataFrame) -> dict[str, object]:
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
    )


def _h5(hypothesis: StructuralHypothesis, mechanism_panel: pd.DataFrame) -> dict[str, object]:
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


def _h6(hypothesis: StructuralHypothesis, heterogeneity_size: pd.DataFrame) -> dict[str, object]:
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
    return _make_verdict(
        hypothesis,
        verdict=verdict,
        confidence=confidence,
        evidence_summary=summary,
        metric_snapshot=f"CN size Q1-Q2 avg={float(small):.2f}; Q4-Q5 avg={float(large):.2f}",
        next_step="补充真实或重建的 weight_change 后，用 weight_change 直接替代 size proxy。",
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
) -> pd.DataFrame:
    hypotheses = {h.hid: h for h in HYPOTHESES}
    rows = [
        _h1(hypotheses["H1"], gap_summary, bootstrap=pre_runup_bootstrap),
        _h2(hypotheses["H2"], time_series_rolling, aum_frame=aum_frame),
        _h3(hypotheses["H3"], mechanism_panel),
        _h4(hypotheses["H4"], gap_summary, regression=gap_drift_regression),
        _h5(hypotheses["H5"], mechanism_panel),
        _h6(hypotheses["H6"], heterogeneity_size),
    ]
    return pd.DataFrame(rows)


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
    )
    verdicts.to_csv(out_path, index=False)
    return out_path
