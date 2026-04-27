from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class StructuralHypothesis:
    hid: str
    name_cn: str
    mechanism: str
    implications: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    verdict_logic: str
    track: str = ""  # one of {"price_pressure", "demand_curve", "identification"}


TRACK_LABELS: dict[str, str] = {
    "price_pressure": "短期价格压力",
    "demand_curve": "需求曲线与长期保留",
    "identification": "制度识别与中国市场",
}


HYPOTHESES: tuple[StructuralHypothesis, ...] = (
    StructuralHypothesis(
        hid="H1",
        name_cn="信息泄露与预运行",
        mechanism="如果 CN 公告前信息泄露比 US 严重，则公告前漂移更大",
        implications=("CN pre_announce_runup 显著高于 US",),
        evidence_refs=("M1:cma_ar_path.csv", "M2:cma_gap_summary.csv"),
        verdict_logic="若 CN pre_announce_runup > US 且 t 显著 → 支持 H1",
        track="identification",
    ),
    StructuralHypothesis(
        hid="H2",
        name_cn="被动基金 AUM 差异",
        mechanism="美股被动规模大、套利充分 → 生效日效应被抹平；A 股被动规模小 → 生效日仍有冲击",
        implications=("US effective CAR 在 2005→2020 随 AUM 增长衰减",),
        evidence_refs=("M5:cma_time_series_rolling.csv",),
        verdict_logic="若 US effective rolling CAR 单调下降、A 股 effective 上升 → 支持 H2",
        track="demand_curve",
    ),
    StructuralHypothesis(
        hid="H3",
        name_cn="散户 vs 机构结构",
        mechanism="A 股散户比重大 → 生效日量能更集中；美股机构主导 → 生效日量能被提前吸收",
        implications=("CN effective volume_change 显著为正且高于 US",),
        evidence_refs=("M3:cma_mechanism_panel.csv",),
        verdict_logic="若 CN effective × volume_change 系数显著 > 0 且 US 对应系数显著 < 0 → 支持 H3",
        track="price_pressure",
    ),
    StructuralHypothesis(
        hid="H4",
        name_cn="卖空约束",
        mechanism="A 股缺少做空通道 → 套利者无法在公告到生效期内压平价差；美股可套利 → 价差被公告日吃光",
        implications=("CN gap_drift 显著为正、US 接近 0",),
        evidence_refs=("M2:cma_gap_summary.csv",),
        verdict_logic="若 CN gap_drift t > US gap_drift t 显著 → 支持 H4",
        track="identification",
    ),
    StructuralHypothesis(
        hid="H5",
        name_cn="涨跌停限制",
        mechanism="A 股公告日 ±10% 涨停截断 → 需求溢出到生效日",
        implications=("CN 公告日 price_limit_hit_share 显著 > 0 且与 effective_jump 正相关",),
        evidence_refs=("M3:cma_mechanism_panel.csv",),
        verdict_logic="若 CN announce × price_limit_hit_share > 0 且 effective × price_limit_hit_share > 0 → 支持 H5",
        track="identification",
    ),
    StructuralHypothesis(
        hid="H6",
        name_cn="指数权重可预测性",
        mechanism="CN 规则下权重更难预判 → 生效日才重新定价；美股权重可预测 → 信息公告日已定价",
        implications=("CN effective_jump 与 weight_change 正相关（若有权重数据）",),
        evidence_refs=("M4:cma_heterogeneity_size.csv",),
        verdict_logic="M4 size 异质性中，CN 小市值更易受权重预判差影响",
        track="demand_curve",
    ),
    StructuralHypothesis(
        hid="H7",
        name_cn="行业结构差异",
        mechanism=(
            "行业内 inclusion 效应可能取决于成分股集中度、被动资金行业偏好和"
            "行业流动性。如果行业差异是 inclusion 效应的关键来源,asymmetry_index"
            "应在 sector 维度上有显著分化。"
        ),
        implications=(
            "US sector 维度上 asymmetry_index 跨行业 spread 显著大于零",
            "CN 因 sector 字段未填充,暂时无法对比",
        ),
        evidence_refs=("M4:cma_heterogeneity_sector.csv",),
        verdict_logic=(
            "若 US 中至少 2 个 n_events>=10 的 sector,asymmetry_index "
            "spread (max-min) > 1.5 → 部分支持 / 支持。CN sector 数据缺失则 CN 部分挂 待补"
        ),
        track="identification",
    ),
)


def export_hypothesis_map(*, output_dir: Path) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "hid": h.hid,
            "name_cn": h.name_cn,
            "mechanism": h.mechanism,
            "implications": " | ".join(h.implications),
            "evidence_refs": " | ".join(h.evidence_refs),
            "verdict_logic": h.verdict_logic,
            "track": h.track,
        }
        for h in HYPOTHESES
    ]
    out_path = output_dir / "cma_hypothesis_map.csv"
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return out_path


def compute_track_verdict_summary(verdicts: pd.DataFrame) -> pd.DataFrame:
    """Aggregate verdict counts per research track.

    Joins verdicts on hypothesis hid → track and emits one row per track
    with counts of (支持 / 部分支持 / 证据不足 / 待补数据) plus the
    associated hypothesis IDs in that track.
    """
    if verdicts is None or verdicts.empty:
        return pd.DataFrame(
            columns=[
                "track", "track_label", "hypotheses",
                "支持", "部分支持", "证据不足", "待补数据", "total",
            ]
        )
    track_by_hid = {h.hid: h.track for h in HYPOTHESES if h.track}
    if not track_by_hid:
        return pd.DataFrame()
    work = verdicts.copy()
    work["track"] = work["hid"].map(track_by_hid).fillna("")
    work = work.loc[work["track"] != ""]
    rows: list[dict[str, object]] = []
    for track, group in work.groupby("track", dropna=False):
        counts = {label: int((group["verdict"] == label).sum())
                  for label in ("支持", "部分支持", "证据不足", "待补数据")}
        rows.append(
            {
                "track": track,
                "track_label": TRACK_LABELS.get(str(track), str(track)),
                "hypotheses": ", ".join(group["hid"].tolist()),
                **counts,
                "total": int(len(group)),
            }
        )
    return pd.DataFrame(rows)


def export_track_verdict_summary(
    verdicts: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_track_verdict_summary.csv"
    compute_track_verdict_summary(verdicts).to_csv(out_path, index=False)
    return out_path
