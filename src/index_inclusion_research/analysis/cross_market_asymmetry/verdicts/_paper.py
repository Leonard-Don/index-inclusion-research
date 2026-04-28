"""Render hypothesis verdicts as a paper-ready markdown section.

This module handles the cross-market-asymmetry verdict narrative that
gets pasted into ``docs/paper_outline.md`` (or fed into a LaTeX
template). The pipeline is:

1. ``_sample_summary_block`` — preamble describing the event sample.
2. ``_methods_block`` — short methods recap.
3. one paragraph per verdict row (HID · name —— verdict).
4. ``_limitations_block`` — caveats + outstanding ``待补数据`` items.

Public API: ``render_paper_verdict_section`` and the thin
``export_paper_verdict_section`` writer; both are re-exported from
``verdicts/__init__.py``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


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
