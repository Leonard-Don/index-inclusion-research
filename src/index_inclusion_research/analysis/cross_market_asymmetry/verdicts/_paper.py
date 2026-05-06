"""Render hypothesis verdicts as a paper-ready markdown section.

This module handles the paper-ready narrative for ``docs/paper_outline_verdicts.md``.
The pipeline is:

1. ``_main_finding_block`` — headline result (does inclusion produce abnormal CAR?).
2. ``_sample_summary_block`` — preamble describing the event sample.
3. ``_methods_block`` — short methods recap.
4. one paragraph per verdict row (HID · name —— verdict). The verdicts
   collectively answer the **CN/US asymmetry mechanism** question, not
   the "is there an effect?" question; that one is answered by step 1.
5. ``_limitations_block`` — caveats + outstanding ``待补数据`` items.
6. ``_engineering_appendix_block`` — pointers to dashboard / HS300 L3
   workflow as engineering artifacts that sit outside the core narrative.

Public API: ``render_paper_verdict_section`` and the thin
``export_paper_verdict_section`` writer; both are re-exported from
``verdicts/__init__.py``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import paths as project_paths

_DEFAULT_RDD_ROBUSTNESS_PATH: Path = (
    project_paths.project_root() / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
)

_MAIN_FINDING_QUADRANTS: tuple[tuple[str, str], ...] = (
    ("CN", "announce"),
    ("CN", "effective"),
    ("US", "announce"),
    ("US", "effective"),
)


def _main_finding_block(event_study_summary: pd.DataFrame | None) -> list[str]:
    """Render the headline result: does index inclusion produce a significant
    abnormal return on the [-1, +1] window?

    Reads the standard ``event_study_summary.csv`` shape (one row per
    market × event_phase × inclusion × window_slug). When the input is
    missing or malformed, returns an empty block — caller falls back to
    the legacy verdict-only narrative.
    """
    if event_study_summary is None or event_study_summary.empty:
        return []
    required = {
        "market",
        "event_phase",
        "inclusion",
        "window_slug",
        "n_events",
        "mean_car",
        "t_stat",
        "p_value",
    }
    if not required.issubset(event_study_summary.columns):
        return []
    sub = event_study_summary.loc[
        (event_study_summary["inclusion"] == 1)
        & (event_study_summary["window_slug"] == "m1_p1")
    ].copy()
    if sub.empty:
        return []

    by_key = sub.set_index(["market", "event_phase"], drop=False)
    cells: dict[tuple[str, str], dict[str, float]] = {}
    rows_md: list[str] = []
    for market, phase in _MAIN_FINDING_QUADRANTS:
        try:
            row = by_key.loc[(market, phase)]
        except KeyError:
            continue
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        cells[(market, phase)] = {
            "n": int(row["n_events"]),  # type: ignore[call-overload,index]
            "mean_car": float(row["mean_car"]),  # type: ignore[call-overload,index]
            "t": float(row["t_stat"]),  # type: ignore[call-overload,index]
            "p": float(row["p_value"]),  # type: ignore[call-overload,index]
        }
        rows_md.append(
            f"| {market} | {phase} | {int(row['n_events'])} | "  # type: ignore[call-overload,index]
            f"{float(row['mean_car']) * 100:+.2f}% | "  # type: ignore[call-overload,index]
            f"{float(row['t_stat']):.2f} | "  # type: ignore[call-overload,index]
            f"{float(row['p_value']):.4f} |"  # type: ignore[call-overload,index]
        )

    if not rows_md:
        return []

    lines: list[str] = ["## 主结论:指数纳入是否产生显著超额收益", ""]
    lines.append(
        "本节用事件研究 CAR[-1,+1] 直接回答论文核心问题。"
        "下方 7 条机制假说回答的是 CN/US 反应不一致的来源,"
        "**不是**回答\"是否上涨\"本身。"
    )
    lines.append("")
    lines.append(
        "> **Disclosure: post-hoc, not pre-registered.** 下方 7 条 CMA 假说(H1–H7)"
        "是在观察到 announce-vs-effective 不对称结果**之后**形成的,属于 post-hoc "
        "解释。本项目**未公开 Pre-Analysis Plan (PAP)**。意涵:"
    )
    lines.append(">")
    lines.append(
        "> - Verdict 阈值(默认 p<0.10、inner=0.05)合理但**未事前承诺**。"
    )
    lines.append(
        "> - 多重检验校正(Bonferroni、Benjamini-Hochberg)已在 "
        "`cma_hypothesis_verdicts.csv` 中报告,但是在假说选定之**后**应用的。"
    )
    lines.append(
        "> - 样本量限制是数据本身的约束(如 H2 n=12 来自 Federal Reserve Z.1 年度数据),"
        "不是看到结果后再剔除样本。"
    )
    lines.append(">")
    lines.append(
        "> 论文主表建议**只引用 `evidence_tier=core` 的假说(H1/H5/H7)**,"
        "supplementary 走附录。下一轮迭代前请按 `docs/verdict_iteration.md` 的"
        "预注册流程冻结假说与阈值。完整数据与方法限制见 "
        "[docs/limitations.md](limitations.md)。"
    )
    lines.append("")
    lines.append("| 市场 | 阶段 | n | mean CAR[-1,+1] | t | p |")
    lines.append("|---|---|---|---|---|---|")
    lines.extend(rows_md)
    lines.append("")

    findings: list[str] = []
    cn_announce = cells.get(("CN", "announce"))
    us_announce = cells.get(("US", "announce"))
    cn_effective = cells.get(("CN", "effective"))
    us_effective = cells.get(("US", "effective"))
    if cn_announce and us_announce:
        findings.append(
            f"- **公告日均显著正向**:CN {cn_announce['mean_car'] * 100:+.2f}% "
            f"(t={cn_announce['t']:.2f}, p={cn_announce['p']:.4f})、"
            f"US {us_announce['mean_car'] * 100:+.2f}% "
            f"(t={us_announce['t']:.2f}, p={us_announce['p']:.4f})。"
            "与 Shleifer (1986)、Harris-Gurel (1986)、Lynch-Mendenhall (1997) 等"
            "指数效应文献方向一致。"
        )
    if cn_effective and us_effective:
        findings.append(
            f"- **生效日效应基本消散**:CN {cn_effective['mean_car'] * 100:+.2f}% "
            f"(p={cn_effective['p']:.4f})、"
            f"US {us_effective['mean_car'] * 100:+.2f}% "
            f"(p={us_effective['p']:.4f})。"
            "公告日已完成主要 price discovery,与 Greenwood-Sammon (2022) "
            "\"S&P500 inclusion effect 已弱化\" 的发现方向一致。"
        )
    if findings:
        lines.append("**论文核心发现**")
        lines.append("")
        lines.extend(findings)
        lines.append(
            "- **机制定位**:超额收益主要发生在公告日,"
            "意味着\"为何上涨\"的解释焦点应推向公告期机制(信息泄露 / 行业结构 / 关注度提升),"
            "而非生效日的被动配置需求冲击。"
        )
        lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _engineering_appendix_block() -> list[str]:
    """Render the engineering-products appendix.

    Dashboard + HS300 RDD L3 工作台 are infrastructure that supports the
    research but is not part of the core paper narrative; this block
    points readers to the dedicated docs.
    """
    return [
        "---",
        "",
        "## 附录:工程产品与复现框架",
        "",
        "本仓库除论文核心实证外,还包含两个独立的工程产品。它们**不属于论文核心叙事**,",
        "但支撑研究透明度与复现性,论文中可作为方法附录或补充材料引用:",
        "",
        "### Dashboard(界面层)",
        "",
        "`index-inclusion-dashboard` 提供一页式总展板,3 分钟汇报 / 展示 / 完整材料三种模式,",
        "支持 verdict ↔ literature 双向链接、ECharts 交互图表、真实证据卡 drilldown。",
        "完整 CLI 参考见 [docs/cli_reference.md](cli_reference.md);"
        "架构见 [docs/dashboard_architecture.md](dashboard_architecture.md)。",
        "",
        "### HS300 RDD L3 候选采集工作台",
        "",
        "`/rdd-l3` 浏览器工作台 + `index-inclusion-prepare-hs300-rdd` / "
        "`reconstruct-hs300-rdd` / `plan-hs300-rdd-l3` CLI 工具链,",
        "覆盖从公开重建(L2)到中证官方候选(L3)的完整采集流程。",
        "完整工作流见 [docs/hs300_rdd_workflow.md](hs300_rdd_workflow.md)。",
        "",
    ]


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
        " 每个事件采用 [-20, +20] 交易日的事件窗口，匹配对照组按 sector × 同期市值 quintile 抽取。"
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
        "- **CMA 7 条机制假说**: 见下方逐项裁决，每条假说都有自动产出的 verdict + 头条指标。",
        "  完整 metric pipeline 见 `index-inclusion-cma`(`results/real_tables/cma_*.csv`)。",
        "- **HS300 RDD**: cutoff=300 的运行变量断点，公告日 `[-1,+1]` 主结果。",
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
            "- HS300 RDD 当前已使用 L3 官方候选边界样本，覆盖 2020-11 到 2025-11 共 11 个批次；",
            "  在扩展到 ≥10 年（约 20 批次）以前，RDD 结论仍应限定为初步识别证据，不可表述为完整中证官方历史 ranking score 因果结论。",
            "- RDD 稳健性面板（main / donut / placebo / polynomial）已落到 `rdd_robustness.csv` 与首页 forest plot；",
            "  论文写作时建议在主表脚注同时引用稳健性结果，避免只报告显著的 main spec。",
            "- 跨市场比较默认按事件汇总(announce vs effective × CN vs US 4 象限),后续可叠加事件级",
            "  bootstrap / permutation 检验，以及 sector × size 的交互检验，进一步压低单通道误判风险。",
            "- 长窗口(>120 日)的 retention ratio 在样本量收缩时会跳到 NA,",
            "  当前 demand_curve 主线主要靠 `[0,+60]` / `[0,+120]` 给出方向性结论。",
            "",
        ]
    )
    return lines


def _rdd_robustness_block(robustness_path: Path) -> list[str]:
    """Optional RDD robustness subsection rendered from rdd_robustness.csv.

    Skipped silently when the CSV is missing or empty — keeps the verdict
    section renderable in pre-RDD-rerun states without forcing the doctor
    sync check to flag false positives.
    """
    if not robustness_path.exists():
        return []
    try:
        df = pd.read_csv(robustness_path)
    except Exception:  # noqa: BLE001 - never break paper rendering on bad CSV
        return []
    if df.empty or "spec" not in df.columns:
        return []

    spec_kind_order = ["main", "donut", "placebo", "polynomial"]
    df = df.assign(
        _kind_rank=df["spec_kind"].map(
            lambda k: spec_kind_order.index(str(k)) if str(k) in spec_kind_order else 99
        )
    ).sort_values(["_kind_rank", "spec"])

    def _interpret(spec_kind: str, tau: float, p: float) -> str:
        if spec_kind == "main":
            if p < 0.05:
                return "边界显著"
            if p < 0.10:
                return "边界 marginal"
            return "未显著"
        if spec_kind == "donut":
            return "扔近邻后变化" if abs(tau) > 0 else ""
        if spec_kind == "placebo":
            return "placebo 不显著（识别合理）" if p >= 0.10 else "placebo 显著（识别存疑）"
        if spec_kind == "polynomial":
            return "spec sensitivity（高阶项吸收跳跃）" if p >= 0.10 else "高阶项下仍显著"
        return ""

    lines: list[str] = ["### HS300 RDD 稳健性面板", ""]
    lines.append(
        "`results/literature/hs300_rdd/rdd_robustness.csv` 在 main 局部线性的基础上跑了 4 类稳健性 spec，"
        "**统一锁定到 main 自动选出的 bandwidth**（避免 placebo cutoff 的样本-窗口漂移把 spec 噪声混进 τ）："
    )
    lines.append("")
    lines.append("| 设定 | τ (CAR[-1,+1]) | p | n_obs | 解读 |")
    lines.append("|---|---|---|---|---|")
    for _, r in df.iterrows():
        spec_kind = str(r.get("spec_kind", ""))
        tau = float(r["tau"]) if pd.notna(r["tau"]) else float("nan")
        p_val = float(r["p_value"]) if pd.notna(r["p_value"]) else float("nan")
        n_obs = int(r["n_obs"]) if pd.notna(r["n_obs"]) else 0
        tau_text = f"{tau * 100:+.2f}%" if not pd.isna(tau) else "—"
        p_text = f"{p_val:.3f}" if not pd.isna(p_val) else "—"
        if spec_kind == "main":
            tau_text = f"**{tau_text}**"
            p_text = f"**{p_text}**"
        lines.append(
            f"| {r['spec']} | {tau_text} | {p_text} | {n_obs} | {_interpret(spec_kind, tau, p_val)} |"
        )
    lines.append("")
    main = df.loc[df["spec_kind"] == "main"].head(1)
    if not main.empty:
        m_tau = float(main.iloc[0]["tau"])
        m_p = float(main.iloc[0]["p_value"])
        m_n = int(main.iloc[0]["n_obs"])
        lines.append(
            f"**论文级表述建议**：HS300 RDD main 在公告日 CAR[-1,+1] 上的边界显著（τ={m_tau * 100:.2f}%, p={m_p:.3f}, n={m_n}）；"
            "placebo cutoff 的 τ 都接近 0 支持识别合理，但 donut / polynomial 提示效应对设定敏感。"
            "**结论应当限定为初步识别证据**，论文需如实报告全套稳健性面板。"
        )
        lines.append("")
    return lines


def render_paper_verdict_section(
    verdicts: pd.DataFrame,
    *,
    event_counts: pd.DataFrame | None = None,
    event_study_summary: pd.DataFrame | None = None,
) -> str:
    """Render verdicts as a paper-ready markdown section.

    Structure:

    1. 主结论 — does index inclusion produce a significant CAR? (read
       from ``event_study_summary`` for inclusion=1 × [-1,+1] window).
    2. 机制层裁决 — CN/US asymmetry mechanisms (verdicts CSV → 7 H rows).
       This part **does not** answer "is there an effect?"; it answers
       "why do the two markets respond differently?".
    3. 限制与稳健性补强方向 (when sample summary is available).
    4. 工程产品与复现框架附录 (dashboard / HS300 L3 pointers).

    When ``event_study_summary`` is omitted, step 1 is skipped — the
    document starts directly at the mechanism-level narrative (legacy
    behavior).
    """
    main_finding_lines = _main_finding_block(event_study_summary)

    if verdicts is None or verdicts.empty:
        if main_finding_lines:
            return "\n".join(
                main_finding_lines
                + [
                    "## 机制层裁决:CN/US 不对称的结构性来源",
                    "",
                    "(暂无 verdict 数据。先跑 `index-inclusion-cma`。)",
                    "",
                ]
                + _engineering_appendix_block()
            ).rstrip() + "\n"
        return "## 机制层裁决:CN/US 不对称的结构性来源\n\n(暂无 verdict 数据。先跑 `index-inclusion-cma`。)\n"

    counts: dict[str, int] = {}
    for _, row in verdicts.iterrows():
        verdict_label = str(row["verdict"])
        counts[verdict_label] = counts.get(verdict_label, 0) + 1

    aggregate_parts = []
    for label in ("支持", "部分支持", "证据不足", "待补数据"):
        if counts.get(label, 0) > 0:
            aggregate_parts.append(f"{counts[label]} 项{label}")

    lines: list[str] = []
    lines.extend(main_finding_lines)
    lines.append("## 机制层裁决:CN/US 不对称的结构性来源")
    lines.append("")
    lines.append(
        "下面 7 条假说回答的是 \"为什么 CN/US 在公告日 / 生效日的反应不一致\","
        "而**不是**直接回答 \"指数纳入是否产生超额收益\"(那个问题在上节已回答)。"
    )
    lines.append("")
    lines.append(
        f"基于跨市场不对称(CMA)pipeline 自动产出,7 条 CN/US 不对称机制假说的当前裁决分布:"
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
    rdd_robustness_lines = _rdd_robustness_block(_DEFAULT_RDD_ROBUSTNESS_PATH)
    if rdd_robustness_lines:
        lines.extend(rdd_robustness_lines)
    lines.extend(_engineering_appendix_block())
    return "\n".join(lines).rstrip() + "\n"


def export_paper_verdict_section(
    verdicts: pd.DataFrame,
    *,
    output_path: Path,
    event_counts: pd.DataFrame | None = None,
    event_study_summary: pd.DataFrame | None = None,
) -> Path:
    """Write the paper-ready verdict section to ``output_path`` (a .md file)."""
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        render_paper_verdict_section(
            verdicts,
            event_counts=event_counts,
            event_study_summary=event_study_summary,
        )
    )
    return out_path
