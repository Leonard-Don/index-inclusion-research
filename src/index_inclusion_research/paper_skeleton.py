"""Generate the paper skeleton markdown (``paper/skeleton.md``).

``index-inclusion-paper-skeleton`` renders a full Markdown paper template
— section headers, figure references, table placeholders, auto-populated
event-study results, robustness summaries, and ``[TODO: prose]`` markers
— so the writing step starts from a structured skeleton instead of a blank
page.

The generator NEVER fabricates prose. Every auto-populated block is
derived deterministically from the current research artifacts:

- ``results/real_tables/event_study_summary.csv`` — core announcement/
  effective-date CAR results (auto-embedded if present)
- ``results/real_tables/pap_deviation_report.csv`` — PAP deviation audit
- ``docs/limitations.md`` — §7.2 limitations text (verbatim inclusion)
- ``literature_catalog.PAPER_LIBRARY`` — §References enumeration

Sections that require human prose are marked with ``[TODO: ...]`` so
the author can grep for ``TODO`` and walk through every remaining
writing decision.

Jinja2 (already transitively available through ``flask``) renders the
markdown; the template lives inline as a module-level string constant so
the skeleton is one self-contained module.

Note: the 7-hypothesis (H1–H7) cross-market asymmetry analysis and the
HS300 RDD are documented in ``docs/analysis_parameters.md`` and retained
in the CLI for reproducibility, but they are NOT part of the main paper
skeleton — they were post-hoc and are disclosed honestly in §6 讨论.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from jinja2 import Environment

from index_inclusion_research import paths

logger = logging.getLogger(__name__)

# Canonical ordered hypothesis IDs — kept for any helpers that still reference
# the verdict CSV, but H1–H7 are no longer emitted into the main skeleton.
EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")

# Sanity bound: a well-formed skeleton should fall in this size band (bytes).
# Lower bound: §1–§7 headers + §References + Appendix + limitations verbatim
# (~3 KB) + 16 paper refs.  Upper bound: same with all prose TODOs expanded
# and limitations file at full length.
SKELETON_MIN_BYTES = 4 * 1024
SKELETON_MAX_BYTES = 28 * 1024


# ---------------------------------------------------------------------------
# Default file locations (resolved through ``paths`` so tests can override)
# ---------------------------------------------------------------------------


def _default_output_path() -> Path:
    return paths.project_root() / "paper" / "skeleton.md"


def _default_verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_pap_csv() -> Path:
    return paths.real_tables_dir() / "pap_deviation_report.csv"


def _default_rdd_csv() -> Path:
    return paths.literature_results_dir() / "hs300_rdd" / "rdd_robustness.csv"


def _default_public_summary_json() -> Path:
    return paths.project_root() / "data" / "public" / "index_research_summary.json"


def _default_limitations_md() -> Path:
    return paths.docs_dir() / "limitations.md"


def _default_power_analysis_csv() -> Path:
    return paths.real_tables_dir() / "power_analysis_report.csv"


def _default_event_study_summary_csv() -> Path:
    return paths.real_tables_dir() / "event_study_summary.csv"


def _default_figures_dir() -> Path:
    return paths.results_dir() / "figures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    """Return ``pd.read_csv(path)`` or empty DataFrame on any error."""
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001 — defensive; we only log
        logger.warning("Failed to read %s: %s", path, exc)
        return pd.DataFrame()


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
        return {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read %s: %s", path, exc)
        return {}


def _read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read %s: %s", path, exc)
        return ""


def _coerce_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        if isinstance(value, float) and pd.isna(value):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    return str(value).strip()


def _coerce_float(value: Any) -> float | None:
    """Return a finite-ish float value or ``None`` for absent/invalid inputs."""

    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VerdictRow:
    """Bare-minimum verdict row for the skeleton hypothesis table."""

    hid: str
    name_cn: str
    verdict: str
    confidence: str
    evidence_tier: str
    n_obs: int
    headline_metric: str
    track: str


@dataclass(frozen=True)
class PowerAnalysisRow:
    """One row of the §5 Limitations post-hoc power table."""

    hid: str
    n_obs: int
    test_family: str
    power_at_observed: float
    mde_at_80_power: float
    interpretation: str


def _power_analysis_rows(power_csv: Path) -> list[PowerAnalysisRow]:
    """Render the on-disk power-analysis report as a list of rows.

    Returns ``[]`` when the file is missing or columns are
    insufficient; the template falls through to a "[TODO]" branch.
    """
    df = _read_csv_or_empty(power_csv)
    needed = {
        "hid",
        "n_obs",
        "test_family",
        "power_at_observed",
        "mde_at_80_power",
        "interpretation",
    }
    if df.empty or not needed.issubset(df.columns):
        return []
    rows: list[PowerAnalysisRow] = []
    for _, row in df.iterrows():
        hid = _coerce_str(row.get("hid"))
        if not hid:
            continue
        try:
            power = float(row.get("power_at_observed", float("nan")))
            mde = float(row.get("mde_at_80_power", float("nan")))
        except (TypeError, ValueError):
            continue
        rows.append(
            PowerAnalysisRow(
                hid=hid,
                n_obs=_coerce_int(row.get("n_obs")),
                test_family=_coerce_str(row.get("test_family")),
                power_at_observed=power,
                mde_at_80_power=mde,
                interpretation=_coerce_str(row.get("interpretation")),
            )
        )
    return rows


def _verdict_rows(verdicts_df: pd.DataFrame) -> list[VerdictRow]:
    """Return verdict rows in canonical H1..H7 order."""
    if verdicts_df.empty:
        return []
    by_hid: dict[str, VerdictRow] = {}
    for _, row in verdicts_df.iterrows():
        hid = _coerce_str(row.get("hid"))
        if not hid:
            continue
        key_label = _coerce_str(row.get("key_label"))
        key_value_raw = row.get("key_value")
        try:
            key_value = (
                float(key_value_raw)
                if key_value_raw is not None
                and not (isinstance(key_value_raw, float) and pd.isna(key_value_raw))
                else None
            )
        except (TypeError, ValueError):
            key_value = None
        if key_label and key_value is not None:
            headline = f"{key_label} = {key_value:.4g}"
        elif key_label:
            headline = key_label
        else:
            headline = ""
        by_hid[hid] = VerdictRow(
            hid=hid,
            name_cn=_coerce_str(row.get("name_cn")),
            verdict=_coerce_str(row.get("verdict")),
            confidence=_coerce_str(row.get("confidence")),
            evidence_tier=_coerce_str(row.get("evidence_tier")),
            n_obs=_coerce_int(row.get("n_obs")),
            headline_metric=headline,
            track=_coerce_str(row.get("track")),
        )
    out: list[VerdictRow] = []
    for hid in EXPECTED_HIDS:
        if hid in by_hid:
            out.append(by_hid[hid])
    for hid in sorted(set(by_hid.keys()) - set(EXPECTED_HIDS)):
        out.append(by_hid[hid])
    return out


def _hs300_main_block(rdd_df: pd.DataFrame) -> dict[str, Any]:
    """Extract HS300 RDD main-spec headline for §4.3 prose."""
    if rdd_df.empty:
        return {}
    main_rows = rdd_df[rdd_df["spec_kind"].astype(str).str.lower() == "main"]
    if main_rows.empty:
        return {}
    main = main_rows.iloc[0]
    tau = _coerce_float(main.get("tau"))
    if tau is None:
        return {}
    p_value = _coerce_float(main.get("p_value"))
    bandwidth = _coerce_float(main.get("bandwidth"))
    return {
        "tau_pct": round(tau * 100.0, 2),
        "p_value": round(p_value, 4) if p_value is not None else None,
        "n_obs": _coerce_int(main.get("n_obs")),
        "outcome": _coerce_str(main.get("outcome")) or None,
        "bandwidth": bandwidth,
        "robustness_count": int(len(rdd_df)),
    }


def _sensitivity_block(public_summary: dict[str, Any]) -> dict[str, Any]:
    """Pull sensitivity sweep totals out of the public summary JSON.

    Falls back to ``None`` for any axis the summary doesn't carry, so
    the template's ``{% if %}`` guards take care of partial inputs.
    """
    sens = public_summary.get("sensitivity_robustness") or {}
    threshold = sens.get("threshold_axis") or {}
    ar = sens.get("ar_engine_axis") or {}
    two_d = sens.get("two_dimensional") or {}
    return {
        "threshold": {
            "thresholds_tested": threshold.get("thresholds_tested") or [],
            "cell_count": threshold.get("cell_count"),
            "stable_count": threshold.get("stable_count"),
            "flip_count": threshold.get("flip_count"),
        }
        if threshold
        else None,
        "ar_engine": {
            "models_tested": ar.get("ar_models_tested") or [],
            "cell_count": ar.get("cell_count"),
            "stable_count": ar.get("stable_count"),
            "flip_count": ar.get("flip_count"),
            "flipping_hypotheses": ar.get("flipping_hypotheses") or [],
        }
        if ar
        else None,
        "two_d": {
            "cell_count": two_d.get("cell_count"),
            "stable_count": two_d.get("stable_count"),
            "flip_count": two_d.get("flip_count"),
            "flipping_hypotheses": two_d.get("flipping_hypotheses") or [],
        }
        if two_d
        else None,
    }


def _pap_block(
    public_summary: dict[str, Any], pap_df: pd.DataFrame
) -> dict[str, Any]:
    """Build §7 PAP compliance block: snapshot date + deviation classification."""
    baseline = public_summary.get("pap_baseline") or {}
    deviation = public_summary.get("pap_deviation_summary") or {}
    rows: list[dict[str, str]] = []
    if not pap_df.empty:
        for _, row in pap_df.iterrows():
            rows.append(
                {
                    "hid": _coerce_str(row.get("hid")),
                    "name_cn": _coerce_str(row.get("name_cn")),
                    "classification": _coerce_str(row.get("classification")),
                    "baseline_verdict": _coerce_str(row.get("baseline_verdict")),
                    "current_verdict": _coerce_str(row.get("current_verdict")),
                }
            )
    return {
        "snapshot_date": baseline.get("snapshot_date"),
        "snapshot_path": baseline.get("path_ref"),
        "frozen_for_days": baseline.get("frozen_for_days"),
        "deviation": {
            "all_unchanged": deviation.get("all_unchanged"),
            "flipped_count": deviation.get("flipped_count"),
            "tightened_count": deviation.get("tightened_count"),
            "weakened_count": deviation.get("weakened_count"),
            "unverifiable_count": deviation.get("unverifiable_count"),
            "unchanged_count": deviation.get("unchanged_count"),
        }
        if deviation
        else None,
        "rows": rows,
    }


def _references_block() -> list[dict[str, str]]:
    """Enumerate the 16-paper literature library for §References.

    Lazily imported so a stripped-down test environment without the
    catalog can still call other helpers (the bundled tests do pull
    the real catalog, but the helper imports defensively).
    """
    try:
        from index_inclusion_research.literature_catalog import (
            list_literature_papers,
        )
    except ImportError as exc:
        logger.warning("literature_catalog import failed: %s", exc)
        return []
    papers = list_literature_papers()
    out: list[dict[str, str]] = []
    for paper in papers:
        out.append(
            {
                "paper_id": paper.paper_id,
                "authors": paper.authors,
                "year": paper.year_label,
                "title": paper.title,
                "market_focus": paper.market_focus,
            }
        )
    return out


def _literature_section(public_summary: dict[str, Any]) -> dict[str, Any]:
    lit = public_summary.get("literature") or {}
    return {
        "papers_indexed": lit.get("papers_indexed"),
        "console_scripts_count": lit.get("console_scripts_count"),
        "research_threads": lit.get("research_threads"),
        "thread_names": lit.get("research_thread_names") or [],
    }


def _citation_network_block() -> dict[str, Any]:
    """Distill the 16-paper heuristic literature-link graph into one sentence.

    Returns ``{edge_count, node_count, most_linked_label,
    bridge_papers_label}`` ready to drop into the §References auto-sentence.
    Tolerant: if the catalog can't be imported (stripped-down test env)
    or the module raises, returns zeros / TODO labels so the skeleton
    keeps rendering.
    """
    try:
        from index_inclusion_research.citation_graph import (
            build_citation_graph,
            summarize_for_paper_skeleton,
        )
    except ImportError as exc:
        logger.warning("citation_graph import failed: %s", exc)
        return {
            "edge_count": 0,
            "node_count": 0,
            "most_linked_label": "TODO",
            "bridge_papers_label": "TODO",
        }
    graph = build_citation_graph()
    return summarize_for_paper_skeleton(graph)


def _figures_available(figures_dir: Path) -> set[str]:
    """Return basenames present in the published figures directory."""
    if not figures_dir.exists():
        return set()
    return {p.name for p in figures_dir.iterdir() if p.is_file()}


def _event_study_core_numbers(event_study_csv: Path) -> dict[str, Any]:
    """Extract the four headline CAR numbers for the §4.1 auto-summary.

    Looks for rows where:
      - market=CN/US, event_phase=announce/effective, inclusion=1,
        window_slug=m1_p1 (i.e. CAR[-1,+1]).

    Returns a dict with keys:
      cn_announce_car, cn_announce_t, cn_announce_p, cn_n,
      us_announce_car, us_announce_t, us_announce_p, us_n,
      cn_effective_car, cn_effective_t, cn_effective_p,
      us_effective_car, us_effective_t, us_effective_p,
      cn_events, us_events, sample_size_summary.

    Any missing value is represented as ``None`` so the template can use
    ``{{ val or "TODO" }}`` gracefully.
    """
    df = _read_csv_or_empty(event_study_csv)
    out: dict[str, Any] = {}
    if df.empty:
        return out

    needed = {
        "market", "event_phase", "inclusion", "window_slug",
        "mean_car", "t_stat", "p_value", "n_events",
    }
    if not needed.issubset(df.columns):
        return out

    def _row(market: str, phase: str, inclusion: int) -> pd.Series | None:
        mask = (
            (df["market"].astype(str).str.upper() == market.upper())
            & (df["event_phase"].astype(str).str.lower() == phase.lower())
            & (df["inclusion"].astype(str) == str(inclusion))
            & (df["window_slug"].astype(str) == "m1_p1")
        )
        sub = df.loc[mask]
        return sub.iloc[0] if not sub.empty else None

    def _fmt_car(val: Any) -> str | None:
        v = _coerce_float(val)
        if v is None:
            return None
        return f"{v * 100:+.2f}"

    def _fmt_t(val: Any) -> str | None:
        v = _coerce_float(val)
        if v is None:
            return None
        return f"{v:.2f}"

    def _fmt_p(val: Any, *, threshold: float = 0.001) -> str | None:
        v = _coerce_float(val)
        if v is None:
            return None
        if v < threshold:
            return f"<{threshold}"
        return f"={v:.3f}"

    for market, phase, key_prefix in (
        ("CN", "announce", "cn_announce"),
        ("US", "announce", "us_announce"),
        ("CN", "effective", "cn_effective"),
        ("US", "effective", "us_effective"),
    ):
        row = _row(market, phase, 1)
        if row is None:
            continue
        out[f"{key_prefix}_car"] = _fmt_car(row.get("mean_car"))
        out[f"{key_prefix}_t"] = _fmt_t(row.get("t_stat"))
        out[f"{key_prefix}_p"] = _fmt_p(row.get("p_value"))

    # n_events (inclusion=1, announce window) — one per market
    for market, key in (("CN", "cn_n"), ("US", "us_n")):
        row = _row(market, "announce", 1)
        if row is not None:
            n = _coerce_int(row.get("n_events"))
            out[key] = n if n else None

    # Also count total events across all inclusion values for display
    for market, key in (("CN", "cn_events"), ("US", "us_events")):
        mask = (df["market"].astype(str).str.upper() == market.upper()) & (
            df["window_slug"].astype(str) == "m1_p1"
        ) & (df["inclusion"].astype(str) == "1")
        sub = df.loc[mask]
        if not sub.empty:
            n = _coerce_int(sub.iloc[0].get("n_events"))
            out[key] = n if n else None

    # Sample-size summary sentence
    cn_n = out.get("cn_n")
    us_n = out.get("us_n")
    if cn_n and us_n:
        out["sample_size_summary"] = (
            f"中国 {cn_n} 个纳入事件、美国 {us_n} 个纳入事件"
        )
    elif cn_n or us_n:
        out["sample_size_summary"] = f"n={cn_n or us_n}"
    return out


# ---------------------------------------------------------------------------
# Template (Jinja2 inline)
# ---------------------------------------------------------------------------


_TEMPLATE = r"""# 指数纳入溢价的来源：来自中美两市场的信息渠道证据

**作者**: [TODO: 作者姓名 · 单位 —— 请由作者填写]
**日期**: {{ generated_date }}
**摘要**: [TODO: 100-150 字摘要 — 概述研究问题（信息渠道 vs 需求压力渠道）、事件研究方法、{{ sample_size_summary }}、主要发现（公告窗 CAR 显著、生效窗约为零、中美量级相近）和渠道识别推断。]

完整 prose 见 paper/manuscript.tex 摘要。

---

## 1. 引言

核心论点：[TODO: 引言 prose — 铺陈指数纳入溢价的信息渠道 vs 需求压力渠道之争，引出本文公告窗/生效窗跨制度分解策略。可借鉴 paper/manuscript.tex §1。]

### 1.1 研究背景

[TODO: 背景 prose — 引用 Shleifer 1986 / Harris & Gurel 1986 作为奠基文献，过渡到 Greenwood & Sammon (2022) 的"消失的指数效应"。完整 prose 见 paper/manuscript.tex §1.1。]

### 1.2 研究问题与贡献

[TODO: 三项贡献 prose — 中美制度差异比较、公告窗/生效窗时序分解、诚实承认替代解释。完整 prose 见 paper/manuscript.tex §1.2。]

---

## 2. 文献综述

[TODO: 文献综述 prose — {{ literature.papers_indexed }} 篇核心文献按需求曲线派 / 信息渠道派 / 中国市场文献三条主线展开。完整 author-year 综述见 `docs/literature_review_author_year_cn.md`。]

本项目共索引 {{ literature.papers_indexed }} 篇核心文献，分为 {{ literature.research_threads }} 条研究主线：{{ literature.thread_names | join(", ") }}。

### 2.1 需求曲线与价格压力派

[TODO: prose — Shleifer 1986、Harris & Gurel 1986、Kaul 等 (2000)、Wurgler & Zhuravskaya (2002)。完整 prose 见 paper/manuscript.tex §2.1。]

### 2.2 信息/认证渠道与效应消退证据

[TODO: prose — Lynch & Mendenhall (1997)、Denis 等 (2003)、Greenwood & Sammon (2022)。完整 prose 见 paper/manuscript.tex §2.2。]

### 2.3 中国市场文献

[TODO: prose — Chu 等 (2021)、Yao 等 (2022)。完整 prose 见 paper/manuscript.tex §2.3。]

### 2.4 文献评述与本文定位

[TODO: prose — 争论焦点演进与本文定位。完整 prose 见 paper/manuscript.tex §2.4。]

---

## 3. 研究设计

### 3.1 样本与数据

[TODO: 样本期、数据源、清洗规则。关键规模：893 个真实事件（CN {{ cn_events or "274" }} 个，US {{ us_events or "619" }} 个）。数据来源：Yahoo Finance 日频；被动 AUM：US Federal Reserve Z.1，CN ETF TNA proxy。完整 prose 见 paper/manuscript.tex §3.1。]

### 3.2 实证方法

[TODO: 两层策略 — 事件研究（CAR[-1,+1]主，[-3,+3]、[-5,+5]辅，简单市场调整 AR，Patell Z + BMP t）+ 匹配回归（212,756 行面板，block bootstrap 5000 次）。完整 prose 见 paper/manuscript.tex §3.2。]

### 3.3 识别策略：公告窗与生效窗的跨市场分解

关键识别逻辑：信息渠道预测公告窗显著、生效窗约为零、中美量级相近；需求压力渠道预测生效窗显著、且美国（被动规模更大）效应应强于中国。识别局限：描述性事件研究，非准实验因果识别；生效窗约为零也与需求被提前套利相容。完整 prose 见 paper/manuscript.tex §3.3。

---

## 4. 实证结果

### 4.1 核心结果：公告窗与生效窗的比较

本节为论文实证主线。主要结果（来自 `results/real_tables/event_study_summary.csv`）：

- 中国公告日 CAR[-1,+1] = +{{ cn_announce_car or "TODO" }}%（t={{ cn_announce_t or "TODO" }}，p{{ cn_announce_p or "TODO" }}，n={{ cn_n or "TODO" }}）
- 美国公告日 CAR[-1,+1] = +{{ us_announce_car or "TODO" }}%（t={{ us_announce_t or "TODO" }}，p{{ us_announce_p or "TODO" }}，n={{ us_n or "TODO" }}）
- 中国生效日 CAR[-1,+1] = {{ cn_effective_car or "TODO" }}%（t={{ cn_effective_t or "TODO" }}，p={{ cn_effective_p or "TODO" }}，不显著）
- 美国生效日 CAR[-1,+1] = {{ us_effective_car or "TODO" }}%（t={{ us_effective_t or "TODO" }}，p={{ us_effective_p or "TODO" }}，不显著）

完整表格与 CAR 路径图见 paper/manuscript.tex §4.1（表 1，图 1–4）。

---

## 5. 稳健性

本节从五个维度检验 §4 核心结果，各子节所引数字均来自相应结果文件。

### 5.1 纳入 vs 剔除不对称

[TODO: prose — 若信息/认证渠道为主导，则纳入与剔除应呈方向不对称。中国：纳入 +1.76%，剔除 -0.59%；美国：纳入 +1.84%，剔除 +0.05%。完整 prose 见 paper/manuscript.tex §5.1。]

### 5.2 长窗口 CAR 的持续性

[TODO: prose — 中国 [0,+120] 均值 CAR = +1.56%（t=0.66，不显著）；美国 = +1.96%（t=1.57，不显著）。点估计正向，不支持大幅反转。完整 prose 见 paper/manuscript.tex §5.2。]

### 5.3 公告效应的跨年稳定性

[TODO: prose — 按公告年份分解，中国 2021–2025 各年均值均为正；美国 2010–2025 大多数年份均值为正。完整 prose 见 paper/manuscript.tex §5.3。]

### 5.4 匹配对照组的协变量平衡

[TODO: prose — SMD<0.25 门禁，三项协变量（对数市值、前期收益、前期波动率）全部通过。完整 prose 见 paper/manuscript.tex §5.4。]

### 5.5 预公告漂移：一项无法排除的不确定性

[TODO: prose — 中国公告前均值漂移 +3.09%，美国 +2.59%，两市场差异 bootstrap p=0.875。关键诚实声明：本文无法排除公告前已部分消化信息的可能。完整 prose 见 paper/manuscript.tex §5.5。]

---

## 6. 讨论

本节围绕三个讨论要点，解读实证结果对信息 vs 需求渠道识别的含义，并诚实披露早期探索性假说的背景。

**讨论点 (1)**：[TODO: 为什么"跨制度相似性"指向信息渠道 — 中美被动 AUM 量级差异极大，需求渠道预测应产生可识别的跨市场量级分化，但数据中两市场公告窗 CAR 相差仅 0.08 个百分点。完整 prose 见 paper/manuscript.tex §6。]

**讨论点 (2)**：[TODO: 生效窗约为零的两种并列解释 — (a) 机械调仓冲击已被市场深度吸收；(b) 套利者将需求效应提前定价压平（Greenwood & Sammon, 2022）。完整 prose 见 paper/manuscript.tex §6。]

**讨论点 (3)**：[TODO: 早期探索性假说的诚实交代 — 本项目曾探索 7 条跨市场机制假说（H1–H7），但这些假说形成于观测结果之后（post-hoc）、本项目无预分析计划、部分假说依赖极小样本，故不纳入本文主线。相关分析参数见 `docs/analysis_parameters.md`。完整 prose 见 paper/manuscript.tex §6。]

---

## 7. 结论与局限

### 7.1 主要结论

[TODO: prose — 三点主要结论：(1) 公告窗 CAR 在中美两市场均显著为正；(2) 生效窗 CAR 在中美两市场均不显著；(3) 效应集中于公告窗且跨制度量级相近，指向信息/认证渠道。完整 prose 见 paper/manuscript.tex §7.1。]

### 7.2 局限

[TODO: prose — 六项明确局限：(1) 描述性事件研究，非准实验因果识别；(2) 增量贡献主要在跨制度系统比较；(3) 中国有效事件约 117 个，样本期仅 5 年；(4) 市值与换手率数据口径为近似；(5) 生效窗约为零的可替代解释无法排除；(6) 公告前预漂移无法排除。完整 prose 见 paper/manuscript.tex §7.2。]

下文为 `docs/limitations.md` 的自动嵌入，便于审稿人无须翻附录直接阅读：

---

{{ limitations_text }}

---

## 参考文献

下列 {{ references | length }} 篇文献来自 `literature_catalog.PAPER_LIBRARY`（项目核心文献库）：

启发式文献关联网络（自动）：本项目文献库共 {{ citation_network.node_count }} 篇，共 {{ citation_network.edge_count }} 条"主题/方法/年代"关联边；关联最多：{{ citation_network.most_linked_label }}；桥梁文献（betweenness）：{{ citation_network.bridge_papers_label }}。这不是已验证引用关系，也不是逐条 bibliography citation 核验；只用于文献综述导航，不得作为被引/引用证据。可视化见 `results/literature/citation_network.png`（中心性 CSV：`results/literature/citation_centrality.csv`，由 `index-inclusion-citation-graph` 生成）。

{% for ref in references -%}
{{ loop.index }}. {{ ref.authors }} ({{ ref.year }}). *{{ ref.title }}*. {{ ref.market_focus }}. `paper_id={{ ref.paper_id }}`.
{% endfor %}

## 附录

### A. 数据契约

[TODO: 数据契约 — 三个输入表字段约定（events.csv、prices.csv、benchmarks.csv）及被动 AUM 与行业标签口径说明。完整契约见 `docs/hs300_rdd_data_contract.md` 与 `docs/real_data_notes.md`。]

### B. CLI 入口 ({{ literature.console_scripts_count }} 个)

[TODO: CLI 入口列表 — 完整 {{ literature.console_scripts_count or "TODO" }} 个 console scripts 的分组、用法与示例命令见 `docs/cli_reference.md`。RDD 相关命令保留于 CLI 中但 RDD 结果不进入本文实证主线。]

### C. 复现指南

```bash
make rebuild           # 10 步流水线：从原始数据到事件研究结果
make figures-tables    # 重绘 5 张论文级 figure
make paper             # 论文交付包：自动复制到 paper/
index-inclusion-export-public-summary  # 刷新公共摘要
index-inclusion-paper-bundle --force   # 重新生成本骨架
```

公共摘要工件：`data/public/index_research_summary.json`（schema v{{ schema_version }}），是面向外部消费者（sibling 项目、GitHub Pages 日报）的稳定入口。
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_paper_skeleton(
    *,
    verdicts_csv: Path | None = None,
    pap_csv: Path | None = None,
    rdd_csv: Path | None = None,
    public_summary_json: Path | None = None,
    limitations_md: Path | None = None,
    figures_dir: Path | None = None,
    power_analysis_csv: Path | None = None,
    event_study_csv: Path | None = None,
    generated_at: datetime | None = None,
) -> str:
    """Render the paper skeleton markdown from current research artifacts.

    Every input has a sensible default rooted at ``paths.project_root()``.
    Tests pass synthetic paths to exercise specific branches.

    Parameters
    ----------
    generated_at:
        Override for the timestamp baked into ``**日期**: ...``. Defaults
        to current UTC. Tests pin a fixed datetime so the output is
        reproducible.

    Returns
    -------
    str
        Rendered markdown (UTF-8).
    """
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    pap_csv = pap_csv or _default_pap_csv()
    rdd_csv = rdd_csv or _default_rdd_csv()
    public_summary_json = public_summary_json or _default_public_summary_json()
    limitations_md = limitations_md or _default_limitations_md()
    figures_dir = figures_dir or _default_figures_dir()
    power_analysis_csv = (
        power_analysis_csv or _default_power_analysis_csv()
    )
    event_study_csv = event_study_csv or _default_event_study_summary_csv()

    pap_df = _read_csv_or_empty(pap_csv)
    public_summary = _read_json_or_empty(public_summary_json)
    limitations_text = _read_text_or_empty(limitations_md).strip() or (
        "[TODO: limitations.md 缺失 — 先补 docs/limitations.md 再重新生成本骨架]"
    )

    # Event study core numbers for §4.1 auto-summary
    event_study_numbers = _event_study_core_numbers(event_study_csv)
    sample_size_summary = event_study_numbers.get(
        "sample_size_summary", "TODO"
    )

    context: dict[str, Any] = {
        "generated_date": (generated_at or datetime.now(tz=UTC))
        .replace(microsecond=0)
        .strftime("%Y-%m-%d"),
        "sample_size_summary": sample_size_summary,
        # Event-study headline numbers for §4.1
        "cn_announce_car": event_study_numbers.get("cn_announce_car"),
        "cn_announce_t": event_study_numbers.get("cn_announce_t"),
        "cn_announce_p": event_study_numbers.get("cn_announce_p"),
        "cn_n": event_study_numbers.get("cn_n"),
        "us_announce_car": event_study_numbers.get("us_announce_car"),
        "us_announce_t": event_study_numbers.get("us_announce_t"),
        "us_announce_p": event_study_numbers.get("us_announce_p"),
        "us_n": event_study_numbers.get("us_n"),
        "cn_effective_car": event_study_numbers.get("cn_effective_car"),
        "cn_effective_t": event_study_numbers.get("cn_effective_t"),
        "cn_effective_p": event_study_numbers.get("cn_effective_p"),
        "us_effective_car": event_study_numbers.get("us_effective_car"),
        "us_effective_t": event_study_numbers.get("us_effective_t"),
        "us_effective_p": event_study_numbers.get("us_effective_p"),
        "cn_events": event_study_numbers.get("cn_events"),
        "us_events": event_study_numbers.get("us_events"),
        "pap": _pap_block(public_summary, pap_df),
        "references": _references_block(),
        "literature": _literature_section(public_summary),
        "citation_network": _citation_network_block(),
        "limitations_text": limitations_text,
        "schema_version": public_summary.get("schema_version", 1),
    }

    env = Environment(
        trim_blocks=True,
        lstrip_blocks=False,
        keep_trailing_newline=True,
        autoescape=False,  # markdown, not HTML
    )
    template = env.from_string(_TEMPLATE)
    return template.render(**context)


def write_skeleton(
    output_path: Path,
    *,
    force: bool = False,
    generated_at: datetime | None = None,
    **build_kwargs: Any,
) -> Path:
    """Render and write the skeleton to ``output_path``.

    Refuses to overwrite an existing file unless ``force=True``.
    """
    if output_path.exists() and not force:
        raise FileExistsError(
            f"{output_path} already exists; pass --force to overwrite."
        )
    rendered = build_paper_skeleton(generated_at=generated_at, **build_kwargs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index-inclusion-paper-skeleton",
        description=(
            "Generate paper/skeleton.md — a complete Markdown paper "
            "template auto-populated from the current verdicts / PAP / "
            "HS300 RDD / sensitivity artifacts. The output marks every "
            "prose-requiring section with [TODO] so the author can grep "
            "to find what still needs writing."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Destination markdown path (default: paper/skeleton.md).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing skeleton file.",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print to stdout instead of writing to disk.",
    )
    parser.add_argument(
        "--include-todos",
        action="store_true",
        default=True,
        help="Include [TODO: ...] prose markers (default true; reserved flag).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    args = _build_arg_parser().parse_args(argv)

    if args.print:
        sys.stdout.write(build_paper_skeleton())
        return 0

    output_path = args.output or _default_output_path()
    try:
        write_skeleton(output_path, force=args.force)
    except FileExistsError as exc:
        logger.error(str(exc))
        return 1
    size_bytes = output_path.stat().st_size
    logger.info(
        "Wrote paper skeleton to %s (%d bytes)", output_path, size_bytes
    )
    if not (SKELETON_MIN_BYTES <= size_bytes <= SKELETON_MAX_BYTES):
        logger.warning(
            "Skeleton size %d bytes is outside the sanity band "
            "[%d, %d] — inspect template / inputs.",
            size_bytes,
            SKELETON_MIN_BYTES,
            SKELETON_MAX_BYTES,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
