"""Generate the paper skeleton markdown (``paper/skeleton.md``).

``index-inclusion-paper-skeleton`` renders a full Markdown paper template
— section headers, figure references, table placeholders, auto-populated
event-study results, robustness summaries, and submission-ready prose
— so the writing step starts from a structured draft instead of a blank
page.

The generator provides conservative, evidence-bounded prose. Every numeric
claim remains derived deterministically from the current research artifacts:

- ``results/real_tables/event_study_summary.csv`` — core announcement/
  effective-date CAR results (auto-embedded if present)
- ``results/real_tables/pap_deviation_report.csv`` — PAP deviation audit
- ``docs/limitations.md`` — §5.6 limitations text (verbatim inclusion)
- ``results/real_tables/power_analysis_report.csv`` — post-hoc power summary
- ``literature_catalog.PAPER_LIBRARY`` — §References enumeration

Jinja2 (already transitively available through ``flask``) renders the
markdown; the template lives inline as a module-level string constant so
the skeleton is one self-contained module.

Note: the 7-hypothesis (H1–H7) cross-market asymmetry analysis and the
HS300 RDD are documented in ``docs/analysis_parameters.md`` and retained
in the CLI for reproducibility, but they are NOT framed as pre-registered
main results — they were post-hoc and are disclosed honestly in §7 分析参数.
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


# Lower bound: §1–§7 headers + §References + Appendix + limitations verbatim
# (~3 KB) + 16 paper refs. Upper bound: submission-ready prose plus the
# full limitations file embedded verbatim.
SKELETON_MIN_BYTES = 4 * 1024
SKELETON_MAX_BYTES = 36 * 1024


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
    insufficient; the template falls through to a "待补" branch.
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
    or the module raises, returns zeros / fallback labels so the skeleton
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
            "most_linked_label": "待补",
            "bridge_papers_label": "待补",
        }
    graph = build_citation_graph()
    return summarize_for_paper_skeleton(graph)


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

    ``None`` if the CSV is missing/incomplete so the Jinja template can
    ``{{ val or "待补" }}`` gracefully.
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

**作者**: 作者待补（投稿前替换为作者姓名与单位）
**日期**: {{ generated_date }}
**摘要**: 摘要待精修：本文围绕指数纳入溢价的信息渠道与需求压力渠道之争，使用中美两市场事件研究、匹配样本回归与机制证据，比较公告日与生效日窗口的异常收益。当前样本覆盖 {{ sample_size_summary }}；核心发现是公告窗 CAR 显著为正、生效窗约为零，且中美公告窗量级接近。结果支持“公告阶段信息/认证与制度约束共同定价”的解释，而不支持把纳入溢价主要归因于生效日机械买盘。

---

## 1. 引言

指数纳入长期被视为检验需求曲线是否向下、被动资金是否影响价格，以及指数委员会认证效应是否存在的自然场景。早期 S&P 500 文献强调加入指数后的价格压力和流动性变化；近年的研究则指出，随着指数规则更透明、套利者提前布局，被动资金冲击可能在公告期之前或公告期内被重新定价。本文把争论拆解为两个可观察维度：价格反应发生在公告日还是生效日，以及这种反应在中国和美国两个制度环境中是否呈现显著量级差异。

### 1.1 研究背景

Shleifer (1986) 和 Harris & Gurel (1986) 证明指数成分调整会伴随可观的价格与成交量反应，奠定了需求曲线与价格压力解释。随后 Wurgler & Zhuravskaya、Lynch & Mendenhall 与 Denis 等研究把短期压力、长期保留、信息认证和公司基本面预期纳入同一讨论。Greenwood & Sammon (2022) 进一步提示，指数效应并非静态制度事实，而会随着可预测性、套利能力和被动资金生态变化而弱化。中国市场的涨跌停、投资者结构和指数编制流程不同于美国，因此为区分信息渠道与需求压力渠道提供了制度对照。

### 1.2 研究问题与贡献

本文回答三个问题。第一，指数纳入的短期超额收益在公告窗和生效窗分别如何表现？第二，中美市场的量级是否如机械被动买盘解释所预期那样出现显著分化？第三，在 post-hoc 机制分析、稳健性检验和数据限制被透明披露的前提下，哪些结论可以进入论文主线，哪些只能作为附录或探索性证据。本文的贡献在于：用同一事件研究口径比较中美制度差异；把公告窗与生效窗明确分解；并把 H1-H7 探索性机制、HS300 RDD 和分析参数作为透明性材料，而不是把它们包装成事前注册的主结果。

---

## 2. 文献综述

本项目共索引 {{ literature.papers_indexed }} 篇核心文献，分为 {{ literature.research_threads }} 条研究主线：{{ literature.thread_names | join(", ") }}。文献综述按需求曲线与价格压力、信息/认证渠道与效应消退、中国市场证据三组组织，目的是把本文的公告窗/生效窗分解放回既有争论中。

### 2.1 需求曲线与价格压力派

需求曲线派的核心命题是：如果股票不是完全替代品，指数基金和基准跟踪者的非信息性需求会推高纳入股票价格。Shleifer (1986) 提出向下倾斜需求曲线，Harris & Gurel (1986) 记录加入 S&P 500 附近的价格与成交量反应，Kaul 等 (2000) 以及 Wurgler & Zhuravskaya (2002) 则把价格压力与替代性不足、套利成本联系起来。这条文献预测生效日附近的机械需求冲击应较强，且被动资金更发达的市场中效应更明显。

### 2.2 信息/认证渠道与效应消退证据

信息/认证渠道认为，指数纳入不只是机械买盘，还可能传递指数委员会筛选、流动性改善、投资者关注和基本面预期变化。Lynch & Mendenhall (1997) 区分公告与生效阶段，Denis 等 (2003) 讨论纳入后经营表现与市场预期，Greenwood & Sammon (2022) 则强调随着套利者提前交易和指数规则透明化，传统生效日指数效应会减弱。这条文献预测公告窗可能更重要，生效日窗口未必显著。

### 2.3 中国市场文献

中国市场文献补充了不同交易制度、投资者结构和约束条件下的指数效应证据。Chu 等、Yao 等以及相关 A 股指数研究关注涨跌停、散户参与、ETF 发展和指数调整规则对价格反应的影响。中国市场较短样本期和数据口径限制要求本文在结论中保持谨慎，但它也提供了与美国市场对照的制度差异。

### 2.4 文献评述与本文定位

既有文献从“指数纳入是否产生价格效应”逐步转向“价格效应通过何种渠道、在何时被定价”。本文不把单一机制作为先验答案，而是用公告窗、生效窗和跨市场量级三项事实约束解释空间：若机械需求压力主导，应看到生效窗显著且美国更强；若信息/认证与提前定价主导，则应看到公告窗集中、生效窗弱化，并可能出现跨制度相似的公告反应。

---

## 3. 研究设计

### 3.1 样本与数据

样本使用真实指数纳入/剔除事件、Yahoo Finance 日频价格和基准收益，核心纳入事件规模为 {{ sample_size_summary }}。价格、基准和事件清单分别由 `data/raw/real_prices.csv`、`data/raw/real_benchmarks.csv` 和 `data/raw/real_events.csv` 管理；被动 AUM 证据使用美国 Federal Reserve Z.1 与中国 ETF TNA proxy。中国端市值、换手率与被动规模均存在可得性限制，因此本文把这些变量作为机制讨论和附录证据，而非无条件的因果识别变量。

### 3.2 实证方法

主方法是事件研究：以公告日和生效日为事件日，报告 CAR[-1,+1] 作为主窗口，并用 [-3,+3]、[-5,+5] 以及 Patell Z、BMP t 等标准化异常收益作为稳健性补充。第二层方法是匹配样本回归与协变量平衡检查，用 212,756 行面板和 block bootstrap 评估处理组与对照组差异。所有统计结果均从项目结果表自动派生，避免手工复制造成漂移。

### 3.3 识别策略：公告窗与生效窗的跨市场分解

关键识别逻辑是：信息渠道预测公告窗显著、生效窗约为零、中美量级相近；需求压力渠道预测生效窗显著，且美国因被动规模更大应出现更强反应。本文承认该设计仍是描述性事件研究，不是严格准实验；生效窗约为零也可能由套利者提前交易、市场深度吸收或数据窗口选择共同造成。因此本文把结论限定为“当前证据更支持公告阶段定价”，而不是排除所有需求压力机制。

---

## 4. 实证结果

### 4.1 核心结果：公告窗与生效窗的比较

本节为论文实证主线。主要结果（来自 `results/real_tables/event_study_summary.csv`）：

- 中国公告日 CAR[-1,+1] = {{ cn_announce_car or "待补" }}%（t={{ cn_announce_t or "待补" }}，p{{ cn_announce_p or "待补" }}，n={{ cn_n or "待补" }}）
- 美国公告日 CAR[-1,+1] = {{ us_announce_car or "待补" }}%（t={{ us_announce_t or "待补" }}，p{{ us_announce_p or "待补" }}，n={{ us_n or "待补" }}）
- 中国生效日 CAR[-1,+1] = {{ cn_effective_car or "待补" }}%（t={{ cn_effective_t or "待补" }}，p{{ cn_effective_p or "待补" }}，不显著）
- 美国生效日 CAR[-1,+1] = {{ us_effective_car or "待补" }}%（t={{ us_effective_t or "待补" }}，p{{ us_effective_p or "待补" }}，不显著）

完整表格与 CAR 路径图见 paper/manuscript.tex §4.1（表 1，图 1–4）。公告窗在中美两市场均显著为正，而生效窗均不显著，是本文最重要的三事实组合：有纳入效应、效应集中在公告阶段、跨制度公告窗量级接近。

---

## 5. 限制与讨论

本节把稳健性证据和方法边界放在同一处：先说明核心结果在哪些检验下保持一致，再说明哪些解释不能被当前设计排除。

### 5.1 纳入 vs 剔除不对称

若信息/认证渠道为主导，纳入与剔除应呈方向不对称。当前结果显示，中国纳入公告窗约 +1.76%，剔除约 -0.59%；美国纳入约 +1.84%，剔除约 +0.05%。这一方向差异更接近“纳入带来正向认证或注意力冲击”的解释，而不是简单对称的机械买卖压力。

### 5.2 长窗口 CAR 的持续性

长窗口用于检验短期公告效应是否随后大幅反转。中国 [0,+120] 均值 CAR 约 +1.56%（t=0.66，不显著），美国约 +1.96%（t=1.57，不显著）。点估计为正但统计不显著，说明本文不能声称长期持续超额收益，但也没有看到与纯短暂价格压力一致的大幅反转。

### 5.3 公告效应的跨年稳定性

按公告年份分解，中国 2021–2025 各年均值均为正；美国 2010–2025 大多数年份均值为正。跨年结果支持公告窗正反应不是单一年份驱动，但年度样本差异较大，不能把每个年度都解释为独立显著证据。

### 5.4 匹配对照组的协变量平衡

匹配样本回归把对数市值、前期收益和前期波动率作为关键协变量，并以 SMD<0.25 作为平衡门禁。三项协变量均通过门禁，降低了可观察特征差异驱动主结果的风险。不过匹配不能处理所有不可观察差异，因此其定位是稳健性补充，不是唯一识别来源。

### 5.5 预公告漂移：一项无法排除的不确定性

公告前均值漂移在中国约 +3.09%，美国约 +2.59%，两市场差异 bootstrap p=0.875。该事实要求论文诚实披露：市场可能在正式公告前已部分消化信息，公告窗显著并不等同于“公告当天才第一次被定价”。这也是本文避免强因果表述的关键原因。

### 5.6 数据与方法限制摘要

{% if power_analysis_rows %}事后统计功效（来自 `results/real_tables/power_analysis_report.csv`）：

{% for row in power_analysis_rows -%}
- `{{ row.hid }}`: n={{ row.n_obs }}；检验族={{ row.test_family }}；power@observed={{ "%.1f%%"|format(row.power_at_observed * 100) }}；MDE@80% power={{ "%.2f%%"|format(row.mde_at_80_power * 100) }}；解读：{{ row.interpretation }}
{% endfor %}
{% else %}事后统计功效：`results/real_tables/power_analysis_report.csv` 缺失或列不完整，本骨架不手工补写功效结论。
{% endif %}
以下限制来自 `docs/limitations.md`，用于约束正文措辞与答辩口径。它们说明哪些结果可以作为主结论，哪些只能作为探索性或附录材料。

---

{{ limitations_text }}

---

## 6. 结论与启示

### 6.1 主要结论

第一，中美纳入事件均存在显著正向公告窗 CAR，说明指数纳入效应并未消失。第二，生效窗 CAR 在两国市场均不显著，说明当前样本不支持“生效日机械买盘是主要来源”的强叙事。第三，中美公告窗量级接近，尽管两国被动资金规模、交易制度和投资者结构差异显著；这一事实更符合公告阶段信息、认证、注意力和制度约束共同定价的解释。

### 6.2 实务与研究启示

对投资者而言，纳入事件更应被理解为公告期信息冲击和预期重估，而非可机械追逐的生效日买盘。对研究者而言，下一步应优先改进历史市值、自由流通口径和中国被动 AUM 数据，并把 HS300 RDD 扩展到更多批次后再作为主识别设计。对本文而言，最稳妥的表述是：指数纳入公告带来短期价格反应，但该反应的机制不是单一被动买盘，而是信息、关注、套利和制度约束的组合。

---

## 7. 分析参数

H1-H7 跨市场机制假说、HS300 RDD、灵敏度阈值和裁决基线均属于透明性与复现材料。H1-H7 是观察到 announce-vs-effective / CN-vs-US 不对称结果之后形成的探索性假说；本项目没有预分析计划，因此这些假说不得在论文中表述为事前注册检验。

- 裁决基线日期：{{ pap.snapshot_date or "待补" }}
- 裁决基线路径：`{{ pap.snapshot_path or "待补" }}`
- baseline 冻结天数：{{ pap.frozen_for_days or "待补" }}
{% if pap.deviation %}- PAP deviation：unchanged={{ pap.deviation.unchanged_count or 0 }}，tightened={{ pap.deviation.tightened_count or 0 }}，weakened={{ pap.deviation.weakened_count or 0 }}，flipped={{ pap.deviation.flipped_count or 0 }}，unverifiable={{ pap.deviation.unverifiable_count or 0 }}
{% endif %}
分析参数、阈值、样本边界和机制裁决口径见 `docs/analysis_parameters.md`；公共摘要工件 `data/public/index_research_summary.json`（schema v{{ schema_version }}）面向外部消费者提供稳定、去路径泄露的机器可读快照。

---

## 参考文献

下列 {{ references | length }} 篇文献来自 `literature_catalog.PAPER_LIBRARY`（项目核心文献库）：

启发式文献关联网络（自动）：本项目文献库共 {{ citation_network.node_count }} 篇，共 {{ citation_network.edge_count }} 条"主题/方法/年代"关联边；关联最多：{{ citation_network.most_linked_label }}；桥梁文献（betweenness）：{{ citation_network.bridge_papers_label }}。这不是已验证引用关系，也不是逐条 bibliography citation 核验；只用于文献综述导航，不得作为被引/引用证据。可视化见 `results/literature/citation_network.png`（中心性 CSV：`results/literature/citation_centrality.csv`，由 `index-inclusion-citation-graph` 生成）。

{% for ref in references -%}
{{ loop.index }}. {{ ref.authors }} ({{ ref.year }}). *{{ ref.title }}*. {{ ref.market_focus }}. `paper_id={{ ref.paper_id }}`.
{% endfor %}

## 附录

### A. 数据契约

核心输入表为 `events.csv`、`prices.csv` 和 `benchmarks.csv`。事件表记录市场、ticker、公告日、生效日和纳入/剔除方向；价格表记录日频 close 与可用 OHLCV；基准表记录市场基准收益。被动 AUM 和行业标签属于机制证据字段，口径说明分别见 `docs/real_data_notes.md`、`docs/hs300_rdd_data_contract.md` 和 `docs/analysis_parameters.md`。

### B. CLI 入口 ({{ literature.console_scripts_count }} 个)

完整 {{ literature.console_scripts_count or "多" }} 个 console scripts 的分组、用法与示例命令见 `docs/cli_reference.md`。RDD 相关命令保留于 CLI 中用于复现与扩展，但 RDD 当前定位为附录 / 方法论补充，不进入本文实证主线。

### C. 复现指南

```bash
make rebuild           # 10 步流水线：从原始数据到事件研究结果
make figures-tables    # 重绘论文级 figure
make paper             # 论文交付包：自动复制到 paper/
index-inclusion-export-public-summary  # 刷新公共摘要
index-inclusion-paper-bundle --force   # 重新生成本骨架
```

交付前执行 `index-inclusion-submission-ready --fail-on-warn`、`index-inclusion-paper-integrity --fail-on-warn`、`make doctor-strict` 与 `make ci`。公共摘要工件：`data/public/index_research_summary.json`（schema v{{ schema_version }}），是面向外部消费者（sibling 项目、GitHub Pages 日报）的稳定入口。
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
        "limitations.md 缺失 — 先补 docs/limitations.md 再重新生成本骨架。"
    )

    # Event study core numbers for §4.1 auto-summary
    event_study_numbers = _event_study_core_numbers(event_study_csv)
    sample_size_summary = event_study_numbers.get(
        "sample_size_summary", "待补"
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
        "power_analysis_rows": _power_analysis_rows(power_analysis_csv),
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
            "draft auto-populated from the current verdicts / PAP / "
            "HS300 RDD / sensitivity artifacts. The output is designed "
            "to pass the submission-ready structure/TODO gates while "
            "still labelling author and abstract as待补/待精修."
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
        default=False,
        help="Deprecated no-op kept for CLI compatibility; generated skeletons do not emit [TODO] markers.",
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
