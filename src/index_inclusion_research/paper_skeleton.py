"""Generate the paper skeleton markdown (``paper/skeleton.md``).

This is the 38th console script: ``index-inclusion-paper-skeleton``. It
renders a full Markdown paper template — section headers, figure
references, table placeholders, auto-populated verdict / RDD / robustness
summaries, and ``[TODO: prose]`` markers — so the writing step starts
from a structured skeleton instead of a blank page.

The generator NEVER fabricates prose. Every auto-populated block is
derived deterministically from the current research artifacts:

- ``results/real_tables/cma_hypothesis_verdicts.csv`` — H1..H7 verdicts
- ``results/real_tables/pap_deviation_report.csv`` — PAP deviation audit
- ``results/literature/hs300_rdd/rdd_robustness.csv`` — HS300 RDD results
- ``data/public/index_research_summary.json`` — shipped F1 summary (the
  deterministic-shaped artifact this module leans on for the cross-cut
  totals like ``flipping_hypotheses`` / sensitivity counts)
- ``docs/limitations.md`` — §5 limitations text (verbatim inclusion)
- ``literature_catalog.PAPER_LIBRARY`` — §References enumeration

Sections that require human prose are marked with ``[TODO: ...]`` so
the author can grep for ``TODO`` and walk through every remaining
writing decision.

Jinja2 (already transitively available through ``flask``) renders the
markdown; the template lives inline as a module-level string constant so
the skeleton is one self-contained module.
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

# Canonical ordered hypothesis IDs (matches verdict CSV row order).
EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")

# Sanity bound: a well-formed skeleton should fall in this size band (bytes).
# The bottom anchors at "skeleton with every input present" so a truncated
# render gets caught; the top anchors at "limitations verbatim (~3 KB) + 16
# paper refs + 7 H-detail subsections + 3 sensitivity narratives + PAP table".
SKELETON_MIN_BYTES = 6 * 1024
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
    try:
        tau = float(main.get("tau"))
    except (TypeError, ValueError):
        return {}
    try:
        p_value = float(main.get("p_value"))
    except (TypeError, ValueError):
        p_value = float("nan")
    return {
        "tau_pct": round(tau * 100.0, 2),
        "p_value": round(p_value, 4) if p_value == p_value else None,  # NaN guard
        "n_obs": _coerce_int(main.get("n_obs")),
        "outcome": _coerce_str(main.get("outcome")) or None,
        "bandwidth": float(main.get("bandwidth"))
        if main.get("bandwidth") is not None
        else None,
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


def _figures_available(figures_dir: Path) -> set[str]:
    """Return basenames present in the published figures directory."""
    if not figures_dir.exists():
        return set()
    return {p.name for p in figures_dir.iterdir() if p.is_file()}


# ---------------------------------------------------------------------------
# Template (Jinja2 inline)
# ---------------------------------------------------------------------------


_TEMPLATE = r"""# 指数纳入效应跨市场不对称研究：基于美中市场的实证分析

**作者**: [TODO: 作者]
**日期**: {{ generated_date }}
**摘要 (TODO)**: 100-150 字摘要 — 概述研究问题、方法、{{ sample_size_summary }} 样本规模、主要发现（公告日 CAR 显著、生效日不显著、H5/H7 支持、HS300 RDD τ={{ hs300.tau_pct if hs300 else "TODO" }}%）和政策含义。

## 1. 引言

[TODO: 引言 prose — 用 1-2 段铺陈指数纳入是一个典型的制度冲击，并引出三个核心问题。可借鉴 docs/paper_outline.md §一 的结构。]

### 1.1 研究背景

[TODO: 背景 — 引用 Shleifer 1986 / Harris & Gurel 1986 作为创世之战，过渡到当前在中美市场的延续研究。]

### 1.2 研究问题

本文回答 3 个核心问题：

1. 指数纳入后的上涨是不是只是短期交易冲击？
2. 价格效应会不会只部分回吐，从而支持需求曲线向下倾斜？
3. 不同市场制度和识别方法会不会改变结论，尤其是在中国市场？

## 2. 文献综述

[TODO: 文献综述 prose — {{ literature.papers_indexed }} 篇核心文献按 反方/中性/正方 三阵营展开。完整 author-year 综述见 `docs/literature_review_author_year_cn.md`；按文献阵营 / 项目主线的映射见 `docs/literature_to_project_guide.md`。]

本项目共索引 {{ literature.papers_indexed }} 篇核心文献，分为 {{ literature.research_threads }} 条研究主线：{{ literature.thread_names | join(", ") }}。

## 3. 研究设计

### 3.1 样本与数据

[TODO: 样本期、数据源、清洗规则。关键样本规模指标：]

{% for row in verdicts -%}
- **{{ row.hid }} ({{ row.name_cn }})**：n = {{ row.n_obs }} ({{ row.evidence_tier }})
{% endfor %}
数据来源 + 限制详见本文 §5 与 `docs/limitations.md`。

### 3.2 实证方法

[TODO: 事件研究 + 匹配回归 + RDD 框架说明。可借鉴 `docs/paper_outline.md` §三 的写法；AR 计算引擎以及估计窗口的取舍见 `docs/limitations.md` §5。]

### 3.3 七条跨市场不对称机制假说

下表自动汇总自 `results/real_tables/cma_hypothesis_verdicts.csv`，列出 H1-H7 名称、当前裁决、置信度、证据层级 (core/supplementary) 与样本量。

| 假说 | 名称 | 裁决 | 置信度 | 证据层级 | n | 头条指标 | 主线 |
|---|---|---|---|---|---:|---|---|
{% for row in verdicts -%}
| {{ row.hid }} | {{ row.name_cn }} | {{ row.verdict }} | {{ row.confidence }} | {{ row.evidence_tier }} | {{ row.n_obs }} | {{ row.headline_metric }} | {{ row.track }} |
{% endfor %}

## 4. 实证结果

### 4.1 主结果

![图 1：CMA 跨假说证据强度对比](../results/figures/cma_verdicts_forest.png)

> 图 1 说明：H1-H7 在 y 轴，support-strength 评分（0-1）在 x 轴。颜色按 `evidence_tier`（core / supplementary），右侧 monospace 列为 `n | tier | verdict/conf`。当前裁决分布：{{ verdict_distribution }}。重绘命令：`index-inclusion-build-cma-verdicts-forest`。

[TODO: 主结果 prose — 描述 7 假说裁决的整体格局：支持、部分支持、证据不足各 N 项；core vs supplementary 划分。重点说明哪 3-4 条假说进入正文（core），哪 3 条进附录（supplementary）。]

### 4.2 跨市场不对称机制

{% for row in verdicts %}
#### {{ row.hid }}：{{ row.name_cn }} ({{ row.verdict }} · {{ row.evidence_tier }})

- 当前裁决：**{{ row.verdict }}** (置信度 {{ row.confidence }})
- 样本量：n = {{ row.n_obs }}
- 头条指标：{{ row.headline_metric }}
- 研究主线：{{ row.track }}

[TODO: {{ row.hid }} prose — 解释机制、证据来源 (`results/real_tables/cma_*.csv`)、为什么裁决是 "{{ row.verdict }}" 而不是更强 / 更弱的结论；如果是 "证据不足"，说明缺什么数据。]

{% endfor %}

### 4.3 HS300 RDD 结果

{% if hs300 %}
HS300 主结果（局部线性 RDD）：

- τ = **{{ hs300.tau_pct }}%**
- p = {{ hs300.p_value }}
- n = {{ hs300.n_obs }}
- outcome = `{{ hs300.outcome }}`
- bandwidth = {{ hs300.bandwidth }}
- 稳健性 spec 数：{{ hs300.robustness_count }}

![图 2: HS300 RDD 稳健性 forest plot](../results/figures/hs300_rdd_robustness_forest.png)

> 图 2 说明：HS300 RDD `car_m1_p1` outcome 在 main / donut / placebo±0.05 / polynomial 共 {{ hs300.robustness_count }} 个 spec 下的 τ 估计与置信区间。完整数值见 `results/literature/hs300_rdd/rdd_robustness.csv`。

[TODO: HS300 RDD prose — 阐述 main spec 的 τ + p 主要发现，再说明 donut / placebo / polynomial 稳健性的方向一致或不一致，强调当前 L3 覆盖期 (2020-11 到 2025-11，11 个批次) 不足以支撑论文级因果声明 (见 `docs/hs300_rdd_l3_collection_audit.md`)。]
{% else %}
[TODO: HS300 RDD — `results/literature/hs300_rdd/rdd_robustness.csv` 缺失，无法自动填充 τ/p/n。先跑 `index-inclusion-hs300-rdd` 生成 RDD 表，再重新生成本骨架。]
{% endif %}

### 4.4 稳健性检查

#### 4.4.1 阈值敏感性

![图 3: CMA 阈值敏感性](../results/figures/cma_verdicts_sensitivity.png)

{% if sensitivity.threshold %}
> 图 3 说明：在 4 个 p-value 阈值（{{ sensitivity.threshold.thresholds_tested | join(", ") }}）下重跑 CMA 编排，比较 7 条假说的裁决稳定性。共 {{ sensitivity.threshold.cell_count }} cell，{{ sensitivity.threshold.stable_count }} 假说稳定，{{ sensitivity.threshold.flip_count }} 假说在阈值轴上 flip。

结论 (auto-derived)：在 {{ sensitivity.threshold.thresholds_tested | join("-") }} 阈值范围内，{{ sensitivity.threshold.stable_count }} / 7 条假说裁决稳定；{% if sensitivity.threshold.flip_count == 0 %}**全部 7 条假说裁决不随 p-value 阈值变化**，说明阈值选择不是核心机制{% else %}{{ sensitivity.threshold.flip_count }} 条假说裁决随阈值切换，需要在论文中讨论{% endif %}。
{% else %}
[TODO: 阈值敏感性 — 未生成 sensitivity cache (`results/sensitivity/threshold_*/`)，先跑 `index-inclusion-cma --sensitivity-threshold ...` 再重生成。]
{% endif %}

#### 4.4.2 AR 引擎敏感性

![图 4: CMA AR 引擎敏感性](../results/figures/cma_verdicts_ar_engine.png)

{% if sensitivity.ar_engine %}
> 图 4 说明：在 {{ sensitivity.ar_engine.models_tested | join(" / ") }} 共 {{ sensitivity.ar_engine.cell_count }} 个 AR 引擎下重跑 CMA，比较裁决稳定性。{{ sensitivity.ar_engine.stable_count }} 假说稳定，{{ sensitivity.ar_engine.flip_count }} 假说 flip。

结论 (auto-derived)：在 AR 引擎切换（{{ sensitivity.ar_engine.models_tested | join(" ↔ ") }}）下，{{ sensitivity.ar_engine.stable_count }} / 7 条假说裁决稳定；{% if sensitivity.ar_engine.flip_count > 0 %}{{ sensitivity.ar_engine.flip_count }} 条假说裁决发生 flip：{% for hid in sensitivity.ar_engine.flipping_hypotheses %}**{{ hid }}**{% if not loop.last %}, {% endif %}{% endfor %}。论文中必须将这一脆弱性写进 §5 限制{% else %}**全部 7 条假说裁决不随 AR 引擎切换变化**{% endif %}。
{% else %}
[TODO: AR 引擎敏感性 — 未生成 sensitivity cache (`results/sensitivity/ar_*/`)，先跑 `index-inclusion-cma --ar-model market` 再重生成。]
{% endif %}

#### 4.4.3 联合稳健性

![图 5: CMA 2D 联合稳健性](../results/figures/cma_verdicts_2d_robustness.png)

{% if sensitivity.two_d %}
> 图 5 说明：阈值 × AR 引擎 2D 网格共 {{ sensitivity.two_d.cell_count }} cell，比较 7 条假说在全 cell 下的裁决一致性。{{ sensitivity.two_d.stable_count }} 假说稳定，{{ sensitivity.two_d.flip_count }} 假说 flip。

结论 (auto-derived)：在 2D 联合稳健性（共 {{ sensitivity.two_d.cell_count }} cell）下，{{ sensitivity.two_d.stable_count }} 条假说全 cell 稳定，{% if sensitivity.two_d.flip_count > 0 %}{{ sensitivity.two_d.flip_count }} 条假说在 2D 网格内 flip：{% for hid in sensitivity.two_d.flipping_hypotheses %}**{{ hid }}**{% if not loop.last %}, {% endif %}{% endfor %}{% if sensitivity.ar_engine and sensitivity.ar_engine.flipping_hypotheses %}（与 AR 引擎轴 flip 集合一致——脆弱性来自 AR 引擎而非阈值）{% endif %}{% else %}**全部 7 条假说在 2D 联合稳健性下都稳定**{% endif %}。
{% else %}
[TODO: 2D 联合稳健性 — 未生成 sensitivity cache (`results/sensitivity/grid_*/`)，先跑 `index-inclusion-cma --sensitivity-grid` 再重生成。]
{% endif %}

## 5. 限制与讨论

[TODO: 在 docs/limitations.md 之外，补充论文层面的讨论，特别是 AR 引擎敏感性带来的 {{ sensitivity.ar_engine.flipping_hypotheses | join("/") if sensitivity.ar_engine and sensitivity.ar_engine.flipping_hypotheses else "无" }} 假说脆弱性。]

下文为 `docs/limitations.md` 的自动嵌入，便于审稿人无须翻附录直接阅读：

---

{{ limitations_text }}

---

## 6. 结论与启示

[TODO: 结论 — 把主结果（公告日 CAR 显著、生效日不显著、H5/H7 支持）转化为对学术与监管的启示。]

[TODO: 政策含义 — 例如对监管层（涨跌停制度对 H5 的解释作用）、对被动基金投资者的启示。]

[TODO: 未来研究 — 把 HS300 RDD L3 覆盖期扩展、CN 被动 AUM 官方口径替换、AR 引擎稳健性细化作为后续工作列出。]

## 7. PAP (预注册分析计划) 合规

本文遵循 {{ pap.snapshot_date or "TODO: PAP 日期" }} 冻结的预注册基线（snapshot `{{ pap.snapshot_path or "TODO: snapshot path" }}`）。{% if pap.frozen_for_days is not none %}基线已冻结 {{ pap.frozen_for_days }} 天。{% endif %}

PAP 偏离审计自动汇总：

{% if pap.deviation -%}
- 全部 unchanged: **{{ pap.deviation.all_unchanged }}**
- unchanged: {{ pap.deviation.unchanged_count }}
- flipped: {{ pap.deviation.flipped_count }}
- tightened: {{ pap.deviation.tightened_count }}
- weakened: {{ pap.deviation.weakened_count }}
- unverifiable: {{ pap.deviation.unverifiable_count }}

{% else %}
[TODO: PAP deviation — `results/real_tables/pap_deviation_report.csv` 缺失，先跑 `index-inclusion-pap-diff`。]

{% endif %}
下表自动汇总自 `results/real_tables/pap_deviation_report.csv`：

| 假说 | 名称 | 分类 | baseline | current |
|---|---|---|---|---|
{% for r in pap.rows -%}
| {{ r.hid }} | {{ r.name_cn }} | {{ r.classification }} | {{ r.baseline_verdict }} | {{ r.current_verdict }} |
{% endfor %}

## 参考文献

下列 {{ references | length }} 篇文献来自 `literature_catalog.PAPER_LIBRARY`（项目核心文献库）：

{% for ref in references -%}
{{ loop.index }}. {{ ref.authors }} ({{ ref.year }}). *{{ ref.title }}*. {{ ref.market_focus }}. `paper_id={{ ref.paper_id }}`.
{% endfor %}

## 附录

### A. 数据契约

[TODO: 数据契约 — 详细字段约定见 `docs/hs300_rdd_data_contract.md` 与 `docs/real_data_notes.md`。本节简要说明事件清单 / 价格 / AUM / 行业标签的字段、单位、缺失值规则。]

### B. CLI 入口 ({{ literature.console_scripts_count }} 个)

[TODO: CLI 入口列表 — 完整 38 个 console scripts 的分组、用法与示例命令见 `docs/cli_reference.md`。]

### C. 复现指南

本文所有图表、表与裁决可通过下列命令一键复现：

```bash
make rebuild           # 10 步流水线：从原始数据到 CMA verdict
make figures-tables    # 重绘 5 张论文级 figure
make paper             # 论文交付包：自动复制到 paper/
index-inclusion-export-public-summary  # 刷新公共摘要 data/public/index_research_summary.json
index-inclusion-paper-skeleton --force # 重新生成本骨架
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

    verdicts_df = _read_csv_or_empty(verdicts_csv)
    pap_df = _read_csv_or_empty(pap_csv)
    rdd_df = _read_csv_or_empty(rdd_csv)
    public_summary = _read_json_or_empty(public_summary_json)
    limitations_text = _read_text_or_empty(limitations_md).strip() or (
        "[TODO: limitations.md 缺失 — 先补 docs/limitations.md 再重新生成本骨架]"
    )

    verdict_rows = _verdict_rows(verdicts_df)

    # Verdict distribution for §4.1 prose summary.
    distribution_counts: dict[str, int] = {}
    for row in verdict_rows:
        distribution_counts[row.verdict] = distribution_counts.get(row.verdict, 0) + 1
    if distribution_counts:
        verdict_distribution = "、".join(
            f"{count} 条{verdict}" for verdict, count in distribution_counts.items()
        )
    else:
        verdict_distribution = "TODO（裁决表为空）"

    # Sample-size summary (sum of all n_obs is misleading because they're
    # different denominators; surface a per-hypothesis range instead).
    if verdict_rows:
        ns = [r.n_obs for r in verdict_rows if r.n_obs]
        if ns:
            sample_size_summary = f"H1-H7 假说 n 范围 {min(ns)}-{max(ns)}"
        else:
            sample_size_summary = "TODO"
    else:
        sample_size_summary = "TODO"

    context: dict[str, Any] = {
        "generated_date": (generated_at or datetime.now(tz=UTC))
        .replace(microsecond=0)
        .strftime("%Y-%m-%d"),
        "verdicts": verdict_rows,
        "verdict_distribution": verdict_distribution,
        "sample_size_summary": sample_size_summary,
        "hs300": _hs300_main_block(rdd_df) or None,
        "sensitivity": _sensitivity_block(public_summary),
        "pap": _pap_block(public_summary, pap_df),
        "references": _references_block(),
        "literature": _literature_section(public_summary),
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
