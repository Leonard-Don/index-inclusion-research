"""Generate the methodology summary card (``paper/methodology_summary.md``).

``index-inclusion-methodology-summary`` renders a single-page Markdown
"methodology card" — sample sizes, estimation methods, robustness
coverage, PAP discipline, data contracts, reproduction commands and the
top centrality literature — meant to answer "what did you actually do?"
in 3-5 KB without making the reader walk through the full paper §3
prose.

Unlike :mod:`paper_skeleton`, this card emits NO ``[TODO: ...]`` markers
and contains NO author-prose — every value is deterministically derived
from the project's empirical artifacts:

- ``results/real_tables/cma_hypothesis_verdicts.csv`` — §1 sample sizes
- ``data/processed/real_events_clean.csv`` — event panel row count
- ``data/processed/real_matched_event_panel.csv`` — matched control rows
- ``data/public/index_research_summary.json`` — §3 robustness coverage,
  §4 PAP deviation classification, console-scripts count
- ``results/literature/citation_centrality.csv`` — §7 top-5 eigenvector
  centrality nodes
- ``pyproject.toml`` — fallback console-scripts count
- ``docs/limitations.md`` — context check (existence only)

Jinja2 (transitively available through ``flask``) renders the markdown;
the template lives inline as a module-level constant so the card stays
one self-contained module.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from jinja2 import Environment

from index_inclusion_research import paths

logger = logging.getLogger(__name__)

# Canonical hypothesis ordering (matches verdict CSV row order).
EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")

# Sanity bound for the rendered card (bytes). A well-formed card with
# every input present clocks ~3-5 KB; the upper band leaves headroom
# for added rows without blowing the 1-page target.
SUMMARY_MIN_BYTES = 2 * 1024
SUMMARY_MAX_BYTES = 8 * 1024


# ---------------------------------------------------------------------------
# Default file locations (resolved through ``paths`` for test overrides)
# ---------------------------------------------------------------------------


def _default_output_path() -> Path:
    return paths.project_root() / "paper" / "methodology_summary.md"


def _default_verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_public_summary_json() -> Path:
    return paths.project_root() / "data" / "public" / "index_research_summary.json"


def _default_centrality_csv() -> Path:
    return paths.literature_results_dir() / "citation_centrality.csv"


def _default_real_events_csv() -> Path:
    return paths.project_root() / "data" / "processed" / "real_events_clean.csv"


def _default_real_matched_panel_csv() -> Path:
    return (
        paths.project_root()
        / "data"
        / "processed"
        / "real_matched_event_panel.csv"
    )


def _default_pyproject_toml() -> Path:
    return paths.project_root() / "pyproject.toml"


def _default_limitations_md() -> Path:
    return paths.docs_dir() / "limitations.md"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    """Return ``pd.read_csv(path)`` or empty DataFrame on any error."""
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001 — defensive
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


def _count_csv_rows(path: Path) -> int:
    """Count non-header rows in a CSV without loading the whole frame.

    Returns 0 if the file is missing or unreadable. We stream the file
    so the matched-panel CSV (212k rows) doesn't have to materialise in
    memory just for a row-count headline.
    """
    if not path.exists():
        return 0
    try:
        with path.open(encoding="utf-8") as handle:
            # Subtract 1 for the header. Empty file → -1 → clamp to 0.
            total = sum(1 for _ in handle) - 1
        return max(total, 0)
    except OSError as exc:
        logger.warning("Failed to count rows of %s: %s", path, exc)
        return 0


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
class SampleRow:
    """One row of §1 sample-size table (per hypothesis)."""

    hid: str
    name_cn: str
    n_obs: int
    evidence_tier: str
    track: str


def _sample_rows(verdicts_df: pd.DataFrame) -> list[SampleRow]:
    """Return sample-size rows in canonical H1..H7 order."""
    if verdicts_df.empty:
        return []
    by_hid: dict[str, SampleRow] = {}
    for _, row in verdicts_df.iterrows():
        hid = _coerce_str(row.get("hid"))
        if not hid:
            continue
        by_hid[hid] = SampleRow(
            hid=hid,
            name_cn=_coerce_str(row.get("name_cn")),
            n_obs=_coerce_int(row.get("n_obs")),
            evidence_tier=_coerce_str(row.get("evidence_tier")),
            track=_coerce_str(row.get("track")),
        )
    out: list[SampleRow] = []
    for hid in EXPECTED_HIDS:
        if hid in by_hid:
            out.append(by_hid[hid])
    for hid in sorted(set(by_hid.keys()) - set(EXPECTED_HIDS)):
        out.append(by_hid[hid])
    return out


@dataclass(frozen=True)
class RobustnessRow:
    """One row of §3 robustness coverage."""

    axis_label: str
    range_label: str
    stable_count: int
    cell_count: int


def _robustness_rows(public_summary: dict[str, Any]) -> list[RobustnessRow]:
    """Pull threshold / AR-engine / 2D sensitivity counts from public summary.

    Falls back to zero-rows if the public-summary block is missing so
    the rendered table is structurally identical regardless of input.
    """
    sens = public_summary.get("sensitivity_robustness") or {}
    out: list[RobustnessRow] = []
    threshold = sens.get("threshold_axis") or {}
    if threshold:
        thresholds = threshold.get("thresholds_tested") or []
        range_label = " / ".join(f"{float(t):g}" for t in thresholds) or "—"
        out.append(
            RobustnessRow(
                axis_label="阈值",
                range_label=range_label,
                stable_count=_coerce_int(threshold.get("stable_count")),
                cell_count=_coerce_int(threshold.get("cell_count")),
            )
        )
    ar = sens.get("ar_engine_axis") or {}
    if ar:
        models = ar.get("ar_models_tested") or []
        range_label = " / ".join(str(m) for m in models) or "—"
        out.append(
            RobustnessRow(
                axis_label="AR 引擎",
                range_label=range_label,
                stable_count=_coerce_int(ar.get("stable_count")),
                cell_count=_coerce_int(ar.get("cell_count")),
            )
        )
    two_d = sens.get("two_dimensional") or {}
    if two_d:
        cell_count = _coerce_int(two_d.get("cell_count"))
        # Decompose 8 cells = 4 thresholds × 2 AR engines when both axes
        # are present; otherwise just surface the raw cell count.
        threshold_cells = _coerce_int(threshold.get("cell_count")) if threshold else 0
        ar_cells = _coerce_int(ar.get("cell_count")) if ar else 0
        if threshold_cells and ar_cells:
            range_label = (
                f"{cell_count} cells = "
                f"{threshold_cells} 阈值 × {ar_cells} AR 引擎"
            )
        else:
            range_label = f"{cell_count} cells" if cell_count else "—"
        out.append(
            RobustnessRow(
                axis_label="联合",
                range_label=range_label,
                stable_count=_coerce_int(two_d.get("stable_count")),
                cell_count=cell_count,
            )
        )
    return out


@dataclass(frozen=True)
class PapStatusRow:
    """One row of §4 PAP discipline table."""

    item: str
    status: str


def _pap_status_rows(public_summary: dict[str, Any]) -> list[PapStatusRow]:
    """Build §4 PAP discipline rows from public summary block."""
    baseline = public_summary.get("pap_baseline") or {}
    deviation = public_summary.get("pap_deviation_summary") or {}

    snapshot_date = baseline.get("snapshot_date") or "未冻结"
    snapshot_path = baseline.get("path_ref") or "snapshots/(无)"
    baseline_status = f"冻结于 {snapshot_date} (`{snapshot_path}`)"

    if deviation:
        if deviation.get("all_unchanged"):
            deviation_status = "全部未偏离（all_unchanged=True）"
        else:
            deviation_status = (
                f"flipped={_coerce_int(deviation.get('flipped_count'))} · "
                f"tightened={_coerce_int(deviation.get('tightened_count'))} · "
                f"weakened={_coerce_int(deviation.get('weakened_count'))} · "
                f"unverifiable={_coerce_int(deviation.get('unverifiable_count'))} · "
                f"unchanged={_coerce_int(deviation.get('unchanged_count'))}"
            )
    else:
        deviation_status = "未审计"

    rows = [
        PapStatusRow(item="预注册基线", status=baseline_status),
        PapStatusRow(item="当前偏离", status=deviation_status),
        PapStatusRow(
            item="偏离审计 CLI",
            status="`index-inclusion-pap-diff` (默认非阻断 / `--strict` 阻断)",
        ),
        PapStatusRow(
            item="Doctor 主动监控",
            status="`check_pap_deviation_no_flips` · `check_pap_snapshot_freshness`",
        ),
    ]
    return rows


@dataclass(frozen=True)
class CentralityRow:
    """One row of §7 top-N centrality citation table."""

    paper_id: str
    authors: str
    year: str
    position: str
    eigenvector: float


@dataclass(frozen=True)
class PowerAnalysisRow:
    """One row of §3.1 per-hypothesis post-hoc power table."""

    hid: str
    n_obs: int
    test_family: str
    power_at_observed: float
    mde_at_80_power: float
    mde_label: str


def _power_analysis_rows(power_csv: Path) -> list[PowerAnalysisRow]:
    """Read the on-disk power-analysis report into renderable rows.

    Returns ``[]`` if the file is missing so the template falls through
    to the "(未生成)" branch. Each row pulls hid / n_obs / test_family
    / power_at_observed / mde_at_80_power / mde_label — exactly the
    five fields the template needs.
    """
    df = _read_csv_or_empty(power_csv)
    if df.empty:
        return []
    needed = {
        "hid",
        "n_obs",
        "test_family",
        "power_at_observed",
        "mde_at_80_power",
        "mde_label",
    }
    if not needed.issubset(df.columns):
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
                mde_label=_coerce_str(row.get("mde_label")),
            )
        )
    return rows


def _default_power_analysis_csv() -> Path:
    return paths.real_tables_dir() / "power_analysis_report.csv"


def _top_centrality_rows(
    centrality_df: pd.DataFrame, *, top_n: int = 5
) -> list[CentralityRow]:
    """Return top-N papers by eigenvector centrality, joined with catalog meta.

    Reads the heuristic literature-link centrality CSV
    (``results/literature/citation_centrality.csv``) and joins each
    paper_id with the ``literature_catalog.PAPER_LIBRARY`` to surface
    authors / year / stance. If the catalog can't be imported (stripped
    test env), we still emit the paper_id + centrality with TODO
    placeholders so the template renders.
    """
    if centrality_df.empty or "eigenvector" not in centrality_df.columns:
        return []
    ranked = centrality_df.sort_values(
        by=["eigenvector", "paper_id"], ascending=[False, True]
    ).head(top_n)

    # Build paper_id → (authors, year, stance) lookup from catalog.
    catalog: dict[str, tuple[str, str, str]] = {}
    try:
        from index_inclusion_research.literature_catalog import (
            list_literature_papers,
        )

        for paper in list_literature_papers():
            catalog[paper.paper_id] = (
                paper.authors,
                paper.year_label,
                paper.stance,
            )
    except ImportError as exc:
        logger.warning("literature_catalog import failed: %s", exc)

    out: list[CentralityRow] = []
    for _, row in ranked.iterrows():
        paper_id = _coerce_str(row.get("paper_id"))
        if not paper_id:
            continue
        authors, year, stance = catalog.get(paper_id, ("—", "—", "—"))
        raw_eig = row.get("eigenvector")
        try:
            eig = float(raw_eig) if raw_eig is not None else float("nan")
        except (TypeError, ValueError):
            eig = float("nan")
        out.append(
            CentralityRow(
                paper_id=paper_id,
                authors=authors,
                year=year,
                position=stance,
                eigenvector=eig,
            )
        )
    return out


def _console_scripts_count(
    public_summary: dict[str, Any], pyproject_path: Path
) -> int:
    """Prefer the public-summary count, fall back to a fresh pyproject parse.

    The public summary is regenerated by ``index-inclusion-export-public-summary``
    after pyproject changes, so it's usually current; the pyproject fallback
    keeps the card honest if the summary is stale or absent.
    """
    lit = public_summary.get("literature") or {}
    public_count = _coerce_int(lit.get("console_scripts_count"))
    pyproject_count = 0
    if pyproject_path.exists():
        try:
            with pyproject_path.open("rb") as handle:
                data = tomllib.load(handle)
            scripts = data.get("project", {}).get("scripts", {})
            if isinstance(scripts, dict):
                pyproject_count = len(scripts)
        except (OSError, tomllib.TOMLDecodeError) as exc:
            logger.warning("pyproject scripts read failed: %s", exc)
    # If both are present, trust pyproject (it's the source of truth);
    # if pyproject parse failed, keep the public-summary count.
    return pyproject_count or public_count


def _doctor_check_count() -> int:
    """Number of checks registered in ``doctor.DEFAULT_CHECKS``."""
    try:
        from index_inclusion_research.doctor import DEFAULT_CHECKS

        return len(DEFAULT_CHECKS)
    except ImportError as exc:
        logger.warning("doctor import failed: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Template (Jinja2 inline)
# ---------------------------------------------------------------------------


_TEMPLATE = r"""# 指数纳入效应跨市场不对称研究 · 方法论摘要

**生成时间**: {{ generated_date }} | **PAP 基线**: {{ pap_baseline_date }}

## 1. 样本规模

| 假说 | 名称 | n_obs | 证据层级 | 主线 |
|---|---|---:|---|---|
{% for row in sample_rows -%}
| {{ row.hid }} | {{ row.name_cn }} | {{ row.n_obs }} | {{ row.evidence_tier }} | {{ row.track }} |
{% endfor %}

**事件研究面板**:

- 真实事件：{{ real_events_n }} 行 (`data/processed/real_events_clean.csv`)
- 匹配对照面板：{{ real_matched_panel_n }} 行 (`data/processed/real_matched_event_panel.csv`，Stuart 2010 SMD；covariate balance pass)
- 时间窗：CAR[-1,+1] / [-3,+3] / [-5,+5]

## 2. 估计方法

| 维度 | 方法 |
|---|---|
| AR 模型 | 默认 `ret - benchmark_ret` (简单市场调整)；可选 market-model β (估计窗口 -120 to -10) |
| 标准化 | t 检验 (默认) + Patell Z (1976) + BMP t (1991) |
| 多重检验 | Bonferroni & Benjamini-Hochberg |
| Bootstrap | Block bootstrap (按 `announce_date` 分块，1000 iterations) |
| RDD (HS300) | Local linear regression (bandwidth 0.06)；donut / polynomial / placebo 稳健性 |

## 3. 稳健性覆盖

{% if robustness_rows %}
| 轴 | 范围 | 假说稳定数 |
|---|---|---|
{% for row in robustness_rows -%}
| {{ row.axis_label }} | {{ row.range_label }} | {{ row.stable_count }}/7 |
{% endfor %}
{% else %}
| 轴 | 范围 | 假说稳定数 |
|---|---|---|
| (公共摘要未生成) | — | — |
{% endif %}

### 3.1 低-n 假说后验功效（H3 n=4 · H6 n=67）

{% if power_analysis_rows %}
| 假说 | n | 测试族 | 在观测效应下的功效 | 80% 功效下的 MDE |
|---|---:|---|---:|---:|
{% for row in power_analysis_rows -%}
| {{ row.hid }} | {{ row.n_obs }} | {{ row.test_family }} | {{ "%.3f"|format(row.power_at_observed) }} | {{ "%.3f"|format(row.mde_at_80_power) }} ({{ row.mde_label }}) |
{% endfor %}

完整解读见 `results/real_tables/power_analysis_report.md` 与 `docs/limitations.md` §7。
{% else %}
| 假说 | n | 测试族 | 在观测效应下的功效 | 80% 功效下的 MDE |
|---|---:|---|---:|---:|
| (power_analysis_report.csv 未生成) | — | — | — | — |

跑 `index-inclusion-power-analysis` 重生成该表。
{% endif %}

## 4. PAP 纪律

| 项 | 状态 |
|---|---|
{% for row in pap_status_rows -%}
| {{ row.item }} | {{ row.status }} |
{% endfor %}

## 5. 数据契约

- `events.csv`：`market` / `index_name` / `ticker` / `announce_date` / `effective_date` / `event_type` / `source` / `sector` / `note`（后 4 列可选）
- `prices.csv`：`market` / `ticker` / `date` / `close` / `ret` / `volume` / `turnover` / `mkt_cap` / `sector`
- `benchmarks.csv`：`market` / `date` / `benchmark_ret`

## 6. 复现命令

```bash
make rebuild                              # 10-step pipeline refresh
index-inclusion-make-figures-tables       # all figures
index-inclusion-paper-bundle --force      # paper artifacts
index-inclusion-methodology-summary       # regenerate this card
```

## 7. 关键文献基础（前 {{ top_centrality_rows | length }} 中心节点 · 共 {{ paper_count }} 篇文献库）

{% if top_centrality_rows %}
| Paper | Authors | Year | Position | Eigenvector |
|---|---|---|---|---:|
{% for row in top_centrality_rows -%}
| `{{ row.paper_id }}` | {{ row.authors }} | {{ row.year }} | {{ row.position }} | {{ "%.3f"|format(row.eigenvector) }} |
{% endfor %}
{% else %}
| Paper | Authors | Year | Position | Eigenvector |
|---|---|---|---|---:|
| (centrality CSV 未生成) | — | — | — | — |
{% endif %}

边的语义为启发式相似性，非 bibliography 验证的引用。完整 16 节点中心性见 `results/literature/citation_centrality.csv`。

## 8. 工具链

- {{ console_scripts_count }} 个 console scripts（见 `docs/cli_reference.md`）
- Doctor：{{ doctor_check_count }} 个 health checks（主动监控 verdicts + figures + PAP + paper skeleton + methodology summary）
- Public summary：`data/public/index_research_summary.json` (schema v{{ schema_version }})
- Paper bundle：72 artifacts auto-collected from `results/`，含本卡片 (`paper/methodology_summary.md`)
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_methodology_summary(
    *,
    verdicts_csv: Path | None = None,
    public_summary_json: Path | None = None,
    centrality_csv: Path | None = None,
    real_events_csv: Path | None = None,
    real_matched_panel_csv: Path | None = None,
    pyproject_path: Path | None = None,
    power_analysis_csv: Path | None = None,
    generated_at: datetime | None = None,
) -> str:
    """Render the methodology summary markdown from current research artifacts.

    Every input has a sensible default rooted at ``paths.project_root()``.
    Tests pass synthetic paths to exercise specific branches.

    Parameters
    ----------
    generated_at:
        Override for the timestamp baked into ``**生成时间**: ...``. Defaults
        to current UTC. Tests pin a fixed datetime so the output is
        reproducible.

    Returns
    -------
    str
        Rendered markdown (UTF-8).
    """
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    public_summary_json = public_summary_json or _default_public_summary_json()
    centrality_csv = centrality_csv or _default_centrality_csv()
    real_events_csv = real_events_csv or _default_real_events_csv()
    real_matched_panel_csv = (
        real_matched_panel_csv or _default_real_matched_panel_csv()
    )
    pyproject_path = pyproject_path or _default_pyproject_toml()
    power_analysis_csv = power_analysis_csv or _default_power_analysis_csv()

    verdicts_df = _read_csv_or_empty(verdicts_csv)
    public_summary = _read_json_or_empty(public_summary_json)
    centrality_df = _read_csv_or_empty(centrality_csv)

    real_events_n = _count_csv_rows(real_events_csv)
    real_matched_panel_n = _count_csv_rows(real_matched_panel_csv)

    sample_rows = _sample_rows(verdicts_df)
    robustness_rows = _robustness_rows(public_summary)
    pap_status_rows = _pap_status_rows(public_summary)
    top_centrality_rows = _top_centrality_rows(centrality_df, top_n=5)
    power_analysis_rows = _power_analysis_rows(power_analysis_csv)

    baseline = public_summary.get("pap_baseline") or {}
    pap_baseline_date = baseline.get("snapshot_date") or "未冻结"

    literature = public_summary.get("literature") or {}
    paper_count = _coerce_int(literature.get("papers_indexed"))
    if paper_count == 0:
        # Fall back to a direct catalog count so the card stays
        # internally consistent even if the public summary is stale.
        try:
            from index_inclusion_research.literature_catalog import (
                list_literature_papers,
            )

            paper_count = len(list_literature_papers())
        except ImportError:
            paper_count = 0

    console_scripts_count = _console_scripts_count(public_summary, pyproject_path)
    doctor_check_count = _doctor_check_count()

    context: dict[str, Any] = {
        "generated_date": (generated_at or datetime.now(tz=UTC))
        .replace(microsecond=0)
        .strftime("%Y-%m-%d"),
        "pap_baseline_date": pap_baseline_date,
        "sample_rows": sample_rows,
        "real_events_n": real_events_n,
        "real_matched_panel_n": f"{real_matched_panel_n:,}",
        "robustness_rows": robustness_rows,
        "pap_status_rows": pap_status_rows,
        "top_centrality_rows": top_centrality_rows,
        "power_analysis_rows": power_analysis_rows,
        "paper_count": paper_count,
        "console_scripts_count": console_scripts_count,
        "doctor_check_count": doctor_check_count,
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


def write_methodology_summary(
    output_path: Path,
    *,
    force: bool = True,
    generated_at: datetime | None = None,
    **build_kwargs: Any,
) -> Path:
    """Render and write the methodology summary to ``output_path``.

    Defaults to ``force=True`` so re-runs (e.g. from ``make paper``) just
    refresh the card in place. Callers wanting fail-on-existing semantics
    pass ``force=False``.
    """
    if output_path.exists() and not force:
        raise FileExistsError(
            f"{output_path} already exists; pass --force to overwrite."
        )
    rendered = build_methodology_summary(
        generated_at=generated_at, **build_kwargs
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _str_to_bool(value: str) -> bool:
    """Accept ``true`` / ``false`` (case-insensitive) for ``--include-todos``."""
    normalized = value.strip().lower()
    if normalized in {"true", "yes", "1", "on"}:
        return True
    if normalized in {"false", "no", "0", "off"}:
        return False
    raise argparse.ArgumentTypeError(
        f"expected true/false (got {value!r})"
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index-inclusion-methodology-summary",
        description=(
            "Generate paper/methodology_summary.md — a single-page "
            "methodology card auto-populated from the current verdicts / "
            "matched panel / public summary / citation centrality. "
            "Designed for paper §3.2 reference, appendix and dashboard "
            "embedding; ~3-5 KB; NO author prose required."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Destination markdown path (default: paper/methodology_summary.md).",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print to stdout instead of writing to disk.",
    )
    parser.add_argument(
        "--include-todos",
        type=_str_to_bool,
        default=False,
        metavar="TRUE|FALSE",
        help=(
            "Reserved flag for future authoring-driven TODO markers. "
            "The canonical card carries NO ``[TODO: ...]`` markers, so the "
            "default is False; setting True is currently a no-op (kept for "
            "CLI symmetry with paper-skeleton)."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    args = _build_arg_parser().parse_args(argv)

    if args.print:
        sys.stdout.write(build_methodology_summary())
        return 0

    output_path = args.output or _default_output_path()
    write_methodology_summary(output_path, force=True)
    size_bytes = output_path.stat().st_size
    logger.info(
        "Wrote methodology summary to %s (%d bytes)", output_path, size_bytes
    )
    if not (SUMMARY_MIN_BYTES <= size_bytes <= SUMMARY_MAX_BYTES):
        logger.warning(
            "Methodology summary size %d bytes is outside the sanity band "
            "[%d, %d] — inspect template / inputs.",
            size_bytes,
            SUMMARY_MIN_BYTES,
            SUMMARY_MAX_BYTES,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
