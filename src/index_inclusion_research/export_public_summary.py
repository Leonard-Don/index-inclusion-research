"""导出指数纳入研究公开摘要 (Phase F1).

把 ``results/real_tables/*``、``snapshots/pre-registration-*.csv`` 与
HS300 RDD 子目录里的若干 CSV 蒸馏为一份小而稳定、可安全提交到版本库
的 ``data/public/index_research_summary.json``。

下游消费者（例如 sibling 项目 ``cn-altdata-brief``、未来的 GitHub Pages
日报）只需要读这份精简文件即可拿到 7 条 CMA 假说裁决、PAP 偏离汇总、
threshold × AR-engine 稳健性、HS300 RDD 主结果、文献覆盖、当前已发布
的 figure 路径——不需要直接访问 runtime caches、不需要跑 ``index-
inclusion-cma`` 或 ``make figures-tables``。

设计要点
========

1. **schema 稳定**：顶层 ``schema_version`` 控制破坏性变更；同输入同
   输出（除了 ``generated_at`` 是当前运行时刻），方便 ``git diff`` 看出
   真实数据变化而不是元数据噪音。
2. **安全过滤**：永远不写入 absolute file path、debug 字段、CSV 原始
   ``evidence_summary`` / ``metric_snapshot`` 的多行长文本（只保留
   ``headline_metric``）。``path_ref`` 只是相对 repo root 的路径字面值
   （例如 ``snapshots/pre-registration-2026-05-16.csv``），不带任何
   ``/Users/...`` 信息。
3. **大小可控**：所有 section 都有 cap（7 verdicts × 7 keys，PAP 五类
   计数，sensitivity 三轴汇总，HS300 RDD main + N robustness specs），
   预期 5–15 KB。
4. **graceful degrade**：缺 CSV 时对应 key 直接缺席，不写入合成数据。

脚本自包含：可在不启动 dashboard 的情况下直接调用
``index-inclusion-export-public-summary``，等价于
``python3 -m index_inclusion_research.export_public_summary``。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import tomllib
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from index_inclusion_research import paths

logger = logging.getLogger(__name__)


# Stable schema version. Bumps when the *shape* of any output field
# changes in a breaking way. Additive fields do NOT bump.
SCHEMA_VERSION = 1

# Expected ordered set of hypothesis IDs (matches verdicts CSV row order
# convention; helps the output stay deterministic even if a verdict row
# is re-shuffled upstream).
EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")

# Canonical research thread names (kept stable for downstream consumers).
EXPECTED_TRACKS: tuple[str, ...] = (
    "price_pressure",
    "demand_curve",
    "identification",
)

# Bounded list of figure files the summary advertises. Mirrors what
# ``make figures-tables`` produces; downstream consumers (cn-altdata-brief,
# GitHub Pages) can use this manifest to render previews.
PUBLISHED_FIGURE_RELPATHS: tuple[str, ...] = (
    "results/figures/hs300_rdd_robustness_forest.png",
    "results/figures/cma_verdicts_forest.png",
    "results/figures/cma_verdicts_sensitivity.png",
    "results/figures/cma_verdicts_ar_engine.png",
    "results/figures/cma_verdicts_2d_robustness.png",
    # 40th CLI: H1..H7 verdict-evolution timeline reconstructed from
    # the git log of cma_hypothesis_verdicts.csv.
    "results/figures/verdict_timeline.png",
)

# Snapshot filename pattern (mirrors doctor.PAP_SNAPSHOT_GLOB).
_SNAPSHOT_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_SNAPSHOT_GLOB = "pre-registration-*.csv"

# Default file locations (resolved via ``paths.project_root()`` so an
# override via ``INDEX_INCLUSION_ROOT`` works in tests / CI workspaces).


def _default_output_path() -> Path:
    return paths.project_root() / "data" / "public" / "index_research_summary.json"


def _default_verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_pap_csv() -> Path:
    return paths.real_tables_dir() / "pap_deviation_report.csv"


def _default_rdd_robustness_csv() -> Path:
    return paths.literature_results_dir() / "hs300_rdd" / "rdd_robustness.csv"


def _default_snapshots_dir() -> Path:
    return paths.project_root() / "snapshots"


def _default_pyproject_path() -> Path:
    return paths.project_root() / "pyproject.toml"


def _default_figures_dir() -> Path:
    return paths.results_dir() / "figures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_utc_iso() -> str:
    """Stable, microsecond-stripped UTC ISO timestamp."""
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def _read_csv_or_none(path: Path) -> pd.DataFrame | None:
    """Return ``pd.read_csv(path)`` or ``None`` (with warning) on any error."""
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001 — defensive; we only log
        logger.warning("Failed to read %s: %s", path, exc)
        return None


def _read_pyproject_version(pyproject_path: Path) -> str:
    """Pull ``[project].version`` from ``pyproject.toml``; ``"unknown"`` on error."""
    try:
        with pyproject_path.open("rb") as handle:
            payload = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        logger.warning("Failed to read pyproject version from %s: %s", pyproject_path, exc)
        return "unknown"
    version = payload.get("project", {}).get("version")
    if not isinstance(version, str) or not version.strip():
        return "unknown"
    return version.strip()


def _coerce_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    return str(value)


def _coerce_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        if isinstance(value, float) and pd.isna(value):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_snapshot_date(path: Path) -> date | None:
    """Extract the ``YYYY-MM-DD`` date from a pre-registration filename."""
    match = _SNAPSHOT_DATE_RE.search(path.name)
    if match is None:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _relative_to_project_root(path: Path) -> str:
    """Return ``path`` rendered as a forward-slash relative path under the
    project root, or just ``path.name`` if it lives elsewhere.

    Used to publish ``path_ref`` fields without leaking absolute paths.
    """
    root = paths.project_root()
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError:
        return path.name


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _build_verdicts_section(verdicts_csv: Path) -> dict[str, Any] | None:
    """Per-hypothesis verdict block, keyed by HID (H1..H7).

    Each entry only carries the smallest stable subset of the verdict CSV:
    ``name``, ``verdict``, ``confidence``, ``evidence_tier``, ``n_obs``,
    ``headline_metric`` (= ``key_label + " = " + key_value`` rendered, with
    the float formatted to 4 sig figs to avoid spurious trailing precision).
    Multi-line ``evidence_summary`` and ``metric_snapshot`` are deliberately
    NOT included — downstream consumers should read the CSV directly when
    they need the full narrative.
    """
    df = _read_csv_or_none(verdicts_csv)
    if df is None or df.empty:
        return None
    out: dict[str, Any] = {}
    # Preserve EXPECTED_HIDS order; ignore unknown HIDs silently.
    by_hid: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        hid = _coerce_str(row.get("hid")).strip()
        if not hid:
            continue
        key_label = _coerce_str(row.get("key_label")).strip()
        key_value = _coerce_float(row.get("key_value"))
        if key_label and key_value is not None:
            headline = f"{key_label} = {key_value:.4g}"
        elif key_label:
            headline = key_label
        else:
            headline = ""
        by_hid[hid] = {
            "name": _coerce_str(row.get("name_cn")).strip(),
            "verdict": _coerce_str(row.get("verdict")).strip(),
            "confidence": _coerce_str(row.get("confidence")).strip(),
            "evidence_tier": _coerce_str(row.get("evidence_tier")).strip(),
            "n_obs": _coerce_int(row.get("n_obs")),
            "headline_metric": headline,
            "track": _coerce_str(row.get("track")).strip(),
        }
    for hid in EXPECTED_HIDS:
        if hid in by_hid:
            out[hid] = by_hid[hid]
    # Surface any extra HIDs at the end (defensive — current schema is 7).
    for hid in sorted(set(by_hid.keys()) - set(EXPECTED_HIDS)):
        out[hid] = by_hid[hid]
    return out or None


def _build_pap_section(
    pap_csv: Path,
    snapshots_dir: Path,
    *,
    today: date | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Build the PAP baseline + deviation-summary blocks.

    Returns ``(pap_baseline_block, deviation_summary_block)``; either may
    be ``None`` if its inputs are missing.
    """
    pap_baseline: dict[str, Any] | None = None
    if snapshots_dir.exists():
        candidates = sorted(snapshots_dir.glob(_SNAPSHOT_GLOB))
        if candidates:
            latest = candidates[-1]
            snapshot_date = _parse_snapshot_date(latest)
            reference_date = today if today is not None else date.today()
            frozen_for_days: int | None = None
            snapshot_date_str: str | None = None
            if snapshot_date is not None:
                snapshot_date_str = snapshot_date.isoformat()
                frozen_for_days = max(0, (reference_date - snapshot_date).days)
            pap_baseline = {
                "snapshot_date": snapshot_date_str,
                "path_ref": _relative_to_project_root(latest),
                "frozen_for_days": frozen_for_days,
            }

    deviation_summary: dict[str, Any] | None = None
    df = _read_csv_or_none(pap_csv)
    if df is not None:
        counts: dict[str, int] = {
            "unchanged": 0,
            "flipped": 0,
            "tightened": 0,
            "weakened": 0,
            "unverifiable": 0,
        }
        for _, row in df.iterrows():
            cls = _coerce_str(row.get("classification")).strip().lower()
            if cls in counts:
                counts[cls] += 1
        deviation_summary = {
            "all_unchanged": counts["flipped"] == 0
            and counts["tightened"] == 0
            and counts["weakened"] == 0
            and counts["unverifiable"] == 0,
            "flipped_count": counts["flipped"],
            "tightened_count": counts["tightened"],
            "weakened_count": counts["weakened"],
            "unverifiable_count": counts["unverifiable"],
            "unchanged_count": counts["unchanged"],
        }
    return pap_baseline, deviation_summary


def _verdicts_from_dir(directory: Path) -> dict[str, str]:
    """Read a ``cma_hypothesis_verdicts.csv`` under ``directory`` and return
    a HID→verdict map; empty dict if the file is missing/empty."""
    csv_path = directory / "cma_hypothesis_verdicts.csv"
    df = _read_csv_or_none(csv_path)
    if df is None or df.empty:
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        hid = _coerce_str(row.get("hid")).strip()
        verdict = _coerce_str(row.get("verdict")).strip()
        if hid and verdict:
            out[hid] = verdict
    return out


def _build_sensitivity_section(sensitivity_root: Path) -> dict[str, Any] | None:
    """Summarize threshold / AR-engine / 2D robustness sweeps.

    The sensitivity caches live under ``results/sensitivity/``:

    - ``threshold_<T>/cma_hypothesis_verdicts.csv`` for the threshold axis
    - ``ar_<engine>/cma_hypothesis_verdicts.csv`` for the AR-engine axis
    - ``grid_<T>_<engine>/cma_hypothesis_verdicts.csv`` for the 2D cross.

    A hypothesis is "stable" along an axis when its verdict matches across
    every cell of that axis. ``flip_count`` is the size of the
    complement.
    """
    if not sensitivity_root.exists():
        return None

    # --- Threshold axis: each ``threshold_<T>/`` cell.
    threshold_dirs: list[tuple[float, Path]] = []
    for sub in sorted(sensitivity_root.iterdir()):
        if not sub.is_dir() or not sub.name.startswith("threshold_"):
            continue
        try:
            t_value = float(sub.name.removeprefix("threshold_").replace("_", "."))
        except ValueError:
            continue
        threshold_dirs.append((t_value, sub))
    threshold_dirs.sort(key=lambda kv: kv[0])

    threshold_block: dict[str, Any] | None = None
    if threshold_dirs:
        per_hid: dict[str, set[str]] = {hid: set() for hid in EXPECTED_HIDS}
        thresholds_tested: list[float] = []
        for t_value, sub in threshold_dirs:
            verdicts = _verdicts_from_dir(sub)
            if not verdicts:
                continue
            thresholds_tested.append(t_value)
            for hid, verdict in verdicts.items():
                per_hid.setdefault(hid, set()).add(verdict)
        if thresholds_tested:
            stable = sum(1 for vs in per_hid.values() if len(vs) <= 1)
            flip = sum(1 for vs in per_hid.values() if len(vs) > 1)
            threshold_block = {
                "stable_count": stable,
                "flip_count": flip,
                "thresholds_tested": [round(t, 4) for t in thresholds_tested],
                "cell_count": len(thresholds_tested),
            }

    # --- AR-engine axis: each ``ar_<engine>/`` cell.
    ar_dirs: list[tuple[str, Path]] = []
    for sub in sorted(sensitivity_root.iterdir()):
        if not sub.is_dir() or not sub.name.startswith("ar_"):
            continue
        engine = sub.name.removeprefix("ar_")
        ar_dirs.append((engine, sub))
    ar_dirs.sort(key=lambda kv: kv[0])

    ar_block: dict[str, Any] | None = None
    if ar_dirs:
        per_hid_ar: dict[str, set[str]] = {hid: set() for hid in EXPECTED_HIDS}
        engines_tested: list[str] = []
        for engine, sub in ar_dirs:
            verdicts = _verdicts_from_dir(sub)
            if not verdicts:
                continue
            engines_tested.append(engine)
            for hid, verdict in verdicts.items():
                per_hid_ar.setdefault(hid, set()).add(verdict)
        if engines_tested:
            stable = sum(1 for vs in per_hid_ar.values() if len(vs) <= 1)
            flip = sum(1 for vs in per_hid_ar.values() if len(vs) > 1)
            flipping = sorted(
                hid for hid, vs in per_hid_ar.items() if len(vs) > 1
            )
            ar_block = {
                "stable_count": stable,
                "flip_count": flip,
                "ar_models_tested": engines_tested,
                "cell_count": len(engines_tested),
                "flipping_hypotheses": flipping,
            }

    # --- 2D grid: each ``grid_<T>_<engine>/`` cell.
    grid_dirs: list[Path] = sorted(
        sub
        for sub in sensitivity_root.iterdir()
        if sub.is_dir() and sub.name.startswith("grid_")
    )
    two_d_block: dict[str, Any] | None = None
    if grid_dirs:
        per_hid_2d: dict[str, set[str]] = {hid: set() for hid in EXPECTED_HIDS}
        cell_count = 0
        for sub in grid_dirs:
            verdicts = _verdicts_from_dir(sub)
            if not verdicts:
                continue
            cell_count += 1
            for hid, verdict in verdicts.items():
                per_hid_2d.setdefault(hid, set()).add(verdict)
        if cell_count:
            stable = sum(1 for vs in per_hid_2d.values() if len(vs) <= 1)
            flip = sum(1 for vs in per_hid_2d.values() if len(vs) > 1)
            flipping = sorted(
                hid for hid, vs in per_hid_2d.items() if len(vs) > 1
            )
            two_d_block = {
                "cell_count": cell_count,
                "stable_count": stable,
                "flip_count": flip,
                "flipping_hypotheses": flipping,
            }

    if not (threshold_block or ar_block or two_d_block):
        return None
    sensitivity: dict[str, Any] = {}
    if threshold_block:
        sensitivity["threshold_axis"] = threshold_block
    if ar_block:
        sensitivity["ar_engine_axis"] = ar_block
    if two_d_block:
        sensitivity["two_dimensional"] = two_d_block
    return sensitivity


def _build_hs300_rdd_section(rdd_robustness_csv: Path) -> dict[str, Any] | None:
    """Headline HS300 RDD result (main spec) plus robustness spec count."""
    df = _read_csv_or_none(rdd_robustness_csv)
    if df is None or df.empty:
        return None
    main_rows = df[df["spec_kind"].astype(str).str.lower() == "main"]
    if main_rows.empty:
        # Defensive: still publish robustness counts, headline absent.
        return {
            "main_tau_pct": None,
            "main_p_value": None,
            "main_n_obs": None,
            "robustness_specs_count": int(len(df)),
            "spec_kinds": sorted(
                {_coerce_str(s).strip() for s in df["spec_kind"].astype(str).tolist()}
            ),
        }
    main = main_rows.iloc[0]
    tau = _coerce_float(main.get("tau"))
    return {
        "main_tau_pct": round(tau * 100.0, 4) if tau is not None else None,
        "main_p_value": _coerce_float(main.get("p_value")),
        "main_n_obs": _coerce_int(main.get("n_obs")),
        "main_outcome": _coerce_str(main.get("outcome")).strip() or None,
        "main_bandwidth": _coerce_float(main.get("bandwidth")),
        "robustness_specs_count": int(len(df)),
        "spec_kinds": sorted(
            {_coerce_str(s).strip() for s in df["spec_kind"].astype(str).tolist()}
        ),
    }


def _build_literature_section(pyproject_path: Path) -> dict[str, Any]:
    """Static-ish literature summary (paper count, thread count, CLI count).

    Reads the paper count from ``literature_catalog`` (16 papers, current)
    and the console-script count from ``pyproject.toml``. Both numbers are
    integers, used by README badges and downstream consumers.
    """
    paper_count = 0
    try:
        from index_inclusion_research.literature_catalog import (
            list_literature_papers,
        )

        paper_count = len(list_literature_papers())
    except ImportError as exc:
        logger.warning("literature_catalog import failed: %s", exc)

    console_scripts_count = 0
    try:
        with pyproject_path.open("rb") as handle:
            data = tomllib.load(handle)
        scripts = data.get("project", {}).get("scripts", {})
        if isinstance(scripts, dict):
            console_scripts_count = len(scripts)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        logger.warning("pyproject scripts read failed: %s", exc)

    return {
        "papers_indexed": paper_count,
        "research_threads": len(EXPECTED_TRACKS),
        "research_thread_names": list(EXPECTED_TRACKS),
        "console_scripts_count": console_scripts_count,
    }


def _build_verdict_timeline_section() -> dict[str, Any] | None:
    """Verdict-evolution timeline slice surfaced in the public summary JSON.

    Returns ``{total_commits_tracked, first_commit_date, last_commit_date,
    total_verdict_changes, verdict_changes_per_hypothesis}`` or ``None``
    if the timeline module fails to import (defensive: the summary
    should still serialize on a partial install). Empty git history
    surfaces as zero counts + ``None`` dates so consumers get a stable
    schema regardless of checkout state.
    """
    try:
        from index_inclusion_research.outputs import (
            build_verdict_timeline_from_git,
            summarize_verdict_timeline_for_public_summary,
        )
    except ImportError as exc:
        logger.warning("verdict_timeline import failed: %s", exc)
        return None
    repo_root = paths.project_root()
    if not (repo_root / ".git").exists():
        return None
    try:
        timeline_df = build_verdict_timeline_from_git(repo_root)
    except Exception as exc:  # noqa: BLE001 - never break summary on git error
        logger.warning("verdict timeline reconstruction failed: %s", exc)
        return None
    return summarize_verdict_timeline_for_public_summary(timeline_df)


def _build_literature_network_section() -> dict[str, Any] | None:
    """Heuristic literature-link slice surfaced in the public summary JSON.

    Returns a small ``{edge_count, node_count, edge_semantics,
    top_3_most_linked, top_3_central_papers}`` dict or ``None`` if the graph
    module fails to import (defensive: the summary should still
    serialize on a partial install). Mirrors the rest of the public
    summary sections in tolerating missing inputs.
    """
    try:
        from index_inclusion_research.citation_graph import (
            build_citation_graph,
            summarize_for_public_summary,
        )
    except ImportError as exc:
        logger.warning("citation_graph import failed: %s", exc)
        return None
    graph = build_citation_graph()
    return summarize_for_public_summary(graph)


def _build_figures_published(figures_dir: Path) -> list[str]:
    """Filter ``PUBLISHED_FIGURE_RELPATHS`` to the entries that actually exist.

    Missing files just drop out of the list (with a warning).
    Downstream consumers can therefore assume every advertised relpath is
    a real artifact in the repo at the time the summary was generated.
    """
    root = paths.project_root()
    surviving: list[str] = []
    for relpath in PUBLISHED_FIGURE_RELPATHS:
        candidate = root / relpath
        if candidate.exists():
            surviving.append(relpath)
        else:
            logger.warning("figure missing for public summary: %s", relpath)
    return surviving


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_public_summary(
    *,
    verdicts_csv: Path | None = None,
    pap_csv: Path | None = None,
    rdd_robustness_csv: Path | None = None,
    snapshots_dir: Path | None = None,
    sensitivity_root: Path | None = None,
    figures_dir: Path | None = None,
    pyproject_path: Path | None = None,
    generated_at: str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Build the public summary dict from on-disk research artifacts.

    Every input has a sensible default rooted at ``paths.project_root()``.
    Tests can pass synthetic paths to exercise specific branches.

    Parameters
    ----------
    generated_at:
        Override for the ``generated_at`` field. Defaults to current UTC.
        Tests pass a fixed value for deterministic assertions.
    today:
        Override for "today's date" when computing ``frozen_for_days``.
        Tests pass a fixed value so PAP age is deterministic.
    """
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    pap_csv = pap_csv or _default_pap_csv()
    rdd_robustness_csv = rdd_robustness_csv or _default_rdd_robustness_csv()
    snapshots_dir = snapshots_dir or _default_snapshots_dir()
    sensitivity_root = sensitivity_root or (paths.results_dir() / "sensitivity")
    figures_dir = figures_dir or _default_figures_dir()
    pyproject_path = pyproject_path or _default_pyproject_path()

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or _now_utc_iso(),
        "source_codebase_version": _read_pyproject_version(pyproject_path),
    }

    pap_baseline, pap_deviation = _build_pap_section(
        pap_csv, snapshots_dir, today=today
    )
    if pap_baseline is not None:
        payload["pap_baseline"] = pap_baseline
    if pap_deviation is not None:
        payload["pap_deviation_summary"] = pap_deviation

    verdicts = _build_verdicts_section(verdicts_csv)
    if verdicts is not None:
        payload["verdicts"] = verdicts

    sensitivity = _build_sensitivity_section(sensitivity_root)
    if sensitivity is not None:
        payload["sensitivity_robustness"] = sensitivity

    hs300_rdd = _build_hs300_rdd_section(rdd_robustness_csv)
    if hs300_rdd is not None:
        payload["hs300_rdd"] = hs300_rdd

    payload["literature"] = _build_literature_section(pyproject_path)
    citation_network = _build_literature_network_section()
    if citation_network is not None:
        payload["literature_network"] = citation_network
    verdict_timeline = _build_verdict_timeline_section()
    if verdict_timeline is not None:
        payload["verdict_timeline"] = verdict_timeline
    payload["figures_published"] = _build_figures_published(figures_dir)

    return payload


def write_public_summary_atomic(payload: dict[str, Any], output_path: Path) -> None:
    """Atomic-write the payload to ``output_path`` using the tempfile pattern.

    Writes to a tempfile in the same directory, then ``rename`` swaps it in
    so a reader never sees a half-written file. JSON is sorted + indented
    so ``git diff`` is human-readable.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temp_name = tempfile.mkstemp(
        dir=output_path.parent,
        prefix=f"{output_path.stem}-",
        suffix=f"{output_path.suffix}.tmp",
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")  # POSIX-friendly trailing newline
        temp_path.replace(output_path)
    finally:
        temp_path.unlink(missing_ok=True)


def export_public_summary(
    output_path: Path | None = None,
    *,
    generated_at: str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """One-shot: build the summary and atomic-write it to disk."""
    output_path = output_path or _default_output_path()
    payload = build_public_summary(generated_at=generated_at, today=today)
    write_public_summary_atomic(payload, output_path)
    return payload


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Distill results/real_tables/* + snapshots/* into the small, "
            "sanitized, committable data/public/index_research_summary.json."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Destination JSON path (default: data/public/index_research_summary.json).",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print the JSON to stdout instead of writing to disk.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    payload = build_public_summary()
    if args.print:
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    output_path = args.output or _default_output_path()
    write_public_summary_atomic(payload, output_path)
    logger.info(
        "Wrote public summary to %s (sections=%s)",
        output_path,
        sorted(k for k in payload if k not in {"schema_version", "generated_at"}),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
