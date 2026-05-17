"""Bundle paper-grade artifacts (tables / figures / narrative / RDD) into one
directory so论文写作或汇报准备时不用再到处翻 results/ 子目录。

The bundle deliberately copies — not symlinks — so that ``paper/`` can be
zipped or moved to a separate writing repo without dangling pointers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths as project_paths

logger = logging.getLogger(__name__)

_BUNDLE_LABEL = "index-inclusion-paper-bundle"


@dataclass(frozen=True)
class BundleSection:
    name: str
    target_subdir: str
    sources: tuple[Path, ...]
    glob_pattern: str | None = None
    explicit_files: tuple[str, ...] = ()


def _section_specs(root: Path) -> tuple[BundleSection, ...]:
    return (
        BundleSection(
            name="tables",
            target_subdir="tables",
            sources=(root / "results" / "real_tables", root / "results" / "real_event_study"),
            glob_pattern="*.tex",
            explicit_files=(
                "patell_bmp_summary.csv",
                # PAP deviation audit (commit 48a22f0): every paper bundle
                # snapshot ships the latest deviation report so reviewers
                # can see how the current verdicts compare to the frozen
                # 2026-05-03 PAP baseline without re-running the auditor.
                "pap_deviation_report.csv",
            ),
        ),
        BundleSection(
            name="figures",
            target_subdir="figures",
            # ``results/figures/`` is the canonical home for the cross-cutting
            # paper-grade forest plots (HS300 RDD robustness, CMA verdicts).
            # ``results/real_figures/`` keeps CMA / event-study panels. We
            # ship PNG (raster preview) + PDF (vector, paper-ready) for the
            # forest plots; the *.png glob below also picks up the PNG, so
            # the PDF is added explicitly.
            sources=(root / "results" / "real_figures", root / "results" / "figures"),
            glob_pattern="*.png",
            explicit_files=(
                "hs300_rdd_robustness_forest.pdf",
                "cma_verdicts_forest.pdf",
                "cma_verdicts_sensitivity.pdf",
                "cma_verdicts_ar_engine.pdf",
                "cma_verdicts_2d_robustness.pdf",
            ),
        ),
        BundleSection(
            name="rdd",
            target_subdir="rdd",
            sources=(
                root / "results" / "literature" / "hs300_rdd",
                root / "results" / "literature" / "hs300_rdd" / "figures",
            ),
            explicit_files=(
                "rdd_summary.csv",
                "rdd_summary.tex",
                "rdd_robustness.csv",
                "rdd_robustness.tex",
                "rdd_status.csv",
                "mccrary_density_test.csv",
                "candidate_batch_audit.csv",
                "summary.md",
                "figures/car_m1_p1_rdd_main.png",
                "figures/car_m1_p1_rdd_bins.png",
                "figures/car_m3_p3_rdd_bins.png",
                "figures/turnover_change_rdd_bins.png",
                "figures/volume_change_rdd_bins.png",
                "figures/l3_coverage_timeline.png",
                "figures/rdd_robustness_forest.png",
            ),
        ),
        BundleSection(
            name="narrative",
            target_subdir="narrative",
            sources=(root / "docs",),
            explicit_files=(
                "paper_outline.md",
                "paper_outline_verdicts.md",
                "research_delivery_package.md",
                "pre_registration.md",
                "limitations.md",
                "verdict_iteration.md",
                "hs300_rdd_l3_collection_audit.md",
            ),
        ),
        BundleSection(
            name="data",
            target_subdir="data",
            sources=(root,),
            explicit_files=(
                "data/raw/hs300_rdd_candidates.csv",
                "snapshots",  # whole snapshots dir
            ),
        ),
    )


def _resolve_files(section: BundleSection) -> list[tuple[Path, Path]]:
    """Return ``(source_path, anchor_path)`` pairs for every file the section
    will copy. The anchor is the source directory the file was discovered
    under — used by the manifest to record where each artifact came from.
    """
    pairs: list[tuple[Path, Path]] = []
    if section.glob_pattern:
        for source in section.sources:
            if source.is_dir():
                for path in sorted(source.glob(section.glob_pattern)):
                    pairs.append((path, source))
    if section.explicit_files:
        for relative in section.explicit_files:
            for anchor in section.sources:
                candidate = anchor / relative
                if candidate.is_dir():
                    for path in sorted(candidate.rglob("*")):
                        pairs.append((path, anchor))
                    break
                if candidate.exists():
                    pairs.append((candidate, anchor))
                    break
    # De-duplicate by absolute path while preserving first-seen anchor.
    seen: dict[Path, tuple[Path, Path]] = {}
    for src, anchor in pairs:
        key = src.resolve()
        if key not in seen:
            seen[key] = (src, anchor)
    return list(seen.values())


@dataclass(frozen=True)
class CopyRecord:
    """Tracks one source-to-destination copy plus its content hash.

    Used by both the README index and the JSON manifest so the two views
    stay consistent.
    """

    section: str
    source: Path
    target: Path
    sha256: str
    size_bytes: int


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_into_section(
    section: BundleSection, dest: Path, *, root: Path
) -> list[CopyRecord]:
    target_dir = dest / section.target_subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    records: list[CopyRecord] = []
    for src, _anchor in _resolve_files(section):
        if src.is_dir():
            continue
        # Flatten nested layout into target_dir; preserve only the basename
        # to make `\input{paper/tables/foo.tex}` style references trivial.
        # Exception: snapshots/ keeps its name so the whole subdir lands
        # at paper/data/<filename>.csv (filename already unique enough).
        target = target_dir / src.name
        if target.exists():
            target.unlink()
        shutil.copy2(src, target)
        records.append(
            CopyRecord(
                section=section.name,
                source=src,
                target=target,
                sha256=_sha256_of(target),
                size_bytes=target.stat().st_size,
            )
        )
    return records


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:  # noqa: BLE001 - never break bundle on bad CSV
        return pd.DataFrame()


# ── Pre-bundle regeneration ──────────────────────────────────────────


def _regenerate_artifacts(root: Path) -> dict[str, str]:
    """Refresh the paper-bundled visualizations + PAP audit from current CSVs.

    The bundle copies whatever exists under ``results/``. If the user
    edits a verdicts row or pulls new RDD data but doesn't re-run the
    figure / audit pipeline, the bundle would ship a stale snapshot. To
    keep ``make paper`` self-consistent we regenerate the derived
    derived-from-CSV artifacts before copying:

    1. HS300 RDD robustness forest (PNG + PDF) ← ``rdd_robustness.csv``
    2. CMA verdicts forest (PNG + PDF) ← ``cma_hypothesis_verdicts.csv``
    3. CMA sensitivity forest (PNG + PDF) ← existing threshold cache only
    4. CMA AR-engine forest (PNG + PDF) ← existing engine cache only
    5. PAP deviation report CSV ← latest snapshot + verdicts CSV

    Each step is wrapped so a single failure (missing input, broken
    CSV) only logs a warning and never aborts the whole bundle. The
    return dict maps step name to status ('ok' / 'skipped' / 'error').
    Useful for tests that want to assert the regeneration ran.
    """
    status: dict[str, str] = {}

    # ── 1) HS300 RDD robustness forest ────────────────────────────
    rdd_robustness_csv = (
        root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
    )
    if rdd_robustness_csv.exists():
        try:
            from index_inclusion_research.outputs import (
                build_hs300_rdd_forest_plot,
            )

            png_path = root / "results" / "figures" / "hs300_rdd_robustness_forest.png"
            pdf_path = root / "results" / "figures" / "hs300_rdd_robustness_forest.pdf"
            build_hs300_rdd_forest_plot(
                robustness_csv_path=rdd_robustness_csv,
                output_png_path=png_path,
                output_pdf_path=pdf_path,
            )
            # Mirror so the dashboard's existing entry point sees a fresh PNG.
            mirror = (
                root
                / "results"
                / "literature"
                / "hs300_rdd"
                / "figures"
                / "rdd_robustness_forest.png"
            )
            mirror.parent.mkdir(parents=True, exist_ok=True)
            mirror.write_bytes(png_path.read_bytes())
            status["hs300_rdd_forest"] = "ok"
        except Exception as exc:  # noqa: BLE001 - never break bundle on render error
            logger.warning("HS300 RDD forest regeneration skipped: %s", exc)
            status["hs300_rdd_forest"] = "error"
    else:
        status["hs300_rdd_forest"] = "skipped"

    # ── 2) CMA verdicts forest ────────────────────────────────────
    verdicts_csv = root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    if verdicts_csv.exists():
        try:
            from index_inclusion_research.outputs import (
                build_cma_verdicts_forest_plot,
            )

            png_path = root / "results" / "figures" / "cma_verdicts_forest.png"
            pdf_path = root / "results" / "figures" / "cma_verdicts_forest.pdf"
            build_cma_verdicts_forest_plot(
                verdicts_csv_path=verdicts_csv,
                output_png_path=png_path,
                output_pdf_path=pdf_path,
            )
            status["cma_verdicts_forest"] = "ok"
        except Exception as exc:  # noqa: BLE001
            logger.warning("CMA verdicts forest regeneration skipped: %s", exc)
            status["cma_verdicts_forest"] = "error"
    else:
        status["cma_verdicts_forest"] = "skipped"

    # ── 2b) CMA verdicts sensitivity forest (multi-threshold) ─────
    # Opt-in regeneration: only re-render from the existing
    # ``results/sensitivity/threshold_<T>/`` cache (don't trigger
    # 4 fresh CMA runs from a bundle build — too slow, and the user
    # who wants a fresh sweep runs the dedicated CLI). If no cache
    # exists, we skip silently so a fresh checkout doesn't FAIL.
    sensitivity_root = root / "results" / "sensitivity"
    if sensitivity_root.exists() and any(
        sensitivity_root.glob("threshold_*/cma_hypothesis_verdicts.csv")
    ):
        try:
            from index_inclusion_research.outputs import (
                build_cma_sensitivity_forest_plot_from_cache,
            )

            png_path = (
                root / "results" / "figures" / "cma_verdicts_sensitivity.png"
            )
            pdf_path = (
                root / "results" / "figures" / "cma_verdicts_sensitivity.pdf"
            )
            build_cma_sensitivity_forest_plot_from_cache(
                output_png_path=png_path,
                output_pdf_path=pdf_path,
                sensitivity_root=sensitivity_root,
            )
            status["cma_verdicts_sensitivity_forest"] = "ok"
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "CMA verdicts sensitivity forest regeneration skipped: %s", exc
            )
            status["cma_verdicts_sensitivity_forest"] = "error"
    else:
        status["cma_verdicts_sensitivity_forest"] = "skipped"

    # ── 2c) CMA verdicts AR-engine forest (engine sweep) ──────────
    # Same opt-in contract as the threshold variant above. A fresh
    # ``--ar-model market`` run is the slow operation here (materialises
    # a market-model panel before re-running the CMA orchestrator), so
    # the bundle never triggers a fresh sweep — it only re-renders the
    # figure from existing ``ar_<engine>/`` caches.
    if sensitivity_root.exists() and any(
        sensitivity_root.glob("ar_*/cma_hypothesis_verdicts.csv")
    ):
        try:
            from index_inclusion_research.outputs import (
                build_cma_ar_engine_forest_plot_from_cache,
            )

            png_path = (
                root / "results" / "figures" / "cma_verdicts_ar_engine.png"
            )
            pdf_path = (
                root / "results" / "figures" / "cma_verdicts_ar_engine.pdf"
            )
            build_cma_ar_engine_forest_plot_from_cache(
                output_png_path=png_path,
                output_pdf_path=pdf_path,
                sensitivity_root=sensitivity_root,
            )
            status["cma_verdicts_ar_engine_forest"] = "ok"
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "CMA verdicts AR-engine forest regeneration skipped: %s", exc
            )
            status["cma_verdicts_ar_engine_forest"] = "error"
    else:
        status["cma_verdicts_ar_engine_forest"] = "skipped"

    # ── 2d) CMA verdicts 2D robustness heatmap (threshold × engine) ──
    # Cross of the two 1D sweeps above; cache-only re-render so the
    # paper bundle never triggers a fresh sweep. Picks up dedicated
    # grid_<T>_<engine>/ caches *or* falls back to the 1D
    # threshold_<T>/ + ar_<engine>/ caches.
    if sensitivity_root.exists() and (
        any(sensitivity_root.glob("grid_*/cma_hypothesis_verdicts.csv"))
        or any(sensitivity_root.glob("threshold_*/cma_hypothesis_verdicts.csv"))
        or any(sensitivity_root.glob("ar_*/cma_hypothesis_verdicts.csv"))
    ):
        try:
            from index_inclusion_research.outputs import (
                build_cma_2d_robustness_heatmap_from_cache,
            )

            png_path = (
                root / "results" / "figures" / "cma_verdicts_2d_robustness.png"
            )
            pdf_path = (
                root / "results" / "figures" / "cma_verdicts_2d_robustness.pdf"
            )
            build_cma_2d_robustness_heatmap_from_cache(
                output_png_path=png_path,
                output_pdf_path=pdf_path,
                sensitivity_root=sensitivity_root,
            )
            status["cma_verdicts_2d_robustness_heatmap"] = "ok"
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "CMA verdicts 2D robustness heatmap regeneration skipped: %s",
                exc,
            )
            status["cma_verdicts_2d_robustness_heatmap"] = "error"
    else:
        status["cma_verdicts_2d_robustness_heatmap"] = "skipped"

    # ── 3) PAP deviation report ───────────────────────────────────
    snapshots_dir = root / "snapshots"
    if verdicts_csv.exists() and snapshots_dir.exists():
        try:
            from index_inclusion_research.pap_diff import (
                build_pap_diff,
                resolve_default_baseline,
            )

            baseline_path = resolve_default_baseline(snapshots_dir)
            if baseline_path is not None and baseline_path.exists():
                baseline_df = _read_csv_safe(baseline_path)
                current_df = _read_csv_safe(verdicts_csv)
                report = build_pap_diff(baseline_df, current_df)
                report_path = (
                    root / "results" / "real_tables" / "pap_deviation_report.csv"
                )
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report.to_csv(report_path, index=False)
                status["pap_deviation_report"] = "ok"
            else:
                status["pap_deviation_report"] = "skipped"
        except Exception as exc:  # noqa: BLE001
            logger.warning("PAP deviation report regeneration skipped: %s", exc)
            status["pap_deviation_report"] = "error"
    else:
        status["pap_deviation_report"] = "skipped"

    return status


def _bundle_summary_lines(root: Path) -> list[str]:
    """Render a research-state snapshot for paper/bundle_summary.md."""
    lines = ["# 研究状态快照", ""]

    # PAP signature info: pull from snapshots/ + docs/pre_registration.md
    snapshots = sorted((root / "snapshots").glob("pre-registration-*.csv"))
    if snapshots:
        latest = snapshots[-1]
        lines.append("## 预注册基线 (PAP)")
        lines.append("")
        lines.append(f"- 快照文件：`snapshots/{latest.name}`")
        # Extract date from filename
        stem = latest.stem  # pre-registration-YYYY-MM-DD
        date_part = stem.replace("pre-registration-", "")
        lines.append(f"- 基线日期：`{date_part}`")
        verdicts_baseline = _read_csv_safe(latest)
        lines.append(f"- 假说数：{len(verdicts_baseline)} 行")
        lines.append("")
    else:
        lines.append("## 预注册基线 (PAP)")
        lines.append("")
        lines.append("- 未找到 `snapshots/pre-registration-*.csv` — PAP 未冻结。")
        lines.append("")

    # Verdict counts
    verdicts_now = _read_csv_safe(
        root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    )
    if not verdicts_now.empty and "verdict" in verdicts_now.columns:
        counts = verdicts_now["verdict"].value_counts().to_dict()
        order = ["支持", "部分支持", "证据不足", "待补数据"]
        parts = [f"{int(counts.get(label, 0))} 项{label}" for label in order if counts.get(label, 0)]
        lines.append("## CMA 假说裁决")
        lines.append("")
        lines.append(f"- 当前裁决分布：{' / '.join(parts) if parts else '（无）'}")
        if "evidence_tier" in verdicts_now.columns:
            core_count = int((verdicts_now["evidence_tier"].astype(str) == "core").sum())
            lines.append(f"- 主表入选 (`evidence_tier=core`)：{core_count} 条假说")
        lines.append(
            "- 详细裁决见 `tables/cma_hypothesis_verdicts.tex` 与 "
            "`narrative/paper_outline_verdicts.md`。"
        )
        lines.append("")

    # HS300 L3 coverage
    rdd_status = _read_csv_safe(
        root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    )
    if not rdd_status.empty:
        first = rdd_status.iloc[0]
        candidate_rows = first.get("candidate_rows")
        candidate_batches = first.get("candidate_batches")
        as_of = first.get("as_of_date")
        coverage_note = first.get("coverage_note", "")
        lines.append("## HS300 RDD L3 样本")
        lines.append("")
        if candidate_rows and candidate_batches:
            lines.append(
                f"- 候选行数 / 批次数：{int(candidate_rows)} 行 / {int(candidate_batches)} 批次"
            )
        if as_of:
            lines.append(f"- 样本期：{as_of}")
        if coverage_note:
            lines.append(f"- 覆盖摘要：{coverage_note}")
        lines.append("- 论文级门槛：≥20 批次 / ≥10 年。当前为初步识别证据。")
        lines.append("")

    # RDD main + robustness headline
    robustness = _read_csv_safe(
        root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
    )
    if not robustness.empty:
        main_rows = robustness.loc[robustness["spec_kind"] == "main"]
        if not main_rows.empty:
            m = main_rows.iloc[0]
            lines.append("## HS300 RDD 主结果")
            lines.append("")
            lines.append(
                f"- main 局部线性 τ = {float(m['tau']) * 100:.2f}% "
                f"(p = {float(m['p_value']):.3f}, n = {int(m['n_obs'])})"
            )
            lines.append("- 完整稳健性面板见 `rdd/rdd_robustness.csv` 与 `rdd/rdd_robustness_forest.png`。")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        f"由 `{_BUNDLE_LABEL}` 自动生成。研究状态来自当前真实数据；要刷新先跑 `make rebuild`。"
    )
    return lines


def _bundle_readme_lines(copies_by_section: dict[str, list[CopyRecord]]) -> list[str]:
    lines = [
        "# Paper Bundle Index",
        "",
        f"由 `{_BUNDLE_LABEL}` 自动生成。",
        "",
        "聚合了论文写作 / 汇报需要的核心产出：",
        "",
    ]
    section_titles = {
        "tables": "## tables/ — LaTeX 表 (\\input 直接用)",
        "figures": "## figures/ — 论文级 PNG 图（CMA + 事件研究 + 跨假说森林图）",
        "rdd": "## rdd/ — HS300 RDD 数据 + 图（含稳健性面板）",
        "narrative": "## narrative/ — 论文写作叙事 / 边界 / 预注册",
        "data": "## data/ — 数据来源（候选样本 + PAP snapshot）",
    }
    section_orders = ["tables", "figures", "rdd", "narrative", "data"]
    for name in section_orders:
        records = copies_by_section.get(name, [])
        if not records:
            continue
        lines.append(section_titles.get(name, f"## {name}/"))
        lines.append("")
        for record in records:
            rel = record.target.relative_to(record.target.parent.parent).as_posix()
            lines.append(f"- `{rel}`")
        lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("研究状态快照见 `bundle_summary.md`；可机读清单见 `manifest.json`。")
    return lines


def _build_manifest(
    *,
    dest: Path,
    root: Path,
    records: list[CopyRecord],
    regenerated: dict[str, str],
) -> dict[str, object]:
    """Build a machine-readable manifest of every copied artifact.

    Each entry records the original source path (relative to project
    root when possible), the in-bundle target path, sha256 hash, and
    file size — so downstream consumers (paper-audit, CI, archival) can
    detect drift between the staged bundle and the live results without
    re-running the bundle command.

    The regeneration status block exposes whether the three pre-copy
    refresh steps fired so reviewers can tell a stale forest plot from
    one that simply couldn't be regenerated (missing CSV).
    """

    def _rel_to_root(path: Path) -> str:
        try:
            return path.resolve().relative_to(root.resolve()).as_posix()
        except ValueError:
            return path.resolve().as_posix()

    def _rel_to_dest(path: Path) -> str:
        try:
            return path.resolve().relative_to(dest.resolve()).as_posix()
        except ValueError:
            return path.resolve().as_posix()

    artifacts: list[dict[str, object]] = []
    for record in records:
        artifacts.append(
            {
                "section": record.section,
                "source": _rel_to_root(record.source),
                "target": _rel_to_dest(record.target),
                "sha256": record.sha256,
                "size_bytes": record.size_bytes,
            }
        )

    return {
        "bundle_label": _BUNDLE_LABEL,
        "manifest_schema_version": 1,
        "artifact_count": len(artifacts),
        "regenerated": regenerated,
        "artifacts": artifacts,
    }


@dataclass(frozen=True)
class BundleResult:
    dest: Path
    copies: tuple[Path, ...]
    readme: Path
    summary: Path
    manifest: Path
    regenerated: dict[str, str]


def build_paper_bundle(
    root: Path | None = None,
    *,
    dest: Path | None = None,
    force: bool = False,
    regenerate: bool = True,
) -> BundleResult:
    """Copy paper-grade artifacts into ``dest`` (default ``root/paper``).

    Existing ``dest`` is replaced when ``force=True``; otherwise re-uses
    the directory and overwrites individual files.

    When ``regenerate=True`` (default), refresh the forest plots and PAP
    deviation report from current CSVs before copying — see
    :func:`_regenerate_artifacts`. Pass ``regenerate=False`` in tests
    that seed the destination with raw fixtures.
    """
    root = root or project_paths.project_root()
    dest = dest or (root / "paper")

    if dest.exists() and force:
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    regenerated: dict[str, str] = (
        _regenerate_artifacts(root) if regenerate else {}
    )

    copies_by_section: dict[str, list[CopyRecord]] = {}
    all_records: list[CopyRecord] = []
    for section in _section_specs(root):
        records = _copy_into_section(section, dest, root=root)
        copies_by_section[section.name] = records
        all_records.extend(records)

    readme_path = dest / "README.md"
    readme_path.write_text("\n".join(_bundle_readme_lines(copies_by_section)) + "\n", encoding="utf-8")
    summary_path = dest / "bundle_summary.md"
    summary_path.write_text("\n".join(_bundle_summary_lines(root)) + "\n", encoding="utf-8")

    manifest_path = dest / "manifest.json"
    manifest = _build_manifest(
        dest=dest,
        root=root,
        records=all_records,
        regenerated=regenerated,
    )
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    copy_paths = tuple(record.target.relative_to(dest) for record in all_records)
    return BundleResult(
        dest=dest,
        copies=copy_paths,
        readme=readme_path,
        summary=summary_path,
        manifest=manifest_path,
        regenerated=regenerated,
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=_BUNDLE_LABEL,
        description=(
            "Aggregate paper-ready tables / figures / narrative into a single "
            "paper/ directory ready for LaTeX writing or sharing."
        ),
    )
    parser.add_argument(
        "--dest",
        default=None,
        help="Destination directory (default: <project>/paper).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove the destination directory before copying.",
    )
    parser.add_argument(
        "--no-regenerate",
        dest="regenerate",
        action="store_false",
        default=True,
        help=(
            "Skip the pre-copy refresh of forest plots / PAP audit. "
            "Use when you already ran `make rebuild` and just want a "
            "fast re-copy."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    dest = Path(args.dest) if args.dest else None
    result = build_paper_bundle(
        dest=dest, force=args.force, regenerate=args.regenerate
    )
    print(f"Paper bundle written to {result.dest}")
    print(f"  files copied: {len(result.copies)}")
    print(f"  README: {result.readme}")
    print(f"  summary: {result.summary}")
    print(f"  manifest: {result.manifest}")
    if result.regenerated:
        regen_summary = ", ".join(
            f"{name}={status}" for name, status in sorted(result.regenerated.items())
        )
        print(f"  regenerated: {regen_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
