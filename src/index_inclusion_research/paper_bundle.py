"""Bundle paper-grade artifacts (tables / figures / narrative / RDD) into one
directory so论文写作或汇报准备时不用再到处翻 results/ 子目录。

The bundle deliberately copies — not symlinks — so that ``paper/`` can be
zipped or moved to a separate writing repo without dangling pointers.
"""

from __future__ import annotations

import argparse
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths as project_paths

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
            explicit_files=("patell_bmp_summary.csv",),
        ),
        BundleSection(
            name="figures",
            target_subdir="figures",
            sources=(root / "results" / "real_figures",),
            glob_pattern="*.png",
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


def _resolve_files(section: BundleSection) -> list[Path]:
    files: list[Path] = []
    if section.glob_pattern:
        for source in section.sources:
            if source.is_dir():
                files.extend(sorted(source.glob(section.glob_pattern)))
    if section.explicit_files:
        for relative in section.explicit_files:
            for anchor in section.sources:
                candidate = anchor / relative
                if candidate.is_dir():
                    files.extend(sorted(candidate.rglob("*")))
                    break
                if candidate.exists():
                    files.append(candidate)
                    break
    return list(dict.fromkeys(files))


def _copy_into_section(
    section: BundleSection, dest: Path, *, root: Path
) -> list[Path]:
    target_dir = dest / section.target_subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    for src in _resolve_files(section):
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
        copied.append(target.relative_to(dest))
    return copied


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:  # noqa: BLE001 - never break bundle on bad CSV
        return pd.DataFrame()


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


def _bundle_readme_lines(copies_by_section: dict[str, list[Path]]) -> list[str]:
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
        "figures": "## figures/ — 论文级 PNG 图（CMA + 事件研究）",
        "rdd": "## rdd/ — HS300 RDD 数据 + 图（含稳健性面板）",
        "narrative": "## narrative/ — 论文写作叙事 / 边界 / 预注册",
        "data": "## data/ — 数据来源（候选样本 + PAP snapshot）",
    }
    section_orders = ["tables", "figures", "rdd", "narrative", "data"]
    for name in section_orders:
        files = copies_by_section.get(name, [])
        if not files:
            continue
        lines.append(section_titles.get(name, f"## {name}/"))
        lines.append("")
        for path in files:
            lines.append(f"- `{path.as_posix()}`")
        lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("研究状态快照见 `bundle_summary.md`。")
    return lines


@dataclass(frozen=True)
class BundleResult:
    dest: Path
    copies: tuple[Path, ...]
    readme: Path
    summary: Path


def build_paper_bundle(
    root: Path | None = None,
    *,
    dest: Path | None = None,
    force: bool = False,
) -> BundleResult:
    """Copy paper-grade artifacts into ``dest`` (default ``root/paper``).

    Existing ``dest`` is replaced when ``force=True``; otherwise re-uses
    the directory and overwrites individual files.
    """
    root = root or project_paths.project_root()
    dest = dest or (root / "paper")

    if dest.exists() and force:
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    copies_by_section: dict[str, list[Path]] = {}
    all_copies: list[Path] = []
    for section in _section_specs(root):
        copied = _copy_into_section(section, dest, root=root)
        copies_by_section[section.name] = copied
        all_copies.extend(copied)

    readme_path = dest / "README.md"
    readme_path.write_text("\n".join(_bundle_readme_lines(copies_by_section)) + "\n", encoding="utf-8")
    summary_path = dest / "bundle_summary.md"
    summary_path.write_text("\n".join(_bundle_summary_lines(root)) + "\n", encoding="utf-8")

    return BundleResult(
        dest=dest,
        copies=tuple(all_copies),
        readme=readme_path,
        summary=summary_path,
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    dest = Path(args.dest) if args.dest else None
    result = build_paper_bundle(dest=dest, force=args.force)
    print(f"Paper bundle written to {result.dest}")
    print(f"  files copied: {len(result.copies)}")
    print(f"  README: {result.readme}")
    print(f"  summary: {result.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
