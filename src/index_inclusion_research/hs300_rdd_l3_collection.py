from __future__ import annotations

import argparse
from pathlib import Path
from typing import TypedDict

import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.analysis.rdd_candidates import (
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    build_candidate_batch_audit,
    read_candidate_input,
    summarize_candidate_audit,
    validate_candidate_frame,
)
from index_inclusion_research.loaders import save_dataframe

ROOT = paths.project_root()

DEFAULT_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
DEFAULT_OUTPUT_DIR = ROOT / "results" / "literature" / "hs300_rdd_l3_collection"
DEFAULT_CHECKLIST_NAME = "batch_collection_checklist.csv"
DEFAULT_TEMPLATE_NAME = "formal_candidate_template.csv"
DEFAULT_BOUNDARY_REFERENCE_NAME = "boundary_reference.csv"
DEFAULT_SUMMARY_NAME = "collection_plan.md"

FORMAL_SOURCE_TARGET = "中证指数官方历史候选名单、公告附件，或人工摘录并可追溯的原始候选表"
ACCEPTANCE_COMMAND = "index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --check-only"
WRITE_COMMAND = "index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --output data/raw/hs300_rdd_candidates.csv --force"
REFRESH_COMMAND = "index-inclusion-hs300-rdd && index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma"
DEFAULT_BOUNDARY_WINDOW = 15


class CollectionPackageOutputs(TypedDict):
    checklist_path: Path
    template_path: Path
    boundary_reference_path: Path
    summary_path: Path
    candidate_batches: int
    candidate_rows: int
    boundary_reference_rows: int


def _relative_or_absolute(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except ValueError:
        return str(path)


def _format_date(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def build_batch_collection_checklist(candidates: pd.DataFrame) -> pd.DataFrame:
    validated = validate_candidate_frame(candidates)
    audit = build_candidate_batch_audit(validated)
    rows: list[dict[str, object]] = []
    required_fields = ", ".join(REQUIRED_COLUMNS)
    recommended_fields = ", ".join(OPTIONAL_COLUMNS)
    for _, batch in audit.iterrows():
        rows.append(
            {
                "batch_id": str(batch["batch_id"]),
                "announce_date": _format_date(batch["announce_date"]),
                "effective_date": _format_date(batch["effective_date"]),
                "reconstructed_candidate_rows": int(batch["n_candidates"]),
                "reconstructed_included_rows": int(batch["n_included"]),
                "reconstructed_control_rows": int(batch["n_excluded"]),
                "has_cutoff_crossing": bool(batch["has_cutoff_crossing"]),
                "formal_source_needed": FORMAL_SOURCE_TARGET,
                "required_fields": required_fields,
                "recommended_fields": recommended_fields,
                "acceptance_command": ACCEPTANCE_COMMAND,
                "write_command": WRITE_COMMAND,
                "refresh_command": REFRESH_COMMAND,
                "collection_note": "用正式来源替换 running_variable / cutoff / inclusion；不要复制公开重建排名口径。",
            }
        )
    return pd.DataFrame(rows)


def build_formal_candidate_template(checklist: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, batch in checklist.iterrows():
        for role, inclusion in [("正式调入候选", 1), ("正式对照候选", 0)]:
            rows.append(
                {
                    "batch_id": batch["batch_id"],
                    "market": "CN",
                    "index_name": "CSI300",
                    "ticker": "",
                    "security_name": "",
                    "announce_date": batch["announce_date"],
                    "effective_date": batch["effective_date"],
                    "running_variable": "",
                    "cutoff": "",
                    "inclusion": inclusion,
                    "event_type": "inclusion_rdd",
                    "source": "",
                    "source_url": "",
                    "note": role,
                    "sector": "",
                    "collection_role": role,
                    "collection_instruction": "每个批次至少保留 cutoff 左右两侧候选；source/source_url 应指向正式来源。",
                }
            )
    return pd.DataFrame(rows)


def build_boundary_reference(candidates: pd.DataFrame, *, window: int = DEFAULT_BOUNDARY_WINDOW) -> pd.DataFrame:
    validated = validate_candidate_frame(candidates)
    if window < 1:
        raise ValueError("Boundary reference window must be at least 1.")
    work = validated.copy()
    work["distance_to_cutoff"] = work["running_variable"] - work["cutoff"]
    work["abs_distance_to_cutoff"] = work["distance_to_cutoff"].abs()
    work["boundary_side"] = work["distance_to_cutoff"].map(
        lambda value: "right_or_at_cutoff" if value >= 0 else "left_of_cutoff"
    )
    rows: list[pd.DataFrame] = []
    for _, group in work.groupby("batch_id", sort=True):
        right = group.loc[group["distance_to_cutoff"] >= 0].sort_values(["abs_distance_to_cutoff", "ticker"]).head(window)
        left = group.loc[group["distance_to_cutoff"] < 0].sort_values(["abs_distance_to_cutoff", "ticker"]).head(window)
        rows.extend([right, left])
    if not rows:
        return pd.DataFrame(
            columns=[
                "batch_id",
                "announce_date",
                "effective_date",
                "ticker",
                "security_name",
                "inclusion",
                "reconstructed_running_variable",
                "cutoff",
                "distance_to_cutoff",
                "boundary_side",
                "reference_warning",
            ]
        )
    boundary = pd.concat(rows, ignore_index=True)
    boundary = boundary.sort_values(["announce_date", "batch_id", "abs_distance_to_cutoff", "ticker"]).reset_index(drop=True)
    boundary["ticker"] = boundary["ticker"].astype("string").str.zfill(6)
    boundary["reference_warning"] = "L2 公开重建边界参考，只用于采集核对；不能直接复制为 L3 running_variable。"
    boundary = boundary.rename(columns={"running_variable": "reconstructed_running_variable"})
    columns = [
        "batch_id",
        "announce_date",
        "effective_date",
        "ticker",
        "security_name",
        "inclusion",
        "reconstructed_running_variable",
        "cutoff",
        "distance_to_cutoff",
        "boundary_side",
        "reference_warning",
    ]
    return boundary.loc[:, columns].copy()


def _build_summary_text(
    *,
    input_path: Path,
    output_dir: Path,
    checklist_path: Path,
    template_path: Path,
    boundary_reference_path: Path,
    checklist: pd.DataFrame,
    boundary_reference: pd.DataFrame,
    audit_summary: dict[str, int | None],
) -> str:
    batch_list = ", ".join(checklist["announce_date"].astype(str).tolist()) if not checklist.empty else "无"
    lines = [
        "# HS300 RDD L3 正式候选样本采集包",
        "",
        f"- 当前参考文件：`{_relative_or_absolute(input_path)}`",
        f"- 输出目录：`{_relative_or_absolute(output_dir)}`",
        f"- 批次采集清单：`{_relative_or_absolute(checklist_path)}`",
        f"- 正式填报模板：`{_relative_or_absolute(template_path)}`",
        f"- 边界参考清单：`{_relative_or_absolute(boundary_reference_path)}`",
        f"- 参考批次数：`{audit_summary.get('candidate_batches')}`",
        f"- 参考候选行数：`{sum(checklist['reconstructed_candidate_rows']) if not checklist.empty else 0}`",
        f"- 边界参考行数：`{len(boundary_reference)}`",
        f"- 参考批次列表：`{batch_list}`",
        "",
        "采集目标：",
        f"- 来源必须是：{FORMAL_SOURCE_TARGET}。",
        "- 可以使用当前 L2 重建样本定位批次和边界附近股票，但不能把 L2 running_variable 直接复制成 L3。",
        "- 不要复制公开重建排名口径；正式文件必须来自可追溯的原始候选名单。",
        "- `boundary_reference.csv` 只列出 cutoff 附近的核对优先级，不是正式候选文件。",
        "- 每个批次至少需要 cutoff 左右两侧候选，并同时包含 inclusion=1 与 inclusion=0。",
        "",
        "验收步骤：",
        f"- `{ACCEPTANCE_COMMAND}`",
        f"- `{WRITE_COMMAND}`",
        f"- `{REFRESH_COMMAND}`",
        "",
        "阻断规则：",
        "- 如果 source/source_url/note 包含 reconstructed、public reconstruction、not official、公开重建等标记，导入脚本会阻止写入正式 L3 路径。",
        "- 如果缺少 cutoff 两侧覆盖，或缺少处理/对照样本，预检会显示“暂不可接入 L3”。",
    ]
    return "\n".join(lines) + "\n"


def write_collection_package(
    *,
    input_path: Path,
    output_dir: Path,
    force: bool,
    boundary_window: int = DEFAULT_BOUNDARY_WINDOW,
) -> CollectionPackageOutputs:
    checklist_path = output_dir / DEFAULT_CHECKLIST_NAME
    template_path = output_dir / DEFAULT_TEMPLATE_NAME
    boundary_reference_path = output_dir / DEFAULT_BOUNDARY_REFERENCE_NAME
    summary_path = output_dir / DEFAULT_SUMMARY_NAME
    for path in [checklist_path, template_path, boundary_reference_path, summary_path]:
        if path.exists() and not force:
            raise FileExistsError(f"Refusing to overwrite existing file without --force: {path}")

    raw = read_candidate_input(input_path)
    validated = validate_candidate_frame(raw)
    audit = build_candidate_batch_audit(validated)
    audit_summary = summarize_candidate_audit(audit)
    checklist = build_batch_collection_checklist(validated)
    template = build_formal_candidate_template(checklist)
    boundary_reference = build_boundary_reference(validated, window=boundary_window)

    save_dataframe(checklist, checklist_path)
    save_dataframe(template, template_path)
    save_dataframe(boundary_reference, boundary_reference_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        _build_summary_text(
            input_path=input_path,
            output_dir=output_dir,
            checklist_path=checklist_path,
            template_path=template_path,
            boundary_reference_path=boundary_reference_path,
            checklist=checklist,
            boundary_reference=boundary_reference,
            audit_summary=audit_summary,
        ),
        encoding="utf-8",
    )
    return {
        "checklist_path": checklist_path,
        "template_path": template_path,
        "boundary_reference_path": boundary_reference_path,
        "summary_path": summary_path,
        "candidate_batches": len(checklist),
        "candidate_rows": len(validated),
        "boundary_reference_rows": len(boundary_reference),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an HS300 RDD L3 formal-sample collection package.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Reference reconstructed candidate CSV.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for the collection package.")
    parser.add_argument("--boundary-window", type=int, default=DEFAULT_BOUNDARY_WINDOW, help="Number of nearest names to keep on each side of the cutoff per batch.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing collection package files.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    try:
        outputs = write_collection_package(
            input_path=input_path,
            output_dir=output_dir,
            force=bool(args.force),
            boundary_window=args.boundary_window,
        )
    except FileExistsError as exc:
        parser.error(str(exc))

    print(
        "Built HS300 RDD L3 collection package for "
        f"{outputs['candidate_batches']} batches / {outputs['candidate_rows']} reference rows"
    )
    print(f"Saved batch checklist to {outputs['checklist_path']}")
    print(f"Saved formal candidate template to {outputs['template_path']}")
    print(f"Saved boundary reference to {outputs['boundary_reference_path']}")
    print(f"Saved collection plan to {outputs['summary_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
