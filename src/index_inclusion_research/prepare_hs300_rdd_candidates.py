from __future__ import annotations

import argparse
import shlex
from pathlib import Path

from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit,
    prepare_candidate_frame,
    read_candidate_input,
    summarize_candidate_audit,
    validate_candidate_frame,
)
from index_inclusion_research.loaders import save_dataframe

ROOT = Path(__file__).resolve().parents[2]

DEFAULT_OUTPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
DEFAULT_RECONSTRUCTED_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
DEFAULT_AUDIT_OUTPUT = ROOT / "results" / "literature" / "hs300_rdd_import" / "candidate_batch_audit.csv"
DEFAULT_SUMMARY_OUTPUT = ROOT / "results" / "literature" / "hs300_rdd_import" / "import_summary.md"

PREFLIGHT_STATUS_LABELS = {
    "ready": "可接入 L3",
    "warning": "可接入但需补充",
    "blocked": "暂不可接入 L3",
}
PREFLIGHT_CHECK_LABELS = {
    "pass": "通过",
    "warn": "提醒",
    "block": "阻断",
}
RECONSTRUCTED_SOURCE_TOKENS = (
    "reconstructed",
    "reconstruction",
    "public reconstruction",
    "not official",
    "not an official",
    "非官方",
    "公开重建",
    "重建样本",
)


def _sheet_argument(value: str) -> str | int:
    return int(value) if value.isdigit() else value


def _relative_or_absolute(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except ValueError:
        return str(path)


def _same_path(left: Path, right: Path) -> bool:
    return left.resolve() == right.resolve()


def _filled_rows(frame, column: str) -> int:
    if column not in frame.columns:
        return 0
    series = frame[column].astype("string").str.strip()
    mask = series.notna() & series.ne("").fillna(False)
    return int(mask.sum())


def _candidate_import_command(input_path: Path) -> str:
    input_value = shlex.quote(_relative_or_absolute(input_path))
    output_value = shlex.quote(_relative_or_absolute(DEFAULT_OUTPUT))
    return f"index-inclusion-prepare-hs300-rdd --input {input_value} --output {output_value} --force"


def _frame_text_contains(frame, columns: list[str], tokens: tuple[str, ...]) -> bool:
    values: list[str] = []
    for column in columns:
        if column not in frame.columns:
            continue
        series = frame[column].dropna().astype("string").str.strip()
        values.extend(value for value in series.unique().tolist() if value)
    text = " ".join(values).lower()
    return any(token.lower() in text for token in tokens)


def _reconstructed_source_reason(input_path: Path, validated) -> str:
    name = input_path.name.lower()
    if _same_path(input_path, DEFAULT_RECONSTRUCTED_INPUT) or "reconstructed" in name:
        return "输入文件路径指向公开重建候选样本"
    if _frame_text_contains(validated, ["source", "source_url", "note"], RECONSTRUCTED_SOURCE_TOKENS):
        return "输入文件的 source/source_url/note 显示它来自公开重建样本"
    return ""


def _build_l3_preflight_report(
    *,
    validated,
    input_path: Path,
    output_path: Path,
    audit_summary: dict[str, int | None],
    check_only: bool,
) -> dict[str, object]:
    checks: list[dict[str, str]] = []

    def add_check(status: str, label: str, copy: str, next_step: str = "") -> None:
        checks.append(
            {
                "status": status,
                "status_label": PREFLIGHT_CHECK_LABELS[status],
                "label": label,
                "copy": copy,
                "next_step": next_step,
            }
        )

    import_command = _candidate_import_command(input_path)

    add_check("pass", "字段校验", "必需列、日期、数值字段和 inclusion 编码已通过标准校验。")
    reconstructed_reason = _reconstructed_source_reason(input_path, validated)
    if reconstructed_reason:
        add_check(
            "block",
            "来源层级",
            f"{reconstructed_reason}；只能维持 L2 证据，不能提升为 L3 正式候选样本。",
            "换用中证官方历史候选名单或人工摘录的原始 Excel/CSV 后重新运行预检。",
        )
    else:
        add_check("pass", "来源层级", "输入文件没有公开重建样本标记，可继续按正式候选样本口径预检。")

    if check_only:
        next_step = "换用正式原始候选名单后再写入 L3 文件。" if reconstructed_reason else f"确认后运行：{import_command}"
        add_check("warn", "写入模式", "本次为 check-only，不会更新正式候选文件。", next_step)
    else:
        add_check("pass", "写入模式", "本次会写入标准化候选样本、批次审计和导入摘要。")

    if _same_path(output_path, DEFAULT_OUTPUT):
        add_check("pass", "正式样本路径", "标准化输出指向 RDD L3 默认候选文件。")
    else:
        add_check(
            "warn",
            "正式样本路径",
            f"本次输出为 `{_relative_or_absolute(output_path)}`，RDD L3 默认读取 `{_relative_or_absolute(DEFAULT_OUTPUT)}`。",
            f"正式接入前运行：{import_command}",
        )

    candidate_batches = audit_summary.get("candidate_batches") or 0
    crossing_batches = audit_summary.get("crossing_batches") or 0
    treated_rows = audit_summary.get("treated_rows") or 0
    control_rows = audit_summary.get("control_rows") or 0
    if candidate_batches > 0:
        add_check("pass", "批次识别", f"识别到 {candidate_batches} 个调样批次。")
    else:
        add_check("block", "批次识别", "未识别到可审计的调样批次。", "补齐 batch_id / announce_date 后重新导入。")

    if candidate_batches and crossing_batches == candidate_batches:
        add_check("pass", "cutoff 两侧覆盖", f"{crossing_batches}/{candidate_batches} 个批次同时覆盖 cutoff 左右两侧。")
    elif crossing_batches > 0:
        add_check(
            "warn",
            "cutoff 两侧覆盖",
            f"只有 {crossing_batches}/{candidate_batches} 个批次同时覆盖 cutoff 左右两侧。",
            "补齐缺少左侧或右侧候选股票的批次后再刷新 RDD。",
        )
    else:
        add_check("block", "cutoff 两侧覆盖", "没有任何批次同时覆盖 cutoff 左右两侧。", "至少补齐一个 cutoff 左右两侧都有候选股的批次。")

    if treated_rows > 0 and control_rows > 0:
        add_check("pass", "处理/对照样本", f"当前包含 {treated_rows} 条调入样本和 {control_rows} 条对照样本。")
    else:
        add_check(
            "block",
            "处理/对照样本",
            f"当前调入样本 {treated_rows} 条、对照样本 {control_rows} 条，无法形成 RDD 对照。",
            "补齐 inclusion=1 和 inclusion=0 的候选样本。",
        )

    source_rows = _filled_rows(validated, "source")
    source_url_rows = _filled_rows(validated, "source_url")
    total_rows = len(validated)
    if source_rows == total_rows and source_url_rows == total_rows:
        add_check("pass", "来源追踪", "每条候选样本都包含 source 和 source_url。")
    elif source_rows:
        add_check("warn", "来源追踪", "已提供 source，但部分 source_url 为空。", "正式归档前补齐公告或指数公司页面链接。")
    else:
        add_check("warn", "来源追踪", "未提供 source/source_url，结果可计算但 provenance 不完整。", "正式归档前补齐来源名称和链接。")

    if any(check["status"] == "block" for check in checks):
        status = "blocked"
    elif any(check["status"] == "warn" for check in checks):
        status = "warning"
    else:
        status = "ready"

    next_commands: list[str] = []
    if status == "blocked":
        next_commands.append("修正阻断项后重新运行 index-inclusion-prepare-hs300-rdd --check-only")
    else:
        if check_only or not _same_path(output_path, DEFAULT_OUTPUT):
            next_commands.append(import_command)
        next_commands.extend(
            [
                "index-inclusion-hs300-rdd",
                "index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma",
            ]
        )

    return {
        "status": status,
        "status_label": PREFLIGHT_STATUS_LABELS[status],
        "checks": checks,
        "next_commands": next_commands,
    }


def _build_summary_text(
    *,
    input_path: Path,
    output_path: Path,
    audit_path: Path,
    metadata: dict[str, object],
    audit_summary: dict[str, int | None],
    preflight_report: dict[str, object],
) -> str:
    mapped_columns = metadata.get("mapped_columns", {})
    defaults_applied = metadata.get("defaults_applied", [])
    derived_fields = metadata.get("derived_fields", [])
    unused_columns = metadata.get("unused_columns", [])
    lines = [
        "# HS300 RDD 候选样本导入摘要",
        "",
        f"- 原始输入：`{_relative_or_absolute(input_path)}`",
        f"- 标准化输出：`{_relative_or_absolute(output_path)}`",
        f"- 批次审计：`{_relative_or_absolute(audit_path)}`",
        f"- 原始行数：`{metadata.get('input_rows')}`",
        f"- 输出行数：`{metadata.get('output_rows')}`",
        f"- 候选批次数：`{audit_summary.get('candidate_batches')}`",
        f"- 调入样本数：`{audit_summary.get('treated_rows')}`",
        f"- 对照候选数：`{audit_summary.get('control_rows')}`",
        f"- 覆盖 cutoff 两侧的批次数：`{audit_summary.get('crossing_batches')}`",
        f"- L3 导入预检：`{preflight_report['status_label']}`",
        "",
        "列映射：",
    ]
    if mapped_columns:
        for canonical, source in sorted(mapped_columns.items()):
            lines.append(f"- `{source}` -> `{canonical}`")
    else:
        lines.append("- 输入文件已经使用标准列名。")

    lines.extend(["", "默认补入字段："])
    lines.extend([f"- `{column}`" for column in defaults_applied] if defaults_applied else ["- 无"])
    lines.extend(["", "自动推导字段："])
    lines.extend([f"- `{column}`" for column in derived_fields] if derived_fields else ["- 无"])
    lines.extend(["", "未使用原始列："])
    lines.extend([f"- `{column}`" for column in unused_columns] if unused_columns else ["- 无"])
    lines.extend(["", "## L3 导入预检", ""])
    lines.append(f"- 总体结论：`{preflight_report['status_label']}`")
    lines.extend(["", "预检项目："])
    for check in preflight_report["checks"]:
        lines.append(f"- `{check['status_label']}` {check['label']}：{check['copy']}")
        if check["next_step"]:
            lines.append(f"  下一步：{check['next_step']}")
    lines.extend(["", "下一步命令："])
    for command in preflight_report["next_commands"]:
        lines.append(f"- `{command}`")
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import, normalize, and audit HS300 RDD candidate samples.")
    parser.add_argument("--input", required=True, help="Raw candidate input file (.csv, .tsv, .xlsx, .xls).")
    parser.add_argument("--sheet", type=_sheet_argument, default=None, help="Excel sheet name or 0-based sheet index.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path for the standardized candidate CSV.")
    parser.add_argument("--audit-output", default=str(DEFAULT_AUDIT_OUTPUT), help="Path for the batch audit CSV.")
    parser.add_argument("--summary-output", default=str(DEFAULT_SUMMARY_OUTPUT), help="Path for the import summary markdown.")
    parser.add_argument("--check-only", action="store_true", help="Validate and print the audit summary without writing files.")
    parser.add_argument("--force", action="store_true", help="Overwrite output files if they already exist.")
    parser.add_argument("--batch-id", default=None, help="Fallback batch_id when the raw file does not provide one.")
    parser.add_argument("--market", default="CN", help="Fallback market code. Defaults to CN.")
    parser.add_argument("--index-name", default="CSI300", help="Fallback index name. Defaults to CSI300.")
    parser.add_argument("--announce-date", default=None, help="Fallback announce_date in YYYY-MM-DD format.")
    parser.add_argument("--effective-date", default=None, help="Fallback effective_date in YYYY-MM-DD format.")
    parser.add_argument("--cutoff", type=float, default=300.0, help="Fallback cutoff value. Defaults to 300.")
    parser.add_argument("--event-type", default="inclusion_rdd", help="Fallback event_type. Defaults to inclusion_rdd.")
    parser.add_argument("--source", default=None, help="Fallback source label.")
    parser.add_argument("--source-url", default=None, help="Fallback source URL.")
    parser.add_argument("--note", default=None, help="Fallback note.")
    parser.add_argument("--sector", default=None, help="Fallback sector label.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)
    audit_path = Path(args.audit_output)
    summary_path = Path(args.summary_output)

    defaults = {
        "batch_id": args.batch_id,
        "market": args.market,
        "index_name": args.index_name,
        "announce_date": args.announce_date,
        "effective_date": args.effective_date,
        "cutoff": args.cutoff,
        "event_type": args.event_type,
        "source": args.source,
        "source_url": args.source_url,
        "note": args.note,
        "sector": args.sector,
    }

    raw = read_candidate_input(input_path, sheet_name=args.sheet)
    prepared, metadata = prepare_candidate_frame(raw, defaults=defaults)
    validated = validate_candidate_frame(prepared)
    audit = build_candidate_batch_audit(validated)
    audit_summary = summarize_candidate_audit(audit)
    preflight_report = _build_l3_preflight_report(
        validated=validated,
        input_path=input_path,
        output_path=output_path,
        audit_summary=audit_summary,
        check_only=args.check_only,
    )

    if not args.check_only:
        reconstructed_reason = _reconstructed_source_reason(input_path, validated)
        if reconstructed_reason and _same_path(output_path, DEFAULT_OUTPUT):
            parser.error(
                "Refusing to promote reconstructed L2 candidates into the formal L3 sample path. "
                "Use an official/raw HS300 candidate file for data/raw/hs300_rdd_candidates.csv."
            )
        for path in [output_path, audit_path, summary_path]:
            if path.exists() and not args.force:
                parser.error(f"Refusing to overwrite existing file without --force: {path}")
        save_dataframe(validated, output_path)
        save_dataframe(audit, audit_path)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            _build_summary_text(
                input_path=input_path,
                output_path=output_path,
                audit_path=audit_path,
                metadata=metadata,
                audit_summary=audit_summary,
                preflight_report=preflight_report,
            ),
            encoding="utf-8",
        )

    print(f"Validated {len(validated)} candidate rows from {input_path}")
    print(
        "Candidate audit: "
        f"{audit_summary.get('candidate_batches')} batches, "
        f"{audit_summary.get('treated_rows')} included rows, "
        f"{audit_summary.get('control_rows')} control rows, "
        f"{audit_summary.get('crossing_batches')} cutoff-crossing batches"
    )
    print(f"L3 preflight: {preflight_report['status_label']}")
    for check in preflight_report["checks"]:
        if check["status"] != "pass":
            print(f"  - {check['status_label']} {check['label']}: {check['copy']}")
            if check["next_step"]:
                print(f"    next: {check['next_step']}")
    if preflight_report["next_commands"]:
        print("Next commands:")
        for command in preflight_report["next_commands"]:
            print(f"  - {command}")
    mapped_columns = metadata.get("mapped_columns", {})
    if mapped_columns:
        print("Mapped columns:")
        for canonical, source in sorted(mapped_columns.items()):
            print(f"  - {source} -> {canonical}")
    if metadata.get("defaults_applied"):
        print("Defaults applied:")
        for column in metadata["defaults_applied"]:
            print(f"  - {column}")
    if metadata.get("derived_fields"):
        print("Derived fields:")
        for column in metadata["derived_fields"]:
            print(f"  - {column}")
    if metadata.get("unused_columns"):
        print("Unused input columns:")
        for column in metadata["unused_columns"]:
            print(f"  - {column}")

    if args.check_only:
        print("Check-only mode: no files were written.")
    else:
        print(f"Saved standardized candidates to {output_path}")
        print(f"Saved batch audit to {audit_path}")
        print(f"Saved import summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
