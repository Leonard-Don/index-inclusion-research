from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit,
    prepare_candidate_frame,
    read_candidate_input,
    summarize_candidate_audit,
    validate_candidate_frame,
)
from index_inclusion_research.loaders import save_dataframe


DEFAULT_OUTPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
DEFAULT_AUDIT_OUTPUT = ROOT / "results" / "literature" / "hs300_rdd_import" / "candidate_batch_audit.csv"
DEFAULT_SUMMARY_OUTPUT = ROOT / "results" / "literature" / "hs300_rdd_import" / "import_summary.md"


def _sheet_argument(value: str) -> str | int:
    return int(value) if value.isdigit() else value


def _relative_or_absolute(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except ValueError:
        return str(path)


def _build_summary_text(
    *,
    input_path: Path,
    output_path: Path,
    audit_path: Path,
    metadata: dict[str, object],
    audit_summary: dict[str, int | None],
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

    if not args.check_only:
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
