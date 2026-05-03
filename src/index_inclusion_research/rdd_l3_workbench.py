from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from index_inclusion_research import (
    doctor,
    hs300_rdd,
    hs300_rdd_l3_collection,
    hs300_rdd_online_sources,
    paths,
    prepare_hs300_rdd_candidates,
)
from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit,
    prepare_candidate_frame,
    read_candidate_input,
    summarize_candidate_audit,
    validate_candidate_frame,
)
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.real_evidence_refresh import (
    build_evidence_manifest,
    write_evidence_manifest,
)
from index_inclusion_research.result_contract import load_rdd_status

ROOT = paths.project_root()
IMPORT_DIR = ROOT / "results" / "literature" / "hs300_rdd_import"
UPLOAD_DIR = IMPORT_DIR / "uploads"
COLLECTION_DIR = ROOT / "results" / "literature" / "hs300_rdd_l3_collection"
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
DEFAULT_AUDIT_OUTPUT = IMPORT_DIR / "candidate_batch_audit.csv"
DEFAULT_SUMMARY_OUTPUT = IMPORT_DIR / "import_summary.md"
DEFAULT_COLLECTION_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"


def _relative_label(path: Path, *, root: Path = ROOT) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)


def _sheet_value(value: str | int | None) -> str | int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    return int(stripped) if stripped.isdigit() else stripped


def _defaults(
    *,
    batch_id: str | None = None,
    announce_date: str | None = None,
    effective_date: str | None = None,
    source: str | None = None,
    source_url: str | None = None,
    note: str | None = None,
    sector: str | None = None,
) -> dict[str, object]:
    return {
        "batch_id": batch_id,
        "market": "CN",
        "index_name": "CSI300",
        "announce_date": announce_date,
        "effective_date": effective_date,
        "cutoff": 300.0,
        "event_type": "inclusion_rdd",
        "source": source,
        "source_url": source_url,
        "note": note,
        "sector": sector,
    }


def _payload_table(frame: pd.DataFrame, *, limit: int = 80) -> dict[str, Any]:
    clean = frame.head(limit).where(pd.notna(frame.head(limit)), None)
    return {
        "columns": list(frame.columns),
        "rows": clean.to_dict(orient="records"),
        "total_rows": int(len(frame)),
        "shown_rows": int(min(len(frame), limit)),
    }


def _select_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    available = [column for column in columns if column in frame.columns]
    return frame.loc[:, available].copy()


def _path_payload(path: Path, *, root: Path = ROOT) -> dict[str, object]:
    return {
        "label": _relative_label(path, root=root),
        "exists": path.exists(),
        "href": f"/files/{_relative_label(path, root=root)}" if path.exists() else "",
    }


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={"ticker": str}, low_memory=False)
    except (OSError, ValueError):
        return pd.DataFrame()


def _mtime_label(paths: list[Path]) -> str:
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    if not mtimes:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(max(mtimes)))


def save_uploaded_candidate_file(
    storage: FileStorage,
    *,
    upload_dir: Path = UPLOAD_DIR,
) -> Path:
    filename = secure_filename(storage.filename or "")
    if not filename:
        raise ValueError("上传文件缺少文件名。")
    if Path(filename).suffix.lower() not in {".csv", ".tsv", ".xlsx", ".xls"}:
        raise ValueError("仅支持 CSV/TSV/XLSX/XLS 候选名单文件。")
    upload_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    path = upload_dir / f"{stamp}-{filename}"
    storage.save(path)
    return path


def build_candidate_preflight(
    input_path: Path,
    *,
    sheet: str | int | None = None,
    output_path: Path = DEFAULT_OUTPUT,
    check_only: bool = True,
    defaults: dict[str, object] | None = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    raw = read_candidate_input(input_path, sheet_name=_sheet_value(sheet))
    prepared, metadata = prepare_candidate_frame(raw, defaults=defaults or _defaults())
    validated = validate_candidate_frame(prepared)
    audit = build_candidate_batch_audit(validated)
    audit_summary = summarize_candidate_audit(audit)
    preflight_report = prepare_hs300_rdd_candidates._build_l3_preflight_report(
        validated=validated,
        input_path=input_path,
        output_path=Path(output_path),
        audit_summary=audit_summary,
        check_only=check_only,
    )
    return {
        "input_path": str(input_path),
        "input_label": _relative_label(input_path),
        "metadata": metadata,
        "audit_summary": audit_summary,
        "preflight": preflight_report,
        "candidate_preview": _payload_table(validated, limit=40),
        "audit_preview": _payload_table(audit, limit=40),
        "validated": validated,
        "audit": audit,
    }


def build_candidate_preflight_result(
    input_path: Path,
    *,
    sheet: str | int | None = None,
    defaults: dict[str, object] | None = None,
) -> dict[str, Any]:
    try:
        return build_candidate_preflight(
            input_path,
            sheet=sheet,
            check_only=True,
            defaults=defaults,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "input_path": str(input_path),
            "input_label": _relative_label(Path(input_path)),
            "preflight": {
                "status": "blocked",
                "status_label": "暂不可接入 L3",
                "checks": [
                    {
                        "status": "block",
                        "status_label": "阻断",
                        "label": "文件解析/字段校验",
                        "copy": f"{type(exc).__name__}: {exc}",
                        "next_step": "修正文件后重新上传预检。",
                    }
                ],
                "next_commands": [],
            },
            "metadata": {},
            "audit_summary": {},
            "candidate_preview": _payload_table(pd.DataFrame()),
            "audit_preview": _payload_table(pd.DataFrame()),
        }


def import_official_candidates(
    input_path: Path,
    *,
    sheet: str | int | None = None,
    defaults: dict[str, object] | None = None,
    output_path: Path = DEFAULT_OUTPUT,
    audit_path: Path = DEFAULT_AUDIT_OUTPUT,
    summary_path: Path = DEFAULT_SUMMARY_OUTPUT,
    force: bool = True,
) -> dict[str, Any]:
    result = build_candidate_preflight(
        input_path,
        sheet=sheet,
        output_path=output_path,
        check_only=False,
        defaults=defaults,
    )
    if result["preflight"]["status"] == "blocked":
        raise ValueError("预检存在阻断项，未写入正式候选样本。")
    validated: pd.DataFrame = result["validated"]
    audit: pd.DataFrame = result["audit"]
    reconstructed_reason = prepare_hs300_rdd_candidates._reconstructed_source_reason(
        Path(input_path),
        validated,
    )
    if reconstructed_reason and Path(output_path).resolve() == DEFAULT_OUTPUT.resolve():
        raise ValueError("公开重建样本不能写入正式 L3 候选路径。")
    for path in (output_path, audit_path, summary_path):
        if path.exists() and not force:
            raise FileExistsError(f"文件已存在，请启用覆盖: {path}")
    save_dataframe(validated, output_path)
    save_dataframe(audit, audit_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        prepare_hs300_rdd_candidates._build_summary_text(
            input_path=Path(input_path),
            output_path=Path(output_path),
            audit_path=Path(audit_path),
            metadata=result["metadata"],
            audit_summary=result["audit_summary"],
            preflight_report=result["preflight"],
        ),
        encoding="utf-8",
    )
    return {
        **result,
        "written_paths": [
            _path_payload(Path(output_path)),
            _path_payload(Path(audit_path)),
            _path_payload(Path(summary_path)),
        ],
    }


def refresh_rdd_and_manifest() -> dict[str, Any]:
    rdd_result = hs300_rdd.run_analysis(
        verbose=False,
        allow_demo=False,
        strict_validation=True,
    )
    doctor_results = doctor.run_all_checks()
    manifest = build_evidence_manifest(doctor_results=doctor_results)
    manifest_json, manifest_csv = write_evidence_manifest(manifest)
    return {
        "rdd_mode": rdd_result.get("mode"),
        "manifest_paths": [_path_payload(manifest_json), _path_payload(manifest_csv)],
    }


def collection_paths_for_root(root: Path) -> list[Path]:
    collection_dir = Path(root) / "results" / "literature" / "hs300_rdd_l3_collection"
    return [
        collection_dir / hs300_rdd_l3_collection.DEFAULT_SUMMARY_NAME,
        collection_dir / hs300_rdd_l3_collection.DEFAULT_CHECKLIST_NAME,
        collection_dir / hs300_rdd_l3_collection.DEFAULT_TEMPLATE_NAME,
        collection_dir / hs300_rdd_l3_collection.DEFAULT_BOUNDARY_REFERENCE_NAME,
    ]


def online_collection_paths_for_root(root: Path) -> list[Path]:
    collection_dir = Path(root) / "results" / "literature" / "hs300_rdd_l3_collection"
    return [
        collection_dir / hs300_rdd_online_sources.DEFAULT_DRAFT_OUTPUT.name,
        collection_dir / hs300_rdd_online_sources.DEFAULT_AUDIT_OUTPUT.name,
        collection_dir / hs300_rdd_online_sources.DEFAULT_SEARCH_DIAGNOSTICS_OUTPUT.name,
        collection_dir / hs300_rdd_online_sources.DEFAULT_YEAR_COVERAGE_OUTPUT.name,
        collection_dir / hs300_rdd_online_sources.DEFAULT_MANUAL_GAP_WORKLIST_OUTPUT.name,
        collection_dir / hs300_rdd_online_sources.DEFAULT_REPORT_OUTPUT.name,
    ]


def build_collection_status(*, root: Path = ROOT) -> dict[str, Any]:
    root = Path(root)
    paths = collection_paths_for_root(root)
    checklist = _read_csv(
        root
        / "results"
        / "literature"
        / "hs300_rdd_l3_collection"
        / hs300_rdd_l3_collection.DEFAULT_CHECKLIST_NAME
    )
    template = _read_csv(
        root
        / "results"
        / "literature"
        / "hs300_rdd_l3_collection"
        / hs300_rdd_l3_collection.DEFAULT_TEMPLATE_NAME
    )
    boundary = _read_csv(
        root
        / "results"
        / "literature"
        / "hs300_rdd_l3_collection"
        / hs300_rdd_l3_collection.DEFAULT_BOUNDARY_REFERENCE_NAME
    )
    all_exists = all(path.exists() for path in paths)
    crossing_batches = 0
    if not checklist.empty and "has_cutoff_crossing" in checklist.columns:
        crossing_batches = int(
            checklist["has_cutoff_crossing"].astype(str).str.lower().isin({"true", "1"}).sum()
        )
    return {
        "status": "ready" if all_exists else "missing",
        "status_label": "采集包已就绪" if all_exists else "待生成采集包",
        "generated_at": _mtime_label(paths),
        "batch_count": int(len(checklist)),
        "crossing_batches": crossing_batches,
        "template_rows": int(len(template)),
        "boundary_reference_rows": int(len(boundary)),
        "paths": [_path_payload(path, root=root) for path in paths],
        "input_path": _path_payload(root / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv", root=root),
    }


def _status_count(frame: pd.DataFrame, status: str) -> int:
    if frame.empty or "status" not in frame.columns:
        return 0
    return int((frame["status"].astype(str) == status).sum())


def build_online_collection_status(*, root: Path = ROOT) -> dict[str, Any]:
    root = Path(root)
    paths = online_collection_paths_for_root(root)
    draft_path, audit_path, search_path, year_path, gap_path, report_path = paths
    draft = _read_csv(draft_path)
    audit = _read_csv(audit_path)
    search = _read_csv(search_path)
    years = _read_csv(year_path)
    has_diagnostics = search_path.exists() and year_path.exists() and gap_path.exists()
    candidate_years: list[str] = []
    notice_only_years: list[str] = []
    no_notice_years: list[str] = []
    if not years.empty and {"year", "status"}.issubset(years.columns):
        status_values = years["status"].astype(str)
        year_values = years["year"].astype(str)
        candidate_years = year_values[status_values == "candidate_found"].tolist()
        notice_only_years = year_values[status_values == "notice_only"].tolist()
        no_notice_years = year_values[status_values == "no_notice"].tolist()
    return {
        "status": "ready" if has_diagnostics else "missing",
        "status_label": "线上诊断已就绪" if has_diagnostics else "待运行线上采集",
        "generated_at": _mtime_label(paths),
        "candidate_rows": int(len(draft)),
        "source_rows": int(len(audit)),
        "search_rows": int(len(search)),
        "year_rows": int(len(years)),
        "candidate_years": candidate_years,
        "notice_only_years": notice_only_years,
        "no_notice_years": no_notice_years,
        "candidate_year_count": len(candidate_years),
        "notice_only_year_count": len(notice_only_years),
        "no_notice_year_count": len(no_notice_years),
        "candidate_found_rows": _status_count(years, "candidate_found"),
        "notice_only_rows": _status_count(years, "notice_only"),
        "no_notice_rows": _status_count(years, "no_notice"),
        "paths": [_path_payload(path, root=root) for path in paths],
        "report_path": _path_payload(report_path, root=root),
    }


def build_collection_preview_tables(*, root: Path = ROOT) -> list[dict[str, Any]]:
    root = Path(root)
    collection_dir = root / "results" / "literature" / "hs300_rdd_l3_collection"
    checklist_path = collection_dir / hs300_rdd_l3_collection.DEFAULT_CHECKLIST_NAME
    template_path = collection_dir / hs300_rdd_l3_collection.DEFAULT_TEMPLATE_NAME
    boundary_path = collection_dir / hs300_rdd_l3_collection.DEFAULT_BOUNDARY_REFERENCE_NAME
    checklist = _select_columns(
        _read_csv(checklist_path),
        [
            "batch_id",
            "announce_date",
            "effective_date",
            "reconstructed_candidate_rows",
            "reconstructed_included_rows",
            "reconstructed_control_rows",
            "has_cutoff_crossing",
            "acceptance_command",
            "write_command",
        ],
    )
    template = _select_columns(
        _read_csv(template_path),
        [
            "batch_id",
            "ticker",
            "security_name",
            "announce_date",
            "effective_date",
            "running_variable",
            "cutoff",
            "inclusion",
            "source",
            "source_url",
            "collection_role",
        ],
    )
    boundary = _select_columns(
        _read_csv(boundary_path),
        [
            "batch_id",
            "announce_date",
            "ticker",
            "security_name",
            "inclusion",
            "reconstructed_running_variable",
            "cutoff",
            "distance_to_cutoff",
            "boundary_side",
        ],
    )
    return [
        {
            "key": "batch_collection_checklist",
            "title": "批次采集清单预览",
            "source_path": _path_payload(checklist_path, root=root),
            **_payload_table(checklist, limit=12),
        },
        {
            "key": "formal_candidate_template",
            "title": "正式填报模板预览",
            "source_path": _path_payload(template_path, root=root),
            **_payload_table(template, limit=12),
        },
        {
            "key": "boundary_reference",
            "title": "边界参考预览",
            "source_path": _path_payload(boundary_path, root=root),
            **_payload_table(boundary, limit=24),
        },
    ]


def build_online_collection_preview_tables(*, root: Path = ROOT) -> list[dict[str, Any]]:
    root = Path(root)
    collection_dir = root / "results" / "literature" / "hs300_rdd_l3_collection"
    audit_path = collection_dir / hs300_rdd_online_sources.DEFAULT_AUDIT_OUTPUT.name
    search_path = collection_dir / hs300_rdd_online_sources.DEFAULT_SEARCH_DIAGNOSTICS_OUTPUT.name
    year_path = collection_dir / hs300_rdd_online_sources.DEFAULT_YEAR_COVERAGE_OUTPUT.name
    gap_path = collection_dir / hs300_rdd_online_sources.DEFAULT_MANUAL_GAP_WORKLIST_OUTPUT.name
    audit = _select_columns(
        _read_csv(audit_path),
        [
            "publish_date",
            "title",
            "attachment_name",
            "status",
            "usable_for_l3",
            "addition_rows",
            "control_rows",
            "reason",
        ],
    )
    search = _select_columns(
        _read_csv(search_path),
        [
            "search_term",
            "raw_rows",
            "title_matched_rows",
            "theme_matched_rows",
            "matched_rows",
            "date_filtered_matched_rows",
            "matched_publish_dates",
            "reason",
        ],
    )
    gaps = _select_columns(
        _read_csv(gap_path),
        [
            "year",
            "priority",
            "gap_type",
            "publish_date",
            "title",
            "attachment_name",
            "addition_rows",
            "control_rows",
            "missing_evidence",
            "suggested_next_step",
        ],
    )
    years = _select_columns(
        _read_csv(year_path),
        [
            "year",
            "notice_rows",
            "attachment_rows",
            "usable_attachment_rows",
            "parsed_addition_rows",
            "parsed_control_rows",
            "candidate_rows",
            "candidate_batches",
            "status",
        ],
    )
    return [
        {
            "key": "online_year_coverage",
            "title": "线上年份覆盖诊断",
            "source_path": _path_payload(year_path, root=root),
            **_payload_table(years, limit=20),
        },
        {
            "key": "online_source_audit",
            "title": "线上来源审计预览",
            "source_path": _path_payload(audit_path, root=root),
            **_payload_table(audit, limit=20),
        },
        {
            "key": "online_manual_gap_worklist",
            "title": "线上补录缺口清单",
            "source_path": _path_payload(gap_path, root=root),
            **_payload_table(gaps, limit=20),
        },
        {
            "key": "online_search_diagnostics",
            "title": "线上搜索诊断预览",
            "source_path": _path_payload(search_path, root=root),
            **_payload_table(search, limit=12),
        },
    ]


def refresh_collection_package(
    *,
    root: Path = ROOT,
    boundary_window: int = hs300_rdd_l3_collection.DEFAULT_BOUNDARY_WINDOW,
    force: bool = True,
) -> dict[str, Any]:
    root = Path(root)
    input_path = root / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
    output_dir = root / "results" / "literature" / "hs300_rdd_l3_collection"
    if not input_path.exists():
        raise FileNotFoundError(f"缺少公开重建参考样本：{_relative_label(input_path, root=root)}")
    outputs = hs300_rdd_l3_collection.write_collection_package(
        input_path=input_path,
        output_dir=output_dir,
        force=force,
        boundary_window=boundary_window,
    )
    written_paths = [
        outputs["summary_path"],
        outputs["checklist_path"],
        outputs["template_path"],
        outputs["boundary_reference_path"],
    ]
    return {
        "candidate_batches": int(outputs["candidate_batches"]),
        "candidate_rows": int(outputs["candidate_rows"]),
        "boundary_reference_rows": int(outputs["boundary_reference_rows"]),
        "written_paths": [_path_payload(Path(path), root=root) for path in written_paths],
        "status": build_collection_status(root=root),
    }


def build_rdd_l3_workbench_context(
    *,
    root: Path = ROOT,
    preflight_result: dict[str, Any] | None = None,
    import_result: dict[str, Any] | None = None,
    collection_result: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    root = Path(root)
    status = dict(load_rdd_status(root))
    import_dir = root / "results" / "literature" / "hs300_rdd_import"
    import_paths = [
        root / "data" / "raw" / "hs300_rdd_candidates.csv",
        import_dir / "candidate_batch_audit.csv",
        import_dir / "import_summary.md",
        root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv",
    ]
    return {
        "status": status,
        "collection_status": build_collection_status(root=root),
        "online_collection_status": build_online_collection_status(root=root),
        "collection_tables": build_collection_preview_tables(root=root),
        "online_collection_tables": build_online_collection_preview_tables(root=root),
        "import_paths": [_path_payload(path, root=root) for path in import_paths],
        "preflight_result": preflight_result,
        "import_result": import_result,
        "collection_result": collection_result,
        "error": error,
        "commands": [
            "index-inclusion-plan-hs300-rdd-l3 --force",
            "index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --sheet 0 --check-only",
            "index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --sheet 0 --output data/raw/hs300_rdd_candidates.csv --force",
            "index-inclusion-hs300-rdd",
        ],
    }
