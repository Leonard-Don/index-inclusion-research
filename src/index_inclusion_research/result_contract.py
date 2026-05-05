from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pandas as pd

from index_inclusion_research.dashboard_types import RddStatus, RddStatusMode
from index_inclusion_research.rdd_evidence import (
    rdd_coverage_note,
    rdd_evidence_tier,
    rdd_evidence_tier_from_status,
    rdd_source_kind,
    rdd_source_label,
)


def read_csv_if_exists(path: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path, parse_dates=parse_dates, low_memory=False)


def _optional_int(value) -> int | None:
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _optional_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def _rdd_status_mode(value: object) -> RddStatusMode:
    mode = str(value or "missing")
    if mode in {"real", "reconstructed", "demo", "missing"}:
        return cast(RddStatusMode, mode)
    return "missing"


def load_rdd_status(
    root: Path,
    *,
    output_dir: Path | None = None,
    read_csv_if_exists_fn=read_csv_if_exists,
) -> RddStatus:
    rdd_dir = output_dir or (root / "results" / "literature" / "hs300_rdd")
    status_path = rdd_dir / "rdd_status.csv"
    if status_path.exists():
        status_frame = read_csv_if_exists_fn(status_path)
        if not status_frame.empty:
            row = status_frame.iloc[0]
            mode = _rdd_status_mode(row.get("status", "missing"))
            evidence_status = str(row.get("evidence_status", "待补正式样本"))
            evidence_tier = ("" if pd.isna(row.get("evidence_tier")) else str(row.get("evidence_tier"))) or rdd_evidence_tier(mode)
            if evidence_tier == "—":
                evidence_tier = rdd_evidence_tier_from_status(evidence_status)
            candidate_rows = _optional_int(row.get("candidate_rows"))
            candidate_batches = _optional_int(row.get("candidate_batches"))
            treated_rows = _optional_int(row.get("treated_rows"))
            control_rows = _optional_int(row.get("control_rows"))
            crossing_batches = _optional_int(row.get("crossing_batches"))
            validation_error = _optional_text(row.get("validation_error"))
            input_file = _optional_text(row.get("input_file"))
            source_file = _optional_text(row.get("source_file")) or input_file
            coverage_note = _optional_text(row.get("coverage_note")) or rdd_coverage_note(
                mode,
                candidate_rows=candidate_rows,
                candidate_batches=candidate_batches,
                treated_rows=treated_rows,
                control_rows=control_rows,
                crossing_batches=crossing_batches,
                validation_error=validation_error,
            )
            return {
                "mode": mode,
                "evidence_tier": evidence_tier,
                "evidence_status": evidence_status,
                "source_kind": _optional_text(row.get("source_kind")) or rdd_source_kind(mode),
                "source_label": _optional_text(row.get("source_label")) or rdd_source_label(mode),
                "source_file": source_file,
                "generated_at": _optional_text(row.get("generated_at")),
                "as_of_date": _optional_text(row.get("as_of_date")),
                "batch_label": _optional_text(row.get("batch_label")),
                "coverage_note": coverage_note,
                "message": str(row.get("message", "等待真实候选样本文件。")),
                "note": str(row.get("note", "等待正式候选样本、公开重建样本，或修复文件校验错误后，RDD 才能进入 L2/L3 证据等级。")),
                "input_file": input_file,
                "audit_file": _optional_text(row.get("audit_file")),
                "candidate_rows": candidate_rows,
                "candidate_batches": candidate_batches,
                "treated_rows": treated_rows,
                "control_rows": control_rows,
                "crossing_batches": crossing_batches,
                "validation_error": validation_error,
            }

    summary_path = rdd_dir / "summary.md"
    if summary_path.exists():
        summary_text = summary_path.read_text(encoding="utf-8")
        if "显式 `--demo` 模式" in summary_text or "demo 伪排名数据" in summary_text:
            return {
                "mode": "demo",
                "evidence_tier": rdd_evidence_tier("demo"),
                "evidence_status": "方法展示",
                "source_kind": rdd_source_kind("demo"),
                "source_label": rdd_source_label("demo"),
                "source_file": "",
                "generated_at": "",
                "as_of_date": "",
                "batch_label": "",
                "coverage_note": rdd_coverage_note("demo"),
                "message": "当前为显式 demo 模式，只用于方法展示。",
                "note": "当前为显式 demo 模式，只用于方法展示，不进入正式证据链。",
                "input_file": "",
                "audit_file": "",
                "candidate_rows": None,
                "candidate_batches": None,
                "treated_rows": None,
                "control_rows": None,
                "crossing_batches": None,
                "validation_error": "",
            }
        if "当前正在使用公开数据重建的候选样本文件" in summary_text:
            return {
                "mode": "reconstructed",
                "evidence_tier": rdd_evidence_tier("reconstructed"),
                "evidence_status": "公开重建样本",
                "source_kind": rdd_source_kind("reconstructed"),
                "source_label": rdd_source_label("reconstructed"),
                "source_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
                "generated_at": "",
                "as_of_date": "",
                "batch_label": "",
                "coverage_note": rdd_coverage_note("reconstructed"),
                "message": "当前正在使用公开数据重建的候选样本文件。",
                "note": "基于公开数据重建的边界样本，可进入公开数据版证据链，但不应表述为中证官方历史候选排名表。",
                "input_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
                "audit_file": "",
                "candidate_rows": None,
                "candidate_batches": None,
                "treated_rows": None,
                "control_rows": None,
                "crossing_batches": None,
                "validation_error": "",
            }
        if "当前正在使用你提供的真实候选排名文件" in summary_text:
            return {
                "mode": "real",
                "evidence_tier": rdd_evidence_tier("real"),
                "evidence_status": "正式边界样本",
                "source_kind": rdd_source_kind("real"),
                "source_label": rdd_source_label("real"),
                "source_file": "",
                "generated_at": "",
                "as_of_date": "",
                "batch_label": "",
                "coverage_note": rdd_coverage_note("real"),
                "message": "当前正在使用你提供的真实候选排名文件。",
                "note": "基于真实候选排名变量，可作为更强识别证据。",
                "input_file": "",
                "audit_file": "",
                "candidate_rows": None,
                "candidate_batches": None,
                "treated_rows": None,
                "control_rows": None,
                "crossing_batches": None,
                "validation_error": "",
            }
    return {
        "mode": "missing",
        "evidence_tier": rdd_evidence_tier("missing"),
        "evidence_status": "待补正式样本",
        "source_kind": rdd_source_kind("missing"),
        "source_label": rdd_source_label("missing"),
        "source_file": "data/raw/hs300_rdd_candidates.csv",
        "generated_at": "",
        "as_of_date": "",
        "batch_label": "",
        "coverage_note": rdd_coverage_note("missing"),
        "message": "等待正式或公开重建候选样本文件：data/raw/hs300_rdd_candidates.csv 或 data/raw/hs300_rdd_candidates.reconstructed.csv。",
        "note": "等待正式候选样本、公开重建样本，或修复文件校验错误后，RDD 才能进入 L2/L3 证据等级。",
        "input_file": "data/raw/hs300_rdd_candidates.csv",
        "audit_file": "",
        "candidate_rows": None,
        "candidate_batches": None,
        "treated_rows": None,
        "control_rows": None,
        "crossing_batches": None,
        "validation_error": "",
    }


def build_results_manifest(profile: str, rdd_status: Mapping[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "contract_version": 1,
                "profile": profile,
                "rdd_mode": str(rdd_status.get("mode", "missing")),
                "rdd_evidence_tier": str(rdd_status.get("evidence_tier", "")),
                "rdd_evidence_status": str(rdd_status.get("evidence_status", "")),
                "rdd_source_kind": str(rdd_status.get("source_kind", "")),
                "rdd_source_label": str(rdd_status.get("source_label", "")),
                "rdd_source_file": str(rdd_status.get("source_file", "")),
                "rdd_generated_at": str(rdd_status.get("generated_at", "")),
                "rdd_as_of_date": str(rdd_status.get("as_of_date", "")),
                "rdd_batch_label": str(rdd_status.get("batch_label", "")),
                "rdd_coverage_note": str(rdd_status.get("coverage_note", "")),
                "rdd_message": str(rdd_status.get("message", "")),
                "rdd_note": str(rdd_status.get("note", "")),
                "rdd_input_file": str(rdd_status.get("input_file", "")),
                "rdd_audit_file": str(rdd_status.get("audit_file", "")),
                "rdd_candidate_rows": rdd_status.get("candidate_rows"),
                "rdd_candidate_batches": rdd_status.get("candidate_batches"),
                "rdd_treated_rows": rdd_status.get("treated_rows"),
                "rdd_control_rows": rdd_status.get("control_rows"),
                "rdd_crossing_batches": rdd_status.get("crossing_batches"),
                "rdd_validation_error": str(rdd_status.get("validation_error", "")),
            }
        ]
    )


def load_results_manifest(path: str | Path) -> dict[str, Any]:
    manifest = read_csv_if_exists(path)
    if manifest.empty:
        return {}
    row = manifest.iloc[0]
    return {
        "contract_version": _optional_int(row.get("contract_version")) or 1,
        "profile": _optional_text(row.get("profile")),
        "rdd_mode": _optional_text(row.get("rdd_mode")),
        "rdd_evidence_tier": _optional_text(row.get("rdd_evidence_tier")),
        "rdd_evidence_status": _optional_text(row.get("rdd_evidence_status")),
        "rdd_source_kind": _optional_text(row.get("rdd_source_kind")),
        "rdd_source_label": _optional_text(row.get("rdd_source_label")),
        "rdd_source_file": _optional_text(row.get("rdd_source_file")),
        "rdd_generated_at": _optional_text(row.get("rdd_generated_at")),
        "rdd_as_of_date": _optional_text(row.get("rdd_as_of_date")),
        "rdd_batch_label": _optional_text(row.get("rdd_batch_label")),
        "rdd_coverage_note": _optional_text(row.get("rdd_coverage_note")),
        "rdd_message": _optional_text(row.get("rdd_message")),
        "rdd_note": _optional_text(row.get("rdd_note")),
        "rdd_input_file": _optional_text(row.get("rdd_input_file")),
        "rdd_audit_file": _optional_text(row.get("rdd_audit_file")),
        "rdd_candidate_rows": _optional_int(row.get("rdd_candidate_rows")),
        "rdd_candidate_batches": _optional_int(row.get("rdd_candidate_batches")),
        "rdd_treated_rows": _optional_int(row.get("rdd_treated_rows")),
        "rdd_control_rows": _optional_int(row.get("rdd_control_rows")),
        "rdd_crossing_batches": _optional_int(row.get("rdd_crossing_batches")),
        "rdd_validation_error": _optional_text(row.get("rdd_validation_error")),
    }
