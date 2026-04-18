from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def rdd_evidence_tier(mode: str) -> str:
    return {
        "real": "L3",
        "reconstructed": "L2",
        "demo": "L1",
        "missing": "L0",
    }.get(mode, "—")


def rdd_evidence_tier_from_status(status: str) -> str:
    return {
        "正式样本": "L3",
        "正式边界样本": "L3",
        "公开重建样本": "L2",
        "方法展示": "L1",
        "待补正式样本": "L0",
        "未生成": "—",
    }.get(status, "—")


def rdd_source_kind(mode: str) -> str:
    return {
        "real": "official",
        "reconstructed": "reconstructed",
        "demo": "demo",
        "missing": "missing",
    }.get(mode, "unknown")


def rdd_source_label(mode: str) -> str:
    return {
        "real": "正式候选样本文件",
        "reconstructed": "公开重建候选样本文件",
        "demo": "demo 伪排名样本",
        "missing": "待补候选样本",
    }.get(mode, "未知来源")


def _status_int(value: Any) -> int | None:
    if value in (None, "", "nan"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _status_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def rdd_coverage_note(
    mode: str,
    *,
    candidate_rows: Any = None,
    candidate_batches: Any = None,
    treated_rows: Any = None,
    control_rows: Any = None,
    crossing_batches: Any = None,
    validation_error: Any = "",
) -> str:
    rows = _status_int(candidate_rows)
    batches = _status_int(candidate_batches)
    treated = _status_int(treated_rows)
    controls = _status_int(control_rows)
    crossings = _status_int(crossing_batches)
    error = _status_text(validation_error)
    if batches is not None:
        parts: list[str] = []
        if rows is not None:
            parts.append(f"{rows:,} 条候选")
        parts.append(f"{batches} 个批次")
        if treated is not None or controls is not None:
            parts.append(f"调入 {treated or 0} / 对照 {controls or 0}")
        if crossings is not None:
            parts.append(f"{crossings} 个批次覆盖 cutoff 两侧")
        return "；".join(parts) + "。"
    if error:
        return f"最近一次校验失败：{error}。"
    if mode == "real":
        return "正式候选样本已通过校验，可直接作为 L3 证据来源。"
    if mode == "reconstructed":
        return "公开重建样本已通过校验，可作为 L2 证据来源。"
    if mode == "demo":
        return "当前使用 demo 伪排名样本，仅用于方法展示。"
    return "尚未提供可校验的正式或公开重建候选样本。"


def rdd_provenance_summary(status: Mapping[str, Any]) -> str:
    mode = _status_text(status.get("mode")) or "missing"
    source_label = _status_text(status.get("source_label")) or rdd_source_label(mode)
    batch_label = _status_text(status.get("batch_label"))
    as_of_date = _status_text(status.get("as_of_date"))
    rows = _status_int(status.get("candidate_rows"))
    parts = [source_label]
    if batch_label:
        parts.append(f"批次 {batch_label}")
    elif as_of_date:
        parts.append(f"公告日 {as_of_date}")
    if rows is not None:
        parts.append(f"{rows:,} 条候选")
    return " · ".join(part for part in parts if part)
