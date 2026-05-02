from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.analysis.cross_market_asymmetry.h6_robustness import (
    build_h6_weight_joined_frame,
)
from index_inclusion_research.real_evidence_refresh import compute_cn_sector_coverage
from index_inclusion_research.result_contract import load_rdd_status

ROOT = paths.project_root()
TABLES_DIR = ROOT / "results" / "real_tables"
MANIFEST_PATH = TABLES_DIR / "evidence_refresh_manifest.json"


def _relative_label(path: Path, *, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={"ticker": str}, low_memory=False)
    except (OSError, ValueError):
        return pd.DataFrame()


def _clean_value(value: Any) -> Any:
    if isinstance(value, list | tuple):
        return [_clean_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _clean_value(item) for key, item in value.items()}
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float):
        return float(value)
    if isinstance(value, int):
        return int(value)
    return value


def _records(frame: pd.DataFrame, *, limit: int = 80) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    clean = frame.head(limit).copy()
    return [
        {column: _clean_value(value) for column, value in row.items()}
        for row in clean.to_dict(orient="records")
    ]


def _table_payload(
    key: str,
    title: str,
    frame: pd.DataFrame,
    *,
    source_path: Path | None = None,
    root: Path,
    limit: int = 80,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "source_path": _relative_label(source_path, root=root) if source_path else "",
        "columns": list(frame.columns) if not frame.empty else [],
        "rows": _records(frame, limit=limit),
        "total_rows": int(len(frame)),
        "shown_rows": int(min(len(frame), limit)),
    }


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _manifest_row(manifest: dict[str, Any], item: str) -> dict[str, Any] | None:
    coverage = manifest.get("coverage", [])
    if not isinstance(coverage, list):
        return None
    for row in coverage:
        if isinstance(row, dict) and str(row.get("item", "")) == item:
            return row
    return None


def _base_detail(root: Path, item: str, manifest: dict[str, Any]) -> dict[str, Any] | None:
    row = _manifest_row(manifest, item)
    if row is None:
        return None
    return {
        "item": item,
        "label": str(row.get("label", item) or item),
        "status": str(row.get("status", "")),
        "value": str(row.get("value", "")),
        "detail": str(row.get("detail", "")),
        "generated_at": str(manifest.get("generated_at", "")),
        "source_paths": [],
        "summary_cards": [],
        "tables": [],
        "notes": [],
        "home_href": "/?mode=full#cross_market_asymmetry",
        "root": str(root),
    }


def _add_source(detail: dict[str, Any], path: Path, *, root: Path) -> None:
    detail["source_paths"].append(
        {
            "label": _relative_label(path, root=root),
            "exists": path.exists(),
            "href": f"/files/{_relative_label(path, root=root)}" if path.exists() else "",
        }
    )


def _detail_h2(detail: dict[str, Any], *, root: Path) -> None:
    path = root / "data" / "raw" / "passive_aum.csv"
    frame = _read_csv(path)
    _add_source(detail, path, root=root)
    if not frame.empty and {"market", "year", "aum_trillion"}.issubset(frame.columns):
        summary = (
            frame.assign(year=pd.to_numeric(frame["year"], errors="coerce"))
            .groupby("market", dropna=False)
            .agg(
                rows=("market", "size"),
                first_year=("year", "min"),
                last_year=("year", "max"),
                latest_aum_trillion=("aum_trillion", "last"),
            )
            .reset_index()
        )
        detail["tables"].append(
            _table_payload("aum_market_summary", "AUM market summary", summary, root=root)
        )
    detail["tables"].append(
        _table_payload("passive_aum_rows", "passive_aum.csv preview", frame, source_path=path, root=root)
    )


def _detail_h6(detail: dict[str, Any], *, root: Path) -> None:
    weight_path = root / "data" / "processed" / "hs300_weight_change.csv"
    gap_path = root / "results" / "real_tables" / "cma_gap_event_level.csv"
    robustness_path = root / "results" / "real_tables" / "cma_h6_weight_robustness.csv"
    explanation_path = root / "results" / "real_tables" / "cma_h6_weight_explanation.csv"
    verdicts_path = root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    for path in (weight_path, gap_path, robustness_path, explanation_path, verdicts_path):
        _add_source(detail, path, root=root)

    weights = _read_csv(weight_path)
    gap = _read_csv(gap_path)
    joined = build_h6_weight_joined_frame(weights, gap)
    if not joined.empty:
        preferred = [
            column
            for column in (
                "ticker",
                "announce_date",
                "weight_proxy",
                "announce_jump",
                "sector",
                "batch_id",
                "gap_length_days",
                "event_id",
            )
            if column in joined.columns
        ]
        joined = joined.loc[:, preferred].sort_values(
            by=[column for column in ("announce_date", "ticker") if column in preferred]
        )
    detail["tables"].extend(
        [
            _table_payload("matched_weight_events", "matched H6 event rows", joined, root=root, limit=120),
            _table_payload(
                "h6_weight_explanation",
                "H6 explanation layer",
                _read_csv(explanation_path),
                source_path=explanation_path,
                root=root,
            ),
            _table_payload(
                "h6_weight_robustness",
                "H6 robustness specs",
                _read_csv(robustness_path),
                source_path=robustness_path,
                root=root,
            ),
        ]
    )
    verdicts = _read_csv(verdicts_path)
    if not verdicts.empty and "hid" in verdicts.columns:
        h6 = verdicts.loc[verdicts["hid"].astype(str) == "H6"]
        detail["tables"].append(
            _table_payload("h6_verdict", "H6 verdict row", h6, source_path=verdicts_path, root=root)
        )


def _detail_h7(detail: dict[str, Any], *, root: Path) -> None:
    events_path = root / "data" / "raw" / "real_events.csv"
    metadata_path = root / "data" / "raw" / "real_metadata.csv"
    for path in (events_path, metadata_path):
        _add_source(detail, path, root=root)
    coverage = compute_cn_sector_coverage(root)
    missing = coverage.get("missing_tickers", [])
    missing_rows = pd.DataFrame({"ticker": missing if isinstance(missing, list) else []})
    detail["summary_cards"].append(
        {
            "label": "CN sector coverage",
            "value": f"{int(coverage['known'])}/{int(coverage['total'])}",
            "detail": f"{float(coverage['rate']):.1%}",
        }
    )
    detail["tables"].append(
        _table_payload("missing_cn_sector_tickers", "missing CN sector tickers", missing_rows, root=root)
    )


def _detail_rdd(detail: dict[str, Any], *, root: Path) -> None:
    status = dict(load_rdd_status(root))
    status_frame = pd.DataFrame([status])
    status_path = root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    audit_path = root / "results" / "literature" / "hs300_rdd" / "candidate_batch_audit.csv"
    formal_path = root / "data" / "raw" / "hs300_rdd_candidates.csv"
    reconstructed_path = root / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
    for path in (status_path, audit_path, formal_path, reconstructed_path):
        _add_source(detail, path, root=root)
    detail["summary_cards"].extend(
        [
            {
                "label": "evidence tier",
                "value": status.get("evidence_tier", ""),
                "detail": status.get("evidence_status", ""),
            },
            {
                "label": "active mode",
                "value": status.get("mode", ""),
                "detail": status.get("source_label", ""),
            },
        ]
    )
    detail["tables"].extend(
        [
            _table_payload("rdd_status", "RDD status", status_frame, source_path=status_path, root=root),
            _table_payload(
                "rdd_candidate_batch_audit",
                "candidate batch audit",
                _read_csv(audit_path),
                source_path=audit_path,
                root=root,
            ),
        ]
    )
    detail["notes"].append("L3 只有在 data/raw/hs300_rdd_candidates.csv 为正式候选排名表时才会变为 pass。")


def _detail_verdicts(detail: dict[str, Any], *, root: Path) -> None:
    path = root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    _add_source(detail, path, root=root)
    verdicts = _read_csv(path)
    if not verdicts.empty and "verdict" in verdicts.columns:
        distribution = (
            verdicts["verdict"].astype(str).value_counts().rename_axis("verdict").reset_index(name="count")
        )
        detail["tables"].append(
            _table_payload("verdict_distribution", "verdict distribution", distribution, root=root)
        )
    detail["tables"].append(
        _table_payload("hypothesis_verdicts", "H1-H7 verdict rows", verdicts, source_path=path, root=root)
    )


def _detail_doctor(detail: dict[str, Any], manifest: dict[str, Any], *, root: Path) -> None:
    doctor_payload = manifest.get("doctor", {})
    checks = doctor_payload.get("checks", []) if isinstance(doctor_payload, dict) else []
    frame = pd.DataFrame(checks if isinstance(checks, list) else [])
    _add_source(detail, root / "results" / "real_tables" / "evidence_refresh_manifest.json", root=root)
    detail["tables"].append(_table_payload("doctor_checks", "doctor checks", frame, root=root))


DETAIL_BUILDERS = {
    "H2_passive_aum": _detail_h2,
    "H6_weight_change": _detail_h6,
    "H7_cn_sector": _detail_h7,
    "RDD_L3_boundary": _detail_rdd,
    "CMA_verdicts": _detail_verdicts,
}


def build_evidence_detail(item: str, *, root: Path = ROOT) -> dict[str, Any] | None:
    root = Path(root)
    manifest = _load_manifest(root / "results" / "real_tables" / "evidence_refresh_manifest.json")
    detail = _base_detail(root, item, manifest)
    if detail is None:
        return None
    if item == "doctor":
        _detail_doctor(detail, manifest, root=root)
        return detail
    builder = DETAIL_BUILDERS.get(item)
    if builder is None:
        return None
    builder(detail, root=root)
    return detail
