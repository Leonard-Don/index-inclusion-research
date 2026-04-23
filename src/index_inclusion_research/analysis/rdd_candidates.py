from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "batch_id",
    "market",
    "index_name",
    "ticker",
    "security_name",
    "announce_date",
    "effective_date",
    "running_variable",
    "cutoff",
    "inclusion",
]
OPTIONAL_COLUMNS = [
    "event_type",
    "source",
    "source_url",
    "note",
    "sector",
]

AUDIT_COLUMNS = [
    "batch_id",
    "market",
    "index_name",
    "announce_date",
    "effective_date",
    "n_candidates",
    "n_included",
    "n_excluded",
    "n_left_of_cutoff",
    "n_right_of_cutoff",
    "min_running_variable",
    "max_running_variable",
    "closest_left_distance",
    "closest_right_distance",
    "n_unique_cutoffs",
    "n_unique_announce_dates",
    "n_unique_effective_dates",
    "duplicate_ticker_rows",
    "has_cutoff_crossing",
    "has_treated_and_control",
]

COLUMN_ALIASES = {
    "batch_id": {"batch_id", "batchid", "batch", "批次", "批次id", "批次日期", "调样批次"},
    "market": {"market", "市场", "市场代码"},
    "index_name": {"index_name", "index", "指数", "指数名称", "指数名"},
    "ticker": {"ticker", "code", "证券代码", "股票代码", "代码", "成分股代码"},
    "security_name": {"security_name", "security", "name", "证券简称", "股票简称", "证券名称", "股票名称", "公司简称"},
    "announce_date": {"announce_date", "announcedate", "公告日", "公告日期", "调样公告日"},
    "effective_date": {"effective_date", "effectivedate", "生效日", "生效日期", "实施日", "实施日期"},
    "running_variable": {"running_variable", "runningvariable", "rank", "ranking", "排名", "候选排名", "排序", "排序指标", "排名变量"},
    "cutoff": {"cutoff", "threshold", "断点", "门槛", "阈值", "cutoffrank"},
    "inclusion": {"inclusion", "included", "是否调入", "是否纳入", "纳入", "是否入选", "是否调样成功"},
    "event_type": {"event_type", "eventtype", "事件类型"},
    "source": {"source", "来源", "数据来源"},
    "source_url": {"source_url", "sourceurl", "url", "链接", "来源链接", "来源url"},
    "note": {"note", "notes", "备注", "说明"},
    "sector": {"sector", "行业", "行业分类"},
}

POSITIVE_INCLUSION_VALUES = {
    "1",
    "1.0",
    "yes",
    "y",
    "true",
    "t",
    "included",
    "inclusion",
    "addition",
    "add",
    "调入",
    "纳入",
    "是",
}
NEGATIVE_INCLUSION_VALUES = {
    "0",
    "0.0",
    "no",
    "n",
    "false",
    "f",
    "excluded",
    "exclusion",
    "deletion",
    "delete",
    "removal",
    "调出",
    "剔除",
    "否",
}

_ALIAS_LOOKUP = {
    re.sub(r"[\s_\-:/\\|,.()（）【】\[\]·]+", "", alias.strip().lower()): canonical
    for canonical, aliases in COLUMN_ALIASES.items()
    for alias in aliases
}


def read_candidate_input(path: str | Path, *, sheet_name: str | int | None = None) -> pd.DataFrame:
    input_path = Path(path)
    suffix = input_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_path, sheet_name=sheet_name or 0)
    if suffix == ".tsv":
        return pd.read_csv(input_path, sep="\t", low_memory=False)
    if suffix in {".txt", ".csv"}:
        return pd.read_csv(input_path, low_memory=False)
    raise ValueError(f"Unsupported candidate input format: {input_path.suffix or '<none>'}")


def prepare_candidate_frame(
    frame: pd.DataFrame,
    *,
    defaults: Mapping[str, object] | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    work = frame.copy()
    work = work.loc[:, [column for column in work.columns if str(column).strip() and not str(column).startswith("Unnamed:")]].copy()
    work = work.dropna(how="all").reset_index(drop=True)

    rename_map: dict[object, str] = {}
    mapped_columns: dict[str, str] = {}
    duplicate_mappings: list[str] = []
    for column in work.columns:
        slug = _column_slug(column)
        canonical = _ALIAS_LOOKUP.get(slug)
        if canonical is None:
            continue
        if canonical in mapped_columns:
            duplicate_mappings.append(f"{mapped_columns[canonical]} / {column} -> {canonical}")
            continue
        mapped_columns[canonical] = str(column)
        rename_map[column] = canonical
    if duplicate_mappings:
        joined = "; ".join(duplicate_mappings)
        raise ValueError(f"Multiple input columns map to the same canonical RDD field: {joined}")

    work = work.rename(columns=rename_map)
    defaults_applied: list[str] = []
    derived_fields: list[str] = []
    for column, value in (defaults or {}).items():
        if value is None or column not in {*(REQUIRED_COLUMNS), *(OPTIONAL_COLUMNS)}:
            continue
        if column not in work.columns:
            work[column] = value
            defaults_applied.append(column)
            continue
        mask = work[column].isna()
        string_mask = work[column].astype("string").str.strip().eq("").fillna(False)
        mask = mask | string_mask
        if mask.any():
            work.loc[mask, column] = value
            defaults_applied.append(column)

    if "batch_id" not in work.columns and "announce_date" in work.columns:
        announce_dates = pd.to_datetime(work["announce_date"], errors="coerce", format="mixed").dropna()
        unique_dates = announce_dates.dt.normalize().unique()
        if len(unique_dates) == 1:
            work["batch_id"] = pd.Timestamp(unique_dates[0]).strftime("%Y-%m-%d")
            derived_fields.append("batch_id")

    for column in ["batch_id", "market", "index_name", "ticker", "security_name", "event_type", "source", "source_url", "note", "sector"]:
        if column in work.columns:
            work[column] = work[column].astype("string").str.strip()

    if "market" in work.columns:
        work["market"] = work["market"].str.upper()
    if "ticker" in work.columns:
        cn_mask = work.get("market", pd.Series(index=work.index, dtype="string")).astype("string").str.upper().eq("CN")
        work.loc[cn_mask, "ticker"] = work.loc[cn_mask, "ticker"].str.zfill(6)
    for column in ["running_variable", "cutoff"]:
        if column in work.columns:
            work[column] = work[column].map(_clean_numeric_token)
    if "inclusion" in work.columns:
        work["inclusion"] = work["inclusion"].map(_normalize_inclusion_value)

    metadata = {
        "input_rows": len(frame),
        "output_rows": len(work),
        "mapped_columns": mapped_columns,
        "unused_columns": [str(column) for column in frame.columns if column not in rename_map],
        "defaults_applied": sorted(set(defaults_applied)),
        "derived_fields": derived_fields,
    }
    return work, metadata


def validate_candidate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("RDD candidate file is empty.")

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"RDD candidate file is missing required columns: {', '.join(missing_columns)}")

    work = frame.copy()
    string_columns = ["batch_id", "market", "index_name", "ticker", "security_name"]
    for column in string_columns:
        work[column] = work[column].astype("string").str.strip()
        if work[column].isna().any() or (work[column] == "").any():
            raise ValueError(f"RDD candidate file contains empty values in required column: {column}")

    for column in ["announce_date", "effective_date"]:
        work[column] = pd.to_datetime(work[column], errors="coerce", format="mixed")
        if work[column].isna().any():
            raise ValueError(f"RDD candidate file contains invalid dates in required column: {column}")

    for column in ["running_variable", "cutoff"]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
        if work[column].isna().any():
            raise ValueError(f"RDD candidate file contains non-numeric values in required column: {column}")

    work["inclusion"] = pd.to_numeric(work["inclusion"], errors="coerce")
    if work["inclusion"].isna().any():
        raise ValueError("RDD candidate file contains non-numeric values in required column: inclusion")
    unique_inclusion = set(work["inclusion"].astype(int).unique())
    if not unique_inclusion.issubset({0, 1}):
        raise ValueError("RDD candidate file must encode inclusion as 0/1.")
    work["inclusion"] = work["inclusion"].astype(int)

    if "event_type" not in work.columns:
        work["event_type"] = "inclusion_rdd"
    else:
        work["event_type"] = work["event_type"].astype("string").fillna("inclusion_rdd").str.strip().replace("", "inclusion_rdd")

    ordered_columns = [*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS]
    return work.loc[:, [column for column in ordered_columns if column in work.columns]].copy()


def build_candidate_batch_audit(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(columns=AUDIT_COLUMNS)

    work = candidates.copy()
    work["distance_to_cutoff"] = work["running_variable"] - work["cutoff"]
    rows: list[dict[str, object]] = []
    for batch_id, group in work.groupby("batch_id", dropna=False):
        distance = group["distance_to_cutoff"]
        left = distance.loc[distance < 0]
        right = distance.loc[distance >= 0]
        rows.append(
            {
                "batch_id": batch_id,
                "market": group["market"].iloc[0],
                "index_name": group["index_name"].iloc[0],
                "announce_date": group["announce_date"].iloc[0],
                "effective_date": group["effective_date"].iloc[0],
                "n_candidates": int(len(group)),
                "n_included": int((group["inclusion"] == 1).sum()),
                "n_excluded": int((group["inclusion"] == 0).sum()),
                "n_left_of_cutoff": int((distance < 0).sum()),
                "n_right_of_cutoff": int((distance >= 0).sum()),
                "min_running_variable": float(group["running_variable"].min()),
                "max_running_variable": float(group["running_variable"].max()),
                "closest_left_distance": float(left.max()) if not left.empty else pd.NA,
                "closest_right_distance": float(right.min()) if not right.empty else pd.NA,
                "n_unique_cutoffs": int(group["cutoff"].nunique(dropna=False)),
                "n_unique_announce_dates": int(group["announce_date"].nunique(dropna=False)),
                "n_unique_effective_dates": int(group["effective_date"].nunique(dropna=False)),
                "duplicate_ticker_rows": int(group.duplicated(subset=["ticker"], keep=False).sum()),
                "has_cutoff_crossing": bool(not left.empty and not right.empty),
                "has_treated_and_control": bool((group["inclusion"] == 1).any() and (group["inclusion"] == 0).any()),
            }
        )
    audit = pd.DataFrame(rows, columns=AUDIT_COLUMNS)
    if not audit.empty:
        audit = audit.sort_values(["announce_date", "effective_date", "batch_id"], na_position="last").reset_index(drop=True)
    return audit


def summarize_candidate_audit(audit: pd.DataFrame) -> dict[str, int | None]:
    if audit.empty:
        return {
            "candidate_batches": None,
            "treated_rows": None,
            "control_rows": None,
            "crossing_batches": None,
        }
    return {
        "candidate_batches": int(len(audit)),
        "treated_rows": int(audit["n_included"].sum()),
        "control_rows": int(audit["n_excluded"].sum()),
        "crossing_batches": int(audit["has_cutoff_crossing"].sum()),
    }


def _column_slug(name: object) -> str:
    return re.sub(r"[\s_\-:/\\|,.()（）【】\[\]·]+", "", str(name).strip().lower())


def _clean_numeric_token(value: object) -> object:
    if value is None or pd.isna(value):
        return pd.NA
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        return cleaned if cleaned else pd.NA
    return value


def _normalize_inclusion_value(value: object) -> object:
    if value is None or pd.isna(value):
        return pd.NA
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)) and not pd.isna(value):
        if float(value).is_integer():
            return int(value)
        return value
    token = str(value).strip().lower()
    if token in POSITIVE_INCLUSION_VALUES:
        return 1
    if token in NEGATIVE_INCLUSION_VALUES:
        return 0
    return value
