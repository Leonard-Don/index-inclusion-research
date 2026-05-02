"""Normalize a raw passive-fund AUM CSV / Excel into ``data/raw/passive_aum.csv``.

Output schema (consumed by ``index-inclusion-cma --aum``):

    market         CN | US
    year           int (e.g. 2014, 2020)
    aum_trillion   positive float, AUM in trillions of local currency

Inputs are flexible: column names, casing, and market spellings are
normalized. Validation drops rows with unknown markets, non-numeric or
non-positive AUM, or invalid year. A short summary is printed and an
optional ``--check-only`` flag runs the full pipeline without touching
the output file.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

from index_inclusion_research import paths

ROOT = paths.project_root()
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "passive_aum.csv"

REQUIRED_COLS: tuple[str, ...] = ("market", "year", "aum_trillion")
ALLOWED_MARKETS: frozenset[str] = frozenset({"CN", "US"})


# Synonyms used to normalize free-form column names to canonical ones.
# Keys are lower-cased; lookup is case-insensitive.
COLUMN_SYNONYMS: dict[str, str] = {
    "market": "market",
    "country": "market",
    "region": "market",
    "国家": "market",
    "市场": "market",
    "year": "year",
    "yr": "year",
    "年份": "year",
    "年": "year",
    "aum_trillion": "aum_trillion",
    "aum": "aum_trillion",
    "aum_t": "aum_trillion",
    "aum (trillion)": "aum_trillion",
    "passive_aum_trillion": "aum_trillion",
    "passive_aum": "aum_trillion",
    "被动aum": "aum_trillion",
    "被动 aum": "aum_trillion",
    "被动基金 aum": "aum_trillion",
}


# Synonyms used to normalize market spellings to canonical CN / US codes.
MARKET_SYNONYMS: dict[str, str] = {
    "cn": "CN",
    "china": "CN",
    "中国": "CN",
    "中国大陆": "CN",
    "a股": "CN",
    "a-share": "CN",
    "a-shares": "CN",
    "us": "US",
    "usa": "US",
    "u.s.": "US",
    "united states": "US",
    "united states of america": "US",
    "美国": "US",
    "美股": "US",
}


def normalize_column_names(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *frame* with columns mapped via ``COLUMN_SYNONYMS``.

    Unrecognized columns are kept as-is (so callers can later spot
    leftover header garbage).
    """
    rename: dict[str, str] = {}
    for col in frame.columns:
        key = str(col).strip().lower()
        if key in COLUMN_SYNONYMS:
            rename[col] = COLUMN_SYNONYMS[key]
    return frame.rename(columns=rename)


def normalize_market_values(frame: pd.DataFrame) -> pd.DataFrame:
    """Map common spellings of CN / US to the canonical 2-letter codes."""
    if "market" not in frame.columns:
        return frame
    out = frame.copy()

    def _map_value(raw: object) -> object:
        if pd.isna(raw):
            return raw
        text = str(raw).strip()
        if text in ALLOWED_MARKETS:
            return text
        return MARKET_SYNONYMS.get(text.lower(), text)

    out["market"] = out["market"].map(_map_value)
    return out


def validate_aum_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Coerce types, drop bad rows, return ``(clean_frame, list_of_issues)``."""
    issues: list[str] = []
    missing = [c for c in REQUIRED_COLS if c not in frame.columns]
    if missing:
        return (
            pd.DataFrame(columns=list(REQUIRED_COLS)),
            [f"Missing required columns: {', '.join(missing)}"],
        )

    out = frame[list(REQUIRED_COLS)].copy()
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out["aum_trillion"] = pd.to_numeric(out["aum_trillion"], errors="coerce")

    invalid_market = ~out["market"].isin(ALLOWED_MARKETS)
    if invalid_market.any():
        bad = sorted(
            {str(v) for v in out.loc[invalid_market, "market"].dropna().unique()}
        )
        issues.append(
            f"Dropped {int(invalid_market.sum())} rows with unknown market values: {bad}"
        )
        out = out.loc[~invalid_market]

    bad_year = out["year"].isna()
    if bad_year.any():
        issues.append(
            f"Dropped {int(bad_year.sum())} rows with non-numeric / missing year."
        )
        out = out.loc[~bad_year]

    bad_aum = out["aum_trillion"].isna() | (out["aum_trillion"] <= 0)
    if bad_aum.any():
        issues.append(
            f"Dropped {int(bad_aum.sum())} rows with non-positive aum_trillion."
        )
        out = out.loc[~bad_aum]

    out = out.sort_values(["market", "year"]).reset_index(drop=True)
    out["year"] = out["year"].astype(int)
    return out, issues


def read_aum_input(path: Path, *, sheet: str | int | None = None) -> pd.DataFrame:
    """Read CSV (any extension other than .xlsx/.xls) or Excel."""
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet if sheet is not None else 0)
    return pd.read_csv(path)


def prepare_passive_aum_frame(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """Full normalization + validation pipeline."""
    work = normalize_column_names(frame)
    work = normalize_market_values(work)
    return validate_aum_frame(work)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize a raw passive-fund AUM CSV / Excel into "
            "data/raw/passive_aum.csv (consumed by index-inclusion-cma --aum)."
        )
    )
    parser.add_argument(
        "--input", required=True, help="Path to raw AUM CSV or Excel file."
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output path for the normalized CSV (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name or 0-indexed integer (only used for .xlsx / .xls).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing output file.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Run validation only — don't write the output file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        print(f"[prepare-passive-aum] input file not found: {input_path}")
        return 1

    sheet: str | int | None = None
    if args.sheet is not None:
        sheet = int(args.sheet) if args.sheet.isdigit() else args.sheet

    raw = read_aum_input(input_path, sheet=sheet)
    normalized, issues = prepare_passive_aum_frame(raw)

    if normalized.empty:
        print("[prepare-passive-aum] no valid AUM rows after normalization. Issues:")
        for issue in issues:
            print(f"  - {issue}")
        return 1

    cn_count = int((normalized["market"] == "CN").sum())
    us_count = int((normalized["market"] == "US").sum())
    year_min = int(normalized["year"].min())
    year_max = int(normalized["year"].max())
    print(
        f"[prepare-passive-aum] normalized {len(normalized)} rows "
        f"({cn_count} CN, {us_count} US; years {year_min}–{year_max})"
    )
    for issue in issues:
        print(f"  ⚠️ {issue}")

    if args.check_only:
        print("[prepare-passive-aum] check-only mode — not writing output.")
        return 0

    if output_path.exists() and not args.force:
        print(
            f"[prepare-passive-aum] refusing to overwrite existing file "
            f"without --force: {output_path}"
        )
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output_path, index=False)
    print(f"[prepare-passive-aum] wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
