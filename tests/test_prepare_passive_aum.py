"""Tests for ``index_inclusion_research.prepare_passive_aum``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.prepare_passive_aum import (
    main,
    normalize_column_names,
    normalize_market_values,
    prepare_passive_aum_frame,
    validate_aum_frame,
)

# ── normalize_column_names ───────────────────────────────────────────


def test_normalize_column_names_maps_known_synonyms() -> None:
    raw = pd.DataFrame(
        {
            "Country": ["US"],
            "Year": [2020],
            "AUM": [7.5],
        }
    )
    out = normalize_column_names(raw)
    assert set(out.columns) == {"market", "year", "aum_trillion"}


def test_normalize_column_names_keeps_unknown_columns() -> None:
    raw = pd.DataFrame({"market": ["CN"], "year": [2020], "extra": [1]})
    out = normalize_column_names(raw)
    assert "extra" in out.columns


def test_normalize_column_names_handles_chinese_headers() -> None:
    raw = pd.DataFrame({"市场": ["CN"], "年份": [2020], "被动AUM": [1.2]})
    out = normalize_column_names(raw)
    assert set(out.columns) >= {"market", "year", "aum_trillion"}


# ── normalize_market_values ──────────────────────────────────────────


def test_normalize_market_values_maps_common_spellings() -> None:
    raw = pd.DataFrame(
        {"market": ["China", "USA", "美国", "中国", "us", "CN"], "year": [2020] * 6, "aum_trillion": [1.0] * 6}
    )
    out = normalize_market_values(raw)
    assert list(out["market"]) == ["CN", "US", "US", "CN", "US", "CN"]


def test_normalize_market_values_passes_through_unknown() -> None:
    raw = pd.DataFrame({"market": ["EUR", "JPN"], "year": [2020, 2021], "aum_trillion": [1.0, 2.0]})
    out = normalize_market_values(raw)
    assert list(out["market"]) == ["EUR", "JPN"]


# ── validate_aum_frame ───────────────────────────────────────────────


def test_validate_aum_frame_drops_unknown_market() -> None:
    raw = pd.DataFrame(
        {
            "market": ["CN", "EUR", "US"],
            "year": [2020, 2020, 2020],
            "aum_trillion": [1.0, 0.5, 7.0],
        }
    )
    out, issues = validate_aum_frame(raw)
    assert len(out) == 2
    assert "EUR" in " ".join(issues) or "unknown" in " ".join(issues).lower()


def test_validate_aum_frame_drops_invalid_year() -> None:
    raw = pd.DataFrame(
        {
            "market": ["CN", "US"],
            "year": ["abc", 2020],
            "aum_trillion": [1.0, 7.0],
        }
    )
    out, _ = validate_aum_frame(raw)
    assert len(out) == 1
    assert int(out["year"].iloc[0]) == 2020


def test_validate_aum_frame_drops_non_positive_aum() -> None:
    raw = pd.DataFrame(
        {
            "market": ["CN", "US", "CN"],
            "year": [2020, 2020, 2021],
            "aum_trillion": [-1.0, 7.0, 0.0],
        }
    )
    out, _ = validate_aum_frame(raw)
    assert len(out) == 1
    assert out["market"].iloc[0] == "US"


def test_validate_aum_frame_returns_canonical_column_order_and_sort() -> None:
    raw = pd.DataFrame(
        {
            "market": ["US", "CN", "US", "CN"],
            "year": [2018, 2018, 2014, 2014],
            "aum_trillion": [3.5, 1.0, 2.0, 0.7],
        }
    )
    out, _ = validate_aum_frame(raw)
    assert list(out.columns) == ["market", "year", "aum_trillion"]
    # CN sorted by year ascending, then US
    assert list(out["market"]) == ["CN", "CN", "US", "US"]
    assert list(out["year"]) == [2014, 2018, 2014, 2018]


def test_validate_aum_frame_reports_missing_columns() -> None:
    raw = pd.DataFrame({"foo": [1], "bar": [2]})
    out, issues = validate_aum_frame(raw)
    assert out.empty
    assert any("Missing required columns" in i for i in issues)


# ── prepare_passive_aum_frame (full pipeline) ────────────────────────


def test_prepare_passive_aum_frame_round_trips_messy_input() -> None:
    raw = pd.DataFrame(
        {
            "Country": ["China", "China", "USA", "USA"],
            "Year": [2014, 2020, 2014, 2020],
            "AUM": [0.5, 1.6, 2.0, 7.0],
        }
    )
    out, issues = prepare_passive_aum_frame(raw)
    assert issues == []
    assert list(out.columns) == ["market", "year", "aum_trillion"]
    assert list(out["market"]) == ["CN", "CN", "US", "US"]


# ── CLI ──────────────────────────────────────────────────────────────


def _write_raw(tmp_path: Path) -> Path:
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "Country,Year,AUM\n"
        "China,2014,0.5\n"
        "China,2020,1.6\n"
        "USA,2014,2.0\n"
        "USA,2020,7.0\n"
        "EUR,2014,1.0\n"
    )
    return raw


def test_main_check_only_does_not_write(tmp_path: Path) -> None:
    raw = _write_raw(tmp_path)
    out = tmp_path / "passive_aum.csv"
    rc = main(["--input", str(raw), "--output", str(out), "--check-only"])
    assert rc == 0
    assert not out.exists()


def test_main_writes_normalized_csv(tmp_path: Path) -> None:
    raw = _write_raw(tmp_path)
    out = tmp_path / "passive_aum.csv"
    rc = main(["--input", str(raw), "--output", str(out)])
    assert rc == 0
    written = pd.read_csv(out)
    assert list(written.columns) == ["market", "year", "aum_trillion"]
    # EUR row dropped
    assert len(written) == 4
    assert set(written["market"]) == {"CN", "US"}


def test_main_refuses_overwrite_without_force(tmp_path: Path) -> None:
    raw = _write_raw(tmp_path)
    out = tmp_path / "passive_aum.csv"
    out.write_text("existing,data\n1,2\n")
    rc = main(["--input", str(raw), "--output", str(out)])
    assert rc == 1
    # original content preserved
    assert "existing" in out.read_text()


def test_main_force_overwrites(tmp_path: Path) -> None:
    raw = _write_raw(tmp_path)
    out = tmp_path / "passive_aum.csv"
    out.write_text("existing,data\n1,2\n")
    rc = main(["--input", str(raw), "--output", str(out), "--force"])
    assert rc == 0
    written = pd.read_csv(out)
    assert "market" in written.columns


def test_main_returns_1_when_input_missing(tmp_path: Path) -> None:
    rc = main(["--input", str(tmp_path / "missing.csv"), "--output", str(tmp_path / "x.csv")])
    assert rc == 1


def test_main_returns_1_when_no_valid_rows(tmp_path: Path) -> None:
    raw = tmp_path / "all-invalid.csv"
    raw.write_text("market,year,aum_trillion\nEUR,2020,1.0\nJPN,2020,2.0\n")
    rc = main(["--input", str(raw), "--output", str(tmp_path / "out.csv")])
    assert rc == 1
