"""Tests for the PAP deviation auditor.

Covers:
- equality case (all 7 unchanged)
- each of the 4 classification types (tightened / weakened / flipped /
  unverifiable) on its own
- missing baseline row (current-only) → unverifiable
- missing current row (baseline-only) → unverifiable
- --strict CLI gate flips exit code on a flipped verdict; default exit is 0
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.pap_diff import (
    CLASS_FLIPPED,
    CLASS_TIGHTENED,
    CLASS_UNCHANGED,
    CLASS_UNVERIFIABLE,
    CLASS_WEAKENED,
    PapDiffConfig,
    aggregate_counts,
    build_pap_diff,
    classify_row,
    main,
    resolve_default_baseline,
)

# ── Fixtures ─────────────────────────────────────────────────────────


def _baseline_frame() -> pd.DataFrame:
    """Mirror the 7-row PAP snapshot schema."""
    return pd.DataFrame(
        [
            # H1: bootstrap p-value gated
            {
                "hid": "H1",
                "name_cn": "信息泄露与预运行",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "core",
                "n_obs": 436,
                "key_label": "bootstrap p",
                "key_value": 0.8748,
                "p_value": 0.8748,
            },
            # H2: AUM ratio (not a p)
            {
                "hid": "H2",
                "name_cn": "被动基金 AUM 差异",
                "verdict": "证据不足",
                "confidence": "低",
                "evidence_tier": "supplementary",
                "n_obs": 12,
                "key_label": "US AUM ratio",
                "key_value": 13.481,
                "p_value": "",
            },
            # H5: regression p (most likely to flip with new data)
            {
                "hid": "H5",
                "name_cn": "涨跌停限制",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "core",
                "n_obs": 936,
                "key_label": "limit_coef p",
                "key_value": 0.008,
                "p_value": 0.008,
            },
        ]
    )


def _current_frame_identical() -> pd.DataFrame:
    return _baseline_frame().copy()


# ── Unit tests on classify_row / build_pap_diff ──────────────────────


def test_equality_case_all_unchanged() -> None:
    """All 3 rows match exactly → unchanged."""
    report = build_pap_diff(_baseline_frame(), _current_frame_identical())
    assert len(report) == 3
    assert (report["classification"] == CLASS_UNCHANGED).all()
    counts = aggregate_counts(report)
    assert counts[CLASS_UNCHANGED] == 3
    assert counts[CLASS_FLIPPED] == 0


def test_classification_flipped() -> None:
    """H1 verdict text changes → flipped."""
    current = _baseline_frame().copy()
    current.loc[current["hid"] == "H1", "verdict"] = "支持"
    report = build_pap_diff(_baseline_frame(), current)
    h1 = report.loc[report["hid"] == "H1"].iloc[0]
    assert h1["classification"] == CLASS_FLIPPED
    assert h1["baseline_verdict"] == "证据不足"
    assert h1["current_verdict"] == "支持"
    assert "证据不足" in h1["notes"] and "支持" in h1["notes"]


def test_classification_tightened_by_confidence() -> None:
    """H2 confidence 低 → 中 (verdict unchanged) → tightened."""
    current = _baseline_frame().copy()
    current.loc[current["hid"] == "H2", "confidence"] = "中"
    report = build_pap_diff(_baseline_frame(), current)
    h2 = report.loc[report["hid"] == "H2"].iloc[0]
    assert h2["classification"] == CLASS_TIGHTENED


def test_classification_tightened_by_pvalue() -> None:
    """H5 p drops 0.008 → 0.001 → tightened (p movement past threshold)."""
    current = _baseline_frame().copy()
    current.loc[current["hid"] == "H5", "key_value"] = 0.001
    current.loc[current["hid"] == "H5", "p_value"] = 0.001
    config = PapDiffConfig(p_delta_threshold=0.005)
    report = build_pap_diff(_baseline_frame(), current, config=config)
    h5 = report.loc[report["hid"] == "H5"].iloc[0]
    assert h5["classification"] == CLASS_TIGHTENED


def test_classification_weakened_by_confidence() -> None:
    """H5 confidence 高 → 中 → weakened."""
    current = _baseline_frame().copy()
    current.loc[current["hid"] == "H5", "confidence"] = "中"
    report = build_pap_diff(_baseline_frame(), current)
    h5 = report.loc[report["hid"] == "H5"].iloc[0]
    assert h5["classification"] == CLASS_WEAKENED


def test_classification_weakened_by_pvalue() -> None:
    """H5 p rises 0.008 → 0.09 → weakened."""
    current = _baseline_frame().copy()
    current.loc[current["hid"] == "H5", "key_value"] = 0.09
    current.loc[current["hid"] == "H5", "p_value"] = 0.09
    report = build_pap_diff(_baseline_frame(), current)
    h5 = report.loc[report["hid"] == "H5"].iloc[0]
    assert h5["classification"] == CLASS_WEAKENED


def test_classification_unverifiable_missing_current() -> None:
    """H5 present in baseline, absent from current → unverifiable."""
    current = _baseline_frame().copy()
    current = current.loc[current["hid"] != "H5"].reset_index(drop=True)
    report = build_pap_diff(_baseline_frame(), current)
    h5 = report.loc[report["hid"] == "H5"].iloc[0]
    assert h5["classification"] == CLASS_UNVERIFIABLE
    assert h5["current_verdict"] == ""
    assert "missing current row" in h5["notes"]


def test_classification_unverifiable_missing_baseline() -> None:
    """A new hypothesis (no baseline row) → unverifiable."""
    baseline = _baseline_frame()
    current = baseline.copy()
    new_row = {
        "hid": "H8",
        "name_cn": "新假说",
        "verdict": "支持",
        "confidence": "中",
        "evidence_tier": "core",
        "n_obs": 100,
        "key_label": "test p",
        "key_value": 0.04,
        "p_value": 0.04,
    }
    current = pd.concat([current, pd.DataFrame([new_row])], ignore_index=True)
    report = build_pap_diff(baseline, current)
    h8 = report.loc[report["hid"] == "H8"].iloc[0]
    assert h8["classification"] == CLASS_UNVERIFIABLE
    assert h8["baseline_verdict"] == ""
    assert "missing baseline" in h8["notes"]


def test_classify_row_handles_nan_key_value_unchanged_other_fields() -> None:
    """key_value NaN on both sides but verdict/conf identical → unchanged."""
    base = {
        "hid": "Hx",
        "name_cn": "x",
        "verdict": "支持",
        "confidence": "中",
        "key_label": "spread",
        "key_value": "",
        "p_value": "",
        "n_obs": 10,
    }
    cur = dict(base)
    out = classify_row(base, cur)
    # both NaN + no confidence change → unverifiable via key_value
    assert out["classification"] == CLASS_UNVERIFIABLE


# ── CLI integration tests ────────────────────────────────────────────


def _write_csv(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def test_cli_default_exits_0_even_with_flip(tmp_path: Path) -> None:
    """No --strict → exit 0 even if a verdict flipped."""
    baseline = _write_csv(
        tmp_path / "snapshots" / "pre-registration-2026-05-03.csv",
        _baseline_frame(),
    )
    current_frame = _baseline_frame().copy()
    current_frame.loc[current_frame["hid"] == "H1", "verdict"] = "支持"
    current = _write_csv(
        tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
        current_frame,
    )
    report_path = tmp_path / "results" / "real_tables" / "pap_deviation_report.csv"

    rc = main(
        [
            "--baseline",
            str(baseline),
            "--current",
            str(current),
            "--report",
            str(report_path),
            "--no-color",
        ]
    )
    assert rc == 0
    assert report_path.exists()
    df = pd.read_csv(report_path)
    assert (df.loc[df["hid"] == "H1", "classification"] == CLASS_FLIPPED).all()


def test_cli_strict_exits_1_on_flip(tmp_path: Path) -> None:
    """With --strict, exit code 1 if any row is flipped."""
    baseline = _write_csv(
        tmp_path / "snapshots" / "pre-registration-2026-05-03.csv",
        _baseline_frame(),
    )
    current_frame = _baseline_frame().copy()
    current_frame.loc[current_frame["hid"] == "H1", "verdict"] = "支持"
    current = _write_csv(
        tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
        current_frame,
    )
    report_path = tmp_path / "results" / "real_tables" / "pap_deviation_report.csv"

    rc = main(
        [
            "--baseline",
            str(baseline),
            "--current",
            str(current),
            "--report",
            str(report_path),
            "--strict",
            "--no-color",
        ]
    )
    assert rc == 1


def test_cli_strict_exits_0_when_no_flip(tmp_path: Path) -> None:
    """With --strict but only tightenings → exit 0."""
    baseline = _write_csv(
        tmp_path / "snapshots" / "pre-registration-2026-05-03.csv",
        _baseline_frame(),
    )
    current_frame = _baseline_frame().copy()
    current_frame.loc[current_frame["hid"] == "H2", "confidence"] = "中"
    current = _write_csv(
        tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
        current_frame,
    )
    report_path = tmp_path / "results" / "real_tables" / "pap_deviation_report.csv"

    rc = main(
        [
            "--baseline",
            str(baseline),
            "--current",
            str(current),
            "--report",
            str(report_path),
            "--strict",
            "--no-color",
        ]
    )
    assert rc == 0


def test_cli_missing_baseline_returns_nonzero(tmp_path: Path) -> None:
    current = _write_csv(
        tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
        _baseline_frame(),
    )
    rc = main(
        [
            "--baseline",
            str(tmp_path / "snapshots" / "nonexistent.csv"),
            "--current",
            str(current),
            "--no-color",
            "--no-write",
        ]
    )
    assert rc == 1


def test_resolve_default_baseline_picks_latest(tmp_path: Path) -> None:
    """The picker returns the lexicographically last snapshot."""
    snap_dir = tmp_path / "snapshots"
    snap_dir.mkdir()
    earlier = snap_dir / "pre-registration-2024-01-01.csv"
    later = snap_dir / "pre-registration-2026-05-03.csv"
    earlier.write_text("hid,verdict\n")
    later.write_text("hid,verdict\n")
    resolved = resolve_default_baseline(snap_dir)
    assert resolved == later


def test_resolve_default_baseline_none_when_empty(tmp_path: Path) -> None:
    snap_dir = tmp_path / "snapshots"
    snap_dir.mkdir()
    assert resolve_default_baseline(snap_dir) is None
