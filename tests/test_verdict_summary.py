"""Tests for ``index_inclusion_research.verdict_summary``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research.verdict_summary import (
    build_sensitivity_table,
    compute_verdict_diff,
    main,
    render_sensitivity_table,
    render_summary,
    render_summary_json,
    render_verdict_diff,
    save_verdict_snapshot,
)


def _verdicts_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "hid": "H1", "name_cn": "信息泄露与预运行",
                "verdict": "证据不足", "confidence": "中",
                "key_label": "bootstrap p", "key_value": 0.640, "n_obs": 436,
            },
            {
                "hid": "H2", "name_cn": "被动基金 AUM 差异",
                "verdict": "待补数据", "confidence": "低",
                "key_label": "", "key_value": float("nan"), "n_obs": 0,
            },
            {
                "hid": "H3", "name_cn": "散户 vs 机构结构",
                "verdict": "部分支持", "confidence": "中",
                "key_label": "双通道命中率", "key_value": 0.500, "n_obs": 4,
            },
        ]
    )


def _track_summary_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "track": "identification", "track_label": "制度识别与中国市场",
                "hypotheses": "H1",
                "支持": 0, "部分支持": 0, "证据不足": 1, "待补数据": 0, "total": 1,
            },
            {
                "track": "demand_curve", "track_label": "需求曲线与长期保留",
                "hypotheses": "H2",
                "支持": 0, "部分支持": 0, "证据不足": 0, "待补数据": 1, "total": 1,
            },
        ]
    )


def test_render_summary_includes_aggregate_breakdown() -> None:
    text = render_summary(_verdicts_fixture(), color=False)
    assert "总览:" in text
    assert "1 证据不足" in text
    assert "1 待补数据" in text
    assert "1 部分支持" in text


def test_render_summary_lists_each_hid() -> None:
    text = render_summary(_verdicts_fixture(), color=False)
    for hid in ("H1", "H2", "H3"):
        assert hid in text
    # column header presents
    assert "头条指标" in text


def test_render_summary_handles_missing_track_summary() -> None:
    text = render_summary(_verdicts_fixture(), track_summary=None, color=False)
    assert "研究主线分布" not in text


def test_render_summary_with_track_summary() -> None:
    text = render_summary(
        _verdicts_fixture(), track_summary=_track_summary_fixture(), color=False
    )
    assert "研究主线分布:" in text
    assert "制度识别与中国市场" in text
    assert "需求曲线与长期保留" in text


def test_render_summary_handles_empty_verdicts() -> None:
    text = render_summary(pd.DataFrame(), color=False)
    assert "verdicts CSV is empty" in text


def test_render_summary_renders_dash_for_nan_key_label(tmp_path: Path) -> None:
    """CSV round-trip turns empty ``key_label`` into NaN — must render as —."""
    verdicts_path = tmp_path / "v.csv"
    _verdicts_fixture().to_csv(verdicts_path, index=False)
    reloaded = pd.read_csv(verdicts_path)
    text = render_summary(reloaded, color=False)
    # H2 has empty key_label which becomes NaN after CSV round-trip
    h2_lines = [line for line in text.splitlines() if "H2" in line]
    assert h2_lines
    assert "nan" not in h2_lines[0].lower()


def test_render_summary_no_color_omits_ansi() -> None:
    text = render_summary(_verdicts_fixture(), color=False)
    assert "\033[" not in text


def test_render_summary_color_includes_ansi() -> None:
    text = render_summary(_verdicts_fixture(), color=True)
    assert "\033[" in text


def test_main_returns_1_when_verdicts_csv_missing(tmp_path: Path) -> None:
    rc = main([
        "--verdicts", str(tmp_path / "missing.csv"),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--no-color",
    ])
    assert rc == 1


def test_save_verdict_snapshot_writes_csv(tmp_path: Path) -> None:
    out = tmp_path / "snap.csv"
    written = save_verdict_snapshot(_verdicts_fixture(), output_path=out)
    assert written == out
    assert out.exists()
    df = pd.read_csv(out)
    assert len(df) == 3
    assert "hid" in df.columns


def test_compute_verdict_diff_detects_tier_flip() -> None:
    previous = _verdicts_fixture()
    current = previous.copy()
    current.loc[current["hid"] == "H1", "verdict"] = "支持"
    current.loc[current["hid"] == "H1", "key_value"] = 0.012
    diff = compute_verdict_diff(current, previous)
    h1 = next(r for r in diff if r["hid"] == "H1")
    assert h1["kind"] == "changed"
    changes = h1["changes"]
    assert changes["verdict"] == {"before": "证据不足", "after": "支持"}
    assert changes["key_value"]["before"] == 0.640
    assert changes["key_value"]["after"] == 0.012


def test_compute_verdict_diff_detects_added_and_removed() -> None:
    previous = _verdicts_fixture()
    current = previous.copy()
    # add H8
    current = pd.concat(
        [current, pd.DataFrame([{"hid": "H8", "name_cn": "新假说",
                                  "verdict": "支持", "confidence": "中",
                                  "key_label": "lol", "key_value": 0.1, "n_obs": 50}])],
        ignore_index=True,
    )
    # remove H3
    current = current.loc[current["hid"] != "H3"].reset_index(drop=True)
    diff = compute_verdict_diff(current, previous)
    kinds = {r["hid"]: r["kind"] for r in diff if r["kind"] != "unchanged"}
    assert kinds["H8"] == "added"
    assert kinds["H3"] == "removed"


def test_compute_verdict_diff_unchanged_for_identical_input() -> None:
    diff = compute_verdict_diff(_verdicts_fixture(), _verdicts_fixture())
    assert all(r["kind"] == "unchanged" for r in diff)


def test_compute_verdict_diff_handles_nan_key_value() -> None:
    previous = _verdicts_fixture()
    current = previous.copy()
    # H2 already has NaN key_value; flip its verdict only
    current.loc[current["hid"] == "H2", "verdict"] = "部分支持"
    diff = compute_verdict_diff(current, previous)
    h2 = next(r for r in diff if r["hid"] == "H2")
    assert h2["kind"] == "changed"
    # NaN-vs-NaN should NOT register as a key_value change
    assert "key_value" not in h2["changes"]


def test_render_verdict_diff_no_changes() -> None:
    diff = compute_verdict_diff(_verdicts_fixture(), _verdicts_fixture())
    text = render_verdict_diff(diff, color=False)
    assert "没有变化" in text


def test_render_verdict_diff_shows_arrows_and_delta() -> None:
    previous = _verdicts_fixture()
    current = previous.copy()
    current.loc[current["hid"] == "H1", "verdict"] = "支持"
    current.loc[current["hid"] == "H1", "key_value"] = 0.012
    text = render_verdict_diff(compute_verdict_diff(current, previous), color=False)
    assert "证据不足" in text
    assert "支持" in text
    assert "→" in text
    assert "Δ" in text  # delta marker for numeric change


def test_main_snapshot_writes_file(tmp_path: Path, capsys) -> None:
    verdicts_path = tmp_path / "verdicts.csv"
    _verdicts_fixture().to_csv(verdicts_path, index=False)
    snap_path = tmp_path / "snap.csv"
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing.csv"),
        "--snapshot", str(snap_path),
        "--no-color",
    ])
    assert rc == 0
    assert snap_path.exists()
    captured = capsys.readouterr().out
    assert "saved snapshot" in captured


def test_main_compare_with_renders_diff(tmp_path: Path, capsys) -> None:
    verdicts_path = tmp_path / "v.csv"
    snap_path = tmp_path / "s.csv"
    fixture = _verdicts_fixture()
    fixture.to_csv(snap_path, index=False)
    altered = fixture.copy()
    altered.loc[altered["hid"] == "H1", "verdict"] = "支持"
    altered.to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing.csv"),
        "--compare-with", str(snap_path),
        "--no-color",
    ])
    assert rc == 0
    captured = capsys.readouterr().out
    assert "VERDICT DIFF" in captured
    assert "证据不足" in captured and "支持" in captured


def test_render_summary_json_carries_aggregate_and_verdicts() -> None:
    import json

    text = render_summary_json(_verdicts_fixture())
    payload = json.loads(text)
    assert "verdicts" in payload and len(payload["verdicts"]) == 3
    assert payload["aggregate"]["证据不足"] == 1
    assert payload["aggregate"]["待补数据"] == 1
    assert payload["aggregate"]["部分支持"] == 1
    assert payload["track_summary"] == []


def test_render_summary_json_normalises_nan_to_null() -> None:
    import json

    payload = json.loads(render_summary_json(_verdicts_fixture()))
    h2 = next(v for v in payload["verdicts"] if v["hid"] == "H2")
    # H2 has NaN key_value in fixture → should be null in JSON
    assert h2["key_value"] is None


def test_render_summary_json_with_diff_rows() -> None:
    import json

    diff = compute_verdict_diff(_verdicts_fixture(), _verdicts_fixture())
    payload = json.loads(render_summary_json(_verdicts_fixture(), diff_rows=diff))
    assert "diff" in payload
    assert len(payload["diff"]) == 3


def test_main_format_json_prints_valid_json(tmp_path: Path, capsys) -> None:
    import json

    verdicts_path = tmp_path / "v.csv"
    _verdicts_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing.csv"),
        "--format", "json",
        "--no-color",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert "verdicts" in payload
    assert "aggregate" in payload


def test_main_compare_with_missing_snapshot_returns_1(tmp_path: Path) -> None:
    verdicts_path = tmp_path / "v.csv"
    _verdicts_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--compare-with", str(tmp_path / "no-such-snap.csv"),
        "--no-color",
    ])
    assert rc == 1


def test_main_prints_summary_when_csv_present(tmp_path: Path, capsys) -> None:
    verdicts_path = tmp_path / "verdicts.csv"
    _verdicts_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--no-color",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "CN/US 不对称机制裁决" in out
    assert "H1" in out and "H3" in out


# ── Sensitivity sweep ────────────────────────────────────────────────


def _verdicts_with_p_value_fixture() -> pd.DataFrame:
    """A 4-row fixture covering the four p_value cases:

    - H1: p < 0.05 (would be flagged sig at every reasonable threshold)
    - H4: p in 0.10-0.15 band (used to test threshold flips)
    - H5: p > 0.20 (never sig at default thresholds)
    - H6: NaN p_value (spread-style hypothesis, out of scope)
    """
    return pd.DataFrame(
        [
            {
                "hid": "H1", "name_cn": "H1 name",
                "verdict": "支持", "confidence": "高",
                "key_label": "bootstrap p", "key_value": 0.012, "n_obs": 440,
                "p_value": 0.012,
            },
            {
                "hid": "H4", "name_cn": "H4 name",
                "verdict": "部分支持", "confidence": "中",
                "key_label": "regression p", "key_value": 0.12, "n_obs": 430,
                "p_value": 0.12,
            },
            {
                "hid": "H5", "name_cn": "H5 name",
                "verdict": "证据不足", "confidence": "中",
                "key_label": "limit_coef p", "key_value": 0.30, "n_obs": 936,
                "p_value": 0.30,
            },
            {
                "hid": "H6", "name_cn": "H6 name",
                "verdict": "部分支持", "confidence": "低",
                "key_label": "Q1Q2−Q4Q5 spread", "key_value": 1.17, "n_obs": 118,
                "p_value": float("nan"),
            },
        ]
    )


def test_build_sensitivity_table_marks_sig_per_threshold() -> None:
    table = build_sensitivity_table(
        _verdicts_with_p_value_fixture(),
        thresholds=(0.05, 0.10, 0.15),
    )
    by_hid = table.set_index("hid")
    # H1 (p=0.012): sig at every threshold
    assert by_hid.loc["H1", "sig_at_0.05"] is True
    assert by_hid.loc["H1", "sig_at_0.1"] is True
    assert by_hid.loc["H1", "sig_at_0.15"] is True
    # H4 (p=0.12): sig only at 0.15 (the loosest)
    assert by_hid.loc["H4", "sig_at_0.05"] is False
    assert by_hid.loc["H4", "sig_at_0.1"] is False
    assert by_hid.loc["H4", "sig_at_0.15"] is True
    # H5 (p=0.30): never sig at these thresholds
    assert by_hid.loc["H5", "sig_at_0.05"] is False
    assert by_hid.loc["H5", "sig_at_0.1"] is False
    assert by_hid.loc["H5", "sig_at_0.15"] is False
    # H6 (NaN): None at every threshold (out of scope)
    assert by_hid.loc["H6", "sig_at_0.05"] is None
    assert by_hid.loc["H6", "sig_at_0.1"] is None
    assert by_hid.loc["H6", "sig_at_0.15"] is None


def test_build_sensitivity_table_dedupes_and_sorts_thresholds() -> None:
    table = build_sensitivity_table(
        _verdicts_with_p_value_fixture(),
        thresholds=(0.10, 0.05, 0.10, 0.15),  # duplicates + unsorted
    )
    expected = [
        "hid",
        "name_cn",
        "p_value",
        "sig_at_0.05",
        "sig_at_0.1",
        "sig_at_0.15",
        "bonferroni_p",
        "bh_q",
    ]
    assert list(table.columns) == expected


def test_build_sensitivity_table_adds_bonferroni_and_bh_columns() -> None:
    table = build_sensitivity_table(
        _verdicts_with_p_value_fixture(),
        thresholds=(0.05, 0.10, 0.15),
    )
    by_hid = table.set_index("hid")
    # Fixture has 3 hypotheses with p-values (H1=0.012, H4=0.12, H5=0.30) plus
    # H6 with NaN, so m = 3 for the multiple-testing correction.
    assert by_hid.loc["H1", "bonferroni_p"] == pytest.approx(0.036, abs=1e-6)
    assert by_hid.loc["H4", "bonferroni_p"] == pytest.approx(0.36, abs=1e-6)
    assert by_hid.loc["H5", "bonferroni_p"] == pytest.approx(0.90, abs=1e-6)
    # H6 has no p-value, so both columns are NaN.
    import math

    assert math.isnan(float(by_hid.loc["H6", "bonferroni_p"]))
    assert math.isnan(float(by_hid.loc["H6", "bh_q"]))
    # BH q-values: sorted [0.012, 0.12, 0.30], raw [0.036, 0.18, 0.30], monotone-min
    # from the right gives [0.036, 0.18, 0.30].
    assert by_hid.loc["H1", "bh_q"] == pytest.approx(0.036, abs=1e-6)
    assert by_hid.loc["H4", "bh_q"] == pytest.approx(0.18, abs=1e-6)
    assert by_hid.loc["H5", "bh_q"] == pytest.approx(0.30, abs=1e-6)


def test_build_sensitivity_table_handles_empty_verdicts() -> None:
    table = build_sensitivity_table(pd.DataFrame(), thresholds=(0.05, 0.10))
    assert table.empty
    assert "p_value" in table.columns


def test_render_sensitivity_table_shows_p_gated_rows_and_out_of_scope_note() -> None:
    text = render_sensitivity_table(
        _verdicts_with_p_value_fixture(),
        thresholds=(0.05, 0.10, 0.15),
    )
    # Header / banner
    assert "p 值灵敏度" in text
    # P-gated hypotheses appear with their p_value
    assert "H1" in text
    assert "0.0120" in text
    assert "H4" in text
    assert "0.1200" in text
    # Out-of-scope hypothesis is called out separately, not in the main grid
    assert "H6" in text
    assert "不在 sweep 范围内" in text


def test_render_sensitivity_table_handles_only_non_p_hypotheses() -> None:
    only_non_p = pd.DataFrame(
        [
            {"hid": "H6", "name_cn": "H6", "verdict": "部分支持", "p_value": float("nan")},
            {"hid": "H7", "name_cn": "H7", "verdict": "部分支持", "p_value": float("nan")},
        ]
    )
    text = render_sensitivity_table(only_non_p, thresholds=(0.05,))
    assert "没有任何假说由单一 p 决定" in text


def test_main_sensitivity_default_uses_three_thresholds(
    tmp_path: Path, capsys
) -> None:
    verdicts_path = tmp_path / "verdicts.csv"
    _verdicts_with_p_value_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--no-color",
        "--sensitivity",  # no values → defaults
    ])
    assert rc == 0
    out = capsys.readouterr().out
    # Default thresholds 0.05 / 0.10 / 0.15 all appear
    assert "p<0.05" in out
    assert "p<0.1" in out
    assert "p<0.15" in out


def test_main_sensitivity_custom_thresholds(tmp_path: Path, capsys) -> None:
    verdicts_path = tmp_path / "verdicts.csv"
    _verdicts_with_p_value_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--no-color",
        "--sensitivity", "0.01", "0.50",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "p<0.01" in out
    assert "p<0.5" in out


def test_main_format_json_with_sensitivity_emits_sensitivity_block(
    tmp_path: Path, capsys
) -> None:
    import json

    verdicts_path = tmp_path / "verdicts.csv"
    _verdicts_with_p_value_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--format", "json",
        "--sensitivity", "0.05", "0.10",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert "sensitivity" in payload
    assert payload["sensitivity"]["thresholds"] == [0.05, 0.10]
    rows_by_hid = {row["hid"]: row for row in payload["sensitivity"]["rows"]}
    # H1 (p=0.012) is sig at both thresholds
    assert rows_by_hid["H1"]["sig_at_0.05"] is True
    assert rows_by_hid["H1"]["sig_at_0.1"] is True
    # H6 (NaN p) is null at both
    assert rows_by_hid["H6"]["sig_at_0.05"] is None
    assert rows_by_hid["H6"]["sig_at_0.1"] is None


def test_main_format_json_without_sensitivity_omits_sensitivity_block(
    tmp_path: Path, capsys
) -> None:
    import json

    verdicts_path = tmp_path / "verdicts.csv"
    _verdicts_with_p_value_fixture().to_csv(verdicts_path, index=False)
    rc = main([
        "--verdicts", str(verdicts_path),
        "--track-summary", str(tmp_path / "missing-track.csv"),
        "--format", "json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert "sensitivity" not in payload
