"""Tests for ``index_inclusion_research.verdict_summary``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.verdict_summary import main, render_summary


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
    assert "假说裁决摘要" in out
    assert "H1" in out and "H3" in out
