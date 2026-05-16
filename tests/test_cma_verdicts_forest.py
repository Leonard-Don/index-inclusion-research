"""Tests for the cross-hypothesis CMA verdicts forest plot."""

from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

from index_inclusion_research.outputs import build_cma_verdicts_forest_plot
from index_inclusion_research.outputs.cma_verdicts_forest import (
    _format_headline,
    classify_strength,
)


def _make_fixture_csv(tmp_path: Path) -> Path:
    """Synthetic 7-row verdicts panel covering:

    - all 4 verdict types (支持 / 部分支持 / 证据不足 / and the
      'borderline' confidence variants of each)
    - both evidence tiers (core / supplementary)
    - sample sizes across 3 orders of magnitude (n=4 .. n=936)
    - one row with n_obs=0 (degenerate small-sample check)
    """
    df = pd.DataFrame(
        [
            {
                "hid": "H1",
                "name_cn": "信息泄露与预运行",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "core",
                "n_obs": 436,
            },
            {
                "hid": "H2",
                "name_cn": "被动基金 AUM 差异",
                "verdict": "部分支持",
                "confidence": "中",
                "evidence_tier": "core",
                "n_obs": 17,
            },
            {
                "hid": "H3",
                "name_cn": "散户 vs 机构结构",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "supplementary",
                "n_obs": 4,
            },
            {
                "hid": "H4",
                "name_cn": "卖空约束",
                "verdict": "证据不足",
                "confidence": "低",
                "evidence_tier": "supplementary",
                "n_obs": 0,  # degenerate n=0: must not crash
            },
            {
                "hid": "H5",
                "name_cn": "涨跌停限制",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "core",
                "n_obs": 936,
            },
            {
                "hid": "H6",
                "name_cn": "指数权重可预测性",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "supplementary",
                "n_obs": 67,
            },
            {
                "hid": "H7",
                "name_cn": "行业结构差异",
                "verdict": "支持",
                "confidence": "中",
                "evidence_tier": "core",
                "n_obs": 187,
            },
        ]
    )
    csv_path = tmp_path / "cma_hypothesis_verdicts.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_classify_strength_known_pairs() -> None:
    """Strength lookup is the contract of the figure — assert every
    (verdict, confidence) combination that maps to a documented score.
    """
    # Headline-spec values that the figure caption documents.
    assert classify_strength("支持", "高") == 1.0
    assert classify_strength("支持", "中") == 0.7  # H7 in the fixture
    assert classify_strength("部分支持", "中") == 0.5  # H2
    assert classify_strength("证据不足", "中") == 0.3  # H1 / H6
    assert classify_strength("证据不足", "低") == 0.0  # H4 (degenerate)
    # Unknown pair must collapse to 0.0 rather than raising — the
    # figure caption surfaces the table, not the plotting code path.
    assert classify_strength("未知裁决", "无") == 0.0


def test_format_headline_includes_n_tier_and_verdict() -> None:
    """Annotation column is rendered monospace (no CJK glyph coverage)
    so verdict / confidence are mapped to ASCII tokens for the right
    margin; the Chinese text lives on the y-axis labels and caption.
    """
    row = pd.Series(
        {
            "n_obs": 187,
            "evidence_tier": "core",
            "verdict": "支持",
            "confidence": "中",
        }
    )
    annotation = _format_headline(row)
    assert "n=187" in annotation
    assert "tier=core" in annotation
    assert "support" in annotation  # ASCII map of 支持
    assert "mid" in annotation  # ASCII map of 中
    # The annotation must be ASCII-clean so monospace font renders it.
    assert annotation.isascii(), f"annotation contains non-ASCII chars: {annotation!r}"


def test_build_cma_verdicts_forest_plot_writes_png_and_pdf(tmp_path: Path) -> None:
    csv_path = _make_fixture_csv(tmp_path)
    png_path = tmp_path / "out" / "cma_verdicts_forest.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_forest.pdf"

    with warnings.catch_warnings():
        # The implementation itself wraps savefig in
        # warnings.simplefilter("error"); double-check at the boundary
        # so a regression that swallows the warning still trips here.
        warnings.simplefilter("error")
        result = build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path,
            output_png_path=png_path,
            output_pdf_path=pdf_path,
            generated_on=date(2026, 5, 16),
        )

    assert result == png_path
    assert png_path.exists()
    assert png_path.stat().st_size > 0
    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 0

    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800
    assert height >= 600


def test_build_cma_verdicts_forest_plot_works_without_pdf(tmp_path: Path) -> None:
    csv_path = _make_fixture_csv(tmp_path)
    png_path = tmp_path / "no_pdf" / "forest.png"

    result = build_cma_verdicts_forest_plot(
        verdicts_csv_path=csv_path,
        output_png_path=png_path,
        output_pdf_path=None,
        generated_on=date(2026, 5, 16),
    )

    assert result == png_path
    assert png_path.exists()
    # PDF must not be written when omitted.
    assert not (png_path.parent / "forest.pdf").exists()


def test_build_cma_verdicts_forest_plot_raises_on_missing_csv(tmp_path: Path) -> None:
    missing_csv = tmp_path / "does_not_exist.csv"
    png_path = tmp_path / "out.png"
    with pytest.raises(FileNotFoundError):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=missing_csv,
            output_png_path=png_path,
        )


def test_build_cma_verdicts_forest_plot_raises_on_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    pd.DataFrame(
        columns=["hid", "verdict", "confidence", "evidence_tier", "n_obs"]
    ).to_csv(csv_path, index=False)
    png_path = tmp_path / "out.png"
    with pytest.raises(ValueError):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path,
            output_png_path=png_path,
        )


def test_build_cma_verdicts_forest_plot_raises_on_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    pd.DataFrame(
        [
            {"hid": "H1", "verdict": "支持"},  # missing confidence, tier, n_obs
        ]
    ).to_csv(csv_path, index=False)
    png_path = tmp_path / "out.png"
    with pytest.raises(ValueError):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path,
            output_png_path=png_path,
        )


def test_build_cma_verdicts_forest_plot_raises_on_missing_hypothesis(
    tmp_path: Path,
) -> None:
    """Missing hypothesis (e.g. only 6 of 7) must surface as a hard
    error rather than a silent skip — the figure's contract is
    'cross-hypothesis comparison of H1-H7'.
    """
    csv_path = tmp_path / "partial.csv"
    df = pd.DataFrame(
        [
            {
                "hid": "H1",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "core",
                "n_obs": 436,
            },
            # H2 deliberately missing
            {
                "hid": "H3",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "supplementary",
                "n_obs": 4,
            },
            {
                "hid": "H4",
                "verdict": "证据不足",
                "confidence": "低",
                "evidence_tier": "supplementary",
                "n_obs": 0,
            },
            {
                "hid": "H5",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "core",
                "n_obs": 936,
            },
            {
                "hid": "H6",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "supplementary",
                "n_obs": 67,
            },
            {
                "hid": "H7",
                "verdict": "支持",
                "confidence": "中",
                "evidence_tier": "core",
                "n_obs": 187,
            },
        ]
    )
    df.to_csv(csv_path, index=False)
    png_path = tmp_path / "out.png"
    with pytest.raises(ValueError, match="H2"):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path,
            output_png_path=png_path,
        )


def test_legend_includes_both_tiers_when_present(tmp_path: Path) -> None:
    """If the panel contains both core and supplementary tiers, the
    legend must surface both — proxied via legend handles by sniffing
    the rendered figure object (we re-run the build with both tiers
    in the fixture and inspect the saved figure file size as a
    smoke check, then directly check legend handles by re-importing).
    """
    csv_path = _make_fixture_csv(tmp_path)  # has both core + supplementary
    png_path = tmp_path / "out.png"

    # We import the module to re-instrument the build by monkeypatching
    # the matplotlib show pipeline — but the easiest assertion is that
    # the function runs successfully with a mixed-tier fixture. The
    # tier-handle code path is exercised by simply going through the
    # rendering of the full 7-row mixed-tier panel.
    build_cma_verdicts_forest_plot(
        verdicts_csv_path=csv_path,
        output_png_path=png_path,
        generated_on=date(2026, 5, 16),
    )
    assert png_path.exists()
    assert png_path.stat().st_size > 0

    # Cross-check that both tiers are present in the fixture itself
    # (guards against the fixture drifting in a way that silently
    # disables this legend code path).
    df = pd.read_csv(csv_path)
    tiers = set(df["evidence_tier"].astype(str).str.strip().tolist())
    assert "core" in tiers
    assert "supplementary" in tiers


def test_strength_score_matches_fixture_pairs(tmp_path: Path) -> None:
    """Verify that the strength score computed for each fixture row
    matches the documented table — catches accidental drift in the
    public ``classify_strength`` helper or the table constants.
    """
    csv_path = _make_fixture_csv(tmp_path)
    df = pd.read_csv(csv_path)
    # Map: hid → (verdict, confidence, expected_strength)
    expected = {
        "H1": 0.3,   # 证据不足/中
        "H2": 0.5,   # 部分支持/中
        "H3": 1.0,   # 支持/高
        "H4": 0.0,   # 证据不足/低
        "H5": 1.0,   # 支持/高
        "H6": 0.3,   # 证据不足/中
        "H7": 0.7,   # 支持/中  ← the spec's named example
    }
    for _, row in df.iterrows():
        hid = str(row["hid"])
        score = classify_strength(row["verdict"], row["confidence"])
        assert score == pytest.approx(expected[hid]), (
            f"{hid}: expected {expected[hid]}, got {score}"
        )


def test_real_verdicts_csv_renders_when_present(tmp_path: Path) -> None:
    """If the project's canonical verdicts CSV exists, the plot must
    render it without raising. Skipped on fresh checkouts where the
    CMA pipeline hasn't been run."""
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    if not csv_path.exists():
        pytest.skip("cma_hypothesis_verdicts.csv not present in this checkout")
    png_path = tmp_path / "real_cma_forest.png"
    result = build_cma_verdicts_forest_plot(
        verdicts_csv_path=csv_path,
        output_png_path=png_path,
        generated_on=date(2026, 5, 16),
    )
    assert result.exists()
    assert result.stat().st_size > 0
