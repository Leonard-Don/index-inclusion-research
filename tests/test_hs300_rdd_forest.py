"""Tests for the HS300 RDD robustness forest plot."""

from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

from index_inclusion_research.outputs import build_hs300_rdd_forest_plot
from index_inclusion_research.outputs.hs300_rdd_forest import _classify_significance


def _make_fixture_csv(tmp_path: Path) -> Path:
    """Synthetic 4-row robustness panel covering all spec_kinds and all
    three significance bands so the legend renders all branches.
    """
    df = pd.DataFrame(
        [
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 120,
                "n_left": 42,
                "n_right": 78,
                "tau": 0.0392,
                "std_error": 0.0199,
                "t_stat": 1.974,
                "p_value": 0.048,
                "r_squared": 0.0378,
                "intercept": -0.0158,
                "running_slope": -0.728,
                "interaction_slope": 0.510,
                "spec": "main · 局部线性",
                "spec_kind": "main",
                "donut_radius": None,
                "cutoff_shift": None,
                "polynomial_order": None,
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 102,
                "n_left": 34,
                "n_right": 68,
                "tau": 0.0493,
                "std_error": 0.0301,
                "t_stat": 1.636,
                "p_value": 0.092,  # 边缘显著 — exercise the p<0.10 band
                "r_squared": 0.0332,
                "intercept": -0.021,
                "running_slope": -0.848,
                "interaction_slope": 0.529,
                "spec": "donut(±0.01)",
                "spec_kind": "donut",
                "donut_radius": 0.01,
                "cutoff_shift": None,
                "polynomial_order": None,
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 72,
                "n_left": 30,
                "n_right": 42,
                "tau": -0.0244,
                "std_error": 0.0217,
                "t_stat": -1.128,
                "p_value": 0.259,  # NS — exercise the gray band
                "r_squared": 0.0723,
                "intercept": 0.0141,
                "running_slope": -0.361,
                "interaction_slope": 0.757,
                "spec": "placebo cutoff -0.05",
                "spec_kind": "placebo",
                "donut_radius": None,
                "cutoff_shift": -0.05,
                "polynomial_order": None,
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 120,
                "n_left": 42,
                "n_right": 78,
                "tau": 0.0037,
                "std_error": 0.0367,
                "t_stat": 0.1,
                "p_value": 0.921,
                "r_squared": 0.0504,
                "intercept": None,
                "running_slope": None,
                "interaction_slope": None,
                "spec": "polynomial order=2",
                "spec_kind": "polynomial",
                "donut_radius": None,
                "cutoff_shift": None,
                "polynomial_order": 2.0,
            },
        ]
    )
    csv_path = tmp_path / "rdd_robustness.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_build_hs300_rdd_forest_plot_writes_png(tmp_path: Path) -> None:
    csv_path = _make_fixture_csv(tmp_path)
    png_path = tmp_path / "out" / "hs300_rdd_robustness_forest.png"
    pdf_path = tmp_path / "out" / "hs300_rdd_robustness_forest.pdf"

    # Pytest converts matplotlib UserWarnings to warnings emitted during
    # the call. The implementation itself wraps savefig in
    # `warnings.simplefilter("error")`, but we double-check at the test
    # boundary so a regression that swallows the warning still trips
    # this assertion.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = build_hs300_rdd_forest_plot(
            robustness_csv_path=csv_path,
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


def test_build_hs300_rdd_forest_plot_works_without_pdf(tmp_path: Path) -> None:
    csv_path = _make_fixture_csv(tmp_path)
    png_path = tmp_path / "no_pdf" / "forest.png"

    result = build_hs300_rdd_forest_plot(
        robustness_csv_path=csv_path,
        output_png_path=png_path,
        output_pdf_path=None,
        generated_on=date(2026, 5, 16),
    )

    assert result == png_path
    assert png_path.exists()
    # PDF must not be written when omitted, even as a side effect of
    # output_dir creation.
    assert not (png_path.parent / "forest.pdf").exists()


def test_build_hs300_rdd_forest_plot_raises_on_missing_csv(tmp_path: Path) -> None:
    missing_csv = tmp_path / "does_not_exist.csv"
    png_path = tmp_path / "out.png"
    with pytest.raises(FileNotFoundError):
        build_hs300_rdd_forest_plot(
            robustness_csv_path=missing_csv,
            output_png_path=png_path,
        )


def test_build_hs300_rdd_forest_plot_raises_on_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    pd.DataFrame(
        columns=["spec", "spec_kind", "tau", "std_error", "p_value", "n_obs"]
    ).to_csv(csv_path, index=False)
    png_path = tmp_path / "out.png"
    with pytest.raises(ValueError):
        build_hs300_rdd_forest_plot(
            robustness_csv_path=csv_path,
            output_png_path=png_path,
        )


def test_build_hs300_rdd_forest_plot_raises_on_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    pd.DataFrame(
        [
            {"spec": "main", "spec_kind": "main", "tau": 0.04, "std_error": 0.02},
            # missing p_value, n_obs
        ]
    ).to_csv(csv_path, index=False)
    png_path = tmp_path / "out.png"
    with pytest.raises(ValueError):
        build_hs300_rdd_forest_plot(
            robustness_csv_path=csv_path,
            output_png_path=png_path,
        )


def test_classify_significance_bands() -> None:
    """Significance bands must be exact: 5%, 10%, NS, plus NaN fallback."""
    color_sig, label_sig = _classify_significance(0.01)
    color_marg, label_marg = _classify_significance(0.07)
    color_ns, label_ns = _classify_significance(0.5)
    color_nan, _ = _classify_significance(float("nan"))

    assert "p<0.05" in label_sig
    assert "p<0.10" in label_marg
    assert "p≥0.10" in label_ns
    assert color_sig != color_marg
    assert color_marg != color_ns
    assert color_nan == color_ns  # NaN falls into the NS band


def test_real_robustness_csv_renders_when_present(tmp_path: Path) -> None:
    """If the project's canonical robustness CSV exists, the plot must
    render it without raising. Skipped on fresh checkouts where the
    pipeline hasn't been run."""
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv"
    if not csv_path.exists():
        pytest.skip("rdd_robustness.csv not present in this checkout")
    png_path = tmp_path / "real_forest.png"
    result = build_hs300_rdd_forest_plot(
        robustness_csv_path=csv_path,
        output_png_path=png_path,
        generated_on=date(2026, 5, 16),
    )
    assert result.exists()
    assert result.stat().st_size > 0
