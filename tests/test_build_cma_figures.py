"""Tests for the unified CMA figures CLI + the builders it drives.

This file consolidates the four former per-figure CLI test modules
(``test_cma_verdicts_forest`` / ``test_cma_verdicts_sensitivity`` /
``test_cma_verdicts_ar_engine`` / ``test_cma_verdicts_2d_robustness``).
The duplicate argparse-wrapper scaffolding is gone; what remains is

1. dispatch coverage for ``index-inclusion-build-cma-figures`` (the new
   ``--which {forest,sensitivity,ar,heatmap,all}`` front-end), and
2. substantive coverage of the four ``outputs`` builders the CLI calls:
   strength classification, sweep shape, flip detection, render output,
   and the round-trip ``build_*`` convenience functions.

The sweeps are driven by deterministic in-process fixture runners so the
suite never invokes the real CMA orchestrator.
"""

from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

from index_inclusion_research import build_cma_figures
from index_inclusion_research.outputs import (
    DEFAULT_2D_AR_MODELS,
    DEFAULT_2D_THRESHOLDS,
    DEFAULT_AR_MODELS,
    DEFAULT_SENSITIVITY_THRESHOLDS,
    build_cma_2d_robustness_heatmap,
    build_cma_2d_sweep,
    build_cma_ar_engine_forest_plot,
    build_cma_ar_engine_sweep,
    build_cma_sensitivity_forest_plot,
    build_cma_sensitivity_sweep,
    build_cma_verdicts_forest_plot,
    render_2d_robustness_heatmap,
    render_ar_engine_forest_plot,
    render_sensitivity_forest_plot,
)
from index_inclusion_research.outputs.cma_verdicts_2d_robustness import (
    SWEEP_OUTPUT_COLUMNS,
    _flip_count_per_hypothesis,
)
from index_inclusion_research.outputs.cma_verdicts_ar_engine import (
    AR_MODEL_ADJUSTED,
    AR_MODEL_MARKET,
    _flipped_hypotheses,
)
from index_inclusion_research.outputs.cma_verdicts_forest import (
    _format_headline,
    classify_strength,
)
from index_inclusion_research.outputs.cma_verdicts_sensitivity import (
    _count_flips_per_hypothesis,
)

# ════════════════════════════════════════════════════════════════════
# Shared fixtures
# ════════════════════════════════════════════════════════════════════


def _make_forest_fixture_csv(tmp_path: Path) -> Path:
    """Synthetic 7-row verdicts panel for the single-snapshot forest plot.

    Covers all verdict types, both evidence tiers, sample sizes across
    three orders of magnitude, and one degenerate ``n_obs=0`` row.
    """
    df = pd.DataFrame(
        [
            {"hid": "H1", "name_cn": "信息泄露与预运行", "verdict": "证据不足",
             "confidence": "中", "evidence_tier": "core", "n_obs": 436},
            {"hid": "H2", "name_cn": "被动基金 AUM 差异", "verdict": "部分支持",
             "confidence": "中", "evidence_tier": "core", "n_obs": 17},
            {"hid": "H3", "name_cn": "散户 vs 机构结构", "verdict": "支持",
             "confidence": "高", "evidence_tier": "supplementary", "n_obs": 4},
            {"hid": "H4", "name_cn": "卖空约束", "verdict": "证据不足",
             "confidence": "低", "evidence_tier": "supplementary", "n_obs": 0},
            {"hid": "H5", "name_cn": "涨跌停限制", "verdict": "支持",
             "confidence": "高", "evidence_tier": "core", "n_obs": 936},
            {"hid": "H6", "name_cn": "指数权重可预测性", "verdict": "证据不足",
             "confidence": "中", "evidence_tier": "supplementary", "n_obs": 67},
            {"hid": "H7", "name_cn": "行业结构差异", "verdict": "支持",
             "confidence": "中", "evidence_tier": "core", "n_obs": 187},
        ]
    )
    csv_path = tmp_path / "cma_hypothesis_verdicts.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def _verdicts_at_threshold(threshold: float) -> pd.DataFrame:
    """7-row panel whose p-gated verdicts (H1/H4/H5) shift with threshold."""
    rows: list[dict[str, object]] = []

    def add(hid, name, verdict, confidence, tier, n, p=None):  # type: ignore[no-untyped-def]
        rows.append({"hid": hid, "name_cn": name, "verdict": verdict,
                     "confidence": confidence, "evidence_tier": tier,
                     "n_obs": n, "p_value": p})

    if threshold <= 0.05:
        add("H1", "信息泄露与预运行", "证据不足", "中", "core", 436, 0.08)
    elif threshold < 0.20:
        add("H1", "信息泄露与预运行", "部分支持", "中", "core", 436, 0.08)
    else:
        add("H1", "信息泄露与预运行", "支持", "高", "core", 436, 0.08)
    add("H2", "被动基金 AUM 差异", "部分支持", "中", "core", 17)
    add("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
    if threshold < 0.15:
        add("H4", "卖空约束", "证据不足", "中", "supplementary", 40, 0.12)
    else:
        add("H4", "卖空约束", "部分支持", "中", "supplementary", 40, 0.12)
    if threshold <= 0.05:
        add("H5", "涨跌停限制", "部分支持", "中", "core", 936, 0.03)
    else:
        add("H5", "涨跌停限制", "支持", "高", "core", 936, 0.03)
    add("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
    add("H7", "行业结构差异", "支持", "中", "core", 187)
    return pd.DataFrame(rows)


def _threshold_runner(threshold: float) -> pd.DataFrame:
    return _verdicts_at_threshold(threshold)


def _verdicts_at_engine(ar_model: str) -> pd.DataFrame:
    """7-row panel; H1/H4 flip between the adjusted and market engines."""
    rows: list[dict[str, object]] = []

    def add(hid, name, verdict, confidence, tier, n, p=None):  # type: ignore[no-untyped-def]
        rows.append({"hid": hid, "name_cn": name, "verdict": verdict,
                     "confidence": confidence, "evidence_tier": tier,
                     "n_obs": n, "p_value": p})

    if ar_model == AR_MODEL_ADJUSTED:
        add("H1", "信息泄露与预运行", "证据不足", "中", "core", 436, 0.875)
        add("H2", "被动基金 AUM 差异", "部分支持", "中", "core", 17)
        add("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
        add("H4", "卖空约束", "证据不足", "中", "supplementary", 40, 0.537)
        add("H5", "涨跌停限制", "支持", "高", "core", 936, 0.008)
        add("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
        add("H7", "行业结构差异", "支持", "中", "core", 187)
    elif ar_model == AR_MODEL_MARKET:
        add("H1", "信息泄露与预运行", "部分支持", "中", "core", 430, 0.072)
        add("H2", "被动基金 AUM 差异", "部分支持", "中", "core", 17)
        add("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
        add("H4", "卖空约束", "部分支持", "中", "supplementary", 38, 0.082)
        add("H5", "涨跌停限制", "支持", "高", "core", 920, 0.010)
        add("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
        add("H7", "行业结构差异", "支持", "中", "core", 184)
    else:  # pragma: no cover - defensive
        raise AssertionError(f"unexpected ar_model {ar_model!r}")
    return pd.DataFrame(rows)


def _engine_runner(ar_model: str) -> pd.DataFrame:
    return _verdicts_at_engine(ar_model)


def _verdicts_at_cell(threshold: float, ar_model: str) -> pd.DataFrame:
    """7-row panel varying along both the threshold and engine axes."""
    is_market = ar_model == AR_MODEL_MARKET

    def row(hid, name, verdict, confidence, tier, n):  # type: ignore[no-untyped-def]
        return {"hid": hid, "name_cn": name, "verdict": verdict,
                "confidence": confidence, "evidence_tier": tier, "n_obs": n}

    return pd.DataFrame([
        row("H1", "信息泄露与预运行", "支持" if is_market else "证据不足",
            "高" if is_market else "中", "core", 436),
        row("H2", "被动基金 AUM 差异", "支持" if is_market else "部分支持",
            "中", "core", 17),
        row("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4),
        row("H4", "卖空约束",
            "部分支持" if (is_market and abs(threshold - 0.20) < 1e-9) else "证据不足",
            "中", "supplementary", 40),
        row("H5", "涨跌停限制",
            "证据不足" if abs(threshold - 0.05) < 1e-9 else "支持",
            "中" if abs(threshold - 0.05) < 1e-9 else "高", "core", 936),
        row("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67),
        row("H7", "行业结构差异", "支持", "中", "core", 187),
    ])


def _cell_runner(threshold: float, ar_model: str) -> pd.DataFrame:
    return _verdicts_at_cell(threshold, ar_model)


# ════════════════════════════════════════════════════════════════════
# Unified CLI dispatch (index-inclusion-build-cma-figures)
# ════════════════════════════════════════════════════════════════════


def test_parser_default_which_is_all() -> None:
    args = build_cma_figures.build_parser().parse_args([])
    assert args.which == "all"


def test_parser_rejects_unknown_which() -> None:
    with pytest.raises(SystemExit):
        build_cma_figures.build_parser().parse_args(["--which", "bogus"])


@pytest.mark.parametrize("which", list(build_cma_figures.FIGURE_KEYS))
def test_main_runs_only_the_selected_builder(
    which: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``--which <key>`` must invoke exactly that one builder thunk."""
    calls: list[str] = []
    for key in build_cma_figures.BUILDERS:
        label, _ = build_cma_figures.BUILDERS[key]

        def _fake(k: str = key) -> Path:
            calls.append(k)
            return tmp_path / f"{k}.png"

        monkeypatch.setitem(
            build_cma_figures.BUILDERS, key, (label, _fake)
        )

    rc = build_cma_figures.main(["--which", which])
    assert rc == 0
    assert calls == [which]


def test_main_all_runs_every_builder_in_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[str] = []
    for key in build_cma_figures.BUILDERS:
        label, _ = build_cma_figures.BUILDERS[key]

        def _fake(k: str = key) -> Path:
            calls.append(k)
            return tmp_path / f"{k}.png"

        monkeypatch.setitem(
            build_cma_figures.BUILDERS, key, (label, _fake)
        )

    rc = build_cma_figures.main([])  # default == all
    assert rc == 0
    assert calls == list(build_cma_figures.FIGURE_KEYS)


def test_default_which_all_covers_every_figure_key() -> None:
    """Guard against a builder being added without a --which choice."""
    assert set(build_cma_figures.BUILDERS) == set(build_cma_figures.FIGURE_KEYS)
    parser_choices = build_cma_figures.build_parser()
    # The choices are (*FIGURE_KEYS, "all").
    action = next(
        a for a in parser_choices._actions if a.dest == "which"
    )
    assert set(action.choices) == set(build_cma_figures.FIGURE_KEYS) | {"all"}


# ════════════════════════════════════════════════════════════════════
# Single-snapshot forest builder (--which forest)
# ════════════════════════════════════════════════════════════════════


def test_classify_strength_known_pairs() -> None:
    assert classify_strength("支持", "高") == 1.0
    assert classify_strength("支持", "中") == 0.7
    assert classify_strength("部分支持", "中") == 0.5
    assert classify_strength("证据不足", "中") == 0.3
    assert classify_strength("证据不足", "低") == 0.0
    assert classify_strength("未知裁决", "无") == 0.0


def test_format_headline_is_ascii_with_n_tier_verdict() -> None:
    row = pd.Series({"n_obs": 187, "evidence_tier": "core",
                     "verdict": "支持", "confidence": "中"})
    annotation = _format_headline(row)
    assert "n=187" in annotation
    assert "tier=core" in annotation
    assert "support" in annotation
    assert "mid" in annotation
    assert annotation.isascii()


def test_forest_writes_png_and_pdf(tmp_path: Path) -> None:
    csv_path = _make_forest_fixture_csv(tmp_path)
    png_path = tmp_path / "out" / "cma_verdicts_forest.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_forest.pdf"
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path,
            output_png_path=png_path,
            output_pdf_path=pdf_path,
            generated_on=date(2026, 5, 16),
        )
    assert result == png_path
    assert png_path.exists() and png_path.stat().st_size > 0
    assert pdf_path.exists() and pdf_path.stat().st_size > 0
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600


def test_forest_works_without_pdf(tmp_path: Path) -> None:
    csv_path = _make_forest_fixture_csv(tmp_path)
    png_path = tmp_path / "no_pdf" / "forest.png"
    result = build_cma_verdicts_forest_plot(
        verdicts_csv_path=csv_path,
        output_png_path=png_path,
        output_pdf_path=None,
        generated_on=date(2026, 5, 16),
    )
    assert result == png_path
    assert png_path.exists()
    assert not (png_path.parent / "forest.pdf").exists()


def test_forest_raises_on_missing_csv(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=tmp_path / "nope.csv",
            output_png_path=tmp_path / "out.png",
        )


def test_forest_raises_on_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    pd.DataFrame(
        columns=["hid", "verdict", "confidence", "evidence_tier", "n_obs"]
    ).to_csv(csv_path, index=False)
    with pytest.raises(ValueError):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path, output_png_path=tmp_path / "out.png"
        )


def test_forest_raises_on_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    pd.DataFrame([{"hid": "H1", "verdict": "支持"}]).to_csv(csv_path, index=False)
    with pytest.raises(ValueError):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path, output_png_path=tmp_path / "out.png"
        )


def test_forest_raises_on_missing_hypothesis(tmp_path: Path) -> None:
    """A missing hypothesis (only 6 of 7) is a hard error, not a skip."""
    df = _make_forest_fixture_csv(tmp_path)
    panel = pd.read_csv(df)
    panel = panel.loc[panel["hid"] != "H2"]
    csv_path = tmp_path / "partial.csv"
    panel.to_csv(csv_path, index=False)
    with pytest.raises(ValueError, match="H2"):
        build_cma_verdicts_forest_plot(
            verdicts_csv_path=csv_path, output_png_path=tmp_path / "out.png"
        )


def test_forest_strength_score_matches_fixture_pairs(tmp_path: Path) -> None:
    csv_path = _make_forest_fixture_csv(tmp_path)
    df = pd.read_csv(csv_path)
    expected = {"H1": 0.3, "H2": 0.5, "H3": 1.0, "H4": 0.0,
                "H5": 1.0, "H6": 0.3, "H7": 0.7}
    for _, row in df.iterrows():
        score = classify_strength(row["verdict"], row["confidence"])
        assert score == pytest.approx(expected[str(row["hid"])])


def test_forest_real_csv_renders_when_present(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    if not csv_path.exists():
        pytest.skip("cma_hypothesis_verdicts.csv not present in this checkout")
    result = build_cma_verdicts_forest_plot(
        verdicts_csv_path=csv_path,
        output_png_path=tmp_path / "real.png",
        generated_on=date(2026, 5, 16),
    )
    assert result.exists() and result.stat().st_size > 0


# ════════════════════════════════════════════════════════════════════
# Threshold-sensitivity sweep builder (--which sensitivity)
# ════════════════════════════════════════════════════════════════════


def test_sensitivity_sweep_is_7x4() -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_threshold_runner
    )
    assert len(sweep) == 7 * 4
    for threshold in DEFAULT_SENSITIVITY_THRESHOLDS:
        per_t = sweep.loc[sweep["threshold"] == threshold]
        assert set(per_t["hid"]) == {f"H{i}" for i in range(1, 8)}


def test_sensitivity_sweep_strength_tracks_threshold() -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_threshold_runner
    )
    h1_strict = sweep.loc[(sweep["hid"] == "H1") & (sweep["threshold"] == 0.05)].iloc[0]
    assert h1_strict["verdict"] == "证据不足"
    assert h1_strict["strength"] == pytest.approx(0.3)
    h1_loose = sweep.loc[(sweep["hid"] == "H1") & (sweep["threshold"] == 0.20)].iloc[0]
    assert h1_loose["verdict"] == "支持"
    assert h1_loose["strength"] == pytest.approx(1.0)


def test_sensitivity_sweep_dedups_and_sorts() -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=[0.20, 0.10, 0.10, 0.05, 0.15], runner=_threshold_runner
    )
    assert sorted(sweep["threshold"].unique().tolist()) == [0.05, 0.10, 0.15, 0.20]


def test_sensitivity_sweep_raises_on_empty_thresholds() -> None:
    with pytest.raises(ValueError, match="at least one"):
        build_cma_sensitivity_sweep(thresholds=[], runner=_threshold_runner)


def test_sensitivity_sweep_raises_on_dropped_hypothesis() -> None:
    def _broken(threshold: float) -> pd.DataFrame:
        df = _verdicts_at_threshold(threshold)
        return df.loc[df["hid"] != "H3"].reset_index(drop=True)

    with pytest.raises(ValueError, match="H3"):
        build_cma_sensitivity_sweep(
            thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_broken
        )


def test_sensitivity_flip_detection() -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_threshold_runner
    )
    flips = _count_flips_per_hypothesis(sweep)
    assert flips["H1"] == 2
    assert flips["H4"] == 1
    assert flips["H5"] == 1
    assert flips["H2"] == flips["H3"] == flips["H6"] == flips["H7"] == 0


def test_sensitivity_render_writes_png_and_pdf(tmp_path: Path) -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_threshold_runner
    )
    png_path = tmp_path / "out" / "cma_verdicts_sensitivity.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_sensitivity.pdf"
    result = render_sensitivity_forest_plot(
        sweep, output_png=png_path, output_pdf=pdf_path,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    assert png_path.exists() and pdf_path.exists()
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600


def test_sensitivity_render_raises_on_empty_sweep(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=[
        "threshold", "hid", "name_cn", "verdict", "confidence",
        "evidence_tier", "n_obs", "strength"])
    with pytest.raises(ValueError, match="empty"):
        render_sensitivity_forest_plot(empty, output_png=tmp_path / "out.png")


def test_sensitivity_round_trip(tmp_path: Path) -> None:
    png_path = tmp_path / "round_trip.png"
    result = build_cma_sensitivity_forest_plot(
        output_png_path=png_path,
        output_pdf_path=tmp_path / "round_trip.pdf",
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_threshold_runner,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600


# ════════════════════════════════════════════════════════════════════
# AR-engine sweep builder (--which ar)
# ════════════════════════════════════════════════════════════════════


def test_ar_sweep_is_7x2() -> None:
    sweep = build_cma_ar_engine_sweep(ar_models=DEFAULT_AR_MODELS, runner=_engine_runner)
    assert len(sweep) == 7 * 2
    for ar_model in DEFAULT_AR_MODELS:
        per_engine = sweep.loc[sweep["ar_model"] == ar_model]
        assert set(per_engine["hid"]) == {f"H{i}" for i in range(1, 8)}


def test_ar_sweep_strength_per_engine() -> None:
    sweep = build_cma_ar_engine_sweep(ar_models=DEFAULT_AR_MODELS, runner=_engine_runner)
    h1_adj = sweep.loc[(sweep["hid"] == "H1") & (sweep["ar_model"] == AR_MODEL_ADJUSTED)].iloc[0]
    assert h1_adj["verdict"] == "证据不足"
    assert h1_adj["strength"] == pytest.approx(0.3)
    h1_mkt = sweep.loc[(sweep["hid"] == "H1") & (sweep["ar_model"] == AR_MODEL_MARKET)].iloc[0]
    assert h1_mkt["verdict"] == "部分支持"
    assert h1_mkt["strength"] == pytest.approx(0.5)


def test_ar_sweep_canonicalises_engine_order() -> None:
    sweep = build_cma_ar_engine_sweep(
        ar_models=["market", "adjusted", "market"], runner=_engine_runner
    )
    assert sweep["ar_model"].drop_duplicates().tolist() == [
        AR_MODEL_ADJUSTED, AR_MODEL_MARKET]


def test_ar_sweep_rejects_unsupported_engine() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        build_cma_ar_engine_sweep(
            ar_models=["adjusted", "fama_french"], runner=_engine_runner
        )


def test_ar_flip_detection() -> None:
    sweep = build_cma_ar_engine_sweep(ar_models=DEFAULT_AR_MODELS, runner=_engine_runner)
    flipped = _flipped_hypotheses(sweep)
    assert flipped["H1"] is True
    assert flipped["H4"] is True
    assert all(
        flipped[h] is False for h in ("H2", "H3", "H5", "H6", "H7")
    )


def test_ar_render_writes_png_and_pdf(tmp_path: Path) -> None:
    sweep = build_cma_ar_engine_sweep(ar_models=DEFAULT_AR_MODELS, runner=_engine_runner)
    png_path = tmp_path / "out" / "cma_verdicts_ar_engine.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_ar_engine.pdf"
    result = render_ar_engine_forest_plot(
        sweep, output_png=png_path, output_pdf=pdf_path,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    assert png_path.exists() and pdf_path.exists()
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600


def test_ar_render_raises_on_empty_sweep(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=[
        "ar_model", "hid", "name_cn", "verdict", "confidence",
        "evidence_tier", "n_obs", "strength"])
    with pytest.raises(ValueError, match="empty"):
        render_ar_engine_forest_plot(empty, output_png=tmp_path / "out.png")


def test_ar_round_trip(tmp_path: Path) -> None:
    png_path = tmp_path / "round_trip.png"
    result = build_cma_ar_engine_forest_plot(
        output_png_path=png_path,
        output_pdf_path=tmp_path / "round_trip.pdf",
        ar_models=DEFAULT_AR_MODELS,
        runner=_engine_runner,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600


# ════════════════════════════════════════════════════════════════════
# 2D robustness heatmap builder (--which heatmap)
# ════════════════════════════════════════════════════════════════════


def test_heatmap_sweep_is_7x4x2() -> None:
    sweep = build_cma_2d_sweep(
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=DEFAULT_2D_AR_MODELS,
        runner=_cell_runner,
    )
    assert len(sweep) == 7 * 4 * 2 == 56


def test_heatmap_sweep_columns_match_contract() -> None:
    sweep = build_cma_2d_sweep(
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=DEFAULT_2D_AR_MODELS,
        runner=_cell_runner,
    )
    assert set(SWEEP_OUTPUT_COLUMNS).issubset(set(sweep.columns))


def test_heatmap_sweep_rejects_unsupported_engine() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        build_cma_2d_sweep(
            ar_models=["adjusted", "fama_french"], runner=_cell_runner
        )


def test_heatmap_flip_counts() -> None:
    sweep = build_cma_2d_sweep(runner=_cell_runner)
    counts = _flip_count_per_hypothesis(sweep)
    # Engine-only flips
    assert counts["H1"] == 1
    assert counts["H2"] == 1
    # Threshold-only flip
    assert counts["H5"] == 1
    # Two-axis pattern still maps to a single flip
    assert counts["H4"] == 1
    # Rock-solid rows
    assert counts["H3"] == counts["H6"] == counts["H7"] == 0


def test_heatmap_render_writes_png_and_pdf(tmp_path: Path) -> None:
    sweep = build_cma_2d_sweep(runner=_cell_runner)
    png_path = tmp_path / "out" / "cma_verdicts_2d_robustness.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_2d_robustness.pdf"
    result = render_2d_robustness_heatmap(
        sweep, output_png=png_path, output_pdf=pdf_path,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    assert png_path.exists() and pdf_path.exists()
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600


def test_heatmap_render_raises_on_empty_sweep(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=list(SWEEP_OUTPUT_COLUMNS))
    with pytest.raises(ValueError, match="empty"):
        render_2d_robustness_heatmap(empty, output_png=tmp_path / "out.png")


def test_heatmap_round_trip(tmp_path: Path) -> None:
    png_path = tmp_path / "round_trip.png"
    result = build_cma_2d_robustness_heatmap(
        output_png_path=png_path,
        output_pdf_path=tmp_path / "round_trip.pdf",
        runner=_cell_runner,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800 and height >= 600
