"""Tests for the 2D (threshold × AR engine) CMA robustness heatmap.

The eight-cell sweep is mocked with a deterministic fixture runner —
running the full CMA orchestrator eight times (especially the four
market-model panel materialisations) in tests would balloon the suite.
Doctor / paper_bundle integration is covered by the existing test files;
we extend their fixture surface here only enough to assert the
contracts unique to the 2D heatmap (engine separator, flip count,
fallback cache behaviour).
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

from index_inclusion_research.doctor import (
    check_cma_2d_robustness_heatmap_artifact,
)
from index_inclusion_research.outputs import (
    DEFAULT_2D_AR_MODELS,
    DEFAULT_2D_THRESHOLDS,
    build_cma_2d_robustness_heatmap,
    build_cma_2d_robustness_heatmap_from_cache,
    build_cma_2d_sweep,
    build_cma_2d_sweep_from_cache,
    render_2d_robustness_heatmap,
)
from index_inclusion_research.outputs.cma_verdicts_2d_robustness import (
    AR_MODEL_ADJUSTED,
    AR_MODEL_MARKET,
    SWEEP_OUTPUT_COLUMNS,
    _build_strength_grid,
    _cache_csv_path,
    _cache_metadata_path,
    _cma_runner_factory,
    _fallback_single_axis_cache_csv,
    _flip_count_per_hypothesis,
    _flip_label,
    _heatmap_colormap,
    _normalise_threshold_engine_label,
    _parse_grid_label,
    _verdict_to_tag,
    _write_cache_metadata,
)

# ── fixtures ─────────────────────────────────────────────────────────


def _verdicts_at_cell(threshold: float, ar_model: str) -> pd.DataFrame:
    """Return a 7-row verdicts panel that varies with (threshold, engine).

    Engineered so the 56-cell grid exercises every regime we care about
    without ever invoking the real CMA pipeline:

    - H1: flipped along the engine axis only ("证据不足" under adjusted at
      *every* threshold; "支持" under market at *every* threshold). The
      headline AR-engine flip from commit 1a6ba77.
    - H2: flipped along the engine axis only ("部分支持" → "支持"). The
      second-line AR-engine flip.
    - H3 / H6 / H7: 100% stable across both axes (the "rock solid" rows).
    - H4: flips both ways — partial support only at the 0.20 threshold
      with the market engine; insufficient everywhere else (a 2D-only
      pattern that the 1D plots would not surface).
    - H5: flipped along the threshold axis only ("证据不足" at 0.05;
      "支持" at 0.10 / 0.15 / 0.20). A p-gated row whose verdict tracks
      the threshold knob, identical under both engines.
    """

    def row(
        hid: str,
        name: str,
        verdict: str,
        confidence: str,
        tier: str,
        n: int,
    ) -> dict[str, object]:
        return {
            "hid": hid,
            "name_cn": name,
            "verdict": verdict,
            "confidence": confidence,
            "evidence_tier": tier,
            "n_obs": n,
        }

    is_market = ar_model == AR_MODEL_MARKET
    h1 = row(
        "H1",
        "信息泄露与预运行",
        "支持" if is_market else "证据不足",
        "高" if is_market else "中",
        "core",
        436,
    )
    h2 = row(
        "H2",
        "被动基金 AUM 差异",
        "支持" if is_market else "部分支持",
        "中",
        "core",
        17,
    )
    h3 = row("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
    h4 = row(
        "H4",
        "卖空约束",
        "部分支持" if (is_market and abs(threshold - 0.20) < 1e-9) else "证据不足",
        "中",
        "supplementary",
        40,
    )
    h5 = row(
        "H5",
        "涨跌停限制",
        "证据不足" if abs(threshold - 0.05) < 1e-9 else "支持",
        "中" if abs(threshold - 0.05) < 1e-9 else "高",
        "core",
        936,
    )
    h6 = row("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
    h7 = row("H7", "行业结构差异", "支持", "中", "core", 187)
    return pd.DataFrame([h1, h2, h3, h4, h5, h6, h7])


def _fixture_runner(threshold: float, ar_model: str) -> pd.DataFrame:
    return _verdicts_at_cell(threshold, ar_model)


def _write_cached_verdicts(
    sensitivity_root: Path,
    threshold: float,
    ar_model: str,
) -> Path:
    csv_path = _cache_csv_path(
        threshold, ar_model, sensitivity_root=sensitivity_root
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    _verdicts_at_cell(threshold, ar_model).to_csv(csv_path, index=False)
    _write_cache_metadata(csv_path, threshold=threshold, ar_model=ar_model)
    return csv_path


def _touch_with_mtime(path: Path, *, mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    os.utime(path, (mtime, mtime))


def _set_mtime(path: Path, *, mtime: float) -> None:
    os.utime(path, (mtime, mtime))


# ── build_cma_2d_sweep ───────────────────────────────────────────────


def test_sweep_cell_count_is_7x4x2() -> None:
    """Spec: 7 hypotheses × 4 thresholds × 2 engines = 56 rows."""
    sweep = build_cma_2d_sweep(
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=DEFAULT_2D_AR_MODELS,
        runner=_fixture_runner,
    )
    assert len(sweep) == 7 * 4 * 2 == 56, (
        f"expected 56 rows (7 hypotheses × 4 thresholds × 2 engines), "
        f"got {len(sweep)}"
    )


def test_sweep_columns_match_contract() -> None:
    sweep = build_cma_2d_sweep(
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=DEFAULT_2D_AR_MODELS,
        runner=_fixture_runner,
    )
    required = set(SWEEP_OUTPUT_COLUMNS)
    assert required.issubset(set(sweep.columns)), (
        f"missing columns: {required - set(sweep.columns)}"
    )


def test_sweep_canonicalises_engine_order() -> None:
    sweep = build_cma_2d_sweep(
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=["market", "adjusted", "market"],
        runner=_fixture_runner,
    )
    distinct_in_order = sweep["ar_model"].drop_duplicates().tolist()
    assert distinct_in_order == [AR_MODEL_ADJUSTED, AR_MODEL_MARKET]


def test_sweep_raises_when_thresholds_empty() -> None:
    with pytest.raises(ValueError, match="at least one"):
        build_cma_2d_sweep(thresholds=[], runner=_fixture_runner)


def test_sweep_raises_when_ar_models_empty() -> None:
    with pytest.raises(ValueError, match="at least one"):
        build_cma_2d_sweep(ar_models=[], runner=_fixture_runner)


def test_sweep_rejects_unsupported_ar_model() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        build_cma_2d_sweep(
            ar_models=["adjusted", "fama_french"], runner=_fixture_runner
        )


def test_sweep_raises_when_runner_drops_hypothesis() -> None:
    def _broken(threshold: float, ar_model: str) -> pd.DataFrame:
        df = _verdicts_at_cell(threshold, ar_model)
        return df.loc[df["hid"] != "H3"].reset_index(drop=True)

    with pytest.raises(ValueError, match="H3"):
        build_cma_2d_sweep(runner=_broken)


def test_sweep_raises_when_runner_missing_columns() -> None:
    def _truncated(threshold: float, ar_model: str) -> pd.DataFrame:
        return _verdicts_at_cell(threshold, ar_model).drop(columns=["evidence_tier"])

    with pytest.raises(ValueError, match="evidence_tier"):
        build_cma_2d_sweep(runner=_truncated)


def test_sweep_strength_score_matches_verdict_per_cell() -> None:
    sweep = build_cma_2d_sweep(
        thresholds=DEFAULT_2D_THRESHOLDS,
        ar_models=DEFAULT_2D_AR_MODELS,
        runner=_fixture_runner,
    )
    # H1 under (0.05, adjusted) is "证据不足/中" → strength 0.3
    threshold_col = sweep["threshold"].astype(float).round(2)
    cell = sweep.loc[
        (sweep["hid"] == "H1")
        & (sweep["ar_model"] == AR_MODEL_ADJUSTED)
        & (threshold_col == 0.05)
    ].iloc[0]
    assert cell["verdict"] == "证据不足"
    assert cell["strength"] == pytest.approx(0.3)
    # H1 under (0.05, market) is "支持/高" → strength 1.0
    cell = sweep.loc[
        (sweep["hid"] == "H1")
        & (sweep["ar_model"] == AR_MODEL_MARKET)
        & (threshold_col == 0.05)
    ].iloc[0]
    assert cell["verdict"] == "支持"
    assert cell["strength"] == pytest.approx(1.0)


# ── flip count detection ─────────────────────────────────────────────


def test_flip_count_detects_engine_only_flips() -> None:
    """H1 / H2 flip on engine axis only → 1 flip (2 distinct verdicts)."""
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    counts = _flip_count_per_hypothesis(sweep)
    assert counts["H1"] == 1
    assert counts["H2"] == 1


def test_flip_count_detects_threshold_only_flips() -> None:
    """H5 flips at threshold 0.05 only → 1 flip (2 distinct verdicts)."""
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    counts = _flip_count_per_hypothesis(sweep)
    assert counts["H5"] == 1


def test_flip_count_detects_two_axis_pattern() -> None:
    """H4 (partial only at threshold 0.20 + market) → 1 flip."""
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    counts = _flip_count_per_hypothesis(sweep)
    assert counts["H4"] == 1


def test_flip_count_stable_rows() -> None:
    """H3 / H6 / H7 are identical across all 8 cells → 0 flips."""
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    counts = _flip_count_per_hypothesis(sweep)
    assert counts["H3"] == 0
    assert counts["H6"] == 0
    assert counts["H7"] == 0


def test_flip_count_three_plus_distinct_returns_two() -> None:
    """A hypothesis with 3+ distinct verdicts maps to '2+ flips' (count=2)."""
    def _three_verdicts(threshold: float, ar_model: str) -> pd.DataFrame:
        df = _verdicts_at_cell(threshold, ar_model)
        # Mutate H6 to take three distinct verdicts.
        if abs(threshold - 0.05) < 1e-9:
            df.loc[df["hid"] == "H6", "verdict"] = "证据不足"
        elif abs(threshold - 0.10) < 1e-9:
            df.loc[df["hid"] == "H6", "verdict"] = "部分支持"
        else:
            df.loc[df["hid"] == "H6", "verdict"] = "支持"
        return df

    sweep = build_cma_2d_sweep(runner=_three_verdicts)
    counts = _flip_count_per_hypothesis(sweep)
    assert counts["H6"] == 2


def test_flip_label_lookup() -> None:
    assert _flip_label(0) == "stable"
    assert _flip_label(1) == "1 flip"
    assert _flip_label(2) == "2+ flips"
    assert _flip_label(5) == "2+ flips"


def test_verdict_to_tag_lookup() -> None:
    assert _verdict_to_tag("支持", 1.0) == "S+"
    assert _verdict_to_tag("支持", 0.7) == "S"
    assert _verdict_to_tag("部分支持", 0.5) == "P+"
    assert _verdict_to_tag("证据不足", 0.3) == "I"
    assert _verdict_to_tag("证据不足", 0.0) == "I"
    assert _verdict_to_tag("unknown", 0.5) == "?"


# ── grid layout ──────────────────────────────────────────────────────


def test_build_strength_grid_is_7x8() -> None:
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    strength_matrix, tag_matrix, column_labels = _build_strength_grid(
        sweep,
        thresholds=sorted(set(sweep["threshold"].round(2).tolist())),
        ar_models=DEFAULT_2D_AR_MODELS,
    )
    assert len(strength_matrix) == 7
    assert all(len(row) == 8 for row in strength_matrix)
    assert len(tag_matrix) == 7
    assert all(len(row) == 8 for row in tag_matrix)
    assert len(column_labels) == 8
    # Column 0..3 = adjusted block, 4..7 = market block.
    assert all("adj" in col for col in column_labels[:4])
    assert all("mar" in col for col in column_labels[4:])


def test_build_strength_grid_groups_engines_contiguously() -> None:
    """Spec: the 4 adjusted columns precede the 4 market columns."""
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    _, _, column_labels = _build_strength_grid(
        sweep,
        thresholds=sorted(set(sweep["threshold"].round(2).tolist())),
        ar_models=DEFAULT_2D_AR_MODELS,
    )
    # All adjusted labels appear before any market label.
    adjusted_idx = max(i for i, c in enumerate(column_labels) if "adj" in c)
    market_idx = min(i for i, c in enumerate(column_labels) if "mar" in c)
    assert adjusted_idx < market_idx


# ── render_2d_robustness_heatmap ─────────────────────────────────────


def test_render_writes_png_and_pdf(tmp_path: Path) -> None:
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    png_path = tmp_path / "out" / "cma_verdicts_2d_robustness.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_2d_robustness.pdf"
    result = render_2d_robustness_heatmap(
        sweep,
        output_png=png_path,
        output_pdf=pdf_path,
        generated_on=date(2026, 5, 17),
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


def test_render_works_without_pdf(tmp_path: Path) -> None:
    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    png_path = tmp_path / "no_pdf" / "heatmap.png"
    render_2d_robustness_heatmap(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )
    assert png_path.exists()
    assert not (png_path.parent / "heatmap.pdf").exists()


def test_render_draws_56_annotated_cells(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec: every (hypothesis × cell) gets a text annotation = 7 × 8 = 56."""
    from index_inclusion_research.outputs import (
        cma_verdicts_2d_robustness as module,
    )

    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    png_path = tmp_path / "annotated.png"

    text_calls: list[dict[str, object]] = []
    real_subplots = module.plt.subplots

    def _capture_subplots(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        fig, ax = real_subplots(*args, **kwargs)
        real_text = ax.text

        def _spy_text(*a: object, **kw: object):  # type: ignore[no-untyped-def]
            # We only care about the cell annotations — they live at
            # integer (x, y) coords. Filter out axis labels / captions
            # at floats outside the grid.
            if len(a) >= 3 and isinstance(a[0], int) and isinstance(a[1], int):
                if 0 <= a[0] < 8 and 0 <= a[1] < 7:
                    text_calls.append({"x": a[0], "y": a[1], "text": a[2]})
            return real_text(*a, **kw)

        ax.text = _spy_text  # type: ignore[assignment]
        return fig, ax

    monkeypatch.setattr(module.plt, "subplots", _capture_subplots)
    render_2d_robustness_heatmap(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )

    # All 56 cells get a tag (the fixture has no NaN cells).
    assert len(text_calls) == 56, (
        f"expected 56 annotated cells (7 hypotheses × 8 cells), "
        f"got {len(text_calls)}"
    )


def test_render_draws_engine_separator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec: a single vertical separator between the engine column groups."""
    from index_inclusion_research.outputs import (
        cma_verdicts_2d_robustness as module,
    )

    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    png_path = tmp_path / "sep.png"

    axvline_calls: list[dict[str, object]] = []
    real_subplots = module.plt.subplots

    def _capture_subplots(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        fig, ax = real_subplots(*args, **kwargs)
        real_axvline = ax.axvline

        def _spy_axvline(*a: object, **kw: object):  # type: ignore[no-untyped-def]
            axvline_calls.append({"args": a, "kwargs": kw})
            return real_axvline(*a, **kw)

        ax.axvline = _spy_axvline  # type: ignore[assignment]
        return fig, ax

    monkeypatch.setattr(module.plt, "subplots", _capture_subplots)
    render_2d_robustness_heatmap(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )

    # Exactly one axvline call (the engine-group separator at x=3.5).
    assert len(axvline_calls) == 1
    sep_x = axvline_calls[0]["args"][0]
    assert sep_x == pytest.approx(3.5), (
        f"engine separator should sit at x=3.5 (between cols 3 and 4); "
        f"got {sep_x}"
    )


def test_render_legend_includes_both_engine_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The figure must label both engine groups so reviewers can decode.

    The 2D heatmap doesn't use a matplotlib legend (every cell color is
    self-explanatory via the colorbar), but it does write engine-group
    labels above the columns. We check the text rendered above the
    grid contains both engine names.
    """
    from index_inclusion_research.outputs import (
        cma_verdicts_2d_robustness as module,
    )

    sweep = build_cma_2d_sweep(runner=_fixture_runner)
    png_path = tmp_path / "legend.png"

    text_calls: list[str] = []
    real_subplots = module.plt.subplots

    def _capture_subplots(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        fig, ax = real_subplots(*args, **kwargs)
        real_text = ax.text

        def _spy_text(*a: object, **kw: object):  # type: ignore[no-untyped-def]
            if len(a) >= 3:
                text_calls.append(str(a[2]))
            return real_text(*a, **kw)

        ax.text = _spy_text  # type: ignore[assignment]
        return fig, ax

    monkeypatch.setattr(module.plt, "subplots", _capture_subplots)
    render_2d_robustness_heatmap(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )

    joined = " | ".join(text_calls)
    assert "adjusted" in joined.lower()
    assert "market" in joined.lower()


def test_render_color_mapping_at_boundary_scores() -> None:
    """Colormap: 0.0 ≈ deep red, 0.5 ≈ near-white, 1.0 ≈ deep blue."""
    cmap = _heatmap_colormap()
    red = cmap(0.0)
    white = cmap(0.5)
    blue = cmap(1.0)
    # Red dominant at 0.0
    assert red[0] > red[2], f"0.0 should be reddish, got RGB={red}"
    # Blue dominant at 1.0
    assert blue[2] > blue[0], f"1.0 should be bluish, got RGB={blue}"
    # Near-white at 0.5 (all channels above 0.85)
    assert min(white[0], white[1], white[2]) > 0.85, (
        f"0.5 should be near-white, got RGB={white}"
    )


def test_render_raises_on_empty_sweep(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=list(SWEEP_OUTPUT_COLUMNS))
    with pytest.raises(ValueError, match="empty"):
        render_2d_robustness_heatmap(empty, output_png=tmp_path / "out.png")


def test_render_raises_on_missing_columns(tmp_path: Path) -> None:
    bad = pd.DataFrame([{"threshold": 0.10, "hid": "H1"}])
    with pytest.raises(ValueError, match="missing columns"):
        render_2d_robustness_heatmap(bad, output_png=tmp_path / "out.png")


# ── high-level wrapper ───────────────────────────────────────────────


def test_build_cma_2d_robustness_heatmap_round_trip(tmp_path: Path) -> None:
    """sweep + render in one call yields a non-empty PNG."""
    png_path = tmp_path / "round_trip.png"
    pdf_path = tmp_path / "round_trip.pdf"
    result = build_cma_2d_robustness_heatmap(
        output_png_path=png_path,
        output_pdf_path=pdf_path,
        runner=_fixture_runner,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800
    assert height >= 600


# ── cache helpers ────────────────────────────────────────────────────


def test_cache_csv_path_uses_grid_prefix(tmp_path: Path) -> None:
    path = _cache_csv_path(0.10, AR_MODEL_MARKET, sensitivity_root=tmp_path)
    assert path == tmp_path / "grid_0_10_market" / "cma_hypothesis_verdicts.csv"


def test_cache_metadata_path_lives_next_to_verdict_csv(tmp_path: Path) -> None:
    csv_path = _cache_csv_path(0.10, AR_MODEL_MARKET, sensitivity_root=tmp_path)
    assert _cache_metadata_path(csv_path) == (
        tmp_path / "grid_0_10_market" / "cma_2d_robustness_cache_metadata.json"
    )


def test_grid_label_round_trip() -> None:
    label = _normalise_threshold_engine_label(0.05, "adjusted")
    assert label == "grid_0_05_adjusted"
    threshold, ar_model = _parse_grid_label(label)
    assert threshold == pytest.approx(0.05)
    assert ar_model == AR_MODEL_ADJUSTED


def test_parse_grid_label_rejects_bad_prefix() -> None:
    with pytest.raises(ValueError, match="invalid 2D grid"):
        _parse_grid_label("threshold_0_10")
    with pytest.raises(ValueError):
        _parse_grid_label("grid_0_10")  # missing engine


def test_fallback_uses_ar_engine_cache_at_default_threshold(
    tmp_path: Path,
) -> None:
    """At threshold 0.10, the (T, engine) cell falls back to ar_<engine>/."""
    ar_csv = tmp_path / "ar_market" / "cma_hypothesis_verdicts.csv"
    ar_csv.parent.mkdir(parents=True, exist_ok=True)
    ar_csv.write_bytes(b"")
    assert (
        _fallback_single_axis_cache_csv(
            0.10, AR_MODEL_MARKET, sensitivity_root=tmp_path
        )
        == ar_csv
    )


def test_fallback_uses_threshold_cache_when_adjusted(tmp_path: Path) -> None:
    """At ar_model='adjusted', non-default thresholds fall back to threshold_<T>/."""
    thr_csv = tmp_path / "threshold_0_05" / "cma_hypothesis_verdicts.csv"
    thr_csv.parent.mkdir(parents=True, exist_ok=True)
    thr_csv.write_bytes(b"")
    assert (
        _fallback_single_axis_cache_csv(
            0.05, AR_MODEL_ADJUSTED, sensitivity_root=tmp_path
        )
        == thr_csv
    )


def test_fallback_returns_none_for_market_off_default_threshold(
    tmp_path: Path,
) -> None:
    """The (T ≠ 0.10, market) cells have no single-axis fallback."""
    # Create an unrelated cache to make sure the function isn't just
    # always None.
    _touch_with_mtime(
        tmp_path / "threshold_0_05" / "cma_hypothesis_verdicts.csv",
        mtime=100.0,
    )
    assert (
        _fallback_single_axis_cache_csv(
            0.05, AR_MODEL_MARKET, sensitivity_root=tmp_path
        )
        is None
    )


def test_cache_only_sweep_reads_existing_csvs_without_orchestrator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache-only rendering must not fall through to fresh CMA runs."""
    for threshold in DEFAULT_2D_THRESHOLDS:
        for ar_model in DEFAULT_2D_AR_MODELS:
            _write_cached_verdicts(tmp_path, threshold, ar_model)

    from index_inclusion_research.outputs import (
        cma_verdicts_2d_robustness as module,
    )

    monkeypatch.setattr(
        module,
        "_cma_runner_factory",
        lambda *args, **kwargs: pytest.fail("fresh CMA runner should not be used"),
    )

    sweep = build_cma_2d_sweep_from_cache(sensitivity_root=tmp_path)
    assert set(sweep["ar_model"]) == set(DEFAULT_2D_AR_MODELS)
    assert len(sweep) == 56


def test_cache_only_plot_renders_from_existing_csvs(tmp_path: Path) -> None:
    cache_root = tmp_path / "sensitivity"
    for threshold in DEFAULT_2D_THRESHOLDS:
        for ar_model in DEFAULT_2D_AR_MODELS:
            _write_cached_verdicts(cache_root, threshold, ar_model)
    png_path = tmp_path / "out" / "cma_verdicts_2d_robustness.png"

    result = build_cma_2d_robustness_heatmap_from_cache(
        output_png_path=png_path,
        output_pdf_path=None,
        sensitivity_root=cache_root,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    assert png_path.exists()


def test_cache_only_sweep_raises_when_no_cache_present(tmp_path: Path) -> None:
    cache_root = tmp_path / "empty_sensitivity"
    cache_root.mkdir()
    with pytest.raises(ValueError, match="no cached"):
        build_cma_2d_sweep_from_cache(sensitivity_root=cache_root)


def test_cache_only_sweep_uses_single_axis_fallbacks(tmp_path: Path) -> None:
    """When only the 1D caches exist, the cache-only sweep should still
    serve every (T, engine) cell that has a single-axis fallback.

    Layout: 4 threshold_<T>/ caches + 2 ar_<engine>/ caches → covers
    8 cells via fallback (the 4 (T, adjusted) cells from threshold_*
    and the 2 (0.10, engine) cells from ar_*; the latter overlaps with
    threshold_0_10/adjusted but de-dups to a single combo).
    """
    cache_root = tmp_path / "sensitivity"
    # Populate the threshold sweep (covers all 4 (T, adjusted) cells).
    for threshold in DEFAULT_2D_THRESHOLDS:
        thr_dir = cache_root / f"threshold_{f'{threshold:.2f}'.replace('.', '_')}"
        thr_dir.mkdir(parents=True, exist_ok=True)
        _verdicts_at_cell(threshold, AR_MODEL_ADJUSTED).to_csv(
            thr_dir / "cma_hypothesis_verdicts.csv", index=False
        )
    # Populate the AR-engine sweep (covers (0.10, adjusted) + (0.10, market)).
    for ar_model in DEFAULT_2D_AR_MODELS:
        ar_dir = cache_root / f"ar_{ar_model}"
        ar_dir.mkdir(parents=True, exist_ok=True)
        _verdicts_at_cell(0.10, ar_model).to_csv(
            ar_dir / "cma_hypothesis_verdicts.csv", index=False
        )

    sweep = build_cma_2d_sweep_from_cache(sensitivity_root=cache_root)
    # 5 unique cells via fallback: (0.05/0.10/0.15/0.20, adjusted) +
    # (0.10, market). Render should not crash even though we're missing
    # the (0.05/0.15/0.20, market) cells.
    combos = {(round(float(t), 2), m) for t, m in zip(
        sweep["threshold"], sweep["ar_model"], strict=False
    )}
    assert (0.10, AR_MODEL_MARKET) in combos
    assert (0.05, AR_MODEL_ADJUSTED) in combos
    assert (0.20, AR_MODEL_ADJUSTED) in combos


def test_cma_runner_serves_dedicated_cache_when_metadata_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A populated grid_<T>_<engine>/ cache with matching metadata and
    fresh-relative-to-inputs mtime must not trigger the orchestrator.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    cache_csv = _write_cached_verdicts(
        sensitivity_root, 0.10, AR_MODEL_ADJUSTED
    )
    _set_mtime(cache_csv, mtime=200.0)
    upstreams = [tmp_path / f"input_{idx}.csv" for idx in range(3)]
    for path in upstreams:
        _touch_with_mtime(path, mtime=100.0)

    monkeypatch.setattr(
        orchestrator,
        "run_cma_pipeline",
        lambda **kwargs: pytest.fail("fresh CMA run should not happen"),
    )
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=upstreams,
        allow_fallback=False,
    )

    result = runner(0.10, AR_MODEL_ADJUSTED)
    assert len(result) == 7


def test_cma_runner_uses_single_axis_fallback_when_no_grid_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When only the threshold_<T>/ cache exists, the runner should
    promote it into a grid_<T>_adjusted/ cache rather than rerun.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    thr_dir = sensitivity_root / "threshold_0_05"
    thr_dir.mkdir(parents=True, exist_ok=True)
    _verdicts_at_cell(0.05, AR_MODEL_ADJUSTED).to_csv(
        thr_dir / "cma_hypothesis_verdicts.csv", index=False
    )

    monkeypatch.setattr(
        orchestrator,
        "run_cma_pipeline",
        lambda **kwargs: pytest.fail("fresh CMA run should not happen"),
    )
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=[],
    )

    result = runner(0.05, AR_MODEL_ADJUSTED)
    assert len(result) == 7
    # The runner should have promoted the fallback into a grid cache.
    grid_csv = _cache_csv_path(
        0.05, AR_MODEL_ADJUSTED, sensitivity_root=sensitivity_root
    )
    assert grid_csv.exists()


def test_cma_runner_ignores_stale_single_axis_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stale 1D fallback must not seed a fresh 2D publication cache."""
    from index_inclusion_research.outputs import cma_verdicts_ar_engine as engine_module

    sensitivity_root = tmp_path / "sensitivity"
    fallback_csv = sensitivity_root / "threshold_0_05" / "cma_hypothesis_verdicts.csv"
    fallback_csv.parent.mkdir(parents=True, exist_ok=True)
    _verdicts_at_cell(0.05, AR_MODEL_ADJUSTED).to_csv(fallback_csv, index=False)
    _set_mtime(fallback_csv, mtime=100.0)
    upstream = tmp_path / "upstream.csv"
    _touch_with_mtime(upstream, mtime=200.0)

    fresh = _verdicts_at_cell(0.05, AR_MODEL_MARKET)
    monkeypatch.setattr(
        engine_module,
        "_cma_runner_factory",
        lambda **kwargs: (lambda ar_model: fresh),
    )
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=[upstream],
    )

    result = runner(0.05, AR_MODEL_ADJUSTED)
    assert result.loc[result["hid"] == "H1", "verdict"].iloc[0] == "支持"
    grid_csv = _cache_csv_path(
        0.05, AR_MODEL_ADJUSTED, sensitivity_root=sensitivity_root
    )
    assert grid_csv.exists()


# ── doctor integration ───────────────────────────────────────────────


def test_doctor_check_passes_when_cache_empty(tmp_path: Path) -> None:
    """No sensitivity cache → opt-in pass, no warn."""
    result = check_cma_2d_robustness_heatmap_artifact(
        png_path=tmp_path / "absent.png",
        pdf_path=tmp_path / "absent.pdf",
        sensitivity_root=tmp_path / "sensitivity",
    )
    assert result.status == "pass"
    assert "opt-in" in result.message


def test_doctor_check_passes_when_cache_dir_empty(tmp_path: Path) -> None:
    sens_root = tmp_path / "sensitivity"
    sens_root.mkdir()
    result = check_cma_2d_robustness_heatmap_artifact(
        png_path=tmp_path / "absent.png",
        pdf_path=tmp_path / "absent.pdf",
        sensitivity_root=sens_root,
    )
    assert result.status == "pass"


def test_doctor_check_warns_when_artifact_missing_but_cache_populated(
    tmp_path: Path,
) -> None:
    sens_root = tmp_path / "sensitivity"
    _touch_with_mtime(
        sens_root / "grid_0_10_adjusted" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_000.0,
    )
    result = check_cma_2d_robustness_heatmap_artifact(
        png_path=tmp_path / "missing.png",
        pdf_path=tmp_path / "missing.pdf",
        sensitivity_root=sens_root,
    )
    assert result.status == "warn"
    assert "missing" in result.message
    assert "cma-2d-robustness-heatmap" in result.fix


def test_doctor_check_warns_when_artifact_stale(tmp_path: Path) -> None:
    sens_root = tmp_path / "sensitivity"
    _touch_with_mtime(
        sens_root / "grid_0_10_market" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_200.0,
    )
    png = tmp_path / "heatmap.png"
    pdf = tmp_path / "heatmap.pdf"
    _touch_with_mtime(png, mtime=1_700_000_100.0)
    _touch_with_mtime(pdf, mtime=1_700_000_100.0)
    result = check_cma_2d_robustness_heatmap_artifact(
        png_path=png,
        pdf_path=pdf,
        sensitivity_root=sens_root,
    )
    assert result.status == "warn"
    assert "overdue" in result.message
    assert any("newer than" in d for d in result.details)


def test_doctor_check_passes_when_artifact_fresh(tmp_path: Path) -> None:
    sens_root = tmp_path / "sensitivity"
    _touch_with_mtime(
        sens_root / "grid_0_10_adjusted" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_000.0,
    )
    _touch_with_mtime(
        sens_root / "grid_0_10_market" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_050.0,
    )
    png = tmp_path / "heatmap.png"
    pdf = tmp_path / "heatmap.pdf"
    _touch_with_mtime(png, mtime=1_700_000_500.0)
    _touch_with_mtime(pdf, mtime=1_700_000_500.0)
    result = check_cma_2d_robustness_heatmap_artifact(
        png_path=png,
        pdf_path=pdf,
        sensitivity_root=sens_root,
    )
    assert result.status == "pass"
    assert "cached cell CSV" in result.message


def test_doctor_check_counts_single_axis_caches(tmp_path: Path) -> None:
    """The check should count single-axis caches as valid inputs since
    they form a fallback for the 2D heatmap.
    """
    sens_root = tmp_path / "sensitivity"
    _touch_with_mtime(
        sens_root / "threshold_0_05" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_100.0,
    )
    result = check_cma_2d_robustness_heatmap_artifact(
        png_path=tmp_path / "absent.png",
        pdf_path=tmp_path / "absent.pdf",
        sensitivity_root=sens_root,
    )
    # The PNG/PDF don't exist, but a fallback cache does → warn (not pass).
    assert result.status == "warn"
