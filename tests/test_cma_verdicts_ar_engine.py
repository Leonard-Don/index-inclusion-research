"""Tests for the AR-engine-aware CMA verdicts forest plot.

The two-engine sweep is mocked with a deterministic fixture runner —
running the full CMA orchestrator twice (especially the slow market-
model panel materialisation) in tests would balloon the suite. Doctor /
paper_bundle integration is covered by the existing test files (we only
extend their fixture surface here).
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

from index_inclusion_research.doctor import (
    check_cma_ar_engine_forest_artifact,
)
from index_inclusion_research.outputs import (
    DEFAULT_AR_ENGINE_THRESHOLD,
    DEFAULT_AR_MODELS,
    build_cma_ar_engine_forest_plot,
    build_cma_ar_engine_forest_plot_from_cache,
    build_cma_ar_engine_sweep,
    build_cma_ar_engine_sweep_from_cache,
    render_ar_engine_forest_plot,
)
from index_inclusion_research.outputs.cma_verdicts_ar_engine import (
    AR_MODEL_ADJUSTED,
    AR_MODEL_MARKET,
    _ar_model_from_cache_dir,
    _cache_csv_path,
    _cache_metadata_path,
    _cma_runner_factory,
    _default_upstream_inputs,
    _flip_label,
    _flipped_hypotheses,
    _normalise_ar_model,
    _normalise_ar_models,
    _write_cache_metadata,
)

# ── fixtures ─────────────────────────────────────────────────────────


def _verdicts_at_engine(ar_model: str) -> pd.DataFrame:
    """Return a 7-row verdicts panel whose verdicts shift with AR engine.

    Engineered so the test suite exercises every meaningful regime
    without ever calling the real CMA pipeline:

    - H1 is *flipped* between engines (insufficient under adjusted,
      partial under market — picks up extra residual signal when β > 1).
    - H4 is also *flipped* (insufficient → partial).
    - H2 / H3 / H5 / H6 / H7 stay stable across engines (the non-p
      headline gates are robust to AR re-definition in this fixture).
    """
    rows = []

    def add(
        hid: str,
        name: str,
        verdict: str,
        confidence: str,
        tier: str,
        n: int,
        p_value: float | None = None,
    ) -> None:
        rows.append(
            {
                "hid": hid,
                "name_cn": name,
                "verdict": verdict,
                "confidence": confidence,
                "evidence_tier": tier,
                "n_obs": n,
                "p_value": p_value,
            }
        )

    if ar_model == AR_MODEL_ADJUSTED:
        # adjusted (ret − benchmark): the historic verdicts panel.
        add("H1", "信息泄露与预运行", "证据不足", "中", "core", 436, 0.875)
        add("H2", "被动基金 AUM 差异", "部分支持", "中", "core", 17)
        add("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
        add("H4", "卖空约束", "证据不足", "中", "supplementary", 40, 0.537)
        add("H5", "涨跌停限制", "支持", "高", "core", 936, 0.008)
        add("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
        add("H7", "行业结构差异", "支持", "中", "core", 187)
    elif ar_model == AR_MODEL_MARKET:
        # market-model β-AR: H1 / H4 pick up extra residual signal.
        add("H1", "信息泄露与预运行", "部分支持", "中", "core", 430, 0.072)
        add("H2", "被动基金 AUM 差异", "部分支持", "中", "core", 17)
        add("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
        add("H4", "卖空约束", "部分支持", "中", "supplementary", 38, 0.082)
        add("H5", "涨跌停限制", "支持", "高", "core", 920, 0.010)
        add("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
        add("H7", "行业结构差异", "支持", "中", "core", 184)
    else:  # pragma: no cover — defensive
        raise AssertionError(f"unexpected ar_model {ar_model!r} in fixture")

    return pd.DataFrame(rows)


def _fixture_runner(ar_model: str) -> pd.DataFrame:
    """Deterministic in-process runner — no orchestrator invocation."""
    return _verdicts_at_engine(ar_model)


def _write_cached_verdicts(
    sensitivity_root: Path,
    ar_model: str,
    *,
    threshold: float = DEFAULT_AR_ENGINE_THRESHOLD,
) -> Path:
    csv_path = _cache_csv_path(ar_model, sensitivity_root=sensitivity_root)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    _verdicts_at_engine(ar_model).to_csv(csv_path, index=False)
    _write_cache_metadata(csv_path, ar_model=ar_model, threshold=threshold)
    return csv_path


def _touch_with_mtime(path: Path, *, mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    os.utime(path, (mtime, mtime))


def _set_mtime(path: Path, *, mtime: float) -> None:
    os.utime(path, (mtime, mtime))


# ── build_cma_ar_engine_sweep ────────────────────────────────────────


def test_sweep_dot_count_is_7x2() -> None:
    """Spec: H1-H7 × 2 engines = 14 dots in the long-format frame."""
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    assert len(sweep) == 7 * 2, (
        f"expected 14 rows (7 hypotheses × 2 engines), got {len(sweep)}"
    )
    for ar_model in DEFAULT_AR_MODELS:
        per_engine = sweep.loc[sweep["ar_model"] == ar_model]
        assert set(per_engine["hid"]) == {"H1", "H2", "H3", "H4", "H5", "H6", "H7"}, (
            f"ar_model={ar_model!r} missing hypotheses"
        )


def test_sweep_columns_match_contract() -> None:
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    required = {
        "ar_model",
        "hid",
        "name_cn",
        "verdict",
        "confidence",
        "evidence_tier",
        "n_obs",
        "strength",
    }
    assert required.issubset(set(sweep.columns)), (
        f"missing columns: {required - set(sweep.columns)}"
    )


def test_sweep_strength_score_matches_verdict_per_engine() -> None:
    """The sweep should carry the support-strength derived from
    (verdict, confidence) at *each* engine — not the snapshot value.
    """
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    h1_adjusted = sweep.loc[
        (sweep["hid"] == "H1") & (sweep["ar_model"] == AR_MODEL_ADJUSTED)
    ].iloc[0]
    assert h1_adjusted["verdict"] == "证据不足"
    assert h1_adjusted["strength"] == pytest.approx(0.3)

    h1_market = sweep.loc[
        (sweep["hid"] == "H1") & (sweep["ar_model"] == AR_MODEL_MARKET)
    ].iloc[0]
    assert h1_market["verdict"] == "部分支持"
    assert h1_market["strength"] == pytest.approx(0.5)


def test_sweep_de_duplicates_and_canonicalises_engine_order() -> None:
    """Duplicate / reversed engine input should normalise to (adjusted, market)."""
    sweep = build_cma_ar_engine_sweep(
        ar_models=["market", "adjusted", "market"],
        runner=_fixture_runner,
    )
    distinct_in_order = sweep["ar_model"].drop_duplicates().tolist()
    assert distinct_in_order == [AR_MODEL_ADJUSTED, AR_MODEL_MARKET]


def test_sweep_raises_when_ar_models_empty() -> None:
    with pytest.raises(ValueError, match="at least one"):
        build_cma_ar_engine_sweep(ar_models=[], runner=_fixture_runner)


def test_sweep_rejects_unsupported_ar_model() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        build_cma_ar_engine_sweep(
            ar_models=["adjusted", "fama_french"],
            runner=_fixture_runner,
        )


def test_sweep_raises_when_runner_drops_hypothesis() -> None:
    def _broken(ar_model: str) -> pd.DataFrame:
        df = _verdicts_at_engine(ar_model)
        return df.loc[df["hid"] != "H3"].reset_index(drop=True)

    with pytest.raises(ValueError, match="H3"):
        build_cma_ar_engine_sweep(
            ar_models=DEFAULT_AR_MODELS, runner=_broken
        )


def test_sweep_raises_when_runner_missing_columns() -> None:
    def _truncated(ar_model: str) -> pd.DataFrame:
        return _verdicts_at_engine(ar_model).drop(columns=["evidence_tier"])

    with pytest.raises(ValueError, match="evidence_tier"):
        build_cma_ar_engine_sweep(
            ar_models=DEFAULT_AR_MODELS, runner=_truncated
        )


# ── flip detection ───────────────────────────────────────────────────


def test_flip_detection_flags_engine_specific_changes() -> None:
    """The fixture is engineered so:

    - H1 / H4 → flipped (verdict text differs between engines)
    - H2 / H3 / H5 / H6 / H7 → stable
    """
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    flipped = _flipped_hypotheses(sweep)
    assert flipped["H1"] is True
    assert flipped["H4"] is True
    assert flipped["H2"] is False
    assert flipped["H3"] is False
    assert flipped["H5"] is False
    assert flipped["H6"] is False
    assert flipped["H7"] is False


def test_flip_label_lookup() -> None:
    assert _flip_label(False) == "stable"
    assert _flip_label(True) == "flipped"


# ── normaliser helpers ───────────────────────────────────────────────


def test_normalise_ar_model_accepts_canonical_labels() -> None:
    assert _normalise_ar_model("adjusted") == AR_MODEL_ADJUSTED
    assert _normalise_ar_model("MARKET") == AR_MODEL_MARKET
    assert _normalise_ar_model(" market ") == AR_MODEL_MARKET


def test_normalise_ar_model_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        _normalise_ar_model("fama_french")


def test_normalise_ar_models_preserves_canonical_order() -> None:
    assert _normalise_ar_models(["market", "adjusted"]) == DEFAULT_AR_MODELS
    assert _normalise_ar_models(["adjusted"]) == (AR_MODEL_ADJUSTED,)


# ── render_ar_engine_forest_plot ─────────────────────────────────────


def test_render_writes_png_and_pdf(tmp_path: Path) -> None:
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "out" / "cma_verdicts_ar_engine.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_ar_engine.pdf"
    result = render_ar_engine_forest_plot(
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
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "no_pdf" / "forest.png"
    render_ar_engine_forest_plot(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )
    assert png_path.exists()
    assert not (png_path.parent / "forest.pdf").exists()


def test_render_arrow_only_when_strengths_differ(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec: the connecting arrow between the two engines is drawn only
    when their support-strength scores differ; overlapping dots get no
    arrow because it would be invisible / misleading.

    We patch ``ax.annotate`` to capture (hid → arrow drawn) calls.
    """
    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "arrows.png"

    annotate_calls: list[dict[str, object]] = []
    captured_ax: dict[str, object] = {}

    from index_inclusion_research.outputs import (
        cma_verdicts_ar_engine as engine_module,
    )

    real_subplots = engine_module.plt.subplots

    def _capture_subplots(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        fig, ax = real_subplots(*args, **kwargs)
        captured_ax["ax"] = ax
        real_annotate = ax.annotate

        def _spy_annotate(*a: object, **kw: object):  # type: ignore[no-untyped-def]
            annotate_calls.append({"args": a, "kwargs": kw})
            return real_annotate(*a, **kw)

        ax.annotate = _spy_annotate  # type: ignore[assignment]
        return fig, ax

    monkeypatch.setattr(engine_module.plt, "subplots", _capture_subplots)
    render_ar_engine_forest_plot(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )

    # H1 and H4 flip in the fixture (strength differs) → arrow drawn for
    # both. H2 / H3 / H5 / H6 / H7 share strength across engines → no
    # arrow. That gives exactly two arrow annotate() calls.
    assert len(annotate_calls) == 2, (
        f"expected 2 arrow annotations (one per flipped hypothesis), "
        f"got {len(annotate_calls)}"
    )


def test_render_legend_includes_both_engines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec: the legend must show both AR engine handles so reviewers
    can decode the figure.
    """
    import warnings

    from index_inclusion_research.outputs import (
        cma_verdicts_ar_engine as engine_module,
    )

    sweep = build_cma_ar_engine_sweep(
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "legend.png"

    captured: dict[str, object] = {}
    real_subplots = engine_module.plt.subplots

    def _capture_subplots(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        fig, ax = real_subplots(*args, **kwargs)
        captured["fig"] = fig
        captured["ax"] = ax
        return fig, ax

    monkeypatch.setattr(engine_module.plt, "subplots", _capture_subplots)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        render_ar_engine_forest_plot(
            sweep,
            output_png=png_path,
            output_pdf=None,
            generated_on=date(2026, 5, 17),
        )

    ax = captured["ax"]
    legend = ax.get_legend()  # type: ignore[attr-defined]
    assert legend is not None
    texts = [t.get_text() for t in legend.get_texts()]
    assert any("adjusted" in label for label in texts)
    assert any("market" in label for label in texts)


def test_render_raises_on_empty_sweep(tmp_path: Path) -> None:
    empty = pd.DataFrame(
        columns=[
            "ar_model",
            "hid",
            "name_cn",
            "verdict",
            "confidence",
            "evidence_tier",
            "n_obs",
            "strength",
        ]
    )
    with pytest.raises(ValueError, match="empty"):
        render_ar_engine_forest_plot(empty, output_png=tmp_path / "out.png")


def test_render_raises_on_missing_columns(tmp_path: Path) -> None:
    bad = pd.DataFrame([{"ar_model": AR_MODEL_ADJUSTED, "hid": "H1"}])
    with pytest.raises(ValueError, match="missing columns"):
        render_ar_engine_forest_plot(bad, output_png=tmp_path / "out.png")


# ── high-level wrapper ───────────────────────────────────────────────


def test_build_cma_ar_engine_forest_plot_round_trip(tmp_path: Path) -> None:
    """The convenience build_X function (sweep + render in one call)
    yields a non-empty PNG of the expected dimensions."""
    png_path = tmp_path / "round_trip.png"
    pdf_path = tmp_path / "round_trip.pdf"
    result = build_cma_ar_engine_forest_plot(
        output_png_path=png_path,
        output_pdf_path=pdf_path,
        ar_models=DEFAULT_AR_MODELS,
        runner=_fixture_runner,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800
    assert height >= 600


# ── cache helper ─────────────────────────────────────────────────────


def test_cache_csv_path_uses_ar_prefix(tmp_path: Path) -> None:
    """The CSV cache layout is documented as
    ``ar_<engine>/cma_hypothesis_verdicts.csv``.
    """
    path = _cache_csv_path(AR_MODEL_MARKET, sensitivity_root=tmp_path)
    assert path == tmp_path / "ar_market" / "cma_hypothesis_verdicts.csv"


def test_cache_metadata_path_lives_next_to_verdict_csv(tmp_path: Path) -> None:
    csv_path = _cache_csv_path(AR_MODEL_MARKET, sensitivity_root=tmp_path)
    assert _cache_metadata_path(csv_path) == (
        tmp_path / "ar_market" / "cma_ar_engine_cache_metadata.json"
    )


def test_ar_model_round_trip_from_cache_dir(tmp_path: Path) -> None:
    assert _ar_model_from_cache_dir(tmp_path / "ar_adjusted") == AR_MODEL_ADJUSTED
    assert _ar_model_from_cache_dir(tmp_path / "ar_market") == AR_MODEL_MARKET
    with pytest.raises(ValueError):
        _ar_model_from_cache_dir(tmp_path / "threshold_0_10")


def test_cache_only_sweep_reads_existing_csvs_without_orchestrator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache-only rendering must not fall through to fresh CMA runs."""
    _write_cached_verdicts(tmp_path, AR_MODEL_ADJUSTED)
    _write_cached_verdicts(tmp_path, AR_MODEL_MARKET)

    from index_inclusion_research.outputs import (
        cma_verdicts_ar_engine as engine_module,
    )

    monkeypatch.setattr(
        engine_module,
        "_cma_runner_factory",
        lambda *args, **kwargs: pytest.fail("fresh CMA runner should not be used"),
    )

    sweep = build_cma_ar_engine_sweep_from_cache(sensitivity_root=tmp_path)

    assert set(sweep["ar_model"]) == set(DEFAULT_AR_MODELS)
    assert set(sweep["hid"]) == {"H1", "H2", "H3", "H4", "H5", "H6", "H7"}


def test_cache_only_plot_renders_from_existing_csvs(tmp_path: Path) -> None:
    cache_root = tmp_path / "sensitivity"
    _write_cached_verdicts(cache_root, AR_MODEL_ADJUSTED)
    _write_cached_verdicts(cache_root, AR_MODEL_MARKET)
    png_path = tmp_path / "out" / "cma_verdicts_ar_engine.png"

    result = build_cma_ar_engine_forest_plot_from_cache(
        output_png_path=png_path,
        output_pdf_path=None,
        sensitivity_root=cache_root,
        generated_on=date(2026, 5, 17),
    )

    assert result == png_path
    assert png_path.exists()


def test_cache_only_sweep_requires_requested_engine_cache(tmp_path: Path) -> None:
    _write_cached_verdicts(tmp_path, AR_MODEL_ADJUSTED)
    with pytest.raises(FileNotFoundError, match="ar_model='market'"):
        build_cma_ar_engine_sweep_from_cache(
            ar_models=[AR_MODEL_ADJUSTED, AR_MODEL_MARKET],
            sensitivity_root=tmp_path,
        )


def test_cache_only_sweep_raises_when_no_cache_present(tmp_path: Path) -> None:
    cache_root = tmp_path / "empty_sensitivity"
    cache_root.mkdir()
    with pytest.raises(ValueError, match="no cached"):
        build_cma_ar_engine_sweep_from_cache(sensitivity_root=cache_root)


def test_default_cma_ar_engine_freshness_inputs_match_cma_pipeline() -> None:
    """The runner's default freshness inputs must mirror the inputs that
    can mutate H1-H7 verdicts. If a new CMA input ever lands, this test
    will fail unless both the threshold and AR-engine sweeps update.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    inputs = set(_default_upstream_inputs())

    assert orchestrator.REAL_EVENT_PANEL in inputs
    assert orchestrator.REAL_MATCHED_EVENT_PANEL in inputs
    assert orchestrator.REAL_EVENTS_CLEAN in inputs
    assert orchestrator.DEFAULT_PASSIVE_AUM_PATH in inputs
    assert orchestrator.DEFAULT_CN_PASSIVE_AUM_PROXY_PATH in inputs
    assert orchestrator.WEIGHT_CHANGE_PATH in inputs


def test_cma_runner_reuses_cache_when_all_inputs_are_older(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    cache_csv = _write_cached_verdicts(sensitivity_root, AR_MODEL_ADJUSTED)
    _set_mtime(cache_csv, mtime=200.0)
    upstreams = [tmp_path / f"input_{idx}.csv" for idx in range(5)]
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
    )

    result = runner(AR_MODEL_ADJUSTED)

    assert len(result) == 7


def test_cma_runner_refreshes_when_cached_threshold_metadata_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A threshold=0.05 run must not reuse threshold=0.10 verdicts.

    The AR-engine cache path is engine-scoped, so threshold safety comes
    from the metadata sidecar. This is the regression for the review
    blocker: a fresh 0.10 cache with old upstream inputs should still be
    ignored when the caller asks for 0.05.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    cache_csv = _write_cached_verdicts(
        sensitivity_root, AR_MODEL_ADJUSTED, threshold=0.10
    )
    _set_mtime(cache_csv, mtime=200.0)
    upstreams = [tmp_path / f"input_{idx}.csv" for idx in range(3)]
    for path in upstreams:
        _touch_with_mtime(path, mtime=100.0)

    calls: list[dict[str, object]] = []

    def _fake_run_cma_pipeline(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        tables_dir = Path(kwargs["tables_dir"])  # type: ignore[arg-type]
        _verdicts_at_engine(AR_MODEL_ADJUSTED).to_csv(
            tables_dir / "cma_hypothesis_verdicts.csv",
            index=False,
        )
        return {}

    monkeypatch.setattr(orchestrator, "run_cma_pipeline", _fake_run_cma_pipeline)
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=upstreams,
        threshold=0.05,
    )

    result = runner(AR_MODEL_ADJUSTED)

    assert calls, "threshold metadata mismatch should invalidate the cache"
    assert not result.empty
    assert calls[0]["significance_level"] == pytest.approx(0.05)


def test_cma_runner_refreshes_when_upstream_is_newer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If any CMA input mtime is newer than the cached verdict CSV the
    runner must re-fire the orchestrator and rewrite the cache.

    For ar_model=adjusted we exercise the no-panel-rewrite path. The
    market-engine specifics (panel materialisation) are covered in a
    separate test below to keep this assertion focused.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    cache_csv = _write_cached_verdicts(sensitivity_root, AR_MODEL_ADJUSTED)
    _set_mtime(cache_csv, mtime=100.0)
    upstreams = [tmp_path / f"input_{idx}.csv" for idx in range(3)]
    for path in upstreams:
        _touch_with_mtime(path, mtime=200.0)  # newer than the cache

    calls: list[dict[str, object]] = []

    def _fake_run_cma_pipeline(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        tables_dir = Path(kwargs["tables_dir"])  # type: ignore[arg-type]
        _verdicts_at_engine(AR_MODEL_ADJUSTED).to_csv(
            tables_dir / "cma_hypothesis_verdicts.csv",
            index=False,
        )
        return {}

    monkeypatch.setattr(orchestrator, "run_cma_pipeline", _fake_run_cma_pipeline)
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=upstreams,
        threshold=DEFAULT_AR_ENGINE_THRESHOLD,
    )

    result = runner(AR_MODEL_ADJUSTED)

    assert calls, "newer upstream input should invalidate the cache"
    assert not result.empty
    assert calls[0]["significance_level"] == pytest.approx(
        DEFAULT_AR_ENGINE_THRESHOLD
    )
    # For ar_model=adjusted we do NOT rewrite the panel — orchestrator
    # gets called with default panel paths so the simple ar column is
    # consumed verbatim.
    assert "event_panel_path" not in calls[0]


def test_cma_runner_market_engine_materialises_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When ar_model='market', the runner must materialise a panel
    whose ``ar`` column equals ar_market_model and thread it through
    the orchestrator's event_panel_path override.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator
    from index_inclusion_research.outputs import (
        cma_verdicts_ar_engine as engine_module,
    )

    sensitivity_root = tmp_path / "sensitivity"
    # Stale cache CSV so the runner falls through to the orchestrator.
    cache_csv = _cache_csv_path(
        AR_MODEL_MARKET, sensitivity_root=sensitivity_root
    )
    cache_csv.parent.mkdir(parents=True, exist_ok=True)
    _verdicts_at_engine(AR_MODEL_MARKET).to_csv(cache_csv, index=False)
    _set_mtime(cache_csv, mtime=100.0)
    fake_source_panel = tmp_path / "real_event_panel.csv"
    _touch_with_mtime(fake_source_panel, mtime=200.0)  # newer than cache

    materialised_paths: list[tuple[Path, Path]] = []

    def _fake_materialise(
        source_panel_path: Path,
        *,
        output_panel_path: Path,
        estimation_window: tuple[int, int] = (-120, -10),
    ) -> Path:
        materialised_paths.append((source_panel_path, output_panel_path))
        output_panel_path.parent.mkdir(parents=True, exist_ok=True)
        output_panel_path.write_bytes(b"event_id,ar\n")
        return output_panel_path

    monkeypatch.setattr(
        engine_module, "_materialise_market_model_panel", _fake_materialise
    )

    calls: list[dict[str, object]] = []

    def _fake_run_cma_pipeline(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        tables_dir = Path(kwargs["tables_dir"])  # type: ignore[arg-type]
        _verdicts_at_engine(AR_MODEL_MARKET).to_csv(
            tables_dir / "cma_hypothesis_verdicts.csv",
            index=False,
        )
        return {}

    monkeypatch.setattr(orchestrator, "run_cma_pipeline", _fake_run_cma_pipeline)
    monkeypatch.setattr(orchestrator, "REAL_EVENT_PANEL", fake_source_panel)
    monkeypatch.setattr(
        orchestrator,
        "REAL_MATCHED_EVENT_PANEL",
        tmp_path / "matched_panel_missing.csv",
    )
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=[fake_source_panel],
        threshold=DEFAULT_AR_ENGINE_THRESHOLD,
    )

    result = runner(AR_MODEL_MARKET)

    assert calls, "market-engine cache miss should call the orchestrator"
    assert materialised_paths, "market engine must rewrite the panel"
    # The event_panel_path kwarg must point at the materialised panel,
    # not the original real_event_panel.csv.
    threaded_panel = calls[0].get("event_panel_path")
    assert threaded_panel == materialised_paths[0][1]
    assert "matched_panel_path" not in calls[0], (
        "matched panel override should only fire when the source exists"
    )
    assert not result.empty


# ── doctor integration ───────────────────────────────────────────────


def test_doctor_check_passes_when_cache_empty(tmp_path: Path) -> None:
    """No sensitivity cache → opt-in pass, no warn."""
    result = check_cma_ar_engine_forest_artifact(
        png_path=tmp_path / "absent.png",
        pdf_path=tmp_path / "absent.pdf",
        sensitivity_root=tmp_path / "sensitivity",
    )
    assert result.status == "pass"
    assert "opt-in" in result.message


def test_doctor_check_passes_when_cache_dir_exists_but_empty(
    tmp_path: Path,
) -> None:
    sens_root = tmp_path / "sensitivity"
    sens_root.mkdir()
    result = check_cma_ar_engine_forest_artifact(
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
        sens_root / "ar_adjusted" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_000.0,
    )
    result = check_cma_ar_engine_forest_artifact(
        png_path=tmp_path / "missing.png",
        pdf_path=tmp_path / "missing.pdf",
        sensitivity_root=sens_root,
    )
    assert result.status == "warn"
    assert "missing" in result.message
    assert "cma-ar-engine-forest" in result.fix


def test_doctor_check_warns_when_artifact_stale(tmp_path: Path) -> None:
    sens_root = tmp_path / "sensitivity"
    _touch_with_mtime(
        sens_root / "ar_market" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_200.0,
    )
    png = tmp_path / "engine.png"
    pdf = tmp_path / "engine.pdf"
    _touch_with_mtime(png, mtime=1_700_000_100.0)  # older than cache CSV
    _touch_with_mtime(pdf, mtime=1_700_000_100.0)
    result = check_cma_ar_engine_forest_artifact(
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
        sens_root / "ar_adjusted" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_000.0,
    )
    _touch_with_mtime(
        sens_root / "ar_market" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_050.0,
    )
    png = tmp_path / "engine.png"
    pdf = tmp_path / "engine.pdf"
    _touch_with_mtime(png, mtime=1_700_000_500.0)
    _touch_with_mtime(pdf, mtime=1_700_000_500.0)
    result = check_cma_ar_engine_forest_artifact(
        png_path=png,
        pdf_path=pdf,
        sensitivity_root=sens_root,
    )
    assert result.status == "pass"
    assert "cached AR-engine CSV" in result.message
