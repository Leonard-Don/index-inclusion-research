"""Tests for the sensitivity-aware CMA verdicts forest plot.

The threshold sweep itself is mocked with a deterministic fixture
runner — running the full CMA orchestrator four times in tests would
balloon the suite. Doctor / paper_bundle integration is covered by
the existing test files (we only extend their fixture surface here).
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

from index_inclusion_research.doctor import (
    check_cma_sensitivity_forest_artifact,
)
from index_inclusion_research.outputs import (
    DEFAULT_SENSITIVITY_THRESHOLDS,
    build_cma_sensitivity_forest_plot,
    build_cma_sensitivity_forest_plot_from_cache,
    build_cma_sensitivity_sweep,
    build_cma_sensitivity_sweep_from_cache,
    render_sensitivity_forest_plot,
)
from index_inclusion_research.outputs.cma_verdicts_sensitivity import (
    _cache_csv_path,
    _cma_runner_factory,
    _count_flips_per_hypothesis,
    _default_upstream_inputs,
    _flip_label,
    _normalise_threshold_label,
)

# ── fixtures ─────────────────────────────────────────────────────────


def _verdicts_at_threshold(threshold: float) -> pd.DataFrame:
    """Return a 7-row verdicts panel whose verdicts shift with threshold.

    Designed so the test suite exercises every meaningful sweep regime
    without ever calling the real CMA pipeline:

    - H1 / H4 / H5 are the p-gated rows and may flip as the threshold
      loosens.
    - H2 / H3 / H6 / H7 are non-p headline rows in the current pipeline
      and deliberately stay stable across this fixture.
    """
    # Map: threshold → (verdict, confidence) per hypothesis.
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

    # H1 — p=0.08: insufficient at 0.05, partial at 0.10/0.15,
    # support at 0.20 because the inner cutoff is threshold/2.
    if threshold <= 0.05:
        add("H1", "信息泄露与预运行", "证据不足", "中", "core", 436, 0.08)
    elif threshold < 0.20:
        add("H1", "信息泄露与预运行", "部分支持", "中", "core", 436, 0.08)
    else:
        add("H1", "信息泄露与预运行", "支持", "高", "core", 436, 0.08)
    # H2 — non-p directional AUM trend; threshold knob should not move it.
    add("H2", "被动基金 AUM 差异", "部分支持", "中", "core", 17)
    # H3 — non-p channel-hit row in the current cached output.
    add("H3", "散户 vs 机构结构", "支持", "高", "supplementary", 4)
    # H4 — p=0.12: moves from insufficient to partial once threshold >=0.15.
    if threshold < 0.15:
        add("H4", "卖空约束", "证据不足", "中", "supplementary", 40, 0.12)
    else:
        add("H4", "卖空约束", "部分支持", "中", "supplementary", 40, 0.12)
    # H5 — p=0.03: partial at 0.05, support at 0.10+.
    if threshold <= 0.05:
        add("H5", "涨跌停限制", "部分支持", "中", "core", 936, 0.03)
    else:
        add("H5", "涨跌停限制", "支持", "高", "core", 936, 0.03)
    # H6 / H7 — non-p headline rows in this figure.
    add("H6", "指数权重可预测性", "证据不足", "中", "supplementary", 67)
    add("H7", "行业结构差异", "支持", "中", "core", 187)

    return pd.DataFrame(rows)


def _fixture_runner(threshold: float) -> pd.DataFrame:
    """Deterministic in-process runner — no orchestrator invocation."""
    return _verdicts_at_threshold(threshold)


def _write_cached_verdicts(sensitivity_root: Path, threshold: float) -> Path:
    csv_path = _cache_csv_path(threshold, sensitivity_root=sensitivity_root)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    _verdicts_at_threshold(threshold).to_csv(csv_path, index=False)
    return csv_path


def _set_mtime(path: Path, *, mtime: float) -> None:
    os.utime(path, (mtime, mtime))


# ── build_cma_sensitivity_sweep ──────────────────────────────────────


def test_sweep_dot_count_is_7x4() -> None:
    """Spec: H1-H7 × 4 thresholds = 28 dots in the long-format frame."""
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    assert len(sweep) == 7 * 4, (
        f"expected 28 rows (7 hypotheses × 4 thresholds), got {len(sweep)}"
    )
    # All H1-H7 present at every threshold
    for threshold in DEFAULT_SENSITIVITY_THRESHOLDS:
        per_t = sweep.loc[sweep["threshold"] == threshold]
        assert set(per_t["hid"]) == {"H1", "H2", "H3", "H4", "H5", "H6", "H7"}, (
            f"threshold={threshold} missing hypotheses"
        )


def test_sweep_columns_match_contract() -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    required = {
        "threshold",
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


def test_sweep_strength_score_matches_verdict_at_each_threshold() -> None:
    """The sweep should carry the support-strength derived from
    (verdict, confidence) at *each* threshold — not the snapshot value.
    """
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    # H1 at 0.05 must be "证据不足/中" → strength 0.3
    h1_strict = sweep.loc[
        (sweep["hid"] == "H1") & (sweep["threshold"] == 0.05)
    ].iloc[0]
    assert h1_strict["verdict"] == "证据不足"
    assert h1_strict["strength"] == pytest.approx(0.3)
    # H1 at 0.20 must be "支持/高" → 1.0
    h1_loose = sweep.loc[
        (sweep["hid"] == "H1") & (sweep["threshold"] == 0.20)
    ].iloc[0]
    assert h1_loose["verdict"] == "支持"
    assert h1_loose["strength"] == pytest.approx(1.0)


def test_sweep_de_duplicates_and_sorts_thresholds() -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=[0.20, 0.10, 0.10, 0.05, 0.15],
        runner=_fixture_runner,
    )
    distinct = sorted(sweep["threshold"].unique().tolist())
    assert distinct == [0.05, 0.10, 0.15, 0.20]


def test_sweep_raises_when_thresholds_empty() -> None:
    with pytest.raises(ValueError, match="at least one"):
        build_cma_sensitivity_sweep(thresholds=[], runner=_fixture_runner)


def test_sweep_rejects_thresholds_that_would_collide_in_cache_label() -> None:
    with pytest.raises(ValueError, match="two decimal"):
        build_cma_sensitivity_sweep(thresholds=[0.105], runner=_fixture_runner)


def test_sweep_raises_when_runner_drops_hypothesis() -> None:
    def _broken(threshold: float) -> pd.DataFrame:
        # Returns only 6/7 hypotheses (drop H3)
        df = _verdicts_at_threshold(threshold)
        return df.loc[df["hid"] != "H3"].reset_index(drop=True)

    with pytest.raises(ValueError, match="H3"):
        build_cma_sensitivity_sweep(
            thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_broken
        )


def test_sweep_raises_when_runner_missing_columns() -> None:
    def _truncated(threshold: float) -> pd.DataFrame:
        df = _verdicts_at_threshold(threshold).drop(columns=["evidence_tier"])
        return df

    with pytest.raises(ValueError, match="evidence_tier"):
        build_cma_sensitivity_sweep(
            thresholds=DEFAULT_SENSITIVITY_THRESHOLDS, runner=_truncated
        )


# ── flip detection ───────────────────────────────────────────────────


def test_flip_detection_classifies_verdict_changes() -> None:
    """The fixture is engineered so:

    - H2, H3, H6, H7 → 0 flips (non-p headline rows)
    - H4 / H5 → 1 flip
    - H1 → 2 flips (证据不足→部分支持→支持)
    """
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    flips = _count_flips_per_hypothesis(sweep)
    assert flips["H1"] == 2
    assert flips["H2"] == 0
    assert flips["H3"] == 0
    assert flips["H4"] == 1
    assert flips["H5"] == 1
    assert flips["H6"] == 0
    assert flips["H7"] == 0


def test_flip_label_lookup() -> None:
    assert _flip_label(0) == "stable"
    assert _flip_label(1) == "1 flip"
    assert _flip_label(2) == "2+ flips"
    assert _flip_label(5) == "2+ flips"


# ── render_sensitivity_forest_plot ───────────────────────────────────


def test_render_writes_png_and_pdf(tmp_path: Path) -> None:
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "out" / "cma_verdicts_sensitivity.png"
    pdf_path = tmp_path / "out" / "cma_verdicts_sensitivity.pdf"
    result = render_sensitivity_forest_plot(
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
    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "no_pdf" / "forest.png"
    render_sensitivity_forest_plot(
        sweep,
        output_png=png_path,
        output_pdf=None,
        generated_on=date(2026, 5, 17),
    )
    assert png_path.exists()
    assert not (png_path.parent / "forest.pdf").exists()


def test_render_legend_includes_both_shapes(tmp_path: Path) -> None:
    """Spec: the legend must show both circle (stable) and triangle
    (flipped) handles so reviewers can decode the figure.

    We assert by sniffing the in-memory legend right after the figure
    is rendered, via a custom build that captures the figure object.
    """
    import warnings

    from index_inclusion_research.outputs import (
        cma_verdicts_sensitivity as sens_module,
    )

    sweep = build_cma_sensitivity_sweep(
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
    )
    png_path = tmp_path / "legend.png"

    # Wrap savefig so we can grab the figure before close().
    captured: dict[str, object] = {}
    real_subplots = sens_module.plt.subplots

    def _capture_subplots(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        fig, ax = real_subplots(*args, **kwargs)
        captured["fig"] = fig
        captured["ax"] = ax
        return fig, ax

    sens_module.plt.subplots = _capture_subplots  # type: ignore[assignment]
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            render_sensitivity_forest_plot(
                sweep,
                output_png=png_path,
                output_pdf=None,
                generated_on=date(2026, 5, 17),
            )
    finally:
        sens_module.plt.subplots = real_subplots  # type: ignore[assignment]

    ax = captured["ax"]
    legend = ax.get_legend()  # type: ignore[attr-defined]
    assert legend is not None
    texts = {t.get_text() for t in legend.get_texts()}
    assert "stable verdict" in texts
    assert "flipped at threshold" in texts


def test_render_raises_on_empty_sweep(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=[
        "threshold", "hid", "name_cn", "verdict", "confidence",
        "evidence_tier", "n_obs", "strength",
    ])
    with pytest.raises(ValueError, match="empty"):
        render_sensitivity_forest_plot(
            empty, output_png=tmp_path / "out.png"
        )


def test_render_raises_on_missing_columns(tmp_path: Path) -> None:
    bad = pd.DataFrame(
        [{"threshold": 0.05, "hid": "H1"}]
    )
    with pytest.raises(ValueError, match="missing columns"):
        render_sensitivity_forest_plot(
            bad, output_png=tmp_path / "out.png"
        )


# ── high-level wrapper ───────────────────────────────────────────────


def test_build_cma_sensitivity_forest_plot_round_trip(tmp_path: Path) -> None:
    """The convenience build_X function (sweep + render in one call)
    yields a non-empty PNG of the expected dimensions."""
    png_path = tmp_path / "round_trip.png"
    pdf_path = tmp_path / "round_trip.pdf"
    result = build_cma_sensitivity_forest_plot(
        output_png_path=png_path,
        output_pdf_path=pdf_path,
        thresholds=DEFAULT_SENSITIVITY_THRESHOLDS,
        runner=_fixture_runner,
        generated_on=date(2026, 5, 17),
    )
    assert result == png_path
    with Image.open(png_path) as img:
        width, height = img.size
    assert width >= 800
    assert height >= 600


# ── cache helper ─────────────────────────────────────────────────────


def test_cache_csv_path_replaces_dot_with_underscore(tmp_path: Path) -> None:
    """The CSV cache layout is documented as
    ``threshold_<T>/cma_hypothesis_verdicts.csv`` with the dot replaced
    by an underscore — so a literal `.` doesn't show up on macOS Finder.
    """
    path = _cache_csv_path(0.10, sensitivity_root=tmp_path)
    assert path == tmp_path / "threshold_0_10" / "cma_hypothesis_verdicts.csv"


def test_cache_label_rejects_more_than_two_decimal_places() -> None:
    with pytest.raises(ValueError, match="two decimal"):
        _normalise_threshold_label(0.104)


def test_cache_only_sweep_reads_existing_csvs_without_orchestrator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cache-only rendering must not fall through to fresh CMA runs."""
    _write_cached_verdicts(tmp_path, 0.05)
    _write_cached_verdicts(tmp_path, 0.10)

    from index_inclusion_research.outputs import (
        cma_verdicts_sensitivity as sens_module,
    )

    monkeypatch.setattr(
        sens_module,
        "_cma_runner_factory",
        lambda *args, **kwargs: pytest.fail("fresh CMA runner should not be used"),
    )

    sweep = build_cma_sensitivity_sweep_from_cache(sensitivity_root=tmp_path)

    assert sorted(sweep["threshold"].unique().tolist()) == [0.05, 0.10]
    assert set(sweep["hid"]) == {"H1", "H2", "H3", "H4", "H5", "H6", "H7"}


def test_cache_only_plot_renders_from_existing_csvs(tmp_path: Path) -> None:
    cache_root = tmp_path / "sensitivity"
    _write_cached_verdicts(cache_root, 0.05)
    _write_cached_verdicts(cache_root, 0.10)
    png_path = tmp_path / "out" / "cma_verdicts_sensitivity.png"

    result = build_cma_sensitivity_forest_plot_from_cache(
        output_png_path=png_path,
        output_pdf_path=None,
        sensitivity_root=cache_root,
        generated_on=date(2026, 5, 17),
    )

    assert result == png_path
    assert png_path.exists()


def test_cache_only_sweep_requires_requested_threshold_cache(tmp_path: Path) -> None:
    _write_cached_verdicts(tmp_path, 0.05)
    with pytest.raises(FileNotFoundError, match="threshold=0.10"):
        build_cma_sensitivity_sweep_from_cache(
            thresholds=[0.05, 0.10],
            sensitivity_root=tmp_path,
        )


def test_default_cma_sensitivity_freshness_inputs_include_h2_h6_sources() -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    inputs = set(_default_upstream_inputs())

    assert orchestrator.REAL_EVENT_PANEL in inputs
    assert orchestrator.REAL_MATCHED_EVENT_PANEL in inputs
    assert orchestrator.REAL_EVENTS_CLEAN in inputs
    assert orchestrator.DEFAULT_PASSIVE_AUM_PATH in inputs
    assert orchestrator.DEFAULT_CN_PASSIVE_AUM_PROXY_PATH in inputs
    assert orchestrator.WEIGHT_CHANGE_PATH in inputs


def test_cma_runner_refreshes_when_passive_or_weight_input_is_newer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    cache_csv = _write_cached_verdicts(sensitivity_root, 0.10)
    _set_mtime(cache_csv, mtime=200.0)
    event_panel = tmp_path / "real_event_panel.csv"
    matched_panel = tmp_path / "real_matched_event_panel.csv"
    events_clean = tmp_path / "real_events_clean.csv"
    passive_aum = tmp_path / "passive_aum.csv"
    weight_change = tmp_path / "hs300_weight_change.csv"
    for path, mtime in (
        (event_panel, 100.0),
        (matched_panel, 100.0),
        (events_clean, 100.0),
        (passive_aum, 250.0),
        (weight_change, 100.0),
    ):
        _touch_with_mtime(path, mtime=mtime)

    calls: list[dict[str, object]] = []

    def _fake_run_cma_pipeline(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        tables_dir = Path(kwargs["tables_dir"])  # type: ignore[arg-type]
        _verdicts_at_threshold(float(kwargs["significance_level"])).to_csv(
            tables_dir / "cma_hypothesis_verdicts.csv",
            index=False,
        )
        return {}

    monkeypatch.setattr(orchestrator, "run_cma_pipeline", _fake_run_cma_pipeline)
    runner = _cma_runner_factory(
        sensitivity_root=sensitivity_root,
        upstream_inputs=[
            event_panel,
            matched_panel,
            events_clean,
            passive_aum,
            weight_change,
        ],
    )

    result = runner(0.10)

    assert calls, "newer passive/H6 input should invalidate the cache"
    assert not result.empty
    assert calls[0]["significance_level"] == 0.10


def test_cma_runner_reuses_cache_when_all_cma_inputs_are_older(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    sensitivity_root = tmp_path / "sensitivity"
    cache_csv = _write_cached_verdicts(sensitivity_root, 0.10)
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

    result = runner(0.10)

    assert len(result) == 7


# ── doctor integration ───────────────────────────────────────────────


def _touch_with_mtime(path: Path, *, mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    os.utime(path, (mtime, mtime))


def test_doctor_check_passes_when_cache_empty(tmp_path: Path) -> None:
    """No sensitivity cache → opt-in pass, no warn."""
    result = check_cma_sensitivity_forest_artifact(
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
    result = check_cma_sensitivity_forest_artifact(
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
        sens_root / "threshold_0_10" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_000.0,
    )
    result = check_cma_sensitivity_forest_artifact(
        png_path=tmp_path / "missing.png",
        pdf_path=tmp_path / "missing.pdf",
        sensitivity_root=sens_root,
    )
    assert result.status == "warn"
    assert "missing" in result.message
    assert "cma-sensitivity-forest" in result.fix


def test_doctor_check_warns_when_artifact_stale(tmp_path: Path) -> None:
    sens_root = tmp_path / "sensitivity"
    _touch_with_mtime(
        sens_root / "threshold_0_10" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_200.0,
    )
    png = tmp_path / "sens.png"
    pdf = tmp_path / "sens.pdf"
    _touch_with_mtime(png, mtime=1_700_000_100.0)  # older than cache CSV
    _touch_with_mtime(pdf, mtime=1_700_000_100.0)
    result = check_cma_sensitivity_forest_artifact(
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
        sens_root / "threshold_0_10" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_000.0,
    )
    _touch_with_mtime(
        sens_root / "threshold_0_05" / "cma_hypothesis_verdicts.csv",
        mtime=1_700_000_050.0,
    )
    png = tmp_path / "sens.png"
    pdf = tmp_path / "sens.pdf"
    _touch_with_mtime(png, mtime=1_700_000_500.0)
    _touch_with_mtime(pdf, mtime=1_700_000_500.0)
    result = check_cma_sensitivity_forest_artifact(
        png_path=png,
        pdf_path=pdf,
        sensitivity_root=sens_root,
    )
    assert result.status == "pass"
    assert "cached threshold CSV" in result.message
