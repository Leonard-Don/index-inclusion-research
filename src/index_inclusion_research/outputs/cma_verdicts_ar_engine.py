"""AR-engine-aware forest plot of CMA hypothesis verdicts.

Parallel to :mod:`cma_verdicts_sensitivity` (commit 87d624c), this module
sweeps the CMA pipeline across the two AR engines exposed by
``--ar-model`` (commit 1e29476): ``adjusted`` (the literature-standard
``ret − benchmark_ret``) and ``market`` (the market-model β-AR with
estimation window ``(-120, -10)``).

The threshold sensitivity sweep answered "would the verdicts change
under a different p-value threshold?". This sweep answers a different
methodological question: "would the verdicts change if the abnormal
return itself were defined via a market-model β rather than a simple
benchmark subtraction?". A reviewer can object to either; the project
ships both robustness panels so the answer is visible at a glance.

The figure is intentionally simpler than the threshold variant — 2 dots
per hypothesis instead of 4 — but the underlying methodological loading
is heavier: switching from ``adjusted`` to ``market`` re-runs the entire
CMA pipeline against a panel whose ``ar`` column is the β-AR residual.
H1-H7 verdicts at each engine come from the same orchestrator code path,
so a flip between engines is an honest "this verdict depends on AR
model choice", not a code-path artefact.

Cache layout: ``results/sensitivity/ar_<engine>/cma_hypothesis_verdicts.csv``.
Sibling to ``threshold_<T>/`` so a future doctor / paper-bundle sweep
that aggregates all sensitivity sub-directories can pick up both.

Two rendering modes mirror the threshold module:

- :func:`build_cma_ar_engine_forest_plot` is the explicit sweep entry
  and may call the CMA orchestrator when the cache is stale.
- :func:`build_cma_ar_engine_forest_plot_from_cache` is cache-only and
  is used by ``make-figures-tables`` / ``paper_bundle`` so those re-
  renderers never trigger fresh CMA passes.
"""

from __future__ import annotations

import json
import logging
import math
import warnings
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from index_inclusion_research import paths
from index_inclusion_research.outputs.cma_verdicts_forest import (
    _HYPOTHESIS_ORDER,
    _VERDICT_ASCII,
    classify_strength,
)

logger = logging.getLogger(__name__)

plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


# Canonical AR engine identifiers. ``adjusted`` is the literature-standard
# simple ``ret − benchmark_ret`` (default of the project) and ``market``
# is the market-model β-AR engine exposed by
# ``run-event-study --ar-model market`` (commit 1e29476). Order is
# deliberate: the default engine renders on the left of the forest plot.
AR_MODEL_ADJUSTED = "adjusted"
AR_MODEL_MARKET = "market"
DEFAULT_AR_MODELS: tuple[str, ...] = (AR_MODEL_ADJUSTED, AR_MODEL_MARKET)
SUPPORTED_AR_MODELS: frozenset[str] = frozenset(DEFAULT_AR_MODELS)

# Default threshold reused for both engine passes. Held constant on
# purpose: we want a single-axis comparison between the two AR engines,
# not a 2-D engine × threshold grid (that would dilute the headline).
DEFAULT_AR_ENGINE_THRESHOLD: float = 0.10

# Market-model estimation window: matches the
# ``run-event-study --ar-model market --estimation-window 120,10``
# default (commit 1e29476). The negative signs are added internally — the
# CMA pipeline reads positive endpoints to stay readable in CLI / docs.
DEFAULT_MARKET_MODEL_ESTIMATION_WINDOW: tuple[int, int] = (-120, -10)
CACHE_METADATA_FILENAME = "cma_ar_engine_cache_metadata.json"
CACHE_METADATA_SCHEMA_VERSION = 1

# Per-engine palette + marker. Teal for the literature-standard
# "adjusted" engine and a saturated purple for the "market" β-AR engine
# — distinct hues so a reviewer can spot at a glance which dot belongs
# to which engine, with the shape providing redundant decoding for
# greyscale prints.
_ENGINE_COLORS: dict[str, str] = {
    AR_MODEL_ADJUSTED: "#0f6e63",
    AR_MODEL_MARKET: "#6a3aa1",
}
_ENGINE_MARKERS: dict[str, str] = {
    AR_MODEL_ADJUSTED: "o",
    AR_MODEL_MARKET: "s",
}
_ENGINE_LABELS: dict[str, str] = {
    AR_MODEL_ADJUSTED: "adjusted (ret − benchmark)",
    AR_MODEL_MARKET: "market-model β (-120,-10)",
}


# Pipeline callable signature: takes an engine label, returns a
# DataFrame matching the ``cma_hypothesis_verdicts.csv`` schema. Tests
# inject a fixture-based mock; the default implementation re-runs the
# CMA orchestrator in-process and caches the CSV under
# ``results/sensitivity/ar_<engine>/``.
PipelineRunner = Callable[[str], pd.DataFrame]

REQUIRED_VERDICT_COLUMNS: tuple[str, ...] = (
    "hid",
    "verdict",
    "confidence",
    "evidence_tier",
    "n_obs",
)

SWEEP_OUTPUT_COLUMNS: tuple[str, ...] = (
    "ar_model",
    "hid",
    "name_cn",
    "verdict",
    "confidence",
    "evidence_tier",
    "n_obs",
    "strength",
)


def _normalise_ar_model(ar_model: str) -> str:
    """Return a canonical AR engine label or raise."""
    if not isinstance(ar_model, str):
        raise TypeError(f"ar_model must be a string, got {type(ar_model)!r}")
    value = ar_model.strip().lower()
    if value not in SUPPORTED_AR_MODELS:
        raise ValueError(
            f"unsupported ar_model {ar_model!r}; expected one of "
            f"{sorted(SUPPORTED_AR_MODELS)}"
        )
    return value


def _normalise_ar_models(ar_models: Sequence[str]) -> tuple[str, ...]:
    if not ar_models:
        raise ValueError("ar_models must contain at least one value")
    seen: list[str] = []
    for raw in ar_models:
        normalised = _normalise_ar_model(raw)
        if normalised not in seen:
            seen.append(normalised)
    # Preserve canonical order (adjusted before market) for stable plots
    # regardless of caller order.
    return tuple(model for model in DEFAULT_AR_MODELS if model in seen)


def _normalise_threshold(threshold: float) -> float:
    value = float(threshold)
    if not math.isfinite(value):
        raise ValueError("threshold must be a finite float")
    if value <= 0 or value > 1:
        raise ValueError("threshold must be in the interval (0, 1]")
    return value


def _cache_csv_path(ar_model: str, *, sensitivity_root: Path | None = None) -> Path:
    """Return ``results/sensitivity/ar_<engine>/cma_hypothesis_verdicts.csv``."""
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    return root / f"ar_{_normalise_ar_model(ar_model)}" / "cma_hypothesis_verdicts.csv"


def _cache_metadata_path(cache_csv: Path) -> Path:
    return cache_csv.with_name(CACHE_METADATA_FILENAME)


def _cache_metadata_matches(
    cache_csv: Path, *, ar_model: str, threshold: float
) -> bool:
    meta_path = _cache_metadata_path(cache_csv)
    if not meta_path.exists():
        return False
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        payload_model = _normalise_ar_model(str(payload.get("ar_model", "")))
        payload_threshold = float(payload["threshold"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return payload_model == ar_model and math.isclose(
        payload_threshold,
        threshold,
        rel_tol=0.0,
        abs_tol=1e-12,
    )


def _write_cache_metadata(
    cache_csv: Path, *, ar_model: str, threshold: float
) -> None:
    payload = {
        "schema_version": CACHE_METADATA_SCHEMA_VERSION,
        "ar_model": _normalise_ar_model(ar_model),
        "threshold": _normalise_threshold(threshold),
    }
    _cache_metadata_path(cache_csv).write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _ar_model_from_cache_dir(cache_dir: Path) -> str:
    label = cache_dir.name
    prefix = "ar_"
    if not label.startswith(prefix):
        raise ValueError(f"invalid CMA AR-engine cache directory: {cache_dir}")
    return _normalise_ar_model(label[len(prefix):])


def _discover_cached_ar_models(
    *, sensitivity_root: Path | None = None
) -> tuple[str, ...]:
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    if not root.exists():
        return ()
    discovered: set[str] = set()
    for csv_path in sorted(root.glob("ar_*/cma_hypothesis_verdicts.csv")):
        try:
            discovered.add(_ar_model_from_cache_dir(csv_path.parent))
        except ValueError:
            continue
    return tuple(model for model in DEFAULT_AR_MODELS if model in discovered)


def _cache_only_runner_factory(
    *, sensitivity_root: Path | None = None
) -> PipelineRunner:
    def _run(ar_model: str) -> pd.DataFrame:
        cache_csv = _cache_csv_path(ar_model, sensitivity_root=sensitivity_root)
        if not cache_csv.exists():
            raise FileNotFoundError(
                f"cached CMA verdicts not found for ar_model={ar_model!r}: "
                f"{cache_csv}"
            )
        return pd.read_csv(cache_csv)

    return _run


def _default_upstream_inputs() -> tuple[Path, ...]:
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    return (
        orchestrator.REAL_EVENT_PANEL,
        orchestrator.REAL_MATCHED_EVENT_PANEL,
        orchestrator.REAL_EVENTS_CLEAN,
        orchestrator.DEFAULT_PASSIVE_AUM_PATH,
        orchestrator.DEFAULT_CN_PASSIVE_AUM_PROXY_PATH,
        orchestrator.WEIGHT_CHANGE_PATH,
    )


def _materialise_market_model_panel(
    source_panel_path: Path,
    *,
    output_panel_path: Path,
    estimation_window: tuple[int, int] = DEFAULT_MARKET_MODEL_ESTIMATION_WINDOW,
) -> Path:
    """Build a CMA-compatible panel whose ``ar`` column is market-model β-AR.

    The CMA orchestrator reads the ``ar`` column verbatim. To re-run the
    pipeline with the market-model engine we materialise a copy of the
    source panel where ``ar`` has been replaced with the per-row output
    of :func:`compute_market_model_abnormal_returns`. Rows whose β could
    not be estimated (thin estimation window, degenerate benchmark
    variance) retain NaN in ``ar`` so they drop out of CAR aggregations,
    matching the existing behaviour of the simple-AR engine when one of
    its inputs is NaN.
    """
    from index_inclusion_research.analysis.event_study import (
        compute_market_model_abnormal_returns,
    )

    panel = pd.read_csv(source_panel_path, low_memory=False)
    enriched = compute_market_model_abnormal_returns(
        panel,
        estimation_window=estimation_window,
    )
    if "ar_market_model" not in enriched.columns:
        raise ValueError(
            "compute_market_model_abnormal_returns did not populate "
            "ar_market_model; cannot materialise market-engine panel."
        )
    enriched["ar"] = enriched["ar_market_model"]
    output_panel_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(output_panel_path, index=False)
    return output_panel_path


def _cma_runner_factory(
    *,
    sensitivity_root: Path | None = None,
    upstream_inputs: Sequence[Path] | None = None,
    threshold: float = DEFAULT_AR_ENGINE_THRESHOLD,
    estimation_window: tuple[int, int] = DEFAULT_MARKET_MODEL_ESTIMATION_WINDOW,
) -> PipelineRunner:
    """Return a runner that calls the real CMA orchestrator per engine.

    The runner caches the verdicts CSV under
    ``results/sensitivity/ar_<engine>/`` and writes a metadata sidecar
    containing the threshold. It reuses an existing cache only when the
    sidecar threshold matches and the CSV mtime is newer than every file
    in ``upstream_inputs``.
    The threshold is held constant across the engine sweep (default
    0.10) so the headline difference between dots is *purely* attributable
    to the AR engine choice, not a confounded threshold knob.

    For ``ar_model='market'`` the runner first materialises a panel whose
    ``ar`` column equals the market-model β-AR (see
    :func:`_materialise_market_model_panel`), writes it into the cache
    directory, and threads the new panel path through the orchestrator's
    ``event_panel_path`` / ``matched_panel_path`` overrides.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    if upstream_inputs is None:
        upstream_inputs = _default_upstream_inputs()

    upstream_paths = tuple(Path(p) for p in upstream_inputs)
    threshold = _normalise_threshold(threshold)
    sensitivity_root_resolved = sensitivity_root or (paths.results_dir() / "sensitivity")

    def _run(ar_model: str) -> pd.DataFrame:
        ar_model = _normalise_ar_model(ar_model)
        cache_csv = _cache_csv_path(ar_model, sensitivity_root=sensitivity_root_resolved)
        cache_csv.parent.mkdir(parents=True, exist_ok=True)
        if cache_csv.exists():
            if _cache_metadata_matches(
                cache_csv, ar_model=ar_model, threshold=threshold
            ):
                cache_mtime = cache_csv.stat().st_mtime
                upstream_mtimes = [
                    p.stat().st_mtime for p in upstream_paths if p.exists()
                ]
                if upstream_mtimes and cache_mtime >= max(upstream_mtimes):
                    logger.info(
                        "reusing cached CMA verdicts at ar_model=%s threshold=%.2f: %s",
                        ar_model,
                        threshold,
                        cache_csv,
                    )
                    return pd.read_csv(cache_csv)
                if not upstream_mtimes:
                    # Upstream inputs absent → trust only threshold-matched
                    # metadata rather than reusing a cache from another gate.
                    logger.info(
                        "no upstream inputs found; using cached CMA verdicts "
                        "at ar_model=%s threshold=%.2f",
                        ar_model,
                        threshold,
                    )
                    return pd.read_csv(cache_csv)
            else:
                logger.info(
                    "ignoring cached CMA verdicts with missing/mismatched "
                    "threshold metadata at ar_model=%s threshold=%.2f: %s",
                    ar_model,
                    threshold,
                    cache_csv,
                )

        logger.info("running CMA pipeline at ar_model=%s", ar_model)
        tables_dir = cache_csv.parent
        figures_dir = tables_dir / "figures"

        pipeline_kwargs: dict[str, object] = {
            "tables_dir": tables_dir,
            "figures_dir": figures_dir,
            "research_summary_path": tables_dir / "research_summary.md",
            "significance_level": threshold,
        }
        if ar_model == AR_MODEL_MARKET:
            # Materialise a panel whose ``ar`` column is the market-model
            # β-AR residual, then feed it back through the orchestrator.
            # The matched panel uses the same recipe — we apply the
            # market-model transform to both so the mechanism / matched
            # regression rows are also driven by the β-AR engine, not a
            # mixed simple/market AR.
            mm_panel_path = tables_dir / "market_model_event_panel.csv"
            _materialise_market_model_panel(
                orchestrator.REAL_EVENT_PANEL,
                output_panel_path=mm_panel_path,
                estimation_window=estimation_window,
            )
            pipeline_kwargs["event_panel_path"] = mm_panel_path
            mm_matched_path = tables_dir / "market_model_matched_event_panel.csv"
            if orchestrator.REAL_MATCHED_EVENT_PANEL.exists():
                _materialise_market_model_panel(
                    orchestrator.REAL_MATCHED_EVENT_PANEL,
                    output_panel_path=mm_matched_path,
                    estimation_window=estimation_window,
                )
                pipeline_kwargs["matched_panel_path"] = mm_matched_path

        orchestrator.run_cma_pipeline(**pipeline_kwargs)  # type: ignore[arg-type]
        _write_cache_metadata(cache_csv, ar_model=ar_model, threshold=threshold)
        return pd.read_csv(cache_csv)

    return _run


def _validate_engine_frame(df: pd.DataFrame, ar_model: str) -> pd.DataFrame:
    """Ensure each per-engine verdicts frame has the columns the sweep needs."""
    missing = [c for c in REQUIRED_VERDICT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"verdicts frame at ar_model={ar_model!r} is missing columns "
            f"{sorted(missing)}; expected {sorted(REQUIRED_VERDICT_COLUMNS)}"
        )
    return df


def build_cma_ar_engine_sweep(
    threshold: float = DEFAULT_AR_ENGINE_THRESHOLD,
    ar_models: Sequence[str] = DEFAULT_AR_MODELS,
    *,
    runner: PipelineRunner | None = None,
) -> pd.DataFrame:
    """Return a long-format sweep DataFrame of verdicts across AR engines.

    Parameters
    ----------
    threshold:
        Significance threshold passed to the CMA pipeline at each engine
        pass. Held constant across the sweep on purpose — the comparison
        is between AR engines, not between (engine, threshold) pairs.
    ar_models:
        Iterable of AR engine labels to evaluate. Duplicates are
        de-duplicated, unknown labels raise ``ValueError``. Order is
        canonicalised to ``(adjusted, market)`` regardless of input.
    runner:
        Callable that, given a single engine label, returns the verdicts
        DataFrame. When ``None``, the default factory wires up the real
        CMA orchestrator with a ``results/sensitivity/ar_<engine>/``
        cache. Tests pass a fixture-backed callable to avoid running the
        pipeline twice.

    Returns
    -------
    pandas.DataFrame
        Long-format DataFrame with one row per (ar_model, hypothesis):
        columns ``ar_model``, ``hid``, ``name_cn``, ``verdict``,
        ``confidence``, ``evidence_tier``, ``n_obs``, ``strength``.

    Raises
    ------
    ValueError
        When ``ar_models`` is empty, contains an unsupported value, or
        any per-engine frame is missing required columns / hypotheses.
    """
    sorted_models = _normalise_ar_models(ar_models)
    if runner is None:
        runner = _cma_runner_factory(threshold=threshold)

    rows: list[dict[str, object]] = []
    for ar_model in sorted_models:
        frame = runner(ar_model)
        frame = _validate_engine_frame(frame, ar_model)
        present = set(frame["hid"].astype(str).str.strip().str.upper().tolist())
        missing_hids = set(_HYPOTHESIS_ORDER) - present
        if missing_hids:
            raise ValueError(
                f"verdicts at ar_model={ar_model!r} missing hypotheses "
                f"{sorted(missing_hids)}; expected H1-H7"
            )
        for _, row in frame.iterrows():
            hid = str(row["hid"]).strip().upper()
            if hid not in set(_HYPOTHESIS_ORDER):
                continue
            verdict = str(row.get("verdict", "")).strip()
            confidence = str(row.get("confidence", "")).strip()
            strength = classify_strength(verdict, confidence)
            rows.append(
                {
                    "ar_model": ar_model,
                    "hid": hid,
                    "name_cn": str(row.get("name_cn", "")).strip(),
                    "verdict": verdict,
                    "confidence": confidence,
                    "evidence_tier": str(row.get("evidence_tier", "")).strip()
                    or "supplementary",
                    "n_obs": row.get("n_obs", 0),
                    "strength": float(strength),
                }
            )

    sweep_df = pd.DataFrame(rows, columns=list(SWEEP_OUTPUT_COLUMNS))
    # Canonical H1..H7 row order (stable within each engine) so the
    # downstream plot renders the eye-anchored order without re-sorting.
    order_lookup = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}
    engine_lookup = {model: idx for idx, model in enumerate(DEFAULT_AR_MODELS)}
    sweep_df["_h_order"] = sweep_df["hid"].map(order_lookup)
    sweep_df["_e_order"] = sweep_df["ar_model"].map(engine_lookup)
    sweep_df = (
        sweep_df.sort_values(["_h_order", "_e_order"], kind="mergesort")
        .reset_index(drop=True)
        .drop(columns=["_h_order", "_e_order"])
    )
    return sweep_df


def build_cma_ar_engine_sweep_from_cache(
    *,
    ar_models: Sequence[str] | None = None,
    sensitivity_root: Path | None = None,
) -> pd.DataFrame:
    """Return a sweep DataFrame by reading cached per-engine CSVs only.

    This helper never calls the CMA orchestrator. If ``ar_models`` is
    omitted, it renders every valid cached engine under
    ``sensitivity_root``. If ``ar_models`` is provided, every requested
    cache must already exist.
    """
    resolved_models = (
        _discover_cached_ar_models(sensitivity_root=sensitivity_root)
        if ar_models is None
        else _normalise_ar_models(ar_models)
    )
    if not resolved_models:
        raise ValueError("no cached CMA AR-engine CSVs found")
    return build_cma_ar_engine_sweep(
        threshold=DEFAULT_AR_ENGINE_THRESHOLD,
        ar_models=resolved_models,
        runner=_cache_only_runner_factory(sensitivity_root=sensitivity_root),
    )


def _flipped_hypotheses(sweep_df: pd.DataFrame) -> dict[str, bool]:
    """Return a hid → True/False map indicating whether the verdict text
    differs between the AR engines for that hypothesis.

    With only two engines on the sweep, a flip is binary — either the
    verdict reads the same under both engines, or it differs. The plot
    annotation column uses this to label each row "stable" / "flipped"
    so a reviewer can scan for engine-fragile rows in one glance.
    """
    flipped: dict[str, bool] = {hid: False for hid in _HYPOTHESIS_ORDER}
    for hid in _HYPOTHESIS_ORDER:
        per_hid = sweep_df.loc[sweep_df["hid"] == hid]
        verdicts = set(per_hid["verdict"].astype(str).str.strip().tolist())
        flipped[hid] = len(verdicts) > 1
    return flipped


def _flip_label(flipped: bool) -> str:
    return "flipped" if flipped else "stable"


def render_ar_engine_forest_plot(
    sweep_df: pd.DataFrame,
    output_png: str | Path,
    output_pdf: str | Path | None = None,
    *,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the AR-engine-aware cross-hypothesis forest plot.

    Parameters
    ----------
    sweep_df:
        Long-format DataFrame from :func:`build_cma_ar_engine_sweep`
        (one row per (ar_model, hypothesis), with columns
        ``ar_model`` / ``hid`` / ``verdict`` / ``confidence`` /
        ``evidence_tier`` / ``strength``).
    output_png:
        Destination PNG. Parent directory is created if missing.
    output_pdf:
        Optional companion PDF. Pass ``None`` to skip PDF.
    title:
        Headline. Defaults to the audit-aligned Chinese title.
    generated_on:
        Override for the provenance footer. Defaults to today.

    Returns
    -------
    Path
        The PNG path written. The PDF, if requested, is written along.

    Raises
    ------
    ValueError
        When ``sweep_df`` is empty or missing required columns.
    """
    if sweep_df.empty:
        raise ValueError("AR engine sweep DataFrame is empty")
    missing_cols = [
        c for c in SWEEP_OUTPUT_COLUMNS if c not in sweep_df.columns
    ]
    if missing_cols:
        raise ValueError(
            f"sweep DataFrame missing columns {missing_cols}; "
            "build it via build_cma_ar_engine_sweep"
        )

    png_path = Path(output_png)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path: Path | None = Path(output_pdf) if output_pdf else None
    if pdf_path is not None:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)

    present_models = [
        model for model in DEFAULT_AR_MODELS
        if model in sweep_df["ar_model"].astype(str).unique()
    ]
    flipped_per_hid = _flipped_hypotheses(sweep_df)

    n_rows = len(_HYPOTHESIS_ORDER)
    fig_height = max(5.4, 0.95 * n_rows + 1.8)
    fig, ax = plt.subplots(figsize=(11.5, fig_height))

    with warnings.catch_warnings():
        # The CJK-friendly font stack the snapshot plots use does not
        # cover every Chinese glyph in every environment; the rendered
        # figure converts gracefully and the figure caption is the
        # source of truth for the verdict labels.
        warnings.simplefilter("ignore", UserWarning)

        # Y positions: H1 at top (y=0) → H7 at bottom (y=6).
        y_positions = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}

        # 1) Connecting arrow per hypothesis: a short horizontal line
        #    between the two engines' dots. Drawn first so the dots sit
        #    on top. Only rendered when the dots differ on the x axis;
        #    otherwise the dots overlap and the arrow would be invisible.
        for hid in _HYPOTHESIS_ORDER:
            per_hid = sweep_df.loc[sweep_df["hid"] == hid].copy()
            if per_hid.empty or per_hid["ar_model"].nunique() < 2:
                continue
            ordered = per_hid.assign(
                _order=per_hid["ar_model"].map(
                    {model: idx for idx, model in enumerate(DEFAULT_AR_MODELS)}
                )
            ).sort_values("_order")
            xs = ordered["strength"].astype(float).tolist()
            if abs(xs[1] - xs[0]) < 1e-9:
                # Dots overlap: skip the arrow (purely cosmetic; doesn't
                # change the verdict story conveyed by the markers).
                continue
            ax.annotate(
                "",
                xy=(xs[1], y_positions[hid]),
                xytext=(xs[0], y_positions[hid]),
                arrowprops={
                    "arrowstyle": "->",
                    "color": "#9ba3ad",
                    "lw": 1.1,
                    "alpha": 0.75,
                },
                zorder=2,
            )

        # 2) Per-engine dots: color + shape by engine.
        for hid in _HYPOTHESIS_ORDER:
            per_hid = sweep_df.loc[sweep_df["hid"] == hid]
            for _, row in per_hid.iterrows():
                ar_model = str(row["ar_model"]).strip()
                color = _ENGINE_COLORS.get(ar_model, "#5c6b77")
                marker = _ENGINE_MARKERS.get(ar_model, "o")
                ax.scatter(
                    [float(row["strength"])],
                    [y_positions[hid]],
                    s=145,
                    c=color,
                    marker=marker,
                    edgecolors="#18212b",
                    linewidths=0.9,
                    zorder=3,
                )

        # 3) Reference lines at the same 0.5 / 1.0 cuts the single-
        #    snapshot forest uses (visual landmarks).
        ax.axvline(0.5, color="#9ba3ad", linestyle="--", linewidth=1.0, zorder=1)
        ax.axvline(1.0, color="#9ba3ad", linestyle=":", linewidth=0.8, zorder=1)

        # 4) Axes / labels.
        ax.set_yticks(list(y_positions.values()))
        labels: list[str] = []
        for hid in _HYPOTHESIS_ORDER:
            per_hid = sweep_df.loc[sweep_df["hid"] == hid]
            name = ""
            if not per_hid.empty:
                first_name = str(per_hid.iloc[0].get("name_cn", "")).strip()
                if first_name:
                    name = first_name
            labels.append(f"{hid} · {name}" if name else hid)
        ax.set_yticklabels(labels, fontsize=11)
        ax.invert_yaxis()  # H1 at top, H7 at bottom

        ax.set_xlabel("证据强度 score (0 = 无支持，1 = 强支持)", fontsize=11)
        plot_title = title or "CMA 跨假说 — AR 引擎选择敏感性"
        ax.set_title(plot_title, fontsize=15, pad=14, fontweight="bold")

        ax.grid(axis="x", alpha=0.22, linestyle=":")
        ax.tick_params(axis="x", labelsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Wider right margin than the single-snapshot forest to fit the
        # per-hypothesis flip annotation.
        ax.set_xlim(0.0, 1.32)

        # 5) Right-margin flip annotation per hypothesis ("stable" /
        #    "flipped"). Only one axis on this sweep, so the label set
        #    is intentionally simpler than the threshold version.
        annotation_x = 1.05
        for hid in _HYPOTHESIS_ORDER:
            ax.text(
                annotation_x,
                y_positions[hid],
                _flip_label(flipped_per_hid.get(hid, False)),
                fontsize=10,
                va="center",
                ha="left",
                color="#30424f",
                family="monospace",
            )

        # 6) Legend: one handle per AR engine (color + shape combined).
        legend_handles = []
        for model in present_models:
            legend_handles.append(
                plt.Line2D(
                    [0],
                    [0],
                    marker=_ENGINE_MARKERS[model],
                    linestyle="",
                    markerfacecolor=_ENGINE_COLORS[model],
                    markeredgecolor="#18212b",
                    markersize=11,
                    label=_ENGINE_LABELS[model],
                )
            )
        if legend_handles:
            ax.legend(
                handles=legend_handles,
                loc="lower right",
                frameon=True,
                fontsize=10,
                title="AR 引擎",
                title_fontsize=10,
            )

        # 7) Captions / provenance.
        engine_label = ", ".join(present_models)
        sample_token = _VERDICT_ASCII.get("支持", "support")
        gen_date = (generated_on or date.today()).isoformat()
        fig.text(
            0.01,
            0.965,
            f"AR 引擎 sweep: [{engine_label}]  ·  每假说 {len(present_models)} 点连箭头  ·  "
            "shape = AR 引擎类型 (circle=adjusted, square=market)",
            fontsize=10,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.045,
            "adjusted = ret − benchmark_ret（文献标准、项目默认）；market = "
            "市场模型 β-AR，估计窗口 (-120,-10) trading days（commit 1e29476）。"
            f"评分对照同 cma_verdicts_forest（{sample_token}/hi=1.0, partial/mid=0.5, "
            "insufficient/mid=0.3, insufficient/lo=0.0）。",
            fontsize=9,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.013,
            "数据来源：results/sensitivity/ar_<engine>/cma_hypothesis_verdicts.csv  "
            f"·  生成日期：{gen_date}  ·  评分仅用于可视化对比，不构成新的统计推断",
            fontsize=9,
            color="#5c6b77",
        )

        fig.tight_layout(rect=(0.0, 0.07, 1.0, 0.94))
        fig.savefig(png_path, dpi=220)
        if pdf_path is not None:
            fig.savefig(pdf_path)
        plt.close(fig)

    return png_path


def build_cma_ar_engine_forest_plot(
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    threshold: float = DEFAULT_AR_ENGINE_THRESHOLD,
    ar_models: Sequence[str] = DEFAULT_AR_MODELS,
    runner: PipelineRunner | None = None,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """High-level convenience: sweep + render in one call.

    Mirrors the threshold-sensitivity ``build_cma_sensitivity_forest_plot``
    signature shape so callers can swap the two with minimal friction.
    Tests inject ``runner`` to avoid running the full CMA pipeline; the
    production path leaves it ``None`` and lets the default cached
    orchestrator handle the work.
    """
    sweep_df = build_cma_ar_engine_sweep(
        threshold=threshold, ar_models=ar_models, runner=runner
    )
    return render_ar_engine_forest_plot(
        sweep_df,
        output_png=output_png_path,
        output_pdf=output_pdf_path,
        title=title,
        generated_on=generated_on,
    )


def build_cma_ar_engine_forest_plot_from_cache(
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    ar_models: Sequence[str] | None = None,
    sensitivity_root: Path | None = None,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the AR-engine forest from existing cache CSVs only.

    Used by figure / paper re-renderers that must not perform fresh CMA
    passes as a side effect.
    """
    sweep_df = build_cma_ar_engine_sweep_from_cache(
        ar_models=ar_models,
        sensitivity_root=sensitivity_root,
    )
    return render_ar_engine_forest_plot(
        sweep_df,
        output_png=output_png_path,
        output_pdf=output_pdf_path,
        title=title,
        generated_on=generated_on,
    )
