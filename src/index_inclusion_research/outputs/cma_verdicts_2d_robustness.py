"""2D robustness heatmap of CMA hypothesis verdicts (threshold × AR engine).

The threshold sensitivity sweep (commit 87d624c, :mod:`cma_verdicts_sensitivity`)
answers "would the verdicts change under a different p-value threshold?"
The AR engine sweep (commit 1a6ba77, :mod:`cma_verdicts_ar_engine`) answers
a different methodological question: "would the verdicts change if the
abnormal return itself were defined via a market-model β rather than a
simple benchmark subtraction?"

Reviewers don't ask those two questions in isolation — they ask them
*together*: "is your conclusion stable across BOTH methodological axes
simultaneously?" This module crosses the two sweeps into a single
publication-quality heatmap so the answer is visible at a glance:

- 7 rows = H1-H7
- 8 columns = 4 thresholds × 2 engines (adjusted on the left, market on
  the right, with a visible vertical separator at the engine boundary)
- cell color = support-strength score (0=red, 0.5=white, 1=blue)
- cell annotation = ASCII verdict tag (``S+`` strong support / ``S``
  support / ``P+`` partial support / ``I`` insufficient)
- right-margin column = per-hypothesis stability summary
  (``stable`` / ``1 flip`` / ``2+ flips``) across the 8 cells

Cache layout: ``results/sensitivity/grid_<threshold>_<engine>/cma_hypothesis_verdicts.csv``.
Sibling to ``threshold_<T>/`` and ``ar_<engine>/`` so a future doctor or
paper-bundle sweep can pick up all three. To minimise re-runs, the
runner first falls back to the matching single-axis cache when the
(threshold, engine) combo matches an existing one:

- ``grid_0_10_adjusted`` falls back to ``ar_adjusted`` (threshold=0.10 by default)
- ``grid_0_10_market`` falls back to ``ar_market`` (threshold=0.10 by default)
- ``grid_<T>_adjusted`` for T in (0.05, 0.15, 0.20) falls back to
  ``threshold_<T>/`` (engine='adjusted' is the project default)

Only the three (T, market) cells with T ≠ 0.10 require a fresh CMA pass.

Two rendering modes mirror the single-axis modules:

- :func:`build_cma_2d_robustness_heatmap` is the explicit sweep entry
  and may call the CMA orchestrator when the cache is stale.
- :func:`build_cma_2d_robustness_heatmap_from_cache` is cache-only and
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
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt

from index_inclusion_research import paths
from index_inclusion_research.outputs.cma_verdicts_ar_engine import (
    AR_MODEL_ADJUSTED,
    AR_MODEL_MARKET,
    DEFAULT_AR_MODELS,
    SUPPORTED_AR_MODELS,
    _normalise_ar_model,
    _normalise_ar_models,
)
from index_inclusion_research.outputs.cma_verdicts_forest import (
    _HYPOTHESIS_ORDER,
    _VERDICT_ASCII,
    classify_strength,
)
from index_inclusion_research.outputs.cma_verdicts_sensitivity import (
    DEFAULT_SENSITIVITY_THRESHOLDS,
    _normalise_threshold,
    _normalise_threshold_label,
    _normalise_thresholds,
)

logger = logging.getLogger(__name__)

plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


# Default thresholds reused from the 1D sensitivity sweep so the
# crossing is consistent with the headline 4-threshold ladder.
DEFAULT_2D_THRESHOLDS: tuple[float, ...] = DEFAULT_SENSITIVITY_THRESHOLDS
DEFAULT_2D_AR_MODELS: tuple[str, ...] = DEFAULT_AR_MODELS

CACHE_METADATA_FILENAME = "cma_2d_robustness_cache_metadata.json"
CACHE_METADATA_SCHEMA_VERSION = 1


# Pipeline callable signature: takes a (threshold, ar_model) pair and
# returns a DataFrame matching the ``cma_hypothesis_verdicts.csv``
# schema. Tests inject a fixture-based callable; the default runner
# wires up the real CMA orchestrator with cache fallback to the
# single-axis caches.
PipelineRunner = Callable[[float, str], pd.DataFrame]

REQUIRED_VERDICT_COLUMNS: tuple[str, ...] = (
    "hid",
    "verdict",
    "confidence",
    "evidence_tier",
    "n_obs",
)

SWEEP_OUTPUT_COLUMNS: tuple[str, ...] = (
    "threshold",
    "ar_model",
    "hid",
    "name_cn",
    "verdict",
    "confidence",
    "evidence_tier",
    "n_obs",
    "strength",
)


# Verdict (Chinese) → short ASCII annotation tag used inside heatmap
# cells. Mirrors the strength score buckets so a reviewer reading the
# figure can decode color → tag → meaning without a legend.
_VERDICT_HEATMAP_TAG: dict[str, str] = {
    "支持": "S",          # support
    "部分支持": "P+",      # partial support
    "证据不足": "I",       # insufficient
}


def _normalise_threshold_engine_label(threshold: float, ar_model: str) -> str:
    """Return the cache-directory name for a (threshold, engine) pair.

    Layout: ``grid_<two-decimal threshold with _ for .>_<engine>``,
    e.g. ``grid_0_10_market``. Keeps the 1D cache layouts intact so a
    user with existing ``threshold_<T>/`` / ``ar_<engine>/`` caches can
    keep them and only add new combos under ``grid_<T>_<engine>/``.
    """
    return f"grid_{_normalise_threshold_label(threshold)}_{_normalise_ar_model(ar_model)}"


def _cache_csv_path(
    threshold: float, ar_model: str, *, sensitivity_root: Path | None = None
) -> Path:
    """Return ``results/sensitivity/grid_<T>_<engine>/cma_hypothesis_verdicts.csv``."""
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    return (
        root
        / _normalise_threshold_engine_label(threshold, ar_model)
        / "cma_hypothesis_verdicts.csv"
    )


def _cache_metadata_path(cache_csv: Path) -> Path:
    return cache_csv.with_name(CACHE_METADATA_FILENAME)


def _cache_metadata_matches(
    cache_csv: Path, *, threshold: float, ar_model: str
) -> bool:
    meta_path = _cache_metadata_path(cache_csv)
    if not meta_path.exists():
        return False
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        payload_threshold = float(payload["threshold"])
        payload_model = _normalise_ar_model(str(payload.get("ar_model", "")))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return payload_model == ar_model and math.isclose(
        payload_threshold, threshold, rel_tol=0.0, abs_tol=1e-12
    )


def _write_cache_metadata(
    cache_csv: Path, *, threshold: float, ar_model: str
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


def _fallback_single_axis_cache_csv(
    threshold: float,
    ar_model: str,
    *,
    sensitivity_root: Path,
    default_threshold: float = 0.10,
) -> Path | None:
    """Return the matching single-axis cache CSV if the (T, engine) pair
    can be served from an existing 1D sweep cache.

    The fallback matrix:

    - ``(0.10, adjusted)`` → ``ar_adjusted/`` (engine sweep default threshold)
    - ``(0.10, market)``   → ``ar_market/``   (engine sweep default threshold)
    - ``(T, adjusted)``     → ``threshold_<T>/`` (project default engine)

    The remaining ``(T ≠ 0.10, market)`` combos have no fallback and
    must be re-run by the CMA orchestrator.
    """
    threshold = _normalise_threshold(threshold)
    ar_model = _normalise_ar_model(ar_model)
    default_threshold = _normalise_threshold(default_threshold)

    if math.isclose(threshold, default_threshold, abs_tol=1e-12):
        ar_path = sensitivity_root / f"ar_{ar_model}" / "cma_hypothesis_verdicts.csv"
        if ar_path.exists():
            return ar_path
    if ar_model == AR_MODEL_ADJUSTED:
        thr_path = (
            sensitivity_root
            / f"threshold_{_normalise_threshold_label(threshold)}"
            / "cma_hypothesis_verdicts.csv"
        )
        if thr_path.exists():
            return thr_path
    return None


def _discover_cached_combinations(
    *, sensitivity_root: Path | None = None
) -> tuple[tuple[float, str], ...]:
    """Return the (threshold, engine) pairs already populated under
    ``sensitivity_root`` — either through the dedicated ``grid_*/``
    cache or via a single-axis fallback.

    Used by the cache-only renderer when the caller doesn't pin an
    explicit (thresholds, ar_models) grid: we render every combo that
    can be served without firing a fresh CMA pass.

    The fallback discovery is conservative: a single-axis cache only
    covers the *specific* (T, engine) cell whose label it matches —
    no cross-multiplication. This is intentional: ``threshold_0_05``
    only serves ``(0.05, adjusted)`` (engine == project default);
    ``ar_market`` only serves ``(0.10, market)`` (threshold ==
    AR-engine sweep default). Cells that have no exact fallback (e.g.
    ``(0.05, market)``) are excluded so a downstream cache-only sweep
    never asks for a CSV that doesn't exist.
    """
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    if not root.exists():
        return ()
    discovered: set[tuple[float, str]] = set()
    # Dedicated 2D caches under grid_*/ — these are unambiguous.
    for csv_path in sorted(root.glob("grid_*/cma_hypothesis_verdicts.csv")):
        label = csv_path.parent.name
        try:
            threshold, ar_model = _parse_grid_label(label)
        except ValueError:
            continue
        discovered.add((threshold, ar_model))
    # threshold_<T>/ caches → (T, adjusted) only (engine = project default).
    for csv_path in sorted(root.glob("threshold_*/cma_hypothesis_verdicts.csv")):
        label = csv_path.parent.name
        prefix = "threshold_"
        if not label.startswith(prefix):
            continue
        raw = label[len(prefix):]
        pieces = raw.split("_")
        if len(pieces) != 2:
            continue
        try:
            threshold = _normalise_threshold(float(f"{int(pieces[0])}.{pieces[1]}"))
        except ValueError:
            continue
        discovered.add((threshold, AR_MODEL_ADJUSTED))
    # ar_<engine>/ caches → (0.10, engine) only (threshold = AR-engine default).
    for csv_path in sorted(root.glob("ar_*/cma_hypothesis_verdicts.csv")):
        label = csv_path.parent.name
        prefix = "ar_"
        if not label.startswith(prefix):
            continue
        raw = label[len(prefix):]
        try:
            ar_model = _normalise_ar_model(raw)
        except ValueError:
            continue
        discovered.add((_normalise_threshold(0.10), ar_model))
    return tuple(sorted(discovered))


def _parse_grid_label(label: str) -> tuple[float, str]:
    """Inverse of :func:`_normalise_threshold_engine_label`."""
    prefix = "grid_"
    if not label.startswith(prefix):
        raise ValueError(f"invalid 2D grid cache label: {label}")
    raw = label[len(prefix):]
    pieces = raw.split("_")
    if len(pieces) < 3:
        raise ValueError(
            f"invalid 2D grid label {label!r}; expected "
            "`grid_<int>_<two decimals>_<engine>` such as `grid_0_10_market`"
        )
    # The engine token is the trailing piece (it has no underscore in
    # the canonical set; future engines with underscores would need a
    # different separator). The two leading numeric pieces form the
    # threshold.
    engine_token = pieces[-1]
    threshold_pieces = pieces[:-1]
    if len(threshold_pieces) != 2 or len(threshold_pieces[1]) != 2:
        raise ValueError(
            f"invalid 2D grid threshold encoding in {label!r}; expected "
            "`<int>_<two decimals>` such as `0_10`"
        )
    threshold = _normalise_threshold(
        float(f"{int(threshold_pieces[0])}.{threshold_pieces[1]}")
    )
    ar_model = _normalise_ar_model(engine_token)
    return threshold, ar_model


def _cache_only_runner_factory(
    *, sensitivity_root: Path | None = None
) -> PipelineRunner:
    """Return a runner that reads a (T, engine) pair from cache only,
    falling back to the matching single-axis cache when available.

    Raises ``FileNotFoundError`` if neither the dedicated 2D cache nor
    a single-axis fallback exists for the requested (T, engine) pair.
    """
    root = sensitivity_root or (paths.results_dir() / "sensitivity")

    def _run(threshold: float, ar_model: str) -> pd.DataFrame:
        threshold = _normalise_threshold(threshold)
        ar_model = _normalise_ar_model(ar_model)
        dedicated = _cache_csv_path(threshold, ar_model, sensitivity_root=root)
        if dedicated.exists():
            return pd.read_csv(dedicated)
        fallback = _fallback_single_axis_cache_csv(
            threshold, ar_model, sensitivity_root=root
        )
        if fallback is not None:
            return pd.read_csv(fallback)
        raise FileNotFoundError(
            f"cached CMA verdicts not found for (threshold={threshold:.2f}, "
            f"ar_model={ar_model!r}): looked in {dedicated} and single-axis "
            "fallback caches"
        )

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


def _cma_runner_factory(
    *,
    sensitivity_root: Path | None = None,
    upstream_inputs: Sequence[Path] | None = None,
    estimation_window: tuple[int, int] = (-120, -10),
    allow_fallback: bool = True,
) -> PipelineRunner:
    """Return a runner that:

    1. Reads a dedicated ``grid_<T>_<engine>/`` cache when present + fresh.
    2. Otherwise falls back to a matching single-axis cache (when
       ``allow_fallback=True``) without firing the orchestrator — keeps
       the prior threshold/engine sweep work amortised across the new
       2D figure.
    3. Otherwise calls :mod:`cma_verdicts_ar_engine`'s runner factory to
       re-run the CMA pipeline at (T, engine) and writes the result
       under ``grid_<T>_<engine>/``.

    Step 3 is the only path that can trigger a fresh CMA pass.
    """
    from index_inclusion_research.outputs.cma_verdicts_ar_engine import (
        _cma_runner_factory as _ar_engine_runner_factory,
    )

    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    upstream_paths = (
        tuple(Path(p) for p in upstream_inputs)
        if upstream_inputs is not None
        else _default_upstream_inputs()
    )

    def _run(threshold: float, ar_model: str) -> pd.DataFrame:
        threshold = _normalise_threshold(threshold)
        ar_model = _normalise_ar_model(ar_model)
        dedicated = _cache_csv_path(threshold, ar_model, sensitivity_root=root)
        dedicated.parent.mkdir(parents=True, exist_ok=True)
        if dedicated.exists() and _cache_metadata_matches(
            dedicated, threshold=threshold, ar_model=ar_model
        ):
            cache_mtime = dedicated.stat().st_mtime
            upstream_mtimes = [
                p.stat().st_mtime for p in upstream_paths if p.exists()
            ]
            if upstream_mtimes and cache_mtime >= max(upstream_mtimes):
                logger.info(
                    "reusing 2D cache (threshold=%.2f, ar_model=%s): %s",
                    threshold,
                    ar_model,
                    dedicated,
                )
                return pd.read_csv(dedicated)
            if not upstream_mtimes:
                logger.info(
                    "no upstream inputs found; using 2D cache "
                    "(threshold=%.2f, ar_model=%s)",
                    threshold,
                    ar_model,
                )
                return pd.read_csv(dedicated)
        if allow_fallback:
            fallback = _fallback_single_axis_cache_csv(
                threshold, ar_model, sensitivity_root=root
            )
            if fallback is not None:
                upstream_mtimes = [
                    p.stat().st_mtime for p in upstream_paths if p.exists()
                ]
                fallback_is_fresh = (
                    not upstream_mtimes or fallback.stat().st_mtime >= max(upstream_mtimes)
                )
                if fallback_is_fresh:
                    logger.info(
                        "serving 2D (threshold=%.2f, ar_model=%s) from single-axis "
                        "cache %s",
                        threshold,
                        ar_model,
                        fallback,
                    )
                    df = pd.read_csv(fallback)
                    # Persist a dedicated copy so future runs hit a single
                    # cache path (and metadata-verifiable threshold/engine).
                    df.to_csv(dedicated, index=False)
                    _write_cache_metadata(
                        dedicated, threshold=threshold, ar_model=ar_model
                    )
                    return df
                logger.info(
                    "single-axis fallback cache is stale for 2D "
                    "(threshold=%.2f, ar_model=%s): %s",
                    threshold,
                    ar_model,
                    fallback,
                )

        # Fresh CMA pass: delegate to the AR-engine runner factory which
        # already knows how to (a) materialise the market-model panel
        # when ar_model='market', and (b) cache under ``ar_<engine>/``
        # with threshold metadata. We then mirror the result into the
        # dedicated ``grid_<T>_<engine>/`` cache.
        logger.info(
            "running fresh CMA pipeline at threshold=%.2f, ar_model=%s",
            threshold,
            ar_model,
        )
        ar_runner = _ar_engine_runner_factory(
            sensitivity_root=root,
            upstream_inputs=upstream_paths,
            threshold=threshold,
            estimation_window=estimation_window,
        )
        df = ar_runner(ar_model)
        df.to_csv(dedicated, index=False)
        _write_cache_metadata(
            dedicated, threshold=threshold, ar_model=ar_model
        )
        return df

    return _run


def _validate_cell_frame(
    df: pd.DataFrame, threshold: float, ar_model: str
) -> pd.DataFrame:
    """Ensure each per-cell verdicts frame has the columns the sweep needs."""
    missing = [c for c in REQUIRED_VERDICT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"verdicts frame at (threshold={threshold:.2f}, ar_model={ar_model!r}) "
            f"is missing columns {sorted(missing)}; expected "
            f"{sorted(REQUIRED_VERDICT_COLUMNS)}"
        )
    return df


def build_cma_2d_sweep(
    thresholds: Sequence[float] = DEFAULT_2D_THRESHOLDS,
    ar_models: Sequence[str] = DEFAULT_2D_AR_MODELS,
    *,
    runner: PipelineRunner | None = None,
) -> pd.DataFrame:
    """Return a long-format sweep DataFrame across (threshold × engine).

    Parameters
    ----------
    thresholds:
        Iterable of p-value thresholds to evaluate (defaults to the
        canonical 0.05 / 0.10 / 0.15 / 0.20 ladder). Sorted ascending.
    ar_models:
        Iterable of AR engine labels (defaults to ``adjusted`` then
        ``market``). Canonicalised to the project order regardless of
        caller order.
    runner:
        Callable that, given a single (threshold, engine) pair, returns
        the verdicts DataFrame. When ``None``, the default factory wires
        up a cached CMA orchestrator with fallback to the existing
        single-axis sweep caches.

    Returns
    -------
    pandas.DataFrame
        Long-format DataFrame with one row per (threshold, ar_model,
        hypothesis): 7 × 4 × 2 = 56 rows in the default grid. Columns:
        ``threshold``, ``ar_model``, ``hid``, ``name_cn``, ``verdict``,
        ``confidence``, ``evidence_tier``, ``n_obs``, ``strength``.

    Raises
    ------
    ValueError
        When ``thresholds`` or ``ar_models`` is empty / contains
        invalid values, or any per-cell frame is missing required
        columns / hypotheses.
    """
    sorted_thresholds = _normalise_thresholds(thresholds)
    sorted_models = _normalise_ar_models(ar_models)
    if runner is None:
        runner = _cma_runner_factory()

    rows: list[dict[str, object]] = []
    for threshold in sorted_thresholds:
        for ar_model in sorted_models:
            frame = runner(threshold, ar_model)
            frame = _validate_cell_frame(frame, threshold, ar_model)
            present = set(
                frame["hid"].astype(str).str.strip().str.upper().tolist()
            )
            missing_hids = set(_HYPOTHESIS_ORDER) - present
            if missing_hids:
                raise ValueError(
                    f"verdicts at (threshold={threshold:.2f}, ar_model="
                    f"{ar_model!r}) missing hypotheses {sorted(missing_hids)}; "
                    "expected H1-H7"
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
                        "threshold": float(threshold),
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
    # Sort H1..H7 within each (threshold, engine) cell so downstream
    # plotting can index by hid without re-sorting.
    order_lookup = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}
    engine_lookup = {model: idx for idx, model in enumerate(DEFAULT_AR_MODELS)}
    sweep_df["_h_order"] = sweep_df["hid"].map(order_lookup)
    sweep_df["_e_order"] = sweep_df["ar_model"].map(engine_lookup)
    sweep_df = (
        sweep_df.sort_values(
            ["_h_order", "_e_order", "threshold"], kind="mergesort"
        )
        .reset_index(drop=True)
        .drop(columns=["_h_order", "_e_order"])
    )
    return sweep_df


def build_cma_2d_sweep_from_cache(
    *,
    thresholds: Sequence[float] | None = None,
    ar_models: Sequence[str] | None = None,
    sensitivity_root: Path | None = None,
) -> pd.DataFrame:
    """Return a 2D sweep DataFrame by reading cached CSVs only.

    Never calls the CMA orchestrator. When both ``thresholds`` and
    ``ar_models`` are omitted, every (T, engine) combo that can be
    served from an existing cache (dedicated or single-axis fallback)
    is rendered, and combos that have no cache are silently dropped —
    the heatmap then surfaces those gaps as NaN cells. When either axis
    is pinned, every requested combo must already be cached (a missing
    one raises ``FileNotFoundError``).
    """
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    cache_runner = _cache_only_runner_factory(sensitivity_root=root)

    if thresholds is None and ar_models is None:
        combos = _discover_cached_combinations(sensitivity_root=root)
        if not combos:
            raise ValueError(
                "no cached CMA 2D-robustness CSVs found under "
                f"{root}; populate via the 1D sweeps or "
                "`index-inclusion-build-cma-2d-robustness-heatmap`"
            )
        discovered_set = set(combos)
        resolved_thresholds: tuple[float, ...] = tuple(
            sorted({t for t, _ in combos})
        )
        resolved_models: tuple[str, ...] = tuple(
            m for m in DEFAULT_2D_AR_MODELS if any(am == m for _, am in combos)
        )

        def _filtered_runner(threshold: float, ar_model: str) -> pd.DataFrame:
            key = (_normalise_threshold(threshold), _normalise_ar_model(ar_model))
            if key not in discovered_set:
                return pd.DataFrame(columns=list(REQUIRED_VERDICT_COLUMNS))
            return cache_runner(threshold, ar_model)

        return _build_cma_2d_sweep_lenient(
            thresholds=resolved_thresholds,
            ar_models=resolved_models,
            runner=_filtered_runner,
        )

    resolved_thresholds = (
        _normalise_thresholds(thresholds)
        if thresholds is not None
        else DEFAULT_2D_THRESHOLDS
    )
    resolved_models = (
        _normalise_ar_models(ar_models)
        if ar_models is not None
        else DEFAULT_2D_AR_MODELS
    )
    return build_cma_2d_sweep(
        thresholds=resolved_thresholds,
        ar_models=resolved_models,
        runner=cache_runner,
    )


def _build_cma_2d_sweep_lenient(
    *,
    thresholds: Sequence[float],
    ar_models: Sequence[str],
    runner: PipelineRunner,
) -> pd.DataFrame:
    """Internal sweep that silently drops cells whose runner returns an
    empty DataFrame. Used by the auto-discovery cache-only path.

    Identical to :func:`build_cma_2d_sweep` except: (1) an empty frame
    from the runner is treated as "combo not cached, skip" rather than
    a contract violation; (2) the result is allowed to have fewer than
    7 hypotheses per (threshold, ar_model) cell. Downstream rendering
    then leaves those cells visually empty.
    """
    sorted_thresholds = _normalise_thresholds(thresholds)
    sorted_models = _normalise_ar_models(ar_models)
    rows: list[dict[str, object]] = []
    for threshold in sorted_thresholds:
        for ar_model in sorted_models:
            frame = runner(threshold, ar_model)
            if frame.empty:
                continue
            frame = _validate_cell_frame(frame, threshold, ar_model)
            present = set(
                frame["hid"].astype(str).str.strip().str.upper().tolist()
            )
            missing_hids = set(_HYPOTHESIS_ORDER) - present
            if missing_hids:
                raise ValueError(
                    f"verdicts at (threshold={threshold:.2f}, ar_model="
                    f"{ar_model!r}) missing hypotheses {sorted(missing_hids)}; "
                    "expected H1-H7"
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
                        "threshold": float(threshold),
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

    if not rows:
        raise ValueError(
            "no cached cells produced verdicts; populate the sensitivity "
            "caches first."
        )
    sweep_df = pd.DataFrame(rows, columns=list(SWEEP_OUTPUT_COLUMNS))
    order_lookup = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}
    engine_lookup = {model: idx for idx, model in enumerate(DEFAULT_AR_MODELS)}
    sweep_df["_h_order"] = sweep_df["hid"].map(order_lookup)
    sweep_df["_e_order"] = sweep_df["ar_model"].map(engine_lookup)
    sweep_df = (
        sweep_df.sort_values(
            ["_h_order", "_e_order", "threshold"], kind="mergesort"
        )
        .reset_index(drop=True)
        .drop(columns=["_h_order", "_e_order"])
    )
    return sweep_df


def _flip_count_per_hypothesis(sweep_df: pd.DataFrame) -> dict[str, int]:
    """Return per-hypothesis count of distinct verdict strings across all
    8 cells. ``0`` flips → identical verdict everywhere → "stable"; ``1``
    flip → exactly two distinct verdicts; ``2+`` flips → ≥3 distinct.

    This is the same definition the single-axis sensitivity module uses
    when it counts adjacent transitions, but recast as a set count
    because in a 2D grid the threshold axis is sorted while the engine
    axis is categorical, so adjacency loses meaning.
    """
    counts: dict[str, int] = {hid: 0 for hid in _HYPOTHESIS_ORDER}
    for hid in _HYPOTHESIS_ORDER:
        per_hid = sweep_df.loc[sweep_df["hid"] == hid]
        verdicts = set(per_hid["verdict"].astype(str).str.strip().tolist())
        if len(verdicts) <= 1:
            counts[hid] = 0
        elif len(verdicts) == 2:
            counts[hid] = 1
        else:
            counts[hid] = 2
    return counts


def _flip_label(flip_count: int) -> str:
    if flip_count <= 0:
        return "stable"
    if flip_count == 1:
        return "1 flip"
    return "2+ flips"


def _verdict_to_tag(verdict: str, strength: float) -> str:
    """Return the short ASCII tag rendered inside a heatmap cell.

    The tag distinguishes the support strength buckets so reviewers can
    decode the cell without reading the color. ``S+`` = strong support
    (高 confidence), ``S`` = support (中/低 confidence), ``P+`` =
    partial support (any confidence), ``I`` = insufficient (any
    confidence). Unknown verdicts render ``?``.
    """
    verdict = verdict.strip()
    if verdict == "支持":
        return "S+" if strength >= 0.95 else "S"
    if verdict == "部分支持":
        return "P+"
    if verdict == "证据不足":
        return "I"
    return "?"


def _build_strength_grid(
    sweep_df: pd.DataFrame,
    *,
    thresholds: Sequence[float],
    ar_models: Sequence[str],
) -> tuple[list[list[float]], list[list[str]], list[str]]:
    """Return (strength_matrix, tag_matrix, column_labels) for the heatmap.

    ``strength_matrix[row][col]`` is the support-strength score for the
    hypothesis at row position and the (engine, threshold) pair encoded
    in column ``col``. Columns are laid out as
    ``[adjusted-0.05, adjusted-0.10, adjusted-0.15, adjusted-0.20,
       market-0.05, market-0.10, market-0.15, market-0.20]`` so the
    "engine group" is contiguous on the x-axis (a vertical separator
    sits between cols 3 and 4).
    """
    thresholds = list(thresholds)
    ar_models = list(ar_models)
    strength_matrix: list[list[float]] = []
    tag_matrix: list[list[str]] = []
    for hid in _HYPOTHESIS_ORDER:
        strengths: list[float] = []
        tags: list[str] = []
        for ar_model in ar_models:
            for threshold in thresholds:
                cell = sweep_df.loc[
                    (sweep_df["hid"] == hid)
                    & (sweep_df["ar_model"] == ar_model)
                    & (sweep_df["threshold"].astype(float).round(2) == round(threshold, 2))
                ]
                if cell.empty:
                    strengths.append(float("nan"))
                    tags.append("·")
                    continue
                row = cell.iloc[0]
                strength = float(row["strength"])
                strengths.append(strength)
                tags.append(
                    _verdict_to_tag(str(row["verdict"]).strip(), strength)
                )
        strength_matrix.append(strengths)
        tag_matrix.append(tags)
    column_labels: list[str] = []
    for ar_model in ar_models:
        for threshold in thresholds:
            column_labels.append(f"{ar_model[:3]}\np={threshold:.2f}")
    return strength_matrix, tag_matrix, column_labels


def _heatmap_colormap() -> mcolors.LinearSegmentedColormap:
    """Diverging colormap: deep red (0.0) → white (0.5) → deep blue (1.0).

    The midpoint at 0.5 is the visual cue for "partial support" — a
    cell whiter than the midpoint reads as borderline; a strongly red
    or strongly blue cell reads as an unambiguous verdict.
    """
    return mcolors.LinearSegmentedColormap.from_list(
        "cma_2d_robustness",
        [(0.0, "#8c2d2d"), (0.5, "#f4f4f0"), (1.0, "#1f4e7a")],
    )


def render_2d_robustness_heatmap(
    sweep_df: pd.DataFrame,
    output_png: str | Path,
    output_pdf: str | Path | None = None,
    *,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the 2D (threshold × engine) robustness heatmap.

    Parameters
    ----------
    sweep_df:
        Long-format DataFrame from :func:`build_cma_2d_sweep` (one row
        per (threshold, ar_model, hypothesis)). Must contain columns
        listed in :data:`SWEEP_OUTPUT_COLUMNS`.
    output_png:
        Destination PNG. Parent directory is created if missing.
    output_pdf:
        Optional companion PDF. Pass ``None`` to skip.
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
        raise ValueError("2D sweep DataFrame is empty")
    missing_cols = [c for c in SWEEP_OUTPUT_COLUMNS if c not in sweep_df.columns]
    if missing_cols:
        raise ValueError(
            f"sweep DataFrame missing columns {missing_cols}; "
            "build it via build_cma_2d_sweep"
        )

    png_path = Path(output_png)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path: Path | None = Path(output_pdf) if output_pdf else None
    if pdf_path is not None:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)

    thresholds = sorted(sweep_df["threshold"].astype(float).round(2).unique().tolist())
    present_models = [
        m for m in DEFAULT_AR_MODELS if m in sweep_df["ar_model"].astype(str).unique()
    ]
    if not present_models:
        raise ValueError("sweep DataFrame contains no recognised AR engine labels")

    strength_matrix, tag_matrix, column_labels = _build_strength_grid(
        sweep_df, thresholds=thresholds, ar_models=present_models
    )
    flip_counts = _flip_count_per_hypothesis(sweep_df)

    n_rows = len(_HYPOTHESIS_ORDER)
    n_cols = len(thresholds) * len(present_models)
    fig_width = max(9.5, 1.1 * n_cols + 4.6)
    fig_height = max(5.4, 0.85 * n_rows + 2.0)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        cmap = _heatmap_colormap()
        norm = mcolors.Normalize(vmin=0.0, vmax=1.0)
        im = ax.imshow(
            strength_matrix,
            cmap=cmap,
            norm=norm,
            aspect="auto",
            origin="upper",
            interpolation="nearest",
        )

        # Cell annotations: ASCII verdict tag in the center of each
        # cell. Contrast picked so dark cells get white text and light
        # cells get dark text — same heuristic as matplotlib's "auto".
        for r in range(n_rows):
            for c in range(n_cols):
                strength = strength_matrix[r][c]
                if math.isnan(strength):
                    continue
                rgba = cmap(norm(strength))
                # Perceptual luminance: black text on warm/bright cells,
                # white text on saturated cells.
                lum = 0.299 * rgba[0] + 0.587 * rgba[1] + 0.114 * rgba[2]
                text_color = "#0a0a0a" if lum > 0.55 else "#fafafa"
                ax.text(
                    c,
                    r,
                    tag_matrix[r][c],
                    ha="center",
                    va="center",
                    color=text_color,
                    fontsize=12,
                    fontweight="bold",
                    family="monospace",
                )

        # Axis ticks: column labels (engine + threshold) on the bottom,
        # hypothesis IDs + names on the left.
        ax.set_xticks(range(n_cols))
        ax.set_xticklabels(column_labels, fontsize=9)
        ax.set_yticks(range(n_rows))
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

        # Engine-group separator: a vertical line between the last
        # adjusted column and the first market column so the visual
        # grouping is unambiguous.
        if len(present_models) > 1:
            separator_x = len(thresholds) - 0.5
            ax.axvline(
                separator_x,
                color="#18212b",
                linewidth=2.2,
                linestyle="-",
                zorder=4,
            )
            # Top-of-figure engine-group labels above each contiguous block.
            for idx, ar_model in enumerate(present_models):
                center_x = idx * len(thresholds) + (len(thresholds) - 1) / 2.0
                ax.text(
                    center_x,
                    -0.85,
                    f"{ar_model} engine",
                    ha="center",
                    va="bottom",
                    fontsize=11,
                    fontweight="bold",
                    color="#18212b",
                )

        # Cell border grid (light grey) so individual cells are visible.
        ax.set_xticks([x - 0.5 for x in range(1, n_cols)], minor=True)
        ax.set_yticks([y - 0.5 for y in range(1, n_rows)], minor=True)
        ax.grid(which="minor", color="#cfd3d8", linewidth=0.6)
        ax.tick_params(which="minor", length=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        plot_title = (
            title or "CMA 2D 稳健性 — 阈值 × AR 引擎 (7 假说 × 8 单元)"
        )
        ax.set_title(plot_title, fontsize=15, pad=22, fontweight="bold")

        # Colourbar on the right.
        cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.18)
        cbar.set_label("support strength (0=无, 0.5=部分, 1=强)", fontsize=10)
        cbar.ax.tick_params(labelsize=9)

        # Right-margin flip annotation column (parallels the 1D forest
        # right-margin column). Drawn in axis-relative coordinates so
        # the spacing doesn't depend on n_cols.
        annotation_x = n_cols - 0.5 + 0.6 + (0.05 * n_cols)
        ax.text(
            annotation_x,
            -0.85,
            "flip count",
            ha="left",
            va="bottom",
            fontsize=10,
            color="#18212b",
            family="monospace",
            fontweight="bold",
        )
        for row_idx, hid in enumerate(_HYPOTHESIS_ORDER):
            ax.text(
                annotation_x,
                row_idx,
                _flip_label(flip_counts.get(hid, 0)),
                ha="left",
                va="center",
                fontsize=10,
                color="#18212b",
                family="monospace",
            )

        # Stretch x-limits so the flip-count column is visible.
        ax.set_xlim(-0.5, annotation_x + 2.2)

        # Captions / provenance.
        threshold_label = ", ".join(f"{t:.2f}" for t in thresholds)
        engine_label = ", ".join(present_models)
        sample_token = _VERDICT_ASCII.get("支持", "support")
        gen_date = (generated_on or date.today()).isoformat()
        fig.text(
            0.01,
            0.965,
            f"阈值 sweep: [{threshold_label}]  ·  AR 引擎: [{engine_label}]  ·  "
            f"{len(_HYPOTHESIS_ORDER)}×{n_cols}={len(_HYPOTHESIS_ORDER) * n_cols} 单元；色温 = "
            "support strength；标签 = ASCII verdict tag (S+/S/P+/I)",
            fontsize=10,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.040,
            f"评分对照同 cma_verdicts_forest（{sample_token}/hi=1.0, partial/mid=0.5, "
            "insufficient/mid=0.3, insufficient/lo=0.0）。flip count = "
            "该假说在 8 单元中出现的 distinct verdict 数 (1 = 一种, 2 = 两种, 3+ = 三种以上)。",
            fontsize=9,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.013,
            "数据来源：results/sensitivity/grid_<T>_<engine>/cma_hypothesis_verdicts.csv  "
            f"(单轴 cache 作为 fallback)  ·  生成日期：{gen_date}  ·  评分仅用于可视化对比，不构成新的统计推断",
            fontsize=9,
            color="#5c6b77",
        )

        fig.tight_layout(rect=(0.0, 0.07, 1.0, 0.92))
        fig.savefig(png_path, dpi=220)
        if pdf_path is not None:
            fig.savefig(pdf_path)
        plt.close(fig)

    return png_path


def build_cma_2d_robustness_heatmap(
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    thresholds: Sequence[float] = DEFAULT_2D_THRESHOLDS,
    ar_models: Sequence[str] = DEFAULT_2D_AR_MODELS,
    runner: PipelineRunner | None = None,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """High-level convenience: 2D sweep + render in one call.

    Mirrors the threshold + AR-engine forest plot signatures so callers
    can swap between the three robustness plots with minimal friction.
    Tests inject ``runner`` to avoid running the full CMA pipeline up
    to 8 times; production callers leave it ``None`` and let the
    default cached orchestrator handle the work.
    """
    sweep_df = build_cma_2d_sweep(
        thresholds=thresholds, ar_models=ar_models, runner=runner
    )
    return render_2d_robustness_heatmap(
        sweep_df,
        output_png=output_png_path,
        output_pdf=output_pdf_path,
        title=title,
        generated_on=generated_on,
    )


def build_cma_2d_robustness_heatmap_from_cache(
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    thresholds: Sequence[float] | None = None,
    ar_models: Sequence[str] | None = None,
    sensitivity_root: Path | None = None,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the 2D robustness heatmap from cache only.

    Used by figure / paper re-renderers that must not perform fresh CMA
    passes as a side effect.
    """
    sweep_df = build_cma_2d_sweep_from_cache(
        thresholds=thresholds,
        ar_models=ar_models,
        sensitivity_root=sensitivity_root,
    )
    return render_2d_robustness_heatmap(
        sweep_df,
        output_png=output_png_path,
        output_pdf=output_pdf_path,
        title=title,
        generated_on=generated_on,
    )


__all__ = [
    "AR_MODEL_ADJUSTED",
    "AR_MODEL_MARKET",
    "CACHE_METADATA_FILENAME",
    "CACHE_METADATA_SCHEMA_VERSION",
    "DEFAULT_2D_AR_MODELS",
    "DEFAULT_2D_THRESHOLDS",
    "REQUIRED_VERDICT_COLUMNS",
    "SUPPORTED_AR_MODELS",
    "SWEEP_OUTPUT_COLUMNS",
    "build_cma_2d_robustness_heatmap",
    "build_cma_2d_robustness_heatmap_from_cache",
    "build_cma_2d_sweep",
    "build_cma_2d_sweep_from_cache",
    "render_2d_robustness_heatmap",
]
