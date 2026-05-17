"""Sensitivity-aware forest plot of CMA hypothesis verdicts across thresholds.

Extends the cross-hypothesis CMA verdicts forest plot (commit f0c2260,
:mod:`cma_verdicts_forest`) to a multi-threshold visualisation for the
reviewer question, "what changes if the p-threshold changes?" The
explicit CLI sweeps the CMA pipeline across user-supplied thresholds
(defaults 0.05 / 0.10 / 0.15 / 0.20), caches each verdict CSV under
``results/sensitivity/threshold_<T>/``, and plots the resulting
(H1-H7, support-strength) trajectories on a single 0-1 axis.

The statistical threshold only gates the hypotheses whose verdicts are
decided by a structured p-value in the current pipeline (H1 / H4 / H5).
H2 / H3 / H6 / H7 are still shown for context, but their current
headline gates are directional spreads / hit rates / ratios rather than
this p-threshold. If those rows move in a threshold figure, that should
come from changed upstream inputs, not from the significance-level knob.

The plot answers two questions at once:

1. *Stability*: how much does the support-strength score swing as the
   threshold tightens / loosens? P-gated rows (H1 / H4 / H5) are the
   rows expected to show threshold-driven movement; non-p rows provide
   context and should usually remain stable across the sweep.
2. *Flip detection*: did any hypothesis swap verdict text between the
   thresholds (e.g. "证据不足" at 0.05 → "部分支持" at 0.20)? The right
   margin annotation labels each row "stable" / "1 flip" / "2+ flips"
   and the marker shape changes (circle = stable, triangle = flipped
   at this threshold) so the swings are visible at a glance.

The score axis itself reuses :func:`cma_verdicts_forest.classify_strength`
so the sensitivity figure and the single-snapshot figure share a single
verdict→strength mapping. No new statistical inference is introduced.

Two rendering modes are intentionally separate:

- ``build_cma_sensitivity_forest_plot`` is the explicit sweep entry and
  may call the CMA orchestrator when the cache is stale.
- ``build_cma_sensitivity_forest_plot_from_cache`` is cache-only and is
  used by ``make-figures-tables`` / ``paper_bundle`` so those re-renderers
  never trigger fresh threshold runs.
"""

from __future__ import annotations

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
    _TIER_COLORS,
    _TIER_LABELS,
    _VERDICT_ASCII,
    classify_strength,
)

logger = logging.getLogger(__name__)

plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


# Default thresholds for the sweep. 0.05 (strict, traditional academic),
# 0.10 (project default — backwards compatible with historic verdicts),
# 0.15 and 0.20 (loose, for symmetry / boundary-stress tests). The same
# four-point ladder is used by the dashboard's CMA threshold chip.
DEFAULT_SENSITIVITY_THRESHOLDS: tuple[float, ...] = (0.05, 0.10, 0.15, 0.20)

# Pipeline callable signature: takes a threshold, returns a DataFrame
# matching the cma_hypothesis_verdicts.csv schema (the same one the
# single-snapshot forest reads). Tests inject a fixture-based mock; the
# default implementation re-runs the CMA orchestrator in-process and
# caches the CSV under results/sensitivity/threshold_<T>/.
PipelineRunner = Callable[[float], pd.DataFrame]

REQUIRED_VERDICT_COLUMNS: tuple[str, ...] = (
    "hid",
    "verdict",
    "confidence",
    "evidence_tier",
    "n_obs",
)

SWEEP_OUTPUT_COLUMNS: tuple[str, ...] = (
    "threshold",
    "hid",
    "name_cn",
    "verdict",
    "confidence",
    "evidence_tier",
    "n_obs",
    "strength",
)


def _normalise_threshold(threshold: float) -> float:
    """Return a cache-safe threshold, rejecting labels that would collide.

    Cache directories are named with two decimal places. Accepting
    arbitrary precision would make ``0.104`` and ``0.10`` share
    ``threshold_0_10`` after formatting, so explicit custom thresholds
    must already be representable at two-decimal precision.
    """
    value = float(threshold)
    if not math.isfinite(value):
        raise ValueError("thresholds must be finite floats")
    if value <= 0 or value > 1:
        raise ValueError("thresholds must be in the interval (0, 1]")
    rounded = round(value, 2)
    if abs(value - rounded) > 1e-12:
        raise ValueError(
            f"threshold {threshold!r} has more than two decimal places; "
            "use a two-decimal p-value to avoid cache-label collisions"
        )
    return rounded


def _normalise_threshold_label(threshold: float) -> str:
    """Return the cache-directory name for a threshold (e.g. ``0.05`` → ``0_05``).

    The CSV cache directory is ``results/sensitivity/threshold_0_05`` so
    a literal ``.`` doesn't show up on macOS Finder where it can confuse
    file-system shells. Two-decimal rounding matches the dashboard chip
    canonical set (0.01 / 0.05 / 0.10 / 0.15 / 0.20).
    """
    return f"{_normalise_threshold(threshold):.2f}".replace(".", "_")


def _threshold_from_cache_dir(cache_dir: Path) -> float:
    label = cache_dir.name
    prefix = "threshold_"
    if not label.startswith(prefix):
        raise ValueError(f"invalid CMA sensitivity cache directory: {cache_dir}")
    raw = label[len(prefix):]
    pieces = raw.split("_")
    if len(pieces) != 2 or len(pieces[1]) != 2:
        raise ValueError(
            f"invalid CMA sensitivity cache label {label!r}; expected "
            "`threshold_<int>_<two decimals>` such as `threshold_0_05`"
        )
    try:
        return _normalise_threshold(float(f"{int(pieces[0])}.{pieces[1]}"))
    except ValueError as exc:
        raise ValueError(
            f"invalid CMA sensitivity cache label {label!r}: {exc}"
        ) from exc


def _normalise_thresholds(thresholds: Sequence[float]) -> tuple[float, ...]:
    if not thresholds:
        raise ValueError("thresholds must contain at least one value")
    return tuple(sorted({_normalise_threshold(float(t)) for t in thresholds}))


def _cache_csv_path(threshold: float, *, sensitivity_root: Path | None = None) -> Path:
    """Return ``results/sensitivity/threshold_<T>/cma_hypothesis_verdicts.csv``."""
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    return root / f"threshold_{_normalise_threshold_label(threshold)}" / "cma_hypothesis_verdicts.csv"


def _discover_cached_thresholds(
    *, sensitivity_root: Path | None = None
) -> tuple[float, ...]:
    root = sensitivity_root or (paths.results_dir() / "sensitivity")
    if not root.exists():
        return ()
    discovered: dict[str, float] = {}
    for csv_path in sorted(root.glob("threshold_*/cma_hypothesis_verdicts.csv")):
        threshold = _threshold_from_cache_dir(csv_path.parent)
        discovered[_normalise_threshold_label(threshold)] = threshold
    return tuple(sorted(discovered.values()))


def _cache_only_runner_factory(
    *, sensitivity_root: Path | None = None
) -> PipelineRunner:
    def _run(threshold: float) -> pd.DataFrame:
        cache_csv = _cache_csv_path(threshold, sensitivity_root=sensitivity_root)
        if not cache_csv.exists():
            raise FileNotFoundError(
                f"cached CMA verdicts not found for threshold={threshold:.2f}: "
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


def _cma_runner_factory(
    *,
    sensitivity_root: Path | None = None,
    upstream_inputs: Sequence[Path] | None = None,
) -> PipelineRunner:
    """Return a runner that calls the real CMA orchestrator per threshold.

    The runner caches the verdicts CSV under
    ``results/sensitivity/threshold_<T>/`` and reuses an existing cache
    when the CSV mtime is newer than every file in ``upstream_inputs``.
    ``upstream_inputs`` defaults to the canonical CMA inputs that can
    alter H1-H7 verdicts: event panel, matched panel, events clean,
    passive AUM, CN passive AUM proxy, and H6 weight-change. Pass an
    explicit list in tests to override.

    Each call writes both ``cma_hypothesis_verdicts.csv`` and supporting
    artifacts (asymmetry summary, etc.) under
    ``results/sensitivity/threshold_<T>/`` so a later debug pass can
    inspect what the pipeline emitted at that threshold without re-
    running.
    """
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    if upstream_inputs is None:
        upstream_inputs = _default_upstream_inputs()

    upstream_paths = tuple(Path(p) for p in upstream_inputs)

    def _run(threshold: float) -> pd.DataFrame:
        threshold = _normalise_threshold(threshold)
        cache_csv = _cache_csv_path(threshold, sensitivity_root=sensitivity_root)
        cache_csv.parent.mkdir(parents=True, exist_ok=True)
        if cache_csv.exists():
            cache_mtime = cache_csv.stat().st_mtime
            upstream_mtimes = [
                p.stat().st_mtime for p in upstream_paths if p.exists()
            ]
            if upstream_mtimes and cache_mtime >= max(upstream_mtimes):
                logger.info(
                    "reusing cached CMA verdicts at threshold=%.2f: %s",
                    threshold,
                    cache_csv,
                )
                return pd.read_csv(cache_csv)
            if not upstream_mtimes:
                # Upstream inputs absent → trust the cache rather than
                # blow up the bundle (tests run without the data files).
                logger.info(
                    "no upstream inputs found; using cached CMA verdicts at threshold=%.2f",
                    threshold,
                )
                return pd.read_csv(cache_csv)

        logger.info("running CMA pipeline at threshold=%.2f", threshold)
        tables_dir = cache_csv.parent
        figures_dir = tables_dir / "figures"
        orchestrator.run_cma_pipeline(
            tables_dir=tables_dir,
            figures_dir=figures_dir,
            research_summary_path=tables_dir / "research_summary.md",
            significance_level=threshold,
        )
        return pd.read_csv(cache_csv)

    return _run


def _validate_threshold_frame(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """Ensure each per-threshold verdicts frame has the columns the sweep needs.

    A missing column (e.g. ``evidence_tier``) would silently propagate
    into the plot's tier colour map as NaN and emit confusing legend
    entries — surface the contract violation early so the failure mode
    is "this CSV was malformed" not "the plot looks wrong".
    """
    missing = [c for c in REQUIRED_VERDICT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"verdicts frame at threshold={threshold} is missing columns "
            f"{sorted(missing)}; expected {sorted(REQUIRED_VERDICT_COLUMNS)}"
        )
    return df


def build_cma_sensitivity_sweep(
    thresholds: Sequence[float] = DEFAULT_SENSITIVITY_THRESHOLDS,
    *,
    runner: PipelineRunner | None = None,
) -> pd.DataFrame:
    """Return a long-format sweep DataFrame of verdicts across thresholds.

    Parameters
    ----------
    thresholds:
        Iterable of p-value thresholds to evaluate. Duplicate values are
        de-duplicated and the output is sorted ascending so the plot
        renders the sweep left-to-right by tightening / loosening.
    runner:
        Callable that, given a single threshold, returns the verdicts
        DataFrame (same schema as ``cma_hypothesis_verdicts.csv``). When
        ``None``, the default factory wires up the real CMA orchestrator
        with a ``results/sensitivity/threshold_<T>/`` cache. Tests pass
        a fixture-backed callable to avoid running the pipeline four
        times.

    Returns
    -------
    pandas.DataFrame
        Long-format DataFrame with one row per (threshold, hypothesis):
        columns ``threshold``, ``hid``, ``name_cn``, ``verdict``,
        ``confidence``, ``evidence_tier``, ``n_obs``, ``strength``.

    Raises
    ------
    ValueError
        When ``thresholds`` is empty after de-dup, or any per-threshold
        verdicts frame is missing required columns / hypotheses.
    """
    sorted_thresholds = _normalise_thresholds(thresholds)
    if runner is None:
        runner = _cma_runner_factory()

    rows: list[dict[str, object]] = []
    for threshold in sorted_thresholds:
        frame = runner(threshold)
        frame = _validate_threshold_frame(frame, threshold)
        present = set(frame["hid"].astype(str).str.strip().str.upper().tolist())
        missing_hids = set(_HYPOTHESIS_ORDER) - present
        if missing_hids:
            raise ValueError(
                f"verdicts at threshold={threshold} missing hypotheses "
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
                    "threshold": float(threshold),
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
    # Canonical H1..H7 row order (stable within each threshold) so the
    # downstream plot renders the eye-anchored order without re-sorting.
    order_lookup = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}
    sweep_df["_order"] = sweep_df["hid"].map(order_lookup)
    sweep_df = sweep_df.sort_values(["_order", "threshold"], kind="mergesort").reset_index(drop=True)
    sweep_df = sweep_df.drop(columns=["_order"])
    return sweep_df


def build_cma_sensitivity_sweep_from_cache(
    *,
    thresholds: Sequence[float] | None = None,
    sensitivity_root: Path | None = None,
) -> pd.DataFrame:
    """Return a sweep DataFrame by reading cached per-threshold CSVs only.

    This helper never calls the CMA orchestrator. If ``thresholds`` is
    omitted, it renders every valid cached threshold under
    ``sensitivity_root``. If ``thresholds`` is provided, every requested
    cache must already exist.
    """
    resolved_thresholds = (
        _discover_cached_thresholds(sensitivity_root=sensitivity_root)
        if thresholds is None
        else _normalise_thresholds(thresholds)
    )
    if not resolved_thresholds:
        raise ValueError("no cached CMA sensitivity threshold CSVs found")
    return build_cma_sensitivity_sweep(
        thresholds=resolved_thresholds,
        runner=_cache_only_runner_factory(sensitivity_root=sensitivity_root),
    )


def _count_flips_per_hypothesis(sweep_df: pd.DataFrame) -> dict[str, int]:
    """Return per-hypothesis count of verdict-text transitions across the sweep.

    A "flip" is any threshold step where ``verdict`` text differs from
    the previous threshold (sorted ascending). 0 flips = stable, 1 flip
    = single transition (typical "支持/边缘 to 部分支持"), 2+ flips =
    truly threshold-sensitive (verdict reads different at three of four
    thresholds). Used by the right-margin annotation.
    """
    flip_counts: dict[str, int] = {hid: 0 for hid in _HYPOTHESIS_ORDER}
    for hid in _HYPOTHESIS_ORDER:
        per_hid = sweep_df.loc[sweep_df["hid"] == hid].sort_values("threshold")
        previous: str | None = None
        flips = 0
        for _, row in per_hid.iterrows():
            current = str(row["verdict"]).strip()
            if previous is not None and current != previous:
                flips += 1
            previous = current
        flip_counts[hid] = flips
    return flip_counts


def _flip_label(flip_count: int) -> str:
    if flip_count <= 0:
        return "stable"
    if flip_count == 1:
        return "1 flip"
    return "2+ flips"


def _marker_for_flip(threshold_flipped: bool) -> str:
    """Return the matplotlib marker token for a stable / flipped dot."""
    return "^" if threshold_flipped else "o"


def render_sensitivity_forest_plot(
    sweep_df: pd.DataFrame,
    output_png: str | Path,
    output_pdf: str | Path | None = None,
    *,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the sensitivity-aware cross-hypothesis forest plot.

    Parameters
    ----------
    sweep_df:
        Long-format DataFrame from :func:`build_cma_sensitivity_sweep`
        (one row per (threshold, hypothesis), with columns
        ``threshold`` / ``hid`` / ``verdict`` / ``confidence`` /
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
        raise ValueError("sensitivity sweep DataFrame is empty")
    missing_cols = [
        c for c in SWEEP_OUTPUT_COLUMNS if c not in sweep_df.columns
    ]
    if missing_cols:
        raise ValueError(
            f"sweep DataFrame missing columns {missing_cols}; "
            "build it via build_cma_sensitivity_sweep"
        )

    png_path = Path(output_png)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path: Path | None = Path(output_pdf) if output_pdf else None
    if pdf_path is not None:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)

    thresholds = sorted(sweep_df["threshold"].unique().tolist())
    flip_counts = _count_flips_per_hypothesis(sweep_df)

    n_rows = len(_HYPOTHESIS_ORDER)
    fig_height = max(5.4, 0.95 * n_rows + 1.8)
    fig, ax = plt.subplots(figsize=(11.5, fig_height))

    with warnings.catch_warnings():
        # The CJK-friendly font stack the single-snapshot plot uses does
        # not cover every Chinese glyph in every environment; the
        # generated figure converts gracefully and the figure caption
        # is the source of truth for the verdict labels.
        warnings.simplefilter("ignore", UserWarning)

        # Y positions: H1 at top (y=0) → H7 at bottom (y=6).
        y_positions = {hid: idx for idx, hid in enumerate(_HYPOTHESIS_ORDER)}

        # 1) Connecting line per hypothesis: thin gray polyline through
        #    the dots sorted by threshold. Drawn first so the dots sit on top.
        for hid in _HYPOTHESIS_ORDER:
            per_hid = sweep_df.loc[sweep_df["hid"] == hid].sort_values("threshold")
            if per_hid.empty:
                continue
            xs = per_hid["strength"].astype(float).tolist()
            ys = [y_positions[hid]] * len(xs)
            ax.plot(
                xs,
                ys,
                color="#9ba3ad",
                linewidth=1.1,
                linestyle="-",
                alpha=0.55,
                zorder=2,
            )

        # 2) Per-threshold dots: color by evidence_tier at that threshold,
        #    shape by "flipped relative to the prior threshold's verdict".
        for hid in _HYPOTHESIS_ORDER:
            per_hid = (
                sweep_df.loc[sweep_df["hid"] == hid]
                .sort_values("threshold")
                .reset_index(drop=True)
            )
            previous_verdict: str | None = None
            for _, row in per_hid.iterrows():
                tier = str(row["evidence_tier"]).strip() or "supplementary"
                color = _TIER_COLORS.get(tier, _TIER_COLORS["supplementary"])
                current_verdict = str(row["verdict"]).strip()
                flipped = (
                    previous_verdict is not None
                    and current_verdict != previous_verdict
                )
                marker = _marker_for_flip(flipped)
                ax.scatter(
                    [float(row["strength"])],
                    [y_positions[hid]],
                    s=130,
                    c=color,
                    marker=marker,
                    edgecolors="#18212b",
                    linewidths=0.9,
                    zorder=3,
                )
                previous_verdict = current_verdict

        # 3) Reference lines at the same 0.5 / 1.0 cuts the single-
        #    snapshot forest uses (visual landmarks).
        ax.axvline(0.5, color="#9ba3ad", linestyle="--", linewidth=1.0, zorder=1)
        ax.axvline(1.0, color="#9ba3ad", linestyle=":", linewidth=0.8, zorder=1)

        # 4) Axes / labels.
        ax.set_yticks(list(y_positions.values()))
        # Name lookup: first non-empty name_cn per hypothesis, defaulting
        # to the bare hid (fixture-friendly).
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
        plot_title = (
            title
            or "CMA verdicts 灵敏度 — 阈值 sweep × H1-H7 证据强度轨迹"
        )
        ax.set_title(plot_title, fontsize=15, pad=14, fontweight="bold")

        ax.grid(axis="x", alpha=0.22, linestyle=":")
        ax.tick_params(axis="x", labelsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Wider right margin than the single-snapshot forest to fit the
        # per-hypothesis flip annotation.
        ax.set_xlim(0.0, 1.32)

        # 5) Right-margin flip annotation per hypothesis.
        annotation_x = 1.05
        for hid in _HYPOTHESIS_ORDER:
            flips = flip_counts.get(hid, 0)
            ax.text(
                annotation_x,
                y_positions[hid],
                _flip_label(flips),
                fontsize=10,
                va="center",
                ha="left",
                color="#30424f",
                family="monospace",
            )

        # 6) Legend: two columns of handles —
        #    (a) evidence-tier colour swatches (only present tiers);
        #    (b) shape legend (circle = stable, triangle = flipped).
        present_tiers: list[str] = []
        for tier in sweep_df["evidence_tier"].astype(str).str.strip().tolist():
            if tier and tier not in present_tiers:
                present_tiers.append(tier)
        legend_handles = []
        for tier in ("core", "supplementary"):
            if tier in present_tiers:
                legend_handles.append(
                    plt.Line2D(
                        [0],
                        [0],
                        marker="o",
                        linestyle="",
                        markerfacecolor=_TIER_COLORS[tier],
                        markeredgecolor="#18212b",
                        markersize=10,
                        label=_TIER_LABELS[tier],
                    )
                )
        # Shape legend (always shown — flips may exist for any sweep).
        legend_handles.append(
            plt.Line2D(
                [0],
                [0],
                marker="o",
                linestyle="",
                markerfacecolor="#5c6b77",
                markeredgecolor="#18212b",
                markersize=10,
                label="stable verdict",
            )
        )
        legend_handles.append(
            plt.Line2D(
                [0],
                [0],
                marker="^",
                linestyle="",
                markerfacecolor="#5c6b77",
                markeredgecolor="#18212b",
                markersize=11,
                label="flipped at threshold",
            )
        )
        ax.legend(
            handles=legend_handles,
            loc="lower right",
            frameon=True,
            fontsize=10,
            title="tier · shape",
            title_fontsize=10,
        )

        # 7) Captions / provenance.
        threshold_label = ", ".join(f"{t:.2f}" for t in thresholds)
        # Verdict→ASCII map for the figure caption so reviewers can
        # cross-reference the strength score without parsing the CJK
        # labels. Mirrors cma_verdicts_forest.
        sample_token = _VERDICT_ASCII.get("支持", "support")
        gen_date = (generated_on or date.today()).isoformat()
        fig.text(
            0.01,
            0.965,
            f"阈值 sweep: [{threshold_label}]  ·  每假说 {len(thresholds)} 点连线  ·  "
            "shape = 是否相对上一阈值 verdict 翻转",
            fontsize=10,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.045,
            f"评分对照同 cma_verdicts_forest（{sample_token}/hi=1.0, partial/mid=0.5, "
            "insufficient/mid=0.3, insufficient/lo=0.0）；阈值 sweep 用各自阈值下的 verdict 评分。",
            fontsize=9,
            color="#30424f",
        )
        fig.text(
            0.01,
            0.013,
            "数据来源：results/sensitivity/threshold_<T>/cma_hypothesis_verdicts.csv  "
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


def build_cma_sensitivity_forest_plot(
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    thresholds: Sequence[float] = DEFAULT_SENSITIVITY_THRESHOLDS,
    runner: PipelineRunner | None = None,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """High-level convenience: sweep + render in one call.

    Mirrors the single-snapshot ``build_cma_verdicts_forest_plot``
    signature shape so callers can swap the two with minimal friction.
    Tests inject ``runner`` to avoid running the full CMA pipeline four
    times; production callers leave it ``None`` and let the default
    cached orchestrator handle the work.
    """
    sweep_df = build_cma_sensitivity_sweep(
        thresholds=thresholds, runner=runner
    )
    return render_sensitivity_forest_plot(
        sweep_df,
        output_png=output_png_path,
        output_pdf=output_pdf_path,
        title=title,
        generated_on=generated_on,
    )


def build_cma_sensitivity_forest_plot_from_cache(
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    thresholds: Sequence[float] | None = None,
    sensitivity_root: Path | None = None,
    title: str | None = None,
    generated_on: date | None = None,
) -> Path:
    """Render the sensitivity forest from existing cache CSVs only.

    Used by figure / paper re-renderers that must not perform fresh CMA
    threshold runs as a side effect.
    """
    sweep_df = build_cma_sensitivity_sweep_from_cache(
        thresholds=thresholds,
        sensitivity_root=sensitivity_root,
    )
    return render_sensitivity_forest_plot(
        sweep_df,
        output_png=output_png_path,
        output_pdf=output_pdf_path,
        title=title,
        generated_on=generated_on,
    )
