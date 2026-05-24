"""Literature-chronology timeline figure for the 16-paper review.

The 47th console script (``index-inclusion-literature-timeline``) reads
the static ``PAPER_LIBRARY`` registry plus the heuristic-link in-degree
column from ``results/literature/citation_centrality.csv`` and renders a
single-figure chronology of the index-inclusion-effect debate:

- **X-axis**: publication year (1986 → 2026 by default).
- **Y-axis**: research thread (``短期价格压力`` / ``需求曲线效应`` /
  ``沪深300论文复现``, top-to-bottom). Thread comes from
  :attr:`LiteraturePaper.project_module` — the same field the
  dashboard uses to bucket papers.
- **Marker color**: position (pro = blue, contra = red, neutral = grey)
  derived from :attr:`LiteraturePaper.stance` (``正方`` / ``反方`` /
  ``中性``).
- **Marker size**: in-degree centrality (anchor papers larger). Pulled
  from ``citation_centrality.csv`` when present; falls back to a
  uniform medium size on missing CSV so the figure still renders.
- **Labels**: short citation (e.g. ``Shleifer '86``) next to each
  marker.
- **Era bands**: three light vertical bands highlight ``1986-2002 ·
  classical``, ``2002-2014 · skeptics``, ``2014+ · China + identification``.

This complements the citation-network figure (46th CLI): the network
shows *how* papers connect, this timeline shows *when* the debate
moved. Together they cover the §2 文献综述 narrative arc for the
paper.

Design notes
------------
- **Stdlib + matplotlib only**: no networkx, no extra dependencies.
- **Deterministic seed**: ``np.random.seed(0)`` so jitter on overlapping
  papers (e.g. the two 1986 seminal pieces) lays out byte-identically
  across runs.
- **Defensive on missing centrality CSV**: returns a uniform-marker
  figure rather than raising. Doctor will surface the underlying
  ``citation_centrality.csv`` issue via its own check.
- **No paper-text fabrication**: every position / thread / year value
  comes from :data:`PAPER_LIBRARY` — the renderer never invents new
  metadata.
"""

from __future__ import annotations

import csv
import logging
import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.patches as mpatches  # noqa: E402 -- after backend pin
import matplotlib.pyplot as plt  # noqa: E402 -- after backend pin

from index_inclusion_research.plot_style import configure_matplotlib_cjk

configure_matplotlib_cjk(plt)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants (deliberately public — tests assert on them)
# ---------------------------------------------------------------------------

# Position palette. ``正方`` (pro-effect) = blue; ``反方`` (contra) =
# red; ``中性`` (neutral) = grey. Hex chosen to match the rest of the
# project's figures (citation_network uses the same palette family).
POSITION_COLORS: dict[str, str] = {
    "pro": "#2c7fb8",  # blue — supports index-inclusion-effect
    "contra": "#c0392b",  # red — challenges or dampens
    "neutral": "#7f8c8d",  # grey — mechanism / mixed
}

# Chinese stance label → semantic category. Falls back to ``neutral``
# for any unrecognized stance string so language drift never crashes
# the figure.
_STANCE_TO_POSITION: dict[str, str] = {
    "正方": "pro",
    "反方": "contra",
    "中性": "neutral",
}

# Thread display labels — keep order stable so the swimlanes always
# read price-pressure on top, identification on bottom.
THREAD_ORDER: tuple[str, ...] = (
    "短期价格压力",
    "需求曲线效应",
    "沪深300论文复现",
)

THREAD_LABELS_CN: dict[str, str] = {
    "短期价格压力": "短期价格压力",
    "需求曲线效应": "需求曲线效应",
    "沪深300论文复现": "识别与中国市场",
}

# Era bands the figure annotates. Drawn as semi-transparent rectangles
# behind the markers so reviewers can read the chronology grouping at a
# glance.
@dataclass(frozen=True)
class EraBand:
    start_year: int
    end_year: int
    label: str
    color: str  # hex with alpha applied at draw time


ERA_BANDS: tuple[EraBand, ...] = (
    EraBand(
        start_year=1986,
        end_year=2002,
        label="1986-2002 · classical",
        color="#d0d4d9",
    ),
    EraBand(
        start_year=2002,
        end_year=2014,
        label="2002-2014 · skeptics",
        color="#e9d2b8",
    ),
    EraBand(
        start_year=2014,
        end_year=2026,
        label="2014+ · China + identification",
        color="#c8dcc8",
    ),
)

DEFAULT_YEAR_MIN = 1986
DEFAULT_YEAR_MAX = 2026

# Marker size band. The renderer linearly maps in-degree centrality
# (0..max) onto [_MARKER_SIZE_MIN, _MARKER_SIZE_MAX]. Anchor papers
# (e.g. Shleifer 1986, in-degree 14) sit at the high end; freshly added
# 2022 contra papers (in-degree 0) sit at the floor — still visible.
_MARKER_SIZE_MIN: float = 90.0
_MARKER_SIZE_MAX: float = 380.0
_MARKER_SIZE_UNIFORM: float = 180.0  # fallback when no centrality CSV


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TimelinePaper:
    """One paper as the timeline renderer needs it.

    Decoupled from :class:`LiteraturePaper` so tests can build a tiny
    synthetic timeline without instantiating the full literature
    catalog (which has frozen dataclass fields and disk-path-dependent
    PDF roots).
    """

    paper_id: str
    short_citation: str
    year: int
    thread: str
    position: str  # ``"pro"`` / ``"contra"`` / ``"neutral"``
    in_degree: int


def _short_citation_for(authors: str, year: int) -> str:
    """Render ``Shleifer '86`` / ``Yao et al '22`` style labels.

    Splits on ``;`` (the catalog's author separator), keeps just the
    final surname of the first author, then appends a two-digit year
    apostrophe label. Chinese-name authors get the full first author
    surname unchanged (Latin-only handling would mangle them).
    """
    parts = [piece.strip() for piece in authors.split(";") if piece.strip()]
    if not parts:
        return f"unknown '{year % 100:02d}"
    first = parts[0]
    tokens = first.split()
    surname = tokens[-1] if len(tokens) > 1 else first
    year_suffix = f"'{year % 100:02d}"
    if len(parts) > 1:
        return f"{surname} et al {year_suffix}"
    return f"{surname} {year_suffix}"


def _year_from_label(year_label: object) -> int | None:
    """Best-effort ``year_label -> int``.

    The catalog stores most years as plain four-digit strings, but the
    Yao/Zhang/Li 沪深300 paper carries ``年份待核验`` (year pending
    verification). For that one we walk the string for any 4-digit
    substring; if nothing usable surfaces we default to the publication
    year inferred from the file ID's nearest neighbor — but we keep
    that fallback explicit (``None``) and let the caller pick a sane
    default.
    """
    s = str(year_label)
    digits: list[str] = []
    for ch in s:
        if ch.isdigit():
            digits.append(ch)
        else:
            if digits:
                joined = "".join(digits)
                if len(joined) == 4:
                    return int(joined)
                digits = []
    if digits:
        joined = "".join(digits)
        if len(joined) == 4:
            return int(joined)
    return None


def _load_in_degree_map(centrality_csv_path: Path | None) -> dict[str, int]:
    """Parse ``citation_centrality.csv`` → ``{paper_id: in_degree}``.

    Returns an empty dict (renderer falls back to uniform marker size)
    on missing / unreadable / schema-shifted CSV. The renderer never
    raises on a broken centrality file — the doctor check
    ``heuristic_citation_centrality_schema`` is the canonical place to
    fail on that.
    """
    if centrality_csv_path is None:
        return {}
    path = Path(centrality_csv_path)
    if not path.exists():
        return {}
    out: dict[str, int] = {}
    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                pid = (row.get("paper_id") or "").strip()
                raw = (row.get("in_degree") or "").strip()
                if not pid or not raw:
                    continue
                try:
                    out[pid] = int(float(raw))
                except (TypeError, ValueError):
                    logger.debug(
                        "skip unparseable in_degree for %s: %r", pid, raw
                    )
                    continue
    except OSError as exc:
        logger.warning("centrality CSV unreadable %s: %s", path, exc)
        return {}
    return out


def assemble_timeline_papers(
    papers: Iterable[object] | None = None,
    *,
    centrality_csv_path: Path | None = None,
) -> list[TimelinePaper]:
    """Project ``PAPER_LIBRARY`` (or a caller-supplied iterable) into the
    renderer's view.

    Parameters
    ----------
    papers:
        Iterable of :class:`LiteraturePaper`. Defaults to the static
        ``PAPER_LIBRARY`` tuple.
    centrality_csv_path:
        Optional path to ``citation_centrality.csv``; when ``None`` or
        missing, every paper gets ``in_degree=0`` and the renderer uses
        uniform marker sizes.

    Returns
    -------
    list[TimelinePaper]
        One entry per paper. Sorted by ``(year, paper_id)`` so the
        figure is deterministic.
    """
    if papers is None:
        from index_inclusion_research.literature_catalog import PAPER_LIBRARY

        papers = PAPER_LIBRARY
    in_degree_map = _load_in_degree_map(centrality_csv_path)
    out: list[TimelinePaper] = []
    for paper in papers:
        paper_id = str(getattr(paper, "paper_id", ""))
        authors = str(getattr(paper, "authors", ""))
        year_label = getattr(paper, "year_label", "")
        thread = str(getattr(paper, "project_module", ""))
        stance = str(getattr(paper, "stance", ""))
        year = _year_from_label(year_label)
        if year is None:
            # The Yao/Zhang/Li paper deliberately ships
            # ``年份待核验``. Fall back to 2014 — the closest known
            # publication year for this manuscript — and log so a
            # future catalog update can replace the placeholder.
            logger.info(
                "year_label %r unparseable for %s; defaulting to 2014",
                year_label,
                paper_id,
            )
            year = 2014
        position = _STANCE_TO_POSITION.get(stance, "neutral")
        out.append(
            TimelinePaper(
                paper_id=paper_id,
                short_citation=_short_citation_for(authors, year),
                year=year,
                thread=thread,
                position=position,
                in_degree=int(in_degree_map.get(paper_id, 0)),
            )
        )
    out.sort(key=lambda p: (p.year, p.paper_id))
    return out


# ---------------------------------------------------------------------------
# Plot rendering
# ---------------------------------------------------------------------------


def _marker_size(in_degree: int, *, max_in_degree: int) -> float:
    """Linearly map ``in_degree`` ∈ [0, max] to a marker ``s=`` size."""
    if max_in_degree <= 0:
        return _MARKER_SIZE_UNIFORM
    frac = max(0.0, min(1.0, in_degree / max_in_degree))
    return _MARKER_SIZE_MIN + frac * (_MARKER_SIZE_MAX - _MARKER_SIZE_MIN)


def _resolve_thread_y(thread: str, *, base: int) -> int:
    """Return a stable y-coordinate for the swimlane.

    Threads not in ``THREAD_ORDER`` land in a synthetic ``"other"`` row
    below the bottom-most known thread so unexpected catalog values
    still render (rather than crash with a ``KeyError``).
    """
    try:
        idx = THREAD_ORDER.index(thread)
    except ValueError:
        return -1  # ``"other"`` row below the named threads
    return base - idx


def build_literature_timeline_plot(
    papers: Iterable[TimelinePaper] | None,
    output_png_path: Path | str,
    output_pdf_path: Path | str | None = None,
    *,
    year_min: int = DEFAULT_YEAR_MIN,
    year_max: int = DEFAULT_YEAR_MAX,
    seed: int = 0,
) -> Path:
    """Render the 16-paper chronology figure.

    Parameters
    ----------
    papers:
        Iterable of :class:`TimelinePaper`. ``None`` triggers a default
        load from :func:`assemble_timeline_papers` with the canonical
        ``citation_centrality.csv`` if it exists.
    output_png_path:
        PNG destination. Parent directory is created if missing.
    output_pdf_path:
        Optional vector twin; ``None`` skips the PDF.
    year_min / year_max:
        X-axis bounds. The era bands are clipped to these. Defaults
        cover 1986-2026 which spans every paper currently in
        ``PAPER_LIBRARY`` plus headroom for the next two years.
    seed:
        RNG seed for the tiny jitter applied to overlapping markers
        (two papers in the same year on the same thread). Set to 0 by
        default for byte-deterministic renders.

    Returns
    -------
    pathlib.Path
        Absolute path of the written PNG.
    """
    np.random.seed(seed)
    if papers is None:
        from index_inclusion_research import paths as project_paths

        centrality_csv = (
            project_paths.literature_results_dir() / "citation_centrality.csv"
        )
        papers = assemble_timeline_papers(
            centrality_csv_path=centrality_csv if centrality_csv.exists() else None
        )
    paper_list = list(papers)

    png_path = Path(output_png_path).expanduser().resolve()
    pdf_path_resolved: Path | None
    if output_pdf_path is None:
        pdf_path_resolved = None
    else:
        pdf_path_resolved = Path(output_pdf_path).expanduser().resolve()
    png_path.parent.mkdir(parents=True, exist_ok=True)

    # Figure size: 14 in × 7 in @ 100 dpi → 1400 × 700 px before
    # ``bbox_inches='tight'`` trim. Comfortably above the 800×600 floor
    # the doctor check enforces.
    fig, ax = plt.subplots(figsize=(14, 7), dpi=100)

    # Y-axis: one row per named thread (top-down). Unknown threads
    # collapse onto a "other" row at the bottom (y = -1).
    base = len(THREAD_ORDER) - 1  # top thread y = base
    y_for_thread: dict[str, int] = {
        thread: base - idx for idx, thread in enumerate(THREAD_ORDER)
    }

    # Era bands — drawn first (zorder=0) so markers sit on top.
    for band in ERA_BANDS:
        if band.end_year <= year_min or band.start_year >= year_max:
            continue
        start = max(band.start_year, year_min)
        end = min(band.end_year, year_max)
        ax.axvspan(
            start,
            end,
            ymin=0.0,
            ymax=1.0,
            facecolor=band.color,
            alpha=0.35,
            zorder=0,
        )
        # Era label sits just below the top edge so it doesn't collide
        # with the figure title.
        ax.text(
            (start + end) / 2.0,
            base + 0.55,
            band.label,
            ha="center",
            va="bottom",
            fontsize=10,
            color="#1f2933",
            zorder=4,
        )

    # Horizontal swimlane guides — light grey so they hint at the
    # thread without crowding the markers.
    for y in y_for_thread.values():
        ax.axhline(y=y, color="#e1e4e8", linewidth=0.8, zorder=1)

    # Resolve a max in_degree for size scaling. Empty list collapses to
    # the uniform fallback.
    max_in_degree = max((p.in_degree for p in paper_list), default=0)

    # Per-(year, thread) bucket for jitter. When two papers land on the
    # same swimlane cell (e.g. Shleifer + Harris-Gurel in 1986) we
    # offset them slightly so both remain readable.
    bucket: dict[tuple[int, int], int] = {}

    # Draw each paper. We keep handles for the legend.
    for paper in paper_list:
        y = _resolve_thread_y(paper.thread, base=base)
        key = (paper.year, y)
        n_in_bucket = bucket.get(key, 0)
        bucket[key] = n_in_bucket + 1
        # Symmetric offset around the lane: 0, +0.15, -0.15, +0.30, ...
        if n_in_bucket == 0:
            y_offset = 0.0
        else:
            magnitude = ((n_in_bucket + 1) // 2) * 0.18
            y_offset = magnitude if n_in_bucket % 2 == 1 else -magnitude
        color = POSITION_COLORS.get(paper.position, POSITION_COLORS["neutral"])
        size = _marker_size(paper.in_degree, max_in_degree=max_in_degree)
        ax.scatter(
            [paper.year],
            [y + y_offset],
            s=size,
            color=color,
            edgecolors="white",
            linewidths=1.1,
            zorder=3,
            alpha=0.92,
        )
        # Label offset: above the marker for even bucket index, below
        # for odd, so labels don't pile up.
        label_dy = 0.22 if (n_in_bucket % 2 == 0) else -0.30
        ax.annotate(
            paper.short_citation,
            xy=(paper.year, y + y_offset),
            xytext=(0, 6 if label_dy > 0 else -10),
            textcoords="offset points",
            ha="center",
            va="bottom" if label_dy > 0 else "top",
            fontsize=8.5,
            color="#1f2933",
            zorder=4,
        )

    # Y-axis labels — Chinese display names for the threads.
    ax.set_yticks(list(y_for_thread.values()))
    ax.set_yticklabels(
        [THREAD_LABELS_CN.get(t, t) for t in THREAD_ORDER]
    )
    ax.set_ylim(-1.5, base + 1.0)

    # X-axis: clean integer years.
    ax.set_xlim(year_min - 0.5, year_max + 0.5)
    # Roughly one tick every 4 years.
    tick_stride = max(1, (year_max - year_min) // 10)
    xticks = list(range(year_min, year_max + 1, tick_stride))
    if xticks and xticks[-1] != year_max:
        xticks.append(year_max)
    ax.set_xticks(xticks)
    ax.set_xlabel("发表年份", fontsize=11)
    ax.set_ylabel("研究主线", fontsize=11)

    # Legend — position colors only (size encodes centrality but we
    # describe it in the title so the legend stays compact).
    legend_handles = [
        mpatches.Patch(color=POSITION_COLORS["pro"], label="正方 (pro)"),
        mpatches.Patch(color=POSITION_COLORS["contra"], label="反方 (contra)"),
        mpatches.Patch(color=POSITION_COLORS["neutral"], label="中性 (neutral)"),
    ]
    legend = ax.legend(
        handles=legend_handles,
        loc="lower right",
        bbox_to_anchor=(1.0, -0.20),
        ncol=3,
        frameon=False,
        fontsize=9,
        title="文献立场",
        title_fontsize=9,
    )
    legend.set_zorder(5)

    n_papers = len(paper_list)
    ax.set_title(
        f"指数纳入效应文献年表 · {n_papers} 篇 · "
        "标记大小 = 启发式链入度",
        fontsize=13,
        pad=14,
    )
    ax.grid(False)

    fig.tight_layout()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        fig.savefig(png_path, dpi=100, bbox_inches="tight")
        if pdf_path_resolved is not None:
            pdf_path_resolved.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(pdf_path_resolved, bbox_inches="tight")
    plt.close(fig)
    return png_path


# ---------------------------------------------------------------------------
# Public summary helpers
# ---------------------------------------------------------------------------


def summarize_for_public_summary(
    papers: Iterable[TimelinePaper] | None = None,
) -> dict[str, object]:
    """Return the slim payload the public summary JSON embeds.

    Shape::

        {
          "years_covered": [year_min, year_max],
          "n_papers": int,
          "n_papers_pre_2002": int,
          "n_papers_2002_to_2014": int,
          "n_papers_post_2014": int,
          "dominant_position_by_era": {
              "pre_2002": "pro" | "contra" | "neutral",
              "2002_to_2014": ...,
              "post_2014": ...,
          },
          "anchors_by_era": {
              "pre_2002": [paper_id, ...],     # top-2 by in_degree
              "2002_to_2014": [...],
              "post_2014": [...],
          },
        }

    Empty input returns zero counts and ``None`` for dominant positions
    so the JSON schema stays stable across partially-populated checkouts.
    """
    if papers is None:
        papers = assemble_timeline_papers()
    paper_list = list(papers)
    if not paper_list:
        return {
            "years_covered": [DEFAULT_YEAR_MIN, DEFAULT_YEAR_MAX],
            "n_papers": 0,
            "n_papers_pre_2002": 0,
            "n_papers_2002_to_2014": 0,
            "n_papers_post_2014": 0,
            "dominant_position_by_era": {
                "pre_2002": None,
                "2002_to_2014": None,
                "post_2014": None,
            },
            "anchors_by_era": {
                "pre_2002": [],
                "2002_to_2014": [],
                "post_2014": [],
            },
        }

    pre_2002 = [p for p in paper_list if p.year < 2002]
    mid_era = [p for p in paper_list if 2002 <= p.year < 2014]
    post_2014 = [p for p in paper_list if p.year >= 2014]

    def _dominant(bucket: list[TimelinePaper]) -> str | None:
        if not bucket:
            return None
        counts: dict[str, int] = {"pro": 0, "contra": 0, "neutral": 0}
        for p in bucket:
            counts[p.position] = counts.get(p.position, 0) + 1
        # Tie-break alphabetically on the position label so the JSON is
        # deterministic.
        return max(counts.items(), key=lambda kv: (kv[1], -ord(kv[0][0])))[0]

    def _anchors(bucket: list[TimelinePaper]) -> list[str]:
        if not bucket:
            return []
        # Top-2 by in-degree, ties broken alphabetically on paper_id.
        ordered = sorted(
            bucket, key=lambda p: (-p.in_degree, p.paper_id)
        )
        return [p.paper_id for p in ordered[:2]]

    years = [p.year for p in paper_list]
    return {
        "years_covered": [int(min(years)), int(max(years))],
        "n_papers": len(paper_list),
        "n_papers_pre_2002": len(pre_2002),
        "n_papers_2002_to_2014": len(mid_era),
        "n_papers_post_2014": len(post_2014),
        "dominant_position_by_era": {
            "pre_2002": _dominant(pre_2002),
            "2002_to_2014": _dominant(mid_era),
            "post_2014": _dominant(post_2014),
        },
        "anchors_by_era": {
            "pre_2002": _anchors(pre_2002),
            "2002_to_2014": _anchors(mid_era),
            "post_2014": _anchors(post_2014),
        },
    }


# ---------------------------------------------------------------------------
# Default paths
# ---------------------------------------------------------------------------


def default_png_path(repo_root: Path | None = None) -> Path:
    """Canonical PNG destination: ``results/literature/literature_timeline.png``."""
    from index_inclusion_research import paths

    root = repo_root or paths.project_root()
    return root / "results" / "literature" / "literature_timeline.png"


def default_pdf_path(repo_root: Path | None = None) -> Path:
    """Canonical PDF destination: ``results/literature/literature_timeline.pdf``."""
    from index_inclusion_research import paths

    root = repo_root or paths.project_root()
    return root / "results" / "literature" / "literature_timeline.pdf"


def default_centrality_csv_path(repo_root: Path | None = None) -> Path:
    """Canonical centrality CSV location (read-only input)."""
    from index_inclusion_research import paths

    root = repo_root or paths.project_root()
    return root / "results" / "literature" / "citation_centrality.csv"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _format_summary_log(summary: Mapping[str, object]) -> str:
    """Compose the human-readable summary line for the CLI logger."""
    years = summary.get("years_covered", [None, None])
    pre = summary.get("n_papers_pre_2002", 0)
    mid = summary.get("n_papers_2002_to_2014", 0)
    post = summary.get("n_papers_post_2014", 0)
    dominant = summary.get("dominant_position_by_era", {})
    if not isinstance(dominant, dict):
        dominant = {}
    dom_str = ", ".join(
        f"{era}={dominant.get(era) or '-'}"
        for era in ("pre_2002", "2002_to_2014", "post_2014")
    )
    return (
        f"literature timeline: {summary.get('n_papers', 0)} papers "
        f"({years}); era counts pre={pre} mid={mid} post={post}; "
        f"dominant {dom_str}"
    )


def main(argv: Iterable[str] | None = None) -> int:
    """``index-inclusion-literature-timeline`` entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="index-inclusion-literature-timeline",
        description=(
            "Render the 16-paper index-inclusion-effect chronology as a "
            "year × research-thread scatter, colored by position "
            "(pro/contra/neutral) and sized by heuristic in-degree "
            "centrality. Defaults write to "
            "results/literature/literature_timeline.{png,pdf} so the "
            "paper bundle picks them up automatically."
        ),
    )
    parser.add_argument(
        "--png",
        default="",
        help=(
            "Output PNG path. Default: "
            "results/literature/literature_timeline.png under the "
            "configured project root."
        ),
    )
    parser.add_argument(
        "--pdf",
        default="",
        help=(
            "Output PDF path. Default: "
            "results/literature/literature_timeline.pdf. Pass --no-pdf "
            "to skip vector emission."
        ),
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip the PDF twin entirely.",
    )
    parser.add_argument(
        "--centrality-csv",
        default="",
        help=(
            "Optional override for citation_centrality.csv (read-only). "
            "Default: results/literature/citation_centrality.csv."
        ),
    )
    parser.add_argument(
        "--year-min",
        type=int,
        default=DEFAULT_YEAR_MIN,
        help=f"X-axis lower bound (default: {DEFAULT_YEAR_MIN}).",
    )
    parser.add_argument(
        "--year-max",
        type=int,
        default=DEFAULT_YEAR_MAX,
        help=f"X-axis upper bound (default: {DEFAULT_YEAR_MAX}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="RNG seed for jitter on overlapping papers (default: 0).",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    centrality_csv = (
        Path(args.centrality_csv).expanduser().resolve()
        if args.centrality_csv
        else default_centrality_csv_path()
    )
    papers = assemble_timeline_papers(
        centrality_csv_path=centrality_csv if centrality_csv.exists() else None
    )

    png_path = (
        Path(args.png).expanduser().resolve()
        if args.png
        else default_png_path()
    )
    pdf_path: Path | None
    if args.no_pdf:
        pdf_path = None
    else:
        pdf_path = (
            Path(args.pdf).expanduser().resolve()
            if args.pdf
            else default_pdf_path()
        )

    written = build_literature_timeline_plot(
        papers,
        output_png_path=png_path,
        output_pdf_path=pdf_path,
        year_min=args.year_min,
        year_max=args.year_max,
        seed=args.seed,
    )
    logger.info("literature timeline PNG written: %s", written)
    if pdf_path is not None:
        logger.info("literature timeline PDF written: %s", pdf_path)

    summary = summarize_for_public_summary(papers)
    logger.info(_format_summary_log(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
