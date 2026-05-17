"""Heuristic literature-link network analyzer for the 16-paper library.

The 39th console script (``index-inclusion-citation-graph``) maps a
within-library **heuristic** relationship graph, computes centrality, and
renders a publication-grade figure plus a structured CSV that the paper's
literature-review section (§References) can lean on. The edges are not
claimed to be bibliography-verified citations; they are transparent
similarity/context links curated from year, topic, and methodology.

Inputs
------
The graph is built from ``literature_catalog.PAPER_LIBRARY``. Each
:class:`~index_inclusion_research.literature_catalog.LiteraturePaper`
carries a ``related_paper_ids`` tuple listing other ``paper_id`` s it is
heuristically related to within the indexed set. ``linked_by`` is derived
from the inverse map (see :func:`literature_catalog.build_linked_by_map`).

Outputs
-------
- ``results/literature/citation_network.png`` — force-directed-ish layout
  (deterministic seed), nodes sized by heuristic in-degree and colored by
  stance, edges drawn as arrows.
- ``results/literature/citation_network.pdf`` — vector twin of the PNG.
- ``results/literature/citation_centrality.csv`` — one row per paper
  with in-degree, out-degree, betweenness (BFS-approximate), eigenvector
  centrality, top-linked-by, top-links-to.

Design notes
------------
- **No new heavy dependencies**: only ``matplotlib`` + stdlib. The
  layout is a small Fruchterman-Reingold-style spring simulation seeded
  by ``numpy.random.default_rng(0)`` so the figure is reproducible.
- **Centrality without networkx**: in-degree and out-degree are direct
  edge counts; betweenness uses Brandes' BFS-based algorithm spelled
  out by hand; eigenvector centrality runs a few power-iteration steps
  on the unweighted adjacency matrix.
- **Stable ordering**: every dict / sort uses ``paper_id`` as the
  secondary key so figure layout and CSV rows are deterministic across
  runs.
"""

from __future__ import annotations

import csv
import logging
import math
import warnings
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import matplotlib

from index_inclusion_research import paths
from index_inclusion_research.literature_catalog import (
    PAPER_LIBRARY,
    LiteraturePaper,
    build_linked_by_map,
)

matplotlib.use("Agg")
import matplotlib.patches as mpatches  # noqa: E402 -- after backend pin
import matplotlib.pyplot as plt  # noqa: E402 -- after backend pin

plt.rcParams["font.sans-serif"] = [
    "Songti SC",
    "STHeiti",
    "Arial Unicode MS",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False

logger = logging.getLogger(__name__)


# Stance-to-color palette mirrors the rest of the project's literature
# pages so reviewers can compare visuals across tracks without
# recalibrating their eyes.
_STANCE_COLORS: dict[str, str] = {
    "反方": "#c0392b",  # crimson — challenges permanent revaluation
    "中性": "#7f8c8d",  # slate — mechanism / friction discussion
    "正方": "#1f6f8b",  # teal — supports downward-sloping demand curve
}
_DEFAULT_NODE_COLOR = "#5c6b77"

# Node size range for the figure. In-degree=0 gets the floor (so isolated
# nodes still render); the most-linked paper gets the ceiling.
_NODE_SIZE_FLOOR = 320.0
_NODE_SIZE_CEIL = 2000.0


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CitationGraph:
    """Frozen, deterministic view of the heuristic literature-link network.

    Attributes
    ----------
    nodes:
        Ordered tuple of ``paper_id`` strings (sort order matches
        ``PAPER_LIBRARY``).
    edges:
        Tuple of ``(source_paper_id, target_paper_id)`` pairs, source links
        to target. Drained from each ``LiteraturePaper.related_paper_ids``.
    papers_by_id:
        Lookup dict so renderers can ask for the full
        :class:`LiteraturePaper` without re-walking the library.
    """

    nodes: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]
    papers_by_id: dict[str, LiteraturePaper] = field(default_factory=dict)


@dataclass(frozen=True)
class CentralityMetrics:
    """One row of centrality results for a single paper."""

    paper_id: str
    in_degree: int
    out_degree: int
    betweenness: float
    eigenvector: float
    linked_by: tuple[str, ...]
    links_to: tuple[str, ...]


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


def build_citation_graph(
    papers: Iterable[LiteraturePaper] | None = None,
) -> CitationGraph:
    """Build a :class:`CitationGraph` from ``PAPER_LIBRARY`` (default) or
    a caller-supplied iterable.

    Edges with a source or target outside the supplied node set are
    silently dropped (defensive: keeps the graph well-defined for tests
    that pass a subset). A warning is logged when this happens so the
    caller can notice fabricated / orphan ``paper_id`` s.
    """
    paper_tuple: tuple[LiteraturePaper, ...] = (
        tuple(papers) if papers is not None else tuple(PAPER_LIBRARY)
    )
    nodes = tuple(p.paper_id for p in paper_tuple)
    node_set = set(nodes)
    edges: list[tuple[str, str]] = []
    for paper in paper_tuple:
        for target in paper.related_paper_ids:
            if target not in node_set:
                logger.warning(
                    "literature link %s -> %s targets a paper outside the supplied "
                    "node set; dropping edge.",
                    paper.paper_id,
                    target,
                )
                continue
            edges.append((paper.paper_id, target))
    papers_by_id = {p.paper_id: p for p in paper_tuple}
    return CitationGraph(
        nodes=nodes, edges=tuple(edges), papers_by_id=papers_by_id
    )


# ---------------------------------------------------------------------------
# Centrality
# ---------------------------------------------------------------------------


def _build_adjacency(graph: CitationGraph) -> dict[str, tuple[str, ...]]:
    """Forward adjacency: source -> tuple of targets it links_to."""
    out: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for src, dst in graph.edges:
        out[src].append(dst)
    return {k: tuple(v) for k, v in out.items()}


def _build_reverse_adjacency(
    graph: CitationGraph,
) -> dict[str, tuple[str, ...]]:
    """Reverse adjacency: target -> tuple of sources that cite it."""
    rev: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for src, dst in graph.edges:
        rev[dst].append(src)
    return {k: tuple(v) for k, v in rev.items()}


def _betweenness_centrality(graph: CitationGraph) -> dict[str, float]:
    """Brandes' BFS algorithm on the undirected projection.

    The heuristic literature-link graph is by construction acyclic (forward-in-time), so
    a directed betweenness centrality would heavily favor the seminal
    pair. For a thesis-defense visual the more useful quantity is the
    "bridge" measure — which papers sit on shortest paths between
    others — which is the undirected projection's betweenness. Returns
    normalized scores (divided by the standard (n-1)(n-2)/2 factor for
    n>=3, zero otherwise).
    """
    nodes = list(graph.nodes)
    n = len(nodes)
    if n < 3:
        return {node: 0.0 for node in nodes}

    # Undirected neighbor map
    neighbors: dict[str, set[str]] = {node: set() for node in nodes}
    for src, dst in graph.edges:
        if src == dst:
            continue
        neighbors[src].add(dst)
        neighbors[dst].add(src)
    neighbor_lists = {k: sorted(v) for k, v in neighbors.items()}

    betweenness: dict[str, float] = {node: 0.0 for node in nodes}
    for source in nodes:
        # Single-source shortest-paths BFS
        stack: list[str] = []
        predecessors: dict[str, list[str]] = {node: [] for node in nodes}
        sigma: dict[str, float] = {node: 0.0 for node in nodes}
        sigma[source] = 1.0
        distance: dict[str, int] = {node: -1 for node in nodes}
        distance[source] = 0
        queue: deque[str] = deque([source])
        while queue:
            v = queue.popleft()
            stack.append(v)
            for w in neighbor_lists[v]:
                if distance[w] < 0:
                    queue.append(w)
                    distance[w] = distance[v] + 1
                if distance[w] == distance[v] + 1:
                    sigma[w] += sigma[v]
                    predecessors[w].append(v)
        # Dependency accumulation (Brandes)
        delta: dict[str, float] = {node: 0.0 for node in nodes}
        while stack:
            w = stack.pop()
            for v in predecessors[w]:
                if sigma[w] == 0.0:
                    continue
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w])
            if w != source:
                betweenness[w] += delta[w]

    # Each unordered pair is counted twice in an undirected graph.
    norm = 1.0 / ((n - 1) * (n - 2))
    for node in nodes:
        betweenness[node] *= norm
    return betweenness


def _eigenvector_centrality(
    graph: CitationGraph,
    *,
    iterations: int = 200,
    tolerance: float = 1e-8,
    damping: float = 0.85,
) -> dict[str, float]:
    """PageRank-flavored eigenvector centrality, damped power iteration.

    The pure ``A x = lambda x`` iteration collapses to 0 on a citation
    DAG: every "important" node (highly-linked seminal piece) has no
    outgoing edges, so the iterative product zeroes itself out. We
    instead solve the more useful fixed-point::

        x[node] = (1 - d) / n + d * sum(x[citer] / out_degree[citer]
                                        for citer in linked_by[node])

    with ``d=0.85`` — the classical PageRank damping. This produces a
    proper stationary distribution: highly-linked papers accumulate
    score from many citers, and the (1 - d)/n teleport term avoids
    sinks zeroing the whole graph. Then L2-normalize so values are
    directly comparable to the in-degree heatmap. Iterates 200 times
    (overkill for n=16; converges in <50) with an early-stop tolerance.

    Returns a dict ``paper_id -> centrality`` with values in [0, 1].
    """
    nodes = list(graph.nodes)
    n = len(nodes)
    if n == 0:
        return {}
    reverse = _build_reverse_adjacency(graph)
    forward = _build_adjacency(graph)
    out_degree = {node: len(forward[node]) for node in nodes}

    score = {node: 1.0 / n for node in nodes}
    teleport = (1.0 - damping) / n
    for _ in range(iterations):
        new_score: dict[str, float] = {node: teleport for node in nodes}
        # Dangling-node mass redistribution: papers with no outgoing
        # citations would otherwise leak probability; redistribute their
        # mass uniformly each step (classical PageRank trick).
        dangling_mass = sum(
            score[node] for node in nodes if out_degree[node] == 0
        )
        dangling_redistribution = damping * dangling_mass / n
        for node in nodes:
            new_score[node] += dangling_redistribution
            for citer in reverse[node]:
                if out_degree[citer] > 0:
                    new_score[node] += damping * score[citer] / out_degree[citer]
        # Check convergence on L1 norm
        diff = sum(abs(new_score[node] - score[node]) for node in nodes)
        score = new_score
        if diff < tolerance:
            break

    # L2-normalize so the returned values are unit-norm — matches the
    # "centrality scores sum to expected totals" convention used by the
    # paired tests.
    norm = math.sqrt(sum(v * v for v in score.values()))
    if norm == 0.0:
        return {node: 0.0 for node in nodes}
    return {node: score[node] / norm for node in nodes}


def compute_centrality(graph: CitationGraph) -> dict[str, CentralityMetrics]:
    """Compute the four centrality measures for every node.

    Returns ``{paper_id: CentralityMetrics(...)}``. Centrality dicts are
    deterministic given a fixed ``graph``.
    """
    forward = _build_adjacency(graph)
    reverse = _build_reverse_adjacency(graph)
    betweenness = _betweenness_centrality(graph)
    eigenvector = _eigenvector_centrality(graph)
    out: dict[str, CentralityMetrics] = {}
    for node in graph.nodes:
        out[node] = CentralityMetrics(
            paper_id=node,
            in_degree=len(reverse[node]),
            out_degree=len(forward[node]),
            betweenness=float(betweenness.get(node, 0.0)),
            eigenvector=float(eigenvector.get(node, 0.0)),
            linked_by=tuple(reverse[node]),
            links_to=tuple(forward[node]),
        )
    return out


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------


def _spring_layout(
    graph: CitationGraph,
    *,
    iterations: int = 200,
    seed: int = 0,
) -> dict[str, tuple[float, float]]:
    """Deterministic Fruchterman-Reingold-style layout.

    Pure-stdlib implementation: avoids the optional networkx dependency
    and produces a stable layout given a fixed ``seed`` (default 0).
    Coordinates land in [-1.0, 1.0] for both axes.
    """
    import random

    rng = random.Random(seed)
    nodes = list(graph.nodes)
    n = len(nodes)
    if n == 0:
        return {}
    if n == 1:
        return {nodes[0]: (0.0, 0.0)}

    # Initial random positions on the unit disk
    positions = {
        node: (rng.uniform(-1.0, 1.0), rng.uniform(-1.0, 1.0)) for node in nodes
    }

    # Undirected edge list for the spring forces
    edge_set: set[tuple[str, str]] = set()
    for src, dst in graph.edges:
        if src == dst:
            continue
        edge_set.add(tuple(sorted((src, dst))))  # type: ignore[arg-type]

    # Force-directed parameters
    area = 1.0
    k = math.sqrt(area / n)  # natural spring length
    temperature = 0.1

    for _step in range(iterations):
        displacement = {node: [0.0, 0.0] for node in nodes}
        # Repulsion: every pair pushes apart
        for i in range(n):
            for j in range(i + 1, n):
                a, b = nodes[i], nodes[j]
                dx = positions[a][0] - positions[b][0]
                dy = positions[a][1] - positions[b][1]
                dist2 = dx * dx + dy * dy
                if dist2 == 0.0:
                    dx, dy = rng.uniform(-0.01, 0.01), rng.uniform(-0.01, 0.01)
                    dist2 = dx * dx + dy * dy
                dist = math.sqrt(dist2)
                rep = k * k / dist
                displacement[a][0] += dx / dist * rep
                displacement[a][1] += dy / dist * rep
                displacement[b][0] -= dx / dist * rep
                displacement[b][1] -= dy / dist * rep

        # Attraction: edges pull endpoints together
        for a, b in edge_set:
            dx = positions[a][0] - positions[b][0]
            dy = positions[a][1] - positions[b][1]
            dist = math.sqrt(dx * dx + dy * dy) or 1e-9
            att = dist * dist / k
            displacement[a][0] -= dx / dist * att
            displacement[a][1] -= dy / dist * att
            displacement[b][0] += dx / dist * att
            displacement[b][1] += dy / dist * att

        # Update positions, clipped to [-1.0, 1.0]
        for node in nodes:
            dx, dy = displacement[node]
            mag = math.sqrt(dx * dx + dy * dy) or 1e-9
            move = min(mag, temperature)
            new_x = positions[node][0] + (dx / mag) * move
            new_y = positions[node][1] + (dy / mag) * move
            new_x = max(-1.0, min(1.0, new_x))
            new_y = max(-1.0, min(1.0, new_y))
            positions[node] = (new_x, new_y)
        # Linear cooling schedule
        temperature = max(0.005, temperature * 0.98)

    return positions


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _short_label(paper: LiteraturePaper) -> str:
    """Compact author-year label, e.g. ``Wurgler '02`` or ``姚等 '22``."""
    if not paper.authors.strip():
        return paper.paper_id
    first = paper.authors.split(";")[0].strip()
    family = first.split()[-1] if " " in first else first
    # Trim year to last two digits
    year = "".join(ch for ch in paper.year_label if ch.isdigit())
    short_year = year[-2:] if len(year) >= 2 else year
    # Multi-author? add 等
    n_authors = len([a for a in paper.authors.split(";") if a.strip()])
    suffix = " 等" if n_authors >= 3 else ""
    if short_year:
        return f"{family}{suffix} '{short_year}"
    return f"{family}{suffix}"


def render_citation_network_plot(
    graph: CitationGraph,
    output_png_path: str | Path,
    output_pdf_path: str | Path | None = None,
    *,
    seed: int = 0,
) -> Path:
    """Render the heuristic literature-link network figure and return the PNG path.

    Parameters
    ----------
    graph:
        :class:`CitationGraph` to visualize.
    output_png_path:
        Mandatory PNG destination. Parent dir is created if missing.
    output_pdf_path:
        Optional PDF twin (vector). Pass ``None`` to skip.
    seed:
        Layout seed; the default ``0`` produces a stable figure that
        matches the doctor / paper-bundle freshness checks.

    Returns
    -------
    pathlib.Path
        Absolute path of the written PNG.
    """
    png_path = Path(output_png_path).expanduser().resolve()
    png_path.parent.mkdir(parents=True, exist_ok=True)

    centrality = compute_centrality(graph)
    positions = _spring_layout(graph, seed=seed)
    max_in_degree = max((m.in_degree for m in centrality.values()), default=0)

    # 12x10 in @ dpi=100 -> 1200x1000 px before bbox_inches='tight' trims.
    # Generous floor keeps the post-trim PNG above the 800x600 contract.
    fig, ax = plt.subplots(figsize=(12, 10), dpi=100)
    ax.set_xlim(-1.25, 1.25)
    ax.set_ylim(-1.25, 1.25)
    ax.set_aspect("equal")
    ax.set_axis_off()

    # Edges first so nodes draw over them. Use FancyArrowPatch for arrows
    # rather than ax.annotate (faster batched draw + cleaner head styling).
    for src, dst in graph.edges:
        if src not in positions or dst not in positions:
            continue
        x0, y0 = positions[src]
        x1, y1 = positions[dst]
        ax.annotate(
            "",
            xy=(x1, y1),
            xytext=(x0, y0),
            arrowprops=dict(
                arrowstyle="-|>",
                color="#9aa5ad",
                lw=0.7,
                alpha=0.55,
                shrinkA=10,
                shrinkB=10,
            ),
            zorder=1,
        )

    # Nodes
    for node in graph.nodes:
        paper = graph.papers_by_id.get(node)
        if paper is None or node not in positions:
            continue
        x, y = positions[node]
        in_deg = centrality[node].in_degree
        size = _NODE_SIZE_FLOOR + (
            (_NODE_SIZE_CEIL - _NODE_SIZE_FLOOR)
            * (in_deg / max_in_degree if max_in_degree > 0 else 0.0)
        )
        color = _STANCE_COLORS.get(paper.stance, _DEFAULT_NODE_COLOR)
        ax.scatter(
            [x],
            [y],
            s=size,
            color=color,
            edgecolors="white",
            linewidths=1.2,
            zorder=2,
            alpha=0.9,
        )
        ax.text(
            x,
            y - 0.075,
            _short_label(paper),
            ha="center",
            va="top",
            fontsize=8,
            color="#1f2933",
            zorder=3,
        )

    # Legend keyed on stance
    legend_handles = [
        mpatches.Patch(color=_STANCE_COLORS[stance], label=stance)
        for stance in ("反方", "中性", "正方")
    ]
    legend = ax.legend(
        handles=legend_handles,
        loc="upper left",
        bbox_to_anchor=(0.0, 1.0),
        frameon=False,
        fontsize=9,
        title="文献立场",
        title_fontsize=9,
    )
    legend.set_zorder(5)

    n_edges = len(graph.edges)
    n_nodes = len(graph.nodes)
    ax.set_title(
        f"指数纳入启发式文献关联网络（{n_nodes} 篇 · {n_edges} 条关联边）",
        fontsize=12,
        pad=14,
    )
    ax.text(
        0.5,
        -0.04,
        "节点大小 = 启发式关联入度（in-degree），颜色 = 立场，箭头方向 = 关联对象。",
        transform=ax.transAxes,
        ha="center",
        va="top",
        fontsize=8,
        color="#5c6b77",
    )

    fig.tight_layout()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        fig.savefig(png_path, dpi=100, bbox_inches="tight")
        if output_pdf_path is not None:
            pdf_path = Path(output_pdf_path).expanduser().resolve()
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------


def _stringify_paper_ids(ids: Iterable[str], limit: int = 3) -> str:
    """Pipe-joined string of up to ``limit`` paper_ids; empty -> ``""``."""
    items = list(ids)[:limit]
    return "|".join(items)


def write_centrality_csv(
    graph: CitationGraph,
    output_csv_path: str | Path,
    *,
    centrality: dict[str, CentralityMetrics] | None = None,
) -> Path:
    """Persist the centrality table as CSV; return the resolved path.

    Columns: ``paper_id, in_degree, out_degree, betweenness, eigenvector,
    top_linked_by, top_links_to``. ``top_*`` are pipe-joined truncated
    paper_id lists (up to 3 each) so the CSV stays single-cell-friendly
    when pasted into a spreadsheet.
    """
    csv_path = Path(output_csv_path).expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if centrality is None:
        centrality = compute_centrality(graph)
    fieldnames = [
        "paper_id",
        "in_degree",
        "out_degree",
        "betweenness",
        "eigenvector",
        "top_linked_by",
        "top_links_to",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for node in graph.nodes:
            metrics = centrality[node]
            writer.writerow(
                {
                    "paper_id": metrics.paper_id,
                    "in_degree": metrics.in_degree,
                    "out_degree": metrics.out_degree,
                    "betweenness": f"{metrics.betweenness:.6f}",
                    "eigenvector": f"{metrics.eigenvector:.6f}",
                    "top_linked_by": _stringify_paper_ids(metrics.linked_by),
                    "top_links_to": _stringify_paper_ids(metrics.links_to),
                }
            )
    return csv_path


# ---------------------------------------------------------------------------
# Default paths + public summary helpers
# ---------------------------------------------------------------------------


def default_png_path() -> Path:
    return paths.literature_results_dir() / "citation_network.png"


def default_pdf_path() -> Path:
    return paths.literature_results_dir() / "citation_network.pdf"


def default_csv_path() -> Path:
    return paths.literature_results_dir() / "citation_centrality.csv"


def summarize_for_public_summary(
    graph: CitationGraph,
    centrality: dict[str, CentralityMetrics] | None = None,
    *,
    top_n: int = 3,
) -> dict[str, object]:
    """Return the slim payload the public summary embeds.

    Shape::

        {
          "edge_count": int,
          "node_count": int,
          "top_3_most_linked": [paper_id, ...],     # by in_degree, desc
          "top_3_central_papers": [paper_id, ...], # by eigenvector, desc
        }

    Ties are broken by ``paper_id`` alphabetically so the output is
    deterministic across runs.
    """
    if centrality is None:
        centrality = compute_centrality(graph)
    most_linked = sorted(
        centrality.values(),
        key=lambda m: (-m.in_degree, m.paper_id),
    )
    most_central = sorted(
        centrality.values(),
        key=lambda m: (-m.eigenvector, m.paper_id),
    )
    return {
        "edge_semantics": "heuristic_similarity_not_bibliographic_citation",
        "edge_count": len(graph.edges),
        "node_count": len(graph.nodes),
        "top_3_most_linked": [m.paper_id for m in most_linked[:top_n]],
        "top_3_central_papers": [m.paper_id for m in most_central[:top_n]],
    }


def summarize_for_paper_skeleton(
    graph: CitationGraph,
    centrality: dict[str, CentralityMetrics] | None = None,
) -> dict[str, object]:
    """Per-summary block used by the §References auto-sentence.

    Returns ``{edge_count, node_count, most_linked_label,
    bridge_papers_label}``: pre-formatted strings the jinja template
    can drop in without further processing.
    """
    if centrality is None:
        centrality = compute_centrality(graph)
    most_linked = sorted(
        centrality.values(),
        key=lambda m: (-m.in_degree, m.paper_id),
    )
    bridges = sorted(
        centrality.values(),
        key=lambda m: (-m.betweenness, m.paper_id),
    )

    def _label(metrics: CentralityMetrics) -> str:
        paper = graph.papers_by_id.get(metrics.paper_id)
        if paper is None:
            return metrics.paper_id
        return _short_label(paper)

    return {
        "edge_count": len(graph.edges),
        "node_count": len(graph.nodes),
        "most_linked_label": "、".join(_label(m) for m in most_linked[:3]),
        "bridge_papers_label": "、".join(_label(m) for m in bridges[:3]),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """``index-inclusion-citation-graph`` entry point.

    Reads ``PAPER_LIBRARY``, computes centrality, renders the figure
    twin (PNG + PDF) and the centrality CSV. Idempotent — re-running
    overwrites the artifacts.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="index-inclusion-citation-graph",
        description=(
            "Analyze the 16-paper heuristic literature-link network: "
            "compute centrality, render a force-directed PNG + PDF figure, "
            "and emit a structured CSV (paper_id, in_degree, out_degree, "
            "betweenness, eigenvector, top_linked_by, top_links_to)."
        ),
    )
    parser.add_argument(
        "--png",
        default=str(default_png_path()),
        help="Output PNG path (default: results/literature/citation_network.png).",
    )
    parser.add_argument(
        "--pdf",
        default=str(default_pdf_path()),
        help=(
            "Output PDF path (default: results/literature/citation_network.pdf). "
            "Pass an empty string to skip PDF generation."
        ),
    )
    parser.add_argument(
        "--csv",
        default=str(default_csv_path()),
        help="Output CSV path (default: results/literature/citation_centrality.csv).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Layout seed (default 0 — produces the canonical figure).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    graph = build_citation_graph()
    centrality = compute_centrality(graph)
    pdf_target = args.pdf.strip() or None
    png_written = render_citation_network_plot(
        graph,
        output_png_path=args.png,
        output_pdf_path=pdf_target,
        seed=args.seed,
    )
    logger.info("heuristic literature network PNG written: %s", png_written)
    if pdf_target:
        logger.info("heuristic literature network PDF written: %s", pdf_target)
    csv_written = write_centrality_csv(
        graph, output_csv_path=args.csv, centrality=centrality
    )
    logger.info("heuristic literature-link centrality CSV written: %s", csv_written)

    summary = summarize_for_public_summary(graph, centrality=centrality)
    logger.info(
        "heuristic literature-link graph summary: %d nodes, %d edges; top-linked: %s; "
        "top-central: %s",
        summary["node_count"],
        summary["edge_count"],
        ",".join(summary["top_3_most_linked"]),  # type: ignore[arg-type]
        ",".join(summary["top_3_central_papers"]),  # type: ignore[arg-type]
    )
    # Avoid unused-import warning for build_linked_by_map under strict mypy
    _ = build_linked_by_map
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
