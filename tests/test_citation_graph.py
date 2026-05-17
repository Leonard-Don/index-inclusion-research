"""Tests for the heuristic literature-link network helper."""

from __future__ import annotations

import csv
import math
import struct
import zlib
from pathlib import Path
from typing import cast

from index_inclusion_research.citation_graph import (
    build_citation_graph,
    compute_centrality,
    render_citation_network_plot,
    summarize_for_paper_skeleton,
    summarize_for_public_summary,
    write_centrality_csv,
)
from index_inclusion_research.literature_catalog import (
    PAPER_LIBRARY,
    build_linked_by_map,
)


def _png_dimensions(png_path: Path) -> tuple[int, int]:
    """Return ``(width, height)`` of a PNG by parsing the IHDR chunk.

    Avoids a Pillow dependency: PNG signature is 8 bytes, IHDR starts at
    offset 8 (4-byte length, 4-byte type ``IHDR``, then 4-byte width,
    4-byte height, big-endian). Validates the signature + CRC implicitly
    by reading just enough bytes.
    """
    data = png_path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "not a PNG file"
    # IHDR chunk: bytes 8..16 = length+type; bytes 16..24 = width+height
    width, height = struct.unpack(">II", data[16:24])
    # Sanity: CRC of (type+data) at bytes 24..29 should match
    ihdr_crc_expected = struct.unpack(">I", data[29:33])[0]
    ihdr_crc_actual = zlib.crc32(data[12:29])
    assert ihdr_crc_expected == ihdr_crc_actual, "PNG IHDR CRC mismatch"
    return width, height


def test_build_citation_graph_is_closed_over_library() -> None:
    graph = build_citation_graph()
    node_set = set(graph.nodes)

    assert len(graph.nodes) == len(PAPER_LIBRARY) == 16
    assert graph.edges
    assert all(src in node_set and dst in node_set for src, dst in graph.edges)

    linked_by = build_linked_by_map()
    assert set(linked_by) == node_set
    for src, dst in graph.edges:
        assert src in linked_by[dst]


def test_no_orphan_edges_in_paper_library() -> None:
    """Every ``related_paper_ids`` entry must point at another indexed paper.

    Guards against typos in the literature catalog itself — an orphan
    target would silently drop in :func:`build_citation_graph` (logged
    as a warning), so the catalog-level check is stricter.
    """
    valid_ids = {paper.paper_id for paper in PAPER_LIBRARY}
    orphans: list[tuple[str, str]] = []
    for paper in PAPER_LIBRARY:
        for target in paper.related_paper_ids:
            if target not in valid_ids:
                orphans.append((paper.paper_id, target))
            assert target != paper.paper_id, (
                f"{paper.paper_id} links to itself; self-loops are not allowed"
            )
    assert orphans == [], f"orphan links in literature catalog: {orphans}"


def test_edge_count_in_expected_range() -> None:
    """The heuristic link graph should sit in the 30-60 edge band.

    Below 30 means the literature catalog has lost links (likely a
    refactor regression); above 60 means we're over-linking and the
    figure becomes unreadable. The current ground truth is 52 edges —
    keep the band wide enough for organic catalog growth without
    silently letting it explode.
    """
    graph = build_citation_graph()
    assert 30 <= len(graph.edges) <= 60, (
        f"unexpected edge count: {len(graph.edges)} (expected 30 <= n <= 60)"
    )


def test_centrality_and_summaries_are_deterministic() -> None:
    graph = build_citation_graph()
    centrality = compute_centrality(graph)

    assert set(centrality) == set(graph.nodes)
    assert centrality["shleifer_1986"].in_degree > 0
    assert centrality["harris_gurel_1986"].in_degree > 0
    eigen_norm = math.sqrt(sum(row.eigenvector**2 for row in centrality.values()))
    assert 0.99 <= eigen_norm <= 1.01

    public_summary = summarize_for_public_summary(graph, centrality=centrality)
    assert public_summary["node_count"] == len(graph.nodes)
    assert public_summary["edge_count"] == len(graph.edges)
    assert public_summary["edge_semantics"] == "heuristic_similarity_not_bibliographic_citation"
    assert len(cast(list[str], public_summary["top_3_most_linked"])) == 3
    assert len(cast(list[str], public_summary["top_3_central_papers"])) == 3

    skeleton_summary = summarize_for_paper_skeleton(graph, centrality=centrality)
    assert skeleton_summary["node_count"] == len(graph.nodes)
    assert skeleton_summary["edge_count"] == len(graph.edges)
    assert skeleton_summary["most_linked_label"]
    assert skeleton_summary["bridge_papers_label"]


def test_centrality_scores_are_valid() -> None:
    """Every centrality scalar must be non-negative and finite.

    Eigenvector centrality is unit-L2 normalized so any single score
    lives in [0, 1]; betweenness is normalized by (n-1)(n-2) so it lives
    in [0, 1] too. In-degree and out-degree are non-negative integers
    bounded above by ``n - 1``.
    """
    graph = build_citation_graph()
    centrality = compute_centrality(graph)
    n = len(graph.nodes)

    for paper_id, metrics in centrality.items():
        assert metrics.paper_id == paper_id
        assert metrics.in_degree >= 0
        assert metrics.out_degree >= 0
        assert metrics.in_degree <= n - 1
        assert metrics.out_degree <= n - 1
        assert metrics.betweenness >= 0.0
        assert metrics.betweenness <= 1.0 + 1e-9
        assert metrics.eigenvector >= 0.0
        assert metrics.eigenvector <= 1.0 + 1e-9
        assert math.isfinite(metrics.betweenness)
        assert math.isfinite(metrics.eigenvector)

    # Sum of in-degrees must equal sum of out-degrees must equal edge count
    total_in = sum(m.in_degree for m in centrality.values())
    total_out = sum(m.out_degree for m in centrality.values())
    assert total_in == total_out == len(graph.edges)


def test_write_centrality_csv(tmp_path: Path) -> None:
    graph = build_citation_graph()
    csv_path = write_centrality_csv(graph, tmp_path / "centrality.csv")

    rows = list(csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines()))
    # CSV must have exactly 16 rows — one per paper in PAPER_LIBRARY.
    assert len(rows) == 16
    assert len(rows) == len(graph.nodes)
    assert set(rows[0]) == {
        "paper_id",
        "in_degree",
        "out_degree",
        "betweenness",
        "eigenvector",
        "top_linked_by",
        "top_links_to",
    }
    assert any(row["paper_id"] == "shleifer_1986" for row in rows)
    # Every row's paper_id must come from PAPER_LIBRARY (no fabricated rows)
    valid_ids = {paper.paper_id for paper in PAPER_LIBRARY}
    assert all(row["paper_id"] in valid_ids for row in rows)


def test_render_citation_network_plot_writes_png_and_pdf(tmp_path: Path) -> None:
    graph = build_citation_graph()
    png = tmp_path / "citation_network.png"
    pdf = tmp_path / "citation_network.pdf"

    returned = render_citation_network_plot(graph, png, pdf, seed=0)

    assert returned == png.resolve()
    assert png.exists() and png.stat().st_size > 5_000
    assert pdf.exists() and pdf.stat().st_size > 5_000


def test_rendered_png_is_at_least_800x600(tmp_path: Path) -> None:
    """The PNG must be at least 800×600 pixels for legibility.

    Smaller frames lose node labels and edge-arrow heads; this is the
    contract the dashboard and paper-bundle freshness checks rely on.
    """
    graph = build_citation_graph()
    png = tmp_path / "citation_network.png"
    render_citation_network_plot(graph, png, output_pdf_path=None, seed=0)
    width, height = _png_dimensions(png)
    assert width >= 800, f"PNG width {width} below the 800-pixel floor"
    assert height >= 600, f"PNG height {height} below the 600-pixel floor"


def test_render_is_deterministic_under_fixed_seed(tmp_path: Path) -> None:
    """Two renders with the same seed must produce byte-identical PNGs.

    Guards the freshness contract in doctor: if the figure is
    nondeterministic, mtime drifts each ``make figures-tables`` pass and
    the artifact's `git diff` becomes noise.
    """
    graph = build_citation_graph()
    a = tmp_path / "a.png"
    b = tmp_path / "b.png"
    render_citation_network_plot(graph, a, output_pdf_path=None, seed=0)
    render_citation_network_plot(graph, b, output_pdf_path=None, seed=0)
    assert a.read_bytes() == b.read_bytes(), (
        "citation network PNG is not deterministic under fixed seed"
    )
