"""Tests for the heuristic literature-link network helper."""

from __future__ import annotations

import csv
import math
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


def test_build_citation_graph_is_closed_over_library() -> None:
    graph = build_citation_graph()
    node_set = set(graph.nodes)

    assert len(graph.nodes) == len(PAPER_LIBRARY)
    assert graph.edges
    assert all(src in node_set and dst in node_set for src, dst in graph.edges)

    linked_by = build_linked_by_map()
    assert set(linked_by) == node_set
    for src, dst in graph.edges:
        assert src in linked_by[dst]


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


def test_write_centrality_csv(tmp_path: Path) -> None:
    graph = build_citation_graph()
    csv_path = write_centrality_csv(graph, tmp_path / "centrality.csv")

    rows = list(csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines()))
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


def test_render_citation_network_plot_writes_png_and_pdf(tmp_path: Path) -> None:
    graph = build_citation_graph()
    png = tmp_path / "citation_network.png"
    pdf = tmp_path / "citation_network.pdf"

    returned = render_citation_network_plot(graph, png, pdf, seed=0)

    assert returned == png.resolve()
    assert png.exists() and png.stat().st_size > 5_000
    assert pdf.exists() and pdf.stat().st_size > 5_000
