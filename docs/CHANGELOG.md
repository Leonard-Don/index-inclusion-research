# Changelog

All notable, user-visible changes to `index-inclusion-research`.

## Unreleased

- feat(literature): heuristic literature-link network analyzer + figure.
  New `index-inclusion-citation-graph` console script (the 39th) walks the
  16-paper `literature_catalog.PAPER_LIBRARY`, derives a within-library
  link graph from each paper's `related_paper_ids` tuple (transparent
  similarity / context links built from year + topic + methodology,
  **not** bibliography-verified citations), and writes three artifacts to
  `results/literature/`: a deterministic force-directed PNG (nodes sized
  by in-degree, colored by stance 反方 / 中性 / 正方), a vector PDF
  twin, and a centrality CSV (`paper_id, in_degree, out_degree,
  betweenness, eigenvector, top_linked_by, top_links_to`). Centrality is
  computed without networkx: stdlib BFS for Brandes' betweenness on the
  undirected projection and damped power iteration (PageRank-flavored)
  for eigenvector. The export-public-summary JSON gains a
  `literature_network` section with `edge_count`, `node_count`,
  `top_3_most_linked`, `top_3_central_papers`, and an explicit
  `edge_semantics: heuristic_similarity_not_bibliographic_citation` tag
  so downstream consumers cannot mistake the links for citations. Doctor
  gains `citation_graph_artifact` (PNG / PDF freshness vs the centrality
  CSV) alongside the pre-existing `heuristic_citation_centrality_schema`
  check (legacy `top_cited_by` / `top_cites` headers fail explicitly).
  39 console scripts total.
- feat(export): public summary JSON for external consumers (Phase F1).
  New `index-inclusion-export-public-summary` console script distills
  `results/real_tables/cma_hypothesis_verdicts.csv`,
  `results/real_tables/pap_deviation_report.csv`,
  `results/literature/hs300_rdd/rdd_robustness.csv`,
  the newest `snapshots/pre-registration-*.csv`, and the published
  `results/figures/*.png` manifest into a small, schema-versioned,
  committable artifact at `data/public/index_research_summary.json`
  (~3-5 KB). Sibling projects (e.g. `cn-altdata-brief`) and future
  GitHub Pages digests can now read the project state without running
  anything — `requests.get(raw_url)` and parse JSON.
  Doctor gains `public_summary_freshness` (warns when input CSV mtime
  exceeds the summary's mtime). 37 console scripts total
  (`index-inclusion-export-public-summary` is the 37th).
