# Changelog

All notable, user-visible changes to `index-inclusion-research`.

## Unreleased

- feat(figures): verdict evolution timeline reconstructed from git log.
  New `index-inclusion-verdict-timeline` console script (the 40th) walks
  `git log --follow -- results/real_tables/cma_hypothesis_verdicts.csv`
  and materialises each historical CSV via `git show <sha>:<path>`,
  stitching the per-commit verdicts into a long-format DataFrame and a
  7-swimlane figure (one row per H1..H7) saved to
  `results/figures/verdict_timeline.{png,pdf}`. Each commit is rendered
  as a circle (verdict held) or square (verdict text changed) coloured
  by category — green for 支持, yellow for 部分支持, red for 证据不足
  — with a 2026-05-16 PAP baseline dashed line and right-edge
  annotations showing the latest verdict per hypothesis. The
  `export-public-summary` JSON gains a `verdict_timeline` section
  exposing `total_commits_tracked`, `first_commit_date`,
  `last_commit_date`, `total_verdict_changes`, and a per-hypothesis
  change-count map so downstream consumers can render a "research
  evolution" widget without re-walking the git history. Doctor gains
  `check_verdict_timeline_artifact` (PNG / PDF freshness vs the source
  verdicts CSV); `make figures-tables` and the paper-bundle pipeline
  auto-regenerate the figure when the checkout has a `.git/` directory
  and skip silently otherwise (e.g. tarball extracts).

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
