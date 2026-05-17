# Changelog

All notable, user-visible changes to `index-inclusion-research`.

## Unreleased

- feat(paper): cross-document integrity gate. New
  `index-inclusion-paper-integrity` console script (the 42nd) is the
  publication-time gate that the paper bundle's artifacts are mutually
  consistent. While each generator is unit-tested in isolation, the
  generated artifacts cross-reference each other — verdicts CSV vs
  paper/skeleton.md vs paper/methodology_summary.md vs
  pap_deviation_report.csv vs data/public/index_research_summary.json
  vs README.md badge vs literature_catalog.PAPER_LIBRARY. This CLI runs
  10 cross-document checks (`check_verdicts_hids_match_skeleton`,
  `check_figures_referenced_exist`, `check_pap_classifications_match_public_summary`,
  `check_console_scripts_count_matches_readme`,
  `check_paper_library_referenced_in_skeleton`,
  `check_sample_sizes_match_methodology`,
  `check_sensitivity_coverage_match`, etc.) and aggregates them into a
  severity-tagged report (info / warn / fail). Exit codes match doctor
  conventions: 0 pass, 1 warn, 2 fail; `--fail-on-warn` makes warns also
  exit 1 for CI. Supports `--format text|json|markdown`. Doctor gains
  `check_paper_integrity` adapter so the gate also surfaces in
  `index-inclusion-doctor`. README CLI badge bumped 41→42,
  `docs/cli_reference.md` gains §19, `docs/research_delivery_package.md`
  mentions the gate as the publish prerequisite. Read-only — never
  mutates any artifact, just compares them.

- feat(paper): methodology summary auto-generator. New
  `index-inclusion-methodology-summary` console script (the 41st)
  renders a single-page methodology card to
  `paper/methodology_summary.md` by combining
  `results/real_tables/cma_hypothesis_verdicts.csv` (§1 sample sizes),
  `data/processed/real_events_clean.csv` and
  `real_matched_event_panel.csv` (event-panel + matched-control row
  counts), `data/public/index_research_summary.json` (§3 stability
  counts per threshold / AR engine / 2D axis and §4 PAP deviation
  classification), `results/literature/citation_centrality.csv` (top-5
  eigenvector centrality + literature_catalog stance/year join), and
  `pyproject.toml` + `doctor.DEFAULT_CHECKS` (toolchain counts). The
  card carries NO `[TODO: prose]` markers — every value is
  deterministically derived — so it answers "what did you actually
  do?" in 3-5 KB without making the reader walk through the full
  paper §3. Doctor gains `check_methodology_summary_freshness` (warns
  when any input mtime is newer than the rendered card; CI auto-passes
  because checkout mtimes are not generation times).
  `paper-bundle --force` and `make paper` add a 7th regeneration step
  so the bundle stays self-consistent. README CLI badge bumped 40→41,
  `docs/cli_reference.md` gains §18, `docs/research_delivery_package.md`
  gains 附录 G.

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
