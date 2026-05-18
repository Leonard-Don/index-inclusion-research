# Changelog

All notable, user-visible changes to `index-inclusion-research`.

## Unreleased

- feat(figures): literature chronology timeline (47th CLI). New
  `index-inclusion-literature-timeline` console script renders the
  16-paper `PAPER_LIBRARY` as a single figure: X-axis is publication
  year (1986→2026), Y-axis is one swimlane per research thread
  (短期价格压力 / 需求曲线效应 / 沪深300论文复现), marker color
  encodes position (正方=蓝, 反方=红, 中性=灰) sourced from the
  catalog's `stance` field, marker size linearly maps the heuristic
  in-degree centrality column from `results/literature/citation_centrality.csv`
  (anchor papers like Shleifer 1986 sit at the top end; freshly added
  2022 contra papers at the floor — still visible). Three light era
  bands annotate the chronology behind the markers: 1986-2002
  classical, 2002-2014 skeptics, 2014+ China + identification. Source
  is read-only — no paper metadata is fabricated; `年份待核验` falls
  back to 2014 with an INFO log so future catalog fixes auto-correct.
  Defaults write `results/literature/literature_timeline.{png,pdf}`;
  `paper-bundle` ships the PDF in the figures/ section so reviewers
  pick it up automatically. `export-public-summary` adds a
  `literature_timeline` block (years_covered, n_papers_pre_2002/
  2002_to_2014/post_2014, dominant_position_by_era, anchors_by_era top-2
  per era). `figures_tables` re-renders the timeline as a `_maybe_*`
  step; `paper-bundle._regenerate_artifacts` does the same in step 5a.
  Doctor `check_literature_timeline_artifact` warns when the PNG/PDF
  twins are missing or older than `citation_centrality.csv`. README
  CLI badge bumped 46→47 (3 places), `docs/cli_reference.md` 头部
  count + new §24, `docs/literature_to_project_guide.md` §1 文献综述
  cross-reference. 18 new tests in `tests/test_literature_timeline.py`
  (catalog coverage, PNG ≥800×600, era distribution + dominant-position
  summary, missing-CSV fallback, synthetic 3-paper render, pyproject
  registration, parametrized era-bucket boundary).

- feat(literature): `index-inclusion-add-paper --print-json-template`
  prints a side-effect-free starter payload for `--from-json`. The JSON
  contains every accepted `NewPaper` field, validates through the same
  constructor, and exits before catalog/BibTeX paths are read or written.

- feat(literature): interactive add-paper CLI (46th). New
  `index-inclusion-add-paper` console script lets the researcher add a
  new academic paper to the 16-paper `literature_catalog.PAPER_LIBRARY`
  with one command instead of touching 6 files. Inputs (paper_id,
  authors, year, title, journal, market_focus US/CN/both, methodology
  event_study/regression/RDD/other, position pro_index_effect/contra/
  neutral, research_thread, related_paper_ids, abstract) are collected
  either via TTY prompts (`interactive_add_paper`) or from a JSON file
  (`--from-json paper.json`). Mandatory fields enforced via
  `NewPaper.__post_init__`; missing optional fields fall back to
  `[TODO: not provided]` placeholders (no fabrication). `paper_id`
  validated against `[a-z][a-z0-9_]*`; duplicates rejected with helpful
  error (idempotent re-runs). Source rendering uses safe Python string
  literals for free text, validates four-digit `year`, known `camp`, and
  legal `related_paper_ids`, and BibTeX escaping covers braces,
  backslashes, controls, newlines, and field-injection-shaped input.
  Side-effects: (1) inserts a new `LiteraturePaper(...)` literal into
  `literature_catalog/_data.py` via a lexicographic scan that preserves
  the existing thematic tuple order (textual edit, reviewable diff), (2)
  appends `@article{<paper_id>, ...}` to `paper/references.bib`,
  (3) regenerates `paper/skeleton.md` §References, `paper/methodology_summary.md`
  top-5 centrality, `data/public/index_research_summary.json`
  `papers_indexed` count, and `results/literature/citation_*` figure
  twin + CSV. Downstream failures now return a non-zero CLI result with
  the partial-write report instead of silently looking successful. Flags:
  `--dry-run` (show diff without writing), `--skip-downstream`
  (catalog + BibTeX only for batch adds), `--run-integrity`
  (`paper-integrity --fail-on-warn` after writes), `--from-json` (non-
  interactive), `--catalog-path` / `--bibtex-path` (test overrides).
  README CLI badge bumped 45→46 (3 places), `docs/cli_reference.md` 头部
  count + appendix §23, `docs/literature_to_project_guide.md` §五 cross-
  reference. Returns structured `AddPaperReport` (paper_library_count
  before/after, catalog_updated, bibtex_updated, downstream_artifacts,
  paper_integrity_exit_code, notes) so callers can audit what changed.

- feat(paper): BibTeX CrossRef enrichment (45th CLI). New
  `index-inclusion-enrich-bib` console script resolves the
  `[TODO: journal]` / `[TODO: volume]` / `[TODO: pages]` / `[TODO: doi]`
  placeholders that `index-inclusion-tex-export` emits into
  `paper/references.bib`. The script queries CrossRef's free REST API
  (`https://api.crossref.org/works`) per entry, scores the candidate
  matches with a 0–1 confidence (author-surname overlap + title token
  Jaccard + year-mismatch penalty), and only fills in fields when the
  score meets `--min-confidence` (default 0.7). Author / title / year
  are *never* overwritten — those are the researcher's canonical choice.
  Responses cache to `cache/crossref_cache.json` for idempotent reruns;
  CrossRef being down ⇒ every entry low-confidence ⇒ output bib equals
  input bib (TODOs preserved). HTML entities decoded
  (`International Journal of Finance &amp; Economics` →
  `\&`-escaped) and pages normalized to BibTeX `--` form. Real-data
  smoke against `paper/references.bib`: 15 / 16 entries enriched,
  1 kept TODO (Chinese-language paper with [TODO: year] placeholder
  CrossRef can't disambiguate above the threshold). `tex-export` gains
  an optional `--enrich-bib` flag that runs the enrichment inline so
  `make paper` can produce a publication-ready bib in one shot.
  README CLI badge bumped 44→45 (3 places), `docs/cli_reference.md`
  gains §22 with full match-heuristic description.

- feat(paper): submission readiness gate (44th CLI). New
  `index-inclusion-submission-ready` console script is the final
  pre-submission go/no-go gate, sitting downstream of `paper-integrity`.
  Where the integrity gate verifies that the generated artifacts agree
  with each other, the submission-ready gate asks the broader
  question — *is the paper actually ready to ship?* — by aggregating
  ~17 checks: skeleton presence + 8 required top-level sections, count
  of `[TODO: ...]` markers (warn with per-section breakdown),
  methodology summary freshness vs skeleton, 9 expected figures present
  + non-empty + ≥ 800x600 (zero-dep PNG IHDR parsing), TeX + BibTeX
  artifacts + 16 BibTeX entries, optional `pdflatex` sanity compile
  in a temp dir (gracefully skipped if pdflatex is not on PATH),
  `paper-integrity` gate bubbled up as a check, PAP `all_unchanged`
  on the public summary, `doctor.run_all_checks` aggregated, public
  summary freshness vs verdicts CSV, raw input CSV schema spot-check,
  literature catalog count, 3 sensitivity figures fresh, verdict
  timeline fresh, and an external `pytest` reminder. Aggregates into
  `SubmissionAssessment` with `overall_status` (ready /
  partially_ready / not_ready), `pass_count` / `warning_count` /
  `blocker_count`, plus a rough `estimated_remaining_work_hours`
  heuristic (`2.0h * fail + 0.5h * warn + 1.0h * TODO`). Supports
  `--format text|json|markdown` and `--fail-on-warn` (CI). Exit codes:
  0 ready / 1 partially_ready / 2 not_ready. Completely read-only —
  every fix_command points at the corresponding generator. README CLI
  badge bumped 43→44 (3 places), `docs/cli_reference.md` gains §21
  with the full check list, `docs/research_delivery_package.md` lists
  it as the last command in the publishing checklist.

- feat(paper): LaTeX / Overleaf export CLI. New
  `index-inclusion-tex-export` console script (the 43rd) converts
  `paper/skeleton.md` and `paper/methodology_summary.md` into
  `paper/manuscript.tex` plus `paper/references.bib`. The exporter keeps
  the existing paper-integrity gate read-only and downstream: it consumes
  the already generated Markdown paper artifacts, preserves TODO markers
  as `\TODO{...}` by default for Overleaf editing, supports
  `--include-todos false` for review drafts, and offers `ctex` or
  `xeCJK` CJK preambles. README / CLI reference counts are bumped 42→43.

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
