# Changelog

All notable, user-visible changes to `index-inclusion-research`.

## Unreleased

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
