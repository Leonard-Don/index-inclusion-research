# `data/raw/` — externally sourced inputs

This directory holds raw, externally-sourced inputs to the pipeline.
Anything here is checked into git (small CSVs only); large binaries
belong in `data/processed/` or `output/` (gitignored).

## Files

### `passive_aum.csv`

US/CN passive-fund AUM yearly series consumed by the H2 hypothesis
verdict (`analysis/cross_market_asymmetry/verdicts/_h_functions._h2`).

* Schema: `market, year, aum_trillion`
* US rows: Federal Reserve Z.1 series `BOGZ1FL564090005A` (US ETF Total
  Financial Assets), USD trillion.
* CN rows: built by `download_passive_aum_cn` (top-down — quarterly
  total public-fund AUM × current index-fund share), RMB trillion. The
  CN rows here are now **overlaid by the bottom-up proxy** below at
  pipeline time, see `analysis/.../orchestrator._merge_aum_with_cn_proxy`.
* Refresh:
  * US: `index-inclusion-prepare-passive-aum`
  * CN: `index-inclusion-download-passive-aum-cn`
* See `docs/limitations.md` §3 for caveats.

### `cn_passive_aum_proxy.csv`

Bottom-up CN passive-AUM proxy: sum of year-end ETF TNA (shares × NAV)
across all major CSI300 / CSI500 tracking ETFs.

* Schema: `index_name, snapshot_date, total_tna_cny_billions, etf_count, source, note`
* Sources (akshare):
  * `fund_etf_scale_sse(date=YYYYMMDD)` — SH ETF shares at date.
  * `fund_scale_daily_szse(start_date, end_date, symbol="ETF")` — SZ ETF
    shares at date.
  * `fund_etf_fund_info_em(fund=code, ...)` — unit NAV at date (with
    `fund_etf_hist_em` close-price fallback when NAV endpoint returns
    empty on transient failure).
* Refresh:
  ```bash
  uv run python -m index_inclusion_research.download_cn_passive_aum_proxy
  ```
  Default snapshot dates: 2020-12-31 through 2024-12-31. Edit
  `DEFAULT_SNAPSHOTS` in `download_cn_passive_aum_proxy.py` to extend.
* Cadence: rerun annually after the year closes (typically by mid-January).
* Caveats (also in `docs/limitations.md` §3 and the CSV `note` column):
  * **Proxy, not direct AUM disclosure** — sums ETF TNA, which is
    narrower than the AMAC official "被动 AUM" series (which would
    include index OEFs and segregated mandate).
  * **ETF universe expands over time** — early snapshots (2020-21)
    understate true passive AUM relative to 2024 because broad-market
    ETFs were less established.
  * **NAV ≈ close fallback** — when the akshare NAV endpoint is empty
    we substitute the same-day close price; the premium/discount is
    typically < 0.5% for broad-index ETFs.
  * **Curated ticker list** — only the ~30 best-known CSI300 / CSI500
    trackers are included; smart-beta and active variants are excluded
    to keep the proxy purely "passive-tracking". The list is at the top
    of `download_cn_passive_aum_proxy.py` (`CSI300_TICKERS`,
    `CSI500_TICKERS`).

### `cn_csi300_changes.csv`

CN CSI300 inclusion / exclusion announcements. Used as the source of
truth for CN inclusion events.

### `hs300_rdd_*.csv`

HS300 RDD candidate snapshots. See `docs/hs300_rdd_data_contract.md` for
the L1 / L2 / L3 tiering.

### `real_*.csv`, `sample_*.csv`

Cleaned event panels and benchmarks used downstream by the event-study
pipeline.

## Refresh cadence summary

| File | Refresh CLI | Recommended cadence |
| --- | --- | --- |
| `passive_aum.csv` (US) | `index-inclusion-prepare-passive-aum` | Annually |
| `passive_aum.csv` (CN) | `index-inclusion-download-passive-aum-cn` | Annually |
| `cn_passive_aum_proxy.csv` | `python -m index_inclusion_research.download_cn_passive_aum_proxy` | Annually |
| `hs300_rdd_candidates.csv` | `index-inclusion-collect-hs300-rdd-l3` | After each CSI 300 rebalance |
| `real_events.csv` etc. | `index-inclusion-download-real-data` | After each rebalance window |
