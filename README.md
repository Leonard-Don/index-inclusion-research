# Index-Inclusion Research Toolkit

[![CI](https://github.com/Leonard-Don/index-inclusion-research/actions/workflows/ci.yml/badge.svg)](https://github.com/Leonard-Don/index-inclusion-research/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-3776AB)
![Research](https://img.shields.io/badge/focus-index%20inclusion%20research-1f6feb)
![Literature](https://img.shields.io/badge/literature-16%20papers-6f42c1)
![Pipeline](https://img.shields.io/badge/pipeline-10%20steps-0969da)
![CLI](https://img.shields.io/badge/CLI-43%20commands-2da44e)
![License](https://img.shields.io/badge/license-MIT-blue)

**English** · [简体中文](README.zh-CN.md)

> **What this is.** An end-to-end empirical-finance research project — *when a stock is added to a major index, does its price actually move, is the move permanent, and does the effect survive scrutiny (especially in China)?* — built solo in Python: a reproducible event-study pipeline, a matched-control design, an interactive research dashboard, ~1,190 tests, and an answer I report **honestly**, including where it comes back null and where my own identification strategy didn't hold up.

It is deliberately a **descriptive** study, not a causal-claims paper — see [The honest version](#the-honest-version-read-this-first) below.

---

## TL;DR

- **The question.** Index-inclusion is a classic "demand shock" laboratory: when CSI 300 / S&P 500 reshuffles, passive funds *must* buy the new names. The textbook prediction is a price pop. I test whether it happens, whether it reverses, and which mechanism drives it — across **two markets (CN + US)**.
- **What I found.** The **announcement-window** effect is real and robust in the US (US announce `CAR[-1,+1] ≈ +1.3%`, permutation `p = 0.0002`, holds under event-clustered SE), marginal in China (`p ≈ 0.03`). But the **effective-day window is null everywhere** (`p > 0.27`), and **5 of 7 mechanism hypotheses are inconclusive**. That pattern — a shrinking, mostly-anticipated effect — is consistent with the *disappearing index effect* (Greenwood & Sammon, 2022), here **replicated cross-market**.
- **What it demonstrates.** Full-stack empirical research (event study, propensity-style matching with covariate balance, pseudo-event placebos, permutation tests, clustered SE, multiple-testing correction), a reproducible pipeline with automated quality gates, and — the part I care most about — **knowing and stating the limits of the data** rather than manufacturing significance.

---

## The honest version (read this first)

A research project is only as good as what it admits. Three things I put up front rather than bury:

1. **My flagship identification design was not valid, and I say so.** I built an HS300 regression-discontinuity (RDD) around the index-membership cutoff. On inspection the "running variable" is a fabricated rank index (evenly spaced `299.85 … 300.28`), **perfectly collinear with treatment, with zero overlap at the cutoff** — mathematically not an RDD at all. I kept the full machinery for reproducibility but **downgraded it to an appendix "design that failed identification"** instead of presenting it as causal evidence. ([why, in detail](docs/identification_roadmap.md))
2. **The hypotheses are post-hoc / exploratory.** The 7 mechanism hypotheses were formed *after* seeing the announce-vs-effective and CN-vs-US asymmetries; there is no pre-analysis plan. The main table reports only `evidence_tier = core` results; small-n / exploratory ones (e.g. H3 with **n = 4**) stay in the appendix, flagged.
3. **The data has real limits.** US market-cap/weights are Yahoo approximations; ~39% of US announcement events are dropped for lack of valid window returns — and that drop is **non-random (delisted / acquired tickers)**, i.e. a survivorship/selection bias I document explicitly (effective `N = 371`). ([full limitations](docs/limitations.md))

Putting this near the top is intentional: it's exactly the signal I'd want to see from a research hire.

---

## Headline results — 7 mechanism hypotheses（7 条机制假说）

The cross-market-asymmetry (CMA) pipeline emits a verdict per hypothesis on the real sample (`index-inclusion-verdict-summary` prints the same table). Verdict column is kept in the project's original notation; the right column is the plain-English reading.

| #  | Mechanism hypothesis | 裁决 | 写作层级 | Reading (headline stat, n) |
|----|----------------------|------|------|-----------------------------|
| H1 | Information leakage / pre-run-up | 证据不足 | 正文 core | inconclusive — permutation `p = 0.97` (n=455) |
| H2 | Passive-fund AUM gap (demand curve) | 证据不足 | 正文 core | inconclusive — US AUM ratio **13.5×**, but effective CAR shows no decay (combined n=18) |
| H3 | Retail vs institutional structure | 支持 | 附录 supplementary | nominally supported, but **n = 4, ~zero power** → appendix only |
| H4 | Short-sale constraints | 证据不足 | 附录 supplementary | inconclusive — regression `p = 0.60` (n=455) |
| H5 | Price-limit (涨跌停) rules | 证据不足 | 正文 core | inconclusive — limit-coef `p = 0.43` (n=1096) |
| H6 | Index-weight predictability | 证据不足 | 附录 supplementary | inconclusive — heavy−light spread −0.016 (n=87) |
| H7 | Sector-structure differences | 支持 | 正文 core | supported — US sector spread 5.97, interaction `p = 0.095` |

*(`证据不足` = insufficient evidence; `支持` = supported.)* Source of truth: [results/real_tables/cma_hypothesis_verdicts.csv](results/real_tables/cma_hypothesis_verdicts.csv) (narrative: [results/real_tables/research_summary.md](results/real_tables/research_summary.md)). A `--sensitivity` flag re-runs every verdict across significance thresholds (0.05 → 0.20) with Bonferroni/BH correction; details in [docs/sensitivity_workflow.md](docs/sensitivity_workflow.md).

> Two findings (H5 price-limits, H2 demand) flipped from "supported" to "inconclusive" once I replaced free Yahoo data with licensed Tushare A-share data. I left the reversal in the record rather than quietly keeping the more flattering numbers.

---

## Robustness — what makes the announcement effect believable

The descriptive claim ("announce-window strong, effective-window null") is backed by four independent checks, all generated by the pipeline into the `results/real_tables/robustness_car_permutation.csv` family and `results/real_figures/parallel_trends_aar_us_announce.png` (one per market/window):

| Check | What it shows | US announce `[-1,+1]` | Effective windows |
|---|---|---|---|
| Daily AAR parallel trends | treated vs matched control overlap pre-event, diverge only in the window | clean pre-trend, day-0 jump | — |
| Pseudo-event-date placebo | real CAR sits in the tail of a placebo distribution | `p = 0.005` | `p > 0.29` |
| Permutation test (sign-flip, 5,000) | empirical significance under H₀ | `p = 0.0002` | `p > 0.27` |
| Event-clustered SE (CRV1, by date) | inference robust to same-day correlation | `p = 0.0003` | not significant |

All three significance tests agree, and the effective-window null holds under every one — the cross-market "anticipated, mostly-gone" story, not a causal index-demand effect.

---

## Interface preview

The whole project is navigable through one local Flask dashboard (`http://localhost:5001`) — literature, sample, figures and verdicts in a single workflow.

<table>
  <tr>
    <td align="center" width="50%">
      <img src="docs/screenshots/readme-dashboard-overview.png" alt="Dashboard research overview" width="100%">
      <br><strong>Research overview</strong><br>
      <sub>16 papers, real sample, identification design and core results in one entry point</sub>
    </td>
    <td align="center" width="50%">
      <img src="docs/screenshots/cma-evidence-tiers.png" alt="CMA evidence tiers and H7 interaction detail" width="100%">
      <br><strong>CMA evidence tiers & H7 interaction</strong><br>
      <sub>support strength, robustness and sector interaction for all 7 hypotheses on one screen</sub>
    </td>
  </tr>
</table>

<details>
<summary>More screenshots (full-page)</summary>

- [Home, full page](docs/screenshots/dashboard-home.png)
- [Single-paper reader](docs/screenshots/paper-brief.png)
- [Mobile view](docs/screenshots/dashboard-mobile.png)

</details>

There is no public hosted demo — run it locally (below).

---

## Run it

```bash
make sync                      # install pinned deps from uv.lock (reproducible)
index-inclusion-dashboard      # then open http://localhost:5001

make rebuild                   # 10 步: re-run the full offline pipeline (events → CMA → figures → report)
make verdicts                  # print the 7-hypothesis verdict table in the terminal
make ci                        # lint + type-check + coverage gate + project health checks
```

Dashboard modes: `/` (overview), `/?mode=brief` (3-min read), `/?mode=full` (everything), `/paper/<id>` (single-paper reader + source PDF).

---

## How it's built (the engineering)

The research is ~11k lines; the rest is the infrastructure that makes it reproducible and auditable end-to-end — built to the standard I'd want a research codebase held to.

- **Deterministic, offline pipeline.** `index-inclusion-rebuild-all` recomputes every result from `data/` in ~3 min with no network calls; the frozen verdict baseline reproduces unchanged on re-run — a `pap-diff` drift audit confirms all 7 hypotheses stay put.
- **Automated quality gates.** A custom `doctor` framework runs 30 health checks (artifact freshness, schema contracts, chart registry, cross-document consistency) and a `paper-integrity` gate cross-checks that the README/paper numbers actually match the committed CSVs; `index-inclusion-paper-skeleton` regenerates the paper skeleton straight from the frozen artifacts. `make ci` is green.
- **Tested.** ~1,190 unit + integration tests (event study, matching + covariate balance, robustness, pipeline `main()` integration, dashboard rendering), lint (`ruff`) and `mypy` clean.
- **Honest seeds & snapshots.** All randomness is seeded; verdict baselines are snapshotted so any drift in conclusions is visible over time.

### Methods stack

Event study (market-adjusted + market-model AR, Patell Z, BMP t) · propensity-style matched controls with Stuart-2010 SMD balance · long-window retention · pseudo-event placebos · sign-flip permutation tests · event-clustered (CRV1) SE · post-hoc power analysis (MDE) · Bonferroni/BH multiple-testing correction.

---

## Repo map

```text
src/index_inclusion_research/
  analysis/          event study, regressions, RDD, cross-market asymmetry, robustness, power
  pipeline/          sample construction, matching (+ covariate balance)
  outputs/           figure & table builders
  dashboard/ web/    Flask app + templates/static (the interactive front-end)
  doctor/            project-health check framework
data/                raw/ + processed/
results/             event_study/, regressions/, figures/, tables/, real_*/, literature/
docs/                literature maps, methodology, limitations, identification roadmap (some in Chinese)
tests/               ~1,190 unit + integration tests
```

Deeper write-ups (several in Chinese): [research delivery package](docs/research_delivery_package.md) · [paper outline](docs/paper_outline.md) · [limitations](docs/limitations.md) · [identification roadmap](docs/identification_roadmap.md) · [CLI reference — 43 个 console scripts](docs/cli_reference.md).

---

## About this project

A solo build that takes an established question in the index-inclusion literature and implements it end-to-end — data, event study, matched-control design, robustness, and an interactive research front-end — with the goal of getting the *process* right (reproducibility, honest inference, clean code) rather than forcing a headline result. Licensed MIT.
