# Match Robustness Grid

This artifact is local-only. It reads the checked local matched-event sample
and local price history; it does not download or scrape web data.

- matched events: `/Users/leonardodon/index-inclusion-research/data/processed/real_matched_events.csv`
- prices: `/Users/leonardodon/index-inclusion-research/data/raw/real_prices.csv`
- threshold: `|SMD| < 0.25`

## Best Current Specification

- `announce_1to2`: 0 covariate(s) over threshold; max |SMD| = 0.157
- default `announce_1to3`: 0 covariate(s) over threshold; max |SMD| = 0.177

## Specification Grid

- `announce_1to1`: controls=1:1, reference=announce_date, over=0, max_abs_smd=0.198, worst=CN/pre_event_volatility (+0.198)
- `announce_1to2`: controls=1:2, reference=announce_date, over=0, max_abs_smd=0.157, worst=CN/mkt_cap_log (-0.157)
- `announce_1to3` (default): controls=1:3, reference=announce_date, over=0, max_abs_smd=0.177, worst=CN/mkt_cap_log (-0.177)
- `effective_1to1`: controls=1:1, reference=effective_date, over=1, max_abs_smd=0.422, worst=CN/pre_event_volatility (+0.422)
- `effective_1to2`: controls=1:2, reference=effective_date, over=1, max_abs_smd=0.276, worst=CN/pre_event_volatility (+0.276)
- `effective_1to3`: controls=1:3, reference=effective_date, over=1, max_abs_smd=0.266, worst=CN/pre_event_volatility (+0.266)
