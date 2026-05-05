# 真实数据说明

这套 `real_*` 数据文件用于把当前项目从示例数据切换到真实公开数据。

## 数据来源

- 美股指数纳入事件：
  - [Wikipedia S&P 500 components](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)
  - 其中 `effective_date` 来自成分变更表，`announce_date` 优先取该表脚注对应的 S&P Dow Jones 引用日期
- A 股指数纳入事件：
- 基于 [data/raw/cn_csi300_changes.csv](data/raw/cn_csi300_changes.csv) 整理的沪深300调样批次，目前覆盖 `2020-06-01` 到 `2025-11-28`
  - 当前真实事件文件会把这些 CN 批次与美股指数变更合并成 `data/raw/real_events.csv`
- 日频价格与基准指数：
  - Yahoo Finance，经 `yfinance` 抓取
- CN 行业标签：
  - `python3 -m index_inclusion_research.enrich_cn_sectors --force` 优先使用
    `akshare` 的 CNInfo 公司资料接口（`所属行业`），并以 Yahoo Finance 元数据兜底，
    回填 `data/raw/real_events.csv` 与 `data/raw/real_metadata.csv` 中缺失的 A 股
    `sector`
- US ETF AUM proxy：
  - `data/raw/passive_aum.csv` 使用 FRED / Federal Reserve Z.1 序列
    [BOGZ1FL564090005A](https://fred.stlouisfed.org/series/BOGZ1FL564090005A)
  - 原序列为 `Exchange-Traded Funds; Total Financial Assets, Level`，单位是
    `Millions of U.S. Dollars`，本项目转换为 `aum_trillion = VALUE / 1,000,000`

## 重要说明

- `close`、`volume`、`benchmark_ret` 属于真实市场数据。
- `mkt_cap` 和 `turnover` 使用 Yahoo 当前可得 `sharesOutstanding` 近似构造，因此更适合课程论文和机制分析，不等同于交易所官方历史自由流通市值。
- CN `sector` 来自 CNInfo 公司资料的 `所属行业`，不等同于中信 / 申万官方行业分类；H7 会把它作为 A 股真实行业桶。
- `passive_aum.csv` 是 US ETF 总金融资产 proxy，用于解锁 H2 的时间序列方向判断；它不是全球全口径被动基金 AUM。
- A 股事件名单当前已经覆盖 2020-06 至 2025-11 的多期沪深300调样批次，足以直接跑通真实样本版本；如果你后面要扩展到更长样本期，可以继续往原始变更源追加公告批次。
- 如果手里没有中证官方候选排名表，但想先构造公开数据口径的边界样本，可以优先使用 `index-inclusion-reconstruct-hs300-rdd --all-batches` 一次重建当前事件源里可稳定回滚的批次代理候选集；如果只想看单批次，也可以改用 `--announce-date 2024-05-31`。这类文件适合公开数据版本的 RDD 复现，但不应表述为官方历史 reserve list。
- 某些美股变更行只有 `effective_date` 的免费公开来源更完整，因此若脚注日期缺失，脚本会回退到 `effective_date`。

## 推荐用法

```bash
index-inclusion-download-real-data
python3 -m index_inclusion_research.enrich_cn_sectors --force
index-inclusion-compute-h6-weight-change --force
index-inclusion-build-event-sample --input data/raw/real_events.csv --output data/processed/real_events_clean.csv
index-inclusion-build-price-panel --events data/processed/real_events_clean.csv --prices data/raw/real_prices.csv --benchmarks data/raw/real_benchmarks.csv --output data/processed/real_event_panel.csv
index-inclusion-cma
```
