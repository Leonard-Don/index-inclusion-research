# 真实数据说明

这套 `real_*` 数据文件用于把当前项目从示例数据切换到真实公开数据。

## 数据来源

- 美股指数纳入事件：
  - [Wikipedia S&P 500 components](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)
  - 其中 `effective_date` 来自成分变更表，`announce_date` 优先取该表脚注对应的 S&P Dow Jones 引用日期
- A 股指数纳入事件：
- 基于 [data/raw/cn_csi300_changes.csv](/Users/leonardodon/index-inclusion-research/data/raw/cn_csi300_changes.csv) 整理的沪深300调样批次，目前覆盖 `2020-06-01` 到 `2025-11-28`
  - 当前真实事件文件会把这些 CN 批次与美股指数变更合并成 `data/raw/real_events.csv`
- 日频价格与基准指数：
  - Yahoo Finance，经 `yfinance` 抓取

## 重要说明

- `close`、`volume`、`benchmark_ret` 属于真实市场数据。
- `mkt_cap` 和 `turnover` 使用 Yahoo 当前可得 `sharesOutstanding` 近似构造，因此更适合课程论文和机制分析，不等同于交易所官方历史自由流通市值。
- A 股事件名单当前已经覆盖 2020-06 至 2025-11 的多期沪深300调样批次，足以直接跑通真实样本版本；如果你后面要扩展到更长样本期，可以继续往原始变更源追加公告批次。
- 如果手里没有中证官方候选排名表，但想先构造公开数据口径的边界样本，可以优先使用 `index-inclusion-reconstruct-hs300-rdd --all-batches` 一次重建当前事件源里可稳定回滚的批次代理候选集；如果只想看单批次，也可以改用 `--announce-date 2024-05-31`。这类文件适合公开数据版本的 RDD 复现，但不应表述为官方历史 reserve list。
- 某些美股变更行只有 `effective_date` 的免费公开来源更完整，因此若脚注日期缺失，脚本会回退到 `effective_date`。

## 推荐用法

```bash
python3 scripts/download_real_data.py
python3 scripts/build_event_sample.py --input data/raw/real_events.csv --output data/processed/real_events_clean.csv
python3 scripts/build_price_panel.py --events data/processed/real_events_clean.csv --prices data/raw/real_prices.csv --benchmarks data/raw/real_benchmarks.csv --output data/processed/real_event_panel.csv
```
