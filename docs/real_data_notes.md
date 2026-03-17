# 真实数据说明

这套 `real_*` 数据文件用于把当前项目从示例数据切换到真实公开数据。

## 数据来源

- 美股指数纳入事件：
  - [Wikipedia S&P 500 components](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)
  - 其中 `effective_date` 来自成分变更表，`announce_date` 优先取该表脚注对应的 S&P Dow Jones 引用日期
- A 股指数纳入事件：
  - 中证指数有限公司官网公告附件整理出的沪深300新增样本名单
  - 当前脚本内置了 `2024-11-29` 与 `2025-11-28` 两批调整名单，并使用对应的实施日
- 日频价格与基准指数：
  - Yahoo Finance，经 `yfinance` 抓取

## 重要说明

- `close`、`volume`、`benchmark_ret` 属于真实市场数据。
- `mkt_cap` 和 `turnover` 使用 Yahoo 当前可得 `sharesOutstanding` 近似构造，因此更适合课程论文和机制分析，不等同于交易所官方历史自由流通市值。
- A 股事件名单目前内置的是较近两期沪深300新增样本，足以直接跑通真实样本版本；如果你后面要扩展到更长样本期，可以继续往脚本里的 `CN_EVENT_GROUPS` 追加公告批次。
- 某些美股变更行只有 `effective_date` 的免费公开来源更完整，因此若脚注日期缺失，脚本会回退到 `effective_date`。

## 推荐用法

```bash
python3 scripts/download_real_data.py
python3 scripts/build_event_sample.py --input data/raw/real_events.csv --output data/processed/real_events_clean.csv
python3 scripts/build_price_panel.py --events data/processed/real_events_clean.csv --prices data/raw/real_prices.csv --benchmarks data/raw/real_benchmarks.csv --output data/processed/real_event_panel.csv
```
