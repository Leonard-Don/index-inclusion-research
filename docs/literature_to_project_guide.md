# 文献到项目的使用指南

这份指南把三篇核心文献对应到当前项目的实际用法上：

- `harris_gurel_sp500_jf.pdf`
- `shleifer_demand_curves_jf.pdf`
- `3439A97D91C0913CDC56FC9521E_597DEABF_14507A.pdf`

## 1. 这三篇文献分别在回答什么

### Harris and Gurel (1986)

核心问题：`指数纳入引起的价格上涨，是短期价格压力，还是长期价值重估？`

这篇文献最适合指导你看：

- 公告/调整日前后是否有显著正异常收益
- 这种收益是否很快回落
- 成交量是否同步放大

如果你的结果是“短期跳升，随后回落”，那么你的项目输出更接近 `price pressure` 解释。

### Shleifer (1986)

核心问题：`股票需求曲线是否向下倾斜？`

这篇文献的重点不是单纯证明有事件效应，而是强调：

- 被动需求冲击可能导致价格发生不完全反转
- 如果价格在较长窗口内仍未回到事件前，说明不是纯粹短期冲击
- 这更支持“股票不是完全可替代品”的需求曲线解释

如果你的结果是“涨了以后只回吐一部分，或者长期仍高于事件前”，那就更接近 `downward-sloping demand curves`。

### 《指数效应存在吗？——来自沪深300断点回归的证据》

核心问题：`在中国市场，传统事件研究会不会识别不干净？能不能用更强识别策略来检验指数效应？`

这篇文献告诉你两件事：

- 中国市场做指数效应，不能只停在普通事件研究
- 更强的做法是找接近调入/调出边界的股票，用断点回归或 DID 做稳健性

它适合指导你把当前项目分成：

1. 基线事件研究
2. 匹配对照组回归
3. 更强识别的扩展版

## 2. 你现在该怎么用这个项目

### 用法 A：按 Harris and Gurel 写“短期价格压力”版本

这是最适合你现在直接动手的版本。

看这些文件：

- `data/raw/real_events.csv`
- `data/processed/real_event_panel.csv`
- `results/real_event_study/event_study_summary.csv`
- `results/real_figures/*_car_path.png`

操作重点：

1. 保持短窗口
   - `config/markets.yml` 里的 `event_window_pre/post` 用 `20`
   - `car_windows` 重点看 `[-1,+1]`、`[-3,+3]`、`[-5,+5]`
2. 分开看 `announce` 和 `effective`
   - 公告日更像信息效应
   - 生效日更像被动调仓带来的价格压力
3. 看是否回落
   - 重点看 `average_paths.csv` 和对应 CAR path 图
   - 如果 0 日后快速回撤，最适合写成 Harris and Gurel 式结论

你论文里可以写成：

“本文首先参考 Harris and Gurel 的思路，检验指数纳入事件前后是否存在显著但短暂的异常收益，并结合成交量变化判断该价格反应是否更符合短期价格压力解释。”

### 用法 B：按 Shleifer 写“需求曲线向下倾斜”版本

这个版本比 A 更强一点，因为它要求你看更长窗口。

当前项目已经能做 80% 的事，你只需要改配置：

1. 把 `config/markets.yml` 中的 `event_window_post` 从 `20` 改成 `60` 或 `120`
2. 把 `car_windows` 增加成：
   - `[-1, 1]`
   - `[-3, 3]`
   - `[-5, 5]`
   - `[0, 20]`
   - `[0, 60]`
3. 重新运行：
   - `scripts/build_price_panel.py`
   - `scripts/run_event_study.py`

你要看的不是“涨没涨”，而是：

- 调整后是否只部分反转
- 长期累计异常收益是否仍显著偏离 0

如果长期不完全回吐，你就可以把结论往 Shleifer 靠：

“指数纳入并非仅造成暂时性价格压力，其影响在更长窗口内仍部分保留，支持股票需求曲线向下倾斜、资产并非完全可替代的解释。”

### 用法 C：按沪深300断点回归论文写“中国市场强化识别”版本

当前项目最接近这篇文献的部分，是：

- `scripts/match_controls.py`
- `scripts/run_regressions.py`
- `results/real_regressions/`

你可以先把它当成 `基线版 DID / 对照组分析` 来用：

1. 先只保留中国样本
   - 从 `data/raw/real_events.csv` 里筛 `market == CN`
2. 跑基线事件研究
3. 再跑匹配对照组回归
4. 检查 `inclusion` 对 `car_m1_p1`、`turnover_change`、`volume_change` 的系数

但要注意：

- 当前项目`还不是完整的断点回归复现`
- 真正的 RD 需要 `排名 running variable`、`断点位置`、`是否刚好调入/调出`

如果你想更像这篇中文论文，下一步要补的数据不是价格，而是：

- 调整前候选股票的市值排名
- 调入/调出边界附近样本
- 断点左右窄窗口样本

换句话说，这个项目现在已经够你写：

- 基线事件研究
- 匹配对照组回归
- 机制检验

但如果你要写“断点回归证据”，还需要再加一个 `rdd` 模块。

现在项目里已经补了一个入口：

- `scripts/start_hs300_rdd.py`

它可以在你提供 `data/raw/hs300_rdd_candidates.csv` 之后，直接把候选样本的排名变量接进来并做 local linear RD。
如果该文件不存在，脚本会退回到 `demo` 伪排名数据，只用于演示流程，不应用于正式结论。

## 3. 结合三篇文献，你最推荐的论文写法

最稳的结构不是三篇都硬复现，而是：

1. `理论部分` 用 Shleifer
   - 解释为什么指数纳入可能带来不只是短期冲击
2. `基线事件研究` 用 Harris and Gurel
   - 检验短期异常收益和回撤
3. `中国市场稳健性` 用沪深300断点回归论文
   - 说明为什么还要做更强识别，而不是只看 CAR

这会让你的论文结构非常自然：

- 理论上：需求曲线向下倾斜
- 现象上：短期价格和成交量反应明显
- 识别上：中国市场需要更强对照设计

## 4. 对当前项目最值得做的三件事

### 第一件：先用真实样本完成一版基线论文

直接使用：

- `results/real_event_study/event_study_summary.csv`
- `results/real_regressions/regression_coefficients.csv`
- `results/real_tables/research_summary.md`

这已经够你写出：

- 研究问题
- 事件研究结果
- 机制检验结果
- 中美差异讨论

### 第二件：把窗口拉长，区分“价格压力”还是“需求曲线”

这一步最关键。

因为它决定你论文主结论是：

- `temporary price pressure`
还是
- `partially permanent demand curve effect`

### 第三件：如果老师要求识别更强，再做中国样本扩展

这时再往断点回归方向升级，不要一开始就把项目复杂化。

## 5. 一句话总结

如果你现在就要写论文：

- 用 `Harris and Gurel` 决定你怎么做短期事件研究
- 用 `Shleifer` 决定你怎么解释“为什么上涨”
- 用 `沪深300断点回归` 决定你怎么写中国市场部分的识别与稳健性

当前这个项目最适合先完成前两步，并把第三步作为扩展研究。
