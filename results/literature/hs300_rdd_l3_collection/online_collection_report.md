# HS300 RDD 官方线上来源采集报告

- 来源域名：`csindex.com.cn` 与 `oss-ch.csindex.com.cn`
- 搜索诊断：`results/literature/hs300_rdd_l3_collection/online_search_diagnostics.csv`
- 年份覆盖：`results/literature/hs300_rdd_l3_collection/online_year_coverage.csv`
- 人工补录清单：`results/literature/hs300_rdd_l3_collection/online_manual_gap_worklist.csv`
- 缺口来源查找入口：`results/literature/hs300_rdd_l3_collection/online_gap_source_hints.csv`
- 来源审计：`results/literature/hs300_rdd_l3_collection/online_source_audit.csv`
- 候选草稿：`results/literature/hs300_rdd_l3_collection/official_candidate_draft.csv`
- 正式文件：未写入；先保留草稿等待人工确认
- 搜索返回原始行数：`48`
- 标题/主题匹配公告数：`40`
- 日期窗口内匹配公告数：`7`
- 可用官方附件数：`6`
- 已解析但缺备选对照附件数：`0`（调入行 `0`）
- 补录缺口行数：`0`（P1 `0`）
- 缺口来源查找入口数：`0`
- 候选行数：`197`
- 批次数：`5`
- 调入样本数：`122`
- 备选对照数：`75`
- 覆盖 cutoff 两侧批次数：`5`
- 已解析候选年份：`2020 | 2021 | 2022`
- 仅命中公告/附件年份：`无`
- 未命中公告年份：`无`

口径说明：
- 本采集只接受中证指数官网公告详情页及其官方附件。
- 正式调入样本来自“沪深300指数样本调整名单”的调入列。
- 对照样本来自“沪深300指数备选名单”的官方排序。
- `running_variable` 是基于官方调入顺序与备选排序映射出的边界序数变量；它不是单独发布的市值分数。
- 该文件可以替代公开重建 L2 样本进入正式 L3 文件路径，但论文正文应披露上述序数变量口径。

验收命令：
- `index-inclusion-prepare-hs300-rdd --input results/literature/hs300_rdd_l3_collection/official_candidate_draft.csv --check-only`
- `index-inclusion-prepare-hs300-rdd --input results/literature/hs300_rdd_l3_collection/official_candidate_draft.csv --output data/raw/hs300_rdd_candidates.csv --force`
- `index-inclusion-hs300-rdd && index-inclusion-rebuild-all`
