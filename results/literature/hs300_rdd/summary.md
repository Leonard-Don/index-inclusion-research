# 制度识别与中国市场证据：断点回归结果包

当前正在使用你提供的真实候选排名文件：`hs300_rdd_candidates.csv`。

当前状态：
- 模式：`real`
- 证据等级：`L3`
- 证据状态：`正式边界样本`
- 来源类型：`official` · 正式候选样本文件
- 来源文件：`data/raw/hs300_rdd_candidates.csv`
- 状态生成时间：`2026-05-05T09:57:54+08:00`
- 批次标签：`11 个批次（csi300-2020-11 至 csi300-2025-11）`
- 对应公告日：`2020-11-27 至 2025-11-28`
- 覆盖说明：356 条候选；11 个批次；调入 191 / 对照 165；11 个批次覆盖 cutoff 两侧。
- 当前口径：基于真实候选排名变量，可作为更强识别证据。
- 候选样本路径：`data/raw/hs300_rdd_candidates.csv`

真实候选样本必需列：
- batch_id
- market
- index_name
- ticker
- security_name
- announce_date
- effective_date
- running_variable
- cutoff
- inclusion

推荐补充列：
- event_type
- source
- source_url
- note
- sector

模板文件：`data/raw/hs300_rdd_candidates.template.csv`
数据契约说明：`docs/hs300_rdd_data_contract.md`

推荐下一步：
- 如果已经拿到原始候选名单，先验收：`index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --sheet 0 --check-only`
- 验收通过后写入正式候选文件：`index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --sheet 0 --output data/raw/hs300_rdd_candidates.csv --force`
- 如果手头没有官方名单，可先重建公开样本：`index-inclusion-reconstruct-hs300-rdd --all-batches --output data/raw/hs300_rdd_candidates.reconstructed.csv --force`
- 如果尚未安装包内 CLI，可改用脚本：`python3 -m index_inclusion_research.prepare_hs300_rdd_candidates --input /path/to/raw_candidates.xlsx --sheet 0 --check-only` 或 `python3 -m index_inclusion_research.reconstruct_hs300_rdd_candidates --all-batches --output data/raw/hs300_rdd_candidates.reconstructed.csv --force`

候选样本审计：
- 批次数：`11`
- 调入样本数：`191`
- 对照候选数：`165`
- 覆盖断点的批次数：`11`
- 批次审计表：`results/literature/hs300_rdd/candidate_batch_audit.csv`

RDD 汇总文件：`results/literature/hs300_rdd/rdd_summary.csv`
事件层文件：`results/literature/hs300_rdd/event_level_with_running.csv`
图表目录：`results/literature/hs300_rdd/figures`
