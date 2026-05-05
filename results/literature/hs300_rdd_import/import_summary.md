# HS300 RDD 候选样本导入摘要

- 原始输入：`results/literature/hs300_rdd_l3_collection/merged_l3_candidate.csv`
- 标准化输出：`data/raw/hs300_rdd_candidates.csv`
- 批次审计：`results/literature/hs300_rdd_import/candidate_batch_audit.csv`
- 原始行数：`356`
- 输出行数：`356`
- 候选批次数：`11`
- 调入样本数：`191`
- 对照候选数：`165`
- 覆盖 cutoff 两侧的批次数：`11`
- L3 导入预检：`可接入 L3`

列映射：
- `announce_date` -> `announce_date`
- `batch_id` -> `batch_id`
- `cutoff` -> `cutoff`
- `effective_date` -> `effective_date`
- `event_type` -> `event_type`
- `inclusion` -> `inclusion`
- `index_name` -> `index_name`
- `market` -> `market`
- `note` -> `note`
- `running_variable` -> `running_variable`
- `sector` -> `sector`
- `security_name` -> `security_name`
- `source` -> `source`
- `source_url` -> `source_url`
- `ticker` -> `ticker`

默认补入字段：
- 无

自动推导字段：
- 无

未使用原始列：
- 无

## L3 导入预检

- 总体结论：`可接入 L3`

预检项目：
- `通过` 字段校验：必需列、日期、数值字段和 inclusion 编码已通过标准校验。
- `通过` 来源层级：输入文件没有公开重建样本标记，可继续按正式候选样本口径预检。
- `通过` 写入模式：本次会写入标准化候选样本、批次审计和导入摘要。
- `通过` 正式样本路径：标准化输出指向 RDD L3 默认候选文件。
- `通过` 批次识别：识别到 11 个调样批次。
- `通过` cutoff 两侧覆盖：11/11 个批次同时覆盖 cutoff 左右两侧。
- `通过` 处理/对照样本：当前包含 191 条调入样本和 165 条对照样本。
- `通过` 来源追踪：每条候选样本都包含 source 和 source_url。

下一步命令：
- `index-inclusion-hs300-rdd`
- `index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma`
