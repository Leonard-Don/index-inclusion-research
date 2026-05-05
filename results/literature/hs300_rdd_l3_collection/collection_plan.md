# HS300 RDD L3 正式候选样本采集包

- 当前参考文件：`data/raw/hs300_rdd_candidates.reconstructed.csv`
- 输出目录：`results/literature/hs300_rdd_l3_collection`
- 批次采集清单：`results/literature/hs300_rdd_l3_collection/batch_collection_checklist.csv`
- 正式填报模板：`results/literature/hs300_rdd_l3_collection/formal_candidate_template.csv`
- 边界参考清单：`results/literature/hs300_rdd_l3_collection/boundary_reference.csv`
- 参考批次数：`6`
- 参考候选行数：`1887`
- 边界参考行数：`160`
- 参考批次列表：`2022-05-27, 2023-11-24, 2024-05-31, 2024-11-29, 2025-05-30, 2025-11-28`

采集目标：
- 来源必须是：中证指数官方历史候选名单、公告附件，或人工摘录并可追溯的原始候选表。
- 可以使用当前 L2 重建样本定位批次和边界附近股票，但不能把 L2 running_variable 直接复制成 L3。
- 不要复制公开重建排名口径；正式文件必须来自可追溯的原始候选名单。
- `boundary_reference.csv` 只列出 cutoff 附近的核对优先级，不是正式候选文件。
- 每个批次至少需要 cutoff 左右两侧候选，并同时包含 inclusion=1 与 inclusion=0。

验收步骤：
- `index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --check-only`
- `index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --output data/raw/hs300_rdd_candidates.csv --force`
- `index-inclusion-hs300-rdd && index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma`

阻断规则：
- 如果 source/source_url/note 包含 reconstructed、public reconstruction、not official、公开重建等标记，导入脚本会阻止写入正式 L3 路径。
- 如果缺少 cutoff 两侧覆盖，或缺少处理/对照样本，预检会显示“暂不可接入 L3”。
