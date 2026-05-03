# HS300 RDD 候选样本工作流

RDD 默认不再自动回退 demo。

- 当 [data/raw/hs300_rdd_candidates.csv](../data/raw/hs300_rdd_candidates.csv) 存在且通过校验时，进入 `L3` 正式边界样本
- 当 [data/raw/hs300_rdd_candidates.reconstructed.csv](../data/raw/hs300_rdd_candidates.reconstructed.csv) 存在且通过校验时，进入 `L2` 公开重建样本
- 显式 demo：`index-inclusion-hs300-rdd --demo`

正式字段模板见 [data/raw/hs300_rdd_candidates.template.csv](../data/raw/hs300_rdd_candidates.template.csv)；数据契约见 [docs/hs300_rdd_data_contract.md](hs300_rdd_data_contract.md)。

## 1. 标准化原始候选名单

拿到沪深 300 调样候选名单后，先用导入脚本做标准化和校验：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.csv \
  --check-only
```

或 `python3 -m index_inclusion_research.prepare_hs300_rdd_candidates --input ... --check-only`。

确认通过后，写入正式候选文件：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.csv \
  --output data/raw/hs300_rdd_candidates.csv \
  --force
```

脚本默认会补 `market=CN`、`index_name=CSI300`、`cutoff=300`，并在 `results/literature/hs300_rdd_import/` 下生成 `candidate_batch_audit.csv` 和 `import_summary.md`。

如果原始列名不标准（CSV / Excel）：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.xlsx \
  --sheet 0 \
  --announce-date 2024-11-29 \
  --effective-date 2024-12-16 \
  --source CSIndex \
  --source-url https://www.csindex.com.cn/
```

## 2. 公开口径重建（L2 退路）

如果手里没有官方候选排名表，可以基于公开口径重建一版边界样本：

```bash
index-inclusion-reconstruct-hs300-rdd \
  --all-batches \
  --output data/raw/hs300_rdd_candidates.reconstructed.csv \
  --force
```

这条路径用当前 CSI300 成分股、后续真实调样批次回滚、以及公开价格 / 总股本代理口径，优先重建当前事件源里"可以稳定回滚"的连续批次后缀。如果只想做单批次：`--announce-date 2024-05-31`。它适合课程论文、方法复现和公开数据稳健性补充，但不应表述为中证官方历史候选排名表。

## 3. L3 采集计划

如果已经准备去采集 L3 正式候选样本（中证官方 / 公告附件 / 人工摘录），先生成一份采集包：

```bash
index-inclusion-plan-hs300-rdd-l3 --force
```

它默认读 `data/raw/hs300_rdd_candidates.reconstructed.csv` 作为参考批次，在 `results/literature/hs300_rdd_l3_collection/` 下生成：

- `batch_collection_checklist.csv`：每批次的 `acceptance_command` / `write_command` / `refresh_command`
- `formal_candidate_template.csv`：可以直接补字段写正式候选名单的模板
- `boundary_reference.csv`：每批次 cutoff 附近的参考排名快照
- `collection_plan.md`：人类可读的采集计划摘要

如果允许联网，可以直接采集中证官网公告附件：

```bash
index-inclusion-collect-hs300-rdd-l3 --force
```

它会下载并解析中证官网“定期调整结果”公告附件中的沪深300调入名单与备选名单，生成 `official_candidate_draft.csv`、`online_source_audit.csv`、`online_search_diagnostics.csv`、`online_year_coverage.csv`、`online_manual_gap_worklist.csv`、`online_gap_source_hints.csv` 和 `online_collection_report.md`。PDF 附件若同时包含调入名单与备选名单，会进入候选草稿；Excel 附件若只有调入/调出名单，会写入来源审计并标记为缺少 reserve controls，不会被提升为正式 L3；人工补录清单会把这类缺口排成 P1/P2/P3，来源查找入口会给出中证详情页、附件、Wayback、网页搜索和巨潮全文搜索。确认草稿后写入正式 L3：

如果只想先补历史窗口，比如 2020-2022 批次：

```bash
index-inclusion-collect-hs300-rdd-l3 \
  --since 2020-01-01 \
  --until 2022-12-31 \
  --notice-rows 120 \
  --search-term "调整沪深300指数样本股" \
  --force
```

`--since` / `--until` 按公告发布日期过滤，`--notice-rows` 控制每个中证搜索词返回的公告数量，`--search-term` 可以重复传入，用来补历史公告标题口径。若没有解析出候选行，先看 `online_search_diagnostics.csv` 判断是搜索接口无返回、标题/主题过滤未命中，还是命中公告但落在日期窗口之外；再看 `online_year_coverage.csv` 判断缺口集中在哪些年份；最后按 `online_manual_gap_worklist.csv` 的 P1/P2/P3 顺序补官方 reserve/control 证据，并用 `online_gap_source_hints.csv` 逐条打开来源查找入口。

```bash
index-inclusion-prepare-hs300-rdd \
  --input results/literature/hs300_rdd_l3_collection/official_candidate_draft.csv \
  --output data/raw/hs300_rdd_candidates.csv \
  --force
```

## 4. 浏览器 L3 工作台

如果更适合在浏览器里操作，启动 dashboard 后打开 `/rdd-l3`。这个工作台和 `prepare-hs300-rdd` 共享一套预检规则：先一键刷新 L3 采集包，并直接预览批次清单 / 正式填报模板 / 边界参考；同时读取 `online_search_diagnostics.csv`、`online_year_coverage.csv`、`online_source_audit.csv`、`online_manual_gap_worklist.csv` 与 `online_gap_source_hints.csv`，把线上搜索命中、年份覆盖、附件审计、`notice_only` 补录缺口和可点击来源入口展示出来。拿到官方候选名单后再上传预检字段、来源、cutoff 两侧覆盖和处理 / 对照样本，最后确认写入 `data/raw/hs300_rdd_candidates.csv` 并刷新 RDD 状态与 evidence manifest。
