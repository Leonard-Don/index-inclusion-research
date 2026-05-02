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

## 4. 浏览器 L3 工作台

如果更适合在浏览器里操作，启动 dashboard 后打开 `/rdd-l3`。这个工作台和 `prepare-hs300-rdd` 共享一套预检规则：先一键刷新 L3 采集包，并直接预览批次清单 / 正式填报模板 / 边界参考；拿到官方候选名单后再上传预检字段、来源、cutoff 两侧覆盖和处理 / 对照样本，最后确认写入 `data/raw/hs300_rdd_candidates.csv` 并刷新 RDD 状态与 evidence manifest。
