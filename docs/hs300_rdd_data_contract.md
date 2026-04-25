# HS300 RDD 正式数据契约

`index-inclusion-hs300-rdd` 现在默认走“正式候选样本文件”模式。  
当 [data/raw/hs300_rdd_candidates.csv](/Users/leonardodon/index-inclusion-research/data/raw/hs300_rdd_candidates.csv) 存在且通过校验时，`RDD` 会进入 `L3` 正式边界样本；当 [data/raw/hs300_rdd_candidates.reconstructed.csv](/Users/leonardodon/index-inclusion-research/data/raw/hs300_rdd_candidates.reconstructed.csv) 存在且通过校验时，`RDD` 会进入 `L2` 公开重建样本。

如果文件缺失或不合法：
- 命令默认写出 `missing` 状态
- 不再自动回退到 demo 系数结果
- 首页只展示“待补正式样本”的说明，不展示伪正式 `tau`

开发调试时，如确实需要跑 demo，请显式使用：

```bash
index-inclusion-hs300-rdd --demo
```

如果你拿到的是原始候选名单 Excel / CSV，推荐先用导入脚本做标准化、字段补齐和批次审计：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.xlsx \
  --sheet 0 \
  --announce-date 2024-11-29 \
  --effective-date 2024-12-16 \
  --source CSIndex \
  --source-url https://www.csindex.com.cn/
```

如果只想先检查、不覆盖正式候选文件，可以改用：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.xlsx \
  --check-only
```

导入脚本会同步输出 `L3 导入预检`：它会检查字段校验、是否写入默认正式路径、批次是否覆盖 cutoff 两侧、是否同时有调入/对照样本，以及 source/source_url 是否完整。预检不是另一个数据入口，而是正式文件写入前的验收清单。

如果还没有正式原始候选表，先生成 L3 采集包：

```bash
index-inclusion-plan-hs300-rdd-l3 --force
```

它会基于当前 L2 公开重建批次生成：

- `results/literature/hs300_rdd_l3_collection/batch_collection_checklist.csv`
- `results/literature/hs300_rdd_l3_collection/formal_candidate_template.csv`
- `results/literature/hs300_rdd_l3_collection/boundary_reference.csv`
- `results/literature/hs300_rdd_l3_collection/collection_plan.md`

`boundary_reference.csv` 默认每个批次在 cutoff 左右各列出 15 个最近的 L2 重建候选，方便优先核对正式来源；它只是采集参考，不能直接复制为 L3 的 `running_variable`。

## 正式文件路径

- 真实候选样本：`data/raw/hs300_rdd_candidates.csv`
- 字段模板：`data/raw/hs300_rdd_candidates.template.csv`
- 导入命令：`index-inclusion-prepare-hs300-rdd`
- 采集包命令：`index-inclusion-plan-hs300-rdd-l3`

## 必需列

- `batch_id`
- `market`
- `index_name`
- `ticker`
- `security_name`
- `announce_date`
- `effective_date`
- `running_variable`
- `cutoff`
- `inclusion`

## 推荐列

- `event_type`
- `source`
- `source_url`
- `note`
- `sector`

## 字段要求

- `batch_id`
  - 同一调样批次的唯一标识，例如 `2024-11-29`
- `market`
  - 当前正式使用场景应为 `CN`
- `index_name`
  - 当前正式使用场景应为 `CSI300`
- `ticker`
  - 候选股票代码
- `security_name`
  - 候选股票名称
- `announce_date`
  - 批次公告日，必须能被解析为有效日期
- `effective_date`
  - 批次生效日，必须能被解析为有效日期
- `running_variable`
  - 断点回归中的 running variable，必须是数值
- `cutoff`
  - 断点位置，必须是数值
- `inclusion`
  - 是否实际调入，只允许 `0` 或 `1`

## 最小示例

```csv
batch_id,market,index_name,ticker,security_name,announce_date,effective_date,running_variable,cutoff,inclusion,event_type,source,source_url,note,sector
2024-11-29,CN,CSI300,000686,东北证券,2024-11-29,2024-12-16,300.22,300,1,addition,CSIndex,https://www.csindex.com.cn/,manual transcription,Financials
2024-11-29,CN,CSI300,000001,平安银行,2024-11-29,2024-12-16,299.91,300,0,borderline,CSIndex,https://www.csindex.com.cn/,manual transcription,Financials
```

## 校验失败会怎样

下面这些情况都会被视为“无效正式文件”：

- 缺少必需列
- 必需列存在空值
- `announce_date` 或 `effective_date` 不是合法日期
- `running_variable` 或 `cutoff` 不是数值
- `inclusion` 不是 `0/1`

命令行为：

- 命令行直接运行时：明确报错，不回退 demo
- 仪表盘刷新或研究主线联动时：写出 `missing` 状态，保留方法说明，但不展示正式 `RDD` 系数
- 导入脚本运行时：会先报告列映射、默认补入字段和 cutoff 两侧覆盖情况；未通过校验时不会写入正式候选文件

## L3 导入预检口径

导入脚本会给出三类总体结论：

- `可接入 L3`
  - 标准化输出指向 `data/raw/hs300_rdd_candidates.csv`
  - 至少有一个调样批次
  - 每个批次都覆盖 cutoff 左右两侧
  - 同时存在 `inclusion=1` 和 `inclusion=0`
  - 每条样本都有 source/source_url
- `可接入但需补充`
  - 样本可计算，但存在非阻断提醒，例如 check-only 未写入、输出路径不是默认正式路径、部分来源链接缺失，或只有部分批次覆盖 cutoff 两侧
- `暂不可接入 L3`
  - 存在阻断项，例如输入是公开重建样本、没有 cutoff 两侧覆盖，或只有处理组/只有对照组

特别注意：`data/raw/hs300_rdd_candidates.reconstructed.csv` 是 L2 公开重建样本，只能验证导入链路和公开证据口径，不能直接写入 `data/raw/hs300_rdd_candidates.csv` 伪装成 L3 正式候选样本。导入脚本不仅会检查默认重建文件路径，也会检查 `source/source_url/note` 中的 reconstructed / public reconstruction / not official / 公开重建 等来源标记，避免复制改名后的重建样本被误升为 L3。正式 L3 需要中证官方历史候选名单或人工摘录的原始 Excel/CSV。

通过或提醒状态下，脚本会在控制台给出下一步命令；非 `--check-only` 写入时也会同步保存到 `results/literature/hs300_rdd_import/import_summary.md`：

```bash
index-inclusion-hs300-rdd
index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma
```

## 正式输出

当真实文件通过校验时，会在 [results/literature/hs300_rdd](/Users/leonardodon/index-inclusion-research/results/literature/hs300_rdd) 下生成：

- `rdd_status.csv`
- `summary.md`
- `candidate_batch_audit.csv`
- `event_level_with_running.csv`
- `rdd_summary.csv`
- `figures/*.png`

其中 `rdd_status.csv` 是前端和主结果层读取的正式状态源。

当前 `rdd_status.csv` 除了 `status / evidence_tier / evidence_status` 之外，还会固定写出一组 provenance 字段，用来回答“当前这版 RDD 到底来自哪份样本”：

- `source_kind`
- `source_label`
- `source_file`
- `generated_at`
- `as_of_date`
- `batch_label`
- `coverage_note`

这组字段会被首页 KPI、识别状态卡和研究边界页同时消费，避免页面各自猜测来源口径。

导入脚本默认会把前置验收结果写到 `results/literature/hs300_rdd_import/`：

- `candidate_batch_audit.csv`
- `import_summary.md`

## 缺文件或无效文件时

当真实文件缺失或无效时，同一目录下只保留：

- `rdd_status.csv`
- `summary.md`

旧的 `rdd_summary.csv`、事件层文件和图表会被清理掉，避免旧 demo 结果继续被前端误读成正式证据。
