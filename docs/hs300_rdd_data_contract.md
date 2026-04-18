# HS300 RDD 正式数据契约

`start_hs300_rdd.py` 现在默认走“正式候选样本文件”模式。  
只有当 [data/raw/hs300_rdd_candidates.csv](/Users/leonardodon/index-inclusion-research/data/raw/hs300_rdd_candidates.csv) 存在且通过校验时，`RDD` 才会进入正式证据链。

如果文件缺失或不合法：
- 脚本默认写出 `missing` 状态
- 不再自动回退到 demo 系数结果
- 首页只展示“待补正式样本”的说明，不展示伪正式 `tau`

开发调试时，如确实需要跑 demo，请显式使用：

```bash
python3 scripts/start_hs300_rdd.py --demo
```

如果你拿到的是原始候选名单 Excel / CSV，推荐先用导入脚本做标准化、字段补齐和批次审计：

```bash
python3 scripts/prepare_hs300_rdd_candidates.py \
  --input /path/to/raw_candidates.xlsx \
  --sheet 0 \
  --announce-date 2024-11-29 \
  --effective-date 2024-12-16 \
  --source CSIndex \
  --source-url https://www.csindex.com.cn/
```

如果已经安装了项目，也可以改用包内 CLI：

```bash
index-inclusion-prepare-hs300-rdd \
  --input /path/to/raw_candidates.xlsx \
  --sheet 0 \
  --check-only
```

如果只想先检查、不覆盖正式候选文件，可以改用：

```bash
python3 scripts/prepare_hs300_rdd_candidates.py \
  --input /path/to/raw_candidates.xlsx \
  --check-only
```

## 正式文件路径

- 真实候选样本：`data/raw/hs300_rdd_candidates.csv`
- 字段模板：`data/raw/hs300_rdd_candidates.template.csv`
- 导入脚本：`scripts/prepare_hs300_rdd_candidates.py`

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

脚本行为：

- 命令行直接运行时：明确报错，不回退 demo
- 仪表盘刷新或研究主线联动时：写出 `missing` 状态，保留方法说明，但不展示正式 `RDD` 系数
- 导入脚本运行时：会先报告列映射、默认补入字段和 cutoff 两侧覆盖情况；未通过校验时不会写入正式候选文件

## 正式输出

当真实文件通过校验时，会在 [results/literature/hs300_rdd](/Users/leonardodon/index-inclusion-research/results/literature/hs300_rdd) 下生成：

- `rdd_status.csv`
- `summary.md`
- `candidate_batch_audit.csv`
- `event_level_with_running.csv`
- `rdd_summary.csv`
- `figures/*.png`

其中 `rdd_status.csv` 是前端和主结果层读取的正式状态源。

导入脚本默认会把前置验收结果写到 `results/literature/hs300_rdd_import/`：

- `candidate_batch_audit.csv`
- `import_summary.md`

## 缺文件或无效文件时

当真实文件缺失或无效时，同一目录下只保留：

- `rdd_status.csv`
- `summary.md`

旧的 `rdd_summary.csv`、事件层文件和图表会被清理掉，避免旧 demo 结果继续被前端误读成正式证据。
