# HS300 RDD 正式数据契约

`start_hs300_rdd.py` 现在默认走“正式候选样本文件”模式。  
只有当 [data/raw/hs300_rdd_candidates.csv](/Users/leonardodon/paper/data/raw/hs300_rdd_candidates.csv) 存在且通过校验时，`RDD` 才会进入正式证据链。

如果文件缺失或不合法：
- 脚本默认写出 `missing` 状态
- 不再自动回退到 demo 系数结果
- 首页只展示“待补正式样本”的说明，不展示伪正式 `tau`

开发调试时，如确实需要跑 demo，请显式使用：

```bash
python3 scripts/start_hs300_rdd.py --demo
```

## 正式文件路径

- 真实候选样本：`data/raw/hs300_rdd_candidates.csv`
- 字段模板：`data/raw/hs300_rdd_candidates.template.csv`

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

## 正式输出

当真实文件通过校验时，会在 [results/literature/hs300_rdd](/Users/leonardodon/paper/results/literature/hs300_rdd) 下生成：

- `rdd_status.csv`
- `summary.md`
- `event_level_with_running.csv`
- `rdd_summary.csv`
- `figures/*.png`

其中 `rdd_status.csv` 是前端和主结果层读取的正式状态源。

## 缺文件或无效文件时

当真实文件缺失或无效时，同一目录下只保留：

- `rdd_status.csv`
- `summary.md`

旧的 `rdd_summary.csv`、事件层文件和图表会被清理掉，避免旧 demo 结果继续被前端误读成正式证据。
