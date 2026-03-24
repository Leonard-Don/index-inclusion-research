# HS300 RDD 数据契约

如果你想让 `start_hs300_rdd.py` 跑出真正接近论文的断点回归证据，请准备：

- 路径：`data/raw/hs300_rdd_candidates.csv`

## 必需列

- `batch_id`
- `market`
- `index_name`
- `ticker`
- `announce_date`
- `effective_date`
- `inclusion`
- `running_variable`
- `cutoff`

## 含义

- `batch_id`
  - 一次调样批次的唯一标识，比如 `2024-11-29`
- `inclusion`
  - 是否实际被调入，`1` 表示调入，`0` 表示未调入但接近边界
- `running_variable`
  - 断点回归里的 running variable，通常是“进入指数的排序得分”或“排名分数”
- `cutoff`
  - 断点位置，比如第 300 名的阈值

## 最小示例

```csv
batch_id,market,index_name,ticker,announce_date,effective_date,inclusion,running_variable,cutoff
2024-11-29,CN,CSI300,000686,2024-11-29,2024-12-16,1,300.22,300
2024-11-29,CN,CSI300,000001,2024-11-29,2024-12-16,0,299.91,300
2024-11-29,CN,CSI300,000002,2024-11-29,2024-12-16,0,299.73,300
```

## 当前脚本会做什么

1. 对每个候选样本构建事件窗口
2. 计算 `car_m1_p1`、`car_m3_p3`、`turnover_change`、`volume_change`
3. 在 cutoff 两侧做 local linear RD
4. 输出 `rdd_summary.csv`
5. 画每个 outcome 的断点分箱图

## 当前的边界

- 如果你没有提供 `hs300_rdd_candidates.csv`，脚本会自动生成 `demo` 版伪排名数据，以便展示流程。
- `demo` 版只用于跑通方法，不应当当作正式论文证据。
