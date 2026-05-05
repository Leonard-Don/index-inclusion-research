# HS300 RDD 公开重建候选样本摘要

- 重建批次数：`6`
- 公告日期范围：`2022-05-27` 至 `2025-11-28`
- 批次列表：`2022-05-27, 2023-11-24, 2024-05-31, 2024-11-29, 2025-05-30, 2025-11-28`
- 标准化输出：`data/raw/hs300_rdd_candidates.reconstructed.csv`
- 批次审计：`results/literature/hs300_rdd_reconstruction/candidate_batch_audit.csv`
- 输出行数：`1887`
- 候选批次数：`6`
- 调入样本数：`1800`
- 对照候选数：`87`
- 覆盖 cutoff 两侧的批次数：`6`

口径说明：
- 当前文件不是中证官方 reserve list，而是根据真实调样批次、当前成分股逆推和公开市值代理口径重建的边界样本。
- `running_variable` 由边界样本内的代理市值降序排名线性映射到 cutoff=300 两侧，便于现有 RDD 流程直接读取。
- 更适合课程论文、方法复现和公开数据版本的稳健性补充，不应表述为官方历史候选排名表。

未纳入本次重建的更早批次：
- `2021-11-26`：Reconstructed pre-review membership for 2021-11-26 has 302 names, expected 300.
- `2020-06-01`：Skipped because a later coverage gap already prevents consistent rollback to this batch.
