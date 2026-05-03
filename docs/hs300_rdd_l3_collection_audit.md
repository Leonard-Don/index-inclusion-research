# HS300 RDD L3 数据收集审计

本文档审计当前 L3（中证官方候选名单）数据覆盖、与论文级因果识别目标的差距，
以及继续扩展的可行路径。

## 1. 当前 L3 覆盖

`data/raw/hs300_rdd_candidates.csv` 当前包含批次：

| batch_id | 公告时点 |
|---|---|
| csi300-2023-05 | 2023-05 |
| csi300-2023-11 | 2023-11 |
| csi300-2024-05 | 2024-05 |
| csi300-2024-11 | 2024-11 |
| csi300-2025-05 | 2025-05 |
| csi300-2025-11 | 2025-11 |

总行数 159；时间跨度：**2023-05 到 2025-11，共 6 个批次（约 2.5 年）**。

参考 L2 公开重建数据（`hs300_rdd_candidates.reconstructed.csv`）：1887 行，
覆盖更长时间但**不等价于中证官方历史排名**，仅作辅助。

## 2. 论文级目标覆盖

支持 RDD 因果推断需要：

- **样本规模**：≥10 年（约 20 个批次），覆盖 2014–2024。
- **当前缺口**：约 14 个批次（2014-2022）。
- **额外要求**：每批次的 `running_variable` 与 `cutoff` 必须可追溯到中证官方调整名单序，
  不是公开重建反推。

## 3. 已有的可用收集路径

`src/index_inclusion_research/hs300_rdd_online_sources.py` 提供：

- `query_rebalance_announcements(...)`：调用中证指数公司公告搜索接口。
- `fetch_notice_detail(...)`：拉取单条公告 JSON。
- `_attachment_links_from_detail(...)`：解析公告中的附件 PDF 链接。
- `_download_attachment(...)` + `_pdf_to_text(...)`：下载并 OCR 附件 PDF。
- `parse_hs300_attachment_text(...)`：提取 inclusion / exclusion 候选名单。
- `collect_official_hs300_sources(...)`：批量采集入口。
- `build_candidate_rows(...)`：把解析结果落到 candidates.csv 的 schema。

CLI 入口：`index-inclusion-prepare-hs300-rdd`（见 `docs/hs300_rdd_workflow.md`）。

## 4. 推荐执行顺序

1. **2020-2022 批次**（PDF URL 模式应仍然稳定）：
   ```bash
   index-inclusion-prepare-hs300-rdd --since 2020-01-01 --until 2022-12-31
   ```
   预期产出 ~6 批次。每批次跑完后 `git diff data/raw/hs300_rdd_candidates.csv` 校对一遍。

2. **2014-2019 批次**（需要手工补完）：
   - 这部分中证站点可能已经清理了原始 PDF 附件，需要：
     - archive.org Wayback Machine snapshot
     - CNInfo "重要事项"档案
     - `data/raw/hs300_rdd_candidates.template.csv` 提供的字段模板
   - 每填一批跑：
     ```bash
     index-inclusion-doctor
     ```
     确保 schema 与新增批次一致。

3. **校对**：
   - 跑 `index-inclusion-hs300-rdd` 验证 RDD 流水线在新数据上不报错。
   - 跑 `index-inclusion-doctor --fail-on-warn` 确保质控全绿。

4. **切换主表**：
   当 L3 ≥ 1500 行（≈10 年覆盖）后，论文表可正式从 L2 fallback 切到 L3，
   并移除 `docs/limitations.md` §4 的当前警示。

## 5. 风险与限制

- **历史 PDF 链接 404**：中证 2020 年前的部分调整公告 PDF 已不可达；
  archive.org snapshot 命中率约 60-70%。
- **schema drift**：中证早期使用的"新增 / 剔除"列表口径与 2020 之后不完全一致；
  字段映射需要逐批校对（特别是 ticker 前缀、行业分类规则）。
- **ranking score vs adjustment list order**：即便补全 PDF，L3 仍是"官方调整名单序"
  重建，不是 ranking score 本体；论文表达时要标注：
  > 我们用官方调整名单序作为 running variable 的 boundary ordinal proxy；
  > 真实的 ranking score 由中证内部维护，不公开发布。

## 6. 在 L3 ≥10 年补全之前的临时定位

- HS300 RDD 结果在论文中作为 **illustrative / preliminary** 呈现，**不作主表**。
- 主要识别策略仍是 announce vs effective 事件研究 + matched DiD，
  详见 `docs/paper_outline.md` 中的"实证设计"章节。
- 完整方法论限制集中说明见 [`docs/limitations.md`](limitations.md)。
- 本文档下方"决策日志"小节记录每次扩展的日期与触达批次。

## 7. 决策日志

| 日期 | 操作 | 触达批次范围 | 维护者 |
|---|---|---|---|
| 2026-05-03 | 创建本文档；首次审计 L3 = 6 批次（2023-05 → 2025-11） | csi300-2023-05 .. csi300-2025-11 | leo |
| _待填_ | _scrape 2020-2022_ | _待填_ | _待填_ |
| _待填_ | _手工补 2014-2019_ | _待填_ | _待填_ |
| _待填_ | _切换主表到 L3_ | _全量_ | _待填_ |
