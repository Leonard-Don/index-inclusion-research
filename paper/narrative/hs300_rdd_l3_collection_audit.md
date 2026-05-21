# HS300 RDD L3 数据收集审计

本文档审计当前 L3（中证官方候选名单）数据覆盖、与论文级因果识别目标的差距，
以及继续扩展的可行路径。

## 1. 当前 L3 覆盖

`data/raw/hs300_rdd_candidates.csv` 当前包含批次：

| batch_id | 公告时点 |
|---|---|
| csi300-2020-11 | 2020-11 |
| csi300-2021-05 | 2021-05 |
| csi300-2021-11 | 2021-11 |
| csi300-2022-05 | 2022-05 |
| csi300-2022-11 | 2022-11 |
| csi300-2023-05 | 2023-05 |
| csi300-2023-11 | 2023-11 |
| csi300-2024-05 | 2024-05 |
| csi300-2024-11 | 2024-11 |
| csi300-2025-05 | 2025-05 |
| csi300-2025-11 | 2025-11 |

总行数 356；时间跨度：**2020-11 到 2025-11，共 11 个批次（5 年）**。

参考 L2 公开重建数据（`hs300_rdd_candidates.reconstructed.csv`）：1887 行，
覆盖更长时间但**不等价于中证官方历史排名**，仅作辅助。

## 2. 论文级目标覆盖

支持 RDD 因果推断需要：

- **样本规模**：≥10 年（约 20 个批次），覆盖 2014–2024。
- **当前缺口**：约 9 个批次（2014-2019），全部卡在 CSIndex API 路径之外，需要外部档案介入。
- **额外要求**：每批次的 `running_variable` 与 `cutoff` 必须可追溯到中证官方调整名单序，
  不是公开重建反推。

## 3. 已有的可用收集路径

`src/index_inclusion_research/hs300_rdd_online_sources.py` 提供：

- `query_rebalance_announcements(...)`：调用中证指数公司公告搜索接口。
- `fetch_notice_detail(...)`：拉取单条公告 JSON。
- `_attachment_links_from_detail(...)`：解析公告中的附件 PDF 链接。
- `_download_attachment(...)` + `_pdf_to_text(...)`：下载并 OCR 附件 PDF。
- `parse_hs300_attachment_text(...)`：提取 inclusion / exclusion 候选名单。
- `parse_hs300_excel_attachment(...)`：读取 XLS/XLSX 调入/调出名单；若缺少备选名单，只进入来源审计，不写入正式 L3 候选。
- `collect_official_hs300_sources(...)`：批量采集入口。
- `build_candidate_rows(...)`：把解析结果落到 candidates.csv 的 schema。
- `online_search_diagnostics.csv`：记录每个搜索词的原始返回、HS300 标题命中、历史标题模式命中、主题命中和日期窗口内命中。
- `online_year_coverage.csv`：按年份标记 `candidate_found` / `notice_only` / `no_notice`，用于决定下一轮先调搜索词还是先找替代档案源。
- `online_manual_gap_worklist.csv`：把 notice-only 和 addition-only 附件转成 P1/P2/P3 补录任务；P1 代表已有调入但缺官方 reserve/control 证据。
- `online_gap_source_hints.csv`：把每个缺口展开为中证详情页、官方附件、Wayback、站内网页搜索和巨潮全文搜索入口；它只提供查找线索，不替代正式 L3 证据。

CLI 入口：`index-inclusion-collect-hs300-rdd-l3`（见 `docs/hs300_rdd_workflow.md`）。
浏览器入口：`/rdd-l3` 的“刷新线上诊断”表单复用同一采集器，可填写公告日期窗口、每个搜索词返回行数、最多公告数和补充搜索词。

## 4. 推荐执行顺序

1. **2020-2022 批次**（PDF URL 模式应仍然稳定）：
   ```bash
   index-inclusion-collect-hs300-rdd-l3 \
     --since 2020-01-01 \
     --until 2022-12-31 \
     --notice-rows 120 \
     --search-term "调整沪深300指数样本股" \
     --force
   ```
   预期先产出 `official_candidate_draft.csv`、`online_source_audit.csv`、`online_search_diagnostics.csv`、`online_year_coverage.csv`、`online_manual_gap_worklist.csv`、`online_gap_source_hints.csv` 与 `online_collection_report.md`。若 `online_year_coverage.csv` 显示 `no_notice`，优先追加历史标题 `--search-term`；若显示 `notice_only`，优先检查附件 URL、PDF/Excel 文本格式和解析规则。Excel 只含调入/调出名单时，必须继续补备选名单或其他官方边界证据，不能直接写入正式 L3。补录顺序以 `online_manual_gap_worklist.csv` 的 P1/P2/P3 为准；来源查找顺序以 `online_gap_source_hints.csv` 的官方详情页、附件、Wayback 和全文搜索入口为准；每批次校对通过后再用 `index-inclusion-prepare-hs300-rdd --input ... --check-only` 验收。

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

- **CSIndex API 在 2020 之前不发布备选名单（实证结论）**：2026-05-03 的 13 词搜索（包含 `沪深300备选 / 备选名单 / 样本备选 / 备选股票` 四个专项词）确认 CSIndex 历史档案没有任何"备选"专项公告；2015-12-28 #2882 唯一可解析的旧格式 Excel 也只有调入/调出列，无 `备选名单` sheet。要扩到 2014-2019，唯一路径是外部档案：
  - archive.org Wayback Machine snapshot（命中率约 60-70%，但快照命中只是 web 页面，背后的附件文件和当前 CSIndex 一样不含 reserve list）
  - CNInfo "重要事项"档案（上市公司端披露，可能包含被调入信息但通常不含完整候选排名）
  - Wind / iFinD / CSMAR 等付费数据库（学术界通常路径，含历史 CSI300 候选排名表）
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
| 2026-05-03 | 为线上采集新增历史搜索词、搜索诊断和年份覆盖输出 | 2020-2022 优先诊断窗口 | leo |
| 2026-05-03 | 跑 collect-hs300-rdd-l3 `--since 2020-01-01 --until 2022-12-31`（3 个补充搜索词）。命中 5 个公告（2020-11、2021-05、2021-11、2022-05、2022-11）+ 6 个官方 Excel 附件，但全部为 addition-only 名单，**无 reserve/control list**。0 行进入 L3 草稿；9 行写入 manual_gap_worklist（P2，需手工 archive 检索）。结论：2020-2022 缺口是结构性 reserve-list 缺失，不是搜索词问题，需走 Wayback / CNInfo / 站内 web 搜索补 reserve 名单。 | 2020-2022 收口诊断 | claude |
| 2026-05-03 | **结论修正**：人工 inspect Excel 附件后发现每个 2020-2022 文件都有 `调入 / 调出 / 备选名单` 三个 sheet，`备选名单` 含 `排序` 列直接给 RDD running variable。原 parser 只处理 6 列单 sheet 格式（2025+），漏读多 sheet 格式。修补 `_excel_single_role_rows` + `_excel_reserve_rows` + 按 sheet 名 dispatch；补 `_infer_csi300_effective_date` fallback（2nd Friday of next month），覆盖 2010-2025 所有批次。再跑 collector 得 5 个新批次 197 行，合并入正式 L3 → 11 批次 356 行。doctor 13/0/0；verdicts diff vs PAP 基线：0 changed；RDD `car_m1_p1` tau=0.039, p=0.048（n=120，新 L3）。 | csi300-2020-11 .. csi300-2022-11 | claude（PAP §7 已记录）|
| 2026-05-03 | **2010-2019 CSIndex 路径定论（负发现）**：跑 collector `--since 2010-01-01 --until 2019-12-31`，13 个搜索词（调整 + 备选两组）。CSIndex API 历史命中只有 2005-06、2005-12、2011-08、2013-08、2015-01、2015-12、2016-11 共 7 条，2014/2017/2018/2019 完全 `no_notice`。唯一可下载并解析的是 2015-12-28 #2882（`20151228cons.xls`），但单 sheet 6 列格式 + **无 `备选名单` sheet**，结构上不可能给 reserve list。"沪深300备选 / 备选名单 / 样本备选 / 备选股票"四个专项搜索词全 0 命中。结论：CSIndex 在 2020 之前没有把备选名单作为公开发布形式，CSIndex API 单一路径已经穷尽 → 11 批次 / 5 年 是当前可以到的上限。要继续扩到 ≥20 批次 / 10 年只能走 Wayback Machine / CNInfo / Wind / iFinD / CSMAR 等外部档案，**这是研究级数据采购任务**，不再是 collector 代码问题。 | 2010-2019 全窗口；2014/2017/2018/2019 完全无证据 | claude |
| 2026-05-03 | **主表切换决策**：当前 L3 = 11 批次 / 5 年，仍 < ≥20 批次 / 10 年门槛。HS300 RDD 主表用法继续保持 **illustrative / preliminary**；论文正文按附录 / 方法论补充章节呈现。门槛不动；触发条件挂在外部档案（Wayback / 付费数据库）。 | §3 RDD 主表用法；§6 临时定位 | claude（PAP §3 不变）|
| _待填_ | _Wayback / 付费数据库扩 2010-2019_ | _2010-2019 全部_ | _待填（外部依赖）_ |
| _待填_ | _切换主表到 L3_ | _全量_ | _待填（卡 ≥20 批次门槛）_ |
