# 论文重构实施计划:指数纳入效应是信息效应

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.
> 这是一个 prose(论文)重写任务,不是代码任务——无 TDD;每个 task 的"验证"是
> 数字可追溯 + paper-audit/doctor 不新增失败。

**Goal:** 把 `paper/manuscript.tex` 从"7 假说 CMA + 伪 RDD + 中美不对称"重构为
单一诚实论点——指数纳入效应是公告/信息效应、中美相似。

**Architecture:** 逐章改写 manuscript.tex(论文交付物);复用 `results/real_tables/`
现有事件研究数字,不重跑分析;砍掉 7 假说与 HS300 RDD 主线;最后同步 skeleton.md /
outline / bundle 并校验数字一致。设计稿见
`docs/superpowers/specs/2026-05-22-paper-reframe-design.md`。

**Tech Stack:** XeLaTeX 中文 LaTeX;Markdown;数据源 `results/real_tables/*.csv`;
校验 `index-inclusion-paper-audit`、`index-inclusion-doctor`。

**关键数字(全部来自 `results/real_tables/event_study_summary.csv`,inclusion=1):**

| 窗口 | CN CSI 300 | US S&P 500 |
|---|---|---|
| 公告 CAR[-1,+1] | +1.76% (t=4.93, n=117) | +1.84% (t=5.25, n=254) |
| 生效 CAR[-1,+1] | +0.42% (t=0.93, n=117, n.s.) | −0.14% (t=−0.51, n=257, n.s.) |

长窗口来自 `long_window_event_study_summary.csv`;纳入/剔除不对称来自
`asymmetry_summary.csv`;逐年趋势来自 `time_series_event_study_summary.csv`。

**分支:** `paper-reframe`(已创建)。

---

## Task 1: 摘要 + §1 引言

**Files:** Modify `paper/manuscript.tex`(摘要约 25–35 行;`\section{1. 引言}` 36–55 行)

- [ ] **Step 1: 改写摘要**
  内容要求:(a) 一句话点出问题——指数纳入溢价来自信息认证还是被动需求;
  (b) 数据:2010–2025 S&P 500 619 事件 + 2020–2025 CSI 300 274 事件,公开来源;
  (c) 方法:公告窗 vs 生效窗 × 美国 vs 中国 的事件研究分解;
  (d) 主结果:公告 CAR[-1,+1] 中国 +1.76%、美国 +1.84%,生效窗两市场均不显著;
  (e) 结论:效应集中在公告日、跨制度量级相近 ⇒ 信息/认证渠道。
  删除摘要里关于"7 条机制假说""跨市场不对称""RDD τ"的旧表述。

- [ ] **Step 2: 改写 §1.1 研究背景**
  围绕 Harris-Gurel(1986)/Shleifer(1986)的奠基张力(信息 vs 需求曲线)展开;
  指出现代证据(Greenwood-Sammon 2022)显示机械需求效应在消退——这正是本文要在
  跨市场维度回答的。

- [ ] **Step 3: 改写 §1.2 研究问题 + 贡献**
  研究问题定为单一问题(信息 vs 需求)。贡献写明:系统的中美对照 + "跨制度相似性
  即证据"的明确论证。诚实声明:这是描述性事件研究,非准实验识别。
  删除"七条假说""中美不对称机制"作为贡献的表述。

- [ ] **Step 4: 验证 + 提交**
  检查摘要中每个数字与上表/CSV 一致。
  ```
  git add paper/manuscript.tex && git commit -m "paper: reframe abstract and intro around information-vs-demand thesis"
  ```

---

## Task 2: §2 文献综述

**Files:** Modify `paper/manuscript.tex`(`\section{2. 文献综述}` 56–75 行)

- [ ] **Step 1: 重组文献综述的组织逻辑**
  现结构是 反方/中性/正方(围绕"效应是否存在")。改为围绕**信息 vs 需求**两大解释
  组织:(2.1) 需求曲线/价格压力派——Shleifer(1986)、Wurgler-Zhuravskaya(2002)、
  Harris-Gurel(1986);(2.2) 信息/认证派 与 效应消退证据——Chen-Noronha-Singal
  (2004,纳入/剔除不对称)、Greenwood-Sammon(2022,效应消退);(2.3) 中国市场文献;
  (2.4) 本文定位:用跨市场对照在两派之间做判别。

- [ ] **Step 2: 保留并复用现有文献条目**
  现有 §2 对这些文献的引用与 `paper/references.bib` 条目保留;只改组织框架与
  评述措辞,不删文献。确认 `references.bib` 仍涵盖上述作者。

- [ ] **Step 3: 验证 + 提交**
  ```
  git add paper/manuscript.tex && git commit -m "paper: reorganize literature review around information-vs-demand axis"
  ```

---

## Task 3: §3 研究设计

**Files:** Modify `paper/manuscript.tex`(`\section{3. 研究设计}` 76–130 行)

- [ ] **Step 1: 保留 §3.1 样本与数据**
  现 §3.1(样本与数据)基本保留:619 US + 274 CN、公开来源、Yahoo 价格。
  补一句:CN 可用于公告窗事件研究的有效样本约 117 个(价格完整)。

- [ ] **Step 2: 保留 §3.2 实证方法**
  现 §3.2(事件研究、CAR、Patell Z、BMP t)保留。

- [ ] **Step 3: 用 2×2 识别替换 §3.3**
  删除 `\subsection{3.3 七条跨市场不对称机制假说}` 整节。新写 §3.3「识别策略:
  公告窗 vs 生效窗 × 中美」:公告日有信息无机械调仓;生效日有机械买盘但无新信息
  (提前公告、可预期);美国被动占比高、中国散户主导被动占比低。预测:需求假说 ⇒
  效应在生效窗且美>中;信息假说 ⇒ 效应在公告窗且中美相近。点明这是 Harris-Gurel
  时点分解的跨市场扩展。

- [ ] **Step 4: 验证 + 提交**
  ```
  git add paper/manuscript.tex && git commit -m "paper: replace 7-hypothesis section with announce-vs-effective 2x2 identification"
  ```

---

## Task 4: §4 实证结果

**Files:** Modify `paper/manuscript.tex`(`\section{4. 实证结果}` 131–303 行)

- [ ] **Step 1: §4.1 升为核心结果**
  §4.1 写公告 vs 生效 × 中美 主表(用上表四个数字),逐窗口判读:效应在公告窗显著、
  生效窗两市场均不显著、且公告窗中美量级几乎相同。引出 §3.3 的判别:这指向信息渠道。

- [ ] **Step 2: 删除 §4.2 七假说机制**
  整节删除 `\subsection{4.2 跨市场不对称机制}` 及其下 H1–H7 七个
  `\subsubsection`(151–230 行)。其中两个有独立价值的零件移入 §5 稳健性
  (见 Task 5):H1 预运行 bootstrap、纳入/剔除不对称。

- [ ] **Step 3: 移除 §4.3 HS300 RDD**
  从结果章删除 `\subsection{4.3 HS300 RDD 结果}`。RDD 的处理见 Task 7
  (附录里一段诚实说明:running variable 是构造序号,不构成有效识别)。

- [ ] **Step 4: 重组 §4.4**
  现 §4.4(阈值敏感性 / AR 引擎敏感性 / 联合稳健性)是针对 7 假说裁决机器的——
  删除 4.4.1 阈值敏感性、4.4.2 AR 引擎敏感性、4.4.3 联合稳健性 中专属于 verdict
  机器的内容。事件研究本身的稳健性(AR 引擎、窗口)归入 §5(Task 5)。

- [ ] **Step 5: 验证 + 提交**
  §4.1 每个数字核对 `event_study_summary.csv`。
  ```
  git add paper/manuscript.tex && git commit -m "paper: rebuild results section around announce/effective core, drop H1-H7 and RDD"
  ```

---

## Task 5: §5 稳健性(新)

**Files:** Modify `paper/manuscript.tex`(在原 §4 之后、原 §5 之前新增 `\section{5. 稳健性}`)

- [ ] **Step 1: 新建稳健性章**
  四个小节,均复用现有结果文件,不重跑分析:
  - 纳入/剔除不对称(`asymmetry_summary.csv`):公告窗 纳入 vs 剔除——CN 纳入 +1.76%
    / 剔除 −0.59%;US 纳入 +1.84% / 剔除 +0.05%。
  - 长窗口保留(`long_window_event_study_summary.csv`):CAR[0,+120] 无显著回吐
    (诚实写明长窗口 CI 较宽)。
  - 逐年趋势(`time_series_event_study_summary.csv`):公告效应跨年稳定/未消失。
  - 匹配对照(`results/real_regressions/match_balance.csv`):协变量平衡作为对照稳健性。
  - H1 预运行 bootstrap 作为"无显著预公告漂移"的稳健性零件并入。

- [ ] **Step 2: 验证 + 提交**
  每个数字核对来源 CSV。
  ```
  git add paper/manuscript.tex && git commit -m "paper: add robustness section (asymmetry, long-window, time trend, matching)"
  ```

---

## Task 6: §6 讨论 + §7 结论与局限

**Files:** Modify `paper/manuscript.tex`(原 `\section{5. 限制与讨论}` 304–321、
`\section{6. 结论与启示}` 463–478、`\section{7. 假说的探索性裁决披露}` 479–515)

- [ ] **Step 1: 重写讨论(原 §5 → 新 §6 讨论)**
  讨论:跨制度相似为何指向信息渠道;并列说明生效窗≈0 的两种解释(真无需求效应
  vs 需求被提前套利,Greenwood-Sammon)——不可只取其一。

- [ ] **Step 2: 重写结论(新 §7.1)**
  三点结论:效应集中在公告窗;生效窗两市场均不显著;跨制度相似指向信息/认证。
  删除涉及"7 假说裁决""中美不对称机制""H5/H7 支持"的旧结论。

- [ ] **Step 3: 重写局限(新 §7.2)**
  采用设计稿 §7 的诚实弱点清单:描述性非准实验;论点接近现代共识、增量在跨市场
  对照;CN 仅 ~117 事件/5 年;Yahoo 近似基本面;生效窗多重解释。

- [ ] **Step 4: 收缩 §7 假说披露**
  原 `\section{7. 假说的探索性裁决披露}` 已不再是论文主线——压缩成讨论章里
  一小段:项目早期探索过 7 条机制假说,因 post-hoc 且部分样本极小未纳入本文主线。
  不保留 H1–H7 逐条裁决表。

- [ ] **Step 5: 验证 + 提交**
  ```
  git add paper/manuscript.tex && git commit -m "paper: reframe discussion, conclusion, and limitations to the information thesis"
  ```

---

## Task 7: 嵌入的限制 / 附录 / 方法论卡

**Files:** Modify `paper/manuscript.tex`(嵌入块 322–462 行;附录 519–552 行;
方法论摘要 553–686 行)

- [ ] **Step 1: 更新嵌入的"数据与方法限制"块(322–462 行)**
  保留数据契约、事件清单、事件研究方法、被动 AUM 说明。
  - `\section{4. HS300 RDD 数据层级}`:改写为一段诚实说明——RDD 曾被探索,但其
    running variable 是由调整名单序号构造(`(2·cutoff+1) − rank`)、与处理变量
    完全共线,不构成有效断点识别,故不进入本文;保留作为方法学教训。
  - `\section{7. CMA 假说证据强度分层}`:删除或压缩(7 假说已退出主线)。
  - `\section{6. 多重检验与 post-hoc 披露}`:保留 post-hoc 诚实披露,但调整为
    "项目早期探索"语境。

- [ ] **Step 2: 更新附录**
  附录 A 数据契约、C 复现指南保留。附录 B「CLI 入口(48 个)」保留。
  方法论摘要卡(553–686 行)中涉及"7 假说/RDD 为核心证据"的措辞改为与新主线一致;
  数字不动。

- [ ] **Step 3: 验证 + 提交**
  ```
  git add paper/manuscript.tex && git commit -m "paper: update embedded limitations and appendix for the reframe"
  ```

---

## Task 8: 同步骨架 / 大纲 + 重生成 bundle + 校验

**Files:** Modify `paper/skeleton.md`、`docs/paper_outline.md`;运行 bundle 与校验。

- [ ] **Step 1: 同步 `paper/skeleton.md`**
  把 skeleton.md 的章节结构改为与新 manuscript.tex 一致(7 章新结构),骨架级
  内容(章节标题 + 每章一两句要点),不逐字复制全文。删除其 §4.2 H1–H7、
  §4.3 RDD 小节。

- [ ] **Step 2: 同步 `docs/paper_outline.md`**
  paper_outline.md 是写作模板;把"二、理论机制与研究假说"(2.1 需求冲击/
  2.2 流动性关注/2.3 信息背书)与"4.3 机制检验/4.4 RDD"更新为信息-vs-需求 +
  2×2 识别的写法。

- [ ] **Step 3: 重生成论文 bundle**
  ```
  index-inclusion-paper-bundle --force
  ```
  确认 `paper/narrative/` 副本、`paper/bundle_summary.md` 随之更新。

- [ ] **Step 4: 校验数字与一致性**
  ```
  index-inclusion-paper-audit
  make doctor
  ```
  Expected: paper-audit 不因改写新增失败;doctor 不新增 fail。若 paper-audit
  报告某条主张找不到证据,多半是旧的 7 假说/RDD 主张残留——回到对应 Task 清理。

- [ ] **Step 5: 通读全文一致性**
  通读 manuscript.tex:确认全文不再有"中美不对称"作为卖点、无"7 假说裁决"主线、
  无 RDD 因果声明;摘要数字 = §4.1 数字 = CSV 数字。

- [ ] **Step 6: 提交**
  ```
  git add paper/skeleton.md docs/paper_outline.md paper/
  git commit -m "paper: sync skeleton/outline and regenerate bundle for the reframe"
  ```

---

## 完成标准

- `paper/manuscript.tex` 围绕单一论点(信息效应、中美相似)组织,7 章新结构。
- 无 7 假说裁决主线、无 RDD 因果声明、无"中美不对称"卖点。
- 摘要 / §4 / CSV 三处数字一致。
- `index-inclusion-paper-audit` 与 `make doctor` 不因改写新增失败。
- 全部改动在 `paper-reframe` 分支,逐 Task 提交。
