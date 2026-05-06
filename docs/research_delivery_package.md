# 论文与答辩交付包

本文档是当前研究版本的写作和答辩总入口。它不替代自动产出的结果表，
而是规定哪些证据进入正文、哪些证据只进附录，以及答辩时应该如何表述边界。

## 1. 一句话结论

指数纳入事件在中美市场都主要表现为**公告日显著正向超额收益**，而不是生效日集中跳涨：
CN inclusion 的公告日 `CAR[-1,+1]` 为 **+1.75%** (t=4.93, p<0.001)，
US inclusion 为 **+1.47%** (t=5.19, p<0.001)；生效日窗口在两国市场都不显著。

论文主线因此应写成：

> 指数纳入效应仍然存在，但主要在公告阶段完成定价；不同市场的制度、行业结构和交易限制
> 影响这一价格反应的传导机制。被动资金需求冲击是候选机制之一，但当前结果不支持把上涨
> 主要归因于生效日机械买盘。

## 2. 正文证据边界

正文只保留三类证据：

| 位置 | 证据 | 当前口径 | 来源 |
|---|---|---|---|
| 主结果 | 事件研究 `CAR[-1,+1]` | 公告日显著，生效日不显著 | `results/real_tables/event_study_summary.csv` |
| 方法稳健性 | Patell/BMP 标准化异常收益 | CN/US inclusion 公告日仍显著 | `results/real_event_study/patell_bmp_summary.csv` |
| 机制主表 | `evidence_tier=core` 的 CMA 假说 | H1 证据不足，H5 支持，H7 支持 | `results/real_tables/cma_hypothesis_verdicts.csv` |

正文不要把 7 条 CMA 假说全部并列成主表。当前 PAP 明确规定：

- **正文 core**：H1 信息泄露与预运行、H5 涨跌停限制、H7 行业结构差异。
- **附录 supplementary**：H2 被动基金 AUM、H3 散户 vs 机构结构、H4 卖空约束、H6 指数权重可预测性。
- **HS300 RDD**：附录 / 方法论补充，定位为 preliminary，不进入主表。

## 3. 推荐论文结构

### 引言

先回答“是否上涨”：公告日 CAR 在 CN 和 US 都显著为正。随后提出真正的问题：
既然生效日不显著，为什么市场提前定价，且中美市场的机制证据不同？

### 数据与样本

引用 `results/real_tables/sample_scope.csv`、`event_counts.csv`、`data_sources.csv`。
必须同时引用 [docs/limitations.md](limitations.md)，尤其说明 `mkt_cap` 和 `turnover`
使用 Yahoo `sharesOutstanding` 近似，不是交易所历史自由流通口径。

### 研究设计

主设计包括：

1. 公告日 / 生效日双窗口事件研究。
2. 匹配样本回归与 covariate balance 检查。
3. CMA 机制假说裁决，正文只引用 core 层。
4. HS300 RDD 作为附录识别补充。

不要把 HS300 RDD 写成唯一因果识别主轴；当前 L3 只有 11 批次 / 356 行，
仍低于 ≥20 批次 / ≥10 年的论文级门槛。

### 实证结果

正文结果建议按这个顺序写：

1. **主结果**：公告日显著正向，生效日不显著。
2. **标准化稳健性**：Patell/BMP 仍支持 announcement inclusion effect。
3. **机制裁决**：H5 和 H7 支持，H1 不支持信息泄露解释。
4. **限制性识别补充**：RDD main spec 显著，但 donut / polynomial 提示设定敏感。

### 结论

结论应强调“公告期定价 + 制度机制差异”，而不是泛泛写“指数基金买入导致上涨”。
更稳妥的表述是：

> 本研究支持指数纳入公告带来的短期价格反应，但当前证据更像是公告阶段的信息、
> 注意力和制度约束共同作用，而不是生效日被动资金机械买盘的单一解释。

## 4. 主表与附录清单

| 类型 | 建议标题 | 使用文件 |
|---|---|---|
| 表 1 | 样本覆盖与数据来源 | `event_counts.tex`、`sample_scope.csv`、`data_sources.tex` |
| 表 2 | 公告日 / 生效日事件研究 | `event_study_summary.tex` |
| 表 3 | CMA core 假说裁决 | `cma_hypothesis_verdicts.csv` 中 `evidence_tier=core` |
| 图 1 | CAR path by market and event phase | `real_figures/*_car_path.png` |
| 图 2 | CMA 机制热力图 | `real_figures/cma_mechanism_heatmap.png` |
| 附录 A | supplementary 假说裁决 | H2/H3/H4/H6 相关 `cma_*.csv` |
| 附录 B | HS300 RDD | `results/literature/hs300_rdd/` |
| 附录 C | 数据与方法限制 | `docs/limitations.md` |
| 附录 D | PAP 与 verdict diff | `docs/pre_registration.md`、`docs/verdict_iteration.md` |

`make paper` 会把上述核心材料聚合到 `paper/` 目录，其中叙事文件在
`paper/narrative/`，表格在 `paper/tables/`，RDD 材料在 `paper/rdd/`。

## 5. 答辩口径

常见追问可以这样回答：

| 追问 | 推荐回答 |
|---|---|
| 这是不是严格因果？ | 主结果是事件研究 + 匹配证据；RDD 是附录识别补充。因果表述限定在设计能支持的范围内。 |
| 为什么不是被动基金生效日买盘？ | 生效日 CAR 不显著，且 H2 为 supplementary、n=12。当前更支持公告期定价与制度机制解释。 |
| 7 条假说是否事前注册？ | 2026-05-03 已冻结 PAP；早期形成过程仍需按 limitations 中的 post-hoc 边界表述。 |
| RDD 为什么不进主表？ | L3 只有 11 批次 / 5 年，低于 ≥20 批次 / ≥10 年门槛；main 显著但对设定敏感。 |
| 数据口径最大限制是什么？ | 历史市值和换手率是 Yahoo sharesOutstanding 近似；HS300 官方 ranking score 不公开。 |

## 6. 交付前验证

每次准备提交论文材料前跑：

```bash
make rebuild
make paper
make paper-audit
make doctor-strict
make ci
```

如果改过 dashboard 或截图，再跑：

```bash
make smoke
```

交付包生成后先看：

1. `paper/bundle_summary.md`：自动研究状态快照。
2. `paper/narrative/research_delivery_package.md`：本文档副本。
3. `paper/narrative/paper_outline_verdicts.md`：当前裁决叙事。
4. `paper/rdd/rdd_robustness.csv`：RDD 全套稳健性。

## 7. 更新规则

- 修改 verdict 计算逻辑、阈值、样本边界或 evidence_tier 前，先更新
  [docs/pre_registration.md](pre_registration.md) §7 决策日志。
- 如果新增外部 HS300 L3 数据，先跑 `index-inclusion-prepare-hs300-rdd --check-only`，
  再跑 `make doctor-strict`。
- 如果 README、paper outline 和 CSV 不一致，以 CSV + PAP 为准，随后同步文档。
