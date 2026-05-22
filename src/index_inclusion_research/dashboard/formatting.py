from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

from index_inclusion_research.dashboard import loaders as dashboard_loaders

TABLE_LABELS = {
    "event_study_summary": "事件研究汇总表",
    "mechanism_summary": "机制变量汇总表",
    "retention_summary": "保留率汇总表",
    "did_summary": "DID 汇总表",
    "event_counts": "真实事件样本表",
    "panel_coverage": "事件窗口覆盖表",
    "regression_coefficients": "回归系数表",
    "regression_models": "模型统计表",
    "regression_dataset": "回归样本表",
    "match_diagnostics": "匹配诊断表",
    "rdd_summary": "RDD 汇总表",
    "event_level_with_running": "事件层运行变量样本表",
    "candidate_batch_audit": "候选样本批次审计表",
    "sample_scope": "样本范围总表",
    "data_sources": "数据来源与口径表",
    "identification_scope": "识别范围说明表",
    "long_window_event_study_summary": "长窗口事件研究汇总表",
    "event_counts_by_year": "按年份事件分布表",
    "time_series_event_study_summary": "时间变化事件研究表",
    "asymmetry_summary": "调入调出非对称性表",
    "sample_filter_summary": "样本过滤摘要表",
    "robustness_event_study_summary": "事件研究稳健性表",
    "robustness_regression_summary": "回归稳健性表",
    "robustness_retention_summary": "长期保留稳健性表",
}

COLUMN_LABELS = {
    "market": "市场",
    "event_phase": "事件阶段",
    "inclusion": "事件方向",
    "treatment_group": "处理组",
    "window": "窗口",
    "window_slug": "窗口代码",
    "n_events": "事件数",
    "n_obs": "样本量",
    "mean_car": "平均 CAR",
    "std_car": "CAR 标准差",
    "se_car": "CAR 标准误",
    "ci_low_95": "95% 区间下界",
    "ci_high_95": "95% 区间上界",
    "t_stat": "t 值",
    "p_value": "p 值",
    "mean_turnover_change": "平均换手率变化",
    "mean_volume_change": "平均成交量变化",
    "mean_volatility_change": "平均波动率变化",
    "short_mean_car": "短窗口平均 CAR",
    "long_mean_car": "长窗口平均 CAR",
    "car_reversal": "CAR 回吐幅度",
    "retention_ratio": "保留率",
    "metric": "指标",
    "treated_post_minus_pre": "处理组后减前",
    "control_post_minus_pre": "对照组后减前",
    "did_estimate": "DID 估计值",
    "n_treated": "处理组数量",
    "n_control": "对照组数量",
    "specification": "回归规格",
    "dependent_variable": "被解释变量",
    "coefficient": "系数",
    "std_error": "标准误",
    "r_squared": "R²",
    "adj_r_squared": "调整后 R²",
    "outcome": "结果变量",
    "bandwidth": "带宽",
    "n_left": "左侧样本数",
    "n_right": "右侧样本数",
    "tau": "断点效应",
    "event_id": "事件 ID",
    "index_name": "指数名称",
    "event_ticker": "股票代码",
    "event_type": "事件类型",
    "event_date": "事件日期",
    "sector": "行业",
    "log_mkt_cap": "对数市值",
    "pre_event_return": "事件前收益",
    "turnover_change": "换手率变化",
    "volume_change": "成交量变化",
    "volatility_change": "波动率变化",
    "running_variable": "运行变量",
    "cutoff": "断点阈值",
    "distance_to_cutoff": "距断点距离",
    "batch_id": "批次 ID",
    "comparison_id": "比较组 ID",
    "parameter": "参数",
    "term": "变量",
    "car_m1_p1": "CAR[-1,+1]",
    "car_m3_p3": "CAR[-3,+3]",
    "car_m5_p5": "CAR[-5,+5]",
    "car_p0_p5": "CAR[0,+5]",
    "car_p0_p20": "CAR[0,+20]",
    "car_p0_p60": "CAR[0,+60]",
    "car_p0_p120": "CAR[0,+120]",
    "announce": "公告日",
    "effective": "生效日",
    "CN": "中国 A 股",
    "US": "美国",
    "数据集": "数据集",
    "文件": "文件",
    "来源": "来源",
    "市场范围": "市场范围",
    "起始日期": "起始日期",
    "结束日期": "结束日期",
    "行数": "行数",
    "股票数": "股票数",
    "事件数": "事件数",
    "备注": "备注",
    "样本层": "样本层",
    "事件相位窗口数": "事件相位窗口数",
    "观测值": "观测值",
    "说明": "说明",
    "分析层": "分析层",
    "样本基础": "样本基础",
    "主要输出": "主要输出",
    "证据状态": "证据状态",
    "当前口径": "当前口径",
    "announce_year": "公告年份",
    "n_batches": "批次数",
    "n_additions": "调入事件数",
    "n_deletions": "调出事件数",
    "addition_car_m1_p1": "调入 CAR[-1,+1]",
    "deletion_car_m1_p1": "调出 CAR[-1,+1]",
    "asymmetry_car_m1_p1": "短窗口非对称差值",
    "addition_turnover_change": "调入换手率变化",
    "deletion_turnover_change": "调出换手率变化",
    "addition_volume_change": "调入成交量变化",
    "deletion_volume_change": "调出成交量变化",
    "addition_car_p0_p120": "调入 CAR[0,+120]",
    "deletion_car_p0_p120": "调出 CAR[0,+120]",
    "asymmetry_car_p0_p120": "长窗口非对称差值",
    "sample_filter": "样本口径",
    "share_of_baseline": "相对基准样本占比",
    "n_treated_events": "处理事件数",
    "n_short_event_phase_windows": "短窗口事件相位数",
    "n_long_event_phase_windows": "长窗口事件相位数",
    "n_regression_comparisons": "回归比较组数",
    "n_regression_rows": "回归样本行数",
    "covariance": "标准误口径",
    "estimation": "稳健性规格",
    "retention_ratio_valid": "保留率是否有效",
    "retention_note": "保留率说明",
    "se_car_m1_p1": "CAR[-1,+1] 标准误",
    "ci_low_95_car_m1_p1": "CAR[-1,+1] 95% 下界",
    "ci_high_95_car_m1_p1": "CAR[-1,+1] 95% 上界",
    "se_car_m3_p3": "CAR[-3,+3] 标准误",
    "ci_low_95_car_m3_p3": "CAR[-3,+3] 95% 下界",
    "ci_high_95_car_m3_p3": "CAR[-3,+3] 95% 上界",
    "se_car_m5_p5": "CAR[-5,+5] 标准误",
    "ci_low_95_car_m5_p5": "CAR[-5,+5] 95% 下界",
    "ci_high_95_car_m5_p5": "CAR[-5,+5] 95% 上界",
    "se_car_p0_p20": "CAR[0,+20] 标准误",
    "ci_low_95_car_p0_p20": "CAR[0,+20] 95% 下界",
    "ci_high_95_car_p0_p20": "CAR[0,+20] 95% 上界",
    "se_car_p0_p120": "CAR[0,+120] 标准误",
    "ci_low_95_car_p0_p120": "CAR[0,+120] 95% 下界",
    "ci_high_95_car_p0_p120": "CAR[0,+120] 95% 上界",
    "item": "证据项",
    "label": "显示名称",
    "status": "状态",
    "value": "数值",
    "detail": "说明",
    "generated_at": "生成时间",
    "rows": "行数",
    "first_year": "起始年份",
    "last_year": "结束年份",
    "latest_aum_trillion": "最新 AUM（万亿美元）",
    "year": "年份",
    "ticker": "股票代码",
    "announce_date": "公告日",
    "effective_date": "生效日",
    "weight_proxy": "权重代理",
    "announce_jump": "公告日跳涨",
    "test": "检验项",
    "topic": "诊断主题",
    "headline": "结论摘要",
    "hid": "假说编号",
    "name_cn": "假说名称",
    "evidence_tier": "证据层级",
    "verdict": "裁决",
    "confidence": "置信口径",
    "evidence_summary": "证据摘要",
    "evidence_refs": "证据来源",
    "metric_snapshot": "指标快照",
    "next_step": "下一步",
    "key_label": "关键指标",
    "key_value": "关键数值",
    "paper_ids": "关联文献",
    "paper_count": "文献数",
    "track": "研究主线",
    "signal": "信号",
    "sector_count": "行业数",
    "eligible_sectors": "可用行业",
    "joint_p_value": "联合 p 值",
    "top_term": "最强项",
    "spec_id": "规格 ID",
    "reference_date_column": "参考日期",
    "control_ratio": "对照比例",
    "over_threshold_covariates": "超阈值协变量数",
    "max_abs_smd": "最大 |SMD|",
    "is_default": "默认规格",
    "covariate": "协变量",
    "smd": "SMD",
    "source_kind": "来源类型",
    "source_label": "来源名称",
    "source_url": "来源 URL",
    "source_file": "来源摘要",
    "input_file": "输入材料",
    "audit_file": "审计材料",
    "next_check_command": "预检命令",
    "import_command": "写入命令",
    "announcement_id": "公告 ID",
    "publish_date": "发布日期",
    "title": "标题",
    "detail_url": "详情 URL",
    "attachment_name": "附件名",
    "attachment_url": "附件 URL",
    "local_path": "本地文件",
    "usable_for_l3": "可用于 L3",
    "addition_rows": "调入行数",
    "control_rows": "对照行数",
    "candidate_rows": "候选行数",
    "candidate_batches": "候选批次",
    "reason": "原因",
    "search_term": "搜索词",
    "requested_rows": "请求行数",
    "api_code": "接口代码",
    "raw_rows": "原始行数",
    "hs300_title_rows": "HS300 标题行数",
    "title_matched_rows": "标题命中行数",
    "theme_matched_rows": "主题命中行数",
    "matched_rows": "命中行数",
    "matched_notice_ids": "命中公告 ID",
    "matched_publish_dates": "命中发布日期",
    "date_filtered_matched_rows": "日期过滤后命中行数",
    "date_filtered_notice_ids": "日期过滤后公告 ID",
    "sample_titles": "样例标题",
    "notice_rows": "公告行数",
    "attachment_rows": "附件行数",
    "usable_attachment_rows": "可用附件行数",
    "parsed_addition_rows": "已解析调入行数",
    "parsed_control_rows": "已解析对照行数",
    "priority": "优先级",
    "gap_type": "缺口类型",
    "missing_evidence": "缺少证据",
    "suggested_next_step": "建议下一步",
    "query": "查询语句",
    "expected_evidence": "预期证据",
    "notes": "备注",
    "n_candidates": "候选数",
    "n_included": "调入数",
    "n_excluded": "对照数",
    "n_left_of_cutoff": "断点左侧数",
    "n_right_of_cutoff": "断点右侧数",
    "min_running_variable": "最小运行变量",
    "max_running_variable": "最大运行变量",
    "closest_left_distance": "左侧最近距离",
    "closest_right_distance": "右侧最近距离",
    "n_unique_cutoffs": "断点数量",
    "n_unique_announce_dates": "公告日数量",
    "n_unique_effective_dates": "生效日数量",
    "duplicate_ticker_rows": "重复股票行",
    "has_cutoff_crossing": "是否跨断点",
    "has_treated_and_control": "是否有处理/对照",
    "gap_length_days": "空窗期天数",
    "count": "数量",
    "dim": "分组维度",
    "bucket": "分组",
    "announce_car": "公告日 CAR",
    "effective_car": "生效日 CAR",
    "gap_drift": "空窗期漂移",
    "asymmetry_index": "不对称指数",
    "relative_day": "相对事件日",
    "ar_mean": "平均 AR",
    "ar_se": "AR 标准误",
    "ar_t": "AR t 值",
    "car_mean": "平均 CAR",
    "car_se": "CAR 标准误",
    "car_t": "CAR t 值",
    "name": "检查项",
    "message": "检查说明",
    "fix": "修复建议",
    "claim": "核对声明",
}

PATH_TEXT_REPLACEMENTS = (
    ("data/raw/hs300_rdd_candidates.reconstructed.csv", "公开重建样本"),
    ("data/raw/hs300_rdd_candidates.csv", "正式候选样本"),
    ("results/literature/hs300_rdd/candidate_batch_audit.csv", "候选样本审计"),
    ("results/real_tables", "结果表"),
    ("data/raw/cn_passive_aum_proxy.csv", "中国 A 股 ETF TNA proxy"),
    ("data/raw/passive_aum.csv", "被动资金规模数据"),
    ("hs300_rdd_candidates.reconstructed.csv", "公开重建样本"),
    ("hs300_rdd_candidates.csv", "正式候选样本"),
    ("cma_h6_weight_robustness.csv", "H6 权重稳健性表"),
    ("cma_h7_sector_interaction.csv", "H7 行业交互回归"),
)


def _display_command_label(text: str) -> str | None:
    if "index-inclusion-prepare-hs300-rdd" not in text:
        return None
    if "--force" in text:
        return "index-inclusion-prepare-hs300-rdd --force"
    if "--check-only" in text:
        return "index-inclusion-prepare-hs300-rdd --check-only"
    return "index-inclusion-prepare-hs300-rdd"

STATUS_LABELS = {
    "pass": "通过",
    "warn": "需关注",
    "fail": "失败",
    "missing": "缺失",
    "skipped": "已跳过",
    "error": "错误",
    "pending": "等待中",
    "running": "运行中",
    "idle": "空闲",
    "success": "成功",
    "candidate_found": "已解析候选",
    "parsed": "已解析",
    "parsed_without_l3_controls": "已解析调入，缺少 L3 对照",
    "detail_fetched": "已抓取详情",
    "found": "已命中公告",
    "no_candidates": "未形成候选",
    "real": "正式样本",
    "reconstructed": "公开重建样本",
    "demo": "演示样本",
}

VALUE_LABELS = {
    "announce": "公告日",
    "effective": "生效日",
    "CN": "中国 A 股",
    "US": "美国",
    "abnormal_return": "异常收益",
    "turnover": "换手率",
    "log_volume": "对数成交量",
    "main_car": "主回归 CAR",
    "turnover_mechanism": "换手率机制",
    "volume_mechanism": "成交量机制",
    "volatility_mechanism": "波动率机制",
    "const": "常数项",
    "inclusion": "调入",
    "addition": "调入",
    "deletion": "调出",
    "treatment_group": "处理组变量",
    "baseline": "基准样本",
    "winsorized_1pct": "1% 缩尾样本",
    "nonoverlap_120d": "去重叠样本",
    "baseline_ols": "基准 OLS",
    "hc3": "HC3 稳健标准误",
    "True": "是",
    "False": "否",
    "log_mkt_cap": "对数市值",
    "pre_event_return": "事件前收益",
    "car_m1_p1": "CAR[-1,+1]",
    "car_m3_p3": "CAR[-3,+3]",
    "car_m5_p5": "CAR[-5,+5]",
    "car_p0_p5": "CAR[0,+5]",
    "car_p0_p20": "CAR[0,+20]",
    "car_p0_p60": "CAR[0,+60]",
    "car_p0_p120": "CAR[0,+120]",
    "turnover_change": "换手率变化",
    "volume_change": "成交量变化",
    "volatility_change": "波动率变化",
    "pass": "通过",
    "warn": "需关注",
    "fail": "失败",
    "missing": "缺失",
    "skipped": "已跳过",
    "error": "错误",
    "support": "支持信号",
    "weak_support": "弱支持信号",
    "no_support": "暂未形成支持信号",
    "coverage": "样本覆盖",
    "direction": "方向检验",
    "robustness": "稳健性",
    "final_read": "最终读取",
    "sample_coverage": "样本覆盖",
    "matched_events": "匹配事件数",
    "core": "正文可引用",
    "supplementary": "附录/探索性",
    "candidate_found": "已解析候选",
    "parsed": "已解析",
    "parsed_without_l3_controls": "已解析调入，缺少 L3 对照",
    "detail_fetched": "已抓取详情",
    "found": "已命中公告",
    "no_candidates": "未形成候选",
    "real": "正式样本",
    "reconstructed": "公开重建样本",
    "demo": "演示样本",
    "parsed_additions_missing_controls": "已解析调入但缺少对照名单",
    "unparsed_attachment": "附件未解析",
    "official_adjustment_addition": "官方调整调入",
    "official_adjustment_reserve": "官方备选对照",
    "CSIndex official adjustment and reserve attachment": "中证官方调整及备选名单附件",
    "Official adjustment-list order mapped to a boundary ordinal running variable.": "按官方调整名单顺序映射为边界排序运行变量。",
    "Official adjustment and reserve lists parsed for HS300.": "已解析沪深300官方调整与备选名单。",
    "Official HS300 additions parsed, but reserve controls are absent; manual/archival reserve-list evidence is still required for L3 RDD.": "已解析沪深300调入名单，但缺少备选对照；L3 RDD 仍需人工或归档来源补齐备选名单。",
    "Attachment did not expose both HS300 additions and reserve controls.": "附件未同时提供沪深300调入与备选对照。",
    "Attachment is not a supported adjustment/result list.": "附件不是当前支持的调整或结果名单格式。",
    "No rows matched the HS300 rebalance title/theme filters.": "没有命中沪深300调样标题或主题过滤条件。",
    "Matched notices exist but fall outside the requested date window.": "有命中公告，但不在当前日期范围内。",
    "PAP baseline, limitations, and verdict-diff workflow are in sync with paper narrative copies.": (
        "裁决基线快照、局限说明和裁决差异流程已与论文叙述口径同步。"
    ),
    "All 21 paper references across 7 hypotheses resolve.": "7 条假说中的 21 条文献引用均可解析。",
    "pap limitations claim": "分析参数与局限说明口径",
    "pap_limitations_claim": "分析参数与局限说明口径",
    "main_event_study_claim": "正文主结论审计",
    "patell_bmp_claim": "Patell/BMP 稳健性审计",
    "cma_core_claim": "跨市场机制主表审计",
    "rdd_appendix_claim": "沪深300 RDD 附录审计",
    "paper_bundle_claim": "论文交付包审计",
    "CN and US announce inclusion rows are positive and significant, and paper bundle copies are present.": (
        "中国 A 股与美国公告日调入行均为正且显著，论文交付包副本已齐备。"
    ),
    "Patell/BMP announce inclusion rows are available for both markets and copied into the paper bundle.": (
        "两个市场的公告日调入 Patell/BMP 稳健性行均已生成，并已复制到论文交付包。"
    ),
    "Core mechanism set matches PAP (H1/H5/H7), with paper bundle and narrative copies present.": (
        "正文机制集合（H1/H5/H7）已与分析参数一致，论文表格与叙事副本已齐备。"
    ),
    "RDD appendix artifacts, robustness panel, McCrary output, and preliminary wording are present.": (
        "RDD 附录产物、稳健性面板、McCrary 输出和“附录/探索性”表述均已齐备。"
    ),
    "Paper bundle contains the expected tables, figures, RDD files, narrative docs, and PAP snapshot.": (
        "论文交付包已包含预期表格、图表、RDD 文件、叙事文档与裁决基线快照。"
    ),
    "hypothesis_paper_ids_resolve": "假说文献引用解析",
    "verdicts_csv_health": "裁决 CSV 健康度",
    "results_snapshot_contract": "结果快照契约",
    "match_robustness_grid": "匹配稳健性网格",
    "chart_builders_register": "图表构建器注册表",
}

TEXT_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("US AUM ratio", "美股 AUM 倍数"),
    ("US AUM", "美国被动资金规模"),
    ("CN / US", "中国 A 股 / 美国"),
    ("CN ", "中国 A 股 "),
    ("US ", "美国 "),
    (" CN", " 中国 A 股"),
    (" US", " 美国"),
    ("matched events=", "匹配事件="),
    ("matched=", "匹配事件="),
    ("unique tickers=", "股票数="),
    ("median weight=", "权重中位数="),
    ("heavy announce_jump=", "重权重公告跳涨="),
    ("light=", "轻权重="),
    ("announce_jump", "公告日跳涨"),
    ("announce-day", "公告日"),
    ("announce inclusion", "公告日调入"),
    ("announce", "公告日"),
    ("effective rolling CAR", "生效日滚动 CAR"),
    ("effective", "生效日"),
    ("inclusion effect", "调入效应"),
    ("inclusion", "调入"),
    ("weight_proxy", "权重代理"),
    ("channel concentration", "通道集中度"),
    ("both-sig", "双通道显著"),
    ("turnover", "换手率"),
    ("volume", "成交量"),
    ("sector×phase/treatment", "行业×事件阶段/处理组"),
    ("eligible sectors", "可用行业"),
    ("joint p", "联合 p 值"),
    ("top=", "最强项="),
    ("max=", "最大值="),
    ("min=", "最小值="),
    ("Materials", "材料"),
    ("Real Estate", "房地产"),
    ("Industrials", "工业"),
    ("limit_coef", "涨跌停系数"),
    ("asymmetry_index", "不对称指数"),
    ("spread=", "差值="),
    ("robustness:", "稳健性："),
    ("ols_weight coef=", "OLS 权重系数="),
    ("sector_fe_weight coef=", "行业固定效应权重系数="),
    ("qreg_weight coef=", "分位数回归权重系数="),
    (" vs ", " 对比 "),
    ("best=", "最佳规格="),
    ("over=", "超阈值="),
    ("pending=", "待补充数据="),
    ("· core ·", "· 正文可引用 ·"),
    ("· supplementary ·", "· 附录/探索性 ·"),
    ("verdict-diff", "裁决差异"),
    ("verdicts CSV", "裁决 CSV"),
    ("current verdicts", "当前裁决"),
    ("verdicts", "裁决"),
    ("verdict rows", "裁决行"),
    ("verdict row", "裁决行"),
    ("verdict", "裁决"),
    ("hids", "假说编号"),
    ("local robustness spec(s) available", "个本地稳健性规格可用"),
    ("best spec", "最佳规格"),
    ("default spec", "默认规格"),
    ("spec count", "规格数"),
    ("paper bundle copies are present", "论文交付包副本已齐备"),
    ("paper bundle", "论文交付包"),
    ("Paper bundle", "论文交付包"),
    ("tables, figures, RDD files, narrative docs, and PAP snapshot", "表格、图表、RDD 文件、叙事文档和裁决基线快照"),
    ("preliminary", "附录/探索性"),
    ("limitations", "局限说明"),
    ("evidence_tier=core", "正文可引用"),
    ("supplementary", "附录/探索性"),
    ("verdict diff", "裁决差异"),
    ("verdicts", "裁决"),
    ("verdict", "裁决"),
    ("rows=", "行数="),
    ("batches=", "批次="),
    ("tier=", "证据层级="),
    ("baseline=", "基线日期="),
    ("drift_state=frozen", "漂移状态=已锁定"),
    ("frozen", "已锁定"),
    ("summary=", "摘要="),
    ("alpha", "超额收益"),
)


def display_column_label(column: object) -> str:
    return COLUMN_LABELS.get(str(column), str(column))


def display_status_label(status: object) -> str:
    return STATUS_LABELS.get(str(status), str(status))


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return False


def _looks_like_p_value(column: object | None) -> bool:
    if column is None:
        return False
    text = str(column).lower()
    return "p_value" in text or text in {"p", "p值", "p 值"} or " p" in text or "p " in text


def _looks_like_year(column: object | None) -> bool:
    if column is None:
        return False
    text = str(column).lower()
    return text in {"year", "年份", "announce_year", "first_year", "last_year"}


def _looks_like_count(column: object | None) -> bool:
    if column is None:
        return False
    text = str(column).lower()
    count_tokens = (
        "n_",
        "rows",
        "count",
        "events",
        "obs",
        "批次",
        "行数",
        "数量",
        "样本量",
        "事件数",
    )
    year_tokens = {"year", "年份", "announce_year", "first_year", "last_year"}
    return text not in year_tokens and any(token in text for token in count_tokens)


def _looks_like_ratio(column: object | None) -> bool:
    if column is None:
        return False
    text = str(column).lower()
    ratio_tokens = ("ratio", "rate", "share", "coverage", "占比", "覆盖率", "比例", "率", "保留率")
    return any(token in text for token in ratio_tokens)


def format_display_number(value: object, column: object | None = None) -> str:
    """Format numeric cells for UI tables without mutating source data."""

    if _is_missing_value(value):
        return "—"
    if isinstance(value, bool):
        return "是" if value else "否"
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return str(value)
    if pd.isna(number):
        return "—"
    if _looks_like_p_value(column):
        if number < 0.0001:
            return "&lt;0.0001"
        return f"{number:.4f}" if number < 0.01 else f"{number:.3f}"
    if _looks_like_year(column):
        return str(int(round(number))) if float(number).is_integer() else f"{number:.0f}"
    if _looks_like_ratio(column) and abs(number) <= 1:
        return f"{number:.2%}"
    if float(number).is_integer() or _looks_like_count(column):
        return f"{int(round(number)):,}"
    return f"{number:,.2f}"


def format_display_cell(value: Any, column: object | None = None) -> str:
    if _is_missing_value(value):
        return "—"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, int | float):
        return format_display_number(value, column)
    display = display_value_label(value)
    if display is None:
        return "—"
    return str(display)


def display_value_label(value: object) -> object:
    if _is_missing_value(value):
        return None
    if isinstance(value, bool):
        return "是" if value else "否"
    text = str(value)
    command_label = _display_command_label(text)
    if command_label is not None:
        return command_label
    labeled = VALUE_LABELS.get(text, text)
    if not isinstance(labeled, str):
        return labeled
    for old, new in PATH_TEXT_REPLACEMENTS:
        labeled = labeled.replace(old, new)
    for old, new in TEXT_REPLACEMENTS:
        labeled = labeled.replace(old, new)
    return labeled


def render_table(frame: pd.DataFrame, compact: bool = False) -> str:
    display = frame.copy()
    classes = ["dataframe"]
    if compact:
        classes.append("compact-table")
    if len(display) > 120:
        display = display.head(120)
    display = display.rename(columns={column: COLUMN_LABELS.get(column, column) for column in display.columns})
    for column in display.columns:
        if column == "事件方向":
            display[column] = display[column].map({1: "调入", 0: "调出"}).fillna(display[column])
        if column == "处理组":
            display[column] = display[column].map({1: "处理组", 0: "对照组"}).fillna(display[column])
        if display[column].dtype == bool:
            display[column] = display[column].map({True: "是", False: "否"})
        if is_object_dtype(display[column]) or is_string_dtype(display[column]):
            display[column] = display[column].map(lambda value, col=column: format_display_cell(value, col))  # type: ignore[misc]
        else:
            display[column] = display[column].map(lambda value, col=column: format_display_cell(value, col))  # type: ignore[misc]
    return display.to_html(
        index=False,
        classes=classes,
        border=0,
        justify="left",
        escape=False,
    )


def translate_label(label: str) -> str:
    return dashboard_loaders.translate_label(label, TABLE_LABELS)


def format_figure_caption(path: Path) -> str:
    return dashboard_loaders.format_figure_caption(path, COLUMN_LABELS)


def build_figure_caption(path: Path, custom_caption: str | None = None, prefix: str | None = None) -> str:
    return dashboard_loaders.build_figure_caption(
        path,
        column_labels=COLUMN_LABELS,
        custom_caption=custom_caption,
        prefix=prefix,
    )


def strip_markdown_title(text: str) -> str:
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[0].lstrip().startswith("#"):
        lines = lines[1:]
    return "\n".join(lines).strip()


def clean_display_text(text: str) -> str:
    cleaned = strip_markdown_title(text)
    lines = []
    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("关键输出文件"):
            continue
        if "/Users/" in line:
            continue
        line = line.replace("`", "").lstrip("- ").strip()
        for old, new in PATH_TEXT_REPLACEMENTS:
            line = line.replace(old, new)
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def format_pct(value: float) -> str:
    return f"{value:.2%}"


def format_p_value(value: float) -> str:
    return f"p 值={value:.3f}"


def format_share(value: float) -> str:
    return f"{value:.1%}"
