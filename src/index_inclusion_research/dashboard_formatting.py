from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_loaders


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
}


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
        if display[column].dtype == object:
            display[column] = display[column].map(lambda value: VALUE_LABELS.get(value, value))
    return display.to_html(
        index=False,
        classes=classes,
        border=0,
        justify="left",
        escape=False,
        float_format=lambda v: f"{v:0.4f}",
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
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def format_pct(value: float) -> str:
    return f"{value:.2%}"


def format_p_value(value: float) -> str:
    return f"p={value:.3f}"


def format_share(value: float) -> str:
    return f"{value:.1%}"
