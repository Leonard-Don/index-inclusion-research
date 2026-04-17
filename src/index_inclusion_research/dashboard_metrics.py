from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from index_inclusion_research import dashboard_loaders
from index_inclusion_research.results_snapshot import ResultsSnapshot
from index_inclusion_research.literature_catalog import build_camp_summary_frame
from index_inclusion_research.supplementary import (
    build_case_playbook_frame,
    build_event_clock_frame,
    build_impact_formula_frame,
    build_mechanism_chain_frame,
)


def build_library_summary_cards() -> list[dict[str, str]]:
    return [
        {
            "kicker": "文献目录",
            "title": "先按阵营读，再按年份读",
            "meta": "统一排序规则",
            "copy": "当前目录已统一按阵营排序，阵营内部按年份排序，适合从研究史角度把握指数效应争论的演进脉络。",
        },
        {
            "kicker": "深度信息",
            "title": "不只看立场，也看识别对象",
            "meta": "目录已加入识别对象与挑战的假设",
            "copy": "这张总表现在不仅告诉你文献属于哪一派，也告诉你每篇文献具体识别了什么，并反驳了哪条旧假设。",
        },
    ]


def build_framework_summary_cards() -> list[dict[str, str]]:
    frame = build_camp_summary_frame()
    cards: list[dict[str, str]] = []
    for row in frame.to_dict("records"):
        cards.append(
            {
                "kicker": "文献阵营",
                "title": str(row["阵营"]),
                "meta": f'{row["副标题"]} · {int(row["文献数量"])} 篇',
                "copy": str(row["核心问题"]),
                "foot": "这一阵营中的文献围绕同一类问题展开，可作为对应研究争论的集中概括。",
            }
        )
    return cards


def build_review_summary_cards() -> list[dict[str, str]]:
    return [
        {
            "kicker": "反方文献",
            "title": "先怀疑永久效应",
            "meta": "价格压力、动量错觉与效应弱化",
            "copy": "先看反方文献如何把早期上涨重新解释为短期流动性冲击、纳入前强势表现或现代市场中的提前定价。",
        },
        {
            "kicker": "中性文献",
            "title": "再看争论如何转向识别",
            "meta": "制度差异、套利约束与价格发现",
            "copy": "中性文献的重要性不在于表态，而在于说明结论会随着指数制度、市场摩擦和识别设计而改变。",
        },
        {
            "kicker": "正方文献",
            "title": "最后看哪些机制仍然成立",
            "meta": "需求曲线、长期保留与中国证据",
            "copy": "正方文献并不只是重复“会涨”，而是在更强设计下继续保留部分永久性、信息背书和中国市场不对称证据。",
        },
    ]


def build_supplement_summary_cards() -> list[dict[str, str]]:
    event = build_event_clock_frame().iloc[1]
    mechanism = build_mechanism_chain_frame().iloc[0]
    impact = build_impact_formula_frame().iloc[2]
    playbook = build_case_playbook_frame().iloc[1]
    return [
        {
            "kicker": "事件时钟",
            "title": str(event["阶段"]),
            "meta": "先分清公告、生效与再平衡",
            "copy": str(event["对应观察指标"]),
            "foot": str(event["最容易犯的误判"]),
        },
        {
            "kicker": "机制链",
            "title": str(mechanism["机制环节"]),
            "meta": str(mechanism["学术对应"]),
            "copy": str(mechanism["交易台语言"]),
            "foot": f'对应变量：{mechanism["项目变量"]}',
        },
        {
            "kicker": "冲击估算",
            "title": str(impact["步骤"]),
            "meta": str(impact["公式/规则"]),
            "copy": str(impact["作用"]),
            "foot": "这一步用于把“指数纳入”转化为交易拥挤度与冲击强弱的估计。",
        },
        {
            "kicker": "表达场景",
            "title": str(playbook["场景"]),
            "meta": str(playbook["对应页面"]),
            "copy": str(playbook["核心表述"]),
            "foot": "这类表述可直接用于研究展示、论文讨论与投研交流。",
        },
    ]


def build_price_pressure_cards(
    root: Path,
    *,
    format_pct: Callable[[float], str],
    format_p_value: Callable[[float], str],
    snapshot: ResultsSnapshot | None = None,
) -> list[dict[str, str]]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    event = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    mechanism = dashboard_loaders.load_single_csv(root / "results" / "literature" / "harris_gurel", "mechanism_summary.csv")
    cards: list[dict[str, str]] = []
    if event is not None:
        us_announce = event.loc[
            (event["market"] == "US")
            & (event["event_phase"] == "announce")
            & (event["window_slug"] == "m1_p1")
            & (event["inclusion"] == 1)
        ]
        cn_effective = event.loc[
            (event["market"] == "CN")
            & (event["event_phase"] == "effective")
            & (event["window_slug"] == "m1_p1")
            & (event["inclusion"] == 1)
        ]
        if not us_announce.empty:
            row = us_announce.iloc[0]
            cards.append({"label": "美股公告日 CAR[-1,+1]", "value": format_pct(float(row["mean_car"])), "copy": format_p_value(float(row["p_value"]))})
        if not cn_effective.empty:
            row = cn_effective.iloc[0]
            cards.append({"label": "A 股生效日 CAR[-1,+1]", "value": format_pct(float(row["mean_car"])), "copy": format_p_value(float(row["p_value"]))})
    if mechanism is not None:
        us_announce_mech = mechanism.loc[
            (mechanism["market"] == "US") & (mechanism["event_phase"] == "announce") & (mechanism["inclusion"] == 1)
        ]
        cn_effective_mech = mechanism.loc[
            (mechanism["market"] == "CN") & (mechanism["event_phase"] == "effective") & (mechanism["inclusion"] == 1)
        ]
        if not us_announce_mech.empty:
            row = us_announce_mech.iloc[0]
            cards.append({"label": "美股公告日成交量变化", "value": format_pct(float(row["mean_volume_change"])), "copy": "反映短期建仓冲击强度"})
        if not cn_effective_mech.empty:
            row = cn_effective_mech.iloc[0]
            cards.append({"label": "A 股生效日成交量变化", "value": format_pct(float(row["mean_volume_change"])), "copy": "用于观察中国样本的调仓压力"})
    return cards[:4]


def build_demand_curve_cards(
    root: Path,
    *,
    format_pct: Callable[[float], str],
    format_p_value: Callable[[float], str],
    snapshot: ResultsSnapshot | None = None,
) -> list[dict[str, str]]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    event = current_snapshot.csv("results", "real_tables", "long_window_event_study_summary.csv")
    retention = current_snapshot.csv("results", "real_tables", "retention_summary.csv")
    cards: list[dict[str, str]] = []
    if retention is not None:
        us_announce = retention.loc[
            (retention["market"] == "US") & (retention["event_phase"] == "announce") & (retention["inclusion"] == 1)
        ]
        cn_effective = retention.loc[
            (retention["market"] == "CN") & (retention["event_phase"] == "effective") & (retention["inclusion"] == 1)
        ]
        if not us_announce.empty:
            row = us_announce.iloc[0]
            cards.append({"label": "美股公告日保留率", "value": f"{float(row['retention_ratio']):.2f}", "copy": "大于 1 表示长窗口效应仍在累积"})
        if not cn_effective.empty:
            row = cn_effective.iloc[0]
            cards.append({"label": "A 股生效日保留率", "value": f"{float(row['retention_ratio']):.2f}", "copy": "用于比较中国样本的长期保留程度"})
    if event is not None:
        us_long = event.loc[
            (event["market"] == "US")
            & (event["event_phase"] == "announce")
            & (event["window_slug"] == "p0_p120")
            & (event["inclusion"] == 1)
        ]
        cn_long = event.loc[
            (event["market"] == "CN")
            & (event["event_phase"] == "announce")
            & (event["window_slug"] == "p0_p120")
            & (event["inclusion"] == 1)
        ]
        if not us_long.empty:
            row = us_long.iloc[0]
            cards.append({"label": "美股公告日 CAR[0,+120]", "value": format_pct(float(row["mean_car"])), "copy": format_p_value(float(row["p_value"]))})
        if not cn_long.empty:
            row = cn_long.iloc[0]
            cards.append({"label": "A 股公告日 CAR[0,+120]", "value": format_pct(float(row["mean_car"])), "copy": format_p_value(float(row["p_value"]))})
    return cards[:4]


def build_identification_cards(
    root: Path,
    *,
    format_pct: Callable[[float], str],
    format_p_value: Callable[[float], str],
    rdd_status: Mapping[str, object] | None = None,
    snapshot: ResultsSnapshot | None = None,
) -> list[dict[str, str]]:
    style_dir = root / "results" / "literature" / "hs300_style"
    current_snapshot = snapshot or ResultsSnapshot(root)
    event = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    asymmetry = current_snapshot.csv("results", "real_tables", "asymmetry_summary.csv")
    did = dashboard_loaders.load_single_csv(style_dir, "did_summary.csv")
    regression = current_snapshot.csv("results", "real_regressions", "regression_coefficients.csv")
    current_rdd_status = dict(rdd_status) if rdd_status is not None else dashboard_loaders.load_rdd_status(root)
    rdd = (
        dashboard_loaders.load_single_csv(dashboard_loaders.rdd_output_dir(root), "rdd_summary.csv")
        if current_rdd_status["mode"] == "real"
        else None
    )
    cards: list[dict[str, str]] = []
    announce = event.loc[
        (event["market"] == "CN")
        & (event["event_phase"] == "announce")
        & (event["window_slug"] == "m1_p1")
        & (event["inclusion"] == 1)
    ]
    if not announce.empty:
        row = announce.iloc[0]
        cards.append({"label": "中国样本公告日 CAR[-1,+1]", "value": format_pct(float(row["mean_car"])), "copy": format_p_value(float(row["p_value"]))})
    announce_gap = asymmetry.loc[(asymmetry["market"] == "CN") & (asymmetry["event_phase"] == "announce")]
    if not announce_gap.empty:
        row = announce_gap.iloc[0]
        cards.append({"label": "中国公告日非对称差值", "value": format_pct(float(row["asymmetry_car_m1_p1"])), "copy": "比较调入与调出短窗口反应差异"})
    if did is not None:
        ar = did.loc[(did["event_phase"] == "announce") & (did["metric"] == "abnormal_return") & (did["inclusion"] == 1)]
        if not ar.empty:
            row = ar.iloc[0]
            cards.append({"label": "DID 异常收益估计", "value": format_pct(float(row["did_estimate"])), "copy": f"处理组 {int(row['n_treated'])} / 对照组 {int(row['n_control'])}"})
    inc = regression.loc[
        (regression["parameter"] == "treatment_group")
        & (regression["specification"] == "main_car")
        & (regression["market"] == "CN")
        & (regression["event_phase"] == "announce")
    ]
    if not inc.empty:
        row = inc.iloc[0]
        cards.append({"label": "匹配回归处理组系数", "value": f"{float(row['coefficient']):.4f}", "copy": format_p_value(float(row["p_value"]))})
    if rdd is not None:
        tau = rdd.loc[rdd["outcome"] == "car_m1_p1"]
        if not tau.empty:
            row = tau.iloc[0]
            cards.append({"label": "RDD 断点效应", "value": f"{float(row['tau']):.4f}", "copy": format_p_value(float(row["p_value"]))})
    return cards[:4]


def build_identification_status_panel(rdd_status: Mapping[str, object]) -> dict[str, object] | None:
    if rdd_status["mode"] == "real":
        return None
    if rdd_status.get("candidate_batches"):
        sample_overview = (
            f"当前已识别 {rdd_status['candidate_batches']} 个候选批次、"
            f"{rdd_status['treated_rows']} 个调入样本和 {rdd_status['control_rows']} 个对照候选；"
            f"其中 {rdd_status['crossing_batches']} 个批次已经覆盖 cutoff 两侧。"
        )
    else:
        sample_overview = "尚未读到通过校验的候选样本文件；当前中国主线的正式结果仍以事件研究与匹配回归为主。"
    if rdd_status.get("validation_error"):
        sample_overview = f"{sample_overview} 最近一次校验失败原因：{rdd_status['validation_error']}。"
    contract_copy = "必需列已固定为 batch_id、announce_date、running_variable、cutoff、inclusion 等字段。"
    if rdd_status.get("audit_file"):
        contract_copy = f"{contract_copy} 当前已生成候选样本审计：{rdd_status['audit_file']}。"
    return {
        "kicker": "方法状态",
        "title": str(rdd_status["evidence_status"]),
        "copy": f"{rdd_status['message']} 只有在提供并通过校验的真实候选样本文件后，断点结果才会进入正式证据链。",
        "meta": [
            {"label": "当前状态", "value": sample_overview},
            {"label": "进入条件", "value": "提供正式候选样本文件并通过字段与日期校验。"},
            {"label": "数据契约", "value": contract_copy},
        ],
    }


def build_price_pressure_tables(
    root: Path,
    *,
    render_table: Callable[..., str],
    snapshot: ResultsSnapshot | None = None,
) -> list[tuple[str, str]]:
    tables: list[tuple[str, str]] = []
    current_snapshot = snapshot or ResultsSnapshot(root)
    event = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    if event is not None:
        focus = event.loc[
            (event["inclusion"] == 1) & event["window_slug"].isin(["m1_p1", "m3_p3"]),
            ["market", "event_phase", "window", "mean_car", "p_value", "n_events"],
        ]
        tables.append(("短窗口 CAR 摘要", render_table(focus, compact=True)))
    time_series = current_snapshot.csv("results", "real_tables", "time_series_event_study_summary.csv")
    focus_time = time_series.loc[
        (time_series["inclusion"] == 1) & (time_series["event_phase"] == "announce"),
        ["market", "announce_year", "mean_car_m1_p1", "mean_car_m3_p3", "n_events"],
    ]
    tables.append(("时间变化摘要", render_table(focus_time, compact=True)))
    mechanism = dashboard_loaders.load_single_csv(root / "results" / "literature" / "harris_gurel", "mechanism_summary.csv")
    if mechanism is not None:
        focus = mechanism.loc[
            mechanism["inclusion"] == 1,
            ["market", "event_phase", "mean_turnover_change", "mean_volume_change", "mean_volatility_change", "n_events"],
        ]
        tables.append(("机制变量变化", render_table(focus, compact=True)))
    return tables


def build_demand_curve_tables(
    root: Path,
    *,
    render_table: Callable[..., str],
    snapshot: ResultsSnapshot | None = None,
) -> list[tuple[str, str]]:
    tables: list[tuple[str, str]] = []
    current_snapshot = snapshot or ResultsSnapshot(root)
    event = current_snapshot.csv("results", "real_tables", "long_window_event_study_summary.csv")
    if event is not None:
        focus = event.loc[
            (event["inclusion"] == 1) & event["window_slug"].isin(["m1_p1", "p0_p20", "p0_p120"]),
            ["market", "event_phase", "window", "mean_car", "p_value", "n_events"],
        ]
        tables.append(("长短窗口 CAR 对比", render_table(focus, compact=True)))
    retention = current_snapshot.csv("results", "real_tables", "retention_summary.csv")
    if retention is not None:
        focus = retention.loc[
            retention["inclusion"] == 1,
            ["market", "event_phase", "short_mean_car", "long_mean_car", "car_reversal", "retention_ratio"],
        ]
        tables.append(("保留率与回吐", render_table(focus, compact=True)))
    return tables


def build_identification_tables(
    root: Path,
    *,
    render_table: Callable[..., str],
    rdd_status: Mapping[str, object] | None = None,
    snapshot: ResultsSnapshot | None = None,
) -> list[tuple[str, str]]:
    style_dir = root / "results" / "literature" / "hs300_style"
    tables: list[tuple[str, str]] = []
    current_snapshot = snapshot or ResultsSnapshot(root)
    event = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    if event is not None:
        focus = event.loc[
            (event["market"] == "CN") & event["window_slug"].isin(["m1_p1", "m3_p3"]),
            ["event_phase", "inclusion", "window", "mean_car", "p_value", "n_events"],
        ]
        tables.append(("中国样本事件研究", render_table(focus, compact=True)))
    asymmetry = current_snapshot.csv("results", "real_tables", "asymmetry_summary.csv")
    if asymmetry is not None:
        focus = asymmetry.loc[
            asymmetry["market"] == "CN",
            ["event_phase", "n_additions", "n_deletions", "addition_car_m1_p1", "deletion_car_m1_p1", "asymmetry_car_m1_p1"],
        ]
        tables.append(("调入调出非对称性", render_table(focus, compact=True)))
    did = dashboard_loaders.load_single_csv(style_dir, "did_summary.csv")
    if did is not None:
        focus = did.loc[:, ["event_phase", "inclusion", "metric", "did_estimate", "n_treated", "n_control"]]
        tables.append(("DID 摘要", render_table(focus, compact=True)))
    regression = current_snapshot.csv("results", "real_regressions", "regression_coefficients.csv")
    if regression is not None:
        focus = regression.loc[
            (regression["market"] == "CN")
            & (regression["parameter"] == "treatment_group")
            & (regression["specification"] == "main_car"),
            ["event_phase", "dependent_variable", "coefficient", "p_value"],
        ]
        tables.append(("匹配回归核心系数", render_table(focus, compact=True)))
    current_rdd_status = dict(rdd_status) if rdd_status is not None else dashboard_loaders.load_rdd_status(root)
    rdd = (
        dashboard_loaders.load_single_csv(dashboard_loaders.rdd_output_dir(root), "rdd_summary.csv")
        if current_rdd_status["mode"] == "real"
        else None
    )
    if rdd is not None:
        focus = rdd.loc[:, ["outcome", "tau", "p_value", "n_obs", "bandwidth"]]
        tables.append(("RDD 摘要", render_table(focus, compact=True)))
    return tables
