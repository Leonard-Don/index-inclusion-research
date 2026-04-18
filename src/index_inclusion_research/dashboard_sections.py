from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from index_inclusion_research.results_snapshot import ResultsSnapshot
from index_inclusion_research.dashboard_types import (
    CsvFrameReader,
    DashboardSection,
    DisplayTableTierSplitter,
    DisplayTableTiersAttacher,
    DisplayTable,
    FigureEntriesBuilder,
    FormatPct,
    FormatPValue,
    FormatShare,
    IdentificationScopeUpdater,
    RobustnessSection,
    SummaryCard,
    TableRenderer,
)
from index_inclusion_research.rdd_evidence import rdd_evidence_tier_from_status


def build_sample_design_cards(
    root: Path,
    *,
    format_share: FormatShare,
    snapshot: ResultsSnapshot | None = None,
) -> list[SummaryCard]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    sample_scope = current_snapshot.csv("results", "real_tables", "sample_scope.csv")
    data_sources = current_snapshot.csv("results", "real_tables", "data_sources.csv")
    diagnostics = current_snapshot.csv("results", "real_regressions", "match_diagnostics.csv")

    event_row = sample_scope.loc[sample_scope["样本层"] == "事件样本"].iloc[0]
    short_row = sample_scope.loc[sample_scope["样本层"] == "事件研究面板"].iloc[0]
    matched_row = sample_scope.loc[sample_scope["样本层"] == "匹配回归面板"].iloc[0]
    source_row = data_sources.loc[data_sources["数据集"] == "事件样本"].iloc[0]
    total_events = int(event_row["事件数"])
    avg_obs = int(round(float(short_row["观测值"]) / float(short_row["事件相位窗口数"])))
    matched_rate = (diagnostics["status"] == "matched").mean()
    return [
        {
            "kicker": "真实样本",
            "title": f"{total_events} 个真实事件",
            "meta": f'{source_row["市场范围"]} · {source_row["起始日期"]} 至 {source_row["结束日期"]}',
            "copy": "当前样本以正式事件样本表为基础，统一覆盖真实调入/调出事件、事件相位窗口与跨市场比较所需的核心样本层。",
            "foot": "样本层的重点不是单纯扩大数量，而是在同一口径下同时覆盖不同市场、不同事件阶段与可比事件窗口。",
        },
        {
            "kicker": "事件口径",
            "title": "公告日与生效日双时点",
            "meta": f"平均事件窗口观测数约 {avg_obs:,}",
            "copy": f'短窗口事件研究面板共 {int(short_row["观测值"]):,} 条观测值，长窗口保留分析另行扩展到 {int(sample_scope.loc[sample_scope["样本层"] == "长窗口保留分析", "观测值"].iloc[0]):,} 条观测值。',
            "foot": "这一步有助于把“预期形成”和“被动调仓执行”分开，并避免用短面板误读长窗口结论。",
        },
        {
            "kicker": "识别设计",
            "title": "事件研究、匹配回归与 RDD",
            "meta": f"匹配成功率 {format_share(matched_rate)}",
            "copy": f'匹配回归面板目前包含 {int(matched_row["观测值"]):,} 条观测值，用于把事件研究结果与控制变量、对照组设计结合起来理解。',
            "foot": "这组设计并非相互替代，而是对应不同研究问题：先确认现象，再讨论机制，最后提升识别可信度。",
        },
    ]


def build_sample_design_tables(
    root: Path,
    *,
    render_table: TableRenderer,
    format_p_value: FormatPValue,
    value_labels: Mapping[object, object],
    snapshot: ResultsSnapshot | None = None,
) -> list[DisplayTable]:
    current_snapshot = snapshot or ResultsSnapshot(root)
    event_counts = current_snapshot.csv("results", "real_tables", "event_counts.csv").copy()
    event_counts_by_year = current_snapshot.csv("results", "real_tables", "event_counts_by_year.csv").copy()
    sample_scope = current_snapshot.csv("results", "real_tables", "sample_scope.csv").copy()
    data_sources = current_snapshot.csv("results", "real_tables", "data_sources.csv").copy()
    if "文件" in data_sources.columns:
        data_sources = data_sources.drop(columns=["文件"])
    event_summary = current_snapshot.csv("results", "real_tables", "event_study_summary.csv")
    regression = current_snapshot.csv("results", "real_regressions", "regression_coefficients.csv")

    comparison_rows = []
    for market, market_label in [("CN", "中国 A 股"), ("US", "美国")]:
        market_events = int(event_counts.loc[event_counts["market"] == market, "n_events"].sum())
        market_years = event_counts_by_year.loc[event_counts_by_year["market"] == market]
        added_events = int(market_years.loc[market_years["inclusion"] == 1, "n_events"].sum())
        deleted_events = int(market_years.loc[market_years["inclusion"] == 0, "n_events"].sum())
        short_window = event_summary.loc[
            (event_summary["market"] == market) & (event_summary["window_slug"] == "m1_p1") & (event_summary["inclusion"] == 1)
        ].copy()
        strongest = short_window.loc[short_window["mean_car"].abs().idxmax()]
        main_reg = regression.loc[
            (regression["market"] == market)
            & (regression["parameter"] == "treatment_group")
            & (regression["specification"] == "main_car")
        ].copy()
        reg_focus = main_reg.loc[main_reg["p_value"].idxmin()]
        stage_text = f'{value_labels.get(str(strongest["event_phase"]), strongest["event_phase"])} {strongest["window"]}'
        car_text = f'{float(strongest["mean_car"]):.2%}（{format_p_value(float(strongest["p_value"]))}）'
        reg_text = f'{float(reg_focus["coefficient"]):.4f}（{format_p_value(float(reg_focus["p_value"]))}）'
        if market == "CN":
            discussion = "生效日短窗口显著为负，说明中国样本更适合围绕执行阶段、制度差异与不对称性展开解释。"
            implication = "仅用美股经典叙事解释 A 股并不充分，匹配回归与中国制度背景讨论更重要。"
        else:
            discussion = "公告日短窗口正向效应最稳定，说明美股市场里预期确认与提前布局仍是最值得优先讨论的环节。"
            implication = "美股部分更适合用短期价格压力、抢跑交易与效应减弱来组织结果解释。"
        comparison_rows.append(
            {
                "市场": market_label,
                "事件数": f"{market_events:,}",
                "调入 / 调出": f"{added_events:,} / {deleted_events:,}",
                "最强阶段": stage_text,
                "最强短窗口 CAR": car_text,
                "主回归处理组系数": reg_text,
                "最值得讨论": discussion,
                "识别含义": implication,
            }
        )

    comparison_table = pd.DataFrame(comparison_rows)
    return [
        {
            "label": "样本范围总表",
            "html": render_table(sample_scope, compact=True),
            "layout_class": "wide",
        },
        {
            "label": "按年份事件分布",
            "html": render_table(event_counts_by_year, compact=True),
            "layout_class": "wide",
        },
        {
            "label": "数据来源与口径",
            "html": render_table(data_sources, compact=True),
            "layout_class": "wide",
        },
        {
            "label": "A 股与美股并列总结",
            "html": render_table(comparison_table, compact=True),
            "layout_class": "wide",
        },
    ]


def build_sample_design_section(
    root: Path,
    *,
    demo_mode: bool,
    render_table: TableRenderer,
    attach_display_tiers: DisplayTableTiersAttacher,
    split_items_by_tier: DisplayTableTierSplitter,
    create_sample_design_figures: FigureEntriesBuilder,
    format_share: FormatShare,
    format_p_value: FormatPValue,
    value_labels: Mapping[object, object],
) -> DashboardSection:
    snapshot = ResultsSnapshot(root)
    tables = attach_display_tiers(
        build_sample_design_tables(
            root,
            render_table=render_table,
            format_p_value=format_p_value,
            value_labels=value_labels,
            snapshot=snapshot,
        )
    )
    primary_tables, detail_tables = split_items_by_tier(tables)
    figures = create_sample_design_figures()
    return {
        "summary": "这一部分优先交代真实样本覆盖、短窗口口径与回归结果的总体轮廓，使后续主线解释建立在清晰的样本与识别设计之上。",
        "summary_cards": build_sample_design_cards(root, format_share=format_share, snapshot=snapshot),
        "figures": figures[:1] if demo_mode else figures,
        "detail_figures": figures[1:] if demo_mode else [],
        "tables": tables,
        "primary_tables": primary_tables,
        "detail_tables": detail_tables,
    }


def build_robustness_section(
    root: Path,
    *,
    read_csv_if_exists: CsvFrameReader,
    render_table: TableRenderer,
    attach_display_tiers: DisplayTableTiersAttacher,
    split_items_by_tier: DisplayTableTierSplitter,
    format_share: FormatShare,
    format_pct: FormatPct,
    snapshot: ResultsSnapshot | None = None,
) -> RobustnessSection:
    del read_csv_if_exists
    current_snapshot = snapshot or ResultsSnapshot(root)
    robustness_events = current_snapshot.optional_csv("results", "real_tables", "robustness_event_study_summary.csv")
    robustness_regressions = current_snapshot.optional_csv("results", "real_tables", "robustness_regression_summary.csv")
    sample_filters = current_snapshot.optional_csv("results", "real_tables", "sample_filter_summary.csv")
    robustness_retention = current_snapshot.optional_csv("results", "real_tables", "robustness_retention_summary.csv")
    if robustness_events.empty or robustness_regressions.empty or sample_filters.empty or robustness_retention.empty:
        return {
            "summary": "稳健性结果尚未生成。刷新数据后，完整材料模式会在这里展示区间估计、异常值处理和样本过滤三类检查。",
            "summary_cards": [],
            "tables": [],
            "primary_tables": [],
            "detail_tables": [],
        }

    nonoverlap_row = sample_filters.loc[sample_filters["sample_filter"] == "nonoverlap_120d"].iloc[0]
    baseline_row = sample_filters.loc[sample_filters["sample_filter"] == "baseline"].iloc[0]
    invalid_retention = robustness_retention.loc[~robustness_retention["retention_ratio_valid"].astype(bool)].copy()

    us_short = robustness_events.loc[
        (robustness_events["market"] == "US")
        & (robustness_events["event_phase"] == "announce")
        & (robustness_events["inclusion"] == 1)
        & (robustness_events["window_slug"] == "m1_p1")
    ].copy()
    short_min = us_short["mean_car"].min() if not us_short.empty else pd.NA
    short_max = us_short["mean_car"].max() if not us_short.empty else pd.NA

    summary_cards = [
        {
            "kicker": "路径不确定性",
            "title": "平均路径图已加入 95% 置信带",
            "meta": "短窗口与长窗口主表同步新增区间估计",
            "copy": "这一步把“平均路径”从单纯均值线升级为带不确定性范围的图形，更适合在论文和答辩中解释哪些阶段的价格路径更稳、哪些阶段离散度更高。",
            "foot": "图表文件名保持不变，因此现有前端引用不需要改链接。",
        },
        {
            "kicker": "重叠过滤",
            "title": f'nonoverlap_120d 保留 {format_share(float(nonoverlap_row["share_of_baseline"]))} 事件相位窗口',
            "meta": f'基准样本 {int(baseline_row["n_short_event_phase_windows"]):,} -> 过滤后 {int(nonoverlap_row["n_short_event_phase_windows"]):,}',
            "copy": "通过剔除同一 ticker、同一事件阶段下相邻 120 日内重叠的事件窗口，检验核心结果是否依赖高频重复进入样本的事件。",
            "foot": str(nonoverlap_row["note"]),
        },
        {
            "kicker": "异常值处理",
            "title": "1% 缩尾后，美股公告日短窗口方向保持一致",
            "meta": f'CAR[-1,+1] 范围 {(format_pct(float(short_min)) if pd.notna(short_min) else "NA")} 至 {(format_pct(float(short_max)) if pd.notna(short_max) else "NA")}',
            "copy": "对事件级 CAR 做 1% / 99% winsorize 后，最核心的短窗口结果仍保持同向，说明主结论并不依赖极少数异常波动事件。",
            "foot": "该口径不改变样本量，只改变事件级 CAR 的尾部取值。",
        },
        {
            "kicker": "长期指标边界",
            "title": f"{len(invalid_retention):,} 组保留率因短窗口基数过小不作解释",
            "meta": "避免在分母过小的组合上过度解释 retention ratio",
            "copy": "长期保留分析继续保留，但当短窗口平均 CAR 绝对值过小、导致比率失真时，页面会明确标注“不可解释”，避免把机械比值误当成强结论。",
            "foot": "默认阈值为 |短窗口平均 CAR| < 0.5%。",
        },
    ]

    event_focus = robustness_events.loc[
        robustness_events["window_slug"].isin(["m1_p1", "m3_p3", "p0_p120"]),
        ["sample_filter", "market", "event_phase", "inclusion", "window", "mean_car", "se_car", "ci_low_95", "ci_high_95", "p_value", "n_events"],
    ].copy()
    regression_focus = robustness_regressions.loc[
        :,
        ["estimation", "covariance", "market", "event_phase", "coefficient", "std_error", "p_value", "n_obs", "r_squared"],
    ].copy()
    sample_filter_focus = sample_filters.loc[
        :,
        ["sample_filter", "n_treated_events", "n_short_event_phase_windows", "n_long_event_phase_windows", "n_regression_comparisons", "share_of_baseline", "note"],
    ].copy()
    retention_focus = robustness_retention.loc[
        :,
        ["sample_filter", "market", "event_phase", "inclusion", "short_mean_car", "long_mean_car", "retention_ratio", "retention_ratio_valid", "retention_note"],
    ].copy()

    tables = attach_display_tiers(
        [
            {"label": "样本过滤摘要", "html": render_table(sample_filter_focus, compact=True), "layout_class": "wide"},
            {"label": "事件研究稳健性", "html": render_table(event_focus, compact=True), "layout_class": "wide"},
            {"label": "回归稳健性", "html": render_table(regression_focus, compact=True), "layout_class": "wide"},
            {"label": "长期保留稳健性", "html": render_table(retention_focus, compact=True), "layout_class": "wide"},
        ]
    )
    primary_tables, detail_tables = split_items_by_tier(tables)

    return {
        "summary": "这一部分通过区间估计、异常值处理和重叠事件过滤，检验主结论是否依赖单一样本口径，从而把当前结果升级成更像论文证据链的默认输出。",
        "summary_cards": summary_cards,
        "tables": tables,
        "primary_tables": primary_tables,
        "detail_tables": detail_tables,
    }


def build_limits_section(
    root: Path,
    *,
    apply_live_rdd_status_to_identification_scope: IdentificationScopeUpdater,
    render_table: TableRenderer,
    attach_display_tiers: DisplayTableTiersAttacher,
    split_items_by_tier: DisplayTableTierSplitter,
    format_share: FormatShare,
    snapshot: ResultsSnapshot | None = None,
) -> DashboardSection:
    current_snapshot = snapshot or ResultsSnapshot(root)
    identification_scope = apply_live_rdd_status_to_identification_scope(
        current_snapshot.csv("results", "real_tables", "identification_scope.csv")
    )
    if not identification_scope.empty and "证据状态" in identification_scope.columns:
        derived_tier = identification_scope["证据状态"].astype(str).map(rdd_evidence_tier_from_status)
        if "证据等级" in identification_scope.columns:
            identification_scope["证据等级"] = identification_scope["证据等级"].where(
                identification_scope["证据等级"].notna() & (identification_scope["证据等级"].astype(str) != ""),
                derived_tier,
            )
        else:
            insert_at = identification_scope.columns.get_loc("证据状态")
            identification_scope.insert(insert_at, "证据等级", derived_tier)
    data_sources = current_snapshot.csv("results", "real_tables", "data_sources.csv")
    sample_scope = current_snapshot.csv("results", "real_tables", "sample_scope.csv")
    diagnostics = current_snapshot.csv("results", "real_regressions", "match_diagnostics.csv")

    event_row = data_sources.loc[data_sources["数据集"] == "事件样本"].iloc[0]
    price_row = data_sources.loc[data_sources["数据集"] == "日频价格"].iloc[0]
    benchmark_row = data_sources.loc[data_sources["数据集"] == "基准收益"].iloc[0]
    matched_row = sample_scope.loc[sample_scope["样本层"] == "匹配回归面板"].iloc[0]
    short_id_row = identification_scope.loc[identification_scope["分析层"] == "短窗口事件研究"].iloc[0]
    rdd_row = identification_scope.loc[identification_scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    rdd_tier = str(rdd_row.get("证据等级", "")) or rdd_evidence_tier_from_status(str(rdd_row["证据状态"]))
    rdd_source = str(rdd_row.get("来源摘要", "")).strip()
    matched_rate = (diagnostics["status"] == "matched").mean()
    sector_relaxed_rate = diagnostics["sector_relaxed"].where(diagnostics["sector_relaxed"].notna(), False).astype(bool).mean()

    summary_cards = [
        {
            "kicker": "样本期",
            "title": "结果覆盖美股 2010 至 2025 年与 A 股 2020 至 2025 年批次",
            "meta": f'事件样本 {event_row["起始日期"]} 至 {event_row["结束日期"]}',
            "copy": "当前默认口径已经把美股长期样本与 A 股近年批次放在同一套框架中，但两边覆盖年限不同，因此更适合做跨市场比较与制度异质性讨论，而不是直接当作完全同质的长历史样本。",
            "foot": f'价格与基准收益分别覆盖到 {price_row["结束日期"]} 与 {benchmark_row["结束日期"]}，用于构造事件窗口与市场调整收益。',
        },
        {
            "kicker": "识别范围",
            "title": "事件研究、匹配回归与 RDD 分别回答不同问题",
            "meta": f"匹配成功率 {format_share(matched_rate)} · 中国 RDD {rdd_tier}",
            "copy": str(short_id_row["当前口径"]),
            "foot": (
                f'当前匹配回归面板共 {int(matched_row["观测值"]):,} 条观测值；'
                f'中国 RDD 扩展目前的证据等级为“{rdd_tier} · {rdd_row["证据状态"]}”'
                f'{f"，来源为“{rdd_source}”" if rdd_source else ""}。'
            ),
        },
        {
            "kicker": "数据口径",
            "title": "公开数据足以支撑课程论文，但并不等同官方成分股数据库",
            "meta": f"行业口径放宽占比 {format_share(sector_relaxed_rate)}",
            "copy": "当前项目优先使用公开可得数据构造价格、成交量、换手率与市值口径，因此适合课程论文、研究展示与方法演示，但不应被表述为交易所官方历史精确口径。",
            "foot": "这一边界主要影响对机制强度和匹配精度的解释，不会改变“不同市场与事件阶段存在明显异质性”这一一级结论。",
        },
    ]

    scope_table = pd.DataFrame(
        [
            {"模块": "真实事件样本", "范围": f'{event_row["起始日期"]} 至 {event_row["结束日期"]}', "说明": "以真实调入/调出事件为基础，覆盖中国 A 股与美国两个市场。"},
            {"模块": "价格数据", "范围": f'{price_row["起始日期"]} 至 {price_row["结束日期"]}', "说明": "用于构造事件窗口、异常收益与机制变量。"},
            {"模块": "基准指数数据", "范围": f'{benchmark_row["起始日期"]} 至 {benchmark_row["结束日期"]}', "说明": "用于市场调整收益与异常收益计算。"},
            {"模块": "匹配回归", "范围": f"匹配成功率 {format_share(matched_rate)}", "说明": "对照组构造总体稳定，但仍存在少量无法匹配的事件。"},
            {"模块": "RDD 扩展", "范围": f"{rdd_tier} · {rdd_row['证据状态']}", "说明": str(rdd_row["当前口径"])},
        ]
    )

    tables = attach_display_tiers(
        [
            {"label": "样本与数据范围", "html": render_table(scope_table, compact=True), "layout_class": "wide"},
            {"label": "识别范围说明", "html": render_table(identification_scope, compact=True), "layout_class": "wide"},
        ]
    )
    primary_tables, detail_tables = split_items_by_tier(tables)

    return {
        "summary": "明确研究边界的目的，不是削弱结果，而是让结论与样本期、识别设计、数据来源保持一致，从而提升整套展示的可信度。",
        "summary_cards": summary_cards,
        "tables": tables,
        "primary_tables": primary_tables,
        "detail_tables": detail_tables,
    }
