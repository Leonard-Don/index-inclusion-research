from __future__ import annotations

from collections.abc import Mapping

from index_inclusion_research.dashboard_types import (
    AbstractPoint,
    DisplayTable,
    DisplayTier,
    FigureEntry,
    ModeName,
    ModeTab,
    ModeTabUrlBuilder,
    NavSection,
    NoteItem,
    RenderedTable,
    ResultCard,
    SecondarySection,
    StatusPanel,
    SummaryCard,
    TrackDisplaySection,
    TrackNote,
)
from index_inclusion_research.dashboard_view_models import (
    build_section_head_view,
    build_table_detail_view,
    build_table_primary_view,
    build_table_suite_section_view,
    build_track_section_view,
)


def nav_sections_for_mode(mode: ModeName) -> list[NavSection]:
    items = [
        {"anchor": "overview", "label": "总览"},
        {"anchor": "design", "label": "样本与设计"},
        {"anchor": "tracks", "label": "主线结果"},
    ]
    if mode != "brief":
        items.extend(
            [
                {"anchor": "framework", "label": "文献框架"},
                {"anchor": "supplement", "label": "机制补充"},
            ]
        )
    if mode == "full":
        items.append({"anchor": "robustness", "label": "稳健性检查"})
    items.append({"anchor": "limits", "label": "研究边界"})
    return items


def available_hashes_for_mode(mode: ModeName) -> list[str]:
    hashes = ["#overview", "#design", "#tracks", "#limits"]
    if mode != "brief":
        hashes.extend(["#framework", "#supplement"])
    if mode == "full":
        hashes.append("#robustness")
    hashes.extend(
        [
            "#price_pressure_track",
            "#demand_curve_track",
            "#identification_china_track",
        ]
    )
    return hashes


def mode_tabs_for_mode(
    mode: ModeName,
    url_builder: ModeTabUrlBuilder,
) -> list[ModeTab]:
    labels = {
        "brief": "3 分钟汇报",
        "demo": "展示版",
        "full": "完整材料",
    }
    default_hash = {
        "brief": "#overview",
        "demo": "#overview",
        "full": "#overview",
    }
    tabs: list[ModeTab] = []
    for tab_mode in ("brief", "demo", "full"):
        tabs.append(
            {
                "mode": tab_mode,
                "label": labels[tab_mode],
                "base_href": url_builder(tab_mode, None),
                "href": url_builder(tab_mode, default_hash[tab_mode].lstrip("#")),
                "default_hash": default_hash[tab_mode],
                "allowed_hashes": available_hashes_for_mode(tab_mode),
                "active": tab_mode == mode,
            }
        )
    return tabs


def table_layout_for_label(label: str) -> str:
    wide_labels = {
        "长短窗口 CAR 对比",
        "文献演进总表",
        "事件时钟",
        "机制链",
        "A 股与美股并列总结",
        "样本窗口口径",
        "样本范围总表",
        "按年份事件分布",
        "数据来源与口径",
        "识别范围说明",
        "时间变化摘要",
        "调入调出非对称性",
        "样本过滤摘要",
        "事件研究稳健性",
        "回归稳健性",
        "长期保留稳健性",
    }
    return "wide" if label in wide_labels else ""


def table_tier_for_label(label: str) -> DisplayTier:
    primary_labels = {
        "A 股与美股并列总结",
        "样本范围总表",
        "短窗口 CAR 摘要",
        "时间变化摘要",
        "长短窗口 CAR 对比",
        "保留率与回吐",
        "中国样本事件研究",
        "调入调出非对称性",
        "匹配回归核心系数",
        "五大阵营概览",
        "事件时钟",
        "机制链",
        "样本过滤摘要",
        "事件研究稳健性",
        "回归稳健性",
        "样本与数据范围",
    }
    return "primary" if label in primary_labels else "detail"


def decorate_display_tables(tables: list[RenderedTable]) -> list[DisplayTable]:
    return [
        {
            "label": label,
            "html": html_table,
            "layout_class": table_layout_for_label(label),
            "tier": table_tier_for_label(label),
        }
        for label, html_table in tables
    ]


def attach_display_tiers(items: list[DisplayTable]) -> list[DisplayTable]:
    enriched: list[DisplayTable] = []
    for item in items:
        row: DisplayTable = dict(item)
        row.setdefault("tier", table_tier_for_label(str(row.get("label", ""))))
        enriched.append(row)
    return enriched


def split_items_by_tier(
    items: list[DisplayTable],
) -> tuple[list[DisplayTable], list[DisplayTable]]:
    primary: list[DisplayTable] = []
    detail: list[DisplayTable] = []
    for item in attach_display_tiers(items):
        if item.get("tier") == "detail":
            detail.append(item)
        else:
            primary.append(item)
    return primary, detail


def _identification_summary_from_status(status_panel: StatusPanel | None) -> str:
    if status_panel is None:
        return (
            "这条主线把中国样本的事件研究、匹配回归与 RDD 识别放在同一结构中，"
            "也把制度差异、选择偏差与套利约束会不会改写结论放回同一框架。"
        )
    tone = str(status_panel.get("tone", ""))
    signal_value = str(status_panel.get("signal_value", ""))
    if tone == "official":
        return (
            f"这条主线把中国样本的事件研究、匹配回归与 RDD 识别放在同一结构中，也用来检查制度差异、选择偏差与套利约束会不会改写结论；当前中国 RDD 已处于 {signal_value}，"
            "可以与事件研究和匹配回归并列进入正式证据链。"
        )
    if tone == "reconstructed":
        return (
            f"这条主线把中国样本的事件研究、匹配回归与 RDD 识别放在同一结构中，也用来检查制度差异、选择偏差与套利约束会不会改写结论；当前中国 RDD 已处于 {signal_value}，"
            "已进入公开数据版证据链，但必须明确标注为公开重建口径。"
        )
    if tone == "demo":
        return (
            f"这条主线把中国样本的事件研究、匹配回归与 RDD 识别放在同一结构中，也用来检查制度差异、选择偏差与套利约束会不会改写结论；当前中国 RDD 仍处于 {signal_value}，"
            "展示重点是识别框架、字段契约与结果链路，而不是正式证据。"
        )
    return (
        f"这条主线把中国样本的事件研究、匹配回归与 RDD 识别放在同一结构中，也用来检查制度差异、选择偏差与套利约束会不会改写结论；当前中国 RDD 仍处于 {signal_value}，"
        "还没有进入正式或公开重建证据链。"
    )


def _identification_takeaway_from_status(status_panel: StatusPanel | None) -> str:
    if status_panel is None:
        return "中国市场证据不仅取决于现象本身，还取决于制度摩擦、识别设计与价格发现条件；匹配回归与 RDD 的并置展示正好体现了这一点。"
    tone = str(status_panel.get("tone", ""))
    if tone == "official":
        return "中国市场证据现在不只是在展示识别框架，RDD 已经进入正式边界样本口径，可以更直接地支撑主结论，也更适合把制度摩擦和识别差异讲清楚。"
    if tone == "reconstructed":
        return "中国市场证据已经从“只讲方法”推进到“有一版可读的边界结果”，但当前仍应明确标注为公开重建口径，并与制度摩擦更强的市场环境一起理解。"
    if tone == "demo":
        return "中国市场证据当前仍以事件研究和匹配回归为主，RDD 在这一版里主要承担方法展示与链路校验的作用。"
    return "中国市场证据当前仍以事件研究和匹配回归为主，RDD 识别框架已经搭好，但边界样本还没有进入可读证据链。"


def _update_identification_notes(
    notes: list[TrackNote], status_panel: StatusPanel | None
) -> list[TrackNote]:
    if not notes:
        return notes
    if status_panel is None:
        return notes
    tone = str(status_panel.get("tone", ""))
    signal_value = str(status_panel.get("signal_value", ""))
    updated: list[TrackNote] = []
    for note in notes:
        row = dict(note)
        if row.get("name") == "阅读顺序":
            if tone in {"official", "reconstructed"}:
                row["copy"] = (
                    "重点看中国样本的事件研究与匹配结果，再对照证据等级卡和断点回归（RDD）摘要表。"
                )
            else:
                row["copy"] = (
                    "重点看中国样本的事件研究与匹配结果，再看双重差分（DID）摘要和断点回归（RDD）证据等级。"
                )
        if row.get("name") == "样本特征":
            if tone == "official":
                row["copy"] = (
                    f"风格识别部分已基于真实样本运行；RDD 当前处于 {signal_value}，可与其他识别结果并列进入正式证据链。"
                )
            elif tone == "reconstructed":
                row["copy"] = (
                    f"风格识别部分已基于真实样本运行；RDD 当前处于 {signal_value}，已进入公开数据版证据链，但必须标注为公开重建口径。"
                )
            elif tone == "demo":
                row["copy"] = (
                    f"风格识别部分已基于真实样本运行；RDD 当前处于 {signal_value}，主要用于方法展示与链路校验。"
                )
            else:
                row["copy"] = (
                    f"风格识别部分已基于真实样本运行；RDD 当前处于 {signal_value}，边界样本仍待补齐。"
                )
        updated.append(row)
    return updated


def prepare_track_display(
    section: TrackDisplaySection,
    analysis_id: str,
    demo_mode: bool,
    *,
    fallback_summary: str,
    result_cards_by_analysis: Mapping[str, list[ResultCard]],
    curated_tables_by_analysis: Mapping[str, list[RenderedTable]],
    extra_figures_by_analysis: Mapping[str, list[FigureEntry]],
    status_panel: StatusPanel | None = None,
) -> TrackDisplaySection:
    curated_summary = {
        "price_pressure_track": "这条主线集中展示短窗口 CAR、公告日与生效日差异、按年份变化，以及交易活跃度。当前样本和新增文献都更支持“短期冲击仍在、但公开 alpha 已被提前交易压缩”的判断；中国 A 股更值得关注的是执行阶段与长期窗口中的调入/调出分化。",
        "demand_curve_track": "这条主线关注价格冲击是否只在短期出现，还是会在更长窗口中部分保留。重点比较长期保留率、长窗口异常收益（CAR）以及短长窗口之间的差异，并结合更外生的权重冲击与套利约束理解哪些保留更可信。",
        "identification_china_track": _identification_summary_from_status(status_panel),
    }
    takeaways = {
        "price_pressure_track": "当前证据更支持“美股可见 alpha 缩窄但短期冲击未消失”，而不是简单地把现代指数效应概括成完全失效。",
        "demand_curve_track": "价格回吐与长期保留可以并存；更外生的权重冲击和更强的市场摩擦下，需求曲线效应往往更容易留下可见痕迹。",
        "identification_china_track": _identification_takeaway_from_status(
            status_panel
        ),
    }

    display: TrackDisplaySection = dict(section)
    display["display_summary"] = curated_summary.get(analysis_id, fallback_summary)
    if demo_mode and "详细稳健性结果见完整材料。" not in str(
        display["display_summary"]
    ):
        display["display_summary"] = (
            f"{display['display_summary']} 详细稳健性结果见完整材料。"
        )
    display["display_support_papers"] = display.get("support_papers", [])
    display["result_cards"] = result_cards_by_analysis.get(analysis_id, [])
    all_figures = [
        *extra_figures_by_analysis.get(analysis_id, []),
        *display.get("figure_paths", []),
    ]
    display["display_figures"] = all_figures[: (3 if demo_mode else 6)]
    display["display_tables"] = decorate_display_tables(
        curated_tables_by_analysis.get(analysis_id, [])
    )
    display["primary_tables"], display["detail_tables"] = split_items_by_tier(
        display["display_tables"]
    )
    display.setdefault("anchor", analysis_id)
    default_badge = "核心结果" if demo_mode else "完整结果"
    if analysis_id == "identification_china_track" and status_panel is not None:
        display["badge"] = f"证据等级 · {status_panel['title']}"
    else:
        display["badge"] = default_badge
    display["takeaway"] = takeaways.get(analysis_id, "")
    if analysis_id == "identification_china_track":
        display["notes"] = _update_identification_notes(
            display.get("notes", []), status_panel
        )
    display["status_panel"] = status_panel
    display["track_view"] = build_track_section_view(
        anchor=str(display.get("anchor", "track")),
        title=str(display.get("title", "本主线")),
        detail_tables_count=len(display.get("detail_tables", [])),
        support_papers_count=len(display.get("display_support_papers", [])),
    )
    return display


def prepare_framework_display(
    section: SecondarySection,
    *,
    summary_cards: list[SummaryCard],
) -> SecondarySection:
    display: SecondarySection = dict(section)
    display["display_summary"] = (
        "这一页把 16 篇文献整理成一条可直接讲述的研究链：经典对决、正方深化的机制补强、效应被重估、价格发现与 RDD 转向，以及中国市场作为独立制度场景。"
    )
    raw_tables = {label: html for label, html in display.get("rendered_tables", [])}
    ordered_tables = [
        ("文献演进总表", raw_tables["文献演进总表"]),
        ("五大阵营概览", raw_tables["五大阵营概览"]),
        ("研究表达框架", raw_tables["研究表达框架"]),
    ]
    tables = decorate_display_tables(ordered_tables)
    primary_tables, detail_tables = split_items_by_tier(tables)
    display["summary_cards"] = summary_cards
    display["display_tables"] = tables
    display["primary_tables"] = primary_tables
    display["detail_tables"] = detail_tables
    display["section_view"] = build_table_suite_section_view(
        head=build_section_head_view(
            section_id="framework",
            waypoint_label="文献框架",
            kicker="文献框架",
            title="16 篇文献如何连成一条研究链。",
            intro="重点不是逐篇罗列，而是看每篇文献在争论里的角色。",
            side_label="阅读焦点",
        ),
        primary=build_table_primary_view(
            key="demo-framework-primary-tables",
            title="核心摘要表",
            copy="阵营概览最适合先建立全貌，再回到演进表和表达框架。",
            container="library-panels",
            collapsed_copy="展示版默认先显示阵营概览，其余主表按需展开。",
        ),
        detail=build_table_detail_view(
            full_title="补充细表",
            full_copy="这些表格保留完整演进顺序与表达框架，适合在问答或写作时回到更细的组织方式。",
            demo_key="demo-framework-detail-tables",
            demo_title=f"文献框架补充表（{len(detail_tables)} 张）",
            demo_copy="展示版默认收起演进顺序和表达框架，减少首页长度。",
        ),
    )
    return display


def prepare_supplement_display(
    section: SecondarySection,
    *,
    summary_cards: list[SummaryCard],
) -> SecondarySection:
    display: SecondarySection = dict(section)
    display["display_summary"] = (
        "这部分把事件研究背后的交易逻辑整理成更便于讨论的解释框架，重点说明资金何时进场、冲击为何形成，以及价格与流动性如何调整。"
    )
    raw_tables = {label: html for label, html in display.get("rendered_tables", [])}
    ordered_tables = [
        ("事件时钟", raw_tables["事件时钟"]),
        ("机制链", raw_tables["机制链"]),
        ("冲击估算步骤", raw_tables["冲击估算步骤"]),
        ("冲击估算示例", raw_tables["冲击估算示例"]),
        ("表达框架", raw_tables["表达框架"]),
    ]
    tables = decorate_display_tables(ordered_tables)
    primary_tables, detail_tables = split_items_by_tier(tables)
    display["summary_cards"] = summary_cards
    display["display_tables"] = tables
    display["primary_tables"] = primary_tables
    display["detail_tables"] = detail_tables
    display["section_view"] = build_table_suite_section_view(
        head=build_section_head_view(
            section_id="supplement",
            waypoint_label="机制补充",
            kicker="机制补充",
            title="把结果放回交易机制与执行场景。",
            intro="先理解事件时钟和机制链，再回到结果本身。",
            side_label="阅读焦点",
        ),
        primary=build_table_primary_view(
            key="demo-supplement-primary-tables",
            title="核心摘要表",
            copy="事件时钟和机制链最适合先建立框架，再把实证结果放回交易逻辑。",
            container="library-panels",
            collapsed_copy="展示版默认先显示事件时钟和机制链，其余主表按需展开。",
        ),
        detail=build_table_detail_view(
            full_title="补充细表",
            full_copy="这里保留冲击估算和表达框架，用于把机制解释进一步转成执行语言或课堂展示语言。",
            demo_key="demo-supplement-detail-tables",
            demo_title=f"机制补充表（{len(detail_tables)} 张）",
            demo_copy="展示版默认收起冲击估算和表达框架，先保留主结果。",
        ),
    )
    return display


def track_notes_for_analysis(analysis_id: str) -> list[TrackNote]:
    if analysis_id == "price_pressure_track":
        return [
            {
                "name": "主问题",
                "copy": "这条主线回答指数调入后的上涨更像短期交易冲击、提前定价，还是长期重估。",
            },
            {
                "name": "阅读顺序",
                "copy": "重点看短窗口异常收益（CAR）、按年份展开的变化，以及成交量、换手率、波动率和事件可预测性。",
            },
            {
                "name": "样本特征",
                "copy": "扩展样本与近年文献都显示，美股公告日仍有短期效应，但公开 alpha 已被更成熟的套利压缩；中国 A 股更值得看执行阶段与长期窗口分化。",
            },
        ]
    if analysis_id == "demand_curve_track":
        return [
            {
                "name": "主问题",
                "copy": "这条主线判断上涨是短暂冲击，还是会在更长窗口中部分保留。",
            },
            {
                "name": "阅读顺序",
                "copy": "重点看长期保留率、长窗口异常收益（CAR）和短长窗口对比，不必只盯公告当天。",
            },
            {
                "name": "样本特征",
                "copy": "当前真实样本显示，中美市场都存在一定程度的长期保留，但更外生的权重冲击和更强市场摩擦更容易留下持续效应。",
            },
        ]
    return [
        {
            "name": "主问题",
            "copy": "这条主线处理识别问题，回答制度背景、选择偏差和套利约束会不会改变结论。",
        },
        {
            "name": "阅读顺序",
            "copy": "重点看中国样本的事件研究与匹配结果，再看双重差分（DID）摘要、断点回归（RDD）证据等级，并把结论放回制度差异里理解。",
        },
        {
            "name": "样本特征",
            "copy": "风格识别部分已基于真实样本运行；断点回归（RDD）会按当前样本状态进入 L0-L3 的不同证据等级，中国市场还需要和做空约束、投资者结构一起理解。",
        },
    ]


def overview_notes() -> list[NoteItem]:
    return [
        {
            "title": "文献层",
            "copy": "16 篇文献既可按反方、中性、正方阅读，也可按五大阵营把效应弱化、价格发现和中国制度差异串成一条研究链。",
        },
        {
            "title": "实证层",
            "copy": "三条研究主线均已接入真实样本、核心表格与可视化结果。",
        },
        {
            "title": "方法层",
            "copy": "页面将事件研究、匹配回归与 RDD 并置展示，并提醒样本选择、套利约束与价格发现会改变结论。",
        },
        {
            "title": "机制层",
            "copy": "补充部分展示事件时钟、机制链与冲击估算，用于解释公开 alpha 如何被提前交易和换手成本重新分配。",
        },
    ]


def overview_notes_for_mode(mode: ModeName) -> list[NoteItem]:
    if mode == "brief":
        return [
            {
                "title": "样本层",
                "copy": "首页先交代真实样本覆盖、事件窗口口径与跨市场比较范围。",
            },
            {
                "title": "结果层",
                "copy": "3 分钟汇报模式保留三条研究主线的核心结果，便于快速建立结论。",
            },
            {
                "title": "识别层",
                "copy": "事件研究、匹配回归与 RDD 的识别含义仍保留在主线解释里，只是不再展开完整附加材料。",
            },
            {
                "title": "边界层",
                "copy": "页面最后仍保留研究边界，用于交代样本期、识别范围和数据口径。",
            },
        ]
    return overview_notes()


def overview_summary() -> str:
    return (
        "首页把文献脉络、真实结果与识别设计放在同一叙述里，"
        "方便把“效应被重估而非简单消失”这件事连续讲清楚。"
    )


def overview_summary_for_mode(mode: ModeName) -> str:
    if mode == "brief":
        return "这一模式把真实样本、三条主线与研究边界压缩到一页里，适合快速汇报。"
    return overview_summary()


def cta_copy_for_mode(mode: ModeName) -> str:
    if mode == "brief":
        return "页面以压缩方式呈现样本、主线与研究边界，适合在较短时间内完成问题提出、证据展示与边界交代。"
    return "页面同步呈现主线结果、文献框架与机制补充，便于在同一叙述里完成现象、机制、识别与制度差异的说明。"


def abstract_lead() -> str:
    return (
        "扩展样本与新增文献表明，指数调整效应并不是简单存在或消失，"
        "而是在短期冲击、长期保留、样本选择与套利约束之间重新分配。"
        "更合理的解释框架是把市场制度、事件时点与识别设计同时纳入分析。"
    )


def abstract_points() -> list[AbstractPoint]:
    return [
        {
            "title": "现象层",
            "copy": "现代美股公告日仍有最稳定的短期正向证据，但可见 alpha 已被提前交易与样本迁移明显压缩；中国 A 股则更多体现为生效阶段和长期窗口中的不对称分化。",
        },
        {
            "title": "机制层",
            "copy": "短期价格压力、需求曲线保留与被动基金隐性换手成本可以同时存在，差别在于事件是否可预测、套利是否充足以及冲击是否集中。",
        },
        {
            "title": "识别层",
            "copy": "事件研究能够说明现象，匹配回归和断点回归（RDD）帮助隔离选择偏差；新增文献还提醒我们，指数化会通过融券约束、流动性与价格发现速度改变市场质量。",
        },
    ]
