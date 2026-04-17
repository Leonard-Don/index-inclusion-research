from __future__ import annotations

from collections.abc import Callable, Mapping


def nav_sections_for_mode(mode: str) -> list[dict[str, str]]:
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


def available_hashes_for_mode(mode: str) -> list[str]:
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
    mode: str,
    url_builder: Callable[[str, str | None], str],
) -> list[dict[str, object]]:
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
    tabs: list[dict[str, object]] = []
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


def table_tier_for_label(label: str) -> str:
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


def decorate_display_tables(tables: list[tuple[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "label": label,
            "html": html_table,
            "layout_class": table_layout_for_label(label),
            "tier": table_tier_for_label(label),
        }
        for label, html_table in tables
    ]


def attach_display_tiers(items: list[dict[str, object]]) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for item in items:
        row = dict(item)
        row.setdefault("tier", table_tier_for_label(str(row.get("label", ""))))
        enriched.append(row)
    return enriched


def split_items_by_tier(
    items: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    primary: list[dict[str, object]] = []
    detail: list[dict[str, object]] = []
    for item in attach_display_tiers(items):
        if item.get("tier") == "detail":
            detail.append(item)
        else:
            primary.append(item)
    return primary, detail


def prepare_track_display(
    section: dict[str, object],
    analysis_id: str,
    demo_mode: bool,
    *,
    fallback_summary: str,
    result_cards_by_analysis: Mapping[str, list[dict[str, str]]],
    curated_tables_by_analysis: Mapping[str, list[tuple[str, str]]],
    extra_figures_by_analysis: Mapping[str, list[dict[str, str]]],
    status_panel: dict[str, object] | None = None,
) -> dict[str, object]:
    curated_summary = {
        "price_pressure_track": "这条主线集中展示短窗口 CAR、公告日与生效日差异，以及交易活跃度变化。当前样本表明，美国市场的公告日效应更强；中国 A 股更值得关注的是生效阶段长期窗口中的调入/调出分化。",
        "demand_curve_track": "这条主线关注价格冲击是否只在短期出现，还是会在更长窗口中保留。阅读时应重点比较保留率、长窗口 CAR，以及短长窗口之间的差异。",
        "identification_china_track": "这条主线把中国样本的事件研究、匹配回归与 RDD 识别放在同一结构中，但只有通过正式候选样本文件校验的 RDD 才会进入正式证据链。",
    }
    takeaways = {
        "price_pressure_track": "当前样本更支持“短期冲击具有明显市场差异”这一判断，而不是简单地认为所有市场都会在指数调整后同步上涨。",
        "demand_curve_track": "价格冲击并未在所有窗口中完全回吐，这意味着需求曲线效应仍有解释力，但其保留程度具有明显的阶段差异。",
        "identification_china_track": "中国市场证据不仅取决于现象本身，还取决于识别设计；匹配回归与 RDD 的并置展示正好体现了这一点。",
    }

    display = dict(section)
    display["display_summary"] = curated_summary.get(analysis_id, fallback_summary)
    if demo_mode and "详细稳健性结果见完整材料。" not in str(display["display_summary"]):
        display["display_summary"] = f'{display["display_summary"]} 详细稳健性结果见完整材料。'
    display["display_support_papers"] = display.get("support_papers", [])
    display["result_cards"] = result_cards_by_analysis.get(analysis_id, [])
    all_figures = [*extra_figures_by_analysis.get(analysis_id, []), *display.get("figure_paths", [])]
    display["display_figures"] = all_figures[: (3 if demo_mode else 6)]
    display["display_tables"] = decorate_display_tables(curated_tables_by_analysis.get(analysis_id, []))
    display["primary_tables"], display["detail_tables"] = split_items_by_tier(display["display_tables"])
    display["badge"] = "核心结果" if demo_mode else "完整结果"
    display["takeaway"] = takeaways.get(analysis_id, "")
    display["status_panel"] = status_panel
    return display


def prepare_framework_display(
    section: dict[str, object],
    *,
    summary_cards: list[dict[str, str]],
) -> dict[str, object]:
    display = dict(section)
    display["display_summary"] = "这里把 16 篇文献组织成一条可以直接讲述的研究史：从 1986 年的经典对决，到现代市场里指数效应的弱化，再到 RDD 方法与中国市场证据的扩展。"
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
    return display


def prepare_supplement_display(
    section: dict[str, object],
    *,
    summary_cards: list[dict[str, str]],
) -> dict[str, object]:
    display = dict(section)
    display["display_summary"] = "这部分把事件研究背后的交易逻辑整理成更便于讨论的解释框架，重点在于说明资金何时进场、冲击为何形成，以及价格与流动性如何在不同阶段调整。"
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
    return display


def track_notes_for_analysis(analysis_id: str) -> list[dict[str, str]]:
    if analysis_id == "price_pressure_track":
        return [
            {"name": "主问题", "copy": "这条主线专门回答指数调入后的上涨是不是主要来自短期交易冲击，而不是长期重估。"},
            {"name": "阅读顺序", "copy": "先比较调入事件的短窗口 CAR，再看按年份展开的时变结果，最后结合成交量、换手率与波动率变化。"},
            {"name": "样本特征", "copy": "扩展样本中，美股公告日调入效应更强；中国 A 股更值得关注的是生效阶段长期窗口中的调入/调出分化。"},
        ]
    if analysis_id == "demand_curve_track":
        return [
            {"name": "主问题", "copy": "这条主线专门判断上涨是不是只短暂发生，还是会保留到更长窗口，从而支持需求曲线向下倾斜。"},
            {"name": "阅读顺序", "copy": "优先观察 retention ratio、长窗口 CAR 与短长窗口对比，而不是只停留在公告当天的涨跌。"},
            {"name": "样本特征", "copy": "当前真实样本显示，中美市场都存在一定程度的长期保留，但公告阶段与生效阶段的保留形态并不一致。"},
        ]
    return [
        {"name": "主问题", "copy": "这条主线专门处理识别问题，回答不同制度背景和识别方法是否会改变对指数效应的判断。"},
        {"name": "阅读顺序", "copy": "先观察中国样本的事件研究与匹配对照组结果，再查看 DID 风格摘要，最后单独阅读 RDD 的方法状态卡。"},
        {"name": "样本特征", "copy": "风格识别部分已基于真实样本运行；RDD 只有在提供并通过校验的正式候选样本文件后，才会进入正式证据链。"},
    ]


def overview_notes() -> list[dict[str, str]]:
    return [
        {"title": "文献层", "copy": "16 篇文献既可按反方、中性、正方阅读，也可按五大阵营理解研究演进。"},
        {"title": "实证层", "copy": "三条研究主线均已接入真实样本、核心表格与可视化结果。"},
        {"title": "方法层", "copy": "页面将事件研究、匹配回归与 RDD 并置展示，便于比较识别强度。"},
        {"title": "机制层", "copy": "补充部分展示事件时钟、机制链与冲击估算，用于解释统计结果背后的交易逻辑。"},
    ]


def overview_notes_for_mode(mode: str) -> list[dict[str, str]]:
    if mode == "brief":
        return [
            {"title": "样本层", "copy": "首页先交代真实样本覆盖、事件窗口口径与跨市场比较的基本范围。"},
            {"title": "结果层", "copy": "3 分钟汇报模式保留三条研究主线的核心结果，便于快速建立主要结论。"},
            {"title": "识别层", "copy": "事件研究、匹配回归与 RDD 的识别含义仍保留在主线解释中，只是不再展开完整附加材料。"},
            {"title": "边界层", "copy": "页面最后仍保留研究边界，用于交代样本期、识别范围与数据口径。"},
        ]
    return overview_notes()


def overview_summary() -> str:
    return (
        "页面将文献框架、真实数据结果、机制解释与识别设计放在同一叙述结构中，"
        "从而形成一条完整且可连续展开的研究链条。"
    )


def overview_summary_for_mode(mode: str) -> str:
    if mode == "brief":
        return (
            "页面将真实样本、三条研究主线与研究边界压缩为一套适合快速汇报的展示材料，"
            "用于在较短时间内说明现象、机制与识别三个层面。"
        )
    return overview_summary()


def cta_copy_for_mode(mode: str) -> str:
    if mode == "brief":
        return "页面以压缩方式呈现样本、主线与研究边界，适合在较短时间内完成问题提出、证据展示与边界交代。"
    return "页面同步呈现主线结果、文献框架与机制补充，便于在同一叙述中完成现象、机制与识别三个层面的展示。"


def abstract_lead() -> str:
    return (
        "扩展样本表明，指数调整效应并非在所有市场、年份与事件方向上都以同一方向出现。"
        "更合理的解释框架是将短期冲击、长期保留与识别设计同时纳入分析。"
    )


def abstract_points() -> list[dict[str, str]]:
    return [
        {
            "title": "现象层",
            "copy": "美国市场的公告日效应最稳定；中国 A 股的关键差异更多体现在生效阶段长期窗口中的调入/调出分化，说明跨市场比较必须区分事件时点与持有窗口。",
        },
        {
            "title": "机制层",
            "copy": "短期价格压力与需求曲线效应并非互相排斥，更可能是在不同窗口中共同发挥作用，只是权重和持续性不同。",
        },
        {
            "title": "识别层",
            "copy": "事件研究能够说明现象，匹配回归帮助控制样本差异，而 RDD 则进一步提升识别强度，三者应被视为互补而非替代。",
        },
    ]
