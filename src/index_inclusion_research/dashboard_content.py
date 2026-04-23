from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pandas as pd

from index_inclusion_research import dashboard_metrics
from index_inclusion_research.dashboard_types import (
    DashboardCard,
    EvolutionNavGroup,
    EvolutionNavView,
    FrameworkResult,
    MetaItem,
    PaperBriefRecord,
    PaperCatalogRecord,
    PaperDashboardRecord,
    PaperDetailResult,
    PaperNavCard,
    SummaryCard,
    SupplementResult,
    TableRenderer,
    TrackResult,
)
from index_inclusion_research.literature_catalog import (
    build_camp_summary_frame,
    build_grouped_literature_frame,
    build_literature_catalog_frame,
    build_literature_dashboard_frame,
    build_literature_evolution_frame,
    build_literature_framework_markdown,
    build_literature_meeting_frame,
    build_literature_review_markdown,
    build_literature_summary_frame,
    build_literature_summary_markdown,
    get_literature_paper,
)
from index_inclusion_research.supplementary import (
    build_case_playbook_frame,
    build_event_clock_frame,
    build_impact_formula_frame,
    build_mechanism_chain_frame,
    build_supplementary_summary_markdown,
    estimate_impact_scenarios,
)


def load_literature_library_result(
    *,
    render_table: TableRenderer,
    library_card: DashboardCard | Mapping[str, str],
) -> TrackResult:
    return {
        "id": "paper_library",
        "title": library_card["title"],
        "description": library_card["description_zh"],
        "subtitle": library_card["subtitle"],
        "summary_text": build_literature_summary_markdown(),
        "summary_cards": dashboard_metrics.build_library_summary_cards(),
        "rendered_tables": [
            (
                "文献分组统计",
                render_table(build_literature_summary_frame(), compact=True),
            ),
            (
                "文献目录",
                render_table(build_literature_dashboard_frame(), compact=True),
            ),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def load_literature_review_result(
    *,
    render_table: TableRenderer,
    review_card: DashboardCard | Mapping[str, str],
) -> TrackResult:
    return {
        "id": "paper_review",
        "title": review_card["title"],
        "description": review_card["description_zh"],
        "subtitle": review_card["subtitle"],
        "summary_text": build_literature_review_markdown(),
        "summary_cards": dashboard_metrics.build_review_summary_cards(),
        "rendered_tables": [
            (
                "反方文献",
                render_table(build_grouped_literature_frame("反方"), compact=True),
            ),
            (
                "中性文献",
                render_table(build_grouped_literature_frame("中性"), compact=True),
            ),
            (
                "正方文献",
                render_table(build_grouped_literature_frame("正方"), compact=True),
            ),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def load_literature_framework_result(
    *,
    render_table: TableRenderer,
    framework_card: DashboardCard | Mapping[str, str],
) -> FrameworkResult:
    return {
        "id": "paper_framework",
        "title": framework_card["title"],
        "description": framework_card["description_zh"],
        "subtitle": framework_card["subtitle"],
        "summary_text": build_literature_framework_markdown(),
        "summary_cards": dashboard_metrics.build_framework_summary_cards(),
        "rendered_tables": [
            ("五大阵营概览", render_table(build_camp_summary_frame(), compact=True)),
            (
                "文献演进总表",
                render_table(build_literature_evolution_frame(), compact=True),
            ),
            (
                "研究表达框架",
                render_table(build_literature_meeting_frame(), compact=True),
            ),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def compact_author_label(authors: str) -> str:
    parts = [part.strip() for part in authors.split(";") if part.strip()]
    if not parts:
        return authors

    def family_name(name: str) -> str:
        tokens = name.split()
        return tokens[-1] if len(tokens) > 1 else name

    families = [family_name(part) for part in parts]
    if len(families) == 1:
        return families[0]
    if len(families) == 2:
        return f"{families[0]}、{families[1]}"
    return f"{families[0]} 等"


def paper_brief_title(record: PaperBriefRecord) -> str:
    return f"{compact_author_label(str(record.get('authors', '')))}（{record.get('year_label', '')}）"


def _paper_reading_lens(*, project_module: str, camp: str) -> str:
    if camp == "市场摩擦与效应重估":
        return "放到今天的阅读里，更重要的是看公开 alpha 如何被提前交易、换手成本和套利资金重新分配。"
    if camp == "方法革命":
        return "放到今天的阅读里，更重要的是看它如何把识别强度、样本选择和价格发现一起拉进争论。"
    if project_module == "短期价格压力":
        return "放到今天的阅读里，更适合把它和提前定价、效应弱化以及公开 alpha 压缩连起来理解。"
    if project_module == "需求曲线效应":
        return "放到今天的阅读里，更适合看哪些冲击会在更外生的权重变化和更强市场摩擦下留下长期痕迹。"
    return "放到今天的阅读里，更适合把它当作独立制度场景的证据，看制度差异、套利约束和识别设计如何共同改写结论。"


def _recommended_card_kicker(
    *,
    current_project_module: str,
    current_camp: str,
    recommended_project_module: str,
    recommended_camp: str,
) -> str:
    if (
        recommended_project_module == current_project_module
        and recommended_camp == current_camp
    ):
        return "同阵营同主线"
    if recommended_camp == current_camp:
        return "同阵营延伸"
    if recommended_project_module == current_project_module:
        return "同主线延伸"
    return "跨主线参照"


def _recommended_card_copy(
    *,
    current_project_module: str,
    current_camp: str,
    recommended_project_module: str,
    recommended_camp: str,
    deep_contribution: str,
) -> str:
    if (
        recommended_project_module == current_project_module
        and recommended_camp == current_camp
    ):
        intro = "适合继续把当前这条争论往后接，看看同一组问题如何被进一步重写。"
    elif recommended_camp == current_camp:
        intro = "适合留在同一阵营里往后看，补齐这一派如何继续推进争论。"
    elif recommended_project_module == current_project_module:
        intro = "适合继续补齐同一主线里的另一种解释或识别角度。"
    elif recommended_project_module == "沪深300论文复现":
        intro = "适合把讨论切到中国制度场景，看看同一问题在更强摩擦下会怎样改写。"
    elif recommended_camp == "方法革命":
        intro = "适合把讨论切到更强识别与价格发现，看看结论会不会因为方法升级而变化。"
    elif recommended_project_module == "需求曲线效应":
        intro = "适合接着看哪些冲击会在更长窗口里部分保留，而不是只停在短期事件日。"
    else:
        intro = "适合把当前结论放到另一条主线里做对照，看看机制和制度条件如何改写结果。"
    return f"{intro} {deep_contribution}"


def _sequence_card_copy(
    *,
    position: str,
    current_project_module: str,
    current_camp: str,
    target_project_module: str,
    target_camp: str,
    deep_contribution: str,
) -> str:
    if position == "prev":
        if target_camp == current_camp:
            intro = "如果你想回看当前这条争论是从哪里起步的，这一篇最适合作为上一环。"
        elif target_project_module == current_project_module:
            intro = "如果你想回看同一主线里更早的解释框架，这一篇最适合作为上一环。"
        else:
            intro = "如果你想回看当前结论之前的另一条参照路径，这一篇最适合作为上一环。"
    elif position == "next":
        if target_camp == current_camp:
            intro = "如果你想继续看这一阵营如何把争论往后推进，这一篇最适合作为下一环。"
        elif target_project_module == current_project_module:
            intro = "如果你想继续补齐同一主线里的下一步解释，这一篇最适合作为下一环。"
        else:
            intro = "如果你想继续看结论如何被别的主线或制度场景改写，这一篇最适合作为下一环。"
    else:
        intro = "你现在就位于这条争论链的当前节点。"
    return f"{intro} {deep_contribution}"


def _sequence_card_action_label(position: str) -> str:
    if position == "prev":
        return "回看上一环"
    if position == "next":
        return "继续下一环"
    return "查看这篇论文"


def _priority_index(value: str, priorities: list[str]) -> int:
    try:
        return priorities.index(value)
    except ValueError:
        return len(priorities)


def _same_track_camp_priorities(current_camp: str) -> list[str]:
    priorities = {
        "创世之战": [
            "正方深化",
            "市场摩擦与效应重估",
            "方法革命",
            "中国A股主战场",
            "创世之战",
        ],
        "正方深化": [
            "正方深化",
            "市场摩擦与效应重估",
            "方法革命",
            "中国A股主战场",
            "创世之战",
        ],
        "市场摩擦与效应重估": [
            "市场摩擦与效应重估",
            "方法革命",
            "中国A股主战场",
            "正方深化",
            "创世之战",
        ],
        "方法革命": [
            "方法革命",
            "中国A股主战场",
            "市场摩擦与效应重估",
            "正方深化",
            "创世之战",
        ],
        "中国A股主战场": [
            "中国A股主战场",
            "方法革命",
            "市场摩擦与效应重估",
            "正方深化",
            "创世之战",
        ],
    }
    return priorities.get(
        current_camp,
        ["创世之战", "正方深化", "市场摩擦与效应重估", "方法革命", "中国A股主战场"],
    )


def _cross_module_priorities(current_project_module: str) -> list[str]:
    priorities = {
        "短期价格压力": ["需求曲线效应", "沪深300论文复现", "短期价格压力"],
        "需求曲线效应": ["短期价格压力", "沪深300论文复现", "需求曲线效应"],
        "沪深300论文复现": ["短期价格压力", "需求曲线效应", "沪深300论文复现"],
    }
    return priorities.get(
        current_project_module,
        ["短期价格压力", "需求曲线效应", "沪深300论文复现"],
    )


def _cross_camp_priorities(current_camp: str) -> list[str]:
    priorities = {
        "创世之战": [
            "市场摩擦与效应重估",
            "正方深化",
            "方法革命",
            "中国A股主战场",
            "创世之战",
        ],
        "正方深化": [
            "市场摩擦与效应重估",
            "方法革命",
            "中国A股主战场",
            "创世之战",
            "正方深化",
        ],
        "市场摩擦与效应重估": [
            "方法革命",
            "中国A股主战场",
            "正方深化",
            "创世之战",
            "市场摩擦与效应重估",
        ],
        "方法革命": [
            "中国A股主战场",
            "市场摩擦与效应重估",
            "正方深化",
            "创世之战",
            "方法革命",
        ],
        "中国A股主战场": [
            "方法革命",
            "市场摩擦与效应重估",
            "正方深化",
            "创世之战",
            "中国A股主战场",
        ],
    }
    return priorities.get(
        current_camp,
        ["市场摩擦与效应重估", "方法革命", "中国A股主战场", "正方深化", "创世之战"],
    )


def _candidate_sort_key(
    record: PaperCatalogRecord,
    *,
    camp_priorities: list[str],
    module_priorities: list[str] | None = None,
) -> tuple[int, int, int]:
    module_rank = 0
    if module_priorities is not None:
        module_rank = _priority_index(
            str(record.get("project_module", "")), module_priorities
        )
    return (
        module_rank,
        _priority_index(str(record.get("camp", "")), camp_priorities),
        int(record.get("year_order", 0)),
    )


def _select_recommended_records(
    *,
    current_record: PaperCatalogRecord,
    candidates: list[PaperCatalogRecord],
    excluded_ids: set[str],
) -> list[PaperCatalogRecord]:
    remaining = [
        record
        for record in candidates
        if str(record.get("paper_id", "")) not in excluded_ids
    ]
    if not remaining:
        return []

    selected: list[PaperCatalogRecord] = []
    current_project_module = str(current_record.get("project_module", ""))
    current_camp = str(current_record.get("camp", ""))

    same_track_candidates = [
        record
        for record in remaining
        if str(record.get("project_module", "")) == current_project_module
    ]
    if same_track_candidates:
        selected.append(
            min(
                same_track_candidates,
                key=lambda record: _candidate_sort_key(
                    record,
                    camp_priorities=_same_track_camp_priorities(current_camp),
                ),
            )
        )

    selected_ids = {str(record.get("paper_id", "")) for record in selected}
    cross_view_candidates = [
        record
        for record in remaining
        if str(record.get("paper_id", "")) not in selected_ids
    ]
    if cross_view_candidates:
        selected.append(
            min(
                cross_view_candidates,
                key=lambda record: _candidate_sort_key(
                    record,
                    camp_priorities=_cross_camp_priorities(current_camp),
                    module_priorities=_cross_module_priorities(current_project_module),
                ),
            )
        )

    if len(selected) < 2:
        fallback_candidates = [
            record
            for record in remaining
            if str(record.get("paper_id", ""))
            not in {str(item.get("paper_id", "")) for item in selected}
        ]
        fallback_candidates.sort(
            key=lambda record: _candidate_sort_key(
                record,
                camp_priorities=_cross_camp_priorities(current_camp),
                module_priorities=_cross_module_priorities(current_project_module),
            )
        )
        selected.extend(fallback_candidates[: 2 - len(selected)])

    return selected[:2]


def project_module_display(
    project_module: str,
    *,
    project_module_display_map: Mapping[str, str],
) -> str:
    return project_module_display_map.get(project_module, project_module)


def group_evolution_cards(
    cards: list[PaperNavCard], key: str
) -> list[EvolutionNavGroup]:
    groups: dict[str, list[PaperNavCard]] = {}
    for card in cards:
        label = str(card.get(key, ""))
        groups.setdefault(label, []).append(card)

    return [
        {
            "title": label,
            "meta": f"{len(group_cards)} 篇文献",
            "cards": group_cards,
        }
        for label, group_cards in groups.items()
    ]


def _catalog_record(row: pd.Series) -> PaperCatalogRecord:
    return cast(PaperCatalogRecord, row.to_dict())


def _dashboard_record(row: pd.Series) -> PaperDashboardRecord:
    return cast(PaperDashboardRecord, row.to_dict())


def load_paper_detail_result(
    paper_id: str,
    *,
    render_table: TableRenderer,
    project_module_display_map: Mapping[str, str],
) -> PaperDetailResult | None:
    paper = get_literature_paper(paper_id)
    if paper is None:
        return None

    catalog_full = build_literature_catalog_frame()
    current_rows = catalog_full.loc[catalog_full["paper_id"] == paper_id]
    if current_rows.empty:
        return None
    current_index = int(current_rows.index[0])
    current_catalog_record = _catalog_record(current_rows.iloc[0])

    catalog = build_literature_dashboard_frame()
    row = catalog.loc[
        catalog["PDF"].str.contains(f'/paper/{paper_id}"', regex=False)
    ].head(1)
    if row.empty:
        return None
    record = _dashboard_record(row.iloc[0])

    authors = [part.strip() for part in paper.authors.split(";") if part.strip()]
    short_authors = authors[0] if len(authors) == 1 else f"{authors[0]} 等"
    display_project_module = project_module_display(
        paper.project_module,
        project_module_display_map=project_module_display_map,
    )

    info_frame = pd.DataFrame(
        [
            {"项目": "作者", "内容": paper.authors},
            {"项目": "年份", "内容": paper.year_label},
            {"项目": "阵营", "内容": record.get("阵营", "")},
            {"项目": "立场", "内容": record.get("立场", "")},
            {"项目": "市场 / 指数", "内容": record.get("市场 / 指数", "")},
            {"项目": "方法 / 关键词", "内容": record.get("方法 / 关键词", "")},
            {"项目": "研究主线", "内容": display_project_module},
            {
                "项目": "原文入口",
                "内容": f'<a href="/paper/{paper_id}/pdf" target="_blank">查看原文 PDF</a>'
                if paper.exists
                else "PDF 不存在",
            },
        ]
    )
    deep_frame = pd.DataFrame(
        [
            {"分析维度": "识别对象", "内容": record.get("识别对象", "")},
            {"分析维度": "挑战的假设", "内容": record.get("挑战的假设", "")},
            {"分析维度": "一句话定位", "内容": record.get("一句话定位", "")},
            {"分析维度": "争论推进", "内容": record.get("争论推进", "")},
            {"分析维度": "研究中的作用", "内容": record.get("研究中的作用", "")},
        ]
    )
    summary_cards: list[SummaryCard] = [
        {
            "kicker": "识别对象",
            "title": str(record.get("识别对象", "")),
            "meta": f"{record.get('阵营', '')} · {record.get('立场', '')}",
            "copy": "这篇论文真正试图识别的核心问题，可以帮助区分它是在讨论短期冲击、长期保留、价格发现、制度差异，还是识别设计本身。",
        },
        {
            "kicker": "挑战的假设",
            "title": str(record.get("挑战的假设", "")),
            "meta": "这篇论文在反驳什么",
            "copy": "读这篇文献时，关键不只是记住它支持哪一派，更要看它究竟在拆掉哪条旧前提，以及为什么旧结论会被重新改写。",
        },
        {
            "kicker": "争论推进",
            "title": str(record.get("一句话定位", "")),
            "meta": "它把文献往前推了哪一步",
            "copy": str(record.get("争论推进", "")),
        },
        {
            "kicker": "本文用途",
            "title": display_project_module,
            "meta": "在整条研究里的位置",
            "copy": str(record.get("研究中的作用", "")),
        },
    ]
    summary_paragraphs = [
        f"{short_authors}（{paper.year_label}）对应的原始论文题目为《{paper.title}》。",
        f"这篇论文位于“{record.get('阵营', '')}”阵营，在这套研究里主要服务于“{display_project_module}”这条主线。",
        _paper_reading_lens(
            project_module=paper.project_module,
            camp=str(current_catalog_record.get("camp", "")),
        ),
        "这页重点回答三个问题：它识别什么、挑战什么、把争论推进到哪里。",
    ]
    summary_text = " ".join(summary_paragraphs)

    sequence_cards: list[PaperNavCard] = []
    prev_row = (
        _catalog_record(catalog_full.iloc[current_index - 1])
        if current_index > 0
        else None
    )
    next_row = (
        _catalog_record(catalog_full.iloc[current_index + 1])
        if current_index < len(catalog_full) - 1
        else None
    )

    if prev_row is not None:
        sequence_cards.append(
            {
                "kicker": "前一篇",
                "title": paper_brief_title(prev_row),
                "year_label": str(prev_row.get("year_label", "")),
                "camp": str(prev_row.get("camp", "")),
                "track_label": project_module_display(
                    str(prev_row.get("project_module", "")),
                    project_module_display_map=project_module_display_map,
                ),
                "meta": f"{prev_row.get('camp', '')} · {prev_row.get('method_focus', '')}",
                "copy": _sequence_card_copy(
                    position="prev",
                    current_project_module=paper.project_module,
                    current_camp=str(current_catalog_record.get("camp", "")),
                    target_project_module=str(prev_row.get("project_module", "")),
                    target_camp=str(prev_row.get("camp", "")),
                    deep_contribution=str(prev_row.get("deep_contribution", "")),
                ),
                "href": f"/paper/{prev_row.get('paper_id')}",
                "action_label": _sequence_card_action_label("prev"),
                "is_current": False,
            }
        )
    sequence_cards.append(
        {
            "kicker": "当前这篇",
            "title": f"{short_authors}（{paper.year_label}）",
            "year_label": str(current_catalog_record.get("year_label", "")),
            "camp": str(current_catalog_record.get("camp", "")),
            "track_label": project_module_display(
                str(current_catalog_record.get("project_module", "")),
                project_module_display_map=project_module_display_map,
            ),
            "meta": f"{current_catalog_record.get('camp', '')} · {current_catalog_record.get('method_focus', '')}",
            "copy": _sequence_card_copy(
                position="current",
                current_project_module=paper.project_module,
                current_camp=str(current_catalog_record.get("camp", "")),
                target_project_module=str(
                    current_catalog_record.get("project_module", "")
                ),
                target_camp=str(current_catalog_record.get("camp", "")),
                deep_contribution=str(
                    current_catalog_record.get("deep_contribution", "")
                ),
            ),
            "href": "",
            "is_current": True,
        }
    )
    if next_row is not None:
        sequence_cards.append(
            {
                "kicker": "后一篇",
                "title": paper_brief_title(next_row),
                "year_label": str(next_row.get("year_label", "")),
                "camp": str(next_row.get("camp", "")),
                "track_label": project_module_display(
                    str(next_row.get("project_module", "")),
                    project_module_display_map=project_module_display_map,
                ),
                "meta": f"{next_row.get('camp', '')} · {next_row.get('method_focus', '')}",
                "copy": _sequence_card_copy(
                    position="next",
                    current_project_module=paper.project_module,
                    current_camp=str(current_catalog_record.get("camp", "")),
                    target_project_module=str(next_row.get("project_module", "")),
                    target_camp=str(next_row.get("camp", "")),
                    deep_contribution=str(next_row.get("deep_contribution", "")),
                ),
                "href": f"/paper/{next_row.get('paper_id')}",
                "action_label": _sequence_card_action_label("next"),
                "is_current": False,
            }
        )

    excluded_recommendation_ids = {
        paper_id,
        str(prev_row.get("paper_id", "")) if prev_row is not None else "",
        str(next_row.get("paper_id", "")) if next_row is not None else "",
    }
    candidate_records = [_catalog_record(row) for _, row in catalog_full.iterrows()]
    recommended = _select_recommended_records(
        current_record=current_catalog_record,
        candidates=candidate_records,
        excluded_ids={item for item in excluded_recommendation_ids if item},
    )
    recommended_cards = [
        {
            "kicker": _recommended_card_kicker(
                current_project_module=paper.project_module,
                current_camp=str(current_catalog_record.get("camp", "")),
                recommended_project_module=str(rec.get("project_module", "")),
                recommended_camp=str(rec.get("camp", "")),
            ),
            "title": paper_brief_title(rec),
            "year_label": str(rec.get("year_label", "")),
            "camp": str(rec.get("camp", "")),
            "meta": (
                f"{rec.get('camp', '')} · "
                f"{project_module_display(str(rec.get('project_module', '')), project_module_display_map=project_module_display_map)}"
                f" · {rec.get('method_focus', '')}"
            ),
            "copy": _recommended_card_copy(
                current_project_module=paper.project_module,
                current_camp=str(current_catalog_record.get("camp", "")),
                recommended_project_module=str(rec.get("project_module", "")),
                recommended_camp=str(rec.get("camp", "")),
                deep_contribution=str(rec.get("deep_contribution", "")),
            ),
            "href": f"/paper/{rec.get('paper_id')}",
        }
        for rec in recommended
    ]
    evolution_nav_cards = [
        {
            "kicker": f"{idx + 1:02d} · {row['stance']}",
            "title": paper_brief_title(_catalog_record(row)),
            "year_label": str(row["year_label"]),
            "camp": str(row["camp"]),
            "stance": str(row["stance"]),
            "project_module": str(row["project_module"]),
            "track_label": project_module_display(
                str(row["project_module"]),
                project_module_display_map=project_module_display_map,
            ),
            "copy": str(row["one_line_role"]),
            "href": f"/paper/{row['paper_id']}",
            "is_current": str(row["paper_id"]) == paper_id,
        }
        for idx, (_, row) in enumerate(catalog_full.iterrows())
    ]
    hero_meta_items: list[MetaItem] = [
        {"label": "年份", "value": paper.year_label},
        {"label": "阵营", "value": str(record.get("阵营", ""))},
        {"label": "立场", "value": str(record.get("立场", ""))},
        {
            "label": "研究主线",
            "value": project_module_display(
                paper.project_module,
                project_module_display_map=project_module_display_map,
            ),
        },
    ]
    evolution_nav_views: list[EvolutionNavView] = [
        {"id": "camp", "groups": group_evolution_cards(evolution_nav_cards, "camp")},
        {
            "id": "track",
            "groups": group_evolution_cards(evolution_nav_cards, "track_label"),
        },
        {
            "id": "stance",
            "groups": group_evolution_cards(evolution_nav_cards, "stance"),
        },
    ]

    return {
        "id": f"paper_{paper_id}",
        "title": f"{short_authors}（{paper.year_label}）",
        "description": paper.title,
        "subtitle": f"{record.get('阵营', '')} · {record.get('方法 / 关键词', '')}",
        "hero_aside_title": str(record.get("一句话定位", "")),
        "hero_meta_items": hero_meta_items,
        "hero_aside_copy": (
            f"{record.get('研究中的作用', '')} "
            f"{_paper_reading_lens(project_module=paper.project_module, camp=str(current_catalog_record.get('camp', '')))}"
        ).strip(),
        "summary_text": summary_text,
        "summary_paragraphs": summary_paragraphs,
        "summary_cards": summary_cards,
        "rendered_tables": [
            ("论文信息", render_table(info_frame, compact=True)),
            ("深度解读", render_table(deep_frame, compact=True)),
        ],
        "sequence_cards": sequence_cards,
        "recommended_cards": recommended_cards,
        "evolution_nav_cards": evolution_nav_cards,
        "evolution_nav_views": evolution_nav_views,
        "figure_paths": [],
        "primary_actions": (
            [
                {
                    "label": "查看原文 PDF",
                    "href": f"/paper/{paper_id}/pdf",
                    "target": "_blank",
                }
            ]
            if paper.exists
            else []
        ),
        "output_dir": "",
    }


def load_supplement_result(
    *,
    render_table: TableRenderer,
    supplement_card: DashboardCard | Mapping[str, str],
) -> SupplementResult:
    return {
        "id": "project_supplement",
        "title": supplement_card["title"],
        "description": supplement_card["description_zh"],
        "subtitle": supplement_card["subtitle"],
        "summary_text": build_supplementary_summary_markdown(),
        "rendered_tables": [
            ("事件时钟", render_table(build_event_clock_frame())),
            ("机制链", render_table(build_mechanism_chain_frame())),
            ("冲击估算步骤", render_table(build_impact_formula_frame())),
            ("冲击估算示例", render_table(estimate_impact_scenarios())),
            ("表达框架", render_table(build_case_playbook_frame())),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }
