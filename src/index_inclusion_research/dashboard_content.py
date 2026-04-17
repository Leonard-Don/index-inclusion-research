from __future__ import annotations

from collections.abc import Callable, Mapping

import pandas as pd

from index_inclusion_research import dashboard_metrics
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
    render_table: Callable[..., str],
    library_card: Mapping[str, str],
) -> dict[str, object]:
    return {
        "id": "paper_library",
        "title": library_card["title"],
        "description": library_card["description_zh"],
        "subtitle": library_card["subtitle"],
        "summary_text": build_literature_summary_markdown(),
        "summary_cards": dashboard_metrics.build_library_summary_cards(),
        "rendered_tables": [
            ("文献分组统计", render_table(build_literature_summary_frame(), compact=True)),
            ("文献目录", render_table(build_literature_dashboard_frame(), compact=True)),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def load_literature_review_result(
    *,
    render_table: Callable[..., str],
    review_card: Mapping[str, str],
) -> dict[str, object]:
    return {
        "id": "paper_review",
        "title": review_card["title"],
        "description": review_card["description_zh"],
        "subtitle": review_card["subtitle"],
        "summary_text": build_literature_review_markdown(),
        "summary_cards": dashboard_metrics.build_review_summary_cards(),
        "rendered_tables": [
            ("反方文献", render_table(build_grouped_literature_frame("反方"), compact=True)),
            ("中性文献", render_table(build_grouped_literature_frame("中性"), compact=True)),
            ("正方文献", render_table(build_grouped_literature_frame("正方"), compact=True)),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def load_literature_framework_result(
    *,
    render_table: Callable[..., str],
    framework_card: Mapping[str, str],
) -> dict[str, object]:
    return {
        "id": "paper_framework",
        "title": framework_card["title"],
        "description": framework_card["description_zh"],
        "subtitle": framework_card["subtitle"],
        "summary_text": build_literature_framework_markdown(),
        "summary_cards": dashboard_metrics.build_framework_summary_cards(),
        "rendered_tables": [
            ("五大阵营概览", render_table(build_camp_summary_frame(), compact=True)),
            ("文献演进总表", render_table(build_literature_evolution_frame(), compact=True)),
            ("研究表达框架", render_table(build_literature_meeting_frame(), compact=True)),
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


def paper_brief_title(record: Mapping[str, object]) -> str:
    return f"{compact_author_label(str(record.get('authors', '')))}（{record.get('year_label', '')}）"


def project_module_display(
    project_module: str,
    *,
    project_module_display_map: Mapping[str, str],
) -> str:
    return project_module_display_map.get(project_module, project_module)


def group_evolution_cards(cards: list[dict[str, object]], key: str) -> list[dict[str, object]]:
    groups: dict[str, list[dict[str, object]]] = {}
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


def load_paper_detail_result(
    paper_id: str,
    *,
    render_table: Callable[..., str],
    project_module_display_map: Mapping[str, str],
) -> dict[str, object] | None:
    paper = get_literature_paper(paper_id)
    if paper is None:
        return None

    catalog_full = build_literature_catalog_frame()
    current_rows = catalog_full.loc[catalog_full["paper_id"] == paper_id]
    if current_rows.empty:
        return None
    current_index = int(current_rows.index[0])
    current_catalog_record = current_rows.iloc[0].to_dict()

    catalog = build_literature_dashboard_frame()
    row = catalog.loc[catalog["PDF"].str.contains(f'/paper/{paper_id}"', regex=False)].head(1)
    if row.empty:
        return None
    record = row.iloc[0].to_dict()

    authors = [part.strip() for part in paper.authors.split(";") if part.strip()]
    short_authors = authors[0] if len(authors) == 1 else f"{authors[0]} 等"

    info_frame = pd.DataFrame(
        [
            {"项目": "作者", "内容": paper.authors},
            {"项目": "年份", "内容": paper.year_label},
            {"项目": "阵营", "内容": record.get("阵营", "")},
            {"项目": "立场", "内容": record.get("立场", "")},
            {"项目": "市场 / 指数", "内容": record.get("市场 / 指数", "")},
            {"项目": "方法 / 关键词", "内容": record.get("方法 / 关键词", "")},
            {"项目": "研究主线", "内容": paper.project_module},
            {
                "项目": "原文入口",
                "内容": f'<a href="/paper/{paper_id}/pdf" target="_blank">打开原文 PDF</a>' if paper.exists else "PDF 不存在",
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
    summary_cards = [
        {
            "kicker": "识别对象",
            "title": str(record.get("识别对象", "")),
            "meta": f'{record.get("阵营", "")} · {record.get("立场", "")}',
            "copy": "这篇论文真正试图识别的核心问题，可以帮助区分它是在讨论短期冲击、长期保留、信息效应，还是识别设计本身。",
        },
        {
            "kicker": "挑战的假设",
            "title": str(record.get("挑战的假设", "")),
            "meta": "这篇论文在反驳什么",
            "copy": "读这篇文献时，关键不只是记住它支持哪一派，更要看它究竟在拆掉哪条旧前提。",
        },
        {
            "kicker": "争论推进",
            "title": str(record.get("一句话定位", "")),
            "meta": "它把文献往前推了哪一步",
            "copy": str(record.get("争论推进", "")),
        },
        {
            "kicker": "本文用途",
            "title": paper.project_module,
            "meta": "在当前项目中的位置",
            "copy": str(record.get("研究中的作用", "")),
        },
    ]
    summary_paragraphs = [
        f"{short_authors}（{paper.year_label}）对应的原始论文题目为《{paper.title}》。",
        f"这篇论文位于“{record.get('阵营', '')}”阵营，在当前项目中主要服务于“{paper.project_module}”这条主线。",
        "阅读这页时，可先看识别对象与挑战的假设，再看它如何推动指数效应争论向前发展，最后再决定是否进入原文 PDF。",
    ]
    summary_text = " ".join(summary_paragraphs)

    sequence_cards: list[dict[str, object]] = []
    prev_row = catalog_full.iloc[current_index - 1].to_dict() if current_index > 0 else None
    next_row = catalog_full.iloc[current_index + 1].to_dict() if current_index < len(catalog_full) - 1 else None

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
                "copy": str(prev_row.get("deep_contribution", "")),
                "href": f"/paper/{prev_row.get('paper_id')}",
                "is_current": False,
            }
        )
    sequence_cards.append(
        {
            "kicker": "当前位置",
            "title": f"{short_authors}（{paper.year_label}）",
            "year_label": str(current_catalog_record.get("year_label", "")),
            "camp": str(current_catalog_record.get("camp", "")),
            "track_label": project_module_display(
                str(current_catalog_record.get("project_module", "")),
                project_module_display_map=project_module_display_map,
            ),
            "meta": f"{current_catalog_record.get('camp', '')} · {current_catalog_record.get('method_focus', '')}",
            "copy": str(current_catalog_record.get("deep_contribution", "")),
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
                "copy": str(next_row.get("deep_contribution", "")),
                "href": f"/paper/{next_row.get('paper_id')}",
                "is_current": False,
            }
        )

    candidates = catalog_full.loc[catalog_full["paper_id"] != paper_id].copy()
    same_module = candidates.loc[candidates["project_module"] == paper.project_module]
    same_camp = same_module.loc[same_module["camp"] == paper.camp]
    recommended = pd.concat(
        [
            same_camp.head(2),
            same_module.loc[~same_module["paper_id"].isin(same_camp["paper_id"])].head(2),
            candidates.loc[~candidates["paper_id"].isin(same_module["paper_id"])].head(2),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["paper_id"]).head(2)
    recommended_cards = [
        {
            "kicker": "推荐下一篇",
            "title": paper_brief_title(rec.to_dict()),
            "year_label": str(rec["year_label"]),
            "camp": str(rec["camp"]),
            "meta": f"{rec['camp']} · {rec['project_module']} · {rec['method_focus']}",
            "copy": str(rec["practical_use"]),
            "href": f"/paper/{rec['paper_id']}",
        }
        for _, rec in recommended.iterrows()
    ]
    evolution_nav_cards = [
        {
            "kicker": f"{idx + 1:02d} · {row['stance']}",
            "title": paper_brief_title(row.to_dict()),
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
    evolution_nav_views = [
        {"id": "camp", "groups": group_evolution_cards(evolution_nav_cards, "camp")},
        {"id": "track", "groups": group_evolution_cards(evolution_nav_cards, "track_label")},
        {"id": "stance", "groups": group_evolution_cards(evolution_nav_cards, "stance")},
    ]

    return {
        "id": f"paper_{paper_id}",
        "title": f"{short_authors}（{paper.year_label}）",
        "description": paper.title,
        "subtitle": f"{record.get('阵营', '')} · {record.get('方法 / 关键词', '')}",
        "hero_aside_title": str(record.get("一句话定位", "")),
        "hero_meta_items": [
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
        ],
        "hero_aside_copy": str(record.get("研究中的作用", "")),
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
        "primary_actions": ([{"label": "打开原文 PDF", "href": f"/paper/{paper_id}/pdf", "target": "_blank"}] if paper.exists else []),
        "output_dir": "",
    }


def load_supplement_result(
    *,
    render_table: Callable[..., str],
    supplement_card: Mapping[str, str],
) -> dict[str, object]:
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
