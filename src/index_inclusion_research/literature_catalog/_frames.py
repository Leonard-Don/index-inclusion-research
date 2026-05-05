"""DataFrame builders that turn the static paper registry into rendered tables.

Each ``build_*`` function produces a pandas DataFrame consumed by the
dashboard or by downstream report generators. They all read from the
``_data`` module (the static registry) and apply the dashboard-facing
column-renames, sort orders, and PDF-link decorators.
"""

from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from ._data import (
    CAMP_LABELS,
    DEEP_ANALYSIS,
    PAPER_LIBRARY,
    _compact_author_label,
    _paper_citation,
    _year_sort_value,
)


def build_literature_catalog_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for paper in PAPER_LIBRARY:
        row = asdict(paper)
        row.update(DEEP_ANALYSIS.get(paper.paper_id, {}))
        row["pdf_path"] = str(paper.pdf_path)
        row["pdf_exists"] = paper.exists
        row["camp_order"] = paper.camp_order
        row["year_order"] = _year_sort_value(paper.year_label)
        rows.append(row)
    return (
        pd.DataFrame(rows)
        .sort_values(["camp_order", "year_order", "title"], kind="stable")
        .reset_index(drop=True)
    )


def build_literature_dashboard_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for paper in PAPER_LIBRARY:
        open_link = f'<a href="/paper/{paper.paper_id}" target="_blank">查看文献速读</a>'
        rows.append(
            {
                "阵营": CAMP_LABELS[paper.camp]["title"],
                "立场": paper.stance,
                "代表文献": _paper_citation(paper),
                "年份": paper.year_label,
                "市场 / 指数": paper.market_focus,
                "方法 / 关键词": paper.method_focus,
                "识别对象": DEEP_ANALYSIS.get(paper.paper_id, {}).get(
                    "identification_target", ""
                ),
                "挑战的假设": DEEP_ANALYSIS.get(paper.paper_id, {}).get(
                    "challenged_assumption", ""
                ),
                "一句话定位": paper.one_line_role,
                "争论推进": DEEP_ANALYSIS.get(paper.paper_id, {}).get(
                    "deep_contribution", ""
                ),
                "项目模块": paper.project_module,
                "研究中的作用": paper.practical_use,
                "PDF": open_link,
            }
        )
    frame = pd.DataFrame(rows)
    frame["阵营顺序"] = frame["阵营"].map(
        lambda value: next(
            config["order"]
            for config in CAMP_LABELS.values()
            if config["title"] == value
        )
    )
    frame["年份顺序"] = frame["年份"].map(_year_sort_value)
    return (
        frame.sort_values(["阵营顺序", "年份顺序", "代表文献"], kind="stable")
        .drop(columns=["阵营顺序", "年份顺序"])
        .reset_index(drop=True)
    )


def build_literature_summary_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    return (
        catalog.groupby(["stance", "project_module"], dropna=False)
        .agg(文献数量=("paper_id", "size"))
        .reset_index()
        .rename(columns={"stance": "立场", "project_module": "项目模块"})
    )


def build_camp_summary_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    counts = (
        catalog.groupby("camp", dropna=False)
        .agg(文献数量=("paper_id", "size"))
        .reset_index()
    )
    rows: list[dict[str, object]] = []
    for _, row in counts.iterrows():
        camp = str(row["camp"])
        config = CAMP_LABELS[camp]
        rows.append(
            {
                "阵营": config["title"],
                "副标题": config["subtitle"],
                "核心问题": config["description"],
                "文献数量": int(row["文献数量"]),
            }
        )
    frame = pd.DataFrame(rows)
    frame["阵营顺序"] = frame["阵营"].map(
        lambda value: next(
            config["order"]
            for config in CAMP_LABELS.values()
            if config["title"] == value
        )
    )
    return (
        frame.sort_values("阵营顺序").drop(columns=["阵营顺序"]).reset_index(drop=True)
    )


def build_literature_evolution_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    frame = catalog[
        [
            "camp",
            "stance",
            "authors",
            "year_label",
            "market_focus",
            "method_focus",
            "identification_target",
            "challenged_assumption",
            "one_line_role",
            "deep_contribution",
            "project_module",
            "practical_use",
            "camp_order",
            "year_order",
        ]
    ].rename(
        columns={
            "camp": "阵营",
            "stance": "立场",
            "authors": "作者",
            "year_label": "年份",
            "market_focus": "市场 / 指数",
            "method_focus": "方法 / 关键词",
            "identification_target": "识别对象",
            "challenged_assumption": "挑战的假设",
            "one_line_role": "一句话定位",
            "deep_contribution": "争论推进",
            "project_module": "项目模块",
            "practical_use": "研究中的作用",
            "camp_order": "camp_order",
            "year_order": "year_order",
        }
    )
    frame["阵营"] = frame["阵营"].map(lambda value: CAMP_LABELS[value]["title"])
    frame["代表文献"] = frame.apply(
        lambda row: f"{_compact_author_label(str(row['作者']))}（{row['年份']}）",
        axis=1,
    )
    ordered = frame[
        [
            "阵营",
            "立场",
            "代表文献",
            "市场 / 指数",
            "方法 / 关键词",
            "识别对象",
            "挑战的假设",
            "一句话定位",
            "争论推进",
            "项目模块",
            "研究中的作用",
            "camp_order",
            "year_order",
        ]
    ]
    return (
        ordered.sort_values(["camp_order", "year_order", "代表文献"], kind="stable")
        .drop(columns=["camp_order", "year_order"])
        .reset_index(drop=True)
    )


def build_literature_meeting_frame() -> pd.DataFrame:
    rows = [
        {
            "讨论主题": "美股指数效应是否仍具显著性",
            "代表文献": "Greenwood and Sammon (2022); Coakley et al. (2022)",
            "核心表述": "S&P 500 纳入异常收益从 1980 年代 3.4%、1990 年代 7.6% 降到近十年 0.8%；期权隐含 beta 的变化也显示效应在公告前就被提前消化，这更像冲击被提前交易和重分配，而不是机制彻底消失。",
        },
        {
            "讨论主题": "短期价格压力",
            "代表文献": "Harris and Gurel (1986); Lynch and Mendenhall (1997)",
            "核心表述": "公告后价格立即上涨逾 3%、约 2 周内几乎完全反转；公告与生效分离后仍有约 +3.8% CAR 并只发生部分反转，表明短期冲击与部分持久效应并存。",
        },
        {
            "讨论主题": "需求曲线与长期保留",
            "代表文献": "Shleifer (1986); Kaul, Mehrotra and Morck (2000)",
            "核心表述": "即使不伴随基本面变化，纯指数权重冲击也可能带来不完全回吐的价格效应（TSX 300 事件周 +2.34% 且成交量恢复后不反转），这支持股票需求曲线并非水平。",
        },
        {
            "讨论主题": "价格偏差为何未被迅速套利抹平",
            "代表文献": "Wurgler and Zhuravskaya (2002); Madhavan (2003); Petajisto (2011)",
            "核心表述": "现实市场中替代资产不完美、冲击集中且交易摩擦显著，指数换手成本被量化为 S&P 500 约 21–28 bp、Russell 2000 约 38–77 bp，因此价格偏离不会像教科书假设那样瞬时消失。",
        },
        {
            "讨论主题": "为何识别与价格发现要一起讨论",
            "代表文献": "Chang, Hong and Liskovich (2014); Ahn and Patatoukas (2022)",
            "核心表述": "传统 CAR 争论常受内生性与样本选择影响；更强识别不只帮助隔离指数化冲击，也能看到指数化在受限样本上可能改善价格发现。",
        },
        {
            "讨论主题": "为何中国市场仍值得持续研究",
            "代表文献": "Chu et al. (2021); 姚东旻等; Yao, Zhou and Chen (2022)",
            "核心表述": "中国市场的做空约束、散户结构与指数制度使得指数效应呈现出更强的不对称性与更鲜明的本土特征，它更像独立制度场景而不是美股翻版。",
        },
    ]
    return pd.DataFrame(rows)


def build_project_track_frame(project_module: str) -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    grouped = catalog.loc[catalog["project_module"] == project_module].copy()
    grouped = grouped[
        [
            "camp",
            "stance",
            "authors",
            "year_label",
            "identification_target",
            "challenged_assumption",
            "one_line_role",
            "practical_use",
            "paper_id",
            "pdf_exists",
            "camp_order",
            "year_order",
        ]
    ].rename(
        columns={
            "camp": "阵营",
            "stance": "立场",
            "authors": "作者",
            "year_label": "年份",
            "identification_target": "识别对象",
            "challenged_assumption": "挑战的假设",
            "one_line_role": "一句话定位",
            "practical_use": "在本项目中的作用",
            "paper_id": "paper_id",
            "pdf_exists": "pdf_exists",
            "camp_order": "camp_order",
            "year_order": "year_order",
        }
    )
    grouped["阵营"] = grouped["阵营"].map(lambda value: CAMP_LABELS[value]["title"])
    grouped["PDF"] = grouped["paper_id"].map(
        lambda paper_id: f'<a href="/paper/{paper_id}" target="_blank">查看文献速读</a>'
    )
    grouped["代表文献"] = grouped.apply(
        lambda row: f"{_compact_author_label(str(row['作者']))}（{row['年份']}）",
        axis=1,
    )
    grouped = grouped[
        [
            "阵营",
            "立场",
            "代表文献",
            "识别对象",
            "挑战的假设",
            "一句话定位",
            "在本项目中的作用",
            "PDF",
            "camp_order",
            "year_order",
        ]
    ].sort_values(["camp_order", "year_order", "代表文献"], kind="stable")
    return grouped.drop(columns=["camp_order", "year_order"]).reset_index(drop=True)


def build_project_track_support_records(project_module: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for paper in PAPER_LIBRARY:
        if paper.project_module != project_module:
            continue
        records.append(
            {
                "citation": _paper_citation(paper),
                "camp": str(CAMP_LABELS[paper.camp]["title"]),
                "year_label": paper.year_label,
                "year_order": _year_sort_value(paper.year_label),
                "stance": paper.stance,
                "market_focus": paper.market_focus,
                "method_focus": paper.method_focus,
                "identification_target": DEEP_ANALYSIS.get(paper.paper_id, {}).get(
                    "identification_target", ""
                ),
                "challenged_assumption": DEEP_ANALYSIS.get(paper.paper_id, {}).get(
                    "challenged_assumption", ""
                ),
                "deep_contribution": DEEP_ANALYSIS.get(paper.paper_id, {}).get(
                    "deep_contribution", ""
                ),
                "one_line_role": paper.one_line_role,
                "practical_use": paper.practical_use,
                "pdf_href": f"/paper/{paper.paper_id}",
            }
        )
    records.sort(
        key=lambda row: (
            next(
                config["order"]
                for config in CAMP_LABELS.values()
                if config["title"] == row["camp"]
            ),
            int(str(row["year_order"])),
            str(row["citation"]),
        )
    )
    return records


def build_grouped_literature_frame(stance: str) -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    grouped = catalog.loc[catalog["stance"] == stance].copy()
    grouped = grouped[
        [
            "camp",
            "authors",
            "year_label",
            "market_focus",
            "identification_target",
            "challenged_assumption",
            "one_line_role",
            "practical_use",
            "paper_id",
            "pdf_exists",
            "camp_order",
            "year_order",
        ]
    ].rename(
        columns={
            "camp": "阵营",
            "authors": "作者",
            "year_label": "年份",
            "market_focus": "市场 / 指数",
            "identification_target": "识别对象",
            "challenged_assumption": "挑战的假设",
            "one_line_role": "一句话定位",
            "practical_use": "研究中的作用",
            "paper_id": "paper_id",
            "pdf_exists": "pdf_exists",
            "camp_order": "camp_order",
            "year_order": "year_order",
        }
    )
    grouped["阵营"] = grouped["阵营"].map(lambda value: CAMP_LABELS[value]["title"])
    grouped["PDF"] = grouped["paper_id"].map(
        lambda paper_id: f'<a href="/paper/{paper_id}" target="_blank">查看文献速读</a>'
    )
    grouped["代表文献"] = grouped.apply(
        lambda row: f"{_compact_author_label(str(row['作者']))}（{row['年份']}）",
        axis=1,
    )
    grouped = grouped[
        [
            "阵营",
            "代表文献",
            "市场 / 指数",
            "识别对象",
            "挑战的假设",
            "一句话定位",
            "研究中的作用",
            "PDF",
            "camp_order",
            "year_order",
        ]
    ].sort_values(["camp_order", "year_order", "代表文献"], kind="stable")
    return grouped.drop(columns=["camp_order", "year_order"]).reset_index(drop=True)
