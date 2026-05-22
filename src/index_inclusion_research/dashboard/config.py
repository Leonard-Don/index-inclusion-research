from __future__ import annotations

from index_inclusion_research.dashboard.types import (
    AnalysesConfig,
    AnalysisRunner,
    DashboardCard,
)


def build_analyses(
    *,
    run_price_pressure_track: AnalysisRunner,
    run_demand_curve_track: AnalysisRunner,
    run_identification_china_track: AnalysisRunner,
) -> AnalysesConfig:
    return {
        "price_pressure_track": {
            "title": "短期价格压力与效应减弱",
            "subtitle": "价格压力与效应消失",
            "description_zh": "从反方文献和早期事件研究出发，检验短窗口 CAR、成交量冲击与效应弱化是否仍然成立",
            "project_module": "短期价格压力",
            "runner": run_price_pressure_track,
        },
        "demand_curve_track": {
            "title": "需求曲线与长期保留",
            "subtitle": "需求曲线与长期保留",
            "description_zh": "从正方机制文献出发，检验价格冲击是否部分保留，以及需求曲线是否仍呈向下倾斜",
            "project_module": "需求曲线效应",
            "runner": run_demand_curve_track,
        },
        "identification_china_track": {
            "title": "制度识别与中国市场证据",
            "subtitle": "识别策略与中国证据",
            "description_zh": "整合中国市场证据、匹配对照组、DID 风格分析和 RDD 扩展，校准制度差异下的识别强度",
            "project_module": "沪深300论文复现",
            "runner": run_identification_china_track,
        },
    }


LIBRARY_CARD: DashboardCard = {
    "title": "16 篇文献库",
    "subtitle": "文献库",
    "description_zh": "按反方、中性、正方梳理核心文献，并映射到对应研究主线",
}

REVIEW_CARD: DashboardCard = {
    "title": "文献综述",
    "subtitle": "综述导航",
    "description_zh": "按立场快速查看 16 篇文献的角色、方法与争论位置",
}

FRAMEWORK_CARD: DashboardCard = {
    "title": "文献框架",
    "subtitle": "五大阵营",
    "description_zh": "按五大阵营展示文献演进、项目映射与可复用的研究表达",
}

SUPPLEMENT_CARD: DashboardCard = {
    "title": "机制与执行补充",
    "subtitle": "机制与执行",
    "description_zh": "补充事件时钟、机制链、冲击估算与表达框架，帮助把结果讲成机制",
}

DETAILS_QUERY_PARAM = "open"


def build_project_module_display(analyses: AnalysesConfig) -> dict[str, str]:
    return {
        str(config["project_module"]): str(config["title"])
        for config in analyses.values()
    }


def build_details_panel_keys(analyses: AnalysesConfig) -> frozenset[str]:
    return frozenset(
        {
            "demo-design-detail-figures",
            "demo-design-detail-tables",
            "demo-framework-detail-tables",
            "demo-supplement-detail-tables",
            "demo-limits-detail-tables",
        }
        | {
            f"demo-{analysis_id}-detail-tables"
            for analysis_id in analyses
        }
        | {
            f"demo-{analysis_id}-support-papers"
            for analysis_id in analyses
        }
    )
