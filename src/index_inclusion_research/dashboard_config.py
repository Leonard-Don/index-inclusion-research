from __future__ import annotations

from index_inclusion_research.dashboard_types import (
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
            "subtitle": "Price Pressure & Disappearing Effect",
            "description_zh": "以反方文献和早期事件研究证据为底，检验短窗口 CAR、成交量冲击和效应减弱问题",
            "project_module": "短期价格压力",
            "runner": run_price_pressure_track,
        },
        "demand_curve_track": {
            "title": "需求曲线与长期保留",
            "subtitle": "Demand Curves & Long-run Retention",
            "description_zh": "以正方机制文献为底，检验价格是否只部分回吐以及需求曲线是否向下倾斜",
            "project_module": "需求曲线效应",
            "runner": run_demand_curve_track,
        },
        "identification_china_track": {
            "title": "制度识别与中国市场证据",
            "subtitle": "Identification & China Evidence",
            "description_zh": "以中国市场正向证据与识别方法论文献为底，整合匹配对照组、DID 风格分析和 RDD 扩展",
            "project_module": "沪深300论文复现",
            "runner": run_identification_china_track,
        },
    }


LIBRARY_CARD: DashboardCard = {
    "title": "16 篇文献库",
    "subtitle": "Literature Library",
    "description_zh": "反方、中性、正方三组文献与项目模块映射",
}

REVIEW_CARD: DashboardCard = {
    "title": "文献综述",
    "subtitle": "Review Navigator",
    "description_zh": "按反方、中性、正方三组查看 16 篇文献",
}

FRAMEWORK_CARD: DashboardCard = {
    "title": "文献框架",
    "subtitle": "Five Camps",
    "description_zh": "按五大阵营查看 16 篇文献的演进脉络、项目映射与研究表达",
}

SUPPLEMENT_CARD: DashboardCard = {
    "title": "机制与执行补充",
    "subtitle": "Mechanics & Execution",
    "description_zh": "事件时钟、机制链、冲击估算与表达框架，不进入文献库，仅作补充层",
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
