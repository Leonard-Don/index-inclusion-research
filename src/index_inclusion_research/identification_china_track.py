from __future__ import annotations

from pathlib import Path

from index_inclusion_research.hs300_rdd import run_analysis as run_hs300_rdd_analysis
from index_inclusion_research.hs300_style import (
    run_analysis as run_hs300_style_analysis,
)


def run_analysis(verbose: bool = True) -> dict[str, object]:
    style_result = run_hs300_style_analysis(verbose=verbose)
    rdd_result = run_hs300_rdd_analysis(verbose=verbose)
    style_summary = ""
    rdd_summary = ""
    if isinstance(style_result.get("summary_path"), Path) and style_result["summary_path"].exists():
        style_summary = style_result["summary_path"].read_text(encoding="utf-8").strip()
    if isinstance(rdd_result.get("summary_path"), Path) and rdd_result["summary_path"].exists():
        rdd_summary = rdd_result["summary_path"].read_text(encoding="utf-8").strip()

    combined_tables: dict[str, object] = {}
    for label, frame in style_result.get("tables", {}).items():
        combined_tables[f"风格识别：{label}"] = frame
    for label, frame in rdd_result.get("tables", {}).items():
        combined_tables[f"断点回归：{label}"] = frame

    combined_figures: list[dict[str, object]] = []
    for path in style_result.get("figures", []):
        combined_figures.append({"path": path, "prefix": "风格识别"})
    for path in rdd_result.get("figures", []):
        combined_figures.append({"path": path, "prefix": "断点回归"})

    summary_text = "\n\n".join(
        [
            "# 制度识别与中国市场证据结果包",
            "",
            "这条研究主线以中国市场正向证据与识别方法论文献为底，在同一页里同时展示匹配对照组结果与断点回归扩展结果。",
            "",
            "## 第一部分：风格识别",
            style_summary or "暂无风格识别摘要。",
            "",
            "## 第二部分：断点回归",
            rdd_summary or "暂无断点回归摘要。",
        ]
    ).strip()

    return {
        "id": "identification_china_track",
        "title": "制度识别与中国市场证据",
        "description": "以中国市场正向证据与识别方法论文献为底，整合匹配对照组、DID 风格分析和 RDD 扩展。",
        "subtitle": "Identification & China Evidence",
        "summary_text": summary_text,
        "tables": combined_tables,
        "figures": combined_figures,
        "output_dir": style_result.get("output_dir"),
    }


def main(argv: list[str] | None = None) -> None:
    del argv
    run_analysis(verbose=True)


if __name__ == "__main__":
    main()
