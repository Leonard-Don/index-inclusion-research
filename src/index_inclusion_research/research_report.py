from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.result_contract import (
    build_results_manifest,
    load_rdd_status,
    load_results_manifest,
)

ROOT = paths.project_root()


def _best_row(frame: pd.DataFrame, phase: str, window_slug: str) -> pd.DataFrame:
    required = {"event_phase", "window_slug", "inclusion"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()
    subset = frame.loc[(frame["event_phase"] == phase) & (frame["window_slug"] == window_slug) & (frame["inclusion"] == 1)].copy()
    if subset.empty:
        return subset
    return subset.sort_values(["market"]).reset_index(drop=True)


def _coef_lookup(frame: pd.DataFrame, specification: str, parameter: str) -> pd.DataFrame:
    required = {"specification", "parameter"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()
    subset = frame.loc[(frame["specification"] == specification) & (frame["parameter"] == parameter)].copy()
    if subset.empty:
        return subset
    return subset.sort_values(["market", "event_phase"]).reset_index(drop=True)


def _fmt(value: float | int | str | None, digits: int = 4) -> str:
    if value is None or pd.isna(value):
        return "NA"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return f"{value:.{digits}f}"


def build_report_text(
    event_summary: pd.DataFrame,
    regression_models: pd.DataFrame,
    regression_coefficients: pd.DataFrame,
    results_manifest: dict[str, object] | None = None,
) -> str:
    lines: list[str] = []
    lines.append("# 研究结果自动摘要")
    lines.append("")
    lines.append("## 一、事件研究主结论")
    lines.append("")

    announce_rows = _best_row(event_summary, "announce", "m1_p1")
    effective_rows = _best_row(event_summary, "effective", "m1_p1")

    for phase_name, phase_rows in [("公告日", announce_rows), ("生效日", effective_rows)]:
        if phase_rows.empty:
            lines.append(f"- {phase_name}：当前没有可用结果。")
            continue
        for row in phase_rows.itertuples(index=False):
            direction = "正向" if row.mean_car > 0 else "负向"
            significance = "显著" if pd.notna(row.p_value) and row.p_value < 0.10 else "不显著"
            lines.append(
                f"- {row.market} 市场在{phase_name} `CAR[-1,+1]` 的平均值为 `{_fmt(row.mean_car)}`，"
                f"方向为{direction}，t 值为 `{_fmt(row.t_stat)}`，p 值为 `{_fmt(row.p_value)}`，统计上{significance}。"
            )

    lines.append("")
    lines.append("## 二、机制检验摘要")
    lines.append("")

    for spec_name, cn_label in [
        ("turnover_mechanism", "换手率变化"),
        ("volume_mechanism", "成交量变化"),
        ("volatility_mechanism", "波动率变化"),
    ]:
        subset = _coef_lookup(regression_coefficients, spec_name, "treatment_group")
        if subset.empty:
            lines.append(f"- {cn_label}：当前没有足够样本支撑回归。")
            continue
        for row in subset.itertuples(index=False):
            direction = "正相关" if row.coefficient > 0 else "负相关"
            significance = "显著" if row.p_value < 0.10 else "不显著"
            lines.append(
                f"- {row.market} 市场 {row.event_phase} 阶段中，处理组变量对 `{row.dependent_variable}` 的系数为 "
                f"`{_fmt(row.coefficient)}`，表现为{direction}，统计上{significance}。"
            )

    lines.append("")
    lines.append("## 三、识别与证据状态")
    lines.append("")
    if results_manifest:
        lines.append(
            f"- 当前中国 RDD 扩展的证据等级为 `{results_manifest.get('rdd_evidence_tier', 'NA')}`，"
            f"证据状态为 `{results_manifest.get('rdd_evidence_status', 'NA')}`。"
        )
        lines.append(
            f"- 当前来源为 `{results_manifest.get('rdd_source_label', 'NA')}`，"
            f"对应模式 `{results_manifest.get('rdd_mode', 'NA')}`。"
        )
        coverage_note = str(results_manifest.get("rdd_coverage_note", "")).strip()
        if coverage_note:
            lines.append(f"- 覆盖与校验说明：{coverage_note}")
    else:
        lines.append("- 当前未找到结构化识别状态，研究摘要仅基于事件研究与回归输出生成。")

    lines.append("")
    lines.append("## 四、论文可直接使用的讨论句式")
    lines.append("")
    lines.append("- 若公告日效应强于生效日，可解释为投资者将纳入指数视作质量背书，信息效应占主导。")
    lines.append("- 若生效日效应强于公告日，可解释为被动指数基金在调仓时点集中买入，需求冲击更关键。")
    lines.append("- 若成交量和换手率同步上升，说明纳入指数伴随着交易活跃度和流动性改善。")
    lines.append("- 若波动率也明显抬升，则表明指数纳入可能伴随短期交易拥挤和价格压力。")

    if not regression_models.empty:
        lines.append("")
        lines.append("## 五、模型覆盖情况")
        lines.append("")
        for row in regression_models.sort_values(["market", "event_phase", "specification"]).itertuples(index=False):
            lines.append(
                f"- {row.market} {row.event_phase} {row.specification}：样本量 `{int(row.n_obs)}`，"
                f"R² 为 `{_fmt(row.r_squared)}`。"
            )

    return "\n".join(lines) + "\n"


def _profile_paths(profile: str, *, root: Path = ROOT) -> dict[str, str]:
    if profile == "real":
        return {
            "event_summary": str(root / "results" / "real_event_study" / "event_study_summary.csv"),
            "regression_models": str(root / "results" / "real_regressions" / "regression_models.csv"),
            "regression_coefficients": str(root / "results" / "real_regressions" / "regression_coefficients.csv"),
            "output": str(root / "results" / "real_tables" / "research_summary.md"),
            "results_manifest": str(root / "results" / "real_tables" / "results_manifest.csv"),
            "rdd_output_dir": str(root / "results" / "literature" / "hs300_rdd"),
        }
    return {
        "event_summary": str(root / "results" / "event_study" / "event_study_summary.csv"),
        "regression_models": str(root / "results" / "regressions" / "regression_models.csv"),
        "regression_coefficients": str(root / "results" / "regressions" / "regression_coefficients.csv"),
        "output": str(root / "results" / "tables" / "research_summary.md"),
        "results_manifest": str(root / "results" / "tables" / "results_manifest.csv"),
        "rdd_output_dir": str(root / "results" / "literature" / "hs300_rdd"),
    }


def _detect_profile(*, root: Path = ROOT) -> str:
    real_markers = [
        root / "results" / "real_event_study" / "event_study_summary.csv",
        root / "results" / "real_regressions" / "regression_models.csv",
        root / "results" / "real_regressions" / "regression_coefficients.csv",
    ]
    return "real" if all(path.exists() for path in real_markers) else "sample"


def _resolve_cli_args(args: argparse.Namespace, *, root: Path = ROOT) -> argparse.Namespace:
    resolved = argparse.Namespace(**vars(args))
    requested_profile = getattr(resolved, "profile", "auto")
    profile = _detect_profile(root=root) if requested_profile == "auto" else requested_profile
    defaults = _profile_paths(profile, root=root)
    for key, value in defaults.items():
        if not getattr(resolved, key):
            setattr(resolved, key, value)
    resolved.profile = profile
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a paper-ready markdown summary from model outputs.")
    parser.add_argument(
        "--profile",
        choices=["auto", "sample", "real"],
        default="auto",
        help="Report profile. Defaults to auto and prefers the real-data workflow when available.",
    )
    parser.add_argument("--event-summary", default="", help="Event-study summary CSV.")
    parser.add_argument("--regression-models", default="", help="Regression model summary CSV.")
    parser.add_argument("--regression-coefficients", default="", help="Regression coefficients CSV.")
    parser.add_argument("--results-manifest", default="", help="Structured result contract CSV.")
    parser.add_argument("--rdd-output-dir", default="", help="RDD output directory with rdd_status.csv.")
    parser.add_argument("--output", default="", help="Markdown report output path.")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _resolve_cli_args(build_parser().parse_args(argv))

    event_summary = pd.read_csv(args.event_summary)
    regression_models = pd.read_csv(args.regression_models)
    regression_coefficients = pd.read_csv(args.regression_coefficients)
    manifest_payload = load_results_manifest(args.results_manifest)
    if not manifest_payload:
        rdd_status = load_rdd_status(ROOT, output_dir=Path(args.rdd_output_dir) if args.rdd_output_dir else None)
        manifest_frame = build_results_manifest(args.profile, rdd_status)
        save_dataframe(manifest_frame, args.results_manifest)
        manifest_payload = load_results_manifest(args.results_manifest)
    report_text = build_report_text(
        event_summary,
        regression_models,
        regression_coefficients,
        results_manifest=manifest_payload,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding="utf-8")
    print(f"Saved markdown report to {output_path} (profile: {args.profile})")


if __name__ == "__main__":
    main()
