from __future__ import annotations


from _literature_runner import (
    display_path,
    ROOT,
    ensure_real_data,
    filter_events,
    prepare_clean_events,
    prepare_matched_panel,
    prepare_panel,
    print_frame,
    run_event_study_bundle,
    run_regression_bundle,
    write_markdown,
)
from index_inclusion_research.literature import compute_did_summary
from index_inclusion_research.loaders import save_dataframe


def run_analysis(verbose: bool = True) -> dict[str, object]:
    output_dir = ROOT / "results" / "literature" / "hs300_style"
    events, prices, benchmarks = ensure_real_data()
    clean_events = prepare_clean_events(events)
    cn_events = filter_events(clean_events, markets=["CN"], index_name="CSI300")

    treated_panel = prepare_panel(cn_events, prices, benchmarks, window_pre=20, window_post=20)
    event_level, summary, _ = run_event_study_bundle(treated_panel, [(-1, 1), (-3, 3), (-5, 5)], output_dir / "treated_only")

    matched_panel, diagnostics = prepare_matched_panel(cn_events, prices, benchmarks, window_pre=20, window_post=20)
    save_dataframe(diagnostics, output_dir / "match_diagnostics.csv")
    did_summary = compute_did_summary(matched_panel)
    save_dataframe(did_summary, output_dir / "did_summary.csv")
    _, coefficients, model_stats = run_regression_bundle(
        matched_panel,
        [(-1, 1), (-3, 3), (-5, 5)],
        output_dir / "matched_regressions",
        main_car_slug="m1_p1",
    )

    report = "\n".join(
        [
            "# 制度识别与中国市场证据：匹配对照组结果包",
            "",
            "这部分聚焦中国样本、匹配对照组和 DID 风格的前后变化比较。",
            "",
            "重要边界：",
            "- 这里是与文献风格一致的强化识别结果，不等于断点回归结果本身。",
            "- 如果仓库同时生成了 `hs300_rdd` 结果包，RDD 的证据等级、来源口径和下一步命令以下方断点回归状态为准。",
            "",
            "关键输出文件：",
            f"- 中国样本事件研究：`{display_path(output_dir / 'treated_only' / 'event_study_summary.csv')}`",
            f"- DID 汇总：`{display_path(output_dir / 'did_summary.csv')}`",
            f"- 匹配回归系数：`{display_path(output_dir / 'matched_regressions' / 'regression_coefficients.csv')}`",
            "",
        ]
    )
    write_markdown(output_dir / "summary.md", report)

    treated_short = summary.loc[(summary["inclusion"] == 1) & (summary["window_slug"] == "m1_p1")].copy()
    treatment_rows = coefficients.loc[coefficients["parameter"] == "treatment_group"].copy()
    figures = sorted((output_dir / "treated_only" / "figures").glob("*.png"))
    result = {
        "id": "hs300_style",
        "title": "制度识别与中国市场证据：匹配对照组",
        "output_dir": output_dir,
        "summary_path": output_dir / "summary.md",
        "tables": {
            "中国样本短窗口 CAR": treated_short,
            "DID 风格汇总": did_summary,
            "处理组变量回归系数": treatment_rows,
            "模型统计量": model_stats,
        },
        "figures": figures,
        "description": "中国样本的匹配对照组与 DID 风格识别证据。",
    }
    if verbose:
        print("\nHS300-style startup script completed.")
        print(f"Output directory: {output_dir}")
        print_frame(
            "China treated-only CAR summary",
            treated_short,
            columns=["market", "event_phase", "window", "n_events", "mean_car", "t_stat", "p_value"],
        )
        print_frame(
            "DID-style summary",
            did_summary,
            columns=[
                "market",
                "event_phase",
                "metric",
                "treated_post_minus_pre",
                "control_post_minus_pre",
                "did_estimate",
                "n_treated",
                "n_control",
            ],
        )
        print_frame(
            "Matched regression treatment coefficients",
            treatment_rows,
            columns=[
                "market",
                "event_phase",
                "specification",
                "coefficient",
                "std_error",
                "t_stat",
                "p_value",
            ],
        )
        if figures:
            print("\nFigures:")
            for path in figures:
                print(path)
    return result


def main() -> None:
    run_analysis(verbose=True)


if __name__ == "__main__":
    main()
