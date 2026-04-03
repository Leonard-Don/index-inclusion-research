from __future__ import annotations

from pathlib import Path

import pandas as pd

from _literature_runner import (
    ROOT,
    ensure_real_data,
    filter_events,
    prepare_clean_events,
    prepare_panel,
    print_frame,
    run_event_study_bundle,
    write_markdown,
)

from index_inclusion_research.literature import summarise_mechanism_changes
from index_inclusion_research.loaders import save_dataframe


def run_analysis(verbose: bool = True) -> dict[str, object]:
    output_dir = ROOT / "results" / "literature" / "harris_gurel"
    events, prices, benchmarks = ensure_real_data()
    clean_events = prepare_clean_events(events)
    panel = prepare_panel(clean_events, prices, benchmarks, window_pre=20, window_post=20)
    event_level, summary, _ = run_event_study_bundle(panel, [(-1, 1), (-3, 3), (-5, 5)], output_dir)

    mechanism_summary = summarise_mechanism_changes(event_level)
    save_dataframe(mechanism_summary, output_dir / "mechanism_summary.csv")
    short_window = summary.loc[(summary["inclusion"] == 1) & (summary["window_slug"].isin(["m1_p1", "m3_p3", "m5_p5"]))].copy()

    report = "\n".join(
        [
            "# 短期价格压力与效应减弱结果包",
            "",
            "这条研究主线重点展示短窗口事件研究结果，以及事件前后交易活跃度变化。",
            "",
            "关键输出文件：",
            f"- 事件研究汇总：`{output_dir / 'event_study_summary.csv'}`",
            f"- 机制变量汇总：`{output_dir / 'mechanism_summary.csv'}`",
            f"- 图表目录：`{output_dir / 'figures'}`",
            "",
            "解释提示：",
            "- 如果短窗口 CAR 为正，但后续路径很快走平或回落，那么结果更接近短期价格压力。",
            "- 如果换手率和成交量在事件前后明显放大，那么交易冲击的解释会更强。",
            "",
        ]
    )
    write_markdown(output_dir / "summary.md", report)

    figure_paths = sorted((output_dir / "figures").glob("*.png"))
    result = {
        "id": "harris_gurel",
        "title": "短期价格压力与效应减弱",
        "output_dir": output_dir,
        "summary_path": output_dir / "summary.md",
        "tables": {
            "短窗口 CAR 汇总": short_window,
            "机制变量汇总": mechanism_summary,
        },
        "figures": figure_paths,
        "description": "用短窗口 CAR 和交易活跃度变化检验价格压力与效应减弱。",
    }
    if verbose:
        print("\nHarris-Gurel startup script completed.")
        print(f"Output directory: {output_dir}")
        print_frame(
            "Short-window CAR summary",
            short_window,
            columns=["market", "event_phase", "window", "n_events", "mean_car", "t_stat", "p_value"],
        )
        print_frame(
            "Mechanism summary",
            mechanism_summary,
            columns=[
                "market",
                "event_phase",
                "n_events",
                "mean_turnover_change",
                "mean_volume_change",
                "mean_volatility_change",
            ],
        )
        if figure_paths:
            print("\nFigures:")
            for path in figure_paths:
                print(path)
    return result


def main() -> None:
    run_analysis(verbose=True)


if __name__ == "__main__":
    main()
