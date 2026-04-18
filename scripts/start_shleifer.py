from __future__ import annotations


from _literature_runner import (
    ROOT,
    ensure_real_data,
    prepare_clean_events,
    prepare_panel,
    print_frame,
    run_event_study_bundle,
    write_markdown,
)
from index_inclusion_research.literature import compute_retention_summary
from index_inclusion_research.loaders import save_dataframe


def run_analysis(verbose: bool = True) -> dict[str, object]:
    output_dir = ROOT / "results" / "literature" / "shleifer"
    events, prices, benchmarks = ensure_real_data()
    clean_events = prepare_clean_events(events)
    car_windows = [(-1, 1), (0, 5), (0, 20), (0, 60), (0, 120)]
    panel = prepare_panel(clean_events, prices, benchmarks, window_pre=20, window_post=120)
    event_level, summary, _ = run_event_study_bundle(panel, car_windows, output_dir)
    retention_summary = compute_retention_summary(event_level, short_window_slug="p0_p20", long_window_slug="p0_p120")
    save_dataframe(retention_summary, output_dir / "retention_summary.csv")

    report = "\n".join(
        [
            "# 需求曲线与长期保留结果包",
            "",
            "这条研究主线重点展示长窗口累计异常收益、保留效应和部分反转。",
            "",
            "关键输出文件：",
            f"- 事件研究汇总：`{output_dir / 'event_study_summary.csv'}`",
            f"- 保留率汇总：`{output_dir / 'retention_summary.csv'}`",
            f"- 图表目录：`{output_dir / 'figures'}`",
            "",
            "解释提示：",
            "- 如果长窗口 CAR 在初始上涨后仍保持为正，那么更支持向下倾斜的需求曲线。",
            "- 如果长窗口 CAR 大部分消失，那么更接近短期价格压力解释。",
            "",
        ]
    )
    write_markdown(output_dir / "summary.md", report)

    display_summary = summary.loc[
        (summary["inclusion"] == 1) & (summary["window_slug"].isin(["m1_p1", "p0_p20", "p0_p60", "p0_p120"]))
    ].copy()
    figure_paths = sorted((output_dir / "figures").glob("*.png"))
    result = {
        "id": "shleifer",
        "title": "需求曲线与长期保留",
        "output_dir": output_dir,
        "summary_path": output_dir / "summary.md",
        "tables": {
            "窗口 CAR 汇总": display_summary,
            "保留率汇总": retention_summary,
        },
        "figures": figure_paths,
        "description": "用长窗口 CAR 和 retention 检验需求曲线与不完全回吐。",
    }
    if verbose:
        print("\nShleifer startup script completed.")
        print(f"Output directory: {output_dir}")
        print_frame(
            "Window CAR summary",
            display_summary,
            columns=["market", "event_phase", "window", "n_events", "mean_car", "t_stat", "p_value"],
        )
        print_frame(
            "Retention summary",
            retention_summary,
            columns=[
                "market",
                "event_phase",
                "n_events",
                "short_mean_car",
                "long_mean_car",
                "car_reversal",
                "retention_ratio",
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
