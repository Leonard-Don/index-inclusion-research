from __future__ import annotations

from pathlib import Path

import pandas as pd

from _literature_runner import (
    ROOT,
    ensure_real_data,
    prepare_clean_events,
    prepare_panel,
    print_frame,
    write_markdown,
)
from index_inclusion_research.analysis import compute_event_level_metrics, plot_rdd_bins, run_rdd_suite
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.pipeline import build_event_sample, build_matched_sample

REAL_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
DEMO_INPUT = ROOT / "data" / "raw" / "hs300_rdd_demo.csv"


def _generate_demo_candidates() -> pd.DataFrame:
    events, prices, _ = ensure_real_data()
    clean_events = prepare_clean_events(events)
    cn_events = clean_events.loc[(clean_events["market"] == "CN") & (clean_events["index_name"] == "CSI300")].copy()
    matched_events, _ = build_matched_sample(cn_events, prices, lookback_days=20, num_controls=3)
    matched_events["batch_id"] = matched_events["matched_to_event_id"].where(
        matched_events["matched_to_event_id"].notna(),
        matched_events["event_id"],
    )
    matched_events["cutoff"] = 300.0
    matched_events["data_mode"] = "demo_pseudo_running_variable"
    matched_events["note"] = (
        "Demo pseudo-ranking data generated from matched controls. Replace this file with actual pre-adjustment ranking data for real RD evidence."
    )

    demo_rows: list[dict[str, object]] = []
    for _, group in matched_events.groupby("batch_id", dropna=False):
        treated = group.loc[group["inclusion"] == 1].copy()
        controls = group.loc[group["inclusion"] == 0].copy().sort_values("ticker").reset_index(drop=True)
        if not treated.empty:
            treated = treated.assign(running_variable=[300.35] * len(treated))
            demo_rows.extend(treated.to_dict(orient="records"))
        demo_scores = [299.85, 299.55, 299.25, 298.95, 298.65]
        for idx, (_, row) in enumerate(controls.iterrows()):
            score = demo_scores[idx] if idx < len(demo_scores) else 298.35 - idx * 0.1
            row_dict = row.to_dict()
            row_dict["running_variable"] = score
            demo_rows.append(row_dict)

    demo = pd.DataFrame(demo_rows)
    columns = [
        "batch_id",
        "market",
        "index_name",
        "ticker",
        "announce_date",
        "effective_date",
        "event_type",
        "inclusion",
        "running_variable",
        "cutoff",
        "data_mode",
        "note",
        "sector",
        "source",
    ]
    demo = demo.loc[:, [column for column in columns if column in demo.columns]].copy()
    save_dataframe(demo, DEMO_INPUT)
    return demo


def _load_candidate_file() -> tuple[pd.DataFrame, str]:
    if REAL_INPUT.exists():
        frame = pd.read_csv(REAL_INPUT, parse_dates=["announce_date", "effective_date"])
        return frame, "real"
    return _generate_demo_candidates(), "demo"


def _prepare_rdd_event_level(candidates: pd.DataFrame) -> pd.DataFrame:
    _, prices, benchmarks = ensure_real_data()
    events = build_event_sample(candidates.copy())
    panel = prepare_panel(events, prices, benchmarks, window_pre=20, window_post=20)
    event_level = compute_event_level_metrics(panel, [(-1, 1), (-3, 3), (-5, 5)])
    metadata = events[
        [
            "event_id",
            "batch_id",
            "running_variable",
            "cutoff",
            "inclusion",
            "market",
            "index_name",
            "ticker",
            "announce_date",
            "effective_date",
        ]
    ].rename(columns={"ticker": "candidate_ticker"})
    event_level = event_level.merge(metadata, on=["event_id", "market", "index_name", "inclusion"], how="left")
    event_level["distance_to_cutoff"] = event_level["running_variable"] - event_level["cutoff"]
    return event_level


def run_analysis(verbose: bool = True) -> dict[str, object]:
    output_dir = ROOT / "results" / "literature" / "hs300_rdd"
    candidates, mode = _load_candidate_file()
    if "batch_id" not in candidates.columns:
        raise ValueError("RDD candidate file must include batch_id.")
    if "running_variable" not in candidates.columns or "cutoff" not in candidates.columns:
        raise ValueError("RDD candidate file must include running_variable and cutoff.")
    if "event_type" not in candidates.columns:
        candidates["event_type"] = "inclusion_rdd"
    if "inclusion" not in candidates.columns:
        raise ValueError("RDD candidate file must include inclusion.")

    event_level = _prepare_rdd_event_level(candidates)
    output_dir.mkdir(parents=True, exist_ok=True)
    save_dataframe(event_level, output_dir / "event_level_with_running.csv")

    outcome_cols = ["car_m1_p1", "car_m3_p3", "turnover_change", "volume_change"]
    rdd_summary = run_rdd_suite(event_level, outcome_cols=outcome_cols)
    save_dataframe(rdd_summary, output_dir / "rdd_summary.csv")

    for outcome_col in outcome_cols:
        plot_rdd_bins(
            event_level,
            outcome_col=outcome_col,
            output_path=output_dir / "figures" / f"{outcome_col}_rdd_bins.png",
        )

    data_note = (
        "当前正在使用你提供的真实候选排名文件。"
        if mode == "real"
        else "当前正在使用 demo 伪排名数据。把 data/raw/hs300_rdd_candidates.csv 换成真实候选排名文件后，才能得到正式 RD 证据。"
    )
    write_markdown(
        output_dir / "summary.md",
        "\n".join(
            [
                "# 制度识别与中国市场证据：断点回归结果包",
                "",
                data_note,
                "",
                "真实数据必需列：",
                "- batch_id",
                "- market",
                "- index_name",
                "- ticker",
                "- announce_date",
                "- effective_date",
                "- inclusion",
                "- running_variable",
                "- cutoff",
                "",
                f"RDD 汇总文件：`{output_dir / 'rdd_summary.csv'}`",
                f"事件层文件：`{output_dir / 'event_level_with_running.csv'}`",
                f"图表目录：`{output_dir / 'figures'}`",
                "",
            ]
        ),
    )
    figures = sorted((output_dir / "figures").glob("*.png"))
    result = {
        "id": "hs300_rdd",
        "title": "制度识别与中国市场证据：断点回归",
        "output_dir": output_dir,
        "summary_path": output_dir / "summary.md",
        "tables": {
            "RDD 汇总": rdd_summary,
            "事件层数据": event_level,
        },
        "figures": figures,
        "description": data_note,
        "mode": mode,
    }
    if verbose:
        print("\nHS300 RDD startup script completed.")
        print(f"Output directory: {output_dir}")
        print(data_note)
        print_frame(
            "RDD summary",
            rdd_summary,
            columns=["outcome", "bandwidth", "n_obs", "n_left", "n_right", "tau", "std_error", "t_stat", "p_value"],
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
