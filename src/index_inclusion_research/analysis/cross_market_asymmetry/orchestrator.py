from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import paths as project_paths

from . import (
    gap_period,
    h6_robustness,
    heterogeneity,
    hypotheses,
    mechanism_panel,
    paths,
    time_series,
    verdicts,
)

ROOT = project_paths.project_root()
REAL_TABLES_DIR = ROOT / "results" / "real_tables"
REAL_FIGURES_DIR = ROOT / "results" / "real_figures"
REAL_EVENT_PANEL = ROOT / "data" / "processed" / "real_event_panel.csv"
REAL_MATCHED_EVENT_PANEL = ROOT / "data" / "processed" / "real_matched_event_panel.csv"
REAL_EVENTS_CLEAN = ROOT / "data" / "processed" / "real_events_clean.csv"
WEIGHT_CHANGE_PATH = ROOT / "data" / "processed" / "hs300_weight_change.csv"
DEFAULT_PASSIVE_AUM_PATH = ROOT / "data" / "raw" / "passive_aum.csv"
PAPER_VERDICT_PATH = ROOT / "docs" / "paper_outline_verdicts.md"

APPEND_MARKER = "## 六、美股 vs A股 不对称"


def _load_panel(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def _append_research_summary(
    *,
    summary_path: Path,
    window_summary: pd.DataFrame,
    gap_summary: pd.DataFrame,
    mechanism_table: pd.DataFrame,
    hypothesis_verdicts: pd.DataFrame,
) -> None:
    lines: list[str] = ["", APPEND_MARKER, ""]
    lines.append("### 4 象限 CAR[-1,+1] 摘要")
    focus = window_summary.loc[
        (window_summary["window_start"] == -1) & (window_summary["window_end"] == 1)
    ]
    for _, row in focus.iterrows():
        car_mean = row["car_mean"] if pd.notna(row["car_mean"]) else float("nan")
        car_t = row["car_t"] if pd.notna(row["car_t"]) else float("nan")
        lines.append(
            f"- {row['market']} {row['event_phase']}：CAR[-1,+1] = `{car_mean:.4f}`，"
            f"t = `{car_t:.2f}`，n = `{int(row['n_events'])}`"
        )
    lines.append("")
    lines.append("### 空窗期与生效日")
    for _, row in gap_summary.iterrows():
        mean = row["mean"] if pd.notna(row["mean"]) else float("nan")
        t = row["t"] if pd.notna(row["t"]) else float("nan")
        lines.append(
            f"- {row['market']} {row['metric']}：均值 `{mean:.4f}`，t = `{t:.2f}`，n = `{int(row['n_events'])}`"
        )
    lines.append("")
    lines.append("### 机制差异（no_fe）")
    focus_mech = mechanism_table.loc[
        (mechanism_table["spec"] == "no_fe")
        & (
            mechanism_table["outcome"].isin(
                ["car_1_1", "turnover_change", "price_limit_hit_share"]
            )
        )
    ]
    for _, row in focus_mech.iterrows():
        coef = row["coef"] if pd.notna(row["coef"]) else float("nan")
        t = row["t"] if pd.notna(row["t"]) else float("nan")
        lines.append(
            f"- {row['market']} {row['event_phase']} {row['outcome']}：coef = `{coef:.4f}`，t = `{t:.2f}`"
        )
    if not hypothesis_verdicts.empty:
        lines.append("")
        lines.append("### CN/US 不对称机制裁决")
        lines.append("")
        lines.append(
            "下面 7 条假说回答的是 \"为什么 CN/US 反应不一致\","
            "不是回答 \"指数纳入是否产生超额收益\"(后者见上文事件研究主结论)。"
        )
        lines.append("")
        lines.append(
            "| 假说 | 名称 | 裁决 | 可信度 | 头条指标 | 值 | n | 关键证据 |"
        )
        lines.append("|---|---|---|---|---|---|---|---|")
        for _, row in hypothesis_verdicts.iterrows():
            evidence = str(row["evidence_summary"]).replace("|", "\\|").replace("\n", " ")
            label = str(row.get("key_label", "") or "—")
            value = row.get("key_value")
            try:
                value_f = float(value) if value is not None else float("nan")
            except (TypeError, ValueError):
                value_f = float("nan")
            value_text = f"{value_f:.3f}" if value_f == value_f else "—"  # NaN check
            n_obs_raw = row.get("n_obs")
            try:
                n_obs_int = int(n_obs_raw) if n_obs_raw is not None else 0
            except (TypeError, ValueError):
                n_obs_int = 0
            n_text = str(n_obs_int) if n_obs_int > 0 else "—"
            lines.append(
                f"| {row['hid']} | {row['name_cn']} | {row['verdict']} | "
                f"{row['confidence']} | {label} | {value_text} | {n_text} | {evidence} |"
            )

    existing = ""
    if summary_path.exists():
        existing = summary_path.read_text()
        if APPEND_MARKER in existing:
            existing = existing.split(APPEND_MARKER)[0].rstrip() + "\n"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(existing + "\n".join(lines) + "\n")


def run_cma_pipeline(
    *,
    event_panel_path: Path = REAL_EVENT_PANEL,
    matched_panel_path: Path = REAL_MATCHED_EVENT_PANEL,
    events_path: Path = REAL_EVENTS_CLEAN,
    tables_dir: Path = REAL_TABLES_DIR,
    figures_dir: Path = REAL_FIGURES_DIR,
    research_summary_path: Path | None = None,
    paper_verdict_path: Path | None = None,
    aum_path: Path | None = None,
    significance_level: float | None = None,
) -> dict[str, object]:
    event_panel = _load_panel(Path(event_panel_path))
    matched_panel = _load_panel(Path(matched_panel_path))
    events = _load_panel(Path(events_path))

    tables_dir = Path(tables_dir)
    figures_dir = Path(figures_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    ar_panel = paths.build_daily_ar_panel(event_panel)
    avg = paths.compute_average_paths(ar_panel)
    window_summary = paths.compute_window_summary(ar_panel)
    paths.export_path_tables(ar_panel, avg, window_summary, output_dir=tables_dir)
    paths.render_path_figures(avg, output_dir=figures_dir)

    gap = gap_period.compute_gap_metrics(events, event_panel)
    gap_summary = gap_period.summarize_gap_metrics(gap)
    gap_period.export_gap_tables(gap, gap_summary, output_dir=tables_dir)
    gap_period.render_gap_figures(gap, gap_summary, output_dir=figures_dir)
    pre_runup_bootstrap = gap_period.compute_pre_runup_bootstrap_test(
        gap, block_by="announce_date"
    )
    gap_period.export_pre_runup_bootstrap_table(pre_runup_bootstrap, output_dir=tables_dir)
    gap_drift_regression = gap_period.compute_gap_drift_cross_market_regression(gap)
    gap_period.export_gap_drift_cross_market_regression_table(
        gap_drift_regression, output_dir=tables_dir
    )

    mech_panel = mechanism_panel.build_mechanism_panel(matched_panel)
    mech_table = mechanism_panel.assemble_mechanism_comparison_table(mech_panel)
    mechanism_panel.export_mechanism_tables(mech_table, output_dir=tables_dir)
    mechanism_panel.render_mechanism_heatmap(mech_table, output_dir=figures_dir)
    channel_concentration = mechanism_panel.compute_channel_concentration_table(mech_table)
    mechanism_panel.export_channel_concentration_table(
        channel_concentration, output_dir=tables_dir
    )
    limit_regression = mechanism_panel.compute_h5_limit_predictive_regression(mech_panel)
    mechanism_panel.export_h5_limit_predictive_regression_table(
        limit_regression, output_dir=tables_dir
    )

    het_tables: dict[str, pd.DataFrame] = {}
    for dim in ("size", "liquidity", "sector", "gap_bucket"):
        try:
            buckets = heterogeneity.build_heterogeneity_panel(
                event_panel,
                dim=dim,
                gap_frame=gap if dim == "gap_bucket" else None,
            )
            stats = heterogeneity.compute_cell_statistics(
                event_panel, buckets, gap_frame=gap
            )
            het_tables[dim] = stats
        except Exception as exc:  # noqa: BLE001
            het_tables[dim] = pd.DataFrame({"error": [str(exc)]})
    heterogeneity.export_heterogeneity_tables(het_tables, output_dir=tables_dir)
    if "size" in het_tables and "asymmetry_index" in het_tables["size"].columns:
        heterogeneity.render_heterogeneity_matrix(
            het_tables["size"], dim="size", output_dir=figures_dir
        )

    rolling = time_series.build_rolling_car(event_panel)
    break_df = time_series.summarize_structural_break(rolling)
    time_series.export_time_series_tables(rolling, break_df, output_dir=tables_dir)
    aum_frame = None
    resolved_aum_path = Path(aum_path) if aum_path is not None else DEFAULT_PASSIVE_AUM_PATH
    if resolved_aum_path.exists():
        aum_frame = pd.read_csv(resolved_aum_path)
    time_series.render_rolling_figure(
        rolling, output_dir=figures_dir, aum_frame=aum_frame
    )

    hypotheses.export_hypothesis_map(output_dir=tables_dir)
    weight_change_frame = (
        pd.read_csv(WEIGHT_CHANGE_PATH) if WEIGHT_CHANGE_PATH.exists() else None
    )
    h6_weight_robustness = h6_robustness.compute_h6_weight_robustness(
        weight_change_frame,
        gap,
    )
    h6_robustness.export_h6_weight_robustness(
        h6_weight_robustness,
        output_dir=tables_dir,
    )
    hypothesis_verdicts = verdicts.build_hypothesis_verdicts(
        gap_summary=gap_summary,
        mechanism_panel=mech_table,
        heterogeneity_size=het_tables.get("size", pd.DataFrame()),
        time_series_rolling=rolling,
        aum_frame=aum_frame,
        pre_runup_bootstrap=pre_runup_bootstrap,
        gap_drift_regression=gap_drift_regression,
        channel_concentration=channel_concentration,
        heterogeneity_sector=het_tables.get("sector", pd.DataFrame()),
        weight_change=weight_change_frame,
        gap_event_level=gap,
        h6_weight_robustness=h6_weight_robustness,
        limit_regression=limit_regression,
        significance_level=(
            significance_level
            if significance_level is not None
            else verdicts.SIGNIFICANCE_LEVEL
        ),
    )
    h6_verdict = None
    if not hypothesis_verdicts.empty and "hid" in hypothesis_verdicts.columns:
        h6_rows = hypothesis_verdicts.loc[
            hypothesis_verdicts["hid"].astype(str) == "H6"
        ]
        if not h6_rows.empty:
            h6_verdict = h6_rows.iloc[0]
    h6_weight_explanation = h6_robustness.build_h6_weight_explanation(
        h6_verdict=h6_verdict,
        robustness=h6_weight_robustness,
    )
    h6_robustness.export_h6_weight_explanation(
        h6_weight_explanation,
        output_dir=tables_dir,
    )
    verdicts_csv_path = tables_dir / "cma_hypothesis_verdicts.csv"
    # Auto-snapshot the previous verdicts so users can always run
    # `index-inclusion-verdict-summary --compare-with cma_hypothesis_verdicts.previous.csv`
    # to see what flipped between two pipeline runs, without having to remember
    # to take a manual snapshot before a re-run.
    if verdicts_csv_path.exists():
        previous_path = tables_dir / "cma_hypothesis_verdicts.previous.csv"
        previous_path.write_bytes(verdicts_csv_path.read_bytes())
    hypothesis_verdicts.to_csv(verdicts_csv_path, index=False)
    verdicts.export_hypothesis_verdicts_tex(hypothesis_verdicts, output_dir=tables_dir)
    if paper_verdict_path is None:
        paper_verdict_path = (
            PAPER_VERDICT_PATH
            if tables_dir.resolve() == REAL_TABLES_DIR.resolve()
            else tables_dir / "paper_outline_verdicts.md"
        )
    event_counts_path = tables_dir / "event_counts_by_year.csv"
    paper_event_counts = (
        pd.read_csv(event_counts_path) if event_counts_path.exists() else None
    )
    event_study_summary_path = tables_dir / "event_study_summary.csv"
    paper_event_study_summary = (
        pd.read_csv(event_study_summary_path)
        if event_study_summary_path.exists()
        else None
    )
    verdicts.export_paper_verdict_section(
        hypothesis_verdicts,
        output_path=paper_verdict_path,
        event_counts=paper_event_counts,
        event_study_summary=paper_event_study_summary,
    )
    hypotheses.export_track_verdict_summary(
        hypothesis_verdicts, output_dir=tables_dir
    )

    if research_summary_path is not None:
        _append_research_summary(
            summary_path=Path(research_summary_path),
            window_summary=window_summary,
            gap_summary=gap_summary,
            mechanism_table=mech_table,
            hypothesis_verdicts=hypothesis_verdicts,
        )

    return {
        "tables_dir": tables_dir,
        "figures_dir": figures_dir,
        "paper_verdict_path": paper_verdict_path,
        "tables_count": sum(1 for _ in tables_dir.glob("cma_*")),
        "figures_count": sum(1 for _ in figures_dir.glob("cma_*.png")),
    }


def regenerate_tex_only(*, tables_dir: Path = REAL_TABLES_DIR) -> dict[str, Path]:
    tables_dir = Path(tables_dir)
    csv_path = tables_dir / "cma_mechanism_panel.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Missing cma_mechanism_panel.csv under {tables_dir}"
        )
    table = pd.read_csv(csv_path)
    return mechanism_panel.export_mechanism_tables(table, output_dir=tables_dir)
