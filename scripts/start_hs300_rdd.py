from __future__ import annotations

import argparse
import shutil
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
from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit as _build_candidate_batch_audit,
    summarize_candidate_audit as _summarize_candidate_audit,
    validate_candidate_frame as _validate_candidate_frame,
)
from index_inclusion_research.loaders import save_dataframe
from index_inclusion_research.pipeline import build_event_sample, build_matched_sample

REAL_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
RECONSTRUCTED_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
DEMO_INPUT = ROOT / "data" / "raw" / "hs300_rdd_demo.csv"
TEMPLATE_INPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.template.csv"
OUTPUT_DIR = ROOT / "results" / "literature" / "hs300_rdd"
STATUS_FILE = OUTPUT_DIR / "rdd_status.csv"
AUDIT_FILE = OUTPUT_DIR / "candidate_batch_audit.csv"


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _prepare_candidate_command(*, packaged: bool, check_only: bool) -> str:
    command = (
        "index-inclusion-prepare-hs300-rdd"
        if packaged
        else "python3 scripts/prepare_hs300_rdd_candidates.py"
    )
    suffix = " --check-only" if check_only else " --output data/raw/hs300_rdd_candidates.csv --force"
    return f"{command} --input /path/to/raw_candidates.xlsx --sheet 0{suffix}"


def _reconstruct_candidate_command(*, packaged: bool) -> str:
    command = (
        "index-inclusion-reconstruct-hs300-rdd"
        if packaged
        else "python3 scripts/reconstruct_hs300_rdd_candidates.py"
    )
    return f"{command} --announce-date 2024-05-31 --output data/raw/hs300_rdd_candidates.reconstructed.csv --force"


def _generate_demo_candidates() -> pd.DataFrame:
    events, prices, _ = ensure_real_data()
    clean_events = prepare_clean_events(events)
    cn_events = clean_events.loc[
        (clean_events["market"] == "CN")
        & (clean_events["index_name"] == "CSI300")
        & (clean_events["inclusion"] == 1)
    ].copy()
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
        treated = group.loc[group["treatment_group"] == 1].copy()
        controls = group.loc[group["treatment_group"] == 0].copy().sort_values("ticker").reset_index(drop=True)
        if not treated.empty:
            treated["inclusion"] = 1
            treated = treated.assign(running_variable=[300.35] * len(treated))
            demo_rows.extend(treated.to_dict(orient="records"))
        demo_scores = [299.85, 299.55, 299.25, 298.95, 298.65]
        for idx, (_, row) in enumerate(controls.iterrows()):
            score = demo_scores[idx] if idx < len(demo_scores) else 298.35 - idx * 0.1
            row_dict = row.to_dict()
            row_dict["inclusion"] = 0
            row_dict["running_variable"] = score
            demo_rows.append(row_dict)

    demo = pd.DataFrame(demo_rows)
    if "security_name" not in demo.columns:
        demo["security_name"] = demo["ticker"].astype(str)
    columns = [
        "batch_id",
        "market",
        "index_name",
        "ticker",
        "security_name",
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
        "source_url",
    ]
    demo = demo.loc[:, [column for column in columns if column in demo.columns]].copy()
    save_dataframe(demo, DEMO_INPUT)
    return demo


def _normalize_demo_frame(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    if "security_name" not in work.columns:
        work["security_name"] = work["ticker"].astype(str)
    return work


def _load_validated_candidate_source(
    path: Path,
    *,
    mode: str,
    success_message: str,
    failure_prefix: str,
    strict_validation: bool,
) -> tuple[pd.DataFrame, str, str]:
    try:
        frame = pd.read_csv(path, low_memory=False)
        validated = _validate_candidate_frame(frame)
        return validated, mode, success_message
    except Exception as exc:
        error = ValueError(f"{failure_prefix}：{exc}")
        if strict_validation:
            raise error
        return pd.DataFrame(), "missing", str(error)


def _load_candidate_file(*, allow_demo: bool, strict_validation: bool) -> tuple[pd.DataFrame, str, str]:
    if REAL_INPUT.exists():
        return _load_validated_candidate_source(
            REAL_INPUT,
            mode="real",
            success_message=f"当前正在使用你提供的真实候选排名文件：`{REAL_INPUT.name}`。",
            failure_prefix="真实候选样本文件校验失败",
            strict_validation=strict_validation,
        )

    if RECONSTRUCTED_INPUT.exists():
        return _load_validated_candidate_source(
            RECONSTRUCTED_INPUT,
            mode="reconstructed",
            success_message=f"当前正在使用公开数据重建的候选样本文件：`{RECONSTRUCTED_INPUT.name}`。公开重建样本可进入公开数据版证据链，但不应表述为中证官方历史候选排名表。",
            failure_prefix="公开重建候选样本文件校验失败",
            strict_validation=strict_validation,
        )

    if allow_demo:
        demo_frame = pd.read_csv(DEMO_INPUT, low_memory=False) if DEMO_INPUT.exists() else _generate_demo_candidates()
        validated_demo = _validate_candidate_frame(_normalize_demo_frame(demo_frame))
        return validated_demo, "demo", "当前处于显式 `--demo` 模式，仅用于方法展示，不进入正式证据链。"

    return (
        pd.DataFrame(),
        "missing",
        "等待正式或公开重建候选样本文件：`data/raw/hs300_rdd_candidates.csv` 或 `data/raw/hs300_rdd_candidates.reconstructed.csv`。当前中国主线的正式证据仅来自事件研究与匹配回归。",
    )


def _clear_rdd_outputs(output_dir: Path) -> None:
    for filename in ["rdd_summary.csv", "event_level_with_running.csv", AUDIT_FILE.name]:
        (output_dir / filename).unlink(missing_ok=True)
    figures_dir = output_dir / "figures"
    if figures_dir.exists():
        shutil.rmtree(figures_dir)


def _validation_error_from_message(message: str) -> str:
    for prefix in ("真实候选样本文件校验失败：", "公开重建候选样本文件校验失败："):
        if message.startswith(prefix):
            return message.removeprefix(prefix).strip()
    return ""


def _status_display(mode: str) -> tuple[str, str]:
    if mode == "real":
        return "正式边界样本", "基于真实候选排名变量，可作为更强识别证据。"
    if mode == "reconstructed":
        return "公开重建样本", "基于公开数据重建的边界样本，可进入公开数据版证据链，但不应表述为中证官方历史候选排名表。"
    if mode == "demo":
        return "方法展示", "当前为显式 demo 模式，只用于方法展示，不进入正式证据链。"
    return "待补正式样本", "等待正式候选样本、公开重建样本，或修复文件校验错误后，RDD 才能进入 L2/L3 证据等级。"


def _write_status(
    output_dir: Path,
    *,
    mode: str,
    message: str,
    input_file: Path,
    used_demo: bool,
    candidate_rows: int | None = None,
    audit: pd.DataFrame | None = None,
    validation_error: str = "",
) -> pd.DataFrame:
    input_path = _display_path(input_file)
    audit_summary = _summarize_candidate_audit(audit if audit is not None else pd.DataFrame())
    evidence_status, default_note = _status_display(mode)
    status_frame = pd.DataFrame(
        [
            {
                "status": mode,
                "evidence_status": evidence_status,
                "message": message,
                "note": default_note,
                "input_file": input_path,
                "input_exists": input_file.exists(),
                "used_demo": used_demo,
                "candidate_rows": candidate_rows if candidate_rows is not None else pd.NA,
                "candidate_batches": audit_summary["candidate_batches"] if audit_summary["candidate_batches"] is not None else pd.NA,
                "treated_rows": audit_summary["treated_rows"] if audit_summary["treated_rows"] is not None else pd.NA,
                "control_rows": audit_summary["control_rows"] if audit_summary["control_rows"] is not None else pd.NA,
                "crossing_batches": audit_summary["crossing_batches"] if audit_summary["crossing_batches"] is not None else pd.NA,
                "audit_file": _display_path(AUDIT_FILE) if audit is not None and not audit.empty else pd.NA,
                "validation_error": validation_error or pd.NA,
            }
        ]
    )
    save_dataframe(status_frame, output_dir / "rdd_status.csv")
    return status_frame


def _write_summary(
    output_dir: Path,
    *,
    mode: str,
    message: str,
    status_frame: pd.DataFrame,
    audit: pd.DataFrame | None = None,
) -> None:
    row = status_frame.iloc[0]
    lines = [
        "# 制度识别与中国市场证据：断点回归结果包",
        "",
        message,
        "",
        "当前状态：",
        f"- 模式：`{row['status']}`",
        f"- 证据状态：`{row['evidence_status']}`",
        f"- 当前口径：{row['note']}",
        f"- 候选样本路径：`{row['input_file']}`",
        "",
        "真实候选样本必需列：",
        "- batch_id",
        "- market",
        "- index_name",
        "- ticker",
        "- security_name",
        "- announce_date",
        "- effective_date",
        "- running_variable",
        "- cutoff",
        "- inclusion",
        "",
        "推荐补充列：",
        "- event_type",
        "- source",
        "- source_url",
        "- note",
        "- sector",
        "",
        f"模板文件：`{_display_path(TEMPLATE_INPUT)}`",
        f"数据契约说明：`{_display_path(ROOT / 'docs' / 'hs300_rdd_data_contract.md')}`",
        "",
        "推荐下一步：",
        f"- 如果已经拿到原始候选名单，先验收：`{_prepare_candidate_command(packaged=True, check_only=True)}`",
        f"- 验收通过后写入正式候选文件：`{_prepare_candidate_command(packaged=True, check_only=False)}`",
        f"- 如果手头没有官方名单，可先重建公开样本：`{_reconstruct_candidate_command(packaged=True)}`",
        f"- 如果尚未安装包内 CLI，可改用脚本：`{_prepare_candidate_command(packaged=False, check_only=True)}` 或 `{_reconstruct_candidate_command(packaged=False)}`",
    ]

    if audit is not None and not audit.empty:
        audit_summary = _summarize_candidate_audit(audit)
        lines.extend(
            [
                "",
                "候选样本审计：",
                f"- 批次数：`{audit_summary['candidate_batches']}`",
                f"- 调入样本数：`{audit_summary['treated_rows']}`",
                f"- 对照候选数：`{audit_summary['control_rows']}`",
                f"- 覆盖断点的批次数：`{audit_summary['crossing_batches']}`",
                f"- 批次审计表：`{_display_path(AUDIT_FILE)}`",
            ]
        )

    if mode == "real":
        lines.extend(
            [
                "",
                f"RDD 汇总文件：`{_display_path(output_dir / 'rdd_summary.csv')}`",
                f"事件层文件：`{_display_path(output_dir / 'event_level_with_running.csv')}`",
                f"图表目录：`{_display_path(output_dir / 'figures')}`",
            ]
        )
    elif mode == "reconstructed":
        lines.extend(
            [
                "",
                "当前正在使用公开数据重建的边界样本。该结果可以进入公开数据版证据链，但应明确标注为公开重建口径，而不是中证官方历史候选排名表。",
                f"RDD 汇总文件：`{_display_path(output_dir / 'rdd_summary.csv')}`",
                f"事件层文件：`{_display_path(output_dir / 'event_level_with_running.csv')}`",
                f"图表目录：`{_display_path(output_dir / 'figures')}`",
            ]
        )
    elif mode == "demo":
        lines.extend(
            [
                "",
                "当前显式启用了 demo 模式。即使生成了系数、图表与摘要，也只用于开发验证，不应用于正式主结论；如需退出方法展示，可改用上面的正式导入或公开重建路径。",
            ]
        )
    elif pd.notna(row["validation_error"]):
        lines.extend(
            [
                "",
                f"校验失败原因：{row['validation_error']}",
            ]
        )

    write_markdown(output_dir / "summary.md", "\n".join(lines) + "\n")


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
            "security_name",
            "announce_date",
            "effective_date",
        ]
    ].rename(columns={"ticker": "candidate_ticker"})
    event_level = event_level.merge(metadata, on=["event_id", "market", "index_name", "inclusion"], how="left")
    event_level["distance_to_cutoff"] = event_level["running_variable"] - event_level["cutoff"]
    return event_level


def run_analysis(
    verbose: bool = True,
    *,
    allow_demo: bool = False,
    strict_validation: bool = False,
) -> dict[str, object]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    validation_error = ""
    try:
        candidates, mode, message = _load_candidate_file(allow_demo=allow_demo, strict_validation=strict_validation)
    except Exception as exc:
        validation_error = str(exc)
        _clear_rdd_outputs(OUTPUT_DIR)
        error_input_file = RECONSTRUCTED_INPUT if str(exc).startswith("公开重建候选样本文件校验失败：") else REAL_INPUT
        status_frame = _write_status(
            OUTPUT_DIR,
            mode="missing",
            message=str(exc),
            input_file=error_input_file,
            used_demo=False,
            candidate_rows=None,
            audit=None,
            validation_error=validation_error,
        )
        _write_summary(OUTPUT_DIR, mode="missing", message=status_frame.iloc[0]["message"], status_frame=status_frame, audit=None)
        raise

    if mode == "missing":
        validation_error = _validation_error_from_message(message)
        _clear_rdd_outputs(OUTPUT_DIR)
        missing_input_file = RECONSTRUCTED_INPUT if message.startswith("公开重建候选样本文件校验失败：") else REAL_INPUT
        status_frame = _write_status(
            OUTPUT_DIR,
            mode="missing",
            message=message,
            input_file=missing_input_file,
            used_demo=False,
            candidate_rows=None,
            audit=None,
            validation_error=validation_error,
        )
        _write_summary(OUTPUT_DIR, mode="missing", message=message, status_frame=status_frame, audit=None)
        result = {
            "id": "hs300_rdd",
            "title": "制度识别与中国市场证据：断点回归",
            "output_dir": OUTPUT_DIR,
            "summary_path": OUTPUT_DIR / "summary.md",
            "tables": {},
            "figures": [],
            "description": message,
            "mode": "missing",
            "status_frame": status_frame,
        }
        if verbose:
            print("\nHS300 RDD startup script completed.")
            print(f"Output directory: {OUTPUT_DIR}")
            print(message)
        return result

    candidate_audit = _build_candidate_batch_audit(candidates)
    event_level = _prepare_rdd_event_level(candidates)
    _clear_rdd_outputs(OUTPUT_DIR)
    save_dataframe(candidate_audit, AUDIT_FILE)
    save_dataframe(event_level, OUTPUT_DIR / "event_level_with_running.csv")

    outcome_cols = ["car_m1_p1", "car_m3_p3", "turnover_change", "volume_change"]
    rdd_summary = run_rdd_suite(event_level, outcome_cols=outcome_cols)
    save_dataframe(rdd_summary, OUTPUT_DIR / "rdd_summary.csv")

    for outcome_col in outcome_cols:
        plot_rdd_bins(
            event_level,
            outcome_col=outcome_col,
            output_path=OUTPUT_DIR / "figures" / f"{outcome_col}_rdd_bins.png",
        )

    input_file = REAL_INPUT if mode == "real" else RECONSTRUCTED_INPUT if mode == "reconstructed" else DEMO_INPUT
    status_frame = _write_status(
        OUTPUT_DIR,
        mode=mode,
        message=message,
        input_file=input_file,
        used_demo=mode == "demo",
        candidate_rows=len(candidates),
        audit=candidate_audit,
    )
    _write_summary(OUTPUT_DIR, mode=mode, message=message, status_frame=status_frame, audit=candidate_audit)

    figures = sorted((OUTPUT_DIR / "figures").glob("*.png"))
    result = {
        "id": "hs300_rdd",
        "title": "制度识别与中国市场证据：断点回归",
        "output_dir": OUTPUT_DIR,
        "summary_path": OUTPUT_DIR / "summary.md",
        "tables": {
            "候选样本批次审计": candidate_audit,
            "RDD 汇总": rdd_summary,
            "事件层数据": event_level,
        },
        "figures": figures,
        "description": message,
        "mode": mode,
        "status_frame": status_frame,
    }
    if verbose:
        print("\nHS300 RDD startup script completed.")
        print(f"Output directory: {OUTPUT_DIR}")
        print(message)
        print_frame(
            "Candidate audit",
            candidate_audit,
            columns=[
                "batch_id",
                "n_candidates",
                "n_included",
                "n_excluded",
                "n_left_of_cutoff",
                "n_right_of_cutoff",
                "has_cutoff_crossing",
            ],
        )
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
    parser = argparse.ArgumentParser(description="Run the HS300 RDD extension.")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Use explicit demo pseudo-ranking input for development only. Demo results do not count as formal evidence.",
    )
    args = parser.parse_args()
    run_analysis(verbose=True, allow_demo=args.demo, strict_validation=True)


if __name__ == "__main__":
    main()
