from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pandas as pd

from index_inclusion_research.dashboard_media import build_figure_entry
from index_inclusion_research.result_contract import (
    load_rdd_status as load_shared_rdd_status,
    load_results_manifest as load_shared_results_manifest,
)
from index_inclusion_research.dashboard_types import (
    AnalysesConfig,
    AnalysisDefinition,
    CsvFrameReader,
    FigureEntry,
    FigureCaptionBuilder,
    LabelTranslator,
    RawAnalysisResult,
    RawFigureEntry,
    RelativePathBuilder,
    RddContractCheck,
    RddStatus,
    RddStatusLoader,
    RenderedTable,
    SavedTablesLoader,
    TableRenderer,
    TrackContextAttacher,
    TrackResult,
    TrackResultLoader,
)


def translate_label(label: str, table_labels: Mapping[str, str]) -> str:
    return table_labels.get(label, label)


def format_figure_caption(path: Path, column_labels: Mapping[str, str]) -> str:
    stem = path.stem
    if stem == "average_abnormal_returns":
        return "平均异常收益路径图"
    if stem.endswith("_rdd_bins"):
        outcome = stem.removesuffix("_rdd_bins")
        outcome_label = column_labels.get(outcome, outcome)
        return f"{outcome_label} 分箱图"
    if stem.endswith("_rdd_main"):
        outcome = stem.removesuffix("_rdd_main")
        outcome_label = column_labels.get(outcome, outcome)
        return f"{outcome_label} 断点回归主图"
    if stem.endswith("_car_path"):
        parts = stem.split("_")
        if len(parts) >= 3:
            market = column_labels.get(parts[0].upper(), parts[0].upper())
            phase = column_labels.get(parts[1], parts[1])
            return f"{market}{phase}平均异常收益路径图"
    return stem.replace("_", " ")


def build_figure_caption(
    path: Path,
    *,
    column_labels: Mapping[str, str],
    custom_caption: str | None = None,
    prefix: str | None = None,
) -> str:
    stem = path.stem
    if custom_caption:
        caption = custom_caption
    elif stem.endswith("_car_path"):
        parts = stem.split("_")
        market = column_labels.get(parts[0].upper(), parts[0].upper()) if len(parts) >= 1 else "样本"
        phase = column_labels.get(parts[1], parts[1]) if len(parts) >= 2 else "事件阶段"
        caption = f"图意：展示 {market}{phase} 的累计异常收益路径。阅读重点：比较事件日前后的斜率变化，以及 0 日之后价格是否继续累积或出现回吐。"
    elif stem.endswith("_rdd_bins"):
        outcome = stem.removesuffix("_rdd_bins")
        outcome_label = column_labels.get(outcome, outcome)
        caption = f"图意：展示 {outcome_label} 在断点两侧的分箱均值。阅读重点：观察 0 附近是否出现明显跳跃，以及左右两侧样本均值是否系统分离。"
    elif stem.endswith("_rdd_main"):
        outcome = stem.removesuffix("_rdd_main")
        outcome_label = column_labels.get(outcome, outcome)
        caption = f"中国样本 RDD 主图。图意：展示 {outcome_label} 的断点回归主图。阅读重点：同时观察断点两侧的分箱均值与拟合线，在 0 附近判断是否存在结构性跳跃。"
    elif stem == "sample_event_timeline":
        caption = "图意：展示真实调入/调出事件在时间轴上的分布。阅读重点：判断样本是否集中在少数批次，以及公告日和生效日是否在时间上形成清晰分层。"
    elif stem == "sample_car_heatmap":
        caption = "图意：把短窗口 CAR 在市场与事件阶段两个维度上压缩成一张总览图。阅读重点：优先比较美国公告日与中国生效日所在单元格的方向、幅度和显著性。"
    elif stem == "main_regression_coefficients":
        caption = "图意：展示主回归中处理组变量的估计系数与置信区间。阅读重点：比较不同市场、不同事件阶段下系数的方向与显著性，而不只看点估计大小。"
    elif stem == "mechanism_regression_coefficients":
        caption = "图意：展示机制回归中处理组变量对换手率、成交量和波动率的影响。阅读重点：比较中国 A 股与美国在公告日、生效日的机制方向是否一致。"
    elif stem == "match_diagnostics_overview":
        caption = "图意：同时展示匹配状态分布与匹配质量指标。阅读重点：先看匹配成功率，再看三对照构造和行业口径放宽占比，以判断对照组设计的稳定性。"
    else:
        caption = format_figure_caption(path, column_labels)
    if prefix:
        return f"{prefix}：{caption}"
    return caption


def normalize_result(
    raw: RawAnalysisResult,
    *,
    translate_label: LabelTranslator,
    render_table: TableRenderer,
    to_relative: RelativePathBuilder,
    build_figure_caption: FigureCaptionBuilder,
) -> TrackResult:
    summary_path = raw.get("summary_path")
    summary_text = raw.get("summary_text", "") if isinstance(raw.get("summary_text"), str) else ""
    if isinstance(summary_path, Path) and summary_path.exists():
        summary_text = summary_path.read_text(encoding="utf-8")
    tables: list[RenderedTable] = []
    table_frames = raw.get("tables", {})
    if isinstance(table_frames, Mapping):
        for label, frame in table_frames.items():
            if frame is None:
                continue
            tables.append((translate_label(str(label)), render_table(frame)))
    figure_paths: list[FigureEntry] = []
    for item in raw.get("figures", []):
        if isinstance(item, Path):
            figure_paths.append(build_figure_entry(item, to_relative=to_relative, caption=build_figure_caption(item)))
        elif isinstance(item, dict) and isinstance(item.get("path"), Path):
            figure_item = cast(RawFigureEntry, item)
            figure_paths.append(
                build_figure_entry(
                    figure_item["path"],
                    to_relative=to_relative,
                    caption=build_figure_caption(
                        figure_item["path"],
                        custom_caption=figure_item.get("caption"),
                        prefix=figure_item.get("prefix"),
                    ),
                )
            )
    output_dir = raw.get("output_dir")
    return {
        "id": raw.get("id"),
        "title": raw.get("title"),
        "description": raw.get("description", ""),
        "subtitle": raw.get("subtitle", ""),
        "summary_text": summary_text,
        "rendered_tables": tables,
        "figure_paths": figure_paths,
        "output_dir": to_relative(output_dir) if isinstance(output_dir, Path) else output_dir,
    }


def load_saved_tables(
    output_dir: Path,
    *,
    translate_label: LabelTranslator,
    render_table: TableRenderer,
    max_tables: int = 6,
) -> list[RenderedTable]:
    csv_files = sorted(output_dir.rglob("*.csv"))
    tables: list[RenderedTable] = []
    seen: set[str] = set()
    preferred_order = [
        "event_study_summary.csv",
        "mechanism_summary.csv",
        "retention_summary.csv",
        "did_summary.csv",
        "regression_coefficients.csv",
        "regression_models.csv",
        "match_diagnostics.csv",
        "rdd_summary.csv",
        "candidate_batch_audit.csv",
        "event_level_with_running.csv",
    ]
    ordered_files: list[Path] = []
    for filename in preferred_order:
        ordered_files.extend(path for path in csv_files if path.name == filename)
    ordered_files.extend(path for path in csv_files if path not in ordered_files)
    for path in ordered_files:
        key = path.stem
        if key == "rdd_status" or key in seen:
            continue
        seen.add(key)
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        tables.append((translate_label(key), render_table(frame)))
        if len(tables) >= max_tables:
            break
    return tables


def load_single_csv(output_dir: Path, filename: str) -> pd.DataFrame | None:
    path = next(output_dir.rglob(filename), None)
    if path is None:
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def read_csv_if_exists(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path, low_memory=False)


def rdd_output_dir(root: Path) -> Path:
    return root / "results" / "literature" / "hs300_rdd"


def results_manifest_path(root: Path) -> Path:
    real_manifest = root / "results" / "real_tables" / "results_manifest.csv"
    if real_manifest.exists():
        return real_manifest
    sample_manifest = root / "results" / "tables" / "results_manifest.csv"
    if sample_manifest.exists():
        return sample_manifest
    return real_manifest


def _display_path(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def _optional_int(value) -> int | None:
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _optional_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def load_rdd_status(
    root: Path,
    *,
    output_dir: Path | None = None,
    read_csv_if_exists: CsvFrameReader = read_csv_if_exists,
) -> RddStatus:
    return load_shared_rdd_status(root, output_dir=output_dir, read_csv_if_exists_fn=read_csv_if_exists)


def load_results_manifest(
    root: Path,
    *,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    return load_shared_results_manifest(manifest_path or results_manifest_path(root))


def build_rdd_contract_check(
    root: Path,
    *,
    rdd_status: RddStatus | None = None,
    output_dir: Path | None = None,
    manifest_path: Path | None = None,
    read_csv_if_exists: CsvFrameReader = read_csv_if_exists,
) -> RddContractCheck:
    live_status = (
        dict(rdd_status)
        if rdd_status is not None
        else load_rdd_status(
            root,
            output_dir=output_dir,
            read_csv_if_exists=read_csv_if_exists,
        )
    )
    resolved_manifest_path = manifest_path or results_manifest_path(root)
    manifest = load_shared_results_manifest(resolved_manifest_path)
    if not manifest:
        return {
            "manifest_exists": False,
            "manifest_path": _display_path(root, resolved_manifest_path),
            "manifest_profile": "",
            "matches": False,
            "mismatched_fields": [],
            "live_status": cast(RddStatus, live_status),
            "manifest": {},
        }

    field_map = {
        "mode": "rdd_mode",
        "evidence_tier": "rdd_evidence_tier",
        "evidence_status": "rdd_evidence_status",
        "source_kind": "rdd_source_kind",
        "source_label": "rdd_source_label",
        "source_file": "rdd_source_file",
        "coverage_note": "rdd_coverage_note",
        "candidate_rows": "rdd_candidate_rows",
        "candidate_batches": "rdd_candidate_batches",
        "treated_rows": "rdd_treated_rows",
        "control_rows": "rdd_control_rows",
        "crossing_batches": "rdd_crossing_batches",
    }
    mismatched_fields = [
        field
        for field, manifest_key in field_map.items()
        if live_status.get(field) != manifest.get(manifest_key)
    ]
    return {
        "manifest_exists": True,
        "manifest_path": _display_path(root, resolved_manifest_path),
        "manifest_profile": str(manifest.get("profile", "")),
        "matches": not mismatched_fields,
        "mismatched_fields": mismatched_fields,
        "live_status": cast(RddStatus, live_status),
        "manifest": manifest,
    }


def saved_output_dir_for_analysis(root: Path, analysis_id: str) -> Path | None:
    mapping = {
        "price_pressure_track": root / "results" / "literature" / "harris_gurel",
        "demand_curve_track": root / "results" / "literature" / "shleifer",
    }
    return mapping.get(analysis_id)


def load_identification_china_saved_result(
    root: Path,
    analyses: AnalysesConfig,
    *,
    load_rdd_status: RddStatusLoader,
    load_saved_tables: SavedTablesLoader,
    to_relative: RelativePathBuilder,
    build_figure_caption: FigureCaptionBuilder,
) -> TrackResult:
    style_dir = root / "results" / "literature" / "hs300_style"
    rdd_dir = rdd_output_dir(root)
    rdd_status = load_rdd_status()

    style_summary_path = style_dir / "summary.md"
    rdd_summary_path = rdd_dir / "summary.md"
    style_summary = style_summary_path.read_text(encoding="utf-8").strip() if style_summary_path.exists() else "暂无风格识别摘要。"
    rdd_summary = rdd_summary_path.read_text(encoding="utf-8").strip() if rdd_summary_path.exists() else rdd_status["message"]

    combined_tables: list[RenderedTable] = []
    for label, html_table in load_saved_tables(style_dir):
        combined_tables.append((f"风格识别：{label}", html_table))
    if rdd_status["mode"] in {"real", "reconstructed"}:
        for label, html_table in load_saved_tables(rdd_dir):
            combined_tables.append((f"断点回归：{label}", html_table))

    figure_paths: list[FigureEntry] = []
    for path in sorted(style_dir.rglob("*.png")):
        figure_paths.append(
            build_figure_entry(
                path,
                to_relative=to_relative,
                caption=build_figure_caption(path, prefix="风格识别"),
            )
        )
    if rdd_status["mode"] in {"real", "reconstructed"}:
        for path in sorted(rdd_dir.rglob("*.png")):
            figure_paths.append(
                build_figure_entry(
                    path,
                    to_relative=to_relative,
                    caption=build_figure_caption(path, prefix="断点回归"),
                )
            )

    summary_text = "\n\n".join(
        [
            "# 制度识别与中国市场证据结果包",
            "",
            "这个页面把中国市场识别主线下的风格识别结果与 RDD 状态放在同一页中，用于区分“现象是否存在”和“更强识别目前处于 L0-L3 的哪个层级”。",
            "",
            "## 第一部分：风格识别",
            style_summary,
            "",
            "## 第二部分：断点回归",
            rdd_summary,
        ]
    ).strip()

    config = analyses["identification_china_track"]
    return {
        "id": "identification_china_track",
        "title": config["title"],
        "description": config["description_zh"],
        "subtitle": config["subtitle"],
        "summary_text": summary_text,
        "rendered_tables": combined_tables,
        "figure_paths": figure_paths,
        "output_dir": to_relative(root / "results" / "literature"),
    }


def load_saved_track_result(
    root: Path,
    analysis_id: str,
    config: AnalysisDefinition,
    *,
    load_identification_china_saved_result: TrackResultLoader,
    attach_project_track_context: TrackContextAttacher,
    load_saved_tables: SavedTablesLoader,
    to_relative: RelativePathBuilder,
    build_figure_caption: FigureCaptionBuilder,
) -> TrackResult | None:
    if analysis_id == "identification_china_track":
        current = load_identification_china_saved_result()
        return attach_project_track_context(current, config)
    output_dir = saved_output_dir_for_analysis(root, analysis_id)
    if output_dir is None:
        return None
    summary_path = output_dir / "summary.md"
    if not summary_path.exists():
        return None
    current: TrackResult = {
        "id": config.get("project_module", analysis_id),
        "title": config["title"],
        "description": config["description_zh"],
        "subtitle": config["subtitle"],
        "summary_text": summary_path.read_text(encoding="utf-8"),
        "rendered_tables": load_saved_tables(output_dir),
        "figure_paths": [
            build_figure_entry(
                path,
                to_relative=to_relative,
                caption=build_figure_caption(path),
            )
            for path in sorted(output_dir.rglob("*.png"))
        ],
        "output_dir": to_relative(output_dir),
    }
    return attach_project_track_context(current, config)
