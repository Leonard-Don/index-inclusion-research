from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pandas as pd

from index_inclusion_research import dashboard_formatting, verdict_summary

from . import hypotheses as cma_hypotheses
from . import verdicts as cma_verdicts

SectionMode = Literal["brief", "demo", "full"]

SECTION_ID = "cross_market_asymmetry"
SECTION_COPY: dict[str, object] = {
    "title": "美股对比 A 股：公告日至生效阶段的不对称集中度",
    "subtitle": "中国 对比 美国公告—生效阶段集中度",
    "lead": (
        "A 股更集中在公告日拉价、生效日放量；美股则在公告日价格和量能同时反应、生效日出现抽回。"
        "这就是跨市场不对称的核心现象。"
    ),
    "brief_summary": (
        "四象限（中国 A 股 / 美国 × 公告日 / 生效日）在 CAR 与微结构两条通道上"
        "呈现互补的集中度差异。"
    ),
    "conclusion_bullets": [
        "价格集中：公告日是两市场共同的 CAR 显著点；生效日 CAR 在两市场均未显著。",
        "量能集中：A 股在生效日出现换手率与成交量上行、波动率回落的需求签名；美股则反向抽回。",
        "异质性集中：不对称在小市值 / 低流动性分组更明显（参见 M4 矩阵）。",
    ],
}

BRIEF_FIGURES = (
    "cma_ar_path_comparison.png",
    "cma_gap_decomposition.png",
    "cma_mechanism_heatmap.png",
)

FULL_FIGURES = BRIEF_FIGURES + (
    "cma_heterogeneity_matrix_size.png",
    "cma_time_series_rolling.png",
    "cma_gap_length_distribution.png",
)


FIGURE_ECHART_IDS: dict[str, str] = {
    "cma_ar_path_comparison.png": "car_path",
    "cma_gap_decomposition.png": "gap_decomposition",
    "cma_heterogeneity_matrix_size.png": "heterogeneity_size",
    "cma_time_series_rolling.png": "time_series_rolling",
    "cma_mechanism_heatmap.png": "cma_mechanism_heatmap",
    "cma_gap_length_distribution.png": "cma_gap_length_distribution",
}

FIGURE_LABELS: dict[str, str] = {
    "cma_ar_path_comparison.png": "日度异常收益路径（中国 A 股 / 美国 × 公告日 / 生效日）",
    "cma_gap_decomposition.png": "公告日至生效日的分段拆解",
    "cma_mechanism_heatmap.png": "机制变量热力图",
    "cma_heterogeneity_matrix_size.png": "市值分组异质性矩阵",
    "cma_time_series_rolling.png": "五年滚动 CAR 时序",
    "cma_gap_length_distribution.png": "公告日至生效日间隔分布",
}


HET_DIMS: tuple[str, ...] = ("size", "liquidity", "sector", "gap_bucket")


def _safe_read(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def _frame_to_payload(frame: pd.DataFrame) -> dict[str, object]:
    rows = frame.to_dict(orient="records")
    payload: dict[str, object] = {
        "columns": list(frame.columns),
        "rows": rows,
    }
    if frame.columns.size:
        payload["column_labels"] = {
            column: dashboard_formatting.display_column_label(column) for column in frame.columns
        }
    if rows:
        payload["display_rows"] = [
            {
                column: dashboard_formatting.format_display_cell(value, column)
                for column, value in row.items()
            }
            for row in rows
        ]
    return payload


def _nested_verdict(row: Mapping[str, object], key: str) -> str:
    payload = row.get(key)
    if not isinstance(payload, Mapping):
        return ""
    return str(payload.get("verdict", ""))


def build_cross_market_section(
    *,
    tables_dir: Path,
    figures_dir: Path,
    mode: SectionMode = "full",
) -> dict[str, object]:
    """Build a dashboard-ready context dict for the CMA section.

    The returned dict is presenter-agnostic: a dashboard layer can render
    the fields through any template or route wiring. This function only
    depends on CSV and PNG artifacts produced by `run_cma_pipeline`.
    """

    tables_dir = Path(tables_dir)
    figures_dir = Path(figures_dir)

    window_summary = _safe_read(tables_dir / "cma_window_summary.csv")
    if not window_summary.empty:
        quadrant = window_summary.loc[
            (window_summary["window_start"] == -1) & (window_summary["window_end"] == 1),
            ["market", "event_phase", "car_mean", "car_t", "n_events"],
        ].reset_index(drop=True)
    else:
        quadrant = pd.DataFrame(
            columns=["market", "event_phase", "car_mean", "car_t", "n_events"]
        )

    gap_summary = _safe_read(tables_dir / "cma_gap_summary.csv")
    hypothesis_map = _safe_read(tables_dir / "cma_hypothesis_map.csv")
    hypothesis_verdicts = _safe_read(tables_dir / "cma_hypothesis_verdicts.csv")
    mechanism_panel = _safe_read(tables_dir / "cma_mechanism_panel.csv")
    heterogeneity: dict[str, pd.DataFrame] = {
        dim: _safe_read(tables_dir / f"cma_heterogeneity_{dim}.csv") for dim in HET_DIMS
    }
    time_series_rolling = _safe_read(tables_dir / "cma_time_series_rolling.csv")
    time_series_break = _safe_read(tables_dir / "cma_time_series_break.csv")
    h6_weight_robustness = _safe_read(tables_dir / "cma_h6_weight_robustness.csv")
    h6_weight_explanation = _safe_read(tables_dir / "cma_h6_weight_explanation.csv")
    h7_sector_interaction = _safe_read(tables_dir / "cma_h7_sector_interaction.csv")
    ar_path = _safe_read(tables_dir / "cma_ar_path.csv")
    car_path = _safe_read(tables_dir / "cma_car_path.csv")
    if hypothesis_verdicts.empty and any(
        not frame.empty
        for frame in (
            gap_summary,
            mechanism_panel,
            heterogeneity["size"],
            time_series_rolling,
        )
    ):
        hypothesis_verdicts = cma_verdicts.build_hypothesis_verdicts(
            gap_summary=gap_summary,
            mechanism_panel=mechanism_panel,
            heterogeneity_size=heterogeneity["size"],
            time_series_rolling=time_series_rolling,
        )

    figure_names: tuple[str, ...]
    if mode == "brief":
        figure_names = ()
    elif mode == "demo":
        figure_names = BRIEF_FIGURES
    else:
        figure_names = FULL_FIGURES

    figures = {
        FIGURE_LABELS.get(name, name): str(figures_dir / name)
        for name in figure_names
        if (figures_dir / name).exists()
    }
    figure_echart_ids = {
        FIGURE_LABELS.get(name, name): chart_id
        for name, chart_id in FIGURE_ECHART_IDS.items()
        if name in figure_names and (figures_dir / name).exists()
    }

    detail_tables: dict[str, dict[str, object]] = {}
    if mode == "full":
        detail_tables = {
            "window_summary_all": _frame_to_payload(window_summary),
            "hypothesis_verdicts": _frame_to_payload(hypothesis_verdicts),
            "mechanism_panel": _frame_to_payload(mechanism_panel),
            "h6_weight_robustness": _frame_to_payload(h6_weight_robustness),
            "h6_weight_explanation": _frame_to_payload(h6_weight_explanation),
            "h7_sector_interaction": _frame_to_payload(h7_sector_interaction),
            "time_series_rolling": _frame_to_payload(time_series_rolling),
            "time_series_break": _frame_to_payload(time_series_break),
            "ar_path": _frame_to_payload(ar_path),
            "car_path": _frame_to_payload(car_path),
        }
        for dim, frame in heterogeneity.items():
            detail_tables[f"heterogeneity_{dim}"] = _frame_to_payload(frame)

    return {
        "id": SECTION_ID,
        "mode": mode,
        "title": SECTION_COPY["title"],
        "subtitle": SECTION_COPY["subtitle"],
        "lead": SECTION_COPY["lead"],
        "brief_summary": SECTION_COPY["brief_summary"],
        "conclusion_bullets": SECTION_COPY["conclusion_bullets"],
        "quadrant_table": {
            "columns": ["market", "event_phase", "car_mean", "car_t", "n_events"],
            "rows": quadrant.to_dict(orient="records"),
        },
        "gap_summary": {
            "columns": list(gap_summary.columns),
            "rows": gap_summary.to_dict(orient="records") if mode != "brief" else [],
        },
        "figures": figures,
        "figure_echart_ids": figure_echart_ids,
        "hypothesis_map": {
            "columns": list(hypothesis_map.columns),
            "rows": hypothesis_map.to_dict(orient="records") if mode == "full" else [],
        },
        "hypothesis_verdicts": {
            "columns": list(hypothesis_verdicts.columns),
            "rows": hypothesis_verdicts.to_dict(orient="records") if mode != "brief" else [],
        },
        "track_summary": _build_track_summary_payload(hypothesis_verdicts, mode),
        "evidence_coverage": _build_evidence_coverage_payload(
            tables_dir=tables_dir,
            h6_weight_robustness=h6_weight_robustness,
            hypothesis_verdicts=hypothesis_verdicts,
            mode=mode,
        ),
        "verdict_diff": _build_verdict_diff_payload(
            hypothesis_verdicts, tables_dir=tables_dir, mode=mode
        ),
        "detail_tables": detail_tables,
    }


def _build_evidence_coverage_payload(
    *,
    tables_dir: Path,
    h6_weight_robustness: pd.DataFrame,
    hypothesis_verdicts: pd.DataFrame,
    mode: str,
) -> dict[str, object]:
    def _verdict_distribution_counts() -> tuple[int, int]:
        if hypothesis_verdicts.empty or "verdict" not in hypothesis_verdicts.columns:
            return 0, 0
        verdict_values = hypothesis_verdicts["verdict"].astype(str)
        support = int(verdict_values.isin(["支持", "部分支持"]).sum())
        insufficient = int(verdict_values.isin(["证据不足", "待补数据"]).sum())
        return support, insufficient

    def _verdict_distribution_text() -> str:
        support, insufficient = _verdict_distribution_counts()
        if not support and not insufficient:
            return "裁决行待生成"
        return f"{support} 项支持，{insufficient} 项证据不足"

    def _status_copy(status: object) -> str:
        status_text = str(status)
        if status_text == "pass":
            return "数据可用"
        if status_text in {"warn", "pending"}:
            return "证据待核验"
        if status_text in {"fail", "missing", "error"}:
            return "数据缺失"
        return dashboard_formatting.display_status_label(status)

    if mode == "brief":
        return {"available": False, "rows": []}
    manifest_path = tables_dir / "evidence_refresh_manifest.json"
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload_rows = payload.get("coverage", [])
            if isinstance(payload_rows, list) and payload_rows:
                rows = [
                    dict(row)
                    for row in payload_rows
                    if isinstance(row, Mapping)
                ]
                for row in rows:
                    if row.get("item") == "CMA_verdicts":
                        _, insufficient = _verdict_distribution_counts()
                        row["status"] = "warn" if insufficient else row.get("status", "pass")
                        row["status_label"] = "假说裁决"
                        row["value"] = _verdict_distribution_text()
                        row["detail"] = "H1-H7 的裁决分布，不代表所有假说都通过。"
                    else:
                        row["status_label"] = _status_copy(row.get("status", ""))
                return {
                    "available": True,
                    "generated_at": str(payload.get("generated_at", "")),
                    "rows": rows,
                }
        except (OSError, ValueError, TypeError):
            pass

    fallback_rows: list[dict[str, object]] = []
    if not h6_weight_robustness.empty and "test" in h6_weight_robustness.columns:
        coverage = h6_weight_robustness.loc[
            h6_weight_robustness["test"].astype(str) == "coverage"
        ]
        if not coverage.empty:
            coverage_row = coverage.iloc[0]
            fallback_rows.append(
                {
                    "item": "H6_weight_change",
                    "label": "H6 权重变化",
                    "status": str(coverage_row.get("status", "warn")),
                    "status_label": _status_copy(coverage_row.get("status", "warn")),
                    "value": f"匹配 {int(coverage_row.get('n_obs', 0) or 0)} 个事件",
                    "detail": str(coverage_row.get("detail", "")).replace("matched events=", "匹配事件="),
                }
            )
    if not hypothesis_verdicts.empty and "verdict" in hypothesis_verdicts.columns:
        _, insufficient = _verdict_distribution_counts()
        fallback_rows.append(
            {
                "item": "CMA_verdicts",
                "label": "CMA 假说裁决",
                "status": "warn" if insufficient else "pass",
                "status_label": "假说裁决已生成",
                "value": _verdict_distribution_text(),
                "detail": "这里统计的是 H1-H7 的假说裁决分布，不代表所有假说都通过。",
            }
        )
    return {"available": bool(fallback_rows), "rows": fallback_rows}


def _build_verdict_diff_payload(
    hypothesis_verdicts: pd.DataFrame,
    *,
    tables_dir: Path,
    mode: str,
) -> dict:
    """Compute a compact verdict diff between current and the orchestrator-
    saved ``cma_hypothesis_verdicts.previous.csv``.

    Returns ``{"available": False}`` when the previous snapshot is absent,
    when the current frame is empty, or in brief mode (where verdicts
    don't render). Otherwise returns ``{"available": True,
    "changed_count": N, "added_count": M, "removed_count": K,
    "unchanged_count": U, "changed_rows": [...], "added_rows": [...],
    "removed_rows": [...]}`` for the template to render as a banner.
    """
    if mode == "brief" or hypothesis_verdicts.empty:
        return {"available": False}
    previous_path = tables_dir / "cma_hypothesis_verdicts.previous.csv"
    if not previous_path.exists():
        return {"available": False}
    try:
        previous = pd.read_csv(previous_path)
    except (OSError, ValueError):
        return {"available": False}
    diff_rows = verdict_summary.compute_verdict_diff(hypothesis_verdicts, previous)
    changed = [r for r in diff_rows if r["kind"] == "changed"]
    added = [r for r in diff_rows if r["kind"] == "added"]
    removed = [r for r in diff_rows if r["kind"] == "removed"]
    unchanged = [r for r in diff_rows if r["kind"] == "unchanged"]
    # Flatten the changed-row payload for the template (avoids deep dict
    # navigation in Jinja). Each entry carries hid, name_cn, and a short
    # human-readable summary like "verdict: 证据不足→支持; key_value: 0.640→0.012".
    changed_summaries: list[dict[str, object]] = []
    for row in changed:
        hid = str(row["hid"])
        name = str(row.get("name_cn", "") or "")
        diff_chunks: list[str] = []
        changes = row.get("changes")
        if not isinstance(changes, Mapping):
            continue
        for field, beats in changes.items():
            if not isinstance(beats, Mapping):
                continue
            before = beats.get("before")
            after = beats.get("after")
            if field == "key_value":
                bef = "—" if isinstance(before, float) and (before != before) else f"{before:.3f}"
                aft = "—" if isinstance(after, float) and (after != after) else f"{after:.3f}"
                diff_chunks.append(f"{field}: {bef}→{aft}")
            else:
                diff_chunks.append(f"{field}: {before}→{after}")
        changed_summaries.append(
            {"hid": hid, "name_cn": name, "summary": "; ".join(diff_chunks)}
        )
    return {
        "available": True,
        "changed_count": len(changed),
        "added_count": len(added),
        "removed_count": len(removed),
        "unchanged_count": len(unchanged),
        "changed_rows": changed_summaries,
        "added_rows": [
            {"hid": str(r["hid"]), "verdict": _nested_verdict(r, "current")}
            for r in added
        ],
        "removed_rows": [
            {"hid": str(r["hid"]), "verdict": _nested_verdict(r, "previous")}
            for r in removed
        ],
    }


def _build_track_summary_payload(
    hypothesis_verdicts: pd.DataFrame,
    mode: str,
) -> dict:
    if hypothesis_verdicts.empty or mode == "brief":
        return {"rows": []}
    summary = cma_hypotheses.compute_track_verdict_summary(hypothesis_verdicts)
    if summary.empty:
        return {"rows": []}
    # Stable ordering matching the project's 3-track narrative
    track_order = {"price_pressure": 0, "demand_curve": 1, "identification": 2}
    summary = summary.sort_values(
        "track", key=lambda s: s.map(track_order).fillna(99)
    ).reset_index(drop=True)
    return {"rows": summary.to_dict(orient="records")}
