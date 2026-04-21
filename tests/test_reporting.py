from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import figures_tables as export_script
from index_inclusion_research import research_report as report_script
from index_inclusion_research.research_report import build_report_text
from index_inclusion_research.result_contract import load_results_manifest


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_build_report_text_includes_key_sections() -> None:
    event_summary = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "window": "[-1,+1]",
                "window_slug": "m1_p1",
                "n_events": 3,
                "mean_car": 0.021,
                "std_car": 0.031,
                "t_stat": 2.0,
                "p_value": 0.08,
            },
            {
                "market": "US",
                "event_phase": "effective",
                "inclusion": 1,
                "window": "[-1,+1]",
                "window_slug": "m1_p1",
                "n_events": 3,
                "mean_car": 0.018,
                "std_car": 0.029,
                "t_stat": 1.8,
                "p_value": 0.09,
            },
        ]
    )
    regression_models = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "specification": "turnover_mechanism",
                "dependent_variable": "turnover_change",
                "n_obs": 12,
                "r_squared": 0.4,
                "adj_r_squared": 0.3,
            }
        ]
    )
    regression_coefficients = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "specification": "turnover_mechanism",
                "dependent_variable": "turnover_change",
                "parameter": "treatment_group",
                "coefficient": 0.12,
                "std_error": 0.04,
                "t_stat": 2.5,
                "p_value": 0.03,
            }
        ]
    )

    report = build_report_text(event_summary, regression_models, regression_coefficients)
    assert "事件研究主结论" in report
    assert "机制检验摘要" in report
    assert "公告日" in report
    assert "turnover_change" in report


def test_export_and_report_artifacts_share_structured_identification_contract(tmp_path: Path) -> None:
    events_path = tmp_path / "events.csv"
    event_summary_path = tmp_path / "event_summary.csv"
    regression_models_path = tmp_path / "regression_models.csv"
    regression_coefficients_path = tmp_path / "regression_coefficients.csv"
    rdd_output_dir = tmp_path / "hs300_rdd"
    tables_dir = tmp_path / "tables"
    manifest_path = tables_dir / "results_manifest.csv"
    report_path = tables_dir / "research_summary.md"

    _write_csv(
        events_path,
        [
            {
                "event_id": 1,
                "market": "CN",
                "index_name": "HS300",
                "ticker": "600000",
                "announce_date": "2024-01-02",
                "effective_date": "2024-01-05",
                "event_type": "addition",
                "inclusion": 1,
                "batch_id": "2024-01-05",
            }
        ],
    )
    _write_csv(
        event_summary_path,
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "window": "[-1,+1]",
                "window_slug": "m1_p1",
                "n_events": 1,
                "mean_car": 0.02,
                "std_car": 0.01,
                "t_stat": 2.1,
                "p_value": 0.04,
            }
        ],
    )
    _write_csv(
        regression_models_path,
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "specification": "turnover_mechanism",
                "dependent_variable": "turnover_change",
                "n_obs": 1,
                "r_squared": 0.3,
                "adj_r_squared": 0.2,
            }
        ],
    )
    _write_csv(
        regression_coefficients_path,
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "specification": "turnover_mechanism",
                "dependent_variable": "turnover_change",
                "parameter": "treatment_group",
                "coefficient": 0.12,
                "std_error": 0.04,
                "t_stat": 2.5,
                "p_value": 0.03,
            }
        ],
    )
    _write_csv(
        rdd_output_dir / "rdd_status.csv",
        [
            {
                "status": "reconstructed",
                "evidence_tier": "L2",
                "evidence_status": "公开重建样本",
                "source_kind": "reconstructed",
                "source_label": "公开重建候选样本文件",
                "source_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
                "coverage_note": "311 条候选；6 个批次；调入 6 / 对照 305。",
                "message": "当前正在使用公开数据重建的候选样本文件。",
                "note": "基于公开数据重建的边界样本，可进入公开数据版证据链。",
                "input_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
                "candidate_rows": 311,
                "candidate_batches": 6,
                "treated_rows": 6,
                "control_rows": 305,
                "crossing_batches": 6,
            }
        ],
    )

    export_script.main(
        [
            "--profile",
            "real",
            "--events",
            str(events_path),
            "--panel",
            str(tmp_path / "missing_panel.csv"),
            "--prices",
            str(tmp_path / "missing_prices.csv"),
            "--benchmarks",
            str(tmp_path / "missing_benchmarks.csv"),
            "--metadata",
            str(tmp_path / "missing_metadata.csv"),
            "--matched-panel",
            str(tmp_path / "missing_matched.csv"),
            "--average-paths",
            str(tmp_path / "missing_average_paths.csv"),
            "--event-summary",
            str(event_summary_path),
            "--regression-coefs",
            str(regression_coefficients_path),
            "--regression-models",
            str(regression_models_path),
            "--rdd-summary",
            str(tmp_path / "missing_rdd_summary.csv"),
            "--rdd-output-dir",
            str(rdd_output_dir),
            "--long-window-output-dir",
            str(tmp_path / "long_window"),
            "--figures-dir",
            str(tmp_path / "figures"),
            "--tables-dir",
            str(tables_dir),
            "--results-manifest",
            str(manifest_path),
        ]
    )
    report_script.main(
        [
            "--profile",
            "real",
            "--event-summary",
            str(event_summary_path),
            "--regression-models",
            str(regression_models_path),
            "--regression-coefficients",
            str(regression_coefficients_path),
            "--results-manifest",
            str(manifest_path),
            "--rdd-output-dir",
            str(rdd_output_dir),
            "--output",
            str(report_path),
        ]
    )

    manifest = load_results_manifest(manifest_path)

    assert manifest
    assert manifest["profile"] == "real"

    identification_scope = pd.read_csv(tables_dir / "identification_scope.csv")
    rdd_row = identification_scope.loc[identification_scope["分析层"] == "中国 RDD 扩展"].iloc[0]

    assert rdd_row["证据等级"] == manifest["rdd_evidence_tier"]
    assert rdd_row["证据状态"] == manifest["rdd_evidence_status"]
    assert rdd_row["来源摘要"] == manifest["rdd_source_label"]

    report_text = report_path.read_text(encoding="utf-8")

    assert "## 三、识别与证据状态" in report_text
    assert f"`{manifest['rdd_evidence_tier']}`" in report_text
    assert f"`{manifest['rdd_evidence_status']}`" in report_text
    assert f"`{manifest['rdd_source_label']}`" in report_text
    assert f"`{manifest['rdd_mode']}`" in report_text

    coverage_note = str(manifest["rdd_coverage_note"]).strip()
    if coverage_note:
        assert coverage_note in report_text
