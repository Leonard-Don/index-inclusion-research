from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from index_inclusion_research import paper_audit


def _write_seed_manifest(root: Path) -> None:
    artifacts: list[dict[str, object]] = []
    paper_root = root / "paper"
    for section in ("tables", "figures", "rdd", "narrative", "data"):
        for target in sorted((paper_root / section).rglob("*")):
            if not target.is_file():
                continue
            rel = target.relative_to(paper_root).as_posix()
            artifacts.append(
                {
                    "section": section,
                    "source": f"seed/{rel}",
                    "target": rel,
                    "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                    "size_bytes": target.stat().st_size,
                }
            )
    payload = {
        "bundle_label": "index-inclusion-paper-bundle",
        "manifest_schema_version": 1,
        "artifact_count": len(artifacts),
        "regenerated": {},
        "artifacts": artifacts,
    }
    _write_manifest_payload(root, payload)


def _write_manifest_payload(root: Path, payload: dict[str, object]) -> None:
    (root / "paper" / "manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _seed_audit_project(root: Path) -> None:
    (root / "docs").mkdir(parents=True)
    for name, text in {
        "research_delivery_package.md": "RDD preliminary，不进主表。",
        "paper_outline.md": "outline",
        "paper_outline_verdicts.md": "verdicts +4.01% 0.045 118",
        "analysis_parameters.md": "analysis parameters",
        "limitations.md": "limitations",
        "verdict_iteration.md": "iteration",
        "hs300_rdd_l3_collection_audit.md": "rdd audit",
    }.items():
        (root / "docs" / name).write_text(text, encoding="utf-8")

    (root / "results" / "real_tables").mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "window_slug": "m1_p1",
                "mean_car": 0.0175,
                "p_value": 0.0001,
            },
            {
                "market": "US",
                "event_phase": "announce",
                "inclusion": 1,
                "window_slug": "m1_p1",
                "mean_car": 0.0147,
                "p_value": 0.0001,
            },
        ]
    ).to_csv(root / "results" / "real_tables" / "event_study_summary.csv", index=False)
    (root / "results" / "real_tables" / "event_study_summary.tex").write_text("% tex\n", encoding="utf-8")
    pd.DataFrame(
        [
            {"hid": "H1", "verdict": "证据不足", "evidence_tier": "core"},
            {"hid": "H2", "verdict": "证据不足", "evidence_tier": "supplementary"},
            {"hid": "H3", "verdict": "支持", "evidence_tier": "supplementary"},
            {"hid": "H4", "verdict": "证据不足", "evidence_tier": "supplementary"},
            {"hid": "H5", "verdict": "支持", "evidence_tier": "core"},
            {"hid": "H6", "verdict": "证据不足", "evidence_tier": "supplementary"},
            {"hid": "H7", "verdict": "支持", "evidence_tier": "core"},
        ]
    ).to_csv(root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv", index=False)
    (root / "results" / "real_tables" / "cma_hypothesis_verdicts.tex").write_text("% tex\n", encoding="utf-8")

    (root / "results" / "real_event_study").mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "window_slug": "m1_p1",
                "patell_p": 0.001,
                "bmp_p": 0.002,
            },
            {
                "market": "US",
                "event_phase": "announce",
                "inclusion": 1,
                "window_slug": "m1_p1",
                "patell_p": 0.001,
                "bmp_p": 0.02,
            },
        ]
    ).to_csv(root / "results" / "real_event_study" / "patell_bmp_summary.csv", index=False)

    rdd = root / "results" / "literature" / "hs300_rdd"
    rdd.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "status": "real",
                "evidence_tier": "L3",
                "evidence_status": "正式边界样本",
                "source_kind": "official",
                "source_label": "正式候选样本文件",
                "source_file": "data/raw/hs300_rdd_candidates.csv",
                "generated_at": "2026-05-20T21:12:12+08:00",
                "candidate_rows": 356,
                "candidate_batches": 11,
                "treated_rows": 191,
                "control_rows": 165,
                "crossing_batches": 11,
            }
        ]
    ).to_csv(rdd / "rdd_status.csv", index=False)
    pd.DataFrame(
        [
            {"spec_kind": "main", "tau": 0.0401, "p_value": 0.045, "n_obs": 118},
            {"spec_kind": "donut", "tau": 0.0512, "p_value": 0.094, "n_obs": 100},
            {"spec_kind": "placebo", "tau": -0.0213, "p_value": 0.165, "n_obs": 128},
            {"spec_kind": "polynomial", "tau": 0.0025, "p_value": 0.946, "n_obs": 118},
        ]
    ).to_csv(rdd / "rdd_robustness.csv", index=False)
    pd.DataFrame([{"p_value": 0.68}]).to_csv(rdd / "mccrary_density_test.csv", index=False)
    pd.DataFrame(
        [
            {
                "profile": "real",
                "rdd_mode": "real",
                "rdd_evidence_tier": "L3",
                "rdd_evidence_status": "正式边界样本",
                "rdd_source_kind": "official",
                "rdd_source_label": "正式候选样本文件",
                "rdd_source_file": "data/raw/hs300_rdd_candidates.csv",
                "rdd_generated_at": "2026-05-20T21:12:12+08:00",
                "rdd_candidate_rows": 356,
                "rdd_candidate_batches": 11,
                "rdd_treated_rows": 191,
                "rdd_control_rows": 165,
                "rdd_crossing_batches": 11,
            }
        ]
    ).to_csv(root / "results" / "real_tables" / "results_manifest.csv", index=False)

    source_figures = (
        root / "results" / "figures",
        root / "results" / "literature" / "hs300_rdd" / "figures",
    )
    for directory in source_figures:
        directory.mkdir(parents=True, exist_ok=True)
    for name in (
        "cma_verdicts_forest.png",
        "cma_verdicts_forest.pdf",
        "cma_verdicts_sensitivity.png",
        "cma_verdicts_sensitivity.pdf",
        "cma_verdicts_ar_engine.png",
        "cma_verdicts_ar_engine.pdf",
        "cma_verdicts_2d_robustness.png",
        "cma_verdicts_2d_robustness.pdf",
        "hs300_rdd_robustness_forest.png",
        "hs300_rdd_robustness_forest.pdf",
    ):
        (root / "results" / "figures" / name).write_bytes(b"fig")
    for name in (
        "car_m1_p1_rdd_main.png",
        "car_m1_p1_rdd_bins.png",
        "car_m3_p3_rdd_bins.png",
        "turnover_change_rdd_bins.png",
        "volume_change_rdd_bins.png",
        "l3_coverage_timeline.png",
        "rdd_robustness_forest.png",
    ):
        (root / "results" / "literature" / "hs300_rdd" / "figures" / name).write_bytes(b"fig")

    (root / "snapshots").mkdir()
    pd.DataFrame(
        [
            {"hid": "H1", "verdict": "证据不足"},
            {"hid": "H2", "verdict": "证据不足"},
            {"hid": "H3", "verdict": "支持"},
            {"hid": "H4", "verdict": "证据不足"},
            {"hid": "H5", "verdict": "支持"},
            {"hid": "H6", "verdict": "证据不足"},
            {"hid": "H7", "verdict": "支持"},
        ]
    ).to_csv(root / "snapshots" / "pre-registration-2026-05-03.csv", index=False)

    for subdir in ("tables", "figures", "rdd", "narrative", "data"):
        (root / "paper" / subdir).mkdir(parents=True, exist_ok=True)
    for name in ("README.md", "bundle_summary.md"):
        (root / "paper" / name).write_text("paper\n", encoding="utf-8")
    for name in ("event_study_summary.tex", "cma_hypothesis_verdicts.tex", "patell_bmp_summary.csv"):
        (root / "paper" / "tables" / name).write_text("x\n", encoding="utf-8")
    for name in (
        "cma_mechanism_heatmap.png",
        "cma_verdicts_2d_robustness.png",
        "cma_verdicts_2d_robustness.pdf",
    ):
        (root / "paper" / "figures" / name).write_bytes(b"png")
    for name in ("rdd_status.csv", "rdd_robustness.csv", "mccrary_density_test.csv"):
        (root / "paper" / "rdd" / name).write_text("x\n", encoding="utf-8")
    for name in (
        "research_delivery_package.md",
        "analysis_parameters.md",
        "limitations.md",
        "verdict_iteration.md",
        "hs300_rdd_l3_collection_audit.md",
    ):
        (root / "paper" / "narrative" / name).write_text("x\n", encoding="utf-8")
    (root / "paper" / "data" / "pre-registration-2026-05-03.csv").write_text("x\n", encoding="utf-8")
    _write_seed_manifest(root)


def test_run_paper_audit_passes_seeded_project(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)

    results = paper_audit.run_paper_audit(tmp_path)

    assert {result.status for result in results} == {"pass"}
    assert paper_audit.summarize_audit(results) == {
        "pass": 7,
        "warn": 0,
        "fail": 0,
        "total": 7,
    }
    assert paper_audit.audit_exit_code(results, fail_on_warn=True) == 0


def test_paper_audit_flags_missing_bundle_copy(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    (tmp_path / "paper" / "tables" / "patell_bmp_summary.csv").unlink()

    result = paper_audit.audit_patell_bmp(tmp_path)

    assert result.status == "fail"
    assert any("patell_bmp_summary.csv" in detail for detail in result.details)


def test_paper_bundle_audit_fails_when_manifest_hashes_drift(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    (tmp_path / "paper" / "tables" / "patell_bmp_summary.csv").write_text(
        "changed after manifest\n", encoding="utf-8"
    )

    result = paper_audit.audit_paper_bundle(tmp_path)

    assert result.status == "fail"
    assert "manifest" in result.message
    assert any(
        "tables/patell_bmp_summary.csv" in detail and "sha256" in detail
        for detail in result.details
    )


def test_paper_bundle_audit_fails_when_manifest_omits_cross_audit_target(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "paper" / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = [
        entry
        for entry in payload["artifacts"]
        if entry["target"] != "rdd/mccrary_density_test.csv"
    ]
    payload["artifacts"] = artifacts
    payload["artifact_count"] = len(artifacts)
    _write_manifest_payload(tmp_path, payload)

    result = paper_audit.audit_paper_bundle(tmp_path)

    assert result.status == "fail"
    assert any(
        "rdd/mccrary_density_test.csv" in detail and "missing from manifest" in detail
        for detail in result.details
    )


def test_paper_bundle_audit_fails_when_manifest_target_escapes_paper_root(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "paper" / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["artifacts"][0]["target"] = "../outside.csv"
    _write_manifest_payload(tmp_path, payload)

    result = paper_audit.audit_paper_bundle(tmp_path)

    assert result.status == "fail"
    assert any("../outside.csv" in detail and "under paper/" in detail for detail in result.details)


def test_paper_bundle_audit_fails_when_manifest_repeats_target(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "paper" / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["artifacts"].append(dict(payload["artifacts"][0]))
    payload["artifact_count"] = len(payload["artifacts"])
    _write_manifest_payload(tmp_path, payload)

    result = paper_audit.audit_paper_bundle(tmp_path)

    assert result.status == "fail"
    duplicated_target = str(payload["artifacts"][0]["target"])
    assert any(
        "duplicate manifest target" in detail and duplicated_target in detail
        for detail in result.details
    )


def test_paper_bundle_audit_fails_when_manifest_size_drifts(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "paper" / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    for entry in payload["artifacts"]:
        if entry["target"] == "tables/patell_bmp_summary.csv":
            entry["size_bytes"] += 1
            break
    _write_manifest_payload(tmp_path, payload)

    result = paper_audit.audit_paper_bundle(tmp_path)

    assert result.status == "fail"
    assert any(
        "tables/patell_bmp_summary.csv" in detail and "size_bytes" in detail
        for detail in result.details
    )


def test_source_only_audit_does_not_require_ignored_paper_dir(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    for path in (tmp_path / "paper").rglob("*"):
        if path.is_file():
            path.unlink()

    results = paper_audit.run_paper_audit(tmp_path, require_bundle=False)

    assert len(results) == 6
    assert {result.status for result in results} == {"pass"}


def test_render_audit_json_is_machine_readable(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    payload = json.loads(paper_audit.render_audit_json(paper_audit.run_paper_audit(tmp_path)))

    assert payload["summary"]["total"] == 7
    assert payload["checks"][0]["claim"]


def test_cma_core_audit_warns_when_core_tier_mapping_drifts(tmp_path: Path) -> None:
    """A core-set drift to a hypothesis OUTSIDE the promotion floor must warn.

    H2 is intentionally promotion-eligible
    (``EVIDENCE_TIER_PROMOTION_FLOOR["H2"]``) so its promotion to core is
    a legitimate proxy-driven path, not a drift. We pick H6 here because
    no promotion is wired for it, so promoting it should still be
    flagged as drift from the frozen PAP.
    """
    _seed_audit_project(tmp_path)
    verdicts_path = tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    verdicts = pd.read_csv(verdicts_path)
    verdicts.loc[verdicts["hid"] == "H6", "evidence_tier"] = "core"
    verdicts.to_csv(verdicts_path, index=False)

    result = paper_audit.audit_cma_core(tmp_path, require_bundle=False)

    assert result.status == "warn"
    assert "Core hypothesis set" in result.message
    assert any("H6 · core" in detail for detail in result.details)


def test_pap_limitations_audit_fails_when_pre_registration_snapshot_missing(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    (tmp_path / "snapshots" / "pre-registration-2026-05-03.csv").unlink()

    result = paper_audit.audit_pap_limitations(tmp_path, require_bundle=False)

    assert result.status == "fail"
    assert any("pre-registration-2026-05-03.csv" in detail for detail in result.details)


def test_rdd_appendix_audit_warns_when_preliminary_wording_dropped(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    (tmp_path / "docs" / "research_delivery_package.md").write_text(
        "RDD: paper-grade main claim.", encoding="utf-8"
    )

    result = paper_audit.audit_rdd_appendix(tmp_path, require_bundle=False)

    assert result.status == "warn"
    assert "preliminary" in result.message


def test_reference_manifest_audit_flags_missing_rdd_dashboard_figure(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    (
        tmp_path
        / "results"
        / "literature"
        / "hs300_rdd"
        / "figures"
        / "car_m1_p1_rdd_main.png"
    ).unlink()

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "fail"
    assert "car_m1_p1_rdd_main.png" in "\n".join(result.details)


def test_reference_manifest_audit_accepts_equivalent_numeric_counts(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    manifest["rdd_candidate_rows"] = 356.0
    manifest["rdd_candidate_batches"] = 11.0
    manifest.to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_matches_manifest_row_by_rdd_mode(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    stale = manifest.iloc[0].copy()
    stale["profile"] = "sample"
    stale["rdd_mode"] = "demo"
    stale["rdd_evidence_tier"] = "L1"
    stale["rdd_candidate_rows"] = 3
    stale["rdd_candidate_batches"] = 1
    pd.concat([stale.to_frame().T, manifest], ignore_index=True).to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_trims_rdd_mode_before_matching_manifest_row(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    stale = manifest.iloc[0].copy()
    stale["profile"] = "sample"
    stale["rdd_mode"] = "demo"
    stale["rdd_evidence_tier"] = "L1"
    stale["rdd_candidate_rows"] = 3
    stale["rdd_candidate_batches"] = 1
    pd.concat([stale.to_frame().T, manifest], ignore_index=True).to_csv(manifest_path, index=False)

    status_path = tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    status = pd.read_csv(status_path)
    status["status"] = " real "
    status.to_csv(status_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_trims_manifest_rdd_mode_before_matching_row(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    stale = manifest.iloc[0].copy()
    stale["profile"] = "sample"
    stale["rdd_mode"] = "demo"
    stale["rdd_evidence_tier"] = "L1"
    stale["rdd_candidate_rows"] = 3
    stale["rdd_candidate_batches"] = 1
    manifest["rdd_mode"] = " real "
    pd.concat([stale.to_frame().T, manifest], ignore_index=True).to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_accepts_case_equivalent_rdd_mode(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    stale = manifest.iloc[0].copy()
    stale["profile"] = "sample"
    stale["rdd_mode"] = "demo"
    stale["rdd_evidence_tier"] = "L1"
    stale["rdd_candidate_rows"] = 3
    stale["rdd_candidate_batches"] = 1
    pd.concat([stale.to_frame().T, manifest], ignore_index=True).to_csv(manifest_path, index=False)

    status_path = tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    status = pd.read_csv(status_path)
    status["status"] = "REAL"
    status.to_csv(status_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_flags_duplicate_rdd_mode_row_drift(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    stale_duplicate = manifest.iloc[0].copy()
    stale_duplicate["profile"] = "real_stale_duplicate"
    stale_duplicate["rdd_evidence_tier"] = "L2"
    stale_duplicate["rdd_candidate_rows"] = 355
    pd.concat([manifest, stale_duplicate.to_frame().T], ignore_index=True).to_csv(
        manifest_path,
        index=False,
    )

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "fail"
    assert any("matching manifest row 2" in detail for detail in result.details)
    assert any("evidence_tier != rdd_evidence_tier" in detail for detail in result.details)


def test_reference_manifest_audit_flags_extended_rdd_count_drift(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    manifest["rdd_treated_rows"] = 190
    manifest.to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "fail"
    assert any("treated_rows != rdd_treated_rows" in detail for detail in result.details)


def test_reference_manifest_audit_flags_rdd_audit_file_reference_drift(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    manifest["rdd_audit_file"] = "results/literature/hs300_rdd/stale_candidate_audit.csv"
    manifest.to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "fail"
    assert any("audit_file != rdd_audit_file" in detail for detail in result.details)


def test_reference_manifest_audit_normalizes_repo_relative_rdd_paths(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    status_path = tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    status = pd.read_csv(status_path)
    status["input_file"] = "data/raw/hs300_rdd_candidates.csv"
    status["audit_file"] = "results/literature/hs300_rdd/candidate_batch_audit.csv"
    status.to_csv(status_path, index=False)

    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    manifest["rdd_source_file"] = "./data/raw/hs300_rdd_candidates.csv"
    manifest["rdd_input_file"] = r"data\raw\hs300_rdd_candidates.csv"
    manifest["rdd_audit_file"] = r".\results\literature\hs300_rdd\candidate_batch_audit.csv"
    manifest.to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_normalizes_repeated_path_separators(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    status_path = tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    status = pd.read_csv(status_path)
    status["input_file"] = "data/raw/hs300_rdd_candidates.csv"
    status["audit_file"] = "results/literature/hs300_rdd/candidate_batch_audit.csv"
    status.to_csv(status_path, index=False)

    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    manifest["rdd_source_file"] = "data//raw/hs300_rdd_candidates.csv"
    manifest["rdd_input_file"] = "data/raw//hs300_rdd_candidates.csv"
    manifest["rdd_audit_file"] = "results//literature/hs300_rdd//candidate_batch_audit.csv"
    manifest.to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"


def test_reference_manifest_audit_normalizes_current_directory_path_segments(
    tmp_path: Path,
) -> None:
    _seed_audit_project(tmp_path)
    status_path = tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_status.csv"
    status = pd.read_csv(status_path)
    status["input_file"] = "data/raw/hs300_rdd_candidates.csv"
    status["audit_file"] = "results/literature/hs300_rdd/candidate_batch_audit.csv"
    status.to_csv(status_path, index=False)

    manifest_path = tmp_path / "results" / "real_tables" / "results_manifest.csv"
    manifest = pd.read_csv(manifest_path)
    manifest["rdd_source_file"] = "./data/./raw/hs300_rdd_candidates.csv"
    manifest["rdd_input_file"] = "data/./raw/hs300_rdd_candidates.csv"
    manifest["rdd_audit_file"] = "results/literature/./hs300_rdd/candidate_batch_audit.csv"
    manifest.to_csv(manifest_path, index=False)

    result = paper_audit.audit_reference_manifest(tmp_path, require_bundle=False)

    assert result.status == "pass"
