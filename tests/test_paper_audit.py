from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from index_inclusion_research import paper_audit


def _seed_audit_project(root: Path) -> None:
    (root / "docs").mkdir(parents=True)
    for name, text in {
        "research_delivery_package.md": "RDD preliminary，不进主表。",
        "paper_outline.md": "outline",
        "paper_outline_verdicts.md": "verdicts",
        "pre_registration.md": "pap",
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
        [{"evidence_tier": "L3", "candidate_rows": 356, "candidate_batches": 11}]
    ).to_csv(rdd / "rdd_status.csv", index=False)
    pd.DataFrame(
        [
            {"spec_kind": "main"},
            {"spec_kind": "donut"},
            {"spec_kind": "placebo"},
            {"spec_kind": "polynomial"},
        ]
    ).to_csv(rdd / "rdd_robustness.csv", index=False)
    pd.DataFrame([{"p_value": 0.68}]).to_csv(rdd / "mccrary_density_test.csv", index=False)

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
    (root / "paper" / "figures" / "cma_mechanism_heatmap.png").write_bytes(b"png")
    for name in ("rdd_status.csv", "rdd_robustness.csv", "mccrary_density_test.csv"):
        (root / "paper" / "rdd" / name).write_text("x\n", encoding="utf-8")
    for name in (
        "research_delivery_package.md",
        "pre_registration.md",
        "limitations.md",
        "verdict_iteration.md",
        "hs300_rdd_l3_collection_audit.md",
    ):
        (root / "paper" / "narrative" / name).write_text("x\n", encoding="utf-8")
    (root / "paper" / "data" / "pre-registration-2026-05-03.csv").write_text("x\n", encoding="utf-8")


def test_run_paper_audit_passes_seeded_project(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)

    results = paper_audit.run_paper_audit(tmp_path)

    assert {result.status for result in results} == {"pass"}
    assert paper_audit.summarize_audit(results) == {
        "pass": 6,
        "warn": 0,
        "fail": 0,
        "total": 6,
    }
    assert paper_audit.audit_exit_code(results, fail_on_warn=True) == 0


def test_paper_audit_flags_missing_bundle_copy(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    (tmp_path / "paper" / "tables" / "patell_bmp_summary.csv").unlink()

    result = paper_audit.audit_patell_bmp(tmp_path)

    assert result.status == "fail"
    assert any("patell_bmp_summary.csv" in detail for detail in result.details)


def test_source_only_audit_does_not_require_ignored_paper_dir(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    for path in (tmp_path / "paper").rglob("*"):
        if path.is_file():
            path.unlink()

    results = paper_audit.run_paper_audit(tmp_path, require_bundle=False)

    assert len(results) == 5
    assert {result.status for result in results} == {"pass"}


def test_render_audit_json_is_machine_readable(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    payload = json.loads(paper_audit.render_audit_json(paper_audit.run_paper_audit(tmp_path)))

    assert payload["summary"]["total"] == 6
    assert payload["checks"][0]["claim"]


def test_cma_core_audit_warns_when_core_tier_mapping_drifts(tmp_path: Path) -> None:
    _seed_audit_project(tmp_path)
    verdicts_path = tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    verdicts = pd.read_csv(verdicts_path)
    verdicts.loc[verdicts["hid"] == "H2", "evidence_tier"] = "core"
    verdicts.to_csv(verdicts_path, index=False)

    result = paper_audit.audit_cma_core(tmp_path, require_bundle=False)

    assert result.status == "warn"
    assert "Core hypothesis set" in result.message
    assert any("H2 · core" in detail for detail in result.details)


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
