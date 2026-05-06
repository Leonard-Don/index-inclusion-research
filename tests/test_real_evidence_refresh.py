from __future__ import annotations

import pandas as pd

from index_inclusion_research import real_evidence_refresh
from index_inclusion_research.doctor import CheckResult
from index_inclusion_research.real_evidence_refresh import (
    build_evidence_manifest,
    compute_cn_sector_coverage,
    run_refresh_pipeline,
)


def test_compute_cn_sector_coverage_deduplicates_events_and_metadata(tmp_path) -> None:
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {"market": "CN", "ticker": "1", "sector": ""},
            {"market": "CN", "ticker": "2", "sector": "Tech"},
        ]
    ).to_csv(raw / "real_events.csv", index=False)
    pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "sector": "Finance"},
            {"market": "US", "ticker": "AAPL", "sector": "Tech"},
        ]
    ).to_csv(raw / "real_metadata.csv", index=False)

    coverage = compute_cn_sector_coverage(tmp_path)

    assert coverage["total"] == 2
    assert coverage["known"] == 2
    assert coverage["rate"] == 1.0


def test_build_evidence_manifest_summarises_current_files(tmp_path) -> None:
    raw = tmp_path / "data" / "raw"
    processed = tmp_path / "data" / "processed"
    tables = tmp_path / "results" / "real_tables"
    regressions = tmp_path / "results" / "real_regressions"
    raw.mkdir(parents=True)
    processed.mkdir(parents=True)
    tables.mkdir(parents=True)
    regressions.mkdir(parents=True)
    pd.DataFrame(
        [
            {"market": "US", "year": 2019, "aum_trillion": 4.0},
            {"market": "US", "year": 2020, "aum_trillion": 5.0},
        ]
    ).to_csv(raw / "passive_aum.csv", index=False)
    pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "sector": "Finance"},
            {"market": "CN", "ticker": "000002", "sector": "Tech"},
        ]
    ).to_csv(raw / "real_events.csv", index=False)
    pd.DataFrame(
        [{"market": "CN", "ticker": "000001", "weight_proxy": 0.01}]
    ).to_csv(processed / "hs300_weight_change.csv", index=False)
    pd.DataFrame(
        [
            {
                "test": "coverage",
                "status": "pass",
                "n_obs": 1,
                "detail": "matched events=1",
            }
        ]
    ).to_csv(tables / "cma_h6_weight_robustness.csv", index=False)
    pd.DataFrame(
        [{"market": "CN", "ticker": "000001", "inclusion": 1}]
    ).to_csv(raw / "hs300_rdd_candidates.reconstructed.csv", index=False)
    pd.DataFrame(
        [
            {"hid": "H1", "verdict": "支持"},
            {"hid": "H2", "verdict": "证据不足"},
        ]
    ).to_csv(tables / "cma_hypothesis_verdicts.csv", index=False)
    pd.DataFrame(
        [
            {
                "spec_id": "announce_1to3",
                "over_threshold_covariates": 0,
                "max_abs_smd": 0.21,
                "is_default": True,
            }
        ]
    ).to_csv(regressions / "match_robustness_grid.csv", index=False)
    checks = [CheckResult("fixture", "pass", "ok")]

    manifest = build_evidence_manifest(
        root=tmp_path,
        tables_dir=tables,
        doctor_results=checks,
    )
    coverage = {row["item"]: row for row in manifest["coverage"]}

    assert coverage["H2_passive_aum"]["status"] == "warn"
    assert "CN comparable passive AUM missing" in coverage["H2_passive_aum"]["detail"]
    assert coverage["H6_weight_change"]["status"] == "pass"
    assert coverage["H7_cn_sector"]["status"] == "pass"
    assert coverage["RDD_L3_boundary"]["status"] == "warn"
    assert coverage["Match_robustness"]["status"] == "pass"
    assert coverage["doctor"]["value"] == "1 pass / 0 warn / 0 fail"


def test_build_evidence_manifest_passes_h2_when_cn_aum_is_present(tmp_path) -> None:
    raw = tmp_path / "data" / "raw"
    tables = tmp_path / "results" / "real_tables"
    regressions = tmp_path / "results" / "real_regressions"
    raw.mkdir(parents=True)
    tables.mkdir(parents=True)
    regressions.mkdir(parents=True)
    pd.DataFrame(
        [
            {"market": "US", "year": 2019, "aum_trillion": 4.0},
            {"market": "US", "year": 2020, "aum_trillion": 5.0},
            {"market": "CN", "year": 2019, "aum_trillion": 0.8},
            {"market": "CN", "year": 2020, "aum_trillion": 1.0},
        ]
    ).to_csv(raw / "passive_aum.csv", index=False)
    checks = [CheckResult("fixture", "pass", "ok")]

    manifest = build_evidence_manifest(
        root=tmp_path,
        tables_dir=tables,
        doctor_results=checks,
    )
    coverage = {row["item"]: row for row in manifest["coverage"]}

    assert coverage["H2_passive_aum"]["status"] == "pass"
    assert coverage["H2_passive_aum"]["value"] == "US 2 rows; CN 2 rows"


def test_run_refresh_pipeline_accepts_fake_step_runner_and_writes_manifest(
    tmp_path, monkeypatch
) -> None:
    tables = tmp_path / "tables"
    calls: list[str] = []

    def fake_runner(step):
        calls.append(step.slug)
        return 0

    monkeypatch.setattr(
        real_evidence_refresh.doctor,
        "run_all_checks",
        lambda: [CheckResult("fixture", "pass", "ok")],
    )

    result = run_refresh_pipeline(
        root=tmp_path,
        tables_dir=tables,
        manifest_json_path=tables / "manifest.json",
        manifest_csv_path=tables / "manifest.csv",
        only=["compute-h6-weight-change"],
        skip_sector_enrich=True,
        step_runner=fake_runner,
    )

    assert result["exit_code"] == 0
    assert calls == ["compute-h6-weight-change"]
    assert (tables / "manifest.json").exists()
    assert (tables / "manifest.csv").exists()
