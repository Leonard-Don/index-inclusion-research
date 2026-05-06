from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from index_inclusion_research.evidence_drilldown import build_evidence_detail


def _write_manifest(root: Path, *items: str) -> None:
    tables = root / "results" / "real_tables"
    tables.mkdir(parents=True)
    coverage = [
        {
            "item": item,
            "label": item,
            "status": "pass",
            "value": "fixture",
            "detail": "fixture detail",
        }
        for item in items
    ]
    (tables / "evidence_refresh_manifest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-02T00:00:00+00:00",
                "coverage": coverage,
                "doctor": {"checks": [{"name": "demo", "status": "pass"}]},
            }
        ),
        encoding="utf-8",
    )


def test_h2_evidence_detail_reads_passive_aum(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "H2_passive_aum")
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {"market": "US", "year": 2024, "aum_trillion": 8.0},
            {"market": "US", "year": 2025, "aum_trillion": 9.0},
        ]
    ).to_csv(raw / "passive_aum.csv", index=False)

    detail = build_evidence_detail("H2_passive_aum", root=tmp_path)

    assert detail is not None
    assert detail["label"] == "H2_passive_aum"
    assert detail["tables"][0]["key"] == "aum_market_summary"
    assert detail["tables"][0]["rows"][0]["market"] == "US"
    assert detail["tables"][0]["column_labels"]["latest_aum_trillion"] == "最新 AUM（万亿美元）"


def test_h6_evidence_detail_exposes_joined_rows_and_explanation(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "H6_weight_change")
    processed = tmp_path / "data" / "processed"
    tables = tmp_path / "results" / "real_tables"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "1",
                "announce_date": "2024-01-01",
                "weight_proxy": 0.02,
            }
        ]
    ).to_csv(processed / "hs300_weight_change.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "000001",
                "announce_date": "2024-01-01",
                "announce_jump": 0.01,
                "sector": "Bank",
            }
        ]
    ).to_csv(tables / "cma_gap_event_level.csv", index=False)
    pd.DataFrame(
        [{"test": "coverage", "status": "pass", "n_obs": 1, "detail": "matched events=1"}]
    ).to_csv(tables / "cma_h6_weight_robustness.csv", index=False)
    pd.DataFrame(
        [{"topic": "sample_coverage", "status": "warn", "headline": "1 match"}]
    ).to_csv(tables / "cma_h6_weight_explanation.csv", index=False)
    pd.DataFrame([{"hid": "H6", "verdict": "证据不足"}]).to_csv(
        tables / "cma_hypothesis_verdicts.csv",
        index=False,
    )

    detail = build_evidence_detail("H6_weight_change", root=tmp_path)
    tables_by_key = {table["key"]: table for table in detail["tables"]}

    assert tables_by_key["matched_weight_events"]["total_rows"] == 1
    assert tables_by_key["h6_weight_explanation"]["rows"][0]["topic"] == "sample_coverage"
    assert tables_by_key["h6_verdict"]["rows"][0]["hid"] == "H6"


def test_h7_evidence_detail_includes_sector_interaction(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "H7_cn_sector")
    raw = tmp_path / "data" / "raw"
    tables = tmp_path / "results" / "real_tables"
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {"market": "CN", "ticker": "000001", "sector": "Tech"},
            {"market": "CN", "ticker": "000002", "sector": "Finance"},
        ]
    ).to_csv(raw / "real_events.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "US",
                "status": "pass",
                "signal": "support",
                "n_obs": 1882,
                "joint_p_value": 0.094,
                "top_term": "effective_x_sector_Industrials",
            }
        ]
    ).to_csv(tables / "cma_h7_sector_interaction.csv", index=False)

    detail = build_evidence_detail("H7_cn_sector", root=tmp_path)
    tables_by_key = {table["key"]: table for table in detail["tables"]}

    assert detail is not None
    assert detail["summary_cards"][1]["label"] == "行业交互回归"
    assert detail["summary_cards"][1]["detail"] == "支持信号"
    assert tables_by_key["h7_sector_interaction"]["rows"][0]["market"] == "US"


def test_rdd_evidence_detail_surfaces_live_status(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "RDD_L3_boundary")
    status_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    status_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "status": "reconstructed",
                "evidence_tier": "L2",
                "evidence_status": "公开重建样本",
                "source_file": "data/raw/hs300_rdd_candidates.reconstructed.csv",
                "message": "fixture",
            }
        ]
    ).to_csv(status_dir / "rdd_status.csv", index=False)

    detail = build_evidence_detail("RDD_L3_boundary", root=tmp_path)

    assert detail is not None
    assert detail["summary_cards"][0]["value"] == "L2"
    assert detail["tables"][0]["key"] == "rdd_status"


def test_match_robustness_evidence_detail_surfaces_grid_and_balance(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "Match_robustness")
    regressions = tmp_path / "results" / "real_regressions"
    processed = tmp_path / "data" / "processed"
    raw = tmp_path / "data" / "raw"
    regressions.mkdir(parents=True)
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "spec_id": "announce_1to3",
                "reference_date_column": "announce_date",
                "control_ratio": 3,
                "over_threshold_covariates": 1,
                "max_abs_smd": 0.31,
                "is_default": True,
            },
            {
                "spec_id": "effective_1to2",
                "reference_date_column": "effective_date",
                "control_ratio": 2,
                "over_threshold_covariates": 0,
                "max_abs_smd": 0.19,
                "is_default": False,
            },
        ]
    ).to_csv(regressions / "match_robustness_grid.csv", index=False)
    pd.DataFrame(
        [
            {
                "spec_id": "effective_1to2",
                "market": "CN",
                "covariate": "mkt_cap_log",
                "smd": 0.19,
            }
        ]
    ).to_csv(regressions / "match_robustness_balance.csv", index=False)
    pd.DataFrame(
        [{"market": "CN", "covariate": "mkt_cap_log", "smd": 0.31}]
    ).to_csv(regressions / "match_balance.csv", index=False)
    (processed / "real_matched_events.csv").write_text("market,ticker\nCN,000001\n")
    (raw / "real_prices.csv").write_text("market,ticker,date\nCN,000001,2024-01-01\n")
    (regressions / "match_robustness_summary.md").write_text("local-only\n")

    detail = build_evidence_detail("Match_robustness", root=tmp_path)

    assert detail is not None
    tables_by_key = {table["key"]: table for table in detail["tables"]}
    assert detail["summary_cards"][0]["value"] == "effective_1to2"
    assert tables_by_key["match_robustness_grid"]["total_rows"] == 2
    assert tables_by_key["match_robustness_balance"]["rows"][0]["spec_id"] == "effective_1to2"


def test_unknown_or_unlisted_evidence_item_returns_none(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "H2_passive_aum")

    assert build_evidence_detail("missing_item", root=tmp_path) is None
