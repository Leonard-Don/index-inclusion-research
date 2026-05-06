from __future__ import annotations

import json

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.dashboard_section import (
    BRIEF_FIGURES,
    FULL_FIGURES,
    SECTION_ID,
    build_cross_market_section,
)


def _seed_tables(tables_dir):
    tables_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "market": m,
                "event_phase": p,
                "window_start": -1,
                "window_end": 1,
                "car_mean": 0.01,
                "car_se": 0.002,
                "car_t": 5.0,
                "p_value": 0.0,
                "n_events": 100,
            }
            for m in ("CN", "US")
            for p in ("announce", "effective")
        ]
    ).to_csv(tables_dir / "cma_window_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "metric": "announce_jump",
                "mean": 0.015,
                "median": 0.014,
                "se": 0.002,
                "t": 7.5,
                "p_value": 0.0,
                "n_events": 500,
            }
        ]
    ).to_csv(tables_dir / "cma_gap_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "hid": f"H{i}",
                "name_cn": f"n{i}",
                "mechanism": "m",
                "implications": "",
                "evidence_refs": "",
                "verdict_logic": "",
            }
            for i in range(1, 8)
        ]
    ).to_csv(tables_dir / "cma_hypothesis_map.csv", index=False)
    pd.DataFrame(
        [
            {
                "hid": f"H{i}",
                "name_cn": f"n{i}",
                "verdict": "部分支持" if i % 2 else "证据不足",
                "confidence": "中",
                "evidence_summary": f"evidence {i}",
                "metric_snapshot": f"metric {i}",
                "next_step": f"next {i}",
                "evidence_refs": "M1",
            }
            for i in range(1, 8)
        ]
    ).to_csv(tables_dir / "cma_hypothesis_verdicts.csv", index=False)


def _seed_figures(figures_dir, names):
    figures_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        (figures_dir / name).write_bytes(b"\x89PNG\r\n\x1a\n")


def test_section_has_expected_top_level_keys(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    _seed_figures(figures, FULL_FIGURES)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    assert section["id"] == SECTION_ID
    for key in (
        "title",
        "subtitle",
        "lead",
        "brief_summary",
        "conclusion_bullets",
        "quadrant_table",
        "gap_summary",
        "figures",
        "hypothesis_map",
        "hypothesis_verdicts",
        "evidence_coverage",
    ):
        assert key in section, f"missing key: {key}"


def test_section_quadrant_has_four_rows(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="demo"
    )
    assert len(section["quadrant_table"]["rows"]) == 4


def test_section_brief_mode_has_no_figures(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    _seed_figures(figures, FULL_FIGURES)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="brief"
    )
    assert section["figures"] == {}


def test_section_demo_mode_has_three_figures(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    _seed_figures(figures, BRIEF_FIGURES)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="demo"
    )
    assert len(section["figures"]) == 3


def test_section_full_mode_hypothesis_map_populated(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    assert len(section["hypothesis_map"]["rows"]) == 7
    assert len(section["hypothesis_verdicts"]["rows"]) == 7


def test_section_computes_hypothesis_verdicts_when_csv_missing(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    (tables / "cma_hypothesis_verdicts.csv").unlink()

    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )

    assert len(section["hypothesis_verdicts"]["rows"]) == 7
    assert section["hypothesis_verdicts"]["rows"][0]["hid"] == "H1"


def test_section_verdict_diff_unavailable_without_previous_csv(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    assert section["verdict_diff"]["available"] is False


def test_section_verdict_diff_reports_no_changes_when_previous_matches(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    # snapshot a copy of the seeded verdicts as the "previous" state
    current = pd.read_csv(tables / "cma_hypothesis_verdicts.csv")
    current.to_csv(tables / "cma_hypothesis_verdicts.previous.csv", index=False)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    diff = section["verdict_diff"]
    assert diff["available"] is True
    assert diff["changed_count"] == 0
    assert diff["unchanged_count"] == 7
    assert diff["changed_rows"] == []


def test_section_verdict_diff_surfaces_tier_flip(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    current = pd.read_csv(tables / "cma_hypothesis_verdicts.csv")
    previous = current.copy()
    # simulate that H1 used to be 证据不足 and is now whatever the seed has
    previous.loc[previous["hid"] == "H1", "verdict"] = "证据不足"
    previous.to_csv(tables / "cma_hypothesis_verdicts.previous.csv", index=False)
    # Force current H1 to a different verdict
    if current.loc[current["hid"] == "H1", "verdict"].iloc[0] == "证据不足":
        current.loc[current["hid"] == "H1", "verdict"] = "支持"
        current.to_csv(tables / "cma_hypothesis_verdicts.csv", index=False)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    diff = section["verdict_diff"]
    assert diff["available"] is True
    assert diff["changed_count"] >= 1
    h1_summary = next(r for r in diff["changed_rows"] if r["hid"] == "H1")
    assert "verdict" in h1_summary["summary"]


def test_section_verdict_diff_unavailable_in_brief_mode(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    current = pd.read_csv(tables / "cma_hypothesis_verdicts.csv")
    current.to_csv(tables / "cma_hypothesis_verdicts.previous.csv", index=False)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="brief"
    )
    assert section["verdict_diff"]["available"] is False


def test_section_demo_mode_exposes_hypothesis_verdict_cards(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="demo"
    )

    assert section["hypothesis_map"]["rows"] == []
    assert len(section["hypothesis_verdicts"]["rows"]) == 7


def test_section_missing_tables_yield_empty_rows(tmp_path):
    section = build_cross_market_section(
        tables_dir=tmp_path / "nonexistent",
        figures_dir=tmp_path / "nofigs",
        mode="full",
    )
    assert section["quadrant_table"]["rows"] == []
    assert section["gap_summary"]["rows"] == []
    assert section["hypothesis_map"]["rows"] == []
    assert section["hypothesis_verdicts"]["rows"] == []
    assert section["figures"] == {}
    assert section["detail_tables"] == {
        "window_summary_all": {"columns": [], "rows": []},
            "hypothesis_verdicts": {"columns": [], "rows": []},
            "mechanism_panel": {"columns": [], "rows": []},
            "h6_weight_explanation": {"columns": [], "rows": []},
            "h6_weight_robustness": {"columns": [], "rows": []},
            "h7_sector_interaction": {"columns": [], "rows": []},
        "heterogeneity_size": {"columns": [], "rows": []},
        "heterogeneity_liquidity": {"columns": [], "rows": []},
        "heterogeneity_sector": {"columns": [], "rows": []},
        "heterogeneity_gap_bucket": {"columns": [], "rows": []},
        "time_series_rolling": {"columns": [], "rows": []},
        "time_series_break": {"columns": [], "rows": []},
        "ar_path": {"columns": [], "rows": []},
        "car_path": {"columns": [], "rows": []},
    }


def test_section_full_mode_exposes_all_detail_tables(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    _seed_figures(figures, FULL_FIGURES)
    import pandas as pd

    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "outcome": "car_1_1",
                "spec": "no_fe",
                "coef": 0.01,
                "se": 0.002,
                "t": 5.0,
                "p_value": 0.0,
                "n_obs": 100,
                "r_squared": 0.1,
            }
        ]
    ).to_csv(tables / "cma_mechanism_panel.csv", index=False)
    for dim in ("size", "liquidity", "sector", "gap_bucket"):
        pd.DataFrame(
            [
                {
                    "market": "CN",
                    "dim": dim,
                    "bucket": "Q1",
                    "asymmetry_index": 0.5,
                    "n_events": 10,
                }
            ]
        ).to_csv(tables / f"cma_heterogeneity_{dim}.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "window_end_year": 2020,
                "car_mean": 0.01,
                "car_se": 0.002,
                "car_t": 5.0,
                "n_events": 100,
            }
        ]
    ).to_csv(tables / "cma_time_series_rolling.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "period": "pre",
                "car_mean": 0.01,
                "car_se": 0.002,
                "n_events": 50,
            }
        ]
    ).to_csv(tables / "cma_time_series_break.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "relative_day": 0,
                "n_events": 100,
                "ar_mean": 0.01,
                "ar_se": 0.002,
                "ar_t": 5.0,
            }
        ]
    ).to_csv(tables / "cma_ar_path.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "relative_day": 0,
                "n_events": 100,
                "car_mean": 0.01,
                "car_se": 0.002,
                "car_t": 5.0,
            }
        ]
    ).to_csv(tables / "cma_car_path.csv", index=False)
    pd.DataFrame(
        [
            {
                "test": "coverage",
                "status": "pass",
                "coefficient": None,
                "p_value": None,
                "n_obs": 12,
                "detail": "matched events=12",
            }
        ]
    ).to_csv(tables / "cma_h6_weight_robustness.csv", index=False)
    pd.DataFrame(
        [
            {
                "topic": "sample_coverage",
                "status": "pass",
                "headline": "12 个 CN 事件同时匹配 weight_proxy 与 announce_jump",
                "detail": "matched events=12",
                "metric": "matched_events",
                "value": 12,
            }
        ]
    ).to_csv(tables / "cma_h6_weight_explanation.csv", index=False)
    pd.DataFrame(
        [
            {
                "market": "US",
                "status": "pass",
                "signal": "support",
                "n_obs": 96,
                "sector_count": 3,
                "eligible_sectors": "Tech | Finance | Energy",
                "joint_p_value": 0.03,
                "top_term": "treatment_x_sector_Tech",
            }
        ]
    ).to_csv(tables / "cma_h7_sector_interaction.csv", index=False)

    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    detail = section["detail_tables"]
    for key in (
        "window_summary_all",
        "hypothesis_verdicts",
        "mechanism_panel",
        "h6_weight_explanation",
        "h6_weight_robustness",
        "h7_sector_interaction",
        "heterogeneity_size",
        "heterogeneity_liquidity",
        "heterogeneity_sector",
        "heterogeneity_gap_bucket",
        "time_series_rolling",
        "time_series_break",
        "ar_path",
        "car_path",
    ):
        assert key in detail, f"missing detail table: {key}"
        assert detail[key]["rows"], f"detail table {key} has no rows"


def test_section_exposes_evidence_coverage_from_manifest(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    (tables / "evidence_refresh_manifest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-02T00:00:00+00:00",
                "coverage": [
                    {
                        "item": "H6_weight_change",
                        "label": "H6 weight_change",
                        "status": "pass",
                        "value": "matched=12",
                        "detail": "fixture",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )

    assert section["evidence_coverage"]["available"] is True
    assert section["evidence_coverage"]["rows"][0]["item"] == "H6_weight_change"


def test_section_demo_mode_has_empty_detail_tables(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="demo"
    )
    assert section["detail_tables"] == {}


def test_section_gap_summary_empty_in_brief_mode(tmp_path):
    tables = tmp_path / "tables"
    figures = tmp_path / "figures"
    _seed_tables(tables)
    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="brief"
    )
    assert section["gap_summary"]["rows"] == []
    assert section["hypothesis_verdicts"]["rows"] == []
