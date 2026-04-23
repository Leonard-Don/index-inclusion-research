from __future__ import annotations

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
            for i in range(1, 7)
        ]
    ).to_csv(tables_dir / "cma_hypothesis_map.csv", index=False)


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
    assert len(section["hypothesis_map"]["rows"]) == 6


def test_section_missing_tables_yield_empty_rows(tmp_path):
    section = build_cross_market_section(
        tables_dir=tmp_path / "nonexistent",
        figures_dir=tmp_path / "nofigs",
        mode="full",
    )
    assert section["quadrant_table"]["rows"] == []
    assert section["gap_summary"]["rows"] == []
    assert section["hypothesis_map"]["rows"] == []
    assert section["figures"] == {}
    assert section["detail_tables"] == {
        "window_summary_all": {"columns": [], "rows": []},
        "mechanism_panel": {"columns": [], "rows": []},
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

    section = build_cross_market_section(
        tables_dir=tables, figures_dir=figures, mode="full"
    )
    detail = section["detail_tables"]
    for key in (
        "window_summary_all",
        "mechanism_panel",
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
