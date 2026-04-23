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
