from __future__ import annotations

from index_inclusion_research.supplementary import (
    build_case_playbook_frame,
    build_event_clock_frame,
    build_impact_formula_frame,
    build_mechanism_chain_frame,
    build_supplementary_summary_markdown,
    estimate_impact_scenarios,
)


def test_supplementary_frames_are_populated() -> None:
    assert len(build_event_clock_frame()) == 5
    assert len(build_mechanism_chain_frame()) >= 4
    assert len(build_impact_formula_frame()) == 4
    assert len(build_case_playbook_frame()) >= 4


def test_impact_scenarios_have_expected_columns() -> None:
    scenarios = estimate_impact_scenarios()
    assert len(scenarios) >= 3
    assert "估计必须成交金额(美元)" in scenarios.columns
    assert "相对ADV占比" in scenarios.columns
    assert "平方根冲击估计(%)" in scenarios.columns


def test_supplement_summary_is_explicitly_a_supplement_layer() -> None:
    markdown = build_supplementary_summary_markdown()
    assert "投研汇报" in markdown
    assert "事件时钟" in markdown
