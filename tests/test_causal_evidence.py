from __future__ import annotations

import json

import pytest

from index_inclusion_research.analysis.causal_evidence import (
    CausalGraphSpec,
    PlaceboTest,
    RefutationReport,
    SensitivityResult,
    build_dashboard_payload,
)


def _graph() -> CausalGraphSpec:
    return CausalGraphSpec(
        treatment="hs300_inclusion",
        outcome="abnormal_return",
        confounders=("size", "liquidity"),
        assumptions=("rdd_continuity", "no_anticipation"),
        description="Index inclusion local RDD",
    )


def test_causal_graph_validates_roles_and_round_trips() -> None:
    graph = _graph()

    assert graph.variables() == (
        "hs300_inclusion",
        "abnormal_return",
        "size",
        "liquidity",
    )
    restored = CausalGraphSpec.from_dict(graph.as_dict())
    assert restored == graph
    assert restored.unknown_assumptions() == ()

    with pytest.raises(ValueError, match="treatment and outcome"):
        CausalGraphSpec(treatment="x", outcome="x")
    with pytest.raises(ValueError, match="cannot be both"):
        CausalGraphSpec(treatment="t", outcome="y", confounders=("z",), mediators=("z",))


def test_placebo_and_sensitivity_contracts_grade_conservatively() -> None:
    clean_placebo = PlaceboTest(
        name="future_outcome",
        kind="lagged_outcome",
        baseline_estimate=0.10,
        placebo_estimate=0.01,
        threshold=0.2,
    )
    failed_placebo = PlaceboTest(
        name="random_treatment",
        kind="random_treatment",
        baseline_estimate=0.10,
        placebo_estimate=0.08,
        threshold=0.2,
    )
    strong_sensitivity = SensitivityResult(
        name="e_value",
        kind="e_value",
        point_estimate=0.10,
        robustness_value=3.0,
        threshold=1.0,
    )
    weak_sensitivity = SensitivityResult(
        name="r2_bound",
        kind="r2_bound",
        point_estimate=0.10,
        robustness_value=0.4,
        threshold=1.0,
    )

    assert clean_placebo.passed is True
    assert clean_placebo.grade() == "A"
    assert failed_placebo.passed is False
    assert failed_placebo.grade() == "D"
    assert strong_sensitivity.passed is True
    assert strong_sensitivity.grade() == "A"
    assert weak_sensitivity.passed is False
    assert weak_sensitivity.grade() == "C"


def test_refutation_report_aggregates_grade_and_serializes() -> None:
    report = RefutationReport(
        graph=_graph(),
        hypothesis_id="H6",
        placebos=(
            PlaceboTest("future_outcome", "lagged_outcome", 0.10, 0.01, threshold=0.2),
        ),
        sensitivities=(
            SensitivityResult("e_value", "e_value", 0.10, 3.0, threshold=1.0),
        ),
        cluster_se_present=True,
        cluster_se_summary="event-clustered, 893 clusters",
        freshness="fresh",
    )

    payload = report.as_dict()
    restored = RefutationReport.from_dict(payload)
    assert restored.evidence_grade() == "A"
    assert payload["components"]["cluster_se_grade"] == "A"
    json.dumps(payload)


def test_failed_placebo_downgrades_dashboard_payload() -> None:
    report = RefutationReport(
        graph=_graph(),
        hypothesis_id="H7",
        placebos=(PlaceboTest("random_treatment", "random_treatment", 0.10, 0.08),),
        sensitivities=(SensitivityResult("r2_bound", "r2_bound", 0.10, 3.0, 1.0),),
        cluster_se_present=True,
        freshness="recent",
    )

    payload = build_dashboard_payload([report])
    row = payload["rows"][0]
    assert row["grade"] == "D"
    assert row["tone"] == "fail"
    assert row["failed_placebos"] == ["random_treatment"]
    assert payload["summary"]["overall_grade"] == "D"


def test_empty_report_is_unverified_not_accidentally_green() -> None:
    report = RefutationReport(graph=_graph(), hypothesis_id="empty")
    assert report.evidence_grade() == "F"
    assert build_dashboard_payload([report])["rows"][0]["grade"] == "F"
