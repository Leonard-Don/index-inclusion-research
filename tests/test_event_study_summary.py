from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.analysis.event_study import (
    summarize_event_level_metrics,
)
from index_inclusion_research.loaders import save_dataframe

EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS = {
    "market",
    "event_phase",
    "inclusion",
    "window",
    "window_slug",
    "n_events",
    "mean_car",
    "std_car",
    "se_car",
    "ci_low_95",
    "ci_high_95",
    "t_stat",
    "p_value",
}


def test_summarize_event_level_metrics_empty_input_round_trips_via_save_dataframe(
    tmp_path: Path,
) -> None:
    """Empty ``summarize_event_level_metrics`` must round-trip through CSV.

    Why: ``run_event_study.main`` writes the helper's output via
    ``save_dataframe(summary, output_dir / 'event_study_summary.csv')``.
    The helper currently returns a bare ``pd.DataFrame()`` with zero columns
    on the empty path (no rows in ``event_level`` or all rows filtered out by
    ``treatment_group == 1``), which ``DataFrame.to_csv`` writes as a single
    newline. ``pd.read_csv`` — used by ``paper_audit.audit_main_event_study``,
    the dashboard loaders, and the cross-market orchestrator — then raises
    ``EmptyDataError`` on that file before it can even check the audit
    schema. Anchoring the empty path on the populated-path column set lets
    a "no events" run and a populated run be interchangeable inputs to
    downstream consumers, mirroring the Patell/BMP empty-schema fix
    (commit ``69f01af``).
    """
    summary = summarize_event_level_metrics(pd.DataFrame())
    assert summary.empty
    assert EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS.issubset(summary.columns), (
        "empty event-study summary must expose audit schema, got "
        f"{list(summary.columns)!r}"
    )

    output_path = tmp_path / "event_study_summary.csv"
    save_dataframe(summary, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS.issubset(reloaded.columns)


def test_summarize_event_level_metrics_filtered_to_empty_preserves_schema() -> None:
    """When ``treatment_group == 1`` filtering empties the frame, schema holds.

    Why: ``compute_event_level_metrics`` defaults ``treatment_group`` to 1,
    but match-controls / robustness pipelines feed event_level frames that
    can be entirely controls (``treatment_group == 0``). The empty branch
    inside ``summarize_event_level_metrics`` previously discarded the audit
    schema in that case too, breaking the same downstream CSV consumers.
    """
    event_level = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "treatment_group": 0,
                "car_m1_p1": 0.01,
            }
        ]
    )
    summary = summarize_event_level_metrics(event_level, car_windows=[(-1, 1)])
    assert summary.empty
    assert EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS.issubset(summary.columns)


def test_summarize_event_level_metrics_no_matching_car_window_preserves_schema(
    tmp_path: Path,
) -> None:
    """Non-empty input with no requested CAR column is still a safe empty CSV."""
    event_level = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "car_m3_p3": 0.01,
            }
        ]
    )

    summary = summarize_event_level_metrics(event_level, car_windows=[(-1, 1)])
    assert summary.empty
    assert EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS.issubset(summary.columns)

    output_path = tmp_path / "event_study_summary.csv"
    save_dataframe(summary, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS.issubset(reloaded.columns)


def test_summarize_event_level_metrics_sample_filter_empty_schema_round_trips(
    tmp_path: Path,
) -> None:
    """Robustness summaries keep the sample_filter column even when empty."""
    summary = summarize_event_level_metrics(pd.DataFrame(), sample_filter="baseline")
    expected_columns = EXPECTED_EVENT_STUDY_SUMMARY_COLUMNS | {"sample_filter"}
    assert summary.empty
    assert expected_columns.issubset(summary.columns)

    output_path = tmp_path / "robustness_event_study_summary.csv"
    save_dataframe(summary, output_path)
    reloaded = pd.read_csv(output_path)
    assert reloaded.empty
    assert expected_columns.issubset(reloaded.columns)
