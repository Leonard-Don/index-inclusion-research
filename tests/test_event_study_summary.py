from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

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


def _correlated_same_date_event_level() -> pd.DataFrame:
    """Event-level CARs where same-date events are positively correlated.

    Three calendar dates each host four events. Within a date, every event's
    CAR is the date's shared shock plus a small idiosyncratic wobble, so the
    within-cluster correlation is strongly positive. Clustering on
    ``event_date`` must therefore widen the standard error relative to the
    iid estimate, which treats every event as independent.
    """
    date_shocks = {
        "2021-01-04": 0.030,
        "2021-02-01": -0.010,
        "2021-03-01": 0.045,
    }
    wobble = [0.001, -0.001, 0.0008, -0.0008]
    rows: list[dict[str, object]] = []
    event_id = 0
    for date, shock in date_shocks.items():
        for offset in wobble:
            event_id += 1
            rows.append(
                {
                    "event_id": event_id,
                    "market": "CN",
                    "event_phase": "announce",
                    "inclusion": 1,
                    "treatment_group": 1,
                    "event_date": pd.Timestamp(date),
                    "car_m1_p1": shock + offset,
                }
            )
    return pd.DataFrame(rows)


def test_summarize_event_level_metrics_exposes_clustered_se_columns() -> None:
    """The summary must additively expose date-clustered SE columns.

    (a) ``se_car_clustered`` / ``p_value_clustered`` exist on every row;
    (b) the existing iid ``se_car`` / ``t_stat`` / ``p_value`` are untouched.
    """
    event_level = _correlated_same_date_event_level()
    summary = summarize_event_level_metrics(event_level, car_windows=[(-1, 1)])

    assert "se_car_clustered" in summary.columns
    assert "p_value_clustered" in summary.columns

    # Existing iid statistics remain primary and unchanged.
    iid = _summarise_iid_reference(event_level["car_m1_p1"])
    row = summary.iloc[0]
    assert row["se_car"] == pytest.approx(iid["se_car"])
    assert row["t_stat"] == pytest.approx(iid["t_stat"])
    assert row["p_value"] == pytest.approx(iid["p_value"])


def _summarise_iid_reference(values: pd.Series) -> dict[str, float]:
    from scipy import stats

    clean = values.dropna().astype(float)
    n = int(clean.count())
    se = clean.std(ddof=1) / np.sqrt(n)
    t_stat, p_value = stats.ttest_1samp(clean, popmean=0.0, nan_policy="omit")
    return {"se_car": se, "t_stat": float(t_stat), "p_value": float(p_value)}


def test_clustered_se_widens_under_positive_within_date_correlation() -> None:
    """Clustering on event date inflates the SE vs iid for correlated data.

    Computed via statsmodels ``cov_type="cluster"`` (a core dependency), so
    this runs unconditionally — no optional methods extra required.
    """
    event_level = _correlated_same_date_event_level()
    summary = summarize_event_level_metrics(event_level, car_windows=[(-1, 1)])
    row = summary.iloc[0]

    assert np.isfinite(row["se_car_clustered"])
    assert np.isfinite(row["p_value_clustered"])
    # Positive within-cluster correlation => clustered SE >= iid SE.
    assert row["se_car_clustered"] >= row["se_car"]


def test_clustered_se_nan_when_single_date_cluster() -> None:
    """A cell with only one distinct event date cannot be clustered → NaN."""
    event_level = _correlated_same_date_event_level()
    event_level = event_level.assign(event_date=pd.Timestamp("2021-01-04"))
    summary = summarize_event_level_metrics(event_level, car_windows=[(-1, 1)])
    row = summary.iloc[0]

    assert "se_car_clustered" in summary.columns
    assert np.isnan(row["se_car_clustered"])
    assert np.isnan(row["p_value_clustered"])
