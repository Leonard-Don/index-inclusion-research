from __future__ import annotations

import numpy as np
import pandas as pd

from index_inclusion_research.analysis.event_study import (
    compute_patell_bmp_summary,
)


def _make_panel(n_events: int = 30, *, seed: int = 0, shock: float = 0.03) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for event_id in range(n_events):
        for relative_day in range(-20, 21):
            base = rng.normal(scale=0.01)
            event_shock = shock if relative_day == 0 else 0.0
            rows.append(
                {
                    "event_id": f"e{event_id:03d}",
                    "event_phase": "announce",
                    "market": "TEST",
                    "inclusion": 1,
                    "relative_day": relative_day,
                    "ar": base + event_shock,
                }
            )
    return pd.DataFrame(rows)


EXPECTED_PATELL_BMP_COLUMNS = {
    "market",
    "event_phase",
    "inclusion",
    "window",
    "window_slug",
    "n_events",
    "patell_z",
    "patell_p",
    "bmp_t",
    "bmp_p",
    "mean_scar",
    "std_scar",
}


def assert_empty_patell_schema(summary: pd.DataFrame) -> None:
    assert summary.empty
    assert EXPECTED_PATELL_BMP_COLUMNS.issubset(summary.columns), (
        "empty patell summary must expose audit schema, got "
        f"{list(summary.columns)!r}"
    )


def test_patell_bmp_runs_and_has_expected_columns() -> None:
    panel = _make_panel()
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1), (-3, 3)],
        estimation_window=(-20, -2),
    )
    expected = {
        "market",
        "event_phase",
        "inclusion",
        "window",
        "n_events",
        "patell_z",
        "patell_p",
        "bmp_t",
        "bmp_p",
    }
    assert expected.issubset(summary.columns)
    assert (summary["n_events"] > 0).all()


def test_patell_detects_event_day_shock() -> None:
    panel = _make_panel(n_events=50, seed=42, shock=0.03)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    row = summary.iloc[0]
    assert row["patell_z"] > 3.0
    assert row["patell_p"] < 0.01
    assert row["bmp_t"] > 3.0


def test_patell_no_shock_returns_non_significant() -> None:
    panel = _make_panel(n_events=80, seed=1, shock=0.0)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    row = summary.iloc[0]
    assert abs(row["patell_z"]) < 2.5
    assert row["patell_p"] > 0.05


def test_patell_skips_events_with_zero_estimation_variance() -> None:
    rows: list[dict[str, object]] = []
    for event_id in range(5):
        for relative_day in range(-20, 21):
            rows.append(
                {
                    "event_id": f"const{event_id}",
                    "event_phase": "announce",
                    "market": "TEST",
                    "inclusion": 1,
                    "relative_day": relative_day,
                    "ar": 0.0,
                }
            )
    panel = pd.DataFrame(rows)
    summary = compute_patell_bmp_summary(
        panel,
        car_windows=[(-1, 1)],
        estimation_window=(-20, -2),
    )
    assert_empty_patell_schema(summary)


def test_patell_empty_panel_returns_empty_frame() -> None:
    summary = compute_patell_bmp_summary(
        pd.DataFrame(),
        car_windows=[(-1, 1)],
    )
    assert summary.empty


def test_patell_empty_panel_preserves_audit_schema() -> None:
    """An empty panel must still expose the Patell/BMP audit schema as columns.

    Why: ``run-event-study`` always writes ``patell_bmp_summary.csv`` via
    ``save_dataframe(compute_patell_bmp_summary(...), ...)``. When the input
    panel collapses to zero rows (e.g., the ``build_event_panel`` empty path
    after no event ticker matched a price row), the helper currently returns
    ``pd.DataFrame()`` with no columns, which ``to_csv`` writes as a bare
    newline and any downstream ``pd.read_csv`` (notably
    ``paper_audit.audit_patell_bmp``) then raises ``EmptyDataError`` on the
    empty file before the audit can even check the schema. Anchoring the
    empty path on the populated path's column set lets a "no events" panel
    and a populated panel be interchangeable as inputs to downstream
    auditors and dashboards — they get a real CSV with headers and zero
    rows instead of a degenerate file.
    """
    summary = compute_patell_bmp_summary(
        pd.DataFrame(),
        car_windows=[(-1, 1)],
    )
    assert_empty_patell_schema(summary)
