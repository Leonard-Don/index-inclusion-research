from __future__ import annotations

import os

from index_inclusion_research.results_snapshot import ResultsSnapshot, read_cached_csv


def test_read_cached_csv_returns_copied_frames_and_refreshes_on_file_change(tmp_path) -> None:
    csv_path = tmp_path / "event_counts.csv"
    csv_path.write_text("n_events\n12\n18\n", encoding="utf-8")

    first = read_cached_csv(csv_path)
    first.loc[0, "n_events"] = 999

    second = read_cached_csv(csv_path)
    assert second.loc[0, "n_events"] == 12

    stat = csv_path.stat()
    csv_path.write_text("n_events\n20\n30\n", encoding="utf-8")
    os.utime(csv_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))

    refreshed = read_cached_csv(csv_path)
    assert refreshed.loc[0, "n_events"] == 20


def test_results_snapshot_optional_csv_returns_empty_frame_for_missing_file(tmp_path) -> None:
    snapshot = ResultsSnapshot(tmp_path)

    frame = snapshot.optional_csv("results", "real_tables", "missing.csv")

    assert frame.empty
