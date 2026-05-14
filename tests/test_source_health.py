from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from index_inclusion_research.source_health import (
    FRESHNESS_FRESH,
    FRESHNESS_INVALID,
    FRESHNESS_MISSING,
    FRESHNESS_RECENT,
    FRESHNESS_STALE,
    FreshnessThresholds,
    audit_data_sources_csv,
    audit_source_paths,
    freshness_label,
)


def test_freshness_label_buckets() -> None:
    thresholds = FreshnessThresholds(fresh_max_days=2, recent_max_days=7)
    assert freshness_label(0.1, thresholds) == FRESHNESS_FRESH
    assert freshness_label(3.0, thresholds) == FRESHNESS_RECENT
    assert freshness_label(8.0, thresholds) == FRESHNESS_STALE
    assert freshness_label(None, thresholds) == FRESHNESS_MISSING
    assert freshness_label(-0.1, thresholds) == FRESHNESS_INVALID


def test_audit_source_paths_marks_existing_file_fresh_and_project_relative(tmp_path) -> None:
    root = tmp_path
    source = root / "data" / "raw" / "sample.csv"
    source.parent.mkdir(parents=True)
    source.write_text("id\n1\n", encoding="utf-8")
    now = datetime(2026, 5, 14, 12, tzinfo=UTC)
    mtime = now - timedelta(hours=3)
    os.utime(source, (mtime.timestamp(), mtime.timestamp()))

    rows = audit_source_paths(
        [{"label": "sample", "path": "data/raw/sample.csv", "category": "raw"}],
        root=root,
        now=now,
        thresholds=FreshnessThresholds(fresh_max_days=1, recent_max_days=7),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.label == "sample"
    assert row.path == "data/raw/sample.csv"
    assert str(root) not in row.path
    assert row.status == "ok"
    assert row.ok is True
    assert row.freshness == FRESHNESS_FRESH
    assert row.reason is None
    assert row.modified_at is not None and row.modified_at.endswith("Z")


def test_audit_source_paths_marks_missing_source() -> None:
    now = datetime(2026, 5, 14, tzinfo=UTC)

    row = audit_source_paths(
        [{"label": "missing", "path": "data/raw/missing.csv", "category": "raw"}],
        root=Path("/tmp/nonexistent-index-root"),
        now=now,
    )[0]

    assert row.status == "missing"
    assert row.ok is False
    assert row.freshness == FRESHNESS_MISSING
    assert row.reason == "file-not-found"


def test_audit_source_paths_marks_stale_and_future_mtime_inputs(tmp_path) -> None:
    root = tmp_path
    stale_file = root / "results" / "old.csv"
    future_file = root / "results" / "future.csv"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("x\n1\n", encoding="utf-8")
    future_file.write_text("x\n2\n", encoding="utf-8")
    now = datetime(2026, 5, 14, tzinfo=UTC)
    stale_mtime = now - timedelta(days=30)
    future_mtime = now + timedelta(days=1)
    os.utime(stale_file, (stale_mtime.timestamp(), stale_mtime.timestamp()))
    os.utime(future_file, (future_mtime.timestamp(), future_mtime.timestamp()))

    rows = audit_source_paths(
        [
            {"label": "old", "path": "results/old.csv"},
            {"label": "future", "path": "results/future.csv"},
            {"label": "invalid", "path": "   "},
        ],
        root=root,
        now=now,
        thresholds=FreshnessThresholds(fresh_max_days=2, recent_max_days=7),
    )

    stale, future, invalid = rows
    assert stale.status == "stale"
    assert stale.ok is False
    assert stale.freshness == FRESHNESS_STALE
    assert stale.reason == "stale-artifact"
    assert future.status == "invalid"
    assert future.ok is False
    assert future.freshness == FRESHNESS_INVALID
    assert future.reason == "future-mtime"
    assert invalid.status == "invalid"
    assert invalid.freshness == FRESHNESS_INVALID
    assert invalid.reason == "missing-path"


def test_audit_source_paths_avoids_absolute_home_leaks_for_out_of_tree_paths(tmp_path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    outside = tmp_path / "Users" / "leonardodon" / "secret-source.csv"
    outside.parent.mkdir(parents=True)
    outside.write_text("x\n1\n", encoding="utf-8")

    row = audit_source_paths(
        [{"label": "outside", "path": str(outside)}],
        root=root,
        now=datetime.now(UTC),
    )[0]

    assert row.path == "secret-source.csv"
    assert "leonardodon" not in row.path
    assert str(tmp_path) not in row.path


def test_audit_data_sources_csv_reads_chinese_manifest_columns(tmp_path) -> None:
    root = tmp_path
    data_file = root / "data" / "raw" / "real_prices.csv"
    data_file.parent.mkdir(parents=True)
    data_file.write_text("date,close\n2026-05-14,1\n", encoding="utf-8")
    manifest = root / "results" / "real_tables" / "data_sources.csv"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("\ufeff数据集,文件,来源\n日频价格,data/raw/real_prices.csv,Yahoo\n", encoding="utf-8")

    row = audit_data_sources_csv(
        manifest,
        root=root,
        now=datetime.now(UTC),
    )[0]

    assert row.label == "日频价格"
    assert row.path == "data/raw/real_prices.csv"
    assert row.category == "Yahoo"
    assert row.ok is True
    assert row.as_dict()["path"] == "data/raw/real_prices.csv"
