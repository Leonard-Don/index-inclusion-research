"""Tests for the H1..H7 verdict-evolution timeline helper.

The strategy is to stand up a tiny throwaway git repo in ``tmp_path``,
commit a few synthetic ``cma_hypothesis_verdicts.csv`` snapshots, and
let the production code walk *that* git log. No mocks of subprocess
itself — we exercise the real path so the schema contract (commit
SHA / date parsing / git-show behavior) stays validated.
"""

from __future__ import annotations

import struct
import subprocess
import textwrap
import zlib
from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research.outputs.verdict_timeline import (
    DEFAULT_MAX_HISTORY,
    EXPECTED_HIDS,
    PAP_BASELINE_DATE,
    build_verdict_timeline_from_git,
    count_verdict_changes,
    default_pap_baseline_date,
    render_verdict_timeline_plot,
    summarize_for_public_summary,
    total_verdict_changes,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _png_dimensions(png_path: Path) -> tuple[int, int]:
    """Return ``(width, height)`` from PNG IHDR — avoids a Pillow dep."""
    data = png_path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "not a PNG file"
    width, height = struct.unpack(">II", data[16:24])
    ihdr_crc_expected = struct.unpack(">I", data[29:33])[0]
    ihdr_crc_actual = zlib.crc32(data[12:29])
    assert ihdr_crc_expected == ihdr_crc_actual, "PNG IHDR CRC mismatch"
    return width, height


def _git(*args: str, cwd: Path) -> str:
    """Tiny wrapper that pins author/committer so commit hashes are stable."""
    env = {
        "GIT_AUTHOR_NAME": "Test",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test",
        "GIT_COMMITTER_EMAIL": "test@example.com",
        # Pin both timestamps so commit SHAs are deterministic.
        # Different commits override these via the call site.
        "GIT_AUTHOR_DATE": "2026-05-01T00:00:00",
        "GIT_COMMITTER_DATE": "2026-05-01T00:00:00",
    }
    completed = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        env={**env, "PATH": __import__("os").environ.get("PATH", "")},
        check=True,
    )
    return completed.stdout


def _make_repo(tmp_path: Path) -> Path:
    """Initialize an empty git repo for fixture commits."""
    repo = tmp_path / "fixture_repo"
    repo.mkdir()
    _git("init", "-q", "-b", "main", cwd=repo)
    return repo


def _write_csv_at_date(
    repo: Path,
    relpath: str,
    rows: list[dict[str, object]],
    *,
    date: str,
    message: str,
    nudge_marker: str | None = None,
) -> None:
    """Stage *rows* as a CSV, commit with the named date + message.

    Passing ``nudge_marker`` appends a comment row that doesn't change
    the verdict columns — useful for fixture commits that mean "refresh
    artifacts without changing the verdict text" (git refuses commits
    when the staged tree matches HEAD, so we need to perturb at least
    one column).
    """
    csv_path = repo / relpath
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if nudge_marker is not None:
        # Nudge a non-verdict column so the file blob differs but
        # downstream verdict counting still sees identical verdicts.
        df = df.copy()
        df["evidence_summary"] = df["evidence_summary"].astype(str) + nudge_marker
    df.to_csv(csv_path, index=False)
    import os

    env = {
        "GIT_AUTHOR_NAME": "Test",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test",
        "GIT_COMMITTER_EMAIL": "test@example.com",
        "GIT_AUTHOR_DATE": f"{date}T12:00:00+0800",
        "GIT_COMMITTER_DATE": f"{date}T12:00:00+0800",
        "PATH": os.environ.get("PATH", ""),
    }
    subprocess.run(
        ["git", "add", relpath], cwd=str(repo), env=env, check=True
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", message],
        cwd=str(repo),
        env=env,
        check=True,
    )


def _baseline_rows(verdicts: list[str]) -> list[dict[str, object]]:
    """Synthesize a 7-row verdicts CSV with the supplied verdicts."""
    assert len(verdicts) == 7
    rows: list[dict[str, object]] = []
    for hid, verdict in zip(EXPECTED_HIDS, verdicts, strict=True):
        rows.append(
            {
                "hid": hid,
                "name_cn": f"{hid}-fixture",
                "verdict": verdict,
                "confidence": "中",
                "evidence_summary": "fixture",
                "metric_snapshot": "n/a",
                "next_step": "n/a",
                "evidence_refs": "n/a",
                "p_value": 0.5,
                "key_label": "n/a",
                "key_value": 0.0,
                "n_obs": 100,
                "paper_ids": "fixture_2024",
                "paper_count": 1,
                "track": "identification",
                "evidence_tier": "core",
            }
        )
    return rows


@pytest.fixture
def fixture_repo(tmp_path: Path) -> Path:
    """Repo with 3 commits to ``results/real_tables/cma_hypothesis_verdicts.csv``.

    Commit 1 (2026-04-01): H2=证据不足 (initial)
    Commit 2 (2026-04-15): same verdicts (no change — exercises "held" path)
    Commit 3 (2026-05-01): H2=部分支持, H7=支持 (one H2 promotion change)
    """
    repo = _make_repo(tmp_path)
    relpath = "results/real_tables/cma_hypothesis_verdicts.csv"
    _write_csv_at_date(
        repo,
        relpath,
        _baseline_rows(
            ["证据不足", "证据不足", "支持", "证据不足", "支持", "证据不足", "证据不足"]
        ),
        date="2026-04-01",
        message="feat: initial verdicts",
    )
    _write_csv_at_date(
        repo,
        relpath,
        _baseline_rows(
            ["证据不足", "证据不足", "支持", "证据不足", "支持", "证据不足", "证据不足"]
        ),
        date="2026-04-15",
        message="chore: refresh artifacts (no verdict change)",
        nudge_marker="-refreshed",
    )
    _write_csv_at_date(
        repo,
        relpath,
        _baseline_rows(
            ["证据不足", "部分支持", "支持", "证据不足", "支持", "证据不足", "支持"]
        ),
        date="2026-05-01",
        message="feat(verdicts): promote H2 + H7 with new evidence",
    )
    return repo


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_verdict_timeline_returns_long_format(fixture_repo: Path) -> None:
    """Three commits × seven hypotheses = 21 rows with the expected columns."""
    timeline_df = build_verdict_timeline_from_git(fixture_repo)
    assert len(timeline_df) == 21
    assert set(timeline_df.columns) == {
        "commit_sha",
        "commit_date",
        "commit_msg_short",
        "hypothesis_id",
        "verdict",
        "verdict_category",
        "confidence",
        "evidence_tier",
        "n_obs",
    }
    # All seven hypotheses present in every commit.
    assert set(timeline_df["hypothesis_id"]) == set(EXPECTED_HIDS)
    # Chronologically ordered.
    assert (
        timeline_df["commit_date"].is_monotonic_increasing
    ), "rows must be sorted oldest -> newest"


def test_verdict_change_detection_counts_only_real_changes(
    fixture_repo: Path,
) -> None:
    """Commit-2 holds verdicts; commit-3 flips H2 + H7."""
    timeline_df = build_verdict_timeline_from_git(fixture_repo)
    counts = count_verdict_changes(timeline_df)
    # H2 evolves 证据不足 -> 证据不足 -> 部分支持: one change.
    assert counts["H2"] == 1
    # H7 evolves 证据不足 -> 证据不足 -> 支持: one change.
    assert counts["H7"] == 1
    # Everyone else held the same verdict across all 3 commits.
    for hid in ("H1", "H3", "H4", "H5", "H6"):
        assert counts[hid] == 0, f"{hid} should have zero changes"
    # Total = 2.
    assert total_verdict_changes(timeline_df) == 2


def test_render_verdict_timeline_emits_png_pdf_above_floor(
    fixture_repo: Path, tmp_path: Path
) -> None:
    """PNG must clear the 800x600 contract; PDF must be a non-empty vector."""
    timeline_df = build_verdict_timeline_from_git(fixture_repo)
    png_path = tmp_path / "verdict_timeline.png"
    pdf_path = tmp_path / "verdict_timeline.pdf"
    returned = render_verdict_timeline_plot(timeline_df, png_path, pdf_path)
    assert returned == png_path.resolve()
    assert png_path.exists() and png_path.stat().st_size > 5_000
    assert pdf_path.exists() and pdf_path.stat().st_size > 5_000
    width, height = _png_dimensions(png_path)
    assert width >= 800, f"PNG width {width} below the 800-pixel floor"
    assert height >= 600, f"PNG height {height} below the 600-pixel floor"


def test_pap_baseline_vertical_line_renders_without_error(
    fixture_repo: Path, tmp_path: Path
) -> None:
    """PAP baseline line annotation must not crash on a custom date."""
    timeline_df = build_verdict_timeline_from_git(fixture_repo)
    png_path = tmp_path / "verdict_timeline.png"
    # Use a custom PAP baseline date well inside the fixture range.
    render_verdict_timeline_plot(
        timeline_df,
        png_path,
        pap_baseline_date="2026-04-20",
    )
    # The PAP line is internal to matplotlib; we assert that the
    # figure renders to a valid PNG larger than the floor (the line
    # is drawn on the same axes).
    width, height = _png_dimensions(png_path)
    assert width >= 800
    assert height >= 600


def test_empty_history_returns_empty_dataframe(tmp_path: Path) -> None:
    """A git repo with no commits touching the CSV must yield an empty frame.

    The renderer's placeholder path is also exercised: the PNG should
    still satisfy the figure floor so the doctor / paper-bundle checks
    never fail-loud on an empty checkout.
    """
    repo = _make_repo(tmp_path)
    # Commit an unrelated file so the repo has a HEAD.
    (repo / "unrelated.txt").write_text("placeholder")
    subprocess.run(["git", "add", "unrelated.txt"], cwd=str(repo), check=True)
    subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=t@e.com",
         "commit", "-q", "-m", "unrelated"],
        cwd=str(repo),
        check=True,
    )

    timeline_df = build_verdict_timeline_from_git(repo)
    assert timeline_df.empty
    # Schema must still be intact so downstream consumers can iterate.
    assert "hypothesis_id" in timeline_df.columns
    assert "commit_date" in timeline_df.columns

    counts = count_verdict_changes(timeline_df)
    assert all(counts[h] == 0 for h in EXPECTED_HIDS)
    assert total_verdict_changes(timeline_df) == 0

    # Placeholder render path.
    png_path = tmp_path / "empty.png"
    render_verdict_timeline_plot(timeline_df, png_path)
    width, height = _png_dimensions(png_path)
    assert width >= 800
    assert height >= 600


def test_max_history_caps_commit_walking(fixture_repo: Path) -> None:
    """``max_history=1`` must keep only the most-recent commit's rows."""
    timeline_df = build_verdict_timeline_from_git(fixture_repo, max_history=1)
    assert timeline_df["commit_sha"].nunique() == 1
    # Only the latest commit (2026-05-01) should survive.
    assert (
        timeline_df["commit_date"].dt.strftime("%Y-%m-%d").unique().tolist()
        == ["2026-05-01"]
    )
    # With only one commit visible, there's nothing to diff against, so
    # the change count is necessarily zero.
    assert total_verdict_changes(timeline_df) == 0


def test_summarize_for_public_summary_contract(fixture_repo: Path) -> None:
    """The public-summary payload exposes the documented keys + values."""
    timeline_df = build_verdict_timeline_from_git(fixture_repo)
    summary = summarize_for_public_summary(timeline_df)
    assert summary["total_commits_tracked"] == 3
    assert summary["first_commit_date"] == "2026-04-01"
    assert summary["last_commit_date"] == "2026-05-01"
    assert summary["total_verdict_changes"] == 2
    counts = summary["verdict_changes_per_hypothesis"]
    assert isinstance(counts, dict)
    assert counts["H2"] == 1
    assert counts["H7"] == 1
    # Every expected hypothesis key is present (no KeyError downstream).
    for hid in EXPECTED_HIDS:
        assert hid in counts


def test_summarize_handles_empty_dataframe_gracefully() -> None:
    """Empty input must return zero counts and ``None`` for the date fields."""
    empty = pd.DataFrame(
        columns=[
            "commit_sha",
            "commit_date",
            "commit_msg_short",
            "hypothesis_id",
            "verdict",
            "verdict_category",
            "confidence",
            "evidence_tier",
            "n_obs",
        ]
    )
    empty["commit_date"] = pd.to_datetime(empty["commit_date"])
    summary = summarize_for_public_summary(empty)
    assert summary["total_commits_tracked"] == 0
    assert summary["first_commit_date"] is None
    assert summary["last_commit_date"] is None
    assert summary["total_verdict_changes"] == 0
    counts = summary["verdict_changes_per_hypothesis"]
    assert isinstance(counts, dict)
    assert all(counts[hid] == 0 for hid in EXPECTED_HIDS)


def test_default_max_history_constant() -> None:
    """Sanity: the documented default must stay ``50``.

    A regression that silently bumped this would mean ``index-inclusion-
    verdict-timeline`` walks more commits than the CLI banner advertises.
    """
    sentinel = textwrap.dedent("""\
        DEFAULT_MAX_HISTORY = 50
    """).strip()
    assert f"DEFAULT_MAX_HISTORY = {DEFAULT_MAX_HISTORY}" == sentinel


def test_default_pap_baseline_date_resolves_latest_snapshot(tmp_path: Path) -> None:
    """The baseline marker tracks the CURRENT (latest) snapshot, not a hardcoded date."""
    snaps = tmp_path / "snapshots"
    snaps.mkdir()
    for date in ("2026-05-03", "2026-05-16", "2026-05-31"):
        (snaps / f"pre-registration-{date}.csv").write_text("x\n", encoding="utf-8")

    assert default_pap_baseline_date(tmp_path) == "2026-05-31"


def test_default_pap_baseline_date_falls_back_without_snapshots(tmp_path: Path) -> None:
    """With no snapshots/ on disk, fall back to the module constant."""
    assert default_pap_baseline_date(tmp_path) == PAP_BASELINE_DATE
