"""Tests for ``index_inclusion_research.submission_ready``.

The submission-ready gate aggregates ~17 small checks, several of which
re-call other heavyweight gates (``paper_integrity``, ``doctor``). To
keep the tests fast and isolated we monkeypatch ``DEFAULT_CHECKS`` to a
restricted subset, mirroring the convention in ``test_paper_integrity``.

We also use one "real-data" smoke test that runs the full gate against
the real repo, asserting only that it produces a sensible
``SubmissionAssessment`` shape — keeping the suite cheap while still
catching API drift between the gate and the actual artifact tree.
"""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

import pytest

from index_inclusion_research import submission_ready

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _png_bytes(width: int = 1200, height: int = 800) -> bytes:
    """Generate the smallest valid PNG with a given declared width/height.

    We only need the IHDR chunk to parse — the dimension checker reads
    the first 24 bytes after the PNG signature.
    """
    sig = b"\x89PNG\r\n\x1a\n"
    ihdr_data = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    crc = zlib.crc32(b"IHDR" + ihdr_data)
    ihdr_chunk = struct.pack(">I", 13) + b"IHDR" + ihdr_data + struct.pack(">I", crc)
    # Trivial IDAT/IEND for completeness, otherwise the file is a few KB
    # of "real PNG" which more closely matches a generated figure.
    iend = struct.pack(">I", 0) + b"IEND" + struct.pack(">I", zlib.crc32(b"IEND"))
    payload = b"\x00" * 4096  # pad past the 200B size threshold
    return sig + ihdr_chunk + payload + iend


def _make_skeleton_text(*, sections_complete: bool = True, todos: int = 0) -> str:
    headers = [
        "## 1. 引言",
        "## 2. 文献综述",
        "## 3. 研究设计",
        "## 4. 实证结果",
        "## 5. 限制与讨论",
        "## 6. 结论与启示",
        "## 7. 分析参数",
        "## 参考文献",
    ]
    if not sections_complete:
        headers = headers[:-1]  # drop the last one to fabricate a fail
    body = "\n".join(f"{h}\n\nprose for {h}.\n" for h in headers)
    todo_block = "\n".join(f"[TODO: write paragraph {i}]" for i in range(todos))
    if todos:
        body = body + "\n" + todo_block
    return "# Paper\n\n" + body + "\n"


def _make_methodology_text() -> str:
    return "# Methodology\n\nMatches verdicts CSV.\n"


def _make_references_bib(n: int = 16) -> str:
    entries = "\n\n".join(
        f"@article{{paper_{i},\n  title={{Title {i}}},\n  year={{202{i % 10}}},\n}}"
        for i in range(n)
    )
    return entries + "\n"


def _make_public_summary() -> dict:
    return {
        "schema_version": 1,
        "pap_deviation_summary": {
            "all_unchanged": True,
            "unchanged_count": 7,
            "flipped_count": 0,
            "tightened_count": 0,
            "weakened_count": 0,
            "unverifiable_count": 0,
        },
    }


def _seed_minimal_paper_tree(tmp_path: Path) -> Path:
    """Write enough fixtures that the *file-system* checks pass.

    Heavy checks (``paper_integrity_passes`` / ``doctor_strict``) are
    monkeypatched away in the per-test ``DEFAULT_CHECKS`` slicing so we
    don't need to fabricate every cross-document artifact here.
    """
    paper = tmp_path / "paper"
    paper.mkdir()
    (paper / "skeleton.md").write_text(_make_skeleton_text(), encoding="utf-8")
    (paper / "methodology_summary.md").write_text(
        _make_methodology_text(), encoding="utf-8"
    )
    (paper / "manuscript.tex").write_text(
        "\\documentclass{article}\\begin{document}x\\end{document}\n",
        encoding="utf-8",
    )
    (paper / "references.bib").write_text(_make_references_bib(), encoding="utf-8")

    figures_dir = tmp_path / "results" / "figures"
    figures_dir.mkdir(parents=True)
    for relpath in submission_ready.EXPECTED_FIGURE_RELPATHS:
        fpath = tmp_path / relpath
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_bytes(_png_bytes())

    # Verdicts CSV + public summary + raw CSVs.
    real_tables = tmp_path / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    (real_tables / "cma_hypothesis_verdicts.csv").write_text(
        "hid,n_obs\nH1,436\n", encoding="utf-8"
    )

    public = tmp_path / "data" / "public"
    public.mkdir(parents=True)
    (public / "index_research_summary.json").write_text(
        json.dumps(_make_public_summary()), encoding="utf-8"
    )

    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "real_events.csv").write_text(
        "market,ticker,announce_date,effective_date\nCN,000001,2020-01-01,2020-01-15\n",
        encoding="utf-8",
    )
    (raw / "real_prices.csv").write_text(
        "market,ticker,date,close\nCN,000001,2020-01-01,10.0\n",
        encoding="utf-8",
    )
    (raw / "real_benchmarks.csv").write_text(
        "market,date,benchmark_ret\nCN,2020-01-01,0.001\n", encoding="utf-8"
    )

    return tmp_path


# Restricted check set: only the file-system checks. The heavy gates
# (paper_integrity / doctor) are exercised by the real-data smoke test.
_FS_CHECKS = (
    submission_ready.check_skeleton_exists,
    submission_ready.check_paper_sections_present,
    submission_ready.check_prose_todo_markers,
    submission_ready.check_methodology_summary_present,
    submission_ready.check_figures_complete,
    submission_ready.check_tex_artifacts,
    submission_ready.check_references_bib_populated,
    submission_ready.check_pap_all_unchanged,
    submission_ready.check_public_summary_fresh,
    submission_ready.check_data_csv_schemas,
    submission_ready.check_literature_catalog,
    submission_ready.check_sensitivity_artifacts_fresh,
    submission_ready.check_verdict_timeline_fresh,
)


@pytest.fixture
def consistent_project(tmp_path: Path, monkeypatch) -> Path:
    _seed_minimal_paper_tree(tmp_path)
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_all_fs_checks_pass_on_consistent_tree(consistent_project: Path) -> None:
    """A fabricated mutually-consistent tree exits the FS check set cleanly."""
    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    assert assessment.blocker_count == 0, [
        (c.name, c.status, c.description) for c in assessment.checks if c.status == "fail"
    ]
    assert assessment.warning_count == 0, [
        (c.name, c.status, c.description) for c in assessment.checks if c.status == "warn"
    ]
    assert assessment.overall_status == "ready"
    assert assessment.pass_count == len(_FS_CHECKS)
    assert submission_ready.submission_exit_code(assessment) == 0
    assert submission_ready.submission_exit_code(assessment, fail_on_warn=True) == 0


def test_missing_skeleton_is_fail(consistent_project: Path) -> None:
    """Removing skeleton.md trips the structure block with a hard fail."""
    (consistent_project / "paper" / "skeleton.md").unlink()
    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    fails = {c.name for c in assessment.checks if c.status == "fail"}
    assert "skeleton_exists" in fails
    assert assessment.overall_status == "not_ready"
    assert submission_ready.submission_exit_code(assessment) == 2


def test_todos_present_yields_warn(consistent_project: Path) -> None:
    """TODO markers in skeleton flip ``prose_todo_markers`` to warn."""
    text = _make_skeleton_text(todos=4)
    (consistent_project / "paper" / "skeleton.md").write_text(text, encoding="utf-8")
    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    warns = {c.name: c for c in assessment.checks if c.status == "warn"}
    assert "prose_todo_markers" in warns
    assert "4 [TODO" in warns["prose_todo_markers"].description
    assert assessment.overall_status == "partially_ready"
    assert submission_ready.submission_exit_code(assessment) == 1


def test_stale_methodology_warns(consistent_project: Path) -> None:
    """Methodology summary older than the skeleton triggers a warn."""
    # Backdate methodology by 7 days, then ``touch`` the skeleton fresh.
    import os
    import time

    meth = consistent_project / "paper" / "methodology_summary.md"
    skel = consistent_project / "paper" / "skeleton.md"
    seven_days_ago = time.time() - 7 * 86400
    os.utime(meth, (seven_days_ago, seven_days_ago))
    # Touch skeleton to "now" to force the methodology to look stale.
    now = time.time()
    os.utime(skel, (now, now))

    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    warns = {c.name for c in assessment.checks if c.status == "warn"}
    assert "methodology_summary_present" in warns
    assert assessment.overall_status == "partially_ready"


def test_missing_figures_is_fail(consistent_project: Path) -> None:
    """Removing 2 of the 9 expected figures triggers figures_complete fail."""
    figures = (
        consistent_project / "results" / "figures" / "cma_verdicts_forest.png",
        consistent_project / "results" / "figures" / "hs300_rdd_robustness_forest.png",
    )
    for f in figures:
        f.unlink()
    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    fails = {c.name: c for c in assessment.checks if c.status == "fail"}
    assert "figures_complete" in fails
    assert "missing" in fails["figures_complete"].evidence[0]
    assert assessment.overall_status == "not_ready"


def test_doctor_strict_fail_bubbles_up(
    consistent_project: Path, monkeypatch
) -> None:
    """When doctor reports any fail, the gate aggregates it as not_ready."""
    from index_inclusion_research import doctor

    def _fake_run_all_checks() -> list[doctor.CheckResult]:
        return [
            doctor.CheckResult(
                name="fake_check_a", status="pass", message="ok"
            ),
            doctor.CheckResult(
                name="fake_check_b", status="fail", message="broken"
            ),
        ]

    monkeypatch.setattr(doctor, "run_all_checks", _fake_run_all_checks)
    # Run only the doctor check in isolation.
    assessment = submission_ready.assess_submission_ready(
        checks=(submission_ready.check_doctor_strict,)
    )
    assert assessment.blocker_count == 1
    assert assessment.overall_status == "not_ready"
    fail = assessment.checks[0]
    assert fail.name == "doctor_strict"
    assert fail.status == "fail"
    assert "1 fail" in fail.description


def test_fail_on_warn_flag_exits_1_on_warn_only(
    consistent_project: Path,
) -> None:
    """``--fail-on-warn`` upgrades warn-only assessments to exit code 1."""
    text = _make_skeleton_text(todos=2)
    (consistent_project / "paper" / "skeleton.md").write_text(text, encoding="utf-8")
    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    assert assessment.blocker_count == 0
    assert assessment.warning_count >= 1
    # Without --fail-on-warn, the warn-only path also returns 1 (matches
    # doctor / paper_integrity convention) but verify it explicitly:
    assert submission_ready.submission_exit_code(assessment) == 1
    assert (
        submission_ready.submission_exit_code(assessment, fail_on_warn=True) == 1
    )


def test_renderers_emit_all_three_formats(consistent_project: Path) -> None:
    """text / json / markdown renderers all produce non-empty, parseable output."""
    assessment = submission_ready.assess_submission_ready(checks=_FS_CHECKS)
    text_out = submission_ready.render_text(assessment, color=False)
    json_out = submission_ready.render_json(assessment)
    md_out = submission_ready.render_markdown(assessment)
    # text contains the headline status
    assert assessment.overall_status.upper() in text_out
    # json round-trips and carries the right keys
    payload = json.loads(json_out)
    assert payload["overall_status"] == assessment.overall_status
    assert payload["pass_count"] == assessment.pass_count
    assert payload["blocker_count"] == assessment.blocker_count
    assert len(payload["checks"]) == len(assessment.checks)
    # markdown produces a table header
    assert "| Status | Check |" in md_out


def test_real_project_assessment_runs_without_crashing() -> None:
    """Smoke: the full DEFAULT_CHECKS set runs against the real repo
    without raising and yields a structurally valid assessment."""
    assessment = submission_ready.assess_submission_ready()
    assert isinstance(assessment, submission_ready.SubmissionAssessment)
    assert assessment.overall_status in {"ready", "partially_ready", "not_ready"}
    total = assessment.pass_count + assessment.warning_count + assessment.blocker_count
    assert total == len(assessment.checks)
    assert total >= 15  # we register 17 checks; at minimum this many run
    # Exit-code helper must agree with overall_status.
    code = submission_ready.submission_exit_code(assessment)
    if assessment.blocker_count > 0:
        assert code == 2
    elif assessment.warning_count > 0:
        assert code == 1
    else:
        assert code == 0
