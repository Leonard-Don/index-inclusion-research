"""Submission-readiness gate (``index-inclusion-submission-ready``).

This is the 44th console script and the *final* go/no-go gate before the
manuscript ships. The existing :mod:`paper_integrity` gate (commit
``9e6115d``) already verifies cross-document consistency — verdicts CSV
agrees with the skeleton, methodology summary, public summary, etc. —
but "submission ready" is a *broader* question:

- Is the prose actually finished, or are there ``[TODO: ...]`` markers?
- Do all 9 figures referenced by the paper bundle exist and look real
  (non-empty + reasonable dimensions)?
- Does the TeX export exist and is the BibTeX file populated?
- Does the paper-integrity gate pass?
- Does PAP report ``all_unchanged``?
- Does the full doctor pass in strict mode?
- Are the raw input CSVs schema-valid?
- Are the sensitivity + verdict-timeline forest plots fresh?

This CLI orchestrates ~17 small checks and aggregates them into a single
human-readable verdict: **ready** / **not_ready** / **partially_ready**.
The intended workflow is:

    make rebuild && make paper && \
        index-inclusion-paper-integrity --fail-on-warn && \
        index-inclusion-submission-ready --fail-on-warn && \
        index-inclusion-tex-export --include-todos false --force

Severity ladder (matches :mod:`paper_integrity` and :mod:`doctor`):

- ``pass``  — the check is satisfied.
- ``warn``  — drift / soft issue; surfaces but does not block by default.
- ``fail``  — hard blocker; the paper is not submission ready.

Exit codes (match doctor / paper_integrity convention):

- ``0`` — overall_status == ``ready``.
- ``1`` — overall_status == ``partially_ready`` (warns only), or any
  warn with ``--fail-on-warn``.
- ``2`` — overall_status == ``not_ready`` (any fail).

The module is intentionally **read-only**: it inspects but never mutates
any artifact. Mutations are the job of the corresponding generators
(``make-figures-tables``, ``paper-skeleton``, ``methodology-summary``,
``tex-export`` etc.) which the ``fix_command`` field points to.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import os
import re
import shutil
import struct
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths

logger = logging.getLogger(__name__)


# ── canonical artifact locations ─────────────────────────────────────


def _skeleton_md() -> Path:
    return paths.project_root() / "paper" / "skeleton.md"


def _methodology_md() -> Path:
    return paths.project_root() / "paper" / "methodology_summary.md"


def _manuscript_tex() -> Path:
    return paths.project_root() / "paper" / "manuscript.tex"


def _references_bib() -> Path:
    return paths.project_root() / "paper" / "references.bib"


def _public_summary_json() -> Path:
    return paths.project_root() / "data" / "public" / "index_research_summary.json"


def _pap_report_csv() -> Path:
    return paths.real_tables_dir() / "pap_deviation_report.csv"


def _events_csv() -> Path:
    return paths.raw_data_dir() / "real_events.csv"


def _prices_csv() -> Path:
    return paths.raw_data_dir() / "real_prices.csv"


def _benchmarks_csv() -> Path:
    return paths.raw_data_dir() / "real_benchmarks.csv"


def _figures_dir() -> Path:
    return paths.results_dir() / "figures"


def _verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


# The 9 figures we expect a fully prepared paper to ship with: the 5 in
# the skeleton, plus verdict_timeline, plus the 3 cross-market CAR path
# plots that the dashboard / paper bundle both reference.
EXPECTED_FIGURE_RELPATHS: tuple[str, ...] = (
    "results/figures/hs300_rdd_robustness_forest.png",
    "results/figures/cma_verdicts_forest.png",
    "results/figures/cma_verdicts_sensitivity.png",
    "results/figures/cma_verdicts_ar_engine.png",
    "results/figures/cma_verdicts_2d_robustness.png",
    "results/figures/verdict_timeline.png",
    "results/figures/cn_announce_car_path.png",
    "results/figures/us_announce_car_path.png",
    "results/figures/cn_effective_car_path.png",
)

EXPECTED_PAPER_LIBRARY_COUNT: int = 16
EXPECTED_BIB_ENTRY_COUNT: int = 16
EXPECTED_HID_COUNT: int = 7
MIN_FIGURE_WIDTH: int = 800
MIN_FIGURE_HEIGHT: int = 600
EXPECTED_PAPER_SECTIONS: tuple[str, ...] = (
    "## 1. 引言",
    "## 2. 文献综述",
    "## 3. 研究设计",
    "## 4. 实证结果",
    "## 5. 限制与讨论",
    "## 6. 结论与启示",
    "## 7. PAP",
    "## 参考文献",
)
FRESHNESS_STALE_DAYS: int = 30  # used for skeleton vs methodology comparisons

Status = str  # "pass" / "warn" / "fail"

_STATUS_GLYPH: dict[Status, str] = {
    "pass": "✓",
    "warn": "!",
    "fail": "✗",
}
_STATUS_COLOR: dict[Status, str] = {
    "pass": "\033[32m",
    "warn": "\033[33m",
    "fail": "\033[31m",
}
_RESET = "\033[0m"


@dataclass(frozen=True)
class SubmissionCheck:
    """A single submission-readiness verdict.

    Attributes:
        name: short stable identifier (e.g. ``skeleton_exists``).
        status: ``pass`` / ``warn`` / ``fail``.
        description: one-line human summary.
        evidence: optional tuple of detail strings (e.g. file paths).
        fix_command: shell command that should remediate the check.
    """

    name: str
    status: Status
    description: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    fix_command: str = ""


@dataclass(frozen=True)
class SubmissionAssessment:
    """Aggregate readiness verdict.

    Attributes:
        overall_status: ``ready`` (0 warn / 0 fail), ``partially_ready``
            (warns only, no fails) or ``not_ready`` (any fail).
        checks: ordered tuple of individual checks.
        pass_count: # passing checks.
        warning_count: # warn-status checks.
        blocker_count: # fail-status checks (hard blockers).
        estimated_remaining_work_hours: rough heuristic — 1h per TODO
            marker in the skeleton plus 2h per fail check.
    """

    overall_status: str  # ready / partially_ready / not_ready
    checks: tuple[SubmissionCheck, ...]
    pass_count: int
    warning_count: int
    blocker_count: int
    estimated_remaining_work_hours: float


# ── tiny helpers ─────────────────────────────────────────────────────


def _relative(path: Path) -> str:
    root = paths.project_root()
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("submission_ready: failed reading %s: %s", path, exc)
        return None


def _read_json(path: Path) -> dict | None:
    text = _read_text(path)
    if text is None:
        return None
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("submission_ready: bad JSON %s: %s", path, exc)
        return None
    return loaded if isinstance(loaded, dict) else None


def _png_dimensions(path: Path) -> tuple[int, int] | None:
    """Parse the IHDR chunk of a PNG and return (width, height).

    Returns ``None`` if the file is not a recognisable PNG (e.g. JPG /
    truncated). Zero-dependency — we don't want to require Pillow just
    to read a 24-byte header.
    """
    try:
        with path.open("rb") as fh:
            sig = fh.read(8)
            if sig != b"\x89PNG\r\n\x1a\n":
                return None
            # IHDR length(4) + type(4) + width(4) + height(4) + ...
            fh.read(4)  # chunk length, always 13
            ctype = fh.read(4)
            if ctype != b"IHDR":
                return None
            w, h = struct.unpack(">II", fh.read(8))
            return int(w), int(h)
    except (OSError, struct.error) as exc:
        logger.warning("submission_ready: png header read failed for %s: %s", path, exc)
        return None


def _count_todo_markers(text: str) -> tuple[int, dict[str, int]]:
    """Count ``[TODO: ...]`` markers and bucket them by leading section.

    Returns ``(total, breakdown)`` where ``breakdown`` maps section
    heading prefix (e.g. ``§4.2``) to the count of TODO markers found
    within that section. The grouping is best-effort — anything we can't
    bucket lands in ``"__unbucketed__"``.
    """
    total = 0
    bucket: dict[str, int] = {}
    current_section = "__preamble__"
    section_re = re.compile(r"^#{1,4}\s+(.*)$")
    todo_re = re.compile(r"\[TODO\b")
    for line in text.splitlines():
        m = section_re.match(line.strip())
        if m:
            current_section = m.group(1).strip()[:60]
            continue
        if todo_re.search(line):
            total += 1
            bucket[current_section] = bucket.get(current_section, 0) + 1
    return total, bucket


def _mtime(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        return path.stat().st_mtime
    except OSError:
        return None


# ── individual checks ────────────────────────────────────────────────


def check_skeleton_exists() -> SubmissionCheck:
    p = _skeleton_md()
    if not p.exists():
        return SubmissionCheck(
            name="skeleton_exists",
            status="fail",
            description=f"Paper skeleton missing at {_relative(p)}.",
            fix_command="index-inclusion-paper-skeleton --force",
        )
    return SubmissionCheck(
        name="skeleton_exists",
        status="pass",
        description=f"Paper skeleton present at {_relative(p)}.",
    )


def check_paper_sections_present() -> SubmissionCheck:
    text = _read_text(_skeleton_md())
    if text is None:
        return SubmissionCheck(
            name="paper_sections_present",
            status="fail",
            description="Cannot enumerate sections — skeleton.md unreadable.",
            fix_command="index-inclusion-paper-skeleton --force",
        )
    missing: list[str] = []
    for header in EXPECTED_PAPER_SECTIONS:
        # Heading might end with extra text (e.g. "## 7. PAP (...)"). We
        # do a startswith probe against each line, anchored on the prefix.
        if not any(line.startswith(header) for line in text.splitlines()):
            missing.append(header)
    if missing:
        return SubmissionCheck(
            name="paper_sections_present",
            status="fail",
            description=(
                f"{len(missing)}/{len(EXPECTED_PAPER_SECTIONS)} expected "
                "top-level sections missing from skeleton.md."
            ),
            evidence=tuple(missing),
            fix_command="index-inclusion-paper-skeleton --force",
        )
    return SubmissionCheck(
        name="paper_sections_present",
        status="pass",
        description=(
            f"All {len(EXPECTED_PAPER_SECTIONS)} required top-level sections "
            "present in skeleton.md."
        ),
    )


def check_prose_todo_markers() -> SubmissionCheck:
    text = _read_text(_skeleton_md())
    if text is None:
        return SubmissionCheck(
            name="prose_todo_markers",
            status="fail",
            description="Cannot count TODOs — skeleton.md unreadable.",
            fix_command="index-inclusion-paper-skeleton --force",
        )
    total, bucket = _count_todo_markers(text)
    if total == 0:
        return SubmissionCheck(
            name="prose_todo_markers",
            status="pass",
            description="No [TODO: ...] markers remaining in skeleton.md.",
        )
    # Sort sections by count desc so the heaviest section sits on top.
    breakdown = sorted(bucket.items(), key=lambda kv: -kv[1])
    evidence = tuple(f"{section}: {n}" for section, n in breakdown[:10])
    return SubmissionCheck(
        name="prose_todo_markers",
        status="warn",
        description=(
            f"{total} [TODO: ...] marker(s) in skeleton.md across "
            f"{len(bucket)} section(s); prose not finalised."
        ),
        evidence=evidence,
        fix_command=(
            "Resolve [TODO: ...] markers in paper/skeleton.md "
            "(prose authoring is manual)."
        ),
    )


def check_methodology_summary_present() -> SubmissionCheck:
    p = _methodology_md()
    if not p.exists():
        return SubmissionCheck(
            name="methodology_summary_present",
            status="fail",
            description=f"Methodology summary missing at {_relative(p)}.",
            fix_command="index-inclusion-methodology-summary",
        )
    # Stale-vs-skeleton check: methodology summary should generally be at
    # least as fresh as the skeleton (skeleton is the canonical narrative
    # and methodology summarises sample sizes / sensitivity in lockstep).
    skel_mtime = _mtime(_skeleton_md())
    meth_mtime = _mtime(p)
    if skel_mtime is not None and meth_mtime is not None and skel_mtime > meth_mtime:
        delta_days = (skel_mtime - meth_mtime) / 86400.0
        if delta_days > 1.0:
            return SubmissionCheck(
                name="methodology_summary_present",
                status="warn",
                description=(
                    f"Methodology summary is {delta_days:.1f}d older than "
                    "skeleton.md; may be stale."
                ),
                fix_command="index-inclusion-methodology-summary",
            )
    return SubmissionCheck(
        name="methodology_summary_present",
        status="pass",
        description=f"Methodology summary present at {_relative(p)}.",
    )


def check_figures_complete() -> SubmissionCheck:
    root = paths.project_root()
    missing: list[str] = []
    empty: list[str] = []
    small: list[str] = []
    ok = 0
    for relpath in EXPECTED_FIGURE_RELPATHS:
        candidate = root / relpath
        if not candidate.exists():
            missing.append(relpath)
            continue
        try:
            size = candidate.stat().st_size
        except OSError:
            size = 0
        if size <= 200:  # PNG header alone is 8B; real figures are KB+
            empty.append(relpath)
            continue
        dims = _png_dimensions(candidate)
        if dims is not None:
            w, h = dims
            if w < MIN_FIGURE_WIDTH or h < MIN_FIGURE_HEIGHT:
                small.append(f"{relpath} ({w}x{h})")
                continue
        ok += 1
    evidence: list[str] = []
    if missing:
        evidence.append(f"missing: {missing}")
    if empty:
        evidence.append(f"empty: {empty}")
    if small:
        evidence.append(f"under {MIN_FIGURE_WIDTH}x{MIN_FIGURE_HEIGHT}: {small}")
    if missing or empty:
        return SubmissionCheck(
            name="figures_complete",
            status="fail",
            description=(
                f"{ok}/{len(EXPECTED_FIGURE_RELPATHS)} expected figures OK; "
                f"{len(missing)} missing, {len(empty)} empty."
            ),
            evidence=tuple(evidence),
            fix_command="index-inclusion-make-figures-tables",
        )
    if small:
        return SubmissionCheck(
            name="figures_complete",
            status="warn",
            description=(
                f"{ok}/{len(EXPECTED_FIGURE_RELPATHS)} figures meet "
                f"{MIN_FIGURE_WIDTH}x{MIN_FIGURE_HEIGHT}; "
                f"{len(small)} below threshold."
            ),
            evidence=tuple(evidence),
            fix_command=(
                "Re-render the small figure(s) at the standard 1200x800 "
                "DPI used by figures_tables.py."
            ),
        )
    return SubmissionCheck(
        name="figures_complete",
        status="pass",
        description=(
            f"All {len(EXPECTED_FIGURE_RELPATHS)} expected figures present, "
            f"non-empty, and ≥ {MIN_FIGURE_WIDTH}x{MIN_FIGURE_HEIGHT}."
        ),
    )


def check_tex_artifacts() -> SubmissionCheck:
    tex = _manuscript_tex()
    bib = _references_bib()
    missing: list[str] = []
    if not tex.exists():
        missing.append(_relative(tex))
    if not bib.exists():
        missing.append(_relative(bib))
    if missing:
        return SubmissionCheck(
            name="tex_artifacts",
            status="fail",
            description=f"TeX artifacts missing: {', '.join(missing)}.",
            fix_command="index-inclusion-tex-export --force",
        )
    return SubmissionCheck(
        name="tex_artifacts",
        status="pass",
        description=(
            f"TeX artifacts present ({_relative(tex)} + {_relative(bib)})."
        ),
    )


def check_tex_compiles() -> SubmissionCheck:
    """If ``pdflatex`` is on PATH, sanity-compile the manuscript.

    The compile is run inside an isolated temp directory so we never
    pollute the repo. We just want to know whether the .tex parses and
    the bibliography keys resolve. If ``pdflatex`` is unavailable we
    return ``warn`` with a "skipped" message rather than ``fail`` —
    pdflatex is an external system dependency.
    """
    tex = _manuscript_tex()
    if not tex.exists():
        return SubmissionCheck(
            name="tex_compiles",
            status="warn",
            description="Skipped — manuscript.tex missing.",
            fix_command="index-inclusion-tex-export --force",
        )
    pdflatex = shutil.which("pdflatex")
    if pdflatex is None:
        return SubmissionCheck(
            name="tex_compiles",
            status="warn",
            description=(
                "Skipped — pdflatex not on PATH; cannot try TeX compile. "
                "Install MacTeX / TeX Live to enable this check."
            ),
        )
    # Run inside an explicit temp dir so we don't touch the repo.
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            result = subprocess.run(  # noqa: S603 — pdflatex from shutil.which is trusted
                [
                    pdflatex,
                    "-interaction=nonstopmode",
                    "-halt-on-error",
                    "-output-directory",
                    tmpdir,
                    str(tex),
                ],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=str(tex.parent),
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            return SubmissionCheck(
                name="tex_compiles",
                status="warn",
                description=f"pdflatex invocation failed: {exc}",
            )
    if result.returncode != 0:
        # Extract the first error-ish line to make the report actionable.
        err_lines = [
            ln for ln in result.stdout.splitlines()
            if ln.startswith("!") or "Error" in ln
        ]
        first = err_lines[0] if err_lines else "(no error line surfaced)"
        return SubmissionCheck(
            name="tex_compiles",
            status="fail",
            description=f"pdflatex returned {result.returncode}: {first}",
            fix_command="index-inclusion-tex-export --force",
        )
    return SubmissionCheck(
        name="tex_compiles",
        status="pass",
        description="pdflatex compiled manuscript.tex without errors.",
    )


def check_references_bib_populated() -> SubmissionCheck:
    text = _read_text(_references_bib())
    if text is None:
        return SubmissionCheck(
            name="references_bib_populated",
            status="fail",
            description="references.bib missing.",
            fix_command="index-inclusion-tex-export --force",
        )
    entries = re.findall(r"^@\w+\{[^,]+,", text, flags=re.MULTILINE)
    if len(entries) != EXPECTED_BIB_ENTRY_COUNT:
        return SubmissionCheck(
            name="references_bib_populated",
            status="fail",
            description=(
                f"references.bib has {len(entries)} entries; expected "
                f"{EXPECTED_BIB_ENTRY_COUNT}."
            ),
            fix_command="index-inclusion-tex-export --force",
        )
    return SubmissionCheck(
        name="references_bib_populated",
        status="pass",
        description=(
            f"references.bib has the expected {EXPECTED_BIB_ENTRY_COUNT} "
            "BibTeX entries."
        ),
    )


def check_paper_integrity_passes() -> SubmissionCheck:
    """Re-run the paper-integrity gate and bubble up its verdict."""
    from index_inclusion_research import paper_integrity

    issues = paper_integrity.check_paper_integrity()
    summary = paper_integrity.integrity_summary(issues)
    if summary["fail"] > 0:
        return SubmissionCheck(
            name="paper_integrity_passes",
            status="fail",
            description=(
                f"paper-integrity gate has {summary['fail']} fail / "
                f"{summary['warn']} warn / {summary['info']} info."
            ),
            evidence=tuple(
                f"[{i.category}] {i.description}"
                for i in issues
                if i.severity == "fail"
            ),
            fix_command="index-inclusion-paper-integrity",
        )
    if summary["warn"] > 0:
        return SubmissionCheck(
            name="paper_integrity_passes",
            status="warn",
            description=(
                f"paper-integrity gate has {summary['warn']} warn / "
                f"{summary['info']} info."
            ),
            evidence=tuple(
                f"[{i.category}] {i.description}"
                for i in issues
                if i.severity == "warn"
            ),
            fix_command="index-inclusion-paper-integrity",
        )
    return SubmissionCheck(
        name="paper_integrity_passes",
        status="pass",
        description=(
            f"paper-integrity gate passes ({summary['info']} info-only note(s))."
        ),
    )


def check_pap_all_unchanged() -> SubmissionCheck:
    summary = _read_json(_public_summary_json())
    if summary is None:
        return SubmissionCheck(
            name="pap_all_unchanged",
            status="fail",
            description="public summary JSON missing — cannot inspect PAP status.",
            fix_command="index-inclusion-export-public-summary",
        )
    block = summary.get("pap_deviation_summary")
    if not isinstance(block, dict):
        return SubmissionCheck(
            name="pap_all_unchanged",
            status="fail",
            description=(
                "public summary JSON missing pap_deviation_summary block."
            ),
            fix_command="index-inclusion-export-public-summary",
        )
    flipped = int(block.get("flipped_count", 0))
    weakened = int(block.get("weakened_count", 0))
    tightened = int(block.get("tightened_count", 0))
    unverifiable = int(block.get("unverifiable_count", 0))
    all_unchanged = bool(block.get("all_unchanged", False))
    if flipped > 0:
        return SubmissionCheck(
            name="pap_all_unchanged",
            status="fail",
            description=(
                f"PAP report shows {flipped} flipped hypothesis "
                "verdict(s); PAP §7 signoff required."
            ),
            fix_command="index-inclusion-pap-diff",
        )
    if weakened or tightened or unverifiable:
        evidence = (
            f"tightened={tightened}", f"weakened={weakened}",
            f"unverifiable={unverifiable}",
        )
        return SubmissionCheck(
            name="pap_all_unchanged",
            status="warn",
            description=(
                "PAP report shows non-zero tightened/weakened/unverifiable "
                "counts; document deviation in PAP §7."
            ),
            evidence=evidence,
            fix_command="index-inclusion-pap-diff",
        )
    if not all_unchanged:
        return SubmissionCheck(
            name="pap_all_unchanged",
            status="warn",
            description=(
                "PAP report does not assert all_unchanged=True even though "
                "the per-category counts are zero."
            ),
            fix_command="index-inclusion-pap-diff",
        )
    return SubmissionCheck(
        name="pap_all_unchanged",
        status="pass",
        description="PAP report: all 7 hypotheses unchanged vs baseline.",
    )


def check_doctor_strict() -> SubmissionCheck:
    """Re-run doctor checks and return ``pass`` only if everything is green.

    We deliberately re-run rather than parse an artifact: the doctor is
    cheap and authoritative.
    """
    try:
        from index_inclusion_research import doctor
    except ImportError as exc:  # pragma: no cover - defensive
        return SubmissionCheck(
            name="doctor_strict",
            status="fail",
            description=f"doctor module import failed: {exc}",
        )
    try:
        results = doctor.run_all_checks()
    except Exception as exc:  # noqa: BLE001 — diagnostics, not crashes
        return SubmissionCheck(
            name="doctor_strict",
            status="fail",
            description=f"doctor.run_all_checks raised {type(exc).__name__}: {exc}",
        )
    fails = [r for r in results if r.status == "fail"]
    warns = [r for r in results if r.status == "warn"]
    if fails:
        return SubmissionCheck(
            name="doctor_strict",
            status="fail",
            description=(
                f"doctor: {len(fails)} fail / {len(warns)} warn / "
                f"{len(results)} total checks."
            ),
            evidence=tuple(f"{r.name}: {r.message}" for r in fails),
            fix_command="index-inclusion-doctor",
        )
    if warns:
        return SubmissionCheck(
            name="doctor_strict",
            status="warn",
            description=(
                f"doctor: {len(warns)} warn / {len(results)} total checks "
                "(no fails). `make doctor-strict` would exit non-zero."
            ),
            evidence=tuple(f"{r.name}: {r.message}" for r in warns[:6]),
            fix_command="index-inclusion-doctor",
        )
    return SubmissionCheck(
        name="doctor_strict",
        status="pass",
        description=(
            f"doctor: all {len(results)} checks pass."
        ),
    )


def check_public_summary_fresh() -> SubmissionCheck:
    p = _public_summary_json()
    if not p.exists():
        return SubmissionCheck(
            name="public_summary_fresh",
            status="fail",
            description=f"public summary missing at {_relative(p)}.",
            fix_command="index-inclusion-export-public-summary",
        )
    summary = _read_json(p)
    if summary is None:
        return SubmissionCheck(
            name="public_summary_fresh",
            status="fail",
            description="public summary JSON unparseable.",
            fix_command="index-inclusion-export-public-summary",
        )
    # Compare mtime against the canonical verdicts CSV — if the CSV is
    # newer, the summary needs a refresh.
    summary_mtime = _mtime(p)
    verdicts_mtime = _mtime(_verdicts_csv())
    if (
        summary_mtime is not None
        and verdicts_mtime is not None
        and verdicts_mtime > summary_mtime + 60.0  # 1-minute clock-skew slack
    ):
        return SubmissionCheck(
            name="public_summary_fresh",
            status="warn",
            description=(
                "public summary JSON is older than cma_hypothesis_verdicts.csv; "
                "regenerate before submission."
            ),
            fix_command="index-inclusion-export-public-summary",
        )
    return SubmissionCheck(
        name="public_summary_fresh",
        status="pass",
        description=(
            f"public summary present and fresh (schema_version="
            f"{summary.get('schema_version', '?')})."
        ),
    )


def check_data_csv_schemas() -> SubmissionCheck:
    """Spot-check that the three canonical raw CSVs have the expected columns."""
    expected: dict[Path, set[str]] = {
        _events_csv(): {"market", "ticker", "announce_date", "effective_date"},
        _prices_csv(): {"market", "ticker", "date", "close"},
        _benchmarks_csv(): {"market", "date", "benchmark_ret"},
    }
    missing_files: list[str] = []
    missing_cols: list[str] = []
    for path, required in expected.items():
        if not path.exists():
            missing_files.append(_relative(path))
            continue
        try:
            head = pd.read_csv(path, nrows=1)
        except (OSError, ValueError) as exc:
            missing_files.append(f"{_relative(path)} (unreadable: {exc})")
            continue
        absent = required - set(head.columns)
        if absent:
            missing_cols.append(f"{_relative(path)}: {sorted(absent)}")
    if missing_files:
        return SubmissionCheck(
            name="data_csv_schemas",
            status="fail",
            description=f"{len(missing_files)} raw input CSV(s) missing or unreadable.",
            evidence=tuple(missing_files),
            fix_command="index-inclusion-download-real-data",
        )
    if missing_cols:
        return SubmissionCheck(
            name="data_csv_schemas",
            status="fail",
            description=f"{len(missing_cols)} raw CSV(s) missing required columns.",
            evidence=tuple(missing_cols),
            fix_command="index-inclusion-download-real-data",
        )
    return SubmissionCheck(
        name="data_csv_schemas",
        status="pass",
        description=(
            "All 3 raw input CSVs (real_events / real_prices / "
            "real_benchmarks) carry the expected columns."
        ),
    )


def check_literature_catalog() -> SubmissionCheck:
    try:
        from index_inclusion_research.literature_catalog import PAPER_LIBRARY
    except ImportError as exc:
        return SubmissionCheck(
            name="literature_catalog",
            status="fail",
            description=f"literature_catalog import failed: {exc}",
        )
    count = len(PAPER_LIBRARY)
    if count < EXPECTED_PAPER_LIBRARY_COUNT:
        return SubmissionCheck(
            name="literature_catalog",
            status="fail",
            description=(
                f"literature_catalog.PAPER_LIBRARY has {count} entries; "
                f"expected ≥ {EXPECTED_PAPER_LIBRARY_COUNT}."
            ),
        )
    return SubmissionCheck(
        name="literature_catalog",
        status="pass",
        description=(
            f"literature_catalog.PAPER_LIBRARY carries {count} entries."
        ),
    )


def check_sensitivity_artifacts_fresh() -> SubmissionCheck:
    """The 4 sensitivity / robustness PNGs must exist and post-date the verdicts CSV."""
    figures_dir = _figures_dir()
    expected = (
        "cma_verdicts_sensitivity.png",
        "cma_verdicts_ar_engine.png",
        "cma_verdicts_2d_robustness.png",
    )
    verdicts_mtime = _mtime(_verdicts_csv())
    missing: list[str] = []
    stale: list[str] = []
    for fname in expected:
        fpath = figures_dir / fname
        if not fpath.exists():
            missing.append(fname)
            continue
        fmtime = _mtime(fpath)
        if (
            verdicts_mtime is not None
            and fmtime is not None
            and verdicts_mtime > fmtime + 60.0
        ):
            stale.append(fname)
    if missing:
        return SubmissionCheck(
            name="sensitivity_artifacts_fresh",
            status="fail",
            description=(
                f"{len(missing)}/{len(expected)} sensitivity figure(s) missing."
            ),
            evidence=tuple(missing),
            fix_command=(
                "index-inclusion-build-cma-sensitivity-forest && "
                "index-inclusion-build-cma-ar-engine-forest && "
                "index-inclusion-build-cma-2d-robustness-heatmap"
            ),
        )
    if stale:
        return SubmissionCheck(
            name="sensitivity_artifacts_fresh",
            status="warn",
            description=(
                f"{len(stale)} sensitivity figure(s) older than the "
                "verdicts CSV; regenerate."
            ),
            evidence=tuple(stale),
            fix_command=(
                "index-inclusion-build-cma-sensitivity-forest && "
                "index-inclusion-build-cma-ar-engine-forest && "
                "index-inclusion-build-cma-2d-robustness-heatmap"
            ),
        )
    return SubmissionCheck(
        name="sensitivity_artifacts_fresh",
        status="pass",
        description=(
            f"All {len(expected)} sensitivity / robustness figures present "
            "and fresh vs verdicts CSV."
        ),
    )


def check_verdict_timeline_fresh() -> SubmissionCheck:
    p = _figures_dir() / "verdict_timeline.png"
    if not p.exists():
        return SubmissionCheck(
            name="verdict_timeline_fresh",
            status="fail",
            description=f"verdict_timeline.png missing at {_relative(p)}.",
            fix_command="index-inclusion-verdict-timeline --force",
        )
    fmtime = _mtime(p)
    verdicts_mtime = _mtime(_verdicts_csv())
    if (
        verdicts_mtime is not None
        and fmtime is not None
        and verdicts_mtime > fmtime + 60.0
    ):
        return SubmissionCheck(
            name="verdict_timeline_fresh",
            status="warn",
            description=(
                "verdict_timeline.png is older than the verdicts CSV; "
                "regenerate."
            ),
            fix_command="index-inclusion-verdict-timeline --force",
        )
    return SubmissionCheck(
        name="verdict_timeline_fresh",
        status="pass",
        description="verdict_timeline.png present and fresh.",
    )


def check_test_suite_status() -> SubmissionCheck:
    """Report whether pytest is invoked here.

    Running the full suite inside this CLI is too slow (8+ min) and
    breaks the read-only constraint (pytest can litter ``.pytest_cache``).
    Instead we report ``warn`` with the canonical command to run it
    out-of-band — gates that want a strict pass should chain pytest + this
    CLI in CI.
    """
    return SubmissionCheck(
        name="test_suite_status",
        status="warn",
        description=(
            "Test suite not executed inline; run `pytest` separately "
            "before submission."
        ),
        fix_command="pytest --maxfail=1 -q",
    )


DEFAULT_CHECKS: tuple[Callable[[], SubmissionCheck], ...] = (
    check_skeleton_exists,
    check_paper_sections_present,
    check_prose_todo_markers,
    check_methodology_summary_present,
    check_figures_complete,
    check_tex_artifacts,
    check_references_bib_populated,
    check_tex_compiles,
    check_paper_integrity_passes,
    check_pap_all_unchanged,
    check_public_summary_fresh,
    check_data_csv_schemas,
    check_literature_catalog,
    check_sensitivity_artifacts_fresh,
    check_verdict_timeline_fresh,
    check_doctor_strict,
    check_test_suite_status,
)


# ── orchestrator + aggregation ───────────────────────────────────────


def assess_submission_ready(
    root: Path | None = None,
    *,
    checks: Sequence[Callable[[], SubmissionCheck]] | None = None,
) -> SubmissionAssessment:
    """Run all configured checks and return an aggregated assessment.

    ``root`` is accepted for API symmetry with :mod:`paper_integrity`.
    The per-check helpers read paths via :mod:`paths` so setting
    ``INDEX_INCLUSION_ROOT`` for the duration of the call is enough to
    rebase the gate against a tmp tree.
    """
    if root is not None:
        prev = os.environ.get("INDEX_INCLUSION_ROOT")
        os.environ["INDEX_INCLUSION_ROOT"] = str(root)
        try:
            return assess_submission_ready(root=None, checks=checks)
        finally:
            if prev is None:
                os.environ.pop("INDEX_INCLUSION_ROOT", None)
            else:
                os.environ["INDEX_INCLUSION_ROOT"] = prev

    checks = checks or DEFAULT_CHECKS
    results: list[SubmissionCheck] = []
    for check in checks:
        try:
            results.append(check())
        except Exception as exc:  # noqa: BLE001 — diagnostics, not crashes
            logger.exception("submission_ready check %s raised", check.__name__)
            results.append(
                SubmissionCheck(
                    name=check.__name__,
                    status="fail",
                    description=(
                        f"check raised {type(exc).__name__}: {exc}"
                    ),
                )
            )
    pass_count = sum(1 for r in results if r.status == "pass")
    warning_count = sum(1 for r in results if r.status == "warn")
    blocker_count = sum(1 for r in results if r.status == "fail")
    if blocker_count > 0:
        overall = "not_ready"
    elif warning_count > 0:
        overall = "partially_ready"
    else:
        overall = "ready"

    # Heuristic remaining-work estimate: each fail check costs ~2 hours,
    # each warn costs ~0.5 hours, and each TODO marker in the skeleton
    # costs ~1 hour. The TODO portion is derived from the same skeleton
    # check above to keep the number self-consistent with the report.
    todo_hours = 0.0
    todo_check = next((r for r in results if r.name == "prose_todo_markers"), None)
    if todo_check is not None:
        # Pull the marker count out of the description: "N [TODO: ...] marker(s)".
        m = re.search(r"(\d+)\s+\[TODO", todo_check.description)
        if m:
            todo_hours = float(m.group(1)) * 1.0
    remaining_hours = blocker_count * 2.0 + warning_count * 0.5 + todo_hours

    return SubmissionAssessment(
        overall_status=overall,
        checks=tuple(results),
        pass_count=pass_count,
        warning_count=warning_count,
        blocker_count=blocker_count,
        estimated_remaining_work_hours=round(remaining_hours, 1),
    )


def submission_exit_code(
    assessment: SubmissionAssessment,
    *,
    fail_on_warn: bool = False,
) -> int:
    """Map a SubmissionAssessment to a CLI exit code (0 / 1 / 2)."""
    if assessment.blocker_count > 0:
        return 2
    if fail_on_warn and assessment.warning_count > 0:
        return 1
    if assessment.warning_count > 0:
        return 1
    return 0


# ── rendering ────────────────────────────────────────────────────────


def render_text(assessment: SubmissionAssessment, *, color: bool = True) -> str:
    lines: list[str] = []
    lines.append("=" * 64)
    lines.append(" INDEX-INCLUSION-RESEARCH · submission-ready")
    lines.append("=" * 64)
    headline = assessment.overall_status.upper()
    glyph = {
        "ready": "✓",
        "partially_ready": "!",
        "not_ready": "✗",
    }.get(assessment.overall_status, "?")
    status_color = {
        "ready": "\033[32m",
        "partially_ready": "\033[33m",
        "not_ready": "\033[31m",
    }.get(assessment.overall_status, "")
    if color and status_color:
        lines.append(f"  {status_color}{glyph}{_RESET}  {headline}")
    else:
        lines.append(f"  {glyph}  {headline}")
    lines.append(
        f"  {assessment.pass_count} pass · {assessment.warning_count} warn · "
        f"{assessment.blocker_count} fail · "
        f"~{assessment.estimated_remaining_work_hours:.1f}h remaining"
    )
    lines.append("")
    for c in assessment.checks:
        g = _STATUS_GLYPH[c.status]
        if color:
            head = f"  {_STATUS_COLOR[c.status]}{g}{_RESET}  {c.name}"
        else:
            head = f"  {g}  {c.name}"
        lines.append(head)
        lines.append(f"      {c.description}")
        for ev in c.evidence:
            lines.append(f"        - {ev}")
        if c.status != "pass" and c.fix_command:
            lines.append(f"      → {c.fix_command}")
        lines.append("")
    if assessment.overall_status == "ready":
        lines.append("  Submission gate: GREEN. Paper may proceed to submission.")
    elif assessment.overall_status == "partially_ready":
        lines.append(
            "  Submission gate: YELLOW. Address warnings before submitting; "
            "no hard blockers."
        )
    else:
        lines.append(
            f"  Submission gate: RED. Fix {assessment.blocker_count} "
            "blocker(s) before submitting."
        )
    return "\n".join(lines).rstrip() + "\n"


def render_json(assessment: SubmissionAssessment) -> str:
    payload = {
        "overall_status": assessment.overall_status,
        "pass_count": assessment.pass_count,
        "warning_count": assessment.warning_count,
        "blocker_count": assessment.blocker_count,
        "estimated_remaining_work_hours": assessment.estimated_remaining_work_hours,
        "generated_at": _dt.datetime.now(_dt.UTC).isoformat(),
        "checks": [
            {
                "name": c.name,
                "status": c.status,
                "description": c.description,
                "evidence": list(c.evidence),
                "fix_command": c.fix_command,
            }
            for c in assessment.checks
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def render_markdown(assessment: SubmissionAssessment) -> str:
    lines: list[str] = [
        "# Submission readiness report",
        "",
        f"**Overall status**: `{assessment.overall_status}`",
        "",
        f"- pass: **{assessment.pass_count}**",
        f"- warn: **{assessment.warning_count}**",
        f"- fail: **{assessment.blocker_count}**",
        f"- estimated remaining work: **{assessment.estimated_remaining_work_hours:.1f}h**",
        "",
        "| Status | Check | Description | Fix |",
        "|---|---|---|---|",
    ]
    for c in assessment.checks:
        desc = c.description.replace("|", "\\|")
        fix = c.fix_command.replace("|", "\\|") if c.fix_command else ""
        fix_cell = f"`{fix}`" if fix else ""
        lines.append(
            f"| {c.status} | {c.name} | {desc} | {fix_cell} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Final pre-submission readiness gate for the paper. Aggregates "
            "~17 checks (prose completion, figures, TeX, paper integrity, "
            "PAP, doctor, raw-data schemas, sensitivity freshness) and "
            "reports ready / partially_ready / not_ready."
        )
    )
    parser.add_argument(
        "--format",
        choices=("text", "json", "markdown"),
        default="text",
        help="Choose output renderer (default: text).",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colour escape codes (text format only).",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help=(
            "Treat warn-status checks as enough to exit 1. Useful for CI."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    assessment = assess_submission_ready()
    if args.format == "json":
        sys.stdout.write(render_json(assessment))
    elif args.format == "markdown":
        sys.stdout.write(render_markdown(assessment))
    else:
        enable_color = not args.no_color and sys.stdout.isatty()
        sys.stdout.write(render_text(assessment, color=enable_color))
    return submission_exit_code(assessment, fail_on_warn=args.fail_on_warn)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
