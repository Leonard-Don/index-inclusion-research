"""Cross-document consistency gate (``index-inclusion-paper-integrity``).

Individual generators across the 41 console scripts are independently
tested, but they emit artifacts that *talk about each other*. The paper
verdicts CSV is referenced in the skeleton, the methodology summary
recaps the same sample sizes, the public summary JSON quotes PAP
classifications, the README boasts a CLI count that must equal the
``[project.scripts]`` table, and so on. If one of these emitters drifts
without its consumers being regenerated, the paper bundle ships with
internal contradictions.

This module is a *read-only* cross-document audit: it never mutates an
artifact, just compares them. Each individual check function returns 0+
``IntegrityIssue`` records; :func:`check_paper_integrity` orchestrates
them and the CLI / doctor wrapper renders the results.

Severity ladder (matches doctor):

- ``info``  — surfaced for visibility but doesn't gate publication.
- ``warn``  — drift the user should look at; ``--fail-on-warn`` gates CI.
- ``fail``  — hard inconsistency, paper is not publication-ready.

Exit codes (matches doctor convention):

- ``0`` — all checks emitted no issues at warn or fail severity.
- ``1`` — at least one warn (or any fail with ``--fail-on-warn``).
- ``2`` — at least one fail.

Each check is intentionally small and self-contained so it stays easy to
unit-test in isolation (see :mod:`tests.test_paper_integrity`).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import tomllib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths

logger = logging.getLogger(__name__)

# ── canonical artifact locations ─────────────────────────────────────


def _default_verdicts_csv() -> Path:
    return paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"


def _default_skeleton_md() -> Path:
    return paths.project_root() / "paper" / "skeleton.md"


def _default_methodology_md() -> Path:
    return paths.project_root() / "paper" / "methodology_summary.md"


def _default_pap_report_csv() -> Path:
    return paths.real_tables_dir() / "pap_deviation_report.csv"


def _default_public_summary_json() -> Path:
    return paths.project_root() / "data" / "public" / "index_research_summary.json"


def _default_pyproject_toml() -> Path:
    return paths.project_root() / "pyproject.toml"


def _default_readme_md() -> Path:
    return paths.project_root() / "README.md"


def _default_figures_dir() -> Path:
    return paths.results_dir() / "figures"


def _default_cli_reference_md() -> Path:
    return paths.docs_dir() / "cli_reference.md"


EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")
EXPECTED_PAPER_LIBRARY_COUNT: int = 16

Severity = str  # one of "info" / "warn" / "fail"
_SEVERITY_GLYPH: dict[Severity, str] = {
    "info": "i",
    "warn": "!",
    "fail": "x",
}
_SEVERITY_COLOR: dict[Severity, str] = {
    "info": "\033[34m",
    "warn": "\033[33m",
    "fail": "\033[31m",
}
_RESET = "\033[0m"


@dataclass(frozen=True)
class IntegrityIssue:
    """A single cross-document inconsistency.

    Attributes:
        severity: ``info``, ``warn`` or ``fail`` (see module docstring).
        category: short tag (e.g. ``hypothesis_set``, ``figures``).
        description: one-line human summary of the inconsistency.
        evidence: tuple of detail strings (e.g. specific mismatched IDs).
        fix_command: shell command to suggest as a remediation hint.
    """

    severity: Severity
    category: str
    description: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    fix_command: str = ""


# ── tiny helpers ─────────────────────────────────────────────────────


def _relative(path: Path, *, root: Path | None = None) -> str:
    root_path = root if root is not None else paths.project_root()
    try:
        return str(path.relative_to(root_path))
    except ValueError:
        return str(path)


def _read_csv_safe(path: Path) -> pd.DataFrame | None:
    """Read a CSV; return ``None`` on missing / unreadable.

    The check functions translate ``None`` into an ``info`` issue rather
    than letting pandas exceptions bubble up — gates should diagnose, not
    crash.
    """
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, keep_default_na=False)
    except (OSError, ValueError) as exc:  # pragma: no cover - defensive
        logger.warning("paper_integrity: failed to read %s: %s", path, exc)
        return None


def _read_text_safe(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - defensive
        logger.warning("paper_integrity: failed to read %s: %s", path, exc)
        return None


def _read_json_safe(path: Path) -> dict | None:
    text = _read_text_safe(path)
    if text is None:
        return None
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        logger.warning("paper_integrity: failed to parse JSON %s: %s", path, exc)
        return None
    return loaded if isinstance(loaded, dict) else None


def _extract_h_rows_from_markdown(text: str) -> dict[str, dict[str, str]]:
    """Return ``{hid: {verdict, tier, n_obs}}`` parsed from markdown tables.

    The skeleton and methodology summary both render H1..H7 as markdown
    pipe rows. We pull the first three meaningful cells per row from any
    table line that starts with ``| H<digit>``. This is intentionally
    permissive — we just want the verdict / tier / n triplet — because
    the two summaries use slightly different column orderings (e.g.
    skeleton has 8 columns including 头条指标 + 主线; methodology has 5
    columns: 假说 / 名称 / n_obs / 证据层级 / 主线).
    """
    rows: dict[str, dict[str, str]] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("| H"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if len(cells) < 2:
            continue
        hid = cells[0]
        if hid not in EXPECTED_HIDS:
            continue
        # Stash the rest of the row's cells; we look them up by header in
        # caller code by indexing positions. We dedupe by hid so the
        # first occurrence wins (multiple tables may quote the same row).
        if hid in rows:
            continue
        rows[hid] = {f"col_{i}": cell for i, cell in enumerate(cells)}
    return rows


# ── individual checks ────────────────────────────────────────────────


def check_verdicts_hids_match_skeleton(
    *,
    verdicts_csv: Path | None = None,
    skeleton_md: Path | None = None,
) -> list[IntegrityIssue]:
    """The 7 H rows in the verdicts CSV must match those in skeleton.md."""
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    skeleton_md = skeleton_md or _default_skeleton_md()
    issues: list[IntegrityIssue] = []

    df = _read_csv_safe(verdicts_csv)
    if df is None or "hid" not in df.columns:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="hypothesis_set",
                description=(
                    f"verdicts CSV not available at {_relative(verdicts_csv)}; "
                    "skipping skeleton hid cross-check."
                ),
                fix_command="index-inclusion-cma",
            )
        )
        return issues

    text = _read_text_safe(skeleton_md)
    if text is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="hypothesis_set",
                description=(
                    f"paper skeleton not available at {_relative(skeleton_md)}; "
                    "skipping skeleton hid cross-check."
                ),
                fix_command="index-inclusion-paper-skeleton --force",
            )
        )
        return issues

    csv_hids = set(df["hid"].astype(str).tolist())
    skeleton_hids = set(_extract_h_rows_from_markdown(text).keys())
    missing_from_skeleton = sorted(csv_hids - skeleton_hids)
    extra_in_skeleton = sorted(skeleton_hids - csv_hids)
    if missing_from_skeleton or extra_in_skeleton:
        evidence: list[str] = []
        if missing_from_skeleton:
            evidence.append(f"in CSV but missing in skeleton: {missing_from_skeleton}")
        if extra_in_skeleton:
            evidence.append(f"in skeleton but missing in CSV: {extra_in_skeleton}")
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="hypothesis_set",
                description=(
                    "Hypothesis set in cma_hypothesis_verdicts.csv does not "
                    "match the H rows rendered in paper/skeleton.md."
                ),
                evidence=tuple(evidence),
                fix_command="index-inclusion-paper-skeleton --force",
            )
        )
    elif csv_hids != set(EXPECTED_HIDS):
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="hypothesis_set",
                description=(
                    f"verdicts CSV has hids {sorted(csv_hids)}, expected "
                    f"{list(EXPECTED_HIDS)}."
                ),
                fix_command="index-inclusion-cma",
            )
        )
    return issues


def check_verdicts_hids_match_methodology(
    *,
    verdicts_csv: Path | None = None,
    methodology_md: Path | None = None,
) -> list[IntegrityIssue]:
    """The 7 H rows in the verdicts CSV must match those in methodology_summary.md."""
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    methodology_md = methodology_md or _default_methodology_md()
    issues: list[IntegrityIssue] = []

    df = _read_csv_safe(verdicts_csv)
    if df is None or "hid" not in df.columns:
        return issues  # already surfaced by the skeleton sibling check

    text = _read_text_safe(methodology_md)
    if text is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="hypothesis_set",
                description=(
                    f"methodology summary not available at {_relative(methodology_md)}; "
                    "skipping methodology hid cross-check."
                ),
                fix_command="index-inclusion-methodology-summary",
            )
        )
        return issues

    csv_hids = set(df["hid"].astype(str).tolist())
    methodology_hids = set(_extract_h_rows_from_markdown(text).keys())
    missing = sorted(csv_hids - methodology_hids)
    extra = sorted(methodology_hids - csv_hids)
    if missing or extra:
        evidence: list[str] = []
        if missing:
            evidence.append(f"in CSV but missing in methodology: {missing}")
        if extra:
            evidence.append(f"in methodology but missing in CSV: {extra}")
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="hypothesis_set",
                description=(
                    "Hypothesis set in cma_hypothesis_verdicts.csv does not "
                    "match the H rows rendered in paper/methodology_summary.md."
                ),
                evidence=tuple(evidence),
                fix_command="index-inclusion-methodology-summary",
            )
        )
    return issues


_FIGURE_REGEX = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")


def check_figures_referenced_exist(
    *,
    skeleton_md: Path | None = None,
    figures_dir: Path | None = None,
) -> list[IntegrityIssue]:
    """Every figure referenced in skeleton.md must exist in results/figures/."""
    skeleton_md = skeleton_md or _default_skeleton_md()
    figures_dir = figures_dir or _default_figures_dir()
    issues: list[IntegrityIssue] = []

    text = _read_text_safe(skeleton_md)
    if text is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="figures",
                description=(
                    f"paper skeleton not available at {_relative(skeleton_md)}; "
                    "skipping figure cross-check."
                ),
                fix_command="index-inclusion-paper-skeleton --force",
            )
        )
        return issues

    missing: list[str] = []
    for match in _FIGURE_REGEX.finditer(text):
        url = match.group(1).strip()
        # Skeleton uses relative paths like ../results/figures/...
        # Resolve against the skeleton's parent directory.
        candidate = (skeleton_md.parent / url).resolve()
        if not candidate.exists():
            missing.append(url)
    if missing:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="figures",
                description=(
                    f"{len(missing)} figure(s) referenced in paper/skeleton.md "
                    "do not exist on disk."
                ),
                evidence=tuple(missing),
                fix_command=(
                    "index-inclusion-make-figures-tables && "
                    "index-inclusion-paper-skeleton --force"
                ),
            )
        )
    return issues


def check_pap_classifications_match_public_summary(
    *,
    pap_report_csv: Path | None = None,
    public_summary_json: Path | None = None,
) -> list[IntegrityIssue]:
    """PAP deviation per-row classifications must match public-summary counts."""
    pap_report_csv = pap_report_csv or _default_pap_report_csv()
    public_summary_json = public_summary_json or _default_public_summary_json()
    issues: list[IntegrityIssue] = []

    df = _read_csv_safe(pap_report_csv)
    if df is None or "classification" not in df.columns:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="pap",
                description=(
                    f"PAP deviation report not available at {_relative(pap_report_csv)}; "
                    "skipping PAP classification cross-check."
                ),
                fix_command="index-inclusion-pap-diff",
            )
        )
        return issues

    payload = _read_json_safe(public_summary_json)
    if payload is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="pap",
                description=(
                    f"public summary JSON not available at {_relative(public_summary_json)}; "
                    "skipping PAP classification cross-check."
                ),
                fix_command="index-inclusion-export-public-summary",
            )
        )
        return issues

    summary = payload.get("pap_deviation_summary")
    if not isinstance(summary, dict):
        issues.append(
            IntegrityIssue(
                severity="warn",
                category="pap",
                description=(
                    "public summary JSON is missing the 'pap_deviation_summary' "
                    "block; cannot cross-check against the PAP report CSV."
                ),
                fix_command="index-inclusion-export-public-summary",
            )
        )
        return issues

    csv_counts = (
        df["classification"].astype(str).str.strip().value_counts().to_dict()
    )
    public_counts = {
        "unchanged": int(summary.get("unchanged_count", 0)),
        "flipped": int(summary.get("flipped_count", 0)),
        "tightened": int(summary.get("tightened_count", 0)),
        "weakened": int(summary.get("weakened_count", 0)),
        "unverifiable": int(summary.get("unverifiable_count", 0)),
    }
    mismatches: list[str] = []
    for kind, want in public_counts.items():
        have = int(csv_counts.get(kind, 0))
        if want != have:
            mismatches.append(
                f"{kind}: pap_report_csv={have} vs public_summary={want}"
            )
    if mismatches:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="pap",
                description=(
                    "PAP deviation per-row classifications in "
                    "pap_deviation_report.csv disagree with the counts in "
                    "data/public/index_research_summary.json."
                ),
                evidence=tuple(mismatches),
                fix_command=(
                    "index-inclusion-pap-diff && "
                    "index-inclusion-export-public-summary"
                ),
            )
        )
    return issues


def check_console_scripts_count_matches_readme(
    *,
    pyproject_toml: Path | None = None,
    readme_md: Path | None = None,
) -> list[IntegrityIssue]:
    """The CLI badge in README must match the actual ``[project.scripts]`` count."""
    pyproject_toml = pyproject_toml or _default_pyproject_toml()
    readme_md = readme_md or _default_readme_md()
    issues: list[IntegrityIssue] = []

    if not pyproject_toml.exists():
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="cli_count",
                description=f"pyproject.toml missing at {_relative(pyproject_toml)}.",
            )
        )
        return issues
    try:
        data = tomllib.loads(pyproject_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="cli_count",
                description=f"pyproject.toml unreadable: {exc}",
            )
        )
        return issues
    scripts = data.get("project", {}).get("scripts", {})
    actual_count = len(scripts)

    text = _read_text_safe(readme_md)
    if text is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="cli_count",
                description=(
                    f"README not available at {_relative(readme_md)}; "
                    "skipping CLI badge cross-check."
                ),
            )
        )
        return issues

    badge_match = re.search(
        r"!\[CLI\]\(https://img\.shields\.io/badge/CLI-(\d+)%20commands",
        text,
    )
    if badge_match is None:
        issues.append(
            IntegrityIssue(
                severity="warn",
                category="cli_count",
                description=(
                    "README.md does not contain a recognizable CLI commands "
                    "badge; cannot verify against pyproject.toml."
                ),
            )
        )
        return issues
    badge_count = int(badge_match.group(1))
    if badge_count != actual_count:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="cli_count",
                description=(
                    f"README CLI badge claims {badge_count} commands but "
                    f"pyproject.toml has {actual_count} entries in "
                    "[project.scripts]."
                ),
                evidence=(f"badge={badge_count}", f"actual={actual_count}"),
                fix_command=(
                    "Update the README badge URL "
                    "`![CLI](.../CLI-<n>%20commands-...)` to match pyproject.toml."
                ),
            )
        )
    return issues


def check_paper_library_referenced_in_skeleton(
    *,
    skeleton_md: Path | None = None,
) -> list[IntegrityIssue]:
    """The 16 papers in ``literature_catalog.PAPER_LIBRARY`` must all appear
    as ``paper_id=`` mentions in the skeleton's references section."""
    skeleton_md = skeleton_md or _default_skeleton_md()
    issues: list[IntegrityIssue] = []

    try:
        from index_inclusion_research.literature_catalog import PAPER_LIBRARY
    except ImportError as exc:  # pragma: no cover - defensive
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="references",
                description=f"literature catalog failed to import: {exc}",
            )
        )
        return issues

    catalog_ids = {p.paper_id for p in PAPER_LIBRARY}
    if len(catalog_ids) != EXPECTED_PAPER_LIBRARY_COUNT:
        issues.append(
            IntegrityIssue(
                severity="warn",
                category="references",
                description=(
                    f"literature_catalog.PAPER_LIBRARY has {len(catalog_ids)} "
                    f"papers, expected {EXPECTED_PAPER_LIBRARY_COUNT}."
                ),
            )
        )

    text = _read_text_safe(skeleton_md)
    if text is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="references",
                description=(
                    f"paper skeleton not available at {_relative(skeleton_md)}; "
                    "skipping references cross-check."
                ),
                fix_command="index-inclusion-paper-skeleton --force",
            )
        )
        return issues

    mentioned = set(re.findall(r"paper_id=([a-z0-9_]+)", text))
    missing = sorted(catalog_ids - mentioned)
    if missing:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="references",
                description=(
                    f"{len(missing)} paper(s) in literature_catalog.PAPER_LIBRARY "
                    "are not cited in paper/skeleton.md §References."
                ),
                evidence=tuple(missing),
                fix_command="index-inclusion-paper-skeleton --force",
            )
        )
    return issues


def check_sample_sizes_match_methodology(
    *,
    verdicts_csv: Path | None = None,
    methodology_md: Path | None = None,
) -> list[IntegrityIssue]:
    """Sample sizes (n_obs) in methodology summary must match the verdicts CSV."""
    verdicts_csv = verdicts_csv or _default_verdicts_csv()
    methodology_md = methodology_md or _default_methodology_md()
    issues: list[IntegrityIssue] = []

    df = _read_csv_safe(verdicts_csv)
    if df is None or "n_obs" not in df.columns or "hid" not in df.columns:
        return issues  # surfaced elsewhere
    text = _read_text_safe(methodology_md)
    if text is None:
        return issues  # surfaced elsewhere

    csv_n: dict[str, int] = {}
    for _, row in df.iterrows():
        try:
            csv_n[str(row["hid"])] = int(float(row["n_obs"]))
        except (TypeError, ValueError):
            continue

    rows = _extract_h_rows_from_markdown(text)
    mismatches: list[str] = []
    for hid in EXPECTED_HIDS:
        cells = rows.get(hid)
        if not cells:
            continue
        # In methodology_summary.md the row is: | hid | name | n_obs | tier | track |
        # so n_obs is col_2.
        n_text = cells.get("col_2", "").strip()
        if not n_text:
            continue
        try:
            n_methodology = int(n_text)
        except ValueError:
            mismatches.append(
                f"{hid}: methodology n cell is non-numeric ({n_text!r})"
            )
            continue
        n_csv = csv_n.get(hid)
        if n_csv is None:
            continue
        if n_methodology != n_csv:
            mismatches.append(
                f"{hid}: methodology n={n_methodology} vs verdicts CSV n={n_csv}"
            )
    if mismatches:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="sample_sizes",
                description=(
                    "Sample sizes (n_obs) in paper/methodology_summary.md "
                    "disagree with cma_hypothesis_verdicts.csv."
                ),
                evidence=tuple(mismatches),
                fix_command="index-inclusion-methodology-summary",
            )
        )
    return issues


def check_sensitivity_coverage_match(
    *,
    methodology_md: Path | None = None,
    public_summary_json: Path | None = None,
) -> list[IntegrityIssue]:
    """Sensitivity stable counts in methodology must match index_research_summary.json."""
    methodology_md = methodology_md or _default_methodology_md()
    public_summary_json = public_summary_json or _default_public_summary_json()
    issues: list[IntegrityIssue] = []

    text = _read_text_safe(methodology_md)
    payload = _read_json_safe(public_summary_json)
    if text is None or payload is None:
        return issues  # surfaced elsewhere
    sensitivity = payload.get("sensitivity_robustness")
    if not isinstance(sensitivity, dict):
        return issues

    # Methodology table:
    #   | 阈值 | 0.05 / ... | 7/7 |
    #   | AR 引擎 | adjusted / market | 5/7 |
    #   | 联合 | 8 cells = ... | 5/7 |
    def _stable_pair_for(label: str) -> str | None:
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith(f"| {label} |"):
                cells = [c.strip() for c in stripped.strip("|").split("|")]
                if len(cells) >= 3:
                    return cells[-1]
        return None

    expected_pairs = {
        "阈值": sensitivity.get("threshold_axis", {}),
        "AR 引擎": sensitivity.get("ar_engine_axis", {}),
        "联合": sensitivity.get("two_dimensional", {}),
    }
    mismatches: list[str] = []
    for label, axis in expected_pairs.items():
        if not isinstance(axis, dict):
            continue
        stable = axis.get("stable_count")
        cell_count = axis.get("cell_count")
        if stable is None:
            continue
        # methodology card uses the format "stable_count/7" (denominator = 7 hids)
        # but earlier versions might use cell_count; accept either of two valid forms.
        actual = _stable_pair_for(label)
        if actual is None:
            mismatches.append(f"{label}: no row found in methodology summary")
            continue
        expected_options = {
            f"{int(stable)}/7",
            f"{int(stable)}/{int(cell_count) if cell_count else 7}",
        }
        if actual not in expected_options:
            mismatches.append(
                f"{label}: methodology says {actual!r}, public summary "
                f"stable={int(stable)} cell_count={cell_count}"
            )
    if mismatches:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="sensitivity",
                description=(
                    "Sensitivity stability counts in paper/methodology_summary.md "
                    "disagree with data/public/index_research_summary.json."
                ),
                evidence=tuple(mismatches),
                fix_command=(
                    "index-inclusion-export-public-summary && "
                    "index-inclusion-methodology-summary"
                ),
            )
        )
    return issues


def check_doctor_checks_listed_in_cli_reference(
    *,
    cli_reference_md: Path | None = None,
) -> list[IntegrityIssue]:
    """Doctor check count referenced in cli_reference should match DEFAULT_CHECKS."""
    cli_reference_md = cli_reference_md or _default_cli_reference_md()
    issues: list[IntegrityIssue] = []

    try:
        from index_inclusion_research.doctor import DEFAULT_CHECKS
    except ImportError as exc:  # pragma: no cover - defensive
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="doctor",
                description=f"doctor module failed to import: {exc}",
            )
        )
        return issues
    actual = len(DEFAULT_CHECKS)

    text = _read_text_safe(cli_reference_md)
    if text is None:
        issues.append(
            IntegrityIssue(
                severity="info",
                category="doctor",
                description=(
                    f"cli_reference.md not available at {_relative(cli_reference_md)}; "
                    "skipping doctor count cross-check."
                ),
            )
        )
        return issues

    # Surface a soft warn so doc-drift doesn't gate publishing — but the
    # info is here so the user can refresh the cli_reference number.
    # Look for "N doctor checks" or "N health checks" pattern.
    counts_in_doc = re.findall(r"(\d+)\s*(?:doctor|health)\s*checks?", text)
    if not counts_in_doc:
        return issues  # nothing to check
    distinct = {int(c) for c in counts_in_doc}
    if actual not in distinct:
        issues.append(
            IntegrityIssue(
                severity="warn",
                category="doctor",
                description=(
                    f"cli_reference.md mentions doctor check counts "
                    f"{sorted(distinct)} but DEFAULT_CHECKS has {actual}."
                ),
                fix_command=(
                    f"Update docs/cli_reference.md to reference {actual} doctor checks."
                ),
            )
        )
    return issues


def check_skeleton_pap_matches_report(
    *,
    skeleton_md: Path | None = None,
    pap_report_csv: Path | None = None,
) -> list[IntegrityIssue]:
    """The PAP table in skeleton.md must agree with pap_deviation_report.csv per hid."""
    skeleton_md = skeleton_md or _default_skeleton_md()
    pap_report_csv = pap_report_csv or _default_pap_report_csv()
    issues: list[IntegrityIssue] = []

    df = _read_csv_safe(pap_report_csv)
    if df is None or "classification" not in df.columns:
        return issues  # surfaced elsewhere
    text = _read_text_safe(skeleton_md)
    if text is None:
        return issues  # surfaced elsewhere

    # Build expected by hid.
    expected: dict[str, str] = {
        str(r["hid"]): str(r["classification"]).strip() for _, r in df.iterrows()
    }
    rows = _extract_h_rows_from_markdown(text)
    mismatches: list[str] = []
    pap_keywords = {"unchanged", "flipped", "tightened", "weakened", "unverifiable"}
    for hid, exp_cls in expected.items():
        cells = rows.get(hid)
        if not cells:
            continue
        # Search every cell of this hid row for a PAP keyword token.
        cell_values = list(cells.values())
        found: str | None = None
        for cell in cell_values:
            tokens = re.split(r"[\s|]+", cell)
            for token in tokens:
                if token in pap_keywords:
                    found = token
                    break
            if found:
                break
        if found is None:
            continue  # this row isn't a PAP row (it's the verdict table)
        if found != exp_cls:
            mismatches.append(
                f"{hid}: skeleton classifies as {found!r}, "
                f"pap_deviation_report.csv has {exp_cls!r}"
            )
    if mismatches:
        issues.append(
            IntegrityIssue(
                severity="fail",
                category="pap",
                description=(
                    "PAP classifications in paper/skeleton.md disagree with "
                    "pap_deviation_report.csv."
                ),
                evidence=tuple(mismatches),
                fix_command=(
                    "index-inclusion-pap-diff && "
                    "index-inclusion-paper-skeleton --force"
                ),
            )
        )
    return issues


DEFAULT_INTEGRITY_CHECKS: tuple[
    Callable[..., list[IntegrityIssue]], ...
] = (
    check_verdicts_hids_match_skeleton,
    check_verdicts_hids_match_methodology,
    check_figures_referenced_exist,
    check_pap_classifications_match_public_summary,
    check_console_scripts_count_matches_readme,
    check_paper_library_referenced_in_skeleton,
    check_sample_sizes_match_methodology,
    check_sensitivity_coverage_match,
    check_doctor_checks_listed_in_cli_reference,
    check_skeleton_pap_matches_report,
)


# ── orchestrator + rendering ─────────────────────────────────────────


def check_paper_integrity(
    root: Path | None = None,
    *,
    checks: Sequence[Callable[..., list[IntegrityIssue]]] | None = None,
) -> list[IntegrityIssue]:
    """Run all configured cross-document checks and return aggregated issues.

    Each check is invoked with no arguments — they look up their own
    defaults via :mod:`paths`. ``root`` is accepted to keep the public
    signature aligned with the spec, but the per-check files key off
    ``paths.project_root()`` which respects the
    ``INDEX_INCLUSION_ROOT`` env var.
    """
    if root is not None:
        # Honour an explicit root by exporting it for the duration of the
        # call. The check helpers read paths.project_root() each time so
        # setting the env var is the cleanest seam without rewiring every
        # default closure.
        import os

        prev = os.environ.get("INDEX_INCLUSION_ROOT")
        os.environ["INDEX_INCLUSION_ROOT"] = str(root)
        try:
            return check_paper_integrity(root=None, checks=checks)
        finally:
            if prev is None:
                os.environ.pop("INDEX_INCLUSION_ROOT", None)
            else:
                os.environ["INDEX_INCLUSION_ROOT"] = prev

    checks = checks or DEFAULT_INTEGRITY_CHECKS
    issues: list[IntegrityIssue] = []
    for check in checks:
        try:
            issues.extend(check())
        except Exception as exc:  # noqa: BLE001 — diagnostics, not crashes
            logger.exception("integrity check %s raised", check.__name__)
            issues.append(
                IntegrityIssue(
                    severity="fail",
                    category="internal",
                    description=(
                        f"{check.__name__} raised {type(exc).__name__}: {exc}"
                    ),
                )
            )
    return issues


def integrity_summary(issues: Sequence[IntegrityIssue]) -> dict[str, int]:
    return {
        "info": sum(1 for i in issues if i.severity == "info"),
        "warn": sum(1 for i in issues if i.severity == "warn"),
        "fail": sum(1 for i in issues if i.severity == "fail"),
        "total": len(issues),
    }


def integrity_exit_code(
    issues: Sequence[IntegrityIssue],
    *,
    fail_on_warn: bool = False,
) -> int:
    """Map issue severities to a CLI exit code.

    - exit 2 if any fail-level issue exists.
    - exit 1 if any warn-level issue (or fail with --fail-on-warn).
    - exit 0 otherwise.
    """
    summary = integrity_summary(issues)
    if summary["fail"] > 0:
        return 2
    if fail_on_warn and summary["warn"] > 0:
        return 1
    if summary["warn"] > 0:
        return 1
    return 0


def render_text(issues: Sequence[IntegrityIssue], *, color: bool = True) -> str:
    summary = integrity_summary(issues)
    lines: list[str] = []
    lines.append("=" * 64)
    lines.append(" INDEX-INCLUSION-RESEARCH · paper-integrity")
    lines.append("=" * 64)
    lines.append(
        f"  {summary['info']} info · {summary['warn']} warn · "
        f"{summary['fail']} fail · {summary['total']} issue(s)"
    )
    lines.append("")
    if not issues:
        lines.append("  (all cross-document checks passed)")
        lines.append("")
        return "\n".join(lines).rstrip() + "\n"
    for issue in issues:
        glyph = _SEVERITY_GLYPH[issue.severity]
        if color:
            head = f"  {_SEVERITY_COLOR[issue.severity]}{glyph}{_RESET}  [{issue.category}] {issue.description}"
        else:
            head = f"  {glyph}  [{issue.category}] {issue.description}"
        lines.append(head)
        for ev in issue.evidence:
            lines.append(f"        - {ev}")
        if issue.fix_command:
            lines.append(f"      → {issue.fix_command}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_json(issues: Sequence[IntegrityIssue]) -> str:
    payload = {
        "summary": integrity_summary(issues),
        "issues": [
            {
                "severity": i.severity,
                "category": i.category,
                "description": i.description,
                "evidence": list(i.evidence),
                "fix_command": i.fix_command,
            }
            for i in issues
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def render_markdown(issues: Sequence[IntegrityIssue]) -> str:
    summary = integrity_summary(issues)
    lines: list[str] = [
        "# Paper integrity report",
        "",
        f"- info: **{summary['info']}**",
        f"- warn: **{summary['warn']}**",
        f"- fail: **{summary['fail']}**",
        f"- total: **{summary['total']}** issue(s)",
        "",
    ]
    if not issues:
        lines.append("All cross-document integrity checks passed.")
        lines.append("")
        return "\n".join(lines)
    lines.append("| Severity | Category | Description | Fix |")
    lines.append("|---|---|---|---|")
    for issue in issues:
        # Escape pipes in descriptions to keep the markdown table valid.
        desc = issue.description.replace("|", "\\|")
        fix = issue.fix_command.replace("|", "\\|")
        lines.append(
            f"| {issue.severity} | {issue.category} | {desc} | "
            f"`{fix}` |" if fix else
            f"| {issue.severity} | {issue.category} | {desc} | |"
        )
    lines.append("")
    return "\n".join(lines)


# ── doctor adapter ───────────────────────────────────────────────────


def check_paper_integrity_doctor():  # pragma: no cover - thin adapter
    """Doctor adapter: surface integrity issues as a single ``CheckResult``.

    Imports :mod:`doctor` lazily to avoid the obvious circular import
    (doctor wants to call us, we want doctor's ``CheckResult`` type).
    """
    from index_inclusion_research.doctor import CheckResult

    issues = check_paper_integrity()
    summary = integrity_summary(issues)
    name = "paper_integrity"
    if summary["fail"] > 0:
        return CheckResult(
            name=name,
            status="fail",
            message=(
                f"paper-integrity gate has {summary['fail']} fail / "
                f"{summary['warn']} warn / {summary['info']} info."
            ),
            fix=(
                "Run `index-inclusion-paper-integrity` for details, then "
                "re-run the offending generator(s)."
            ),
            details=tuple(
                f"[{i.category}] {i.description}"
                for i in issues
                if i.severity == "fail"
            ),
        )
    if summary["warn"] > 0:
        return CheckResult(
            name=name,
            status="warn",
            message=(
                f"paper-integrity gate has {summary['warn']} warn / "
                f"{summary['info']} info."
            ),
            fix=(
                "Run `index-inclusion-paper-integrity` for details."
            ),
            details=tuple(
                f"[{i.category}] {i.description}"
                for i in issues
                if i.severity == "warn"
            ),
        )
    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"paper-integrity cross-document gate passes "
            f"({summary['info']} info-level note(s))."
        ),
    )


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Cross-document consistency gate for the paper bundle. "
            "Verifies that verdicts CSV / paper skeleton / methodology "
            "summary / PAP report / public summary / README all agree."
        )
    )
    parser.add_argument(
        "--format",
        choices=("text", "json", "markdown"),
        default="text",
        help="Choose output renderer.",
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
            "Treat warn-level issues as enough to exit 1. Useful for CI."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    issues = check_paper_integrity()
    if args.format == "json":
        sys.stdout.write(render_json(issues))
    elif args.format == "markdown":
        sys.stdout.write(render_markdown(issues))
    else:
        enable_color = not args.no_color and sys.stdout.isatty()
        sys.stdout.write(render_text(issues, color=enable_color))
    return integrity_exit_code(issues, fail_on_warn=args.fail_on_warn)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
