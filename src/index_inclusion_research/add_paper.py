"""Interactive add-paper CLI: extend the 16-paper literature library safely.

The :mod:`literature_catalog` package stores the canonical paper registry
as a frozen ``PAPER_LIBRARY`` tuple in ``_data.py``. Adding a new paper
manually means editing six different artifacts — ``_data.py``,
``paper/references.bib``, the citation graph CSV / figure twin, the paper
skeleton's §References, the methodology summary's top-5 centrality, and
``data/public/index_research_summary.json`` — while keeping the
paper-integrity gate green.

This module wraps that surgery in a single console script. The
:func:`add_paper` programmatic API mutates ``_data.py`` (textual edit,
not import-time), appends a ``@article`` BibTeX entry, then triggers the
downstream regenerators so every cross-artifact reference (``paper_id``
mentions in the skeleton, BibTeX cite-key, citation-graph node) lands in
the same commit. The :func:`interactive_add_paper` companion prompts on
stdin for use when the researcher is at a terminal.

Design contract:

- **Mandatory fields enforced** via :class:`NewPaper` ``__post_init__``.
  Missing ``paper_id`` / ``authors`` / ``year`` / ``title`` / ``position`` /
  ``market_focus`` → :class:`AddPaperError`. Optional ``journal`` /
  ``abstract`` / ``methodology`` / ``related_paper_ids`` fall back to
  ``[TODO: ...]`` placeholders rather than being fabricated.
- **paper_id discipline.** Must match ``[a-z][a-z0-9_]*`` (lowercase
  underscore_only). Duplicates of an existing ``paper_id`` are rejected
  with a helpful error so re-runs are idempotent / never silently double.
- **related_paper_ids discipline.** Related ids accept a list/tuple or
  comma-separated string, but must be unique and must not reference the new
  paper itself. Every related id must already exist in ``PAPER_LIBRARY``;
  unknown ids are rejected before any catalog / BibTeX write so typos cannot
  be persisted into paper-integrity state.
- **Catalog mutation is text-edit.** We rewrite ``_data.py`` with a new
  ``LiteraturePaper(...)`` literal inserted by a lexicographic scan of the
  existing thematic tuple. Import-time mutation would be impossible
  (frozen dataclass + tuple) and would not survive a Python restart.
  Text-edit produces a reviewable diff.
- **Dry-run** prints the planned diff and exits without writing. Useful
  before committing real catalog growth.
- **--skip-downstream** updates ``PAPER_LIBRARY`` and ``references.bib``
  but skips citation graph / paper-summary re-rendering. Use for batch
  adds — call the regenerators once at the end.
- **No paper-integrity gate is run by default.** The caller decides when
  the cross-artifact checks should fire (typically after the final add
  in a batch). The CLI exposes ``--run-integrity`` to opt in.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from index_inclusion_research import paths

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


PAPER_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
YEAR_RE = re.compile(r"^(?:18|19|20|21)\d{2}$")

VALID_POSITIONS = ("pro_index_effect", "contra", "neutral")
POSITION_TO_STANCE = {
    "pro_index_effect": "正方",
    "contra": "反方",
    "neutral": "中性",
}

VALID_MARKET_FOCUS = ("US", "CN", "both")
MARKET_FOCUS_TO_LABEL = {
    "US": "美国",
    "CN": "中国",
    "both": "跨市场",
}

VALID_METHODOLOGIES = ("event_study", "regression", "RDD", "other")
METHODOLOGY_TO_METHOD_FOCUS = {
    "event_study": "事件研究",
    "regression": "回归分析",
    "RDD": "断点回归",
    "other": "其他",
}

VALID_RESEARCH_THREADS = ("price_pressure", "demand_curve", "identification")
THREAD_TO_PROJECT_MODULE = {
    "price_pressure": "短期价格压力",
    "demand_curve": "需求曲线效应",
    "identification": "沪深300论文复现",
}

DEFAULT_CAMP = "市场摩擦与效应重估"
VALID_CAMPS = (
    "创世之战",
    "正方深化",
    "市场摩擦与效应重估",
    "方法革命",
    "中国A股主战场",
)

PLACEHOLDER_LITERAL = "[TODO: not provided]"

JSON_TEMPLATE: dict[str, Any] = {
    "paper_id": "greenwood_sammon_2024",
    "authors": "Robin Greenwood; Marco Sammon",
    "year": "2024",
    "title": "The Disappearing Index Effect (Extended Sample)",
    "position": "contra",
    "market_focus": "US",
    "journal": "Journal of Finance (working paper)",
    "abstract": "One-sentence core logic or abstract excerpt.",
    "methodology": "event_study",
    "research_thread": "price_pressure",
    "related_paper_ids": ["greenwood_sammon_2022", "shleifer_1986"],
    "camp": DEFAULT_CAMP,
}

JSON_TEMPLATE_FIELDS: tuple[str, ...] = tuple(JSON_TEMPLATE)
REQUIRED_JSON_FIELDS: tuple[str, ...] = (
    "paper_id",
    "authors",
    "year",
    "title",
    "position",
    "market_focus",
)


# ---------------------------------------------------------------------------
# Errors and data classes
# ---------------------------------------------------------------------------


class AddPaperError(ValueError):
    """Raised when paper data fails validation or write-back fails."""

    def __init__(
        self,
        message: str,
        *,
        report: AddPaperReport | None = None,
    ) -> None:
        super().__init__(message)
        self.report = report


def _normalize_related_paper_ids(value: object) -> tuple[str, ...]:
    """Normalize and validate accepted ``related_paper_ids`` input shapes."""
    if isinstance(value, str):
        candidates = (
            piece.strip()
            for piece in value.split(",")
            if piece.strip()
        )
        normalized: list[str] = []
        for cleaned in candidates:
            if not PAPER_ID_RE.match(cleaned):
                raise AddPaperError(
                    f"invalid related_paper_id {cleaned!r}: must match "
                    "[a-z][a-z0-9_]*."
                )
            normalized.append(cleaned)
        return _reject_duplicate_related_paper_ids(tuple(normalized))

    if not isinstance(value, (list, tuple)):
        raise AddPaperError(
            "related_paper_ids must be a list/tuple of paper_id strings or "
            "a comma-separated string."
        )

    normalized = []
    for index, raw in enumerate(value):
        field = f"related_paper_ids[{index}]"
        if not isinstance(raw, str):
            raise AddPaperError(
                f"invalid {field}: expected string paper_id, got "
                f"{type(raw).__name__}."
            )
        cleaned = raw.strip()
        if not cleaned:
            raise AddPaperError(
                f"invalid {field}: expected non-empty string paper_id."
            )
        if not PAPER_ID_RE.match(cleaned):
            raise AddPaperError(
                f"invalid {field} {cleaned!r}: must match [a-z][a-z0-9_]*."
            )
        normalized.append(cleaned)
    return _reject_duplicate_related_paper_ids(tuple(normalized))


def _reject_duplicate_related_paper_ids(
    related_paper_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Reject repeated related paper ids while preserving input order."""
    seen: set[str] = set()
    for paper_id in related_paper_ids:
        if paper_id in seen:
            raise AddPaperError(
                "related_paper_ids contains duplicate paper_id "
                f"{paper_id!r}; values must be unique."
            )
        seen.add(paper_id)
    return related_paper_ids


@dataclass
class NewPaper:
    """A typed wrapper for the inputs the add-paper CLI collects.

    Mandatory: ``paper_id``, ``authors``, ``year``, ``title``,
    ``position``, ``market_focus``. The remaining fields fall back to
    ``[TODO: ...]`` placeholders so the catalog never gets fabricated
    content.
    """

    paper_id: str
    authors: str
    year: str
    title: str
    position: str
    market_focus: str
    journal: str = ""
    abstract: str = ""
    methodology: str = "other"
    research_thread: str = "identification"
    related_paper_ids: tuple[str, ...] = ()
    camp: str = DEFAULT_CAMP

    def __post_init__(self) -> None:
        for field_name in (
            "paper_id",
            "authors",
            "year",
            "title",
            "position",
            "market_focus",
            "journal",
            "abstract",
            "methodology",
            "research_thread",
            "camp",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise AddPaperError(
                    f"invalid {field_name}: expected string, got "
                    f"{type(value).__name__}."
                )
            setattr(self, field_name, value.strip())

        missing: list[str] = []
        for field_name in ("paper_id", "authors", "year", "title"):
            value = getattr(self, field_name)
            if not value:
                missing.append(field_name)
        if missing:
            raise AddPaperError(
                f"missing mandatory field(s): {', '.join(missing)}"
            )
        # paper_id must be lowercase_underscore (no spaces, no uppercase).
        if not PAPER_ID_RE.match(self.paper_id):
            raise AddPaperError(
                f"invalid paper_id {self.paper_id!r}: must match "
                "[a-z][a-z0-9_]* (lowercase, starts with letter, "
                "underscore-separated)."
            )
        if not YEAR_RE.match(self.year):
            raise AddPaperError(
                f"invalid year {self.year!r}: must be a four-digit year "
                "between 1800 and 2199."
            )
        if self.position not in VALID_POSITIONS:
            raise AddPaperError(
                f"invalid position {self.position!r}: must be one of "
                f"{', '.join(VALID_POSITIONS)}."
            )
        if self.market_focus not in VALID_MARKET_FOCUS:
            raise AddPaperError(
                f"invalid market_focus {self.market_focus!r}: must be "
                f"one of {', '.join(VALID_MARKET_FOCUS)}."
            )
        if self.methodology not in VALID_METHODOLOGIES:
            raise AddPaperError(
                f"invalid methodology {self.methodology!r}: must be one "
                f"of {', '.join(VALID_METHODOLOGIES)}."
            )
        if self.research_thread not in VALID_RESEARCH_THREADS:
            raise AddPaperError(
                f"invalid research_thread {self.research_thread!r}: "
                f"must be one of {', '.join(VALID_RESEARCH_THREADS)}."
            )
        if self.camp not in VALID_CAMPS:
            raise AddPaperError(
                f"invalid camp {self.camp!r}: must be one of "
                f"{', '.join(VALID_CAMPS)}."
            )
        # Use object.__setattr__ to support frozen-like reassignment.
        self.related_paper_ids = _normalize_related_paper_ids(
            self.related_paper_ids
        )
        if self.paper_id in self.related_paper_ids:
            raise AddPaperError(
                "related_paper_ids self-reference is not allowed: current "
                f"paper_id {self.paper_id!r} appears in related_paper_ids."
            )


@dataclass
class AddPaperReport:
    """Structured report describing what an add-paper invocation did.

    Returned by both :func:`add_paper` and :func:`interactive_add_paper`.
    The CLI prints a human summary; tests inspect fields directly.
    """

    paper_id: str
    catalog_path: Path
    catalog_updated: bool = False
    bibtex_path: Path | None = None
    bibtex_updated: bool = False
    downstream_artifacts: tuple[str, ...] = ()
    paper_integrity_exit_code: int | None = None
    dry_run: bool = False
    skipped_downstream: bool = False
    paper_library_count_before: int = 0
    paper_library_count_after: int = 0
    related_paper_ids: tuple[str, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)

    def render_summary(self) -> str:
        lines = [
            f"paper_id            : {self.paper_id}",
            f"catalog_path        : {self.catalog_path}",
            f"catalog_updated     : {self.catalog_updated}",
            f"bibtex_updated      : {self.bibtex_updated}",
            f"dry_run             : {self.dry_run}",
            f"skipped_downstream  : {self.skipped_downstream}",
            f"PAPER_LIBRARY count : {self.paper_library_count_before} → "
            f"{self.paper_library_count_after}",
        ]
        if self.downstream_artifacts:
            lines.append(
                "downstream artifacts: "
                + ", ".join(self.downstream_artifacts)
            )
        if self.paper_integrity_exit_code is not None:
            lines.append(
                f"paper_integrity rc : {self.paper_integrity_exit_code}"
            )
        if self.related_paper_ids:
            lines.append(
                "related_paper_ids   : "
                + ", ".join(self.related_paper_ids)
            )
        if self.notes:
            lines.append("notes:")
            for note in self.notes:
                lines.append(f"  - {note}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Defaults / path helpers
# ---------------------------------------------------------------------------


def _default_catalog_path() -> Path:
    return (
        paths.project_root()
        / "src"
        / "index_inclusion_research"
        / "literature_catalog"
        / "_data.py"
    )


def _default_bibtex_path() -> Path:
    return paths.project_root() / "paper" / "references.bib"


# ---------------------------------------------------------------------------
# Catalog text-edit
# ---------------------------------------------------------------------------


_LIBRARY_OPEN = "PAPER_LIBRARY: tuple[LiteraturePaper, ...] = ("
_LIBRARY_CLOSE = "\n)\n"


def _list_existing_paper_ids(catalog_text: str) -> list[str]:
    """Extract the ``paper_id=...`` literals already present in PAPER_LIBRARY."""
    library_start = catalog_text.find(_LIBRARY_OPEN)
    if library_start == -1:
        raise AddPaperError(
            "could not locate PAPER_LIBRARY tuple in catalog source — "
            "the literature_catalog/_data.py layout has changed; refusing "
            "to edit."
        )
    # The tuple ends at the first ``\n)\n`` after library_start.
    library_end = catalog_text.find(_LIBRARY_CLOSE, library_start)
    if library_end == -1:
        raise AddPaperError(
            "could not locate end of PAPER_LIBRARY tuple in catalog source — "
            "the literature_catalog/_data.py layout has changed; refusing "
            "to edit."
        )
    body = catalog_text[library_start:library_end]
    return re.findall(r'paper_id="([a-z][a-z0-9_]*)"', body)


def _render_literature_paper_literal(paper: NewPaper) -> str:
    """Render a NewPaper as a ``LiteraturePaper(...)`` Python source literal.

    Matches the style of the existing 16 entries: kwarg-per-line, 4-space
    indent. ``related_paper_ids`` is emitted as a tuple literal when
    non-empty, ``()`` otherwise.
    """
    stance = POSITION_TO_STANCE[paper.position]
    market_label = MARKET_FOCUS_TO_LABEL[paper.market_focus]
    method_focus = METHODOLOGY_TO_METHOD_FOCUS[paper.methodology]
    project_module = THREAD_TO_PROJECT_MODULE[paper.research_thread]
    market_focus_full = (
        market_label if not paper.journal.strip()
        else f"{market_label} / {paper.journal.strip()}"
    )

    related_segment = _render_related_paper_ids(paper.related_paper_ids)

    abstract_or_placeholder = (
        paper.abstract.strip() if paper.abstract.strip() else PLACEHOLDER_LITERAL
    )

    lines = [
        "    LiteraturePaper(",
        f"        paper_id={_py_string_literal(paper.paper_id)},",
        f"        stance={_py_string_literal(stance)},",
        f"        camp={_py_string_literal(paper.camp)},",
        f"        title={_py_string_literal(paper.title)},",
        f"        authors={_py_string_literal(paper.authors)},",
        f"        year_label={_py_string_literal(paper.year)},",
        f"        market_focus={_py_string_literal(market_focus_full)},",
        f"        method_focus={_py_string_literal(method_focus)},",
        f"        project_module={_py_string_literal(project_module)},",
        f"        relevance_note={_py_string_literal(PLACEHOLDER_LITERAL)},",
        f"        core_logic={_py_string_literal(abstract_or_placeholder)},",
        f"        one_line_role={_py_string_literal(PLACEHOLDER_LITERAL)},",
        f"        practical_use={_py_string_literal(PLACEHOLDER_LITERAL)},",
        f"        pdf_path=PDF_ROOT / {_py_string_literal(f'{paper.paper_id}.pdf')},",
        related_segment,
        "    ),",
    ]
    return "\n".join(lines)


def _render_related_paper_ids(related: Sequence[str]) -> str:
    if not related:
        return "        related_paper_ids=(),"
    if len(related) == 1:
        return (
            "        related_paper_ids=("
            + _py_string_literal(related[0])
            + ",),"
        )
    inner = ",\n".join(f"            {_py_string_literal(rid)}" for rid in related)
    return (
        "        related_paper_ids=(\n"
        + inner
        + ",\n        ),"
    )


def _py_string_literal(value: str) -> str:
    """Render ``value`` as a safe Python source string literal."""
    return json.dumps(value, ensure_ascii=False)


def _insert_paper_into_catalog(
    catalog_text: str,
    paper: NewPaper,
    *,
    existing_ids: Sequence[str],
) -> str:
    """Return ``catalog_text`` with the new LiteraturePaper inserted.

    The existing 16 entries are NOT alphabetical (they're thematic), so
    insertion keeps their current order intact: find the first scanned
    entry whose paper_id sorts after the new one, and insert before it.
    If the new paper_id sorts after every existing one, append at the end
    of the tuple.
    """
    library_start = catalog_text.find(_LIBRARY_OPEN)
    library_end = catalog_text.find(_LIBRARY_CLOSE, library_start)
    if library_start == -1 or library_end == -1:
        raise AddPaperError(
            "could not locate PAPER_LIBRARY tuple boundaries — refusing to edit."
        )

    new_literal = _render_literature_paper_literal(paper)

    # Walk the existing entries to find an insertion point. We use the
    # ``paper_id="<id>",`` marker on each LiteraturePaper(...) block.
    paper_id_lines = list(
        re.finditer(r'        paper_id="([a-z][a-z0-9_]*)",', catalog_text)
    )
    insert_before_match: re.Match[str] | None = None
    for match in paper_id_lines:
        if match.group(1) > paper.paper_id:
            insert_before_match = match
            break

    if insert_before_match is None:
        # Append at the end of the tuple. The tuple closes with "\n)\n";
        # we want to insert just before the closing ")".
        # library_end points at the "\n)\n" — insert at library_end.
        return (
            catalog_text[:library_end]
            + "\n"
            + new_literal
            + catalog_text[library_end:]
        )

    # Insert before the ``    LiteraturePaper(`` block that owns
    # ``insert_before_match``. Each entry begins at the line containing
    # "    LiteraturePaper(" — find it by stepping back from the match.
    block_start = catalog_text.rfind(
        "    LiteraturePaper(", 0, insert_before_match.start()
    )
    if block_start == -1:
        raise AddPaperError(
            "could not locate LiteraturePaper( block start for "
            f"{insert_before_match.group(1)!r} — refusing to edit."
        )
    return (
        catalog_text[:block_start]
        + new_literal
        + "\n"
        + catalog_text[block_start:]
    )


# ---------------------------------------------------------------------------
# BibTeX append
# ---------------------------------------------------------------------------


_BIBTEX_LATEX_ESCAPES = {
    "\\": r"\textbackslash{}",
    "{": r"\textbraceleft{}",
    "}": r"\textbraceright{}",
    "&": r"\&",
    "%": r"\%",
    "#": r"\#",
    "$": r"\$",
    "_": r"\_",
}


def _bibtex_escape(text: str) -> str:
    rendered: list[str] = []
    last_was_space = False
    for ch in text:
        if ch in "\r\n\t" or (ord(ch) < 32):
            if not last_was_space:
                rendered.append(" ")
                last_was_space = True
            continue
        escaped = _BIBTEX_LATEX_ESCAPES.get(ch, ch)
        rendered.append(escaped)
        last_was_space = ch.isspace()
    return "".join(rendered).strip()


def _render_bibtex_entry(paper: NewPaper) -> str:
    authors_bib = _bibtex_escape(paper.authors.replace(";", " and"))
    title_bib = _bibtex_escape(paper.title)
    journal = paper.journal.strip() if paper.journal.strip() else "[TODO: journal]"
    journal_bib = _bibtex_escape(journal) if paper.journal.strip() else journal
    market_label = MARKET_FOCUS_TO_LABEL[paper.market_focus]
    method_focus = METHODOLOGY_TO_METHOD_FOCUS[paper.methodology]
    note_bib = _bibtex_escape(
        f"{market_label}; {method_focus}; camp={paper.camp}"
    )
    return (
        "@article{" + paper.paper_id + ",\n"
        "  author    = {" + authors_bib + "},\n"
        "  title     = {" + title_bib + "},\n"
        "  year      = {" + paper.year + "},\n"
        "  journal   = {" + journal_bib + "},\n"
        "  note      = {" + note_bib + "},\n"
        "}\n"
    )


def _append_to_bibtex(bibtex_path: Path, paper: NewPaper) -> bool:
    """Append ``paper`` to ``bibtex_path``. Skip if cite-key already present."""
    entry = _render_bibtex_entry(paper)
    if bibtex_path.exists():
        existing = bibtex_path.read_text(encoding="utf-8")
        if f"@article{{{paper.paper_id}," in existing:
            return False
        sep = "" if existing.endswith("\n\n") else (
            "\n" if existing.endswith("\n") else "\n\n"
        )
        bibtex_path.write_text(existing + sep + entry, encoding="utf-8")
    else:
        bibtex_path.parent.mkdir(parents=True, exist_ok=True)
        bibtex_path.write_text(entry, encoding="utf-8")
    return True


# ---------------------------------------------------------------------------
# Downstream regeneration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DownstreamRunResult:
    """Result of running downstream generators after a catalog write."""

    artifacts: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()


def _record_downstream_step(
    label: str,
    run_step: Callable[[], int | None],
    *,
    triggered: list[str],
    failures: list[str],
) -> None:
    try:
        rc = run_step()
    except SystemExit as exc:
        if exc.code in (0, None):
            triggered.append(label)
        else:
            failures.append(f"{label} exited with {exc.code}")
    except Exception as exc:  # noqa: BLE001 - report exact downstream failure
        failures.append(f"{label} failed: {type(exc).__name__}: {exc}")
    else:
        if rc in (0, None):
            triggered.append(label)
        else:
            failures.append(f"{label} exited with {rc}")


def _regenerate_downstream(
    *,
    invalidate_cache: bool = True,
) -> DownstreamRunResult:
    """Trigger the downstream regenerators that depend on PAPER_LIBRARY.

    Returns succeeded artifact labels plus failures. The caller turns any
    failures into a non-zero CLI result because the catalog/BibTeX writes
    have already happened and partial downstream state should not look
    successful. The citation-graph render is the most expensive step
    (matplotlib figure + PDF + CSV) — keep it last so the cheap
    regenerators run even if matplotlib is unavailable in a test env.
    """
    triggered: list[str] = []
    failures: list[str] = []

    # Reload _data so the in-memory PAPER_LIBRARY reflects the textual
    # edit (Python imports cache modules; without reload the existing
    # process still sees the 16-entry frozen tuple).
    if invalidate_cache:
        def reload_catalog() -> None:
            import importlib

            from index_inclusion_research import literature_catalog
            from index_inclusion_research.literature_catalog import _data

            importlib.reload(_data)
            importlib.reload(literature_catalog)

        _record_downstream_step(
            "literature_catalog_reload",
            reload_catalog,
            triggered=triggered,
            failures=failures,
        )

    # 1. paper_skeleton regenerates §References
    def run_paper_skeleton() -> int | None:
        from index_inclusion_research import paper_skeleton

        return paper_skeleton.main(["--force"])

    _record_downstream_step(
        "paper_skeleton",
        run_paper_skeleton,
        triggered=triggered,
        failures=failures,
    )

    # 2. methodology_summary regenerates the top-5 centrality table
    def run_methodology_summary() -> int | None:
        from index_inclusion_research import methodology_summary

        return methodology_summary.main([])

    _record_downstream_step(
        "methodology_summary",
        run_methodology_summary,
        triggered=triggered,
        failures=failures,
    )

    # 3. export_public_summary regenerates papers_indexed
    def run_export_public_summary() -> int | None:
        from index_inclusion_research import export_public_summary

        return export_public_summary.main([])

    _record_downstream_step(
        "export_public_summary",
        run_export_public_summary,
        triggered=triggered,
        failures=failures,
    )

    # 4. citation_graph (last — expensive matplotlib render)
    def run_citation_graph() -> int | None:
        from index_inclusion_research import citation_graph

        return citation_graph.main([])

    _record_downstream_step(
        "citation_graph",
        run_citation_graph,
        triggered=triggered,
        failures=failures,
    )

    return DownstreamRunResult(tuple(triggered), tuple(failures))


def _run_paper_integrity() -> int:
    from index_inclusion_research import paper_integrity

    return int(paper_integrity.main(["--fail-on-warn"]))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def add_paper(
    paper_data: dict | NewPaper,
    *,
    catalog_path: Path | None = None,
    bibtex_path: Path | None = None,
    dry_run: bool = False,
    skip_downstream: bool = False,
    run_integrity: bool = False,
) -> AddPaperReport:
    """Append a new paper to the literature library, idempotent on paper_id.

    ``paper_data`` is either a :class:`NewPaper` or a dict matching its
    fields. Missing mandatory fields → :class:`AddPaperError`. Duplicate
    ``paper_id`` (already in PAPER_LIBRARY) → :class:`AddPaperError`.

    Default behavior: edit ``literature_catalog/_data.py``, append a
    BibTeX entry, then regenerate the downstream artifacts. ``dry_run``
    skips all writes. ``skip_downstream`` writes the catalog + BibTeX but
    does not call the regenerators (use for batch adds). ``run_integrity``
    runs ``paper_integrity --fail-on-warn`` after writes and propagates a
    non-zero result.
    """
    catalog_path = catalog_path or _default_catalog_path()
    bibtex_path = bibtex_path or _default_bibtex_path()

    paper = (
        paper_data
        if isinstance(paper_data, NewPaper)
        else NewPaper(**paper_data)
    )

    if not catalog_path.exists():
        raise AddPaperError(
            f"catalog source not found at {catalog_path} — refusing to edit."
        )
    catalog_text = catalog_path.read_text(encoding="utf-8")
    existing_ids = _list_existing_paper_ids(catalog_text)
    count_before = len(existing_ids)

    if paper.paper_id in existing_ids:
        raise AddPaperError(
            f"paper_id {paper.paper_id!r} already exists in PAPER_LIBRARY "
            f"({count_before} entries). To update an existing entry, edit "
            f"{catalog_path} directly."
        )

    # Validate related ids against the current catalog before any write.
    unknown_related = [
        rid for rid in paper.related_paper_ids
        if rid not in existing_ids and rid != paper.paper_id
    ]
    if unknown_related:
        raise AddPaperError(
            "related_paper_ids reference unknown paper_id(s): "
            + ", ".join(unknown_related)
            + ". Add those papers first or remove the unresolved relation."
        )
    notes: list[str] = []

    new_catalog_text = _insert_paper_into_catalog(
        catalog_text, paper, existing_ids=existing_ids
    )

    report = AddPaperReport(
        paper_id=paper.paper_id,
        catalog_path=catalog_path,
        bibtex_path=bibtex_path,
        dry_run=dry_run,
        skipped_downstream=skip_downstream,
        paper_library_count_before=count_before,
        paper_library_count_after=count_before + 1,
        related_paper_ids=paper.related_paper_ids,
        notes=tuple(notes),
    )

    if dry_run:
        report.notes = report.notes + (
            f"DRY-RUN: would write {len(new_catalog_text) - len(catalog_text)} "
            "added characters to catalog",
            f"DRY-RUN: would append BibTeX entry for {paper.paper_id}",
        )
        return report

    catalog_path.write_text(new_catalog_text, encoding="utf-8")
    report.catalog_updated = True
    report.bibtex_updated = _append_to_bibtex(bibtex_path, paper)

    if skip_downstream:
        report.notes = report.notes + (
            "skipped downstream regeneration (--skip-downstream); "
            "run `index-inclusion-paper-skeleton --force && "
            "index-inclusion-methodology-summary && "
            "index-inclusion-export-public-summary && "
            "index-inclusion-citation-graph` to catch up.",
        )
        if run_integrity:
            report.paper_integrity_exit_code = _run_paper_integrity()
            if report.paper_integrity_exit_code != 0:
                report.notes = report.notes + (
                    "paper-integrity returned non-zero after catalog/BibTeX "
                    "writes; downstream regeneration was skipped.",
                )
                raise AddPaperError(
                    "paper-integrity failed after partial add-paper writes "
                    f"(exit {report.paper_integrity_exit_code}).",
                    report=report,
                )
        return report

    downstream = _regenerate_downstream()
    report.downstream_artifacts = downstream.artifacts
    if downstream.failures:
        report.notes = report.notes + (
            "downstream regeneration failed after catalog/BibTeX writes: "
            + "; ".join(downstream.failures),
        )
        raise AddPaperError(
            "downstream regeneration failed after partial add-paper writes.",
            report=report,
        )
    if run_integrity:
        report.paper_integrity_exit_code = _run_paper_integrity()
        if report.paper_integrity_exit_code != 0:
            report.notes = report.notes + (
                "paper-integrity returned non-zero after catalog/BibTeX "
                "and downstream writes.",
            )
            raise AddPaperError(
                "paper-integrity failed after add-paper writes "
                f"(exit {report.paper_integrity_exit_code}).",
                report=report,
            )
    return report


# ---------------------------------------------------------------------------
# Interactive (TTY) flow
# ---------------------------------------------------------------------------


def _prompt(
    label: str,
    *,
    default: str | None = None,
    choices: Sequence[str] | None = None,
    required: bool = False,
    input_fn=input,
) -> str:
    """Prompt the user once and return their (stripped) answer.

    If ``choices`` is given, the prompt rejects anything not in the
    enumeration and re-asks until valid (or up to 5 attempts to keep
    non-interactive misuse from looping forever).
    """
    suffix = ""
    if choices:
        suffix += f" [{'/'.join(choices)}]"
    if default is not None:
        suffix += f" (default: {default})"
    for _ in range(5):
        raw = input_fn(f"{label}{suffix}: ").strip()
        if not raw and default is not None:
            return default
        if not raw:
            if required:
                print(f"  ! {label} is required.", file=sys.stderr)
                continue
            return ""
        if choices and raw not in choices:
            print(
                f"  ! {raw!r} is not one of {', '.join(choices)}",
                file=sys.stderr,
            )
            continue
        return raw
    raise AddPaperError(
        f"too many invalid attempts for {label!r}; aborting."
    )


def interactive_add_paper(
    *,
    catalog_path: Path | None = None,
    bibtex_path: Path | None = None,
    dry_run: bool = False,
    skip_downstream: bool = False,
    run_integrity: bool = False,
    input_fn=input,
) -> AddPaperReport:
    """TTY prompt flow that gathers a NewPaper and forwards to add_paper.

    The order matches the natural reading order of a paper:
    paper_id → authors → year → title → journal → market_focus →
    methodology → position → research_thread → related_paper_ids.
    """
    print("=== index-inclusion-add-paper (interactive) ===")
    print(
        "Add a paper to the literature library. Mandatory fields are "
        "marked with [*]; press Enter to accept a default.\n"
    )

    paper_id = _prompt(
        "paper_id [*] (lowercase_underscore, e.g. greenwood_sammon_2024)",
        required=True,
        input_fn=input_fn,
    )
    authors = _prompt(
        "authors [*] (semicolon-separated, e.g. Robin Greenwood; Marco Sammon)",
        required=True,
        input_fn=input_fn,
    )
    year = _prompt("year [*] (e.g. 2024)", required=True, input_fn=input_fn)
    title = _prompt("title [*]", required=True, input_fn=input_fn)
    journal = _prompt("journal (optional)", default="", input_fn=input_fn)
    market_focus = _prompt(
        "market_focus [*]",
        choices=VALID_MARKET_FOCUS,
        required=True,
        input_fn=input_fn,
    )
    methodology = _prompt(
        "methodology",
        choices=VALID_METHODOLOGIES,
        default="other",
        input_fn=input_fn,
    )
    position = _prompt(
        "position [*]",
        choices=VALID_POSITIONS,
        required=True,
        input_fn=input_fn,
    )
    research_thread = _prompt(
        "research_thread",
        choices=VALID_RESEARCH_THREADS,
        default="identification",
        input_fn=input_fn,
    )
    related_raw = _prompt(
        "related_paper_ids (comma-separated, e.g. shleifer_1986,harris_gurel_1986)",
        default="",
        input_fn=input_fn,
    )
    abstract = _prompt(
        "abstract / core_logic (one line, optional)",
        default="",
        input_fn=input_fn,
    )

    related: tuple[str, ...] = ()
    if related_raw:
        related = tuple(
            piece.strip() for piece in related_raw.split(",") if piece.strip()
        )

    paper = NewPaper(
        paper_id=paper_id,
        authors=authors,
        year=year,
        title=title,
        journal=journal,
        market_focus=market_focus,
        methodology=methodology,
        position=position,
        research_thread=research_thread,
        related_paper_ids=related,
        abstract=abstract,
    )
    return add_paper(
        paper,
        catalog_path=catalog_path,
        bibtex_path=bibtex_path,
        dry_run=dry_run,
        skip_downstream=skip_downstream,
        run_integrity=run_integrity,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def render_json_template() -> str:
    """Return a copy-pasteable ``--from-json`` starter payload.

    This mode is intentionally side-effect free: it does not import or read the
    live catalog, and it never prompts. Researchers can save the printed JSON,
    edit the fields, then run ``index-inclusion-add-paper --from-json``.
    """

    return json.dumps(JSON_TEMPLATE, ensure_ascii=False, indent=2) + "\n"


def _reject_duplicate_json_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while rejecting duplicate keys explicitly."""

    seen: set[str] = set()
    duplicates: list[str] = []
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen and key not in duplicates:
            duplicates.append(key)
        seen.add(key)
        payload[key] = value
    if duplicates:
        raise AddPaperError(
            "--from-json contains duplicate field(s): "
            + ", ".join(sorted(duplicates))
            + ". Remove duplicate keys so the payload is unambiguous."
        )
    return payload


def _load_from_json(path: Path) -> NewPaper:
    """Load a NewPaper from a JSON file (non-interactive add)."""
    if not path.exists():
        raise AddPaperError(f"--from-json file not found: {path}")
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_object_pairs,
        )
    except json.JSONDecodeError as exc:
        raise AddPaperError(f"--from-json file is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise AddPaperError(
            f"--from-json file must contain a JSON object at top level "
            f"(got {type(payload).__name__})."
        )
    unexpected = sorted(set(payload) - set(JSON_TEMPLATE_FIELDS))
    if unexpected:
        raise AddPaperError(
            "--from-json contains unexpected field(s): "
            + ", ".join(unexpected)
            + ". Supported fields: "
            + ", ".join(JSON_TEMPLATE_FIELDS)
            + "."
        )
    missing = [field for field in REQUIRED_JSON_FIELDS if field not in payload]
    if missing:
        raise AddPaperError(
            "--from-json missing mandatory field(s): " + ", ".join(missing)
        )
    payload["related_paper_ids"] = _normalize_related_paper_ids(
        payload.get("related_paper_ids", ())
    )
    return NewPaper(**payload)


def main(argv: list[str] | None = None) -> int:
    """``index-inclusion-add-paper`` entry point.

    Three modes:

    - **interactive** (default): prompt on stdin for each field.
    - **--from-json paper.json**: load fields from JSON (non-interactive).
    - **--dry-run**: show what would change, don't write.
    """
    parser = argparse.ArgumentParser(
        prog="index-inclusion-add-paper",
        description=(
            "Append a new academic paper to the literature catalog and "
            "keep all downstream artifacts (BibTeX, citation graph, paper "
            "skeleton §References, public summary) in sync."
        ),
    )
    parser.add_argument(
        "--from-json",
        type=Path,
        default=None,
        metavar="PATH",
        help="Read paper fields from a JSON file instead of prompting.",
    )
    parser.add_argument(
        "--print-json-template",
        action="store_true",
        help="Print a side-effect-free starter JSON payload for --from-json and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing anything.",
    )
    parser.add_argument(
        "--skip-downstream",
        action="store_true",
        help=(
            "Update PAPER_LIBRARY and append BibTeX, but skip expensive "
            "downstream regenerators — use for batch adds (then run the "
            "regenerators once at the end)."
        ),
    )
    parser.add_argument(
        "--run-integrity",
        action="store_true",
        help=(
            "Run index-inclusion-paper-integrity --fail-on-warn after writes "
            "and return non-zero if the gate fails."
        ),
    )
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=None,
        help="Override the catalog source path (default: literature_catalog/_data.py).",
    )
    parser.add_argument(
        "--bibtex-path",
        type=Path,
        default=None,
        help="Override the BibTeX path (default: paper/references.bib).",
    )
    args = parser.parse_args(argv)

    if args.print_json_template:
        print(render_json_template(), end="")
        return 0

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        if args.from_json is not None:
            paper = _load_from_json(args.from_json)
            report = add_paper(
                paper,
                catalog_path=args.catalog_path,
                bibtex_path=args.bibtex_path,
                dry_run=args.dry_run,
                skip_downstream=args.skip_downstream,
                run_integrity=args.run_integrity,
            )
        else:
            report = interactive_add_paper(
                catalog_path=args.catalog_path,
                bibtex_path=args.bibtex_path,
                dry_run=args.dry_run,
                skip_downstream=args.skip_downstream,
                run_integrity=args.run_integrity,
            )
    except AddPaperError as exc:
        if exc.report is not None:
            print(exc.report.render_summary(), file=sys.stderr)
        print(f"add-paper error: {exc}", file=sys.stderr)
        return 2

    print(report.render_summary())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
