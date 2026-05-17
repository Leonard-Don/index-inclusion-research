"""Tests for ``index_inclusion_research.add_paper``.

We exercise the public API (:func:`add_paper`) against a *copy* of the
real ``literature_catalog/_data.py`` placed in ``tmp_path``. Mutating the
real catalog in tests would be unsafe (the live ``PAPER_LIBRARY`` would
gain a phantom 17th entry that bleeds into every other test, and the
paper-integrity gate would start failing). Using a tmp copy keeps the
test isolated and lets us re-parse the resulting source to verify the
17th entry landed correctly.

Test plan (focused cases):

1. Add 17th paper → ``paper_library_count_after == 17`` and the
   appended ``LiteraturePaper(...)`` literal contains the new paper_id.
2. Dry-run shows what would change but writes nothing.
3. Invalid paper_id (uppercase, spaces) → :class:`AddPaperError`.
4. Duplicate paper_id (already in PAPER_LIBRARY) → :class:`AddPaperError`
   with a helpful "already exists" message.
5. Mandatory fields enforced (missing ``authors``) → :class:`AddPaperError`.
6. ``skip_downstream`` touches catalog + BibTeX but skips expensive
   downstream artifacts.
7. BibTeX entry is appended with the project ``paper_id`` as cite-key
   and rejects duplicate cite-keys.
8. ``--from-json`` payload path: load + validate + add round-trip.
"""

from __future__ import annotations

import ast
import json
import re
import shutil
from pathlib import Path

import pytest

from index_inclusion_research import add_paper as ap

REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_DATA_PY = (
    REPO_ROOT
    / "src"
    / "index_inclusion_research"
    / "literature_catalog"
    / "_data.py"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_catalog(tmp_path: Path) -> Path:
    """Return a tmp_path copy of the real literature_catalog/_data.py.

    Tests mutate this copy, so the live PAPER_LIBRARY is never touched.
    """
    target = tmp_path / "_data.py"
    shutil.copy(REAL_DATA_PY, target)
    return target


@pytest.fixture
def tmp_bibtex(tmp_path: Path) -> Path:
    """Return a tmp_path bibtex with a couple of existing entries.

    Mirrors the on-disk ``paper/references.bib`` shape so the cite-key
    duplicate-rejection path is exercised against realistic input.
    """
    target = tmp_path / "references.bib"
    target.write_text(
        "@article{shleifer_1986,\n"
        "  author    = {Andrei Shleifer},\n"
        "  title     = {Do Demand Curves for Stocks Slope Down?},\n"
        "  year      = {1986},\n"
        "  journal   = {[TODO: journal]},\n"
        "  note      = {美国 / S\\&P 500; 需求曲线, 非完全替代; camp=创世之战},\n"
        "}\n",
        encoding="utf-8",
    )
    return target


def _sample_paper_data(**overrides) -> dict:
    """A 2024 paper that doesn't collide with any of the existing 16 IDs.

    Greenwood-Sammon 2024 "The Disappearing Index Effect" (extension of
    their 2022 piece) is a realistic candidate the user might actually
    add when the thesis citing it lands. We rename to ``_2024`` so it
    doesn't collide with the existing ``greenwood_sammon_2022`` entry.
    """
    base = {
        "paper_id": "greenwood_sammon_2024",
        "authors": "Robin Greenwood; Marco Sammon",
        "year": "2024",
        "title": "The Disappearing Index Effect (Extended Sample)",
        "position": "contra",
        "market_focus": "US",
        "methodology": "event_study",
        "research_thread": "price_pressure",
        "journal": "Journal of Finance (working paper)",
        "abstract": (
            "Extended sample through 2023 shows the SP500 inclusion "
            "abnormal return has further compressed."
        ),
        "related_paper_ids": ("greenwood_sammon_2022", "shleifer_1986"),
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_add_17th_paper_grows_paper_library(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """A successful add reports 16→17 and the new literal lands in source."""
    report = ap.add_paper(
        _sample_paper_data(),
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )
    assert report.paper_library_count_before == 16
    assert report.paper_library_count_after == 17
    assert report.catalog_updated is True
    assert report.paper_id == "greenwood_sammon_2024"

    updated_text = tmp_catalog.read_text(encoding="utf-8")
    assert 'paper_id="greenwood_sammon_2024"' in updated_text
    # The new literal should sit inside the PAPER_LIBRARY tuple, not in a
    # comment or docstring — confirm by counting paper_id="..." occurrences.
    paper_id_count = len(
        re.findall(r'        paper_id="[a-z][a-z0-9_]*",', updated_text)
    )
    assert paper_id_count == 17


def test_dry_run_makes_no_writes(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """``dry_run=True`` reports the planned change but leaves files alone."""
    catalog_before = tmp_catalog.read_text(encoding="utf-8")
    bibtex_before = tmp_bibtex.read_text(encoding="utf-8")

    report = ap.add_paper(
        _sample_paper_data(),
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        dry_run=True,
    )

    assert report.dry_run is True
    assert report.catalog_updated is False
    assert report.bibtex_updated is False
    assert report.paper_library_count_after == 17  # reported, not realized
    assert any("DRY-RUN" in note for note in report.notes)

    assert tmp_catalog.read_text(encoding="utf-8") == catalog_before
    assert tmp_bibtex.read_text(encoding="utf-8") == bibtex_before


@pytest.mark.parametrize(
    "bad_paper_id",
    [
        "Greenwood_Sammon_2024",  # uppercase
        "greenwood sammon 2024",  # space
        "2024_greenwood",  # starts with digit
        "greenwood-sammon-2024",  # hyphen
        "",  # empty
    ],
)
def test_invalid_paper_id_rejected(
    tmp_catalog: Path, tmp_bibtex: Path, bad_paper_id: str
) -> None:
    """paper_id must match ``[a-z][a-z0-9_]*`` (lowercase_underscore)."""
    with pytest.raises(ap.AddPaperError):
        ap.add_paper(
            _sample_paper_data(paper_id=bad_paper_id),
            catalog_path=tmp_catalog,
            bibtex_path=tmp_bibtex,
            skip_downstream=True,
        )


def test_duplicate_paper_id_rejected_with_helpful_message(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """Adding an existing paper_id surfaces a clear ``already exists`` error."""
    duplicate_data = _sample_paper_data(paper_id="shleifer_1986")
    with pytest.raises(ap.AddPaperError) as excinfo:
        ap.add_paper(
            duplicate_data,
            catalog_path=tmp_catalog,
            bibtex_path=tmp_bibtex,
            skip_downstream=True,
        )
    msg = str(excinfo.value)
    assert "shleifer_1986" in msg
    assert "already exists" in msg


def test_mandatory_fields_enforced() -> None:
    """``NewPaper.__post_init__`` rejects missing mandatory fields."""
    with pytest.raises(ap.AddPaperError) as excinfo:
        ap.NewPaper(
            paper_id="something_2024",
            authors="",  # missing
            year="2024",
            title="A title",
            position="pro_index_effect",
            market_focus="US",
        )
    assert "authors" in str(excinfo.value)

    # Invalid position too
    with pytest.raises(ap.AddPaperError):
        ap.NewPaper(
            paper_id="something_2024",
            authors="J. Doe",
            year="2024",
            title="A title",
            position="for",  # not in VALID_POSITIONS
            market_focus="US",
        )

    # Invalid market_focus too
    with pytest.raises(ap.AddPaperError):
        ap.NewPaper(
            paper_id="something_2024",
            authors="J. Doe",
            year="2024",
            title="A title",
            position="contra",
            market_focus="EU",  # not in VALID_MARKET_FOCUS
        )


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("year", "2024},\n  journal = {pwned}"),
        ("year", "twenty"),
        ("year", "1799"),
        ("year", "2200"),
        ("camp", "市场摩擦与效应重估\npwned=True"),
        ("related_paper_ids", ("shleifer_1986\",),\n    pwned=True",)),
        ("related_paper_ids", ("Bad_ID",)),
        ("related_paper_ids", (123,)),
    ],
)
def test_adversarial_structured_fields_are_rejected(
    field: str, bad_value: object
) -> None:
    """Structured fields cannot inject Python/BibTeX syntax."""
    with pytest.raises(ap.AddPaperError):
        ap.NewPaper(**_sample_paper_data(**{field: bad_value}))


def test_python_source_literal_rendering_escapes_adversarial_text(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """Text fields may contain hostile JSON strings without corrupting _data.py."""
    payload = _sample_paper_data(
        title='A "quoted" title\nwith braces } and slash \\',
        authors='Alice Example; Bob "Quote"\nMallory',
        journal='Journal },\n    injected=True',
        abstract='First line\nsecond line with control \x01 and "quote"',
        camp="方法革命",
        related_paper_ids="greenwood_sammon_2022, shleifer_1986",
    )

    ap.add_paper(
        payload,
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )

    updated_text = tmp_catalog.read_text(encoding="utf-8")
    parsed = ast.parse(updated_text)
    call = next(
        node
        for node in ast.walk(parsed)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "LiteraturePaper"
        and any(
            kw.arg == "paper_id"
            and isinstance(kw.value, ast.Constant)
            and kw.value.value == "greenwood_sammon_2024"
            for kw in node.keywords
        )
    )
    values = {
        kw.arg: ast.literal_eval(kw.value)
        for kw in call.keywords
        if kw.arg in {"title", "authors", "camp", "core_logic", "related_paper_ids"}
    }
    assert values["title"] == 'A "quoted" title\nwith braces } and slash \\'
    assert values["authors"] == 'Alice Example; Bob "Quote"\nMallory'
    assert values["camp"] == "方法革命"
    assert values["core_logic"] == 'First line\nsecond line with control \x01 and "quote"'
    assert values["related_paper_ids"] == (
        "greenwood_sammon_2022",
        "shleifer_1986",
    )


def test_skip_downstream_updates_catalog_and_bibtex_only(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """``skip_downstream=True`` writes catalog + bib, but no regenerators."""
    bibtex_before = tmp_bibtex.read_text(encoding="utf-8")

    report = ap.add_paper(
        _sample_paper_data(),
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )

    assert report.catalog_updated is True
    assert report.skipped_downstream is True
    assert report.bibtex_updated is True
    assert report.downstream_artifacts == ()
    # The note should tell the user how to catch up.
    assert any("skipped downstream" in note for note in report.notes)
    # BibTeX is still maintained when skip_downstream is set.
    bibtex_after = tmp_bibtex.read_text(encoding="utf-8")
    assert bibtex_after != bibtex_before
    assert "@article{greenwood_sammon_2024," in bibtex_after


def test_bibtex_append_idempotent_on_cite_key(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """Appending the BibTeX entry uses paper_id as cite-key and skips duplicates."""
    # First add: skip_downstream still appends BibTeX.
    report1 = ap.add_paper(
        _sample_paper_data(),
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )
    paper = ap.NewPaper(**_sample_paper_data())

    bibtex_text = tmp_bibtex.read_text(encoding="utf-8")
    assert "@article{greenwood_sammon_2024," in bibtex_text
    assert "Robin Greenwood and Marco Sammon" in bibtex_text

    # Second call with the same cite-key → False (no double-write).
    appended_again = ap._append_to_bibtex(tmp_bibtex, paper)
    assert appended_again is False

    # The bibtex still has exactly one matching entry, not two.
    occurrences = bibtex_text.count("@article{greenwood_sammon_2024,")
    assert occurrences == 1
    assert report1.paper_id == "greenwood_sammon_2024"


def test_bibtex_escape_blocks_field_injection() -> None:
    """Braces, slashes, controls and newlines cannot create extra fields."""
    paper = ap.NewPaper(
        **_sample_paper_data(
            title="Safe},\n  year      = {1999},\n  note      = {pwned}",
            authors="Alice {A}\\B\nMallory; Eve_Example",
            journal="Journal},\n  evil      = {field}",
            abstract="not used in bibtex",
        )
    )

    entry = ap._render_bibtex_entry(paper)

    assert entry.count("\n  year      = {") == 1
    assert "\n  evil" not in entry
    assert "\n  note      = {pwned}" not in entry
    assert r"Safe\textbraceright{}" in entry
    assert r"\textbraceleft{}" in entry
    assert r"\textbackslash{}" in entry
    assert "Eve\\_Example" in entry


def test_from_json_roundtrip_loads_and_adds(
    tmp_catalog: Path, tmp_bibtex: Path, tmp_path: Path
) -> None:
    """``--from-json`` accepts a JSON file with the same fields as NewPaper."""
    payload = _sample_paper_data()
    json_path = tmp_path / "new_paper.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    loaded = ap._load_from_json(json_path)
    assert isinstance(loaded, ap.NewPaper)
    assert loaded.paper_id == "greenwood_sammon_2024"
    assert loaded.related_paper_ids == (
        "greenwood_sammon_2022",
        "shleifer_1986",
    )

    # The full add_paper round-trip works the same as a dict input.
    report = ap.add_paper(
        loaded,
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )
    assert report.paper_library_count_after == 17

    # Bad JSON (not an object) → AddPaperError, not a JSON crash.
    bad_path = tmp_path / "bad.json"
    bad_path.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ap.AddPaperError):
        ap._load_from_json(bad_path)


def test_alphabetical_insertion_position(
    tmp_catalog: Path, tmp_bibtex: Path
) -> None:
    """A new paper uses a lexicographic insertion scan.

    The existing tuple is thematic, not globally alphabetical. The add
    path therefore preserves the existing tuple and inserts before the
    first scanned paper_id that sorts after the new one, or at the tail.
    """
    early_paper = _sample_paper_data(paper_id="aaa_test_2024")
    ap.add_paper(
        early_paper,
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )
    text = tmp_catalog.read_text(encoding="utf-8")
    paper_ids_in_order = re.findall(
        r'        paper_id="([a-z][a-z0-9_]*)",', text
    )
    assert paper_ids_in_order[0] == "aaa_test_2024"

    # Now a lex-max entry — it should land at the tail.
    late_paper = _sample_paper_data(paper_id="zzz_test_2024")
    ap.add_paper(
        late_paper,
        catalog_path=tmp_catalog,
        bibtex_path=tmp_bibtex,
        skip_downstream=True,
    )
    text = tmp_catalog.read_text(encoding="utf-8")
    paper_ids_in_order = re.findall(
        r'        paper_id="([a-z][a-z0-9_]*)",', text
    )
    assert paper_ids_in_order[-1] == "zzz_test_2024"
    assert paper_ids_in_order[0] == "aaa_test_2024"
    assert len(paper_ids_in_order) == 18  # 16 original + 2 added
    assert paper_ids_in_order != sorted(paper_ids_in_order)


def test_cli_main_returns_nonzero_on_validation_error(
    tmp_catalog: Path, tmp_bibtex: Path, tmp_path: Path, capsys
) -> None:
    """``main()`` returns exit code 2 when validation rejects the input."""
    bad_json = tmp_path / "bad.json"
    bad_json.write_text(
        json.dumps({"paper_id": "Bad_ID", "authors": "X", "year": "2024",
                    "title": "T", "position": "contra", "market_focus": "US"}),
        encoding="utf-8",
    )
    rc = ap.main(
        [
            "--from-json", str(bad_json),
            "--catalog-path", str(tmp_catalog),
            "--bibtex-path", str(tmp_bibtex),
            "--skip-downstream",
        ]
    )
    assert rc == 2
    captured = capsys.readouterr()
    assert "add-paper error" in captured.err


def test_downstream_failure_raises_with_partial_write_report(
    tmp_catalog: Path, tmp_bibtex: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed regenerator must not look like a successful add-paper run."""

    def fail_downstream() -> ap.DownstreamRunResult:
        return ap.DownstreamRunResult(
            artifacts=("paper_skeleton",),
            failures=("citation_graph exited with 2",),
        )

    monkeypatch.setattr(ap, "_regenerate_downstream", fail_downstream)

    with pytest.raises(ap.AddPaperError) as excinfo:
        ap.add_paper(
            _sample_paper_data(),
            catalog_path=tmp_catalog,
            bibtex_path=tmp_bibtex,
        )

    report = excinfo.value.report
    assert report is not None
    assert report.catalog_updated is True
    assert report.bibtex_updated is True
    assert report.downstream_artifacts == ("paper_skeleton",)
    assert any("citation_graph exited with 2" in note for note in report.notes)
    assert "partial add-paper writes" in str(excinfo.value)


def test_run_integrity_failure_returns_report(
    tmp_catalog: Path, tmp_bibtex: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``run_integrity`` runs after writes and propagates a non-zero gate."""
    monkeypatch.setattr(ap, "_run_paper_integrity", lambda: 1)

    with pytest.raises(ap.AddPaperError) as excinfo:
        ap.add_paper(
            _sample_paper_data(),
            catalog_path=tmp_catalog,
            bibtex_path=tmp_bibtex,
            skip_downstream=True,
            run_integrity=True,
        )

    report = excinfo.value.report
    assert report is not None
    assert report.catalog_updated is True
    assert report.bibtex_updated is True
    assert report.paper_integrity_exit_code == 1
    assert "paper-integrity failed" in str(excinfo.value)
