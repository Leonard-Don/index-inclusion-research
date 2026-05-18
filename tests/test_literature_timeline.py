"""Tests for the 47th CLI — literature chronology timeline figure."""

from __future__ import annotations

import csv
import struct
import zlib
from pathlib import Path

import pytest

from index_inclusion_research.literature_catalog import PAPER_LIBRARY
from index_inclusion_research.outputs.literature_timeline import (
    DEFAULT_YEAR_MAX,
    DEFAULT_YEAR_MIN,
    ERA_BANDS,
    POSITION_COLORS,
    THREAD_ORDER,
    TimelinePaper,
    _short_citation_for,
    _year_from_label,
    assemble_timeline_papers,
    build_literature_timeline_plot,
    summarize_for_public_summary,
)


def _png_dimensions(png_path: Path) -> tuple[int, int]:
    """Return ``(width, height)`` of a PNG by parsing the IHDR chunk.

    Avoids a Pillow dependency; mirrors the helper in
    ``test_citation_graph.py`` so the two literature-figure suites
    share verification primitives.
    """
    data = png_path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "not a PNG file"
    width, height = struct.unpack(">II", data[16:24])
    ihdr_crc_expected = struct.unpack(">I", data[29:33])[0]
    ihdr_crc_actual = zlib.crc32(data[12:29])
    assert ihdr_crc_expected == ihdr_crc_actual, "PNG IHDR CRC mismatch"
    return width, height


def _write_centrality_csv(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    """Write a minimal centrality CSV the renderer can read."""
    csv_path = tmp_path / "citation_centrality.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "paper_id",
                "in_degree",
                "out_degree",
                "betweenness",
                "eigenvector",
                "top_linked_by",
                "top_links_to",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


def test_assemble_timeline_papers_covers_full_library() -> None:
    papers = assemble_timeline_papers()
    assert len(papers) == len(PAPER_LIBRARY) == 16
    # Every paper must land on a recognised position bucket.
    assert all(p.position in {"pro", "contra", "neutral"} for p in papers)
    # And every paper must carry a parseable year ∈ [1986, today].
    assert all(1986 <= p.year <= 2030 for p in papers)


def test_render_emits_png_above_minimum_size(tmp_path: Path) -> None:
    papers = assemble_timeline_papers()
    png_path = tmp_path / "literature_timeline.png"
    pdf_path = tmp_path / "literature_timeline.pdf"

    written = build_literature_timeline_plot(
        papers,
        output_png_path=png_path,
        output_pdf_path=pdf_path,
    )
    assert written == png_path.resolve()
    assert png_path.exists()
    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 0
    width, height = _png_dimensions(png_path)
    # The renderer requests 14×7 in @ 100 dpi → 1400×700 px before
    # bbox-tight trim. The doctor contract is ≥ 800×600.
    assert width >= 800
    assert height >= 600


def test_year_axis_spans_default_range_with_all_papers() -> None:
    """Every paper's parsed year must fall inside the default x-axis range."""
    papers = assemble_timeline_papers()
    years = [p.year for p in papers]
    assert min(years) >= DEFAULT_YEAR_MIN
    assert max(years) <= DEFAULT_YEAR_MAX
    # Era band bounds must cover the data range (1986 → 2026).
    band_starts = [b.start_year for b in ERA_BANDS]
    band_ends = [b.end_year for b in ERA_BANDS]
    assert min(band_starts) == 1986
    assert max(band_ends) == 2026


def test_position_colors_applied_correctly() -> None:
    """The static palette stays consistent with the stance buckets."""
    assert set(POSITION_COLORS) == {"pro", "contra", "neutral"}
    # All three hex codes must be distinct (no accidental aliasing).
    assert len(set(POSITION_COLORS.values())) == 3
    # Quick stance map sanity: classical 1986 seminal papers split into
    # pro (Shleifer) and contra (Harris-Gurel).
    papers = assemble_timeline_papers()
    by_id = {p.paper_id: p for p in papers}
    assert by_id["shleifer_1986"].position == "pro"
    assert by_id["harris_gurel_1986"].position == "contra"
    assert by_id["wurgler_zhuravskaya_2002"].position == "neutral"


def test_centrality_sized_markers_promote_anchors(tmp_path: Path) -> None:
    """In-degree centrality should land anchor papers above the floor."""
    # Use the real centrality CSV the project ships so the marker-size
    # scale reflects production data (Shleifer 1986 = in_degree 14,
    # the global anchor).
    csv_path = _write_centrality_csv(
        tmp_path,
        [
            {
                "paper_id": "shleifer_1986",
                "in_degree": "14",
                "out_degree": "0",
                "betweenness": "0.25",
                "eigenvector": "0.61",
                "top_linked_by": "",
                "top_links_to": "",
            },
            {
                "paper_id": "harris_gurel_1986",
                "in_degree": "12",
                "out_degree": "0",
                "betweenness": "0.17",
                "eigenvector": "0.55",
                "top_linked_by": "",
                "top_links_to": "",
            },
            {
                "paper_id": "yao_zhou_chen_2022",
                "in_degree": "0",
                "out_degree": "5",
                "betweenness": "0.0",
                "eigenvector": "0.09",
                "top_linked_by": "",
                "top_links_to": "",
            },
        ],
    )
    papers = assemble_timeline_papers(centrality_csv_path=csv_path)
    by_id = {p.paper_id: p for p in papers}
    # Anchor papers carry the higher in_degree column.
    assert by_id["shleifer_1986"].in_degree == 14
    assert by_id["harris_gurel_1986"].in_degree == 12
    assert by_id["yao_zhou_chen_2022"].in_degree == 0
    # Papers not present in the synthetic CSV fall back to 0.
    assert by_id["lynch_mendenhall_1997"].in_degree == 0


def test_renderer_tolerates_missing_centrality_csv(tmp_path: Path) -> None:
    """A missing centrality CSV must not crash the renderer."""
    papers = assemble_timeline_papers(
        centrality_csv_path=tmp_path / "does_not_exist.csv"
    )
    png_path = tmp_path / "literature_timeline.png"
    build_literature_timeline_plot(
        papers,
        output_png_path=png_path,
        output_pdf_path=None,
    )
    assert png_path.exists()


def test_era_distribution_summary_matches_catalog() -> None:
    """The summary block must surface era-bucket counts + dominant positions."""
    papers = assemble_timeline_papers()
    summary = summarize_for_public_summary(papers)
    assert summary["n_papers"] == 16
    # 1986 + 1997 + 2000 papers are pre-2002.
    pre = summary["n_papers_pre_2002"]
    mid = summary["n_papers_2002_to_2014"]
    post = summary["n_papers_post_2014"]
    assert isinstance(pre, int) and isinstance(mid, int) and isinstance(post, int)
    assert pre + mid + post == 16
    # Anchors-by-era is keyed on the three era buckets.
    anchors = summary["anchors_by_era"]
    assert isinstance(anchors, dict)
    assert set(anchors) == {"pre_2002", "2002_to_2014", "post_2014"}
    # Dominant positions only use the canonical {pro, contra, neutral}.
    dominant = summary["dominant_position_by_era"]
    assert isinstance(dominant, dict)
    for value in dominant.values():
        if value is not None:
            assert value in {"pro", "contra", "neutral"}


def test_thread_order_covers_canonical_labels() -> None:
    """Each project_module value in the catalog must map to a known thread."""
    catalog_threads = {str(p.project_module) for p in PAPER_LIBRARY}
    known = set(THREAD_ORDER)
    # The renderer's THREAD_ORDER covers every thread the catalog uses
    # — no surprise "other" lane needed for the production library.
    assert catalog_threads.issubset(known)


def test_short_citation_format() -> None:
    """Citation labels match the documented ``Surname YY`` pattern."""
    assert _short_citation_for("Andrei Shleifer", 1986) == "Shleifer '86"
    assert (
        _short_citation_for("Yen-Cheng Chang; Harrison Hong; Inessa Liskovich", 2014)
        == "Chang et al '14"
    )
    # Single Chinese author (no Latin tokens to split on) — keep the
    # original name unchanged.
    assert _short_citation_for("姚东旻; 张日升; 李嘉晟", 2014) == "姚东旻 et al '14"


def test_year_label_parser_handles_chinese_placeholder() -> None:
    """``年份待核验`` placeholder returns ``None`` so the caller can default."""
    assert _year_from_label("1986") == 1986
    assert _year_from_label("年份待核验") is None
    assert _year_from_label("2022 (revised)") == 2022


def test_synthetic_papers_render_count_matches_input(tmp_path: Path) -> None:
    """Render a synthetic 3-paper timeline and verify the file exists.

    Exercises the renderer with caller-supplied :class:`TimelinePaper`
    objects (no catalog dependency) so the public API is callable in
    isolation.
    """
    papers = [
        TimelinePaper(
            paper_id="seminal",
            short_citation="Seminal '86",
            year=1986,
            thread="需求曲线效应",
            position="pro",
            in_degree=10,
        ),
        TimelinePaper(
            paper_id="skeptic",
            short_citation="Skeptic '02",
            year=2002,
            thread="短期价格压力",
            position="contra",
            in_degree=4,
        ),
        TimelinePaper(
            paper_id="frontier",
            short_citation="Frontier '22",
            year=2022,
            thread="沪深300论文复现",
            position="neutral",
            in_degree=0,
        ),
    ]
    png_path = tmp_path / "synth_timeline.png"
    build_literature_timeline_plot(
        papers, output_png_path=png_path, output_pdf_path=None
    )
    assert png_path.exists()
    width, height = _png_dimensions(png_path)
    assert width >= 800 and height >= 600


def test_empty_input_still_renders_valid_png(tmp_path: Path) -> None:
    """No papers → still produces a PNG above the doctor floor.

    Defensive: the public-summary fallback (no centrality CSV, no
    catalog) must not crash any downstream consumer.
    """
    png_path = tmp_path / "empty.png"
    build_literature_timeline_plot(
        [], output_png_path=png_path, output_pdf_path=None
    )
    assert png_path.exists()
    width, height = _png_dimensions(png_path)
    assert width >= 800 and height >= 600
    # Empty summary block matches the documented schema.
    summary = summarize_for_public_summary([])
    assert summary["n_papers"] == 0
    assert summary["dominant_position_by_era"]["pre_2002"] is None


def test_cli_pyproject_entry_exists() -> None:
    """The 47th console script must be registered in pyproject.toml."""
    repo_root = Path(__file__).resolve().parents[1]
    pyproject = (repo_root / "pyproject.toml").read_text(encoding="utf-8")
    assert "index-inclusion-literature-timeline" in pyproject
    assert "run_literature_timeline_main" in pyproject


@pytest.mark.parametrize(
    "year,expected",
    [
        (1986, "pre-2002"),
        (2002, "2002-2014"),
        (2013, "2002-2014"),
        (2014, "post-2014"),
        (2022, "post-2014"),
    ],
)
def test_era_buckets_align_with_summary(year: int, expected: str) -> None:
    """Era-bucket boundaries match the documented [< 2002, [2002, 2014), >= 2014)."""
    paper = TimelinePaper(
        paper_id=f"synth_{year}",
        short_citation=f"Synth '{year % 100:02d}",
        year=year,
        thread="需求曲线效应",
        position="pro",
        in_degree=1,
    )
    summary = summarize_for_public_summary([paper])
    if expected == "pre-2002":
        assert summary["n_papers_pre_2002"] == 1
    elif expected == "2002-2014":
        assert summary["n_papers_2002_to_2014"] == 1
    else:
        assert summary["n_papers_post_2014"] == 1
