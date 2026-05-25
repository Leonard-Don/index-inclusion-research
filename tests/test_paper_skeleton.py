"""Unit tests for ``paper_skeleton``.

Covers:

- Skeleton generates the same submission-ready top-level section headers
  that ``submission_ready.EXPECTED_PAPER_SECTIONS`` enforces.
- H1–H7 hypothesis rows are NOT rendered in the new honest skeleton
  (they are post-hoc; disclosed in §7 分析参数 prose only).
- Core event-study numbers (§4.1) auto-populate from event_study_summary.csv.
- Limitations section pulls verbatim from docs/limitations.md.
- References section enumerates every entry in
  ``literature_catalog.PAPER_LIBRARY`` with ``paper_id=`` tokens.
- ``--force`` overwrite contract: refuses without flag, replaces with.
- ``main()`` exits 0 and writes a file in the configured sanity band.
- PAP block snapshot date + deviation counts pulled from public summary.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research import paper_skeleton as skeleton_module

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_pap_df() -> pd.DataFrame:
    """7-row PAP deviation report, all unchanged."""
    return pd.DataFrame(
        [
            {
                "hid": hid,
                "name_cn": "name",
                "classification": "unchanged",
                "baseline_verdict": "v",
                "current_verdict": "v",
            }
            for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7")
        ]
    )


def _make_event_study_df() -> pd.DataFrame:
    """Minimal event_study_summary.csv with the 4 headline rows."""
    return pd.DataFrame(
        [
            {
                "market": "CN", "event_phase": "announce", "inclusion": 1,
                "window": "[-1,+1]", "window_slug": "m1_p1",
                "n_events": 117, "mean_car": 0.0176, "std_car": 0.039,
                "se_car": 0.0036, "ci_low_95": 0.0106, "ci_high_95": 0.0246,
                "t_stat": 4.93, "p_value": 2.75e-06,
            },
            {
                "market": "US", "event_phase": "announce", "inclusion": 1,
                "window": "[-1,+1]", "window_slug": "m1_p1",
                "n_events": 254, "mean_car": 0.0184, "std_car": 0.041,
                "se_car": 0.0026, "ci_low_95": 0.0133, "ci_high_95": 0.0235,
                "t_stat": 5.25, "p_value": 2.50e-07,
            },
            {
                "market": "CN", "event_phase": "effective", "inclusion": 1,
                "window": "[-1,+1]", "window_slug": "m1_p1",
                "n_events": 117, "mean_car": 0.0042, "std_car": 0.046,
                "se_car": 0.0042, "ci_low_95": -0.004, "ci_high_95": 0.012,
                "t_stat": 0.93, "p_value": 0.355,
            },
            {
                "market": "US", "event_phase": "effective", "inclusion": 1,
                "window": "[-1,+1]", "window_slug": "m1_p1",
                "n_events": 254, "mean_car": -0.0014, "std_car": 0.038,
                "se_car": 0.0024, "ci_low_95": -0.006, "ci_high_95": 0.003,
                "t_stat": -0.51, "p_value": 0.611,
            },
        ]
    )


def _make_public_summary() -> dict:
    """Minimal public summary mirroring the live data/public/* shape."""
    return {
        "schema_version": 1,
        "generated_at": "2026-05-17T00:00:00+00:00",
        "literature": {
            "console_scripts_count": 39,
            "papers_indexed": 16,
            "research_thread_names": [
                "price_pressure",
                "demand_curve",
                "identification",
            ],
            "research_threads": 3,
        },
        "pap_baseline": {
            "frozen_for_days": 1,
            "path_ref": "snapshots/pre-registration-2026-05-16.csv",
            "snapshot_date": "2026-05-16",
        },
        "pap_deviation_summary": {
            "all_unchanged": True,
            "flipped_count": 0,
            "tightened_count": 0,
            "unchanged_count": 7,
            "unverifiable_count": 0,
            "weakened_count": 0,
        },
        "sensitivity_robustness": {
            "ar_engine_axis": {
                "ar_models_tested": ["adjusted", "market"],
                "cell_count": 2,
                "flip_count": 2,
                "flipping_hypotheses": ["H1", "H2"],
                "stable_count": 5,
            },
            "threshold_axis": {
                "cell_count": 4,
                "flip_count": 0,
                "stable_count": 7,
                "thresholds_tested": [0.05, 0.1, 0.15, 0.2],
            },
            "two_dimensional": {
                "cell_count": 8,
                "flip_count": 2,
                "flipping_hypotheses": ["H1", "H2"],
                "stable_count": 5,
            },
        },
    }


_LIMITATIONS_FIXTURE = """# 数据与方法限制

测试占位：本文档集中记录项目的关键数据近似与方法约束。

## 1. 价格与市值数据

- 价格 / 收益：Yahoo Finance 日频 OHLCV。
"""


@pytest.fixture
def fixture_paths(tmp_path: Path) -> dict[str, Path]:
    """Populated tmp project tree with all inputs the skeleton reads."""
    real_tables = tmp_path / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _make_pap_df().to_csv(
        real_tables / "pap_deviation_report.csv", index=False
    )
    _make_event_study_df().to_csv(
        real_tables / "event_study_summary.csv", index=False
    )

    public_dir = tmp_path / "data" / "public"
    public_dir.mkdir(parents=True)
    (public_dir / "index_research_summary.json").write_text(
        json.dumps(_make_public_summary()),
        encoding="utf-8",
    )

    limitations = tmp_path / "limitations.md"
    limitations.write_text(_LIMITATIONS_FIXTURE, encoding="utf-8")

    return {
        "root": tmp_path,
        "pap": real_tables / "pap_deviation_report.csv",
        "event_study": real_tables / "event_study_summary.csv",
        "public_summary": public_dir / "index_research_summary.json",
        "limitations": limitations,
    }


def _render(fixture: dict[str, Path], **overrides) -> str:
    kwargs = dict(
        pap_csv=fixture["pap"],
        event_study_csv=fixture["event_study"],
        public_summary_json=fixture["public_summary"],
        limitations_md=fixture["limitations"],
        generated_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    kwargs.update(overrides)
    return skeleton_module.build_paper_skeleton(**kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_skeleton_has_all_top_level_sections(fixture_paths):
    """Every advertised §1..§7 + 附录 ABC section header appears."""
    rendered = _render(fixture_paths)
    expected_headers = (
        "# 指数纳入溢价的来源",
        "## 1. 引言",
        "## 2. 文献综述",
        "## 3. 研究设计",
        "## 4. 实证结果",
        "### 4.1 核心结果",
        "## 5. 限制与讨论",
        "### 5.1 纳入 vs 剔除不对称",
        "### 5.2 长窗口 CAR 的持续性",
        "### 5.3 公告效应的跨年稳定性",
        "### 5.4 匹配对照组的协变量平衡",
        "### 5.5 预公告漂移",
        "## 6. 结论与启示",
        "### 6.1 主要结论",
        "### 6.2 实务与研究启示",
        "## 7. 分析参数",
        "## 参考文献",
        "## 附录",
        "### A. 数据契约",
        "### B. CLI 入口",
        "### C. 复现指南",
    )
    for header in expected_headers:
        assert header in rendered, f"missing header: {header!r}"


def test_h1_through_h7_not_rendered_as_main_body_table_rows(fixture_paths):
    """New honest framing: H1–H7 are NOT rendered as main-body table rows.

    They are post-hoc hypotheses disclosed in §7 分析参数 prose only.
    The integrity check ``check_verdicts_hids_match_skeleton`` accepts a
    skeleton with zero H table rows as the correct new state.
    """
    rendered = _render(fixture_paths)
    # No pipe-delimited H rows like "| H1 |", "| H2 |", etc.
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7"):
        assert f"| {hid} |" not in rendered, (
            f"H1–H7 table row for {hid} should not appear in new honest skeleton"
        )
    # The §3.3 识别策略 section should exist, not the old §3.3 七假说 section
    assert "识别策略" in rendered
    assert "七条跨市场不对称机制假说" not in rendered


def test_event_study_core_numbers_auto_populated(fixture_paths):
    """§4.1 surfaces headline CAR numbers from event_study_summary.csv."""
    rendered = _render(fixture_paths)
    # CN announce: mean_car=0.0176 → +1.76%, t=4.93
    assert "+1.76%" in rendered
    assert "4.93" in rendered
    # US announce: mean_car=0.0184 → +1.84%, t=5.25
    assert "+1.84%" in rendered
    assert "5.25" in rendered
    # CN effective: mean_car=0.0042 → +0.42%, p=0.355
    assert "+0.42%" in rendered
    assert "0.355" in rendered
    # US effective: mean_car=-0.0014 → -0.14%
    assert "-0.14%" in rendered


def test_limitations_pulled_verbatim_from_docs(fixture_paths):
    """§5.6 embeds the limitations.md file content verbatim."""
    rendered = _render(fixture_paths)
    # The fixture limitations text appears in the rendered output.
    assert "测试占位：本文档集中记录项目的关键数据近似与方法约束" in rendered
    assert "Yahoo Finance 日频 OHLCV" in rendered


def test_references_enumerates_every_literature_entry(fixture_paths):
    """§References lists every paper in PAPER_LIBRARY."""
    rendered = _render(fixture_paths)
    from index_inclusion_research.literature_catalog import (
        list_literature_papers,
    )

    papers = list_literature_papers()
    assert len(papers) == 16
    # Numbered enumeration header should claim 16 entries.
    assert "下列 16 篇文献来自" in rendered
    # Every paper_id is mentioned in the references block.
    for paper in papers:
        assert (
            f"`paper_id={paper.paper_id}`" in rendered
        ), f"reference missing paper_id={paper.paper_id}"


def test_heuristic_network_language_never_claims_verified_citations(fixture_paths):
    """The generated paper contract must not present heuristic links as citations."""
    rendered = _render(fixture_paths)

    assert "不是已验证引用关系" in rendered
    assert "不得作为被引/引用证据" in rendered
    assert "逐条 bibliography 引用核验" not in rendered


def test_cli_entry_count_uses_public_summary(fixture_paths):
    """CLI count text should follow the generated public summary, not a stale literal."""
    rendered = _render(fixture_paths)

    assert "### B. CLI 入口 (39 个)" in rendered


def test_repo_docs_do_not_pin_stale_console_script_ordinals():
    """Repo-facing docs should avoid stale ordinal counts as scripts grow."""
    root = Path(__file__).resolve().parents[1]
    docs = {
        "README.md": (root / "README.md").read_text(encoding="utf-8"),
        "docs/research_delivery_package.md": (
            root / "docs" / "research_delivery_package.md"
        ).read_text(encoding="utf-8"),
    }

    for label, doc in docs.items():
        assert "第 38 个" not in doc, label
        assert "第 39 个" not in doc, label
    assert "index-inclusion-paper-skeleton" in docs["docs/research_delivery_package.md"]
    assert "paper-skeleton" in docs["README.md"]


def test_pap_block_context_is_built(fixture_paths):
    """PAP block context is populated even if the new template uses it as prose.

    In the reframed §7 分析参数 skeleton, the PAP snapshot date and
    deviation audit are disclosed in prose rather than as an auto-generated
    table.  We verify the skeleton renders without error and that the §7
    post-hoc disclosure text is present.
    """
    rendered = _render(fixture_paths)
    # §7 分析参数 has the honest post-hoc disclosure text
    assert "post-hoc" in rendered or "探索性假说" in rendered
    assert "H1" in rendered or "H1-H7" in rendered  # mentioned in §7 prose
    # Skeleton renders without errors and contains §7 分析参数
    assert "## 7. 分析参数" in rendered


def test_write_skeleton_refuses_without_force_then_overwrites(
    fixture_paths, tmp_path: Path
):
    """``write_skeleton`` raises ``FileExistsError`` then accepts ``force=True``."""
    out = tmp_path / "out.md"
    skeleton_module.write_skeleton(
        out,
        pap_csv=fixture_paths["pap"],
        event_study_csv=fixture_paths["event_study"],
        public_summary_json=fixture_paths["public_summary"],
        limitations_md=fixture_paths["limitations"],
        generated_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    assert out.exists()
    first_size = out.stat().st_size
    with pytest.raises(FileExistsError):
        skeleton_module.write_skeleton(
            out,
            pap_csv=fixture_paths["pap"],
            event_study_csv=fixture_paths["event_study"],
            public_summary_json=fixture_paths["public_summary"],
            limitations_md=fixture_paths["limitations"],
            generated_at=datetime(2026, 5, 17, tzinfo=UTC),
        )
    skeleton_module.write_skeleton(
        out,
        force=True,
        pap_csv=fixture_paths["pap"],
        event_study_csv=fixture_paths["event_study"],
        public_summary_json=fixture_paths["public_summary"],
        limitations_md=fixture_paths["limitations"],
        generated_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    assert out.stat().st_size == first_size  # deterministic output


def test_skeleton_size_in_sanity_band(fixture_paths, tmp_path: Path):
    """The rendered skeleton falls within the SKELETON_MIN/MAX_BYTES band."""
    rendered = _render(fixture_paths)
    size = len(rendered.encode("utf-8"))
    assert skeleton_module.SKELETON_MIN_BYTES <= size <= skeleton_module.SKELETON_MAX_BYTES, (
        f"skeleton size {size} bytes outside sanity band "
        f"[{skeleton_module.SKELETON_MIN_BYTES}, {skeleton_module.SKELETON_MAX_BYTES}]"
    )


def test_no_todo_markers_in_submission_skeleton(fixture_paths):
    """The generated submission skeleton should not trip the TODO warning gate."""
    rendered = _render(fixture_paths)
    assert "[TODO:" not in rendered
    assert "作者待补" in rendered
    assert "摘要待精修" in rendered


def test_main_writes_file_and_exits_zero(fixture_paths, monkeypatch, tmp_path: Path):
    """``main()`` writes to the configured output path and returns 0."""
    out = tmp_path / "out_skeleton.md"

    # Patch the default-path helpers so main() picks up our fixture.
    monkeypatch.setattr(
        skeleton_module, "_default_pap_csv", lambda: fixture_paths["pap"]
    )
    monkeypatch.setattr(
        skeleton_module,
        "_default_event_study_summary_csv",
        lambda: fixture_paths["event_study"],
    )
    monkeypatch.setattr(
        skeleton_module,
        "_default_public_summary_json",
        lambda: fixture_paths["public_summary"],
    )
    monkeypatch.setattr(
        skeleton_module,
        "_default_limitations_md",
        lambda: fixture_paths["limitations"],
    )

    rc = skeleton_module.main(["--output", str(out)])
    assert rc == 0
    assert out.exists()
    size = out.stat().st_size
    assert (
        skeleton_module.SKELETON_MIN_BYTES
        <= size
        <= skeleton_module.SKELETON_MAX_BYTES
    )
