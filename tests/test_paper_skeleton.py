"""Unit tests for ``paper_skeleton``.

Covers:

- Skeleton generates all expected top-level section headers when every
  input exists.
- Verdict table is populated correctly from CSV (7 rows H1..H7).
- Sensitivity-section conclusion auto-derives from the public summary
  JSON (flipping_hypotheses listed by HID).
- HS300 RDD headline (τ / p / n) auto-populates from rdd_robustness.csv.
- Limitations section pulls verbatim from docs/limitations.md.
- References section enumerates every entry in
  ``literature_catalog.PAPER_LIBRARY``.
- ``--force`` overwrite contract: refuses without flag, replaces with.
- Figure paths advertised in the skeleton match what's in
  ``results/figures/`` (no dangling references).
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


_VERDICTS_COLUMNS = [
    "hid",
    "name_cn",
    "verdict",
    "confidence",
    "evidence_summary",
    "metric_snapshot",
    "next_step",
    "evidence_refs",
    "p_value",
    "key_label",
    "key_value",
    "n_obs",
    "paper_ids",
    "paper_count",
    "track",
    "evidence_tier",
]


def _make_verdicts_df() -> pd.DataFrame:
    rows = [
        ("H1", "信息泄露与预运行", "证据不足", "中", "core", "identification",
         "bootstrap p", 0.8748, 436),
        ("H2", "被动基金 AUM 差异", "部分支持", "中", "core", "demand_curve",
         "US AUM ratio", 13.48, 17),
        ("H3", "散户 vs 机构结构", "支持", "高", "supplementary",
         "price_pressure", "双通道命中率", 0.75, 4),
        ("H4", "卖空约束", "证据不足", "中", "supplementary",
         "identification", "regression p", 0.5366, 436),
        ("H5", "涨跌停限制", "支持", "高", "core", "identification",
         "limit_coef p", 0.0082, 936),
        ("H6", "指数权重可预测性", "证据不足", "中", "supplementary",
         "demand_curve", "heavy−light spread", -0.019, 67),
        ("H7", "行业结构差异", "支持", "中", "core", "identification",
         "US sector spread", 5.95, 187),
    ]
    data = []
    for hid, name, verdict, confidence, tier, track, kl, kv, n in rows:
        data.append({
            "hid": hid,
            "name_cn": name,
            "verdict": verdict,
            "confidence": confidence,
            "evidence_summary": "long summary omitted",
            "metric_snapshot": "multi-line snapshot omitted",
            "next_step": "next-step omitted",
            "evidence_refs": "M1:cma_ar_path.csv",
            "p_value": kv if "p" in kl else None,
            "key_label": kl,
            "key_value": kv,
            "n_obs": n,
            "paper_ids": "paper_a | paper_b",
            "paper_count": 2,
            "track": track,
            "evidence_tier": tier,
        })
    return pd.DataFrame(data, columns=_VERDICTS_COLUMNS)


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


def _make_rdd_df() -> pd.DataFrame:
    """Minimal rdd_robustness.csv with a single main spec + 4 robustness rows."""
    return pd.DataFrame(
        [
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 120,
                "tau": 0.0392,
                "p_value": 0.0483,
                "spec": "main",
                "spec_kind": "main",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 102,
                "tau": 0.049,
                "p_value": 0.10,
                "spec": "donut(±0.01)",
                "spec_kind": "donut",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 72,
                "tau": -0.024,
                "p_value": 0.26,
                "spec": "placebo -0.05",
                "spec_kind": "placebo",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 130,
                "tau": -0.020,
                "p_value": 0.18,
                "spec": "placebo +0.05",
                "spec_kind": "placebo",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 120,
                "tau": 0.004,
                "p_value": 0.92,
                "spec": "polynomial=2",
                "spec_kind": "polynomial",
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
    _make_verdicts_df().to_csv(
        real_tables / "cma_hypothesis_verdicts.csv", index=False
    )
    _make_pap_df().to_csv(
        real_tables / "pap_deviation_report.csv", index=False
    )

    rdd_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    rdd_dir.mkdir(parents=True)
    _make_rdd_df().to_csv(rdd_dir / "rdd_robustness.csv", index=False)

    public_dir = tmp_path / "data" / "public"
    public_dir.mkdir(parents=True)
    (public_dir / "index_research_summary.json").write_text(
        json.dumps(_make_public_summary()),
        encoding="utf-8",
    )

    limitations = tmp_path / "limitations.md"
    limitations.write_text(_LIMITATIONS_FIXTURE, encoding="utf-8")

    figures_dir = tmp_path / "results" / "figures"
    figures_dir.mkdir(parents=True)
    for name in (
        "hs300_rdd_robustness_forest.png",
        "cma_verdicts_forest.png",
        "cma_verdicts_sensitivity.png",
        "cma_verdicts_ar_engine.png",
        "cma_verdicts_2d_robustness.png",
    ):
        (figures_dir / name).write_bytes(b"fake-png")

    return {
        "root": tmp_path,
        "verdicts": real_tables / "cma_hypothesis_verdicts.csv",
        "pap": real_tables / "pap_deviation_report.csv",
        "rdd": rdd_dir / "rdd_robustness.csv",
        "public_summary": public_dir / "index_research_summary.json",
        "limitations": limitations,
        "figures": figures_dir,
    }


def _render(fixture: dict[str, Path], **overrides) -> str:
    kwargs = dict(
        verdicts_csv=fixture["verdicts"],
        pap_csv=fixture["pap"],
        rdd_csv=fixture["rdd"],
        public_summary_json=fixture["public_summary"],
        limitations_md=fixture["limitations"],
        figures_dir=fixture["figures"],
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
        "# 指数纳入效应跨市场不对称研究",
        "## 1. 引言",
        "## 2. 文献综述",
        "## 3. 研究设计",
        "## 4. 实证结果",
        "### 4.1 主结果",
        "### 4.2 跨市场不对称机制",
        "### 4.3 HS300 RDD 结果",
        "### 4.4 稳健性检查",
        "#### 4.4.1 阈值敏感性",
        "#### 4.4.2 AR 引擎敏感性",
        "#### 4.4.3 联合稳健性",
        "## 5. 限制与讨论",
        "## 6. 结论与启示",
        "## 7. PAP",
        "## 参考文献",
        "## 附录",
        "### A. 数据契约",
        "### B. CLI 入口",
        "### C. 复现指南",
    )
    for header in expected_headers:
        assert header in rendered, f"missing header: {header!r}"


def test_verdict_table_populated_h1_through_h7(fixture_paths):
    """The 3.3 verdict table contains every H1..H7 row in canonical order."""
    rendered = _render(fixture_paths)
    # Find the verdict table block.
    assert "| 假说 | 名称 | 裁决 | 置信度 | 证据层级 |" in rendered
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7"):
        # Each HID should appear in a table row (`| H1 |` style).
        assert f"| {hid} |" in rendered, f"missing verdict row for {hid}"
    # Spot-check that representative verdicts text is in there.
    assert "证据不足" in rendered
    assert "部分支持" in rendered
    assert "支持" in rendered
    # H1..H7 ordering (H1 row appears before H2 row).
    h1_idx = rendered.index("| H1 |")
    h7_idx = rendered.index("| H7 |")
    assert h1_idx < h7_idx, "H1..H7 not in canonical order"


def test_sensitivity_conclusion_auto_derived_from_public_summary(fixture_paths):
    """The §4.4 sensitivity conclusions use counts from the public summary."""
    rendered = _render(fixture_paths)
    # Threshold axis: 7 / 7 stable, all 7 stable wording.
    assert "7 / 7 条假说裁决稳定" in rendered
    assert "全部 7 条假说裁决不随 p-value 阈值变化" in rendered
    # AR engine axis: 5 / 7 stable, H1+H2 flip.
    assert "5 / 7 条假说裁决稳定" in rendered
    assert "**H1**, **H2**" in rendered
    # 2D: 5 stable, 2 flip; phrasing notes AR-engine axis lineage.
    assert "8 cell" in rendered or "8 cell" in rendered
    assert "脆弱性来自 AR 引擎而非阈值" in rendered


def test_hs300_rdd_headline_auto_populated(fixture_paths):
    """§4.3 surfaces τ (%) / p / n from the main spec in rdd_robustness.csv."""
    rendered = _render(fixture_paths)
    # tau in our fixture: 0.0392 → 3.92%
    assert "τ = **3.92%**" in rendered
    assert "p = 0.0483" in rendered
    assert "n = 120" in rendered
    assert "outcome = `car_m1_p1`" in rendered
    # Robustness spec count (1 main + 4 others = 5).
    assert "稳健性 spec 数：5" in rendered


def test_limitations_pulled_verbatim_from_docs(fixture_paths):
    """§5 embeds the limitations.md file content verbatim."""
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
    assert "完整 39 个 console scripts" in rendered
    assert "完整 38 个 console scripts" not in rendered


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


def test_advertised_figure_paths_exist_in_results_figures(fixture_paths):
    """All ![...](../results/figures/X.png) refs in the skeleton correspond
    to files actually present in the fixture figures dir.

    Mirrors what doctor.check_paper_skeleton_freshness will eventually do:
    a stale skeleton that references a missing PNG is a bug. Here we only
    check that the skeleton doesn't fabricate figure paths.
    """
    rendered = _render(fixture_paths)
    import re

    refs = re.findall(r"!\[[^\]]+\]\(\.\.\/results\/figures\/([^)]+)\)", rendered)
    assert refs, "no figure references found in skeleton"
    figures_present = {p.name for p in fixture_paths["figures"].iterdir()}
    for ref in refs:
        assert ref in figures_present, (
            f"skeleton references {ref!r} which is missing from figures dir; "
            f"have {figures_present}"
        )


def test_pap_block_pulls_snapshot_date_and_deviation_counts(fixture_paths):
    """§7 surfaces the PAP snapshot date + deviation 5-class counts."""
    rendered = _render(fixture_paths)
    assert "2026-05-16" in rendered
    assert "snapshots/pre-registration-2026-05-16.csv" in rendered
    assert "基线已冻结 1 天" in rendered
    assert "全部 unchanged: **True**" in rendered
    assert "unchanged: 7" in rendered
    assert "flipped: 0" in rendered


def test_write_skeleton_refuses_without_force_then_overwrites(
    fixture_paths, tmp_path: Path
):
    """``write_skeleton`` raises ``FileExistsError`` then accepts ``force=True``."""
    out = tmp_path / "out.md"
    skeleton_module.write_skeleton(
        out,
        verdicts_csv=fixture_paths["verdicts"],
        pap_csv=fixture_paths["pap"],
        rdd_csv=fixture_paths["rdd"],
        public_summary_json=fixture_paths["public_summary"],
        limitations_md=fixture_paths["limitations"],
        figures_dir=fixture_paths["figures"],
        generated_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    assert out.exists()
    first_size = out.stat().st_size
    with pytest.raises(FileExistsError):
        skeleton_module.write_skeleton(
            out,
            verdicts_csv=fixture_paths["verdicts"],
            pap_csv=fixture_paths["pap"],
            rdd_csv=fixture_paths["rdd"],
            public_summary_json=fixture_paths["public_summary"],
            limitations_md=fixture_paths["limitations"],
            figures_dir=fixture_paths["figures"],
            generated_at=datetime(2026, 5, 17, tzinfo=UTC),
        )
    skeleton_module.write_skeleton(
        out,
        force=True,
        verdicts_csv=fixture_paths["verdicts"],
        pap_csv=fixture_paths["pap"],
        rdd_csv=fixture_paths["rdd"],
        public_summary_json=fixture_paths["public_summary"],
        limitations_md=fixture_paths["limitations"],
        figures_dir=fixture_paths["figures"],
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


def test_todo_markers_present_for_prose_sections(fixture_paths):
    """Every section that requires human prose carries an explicit TODO marker."""
    rendered = _render(fixture_paths)
    # Use the section-prose TODOs (different from the table-headline TODOs in
    # parentheses like "(TODO)"); look for the bracketed "[TODO:" form.
    todo_count = rendered.count("[TODO:")
    # 7 H-detail subsections + introduction (3) + 6.结论 (3) + 数据契约 + CLI入口
    # + 文献综述 + 样本与数据 + 实证方法 + §4.1 main results + §5 prose = ~17+
    assert todo_count >= 15, f"only {todo_count} TODO markers, expected ≥15"
    # Specific anchor TODOs that must be present.
    assert "[TODO: 引言 prose" in rendered
    assert "[TODO: 主结果 prose" in rendered
    assert "[TODO: H1 prose" in rendered
    assert "[TODO: H7 prose" in rendered


def test_main_writes_file_and_exits_zero(fixture_paths, monkeypatch, tmp_path: Path):
    """``main()`` writes to the configured output path and returns 0."""
    out = tmp_path / "out_skeleton.md"

    # Patch the default-path helpers so main() picks up our fixture.
    monkeypatch.setattr(
        skeleton_module, "_default_verdicts_csv", lambda: fixture_paths["verdicts"]
    )
    monkeypatch.setattr(
        skeleton_module, "_default_pap_csv", lambda: fixture_paths["pap"]
    )
    monkeypatch.setattr(
        skeleton_module, "_default_rdd_csv", lambda: fixture_paths["rdd"]
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
    monkeypatch.setattr(
        skeleton_module,
        "_default_figures_dir",
        lambda: fixture_paths["figures"],
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
