"""Unit tests for ``methodology_summary``.

Covers:

- All 8 top-level section headers present.
- Verdict / sample-size table populated with every H1..H7 row.
- Sensitivity coverage table auto-derives from the public summary JSON.
- PAP deviation block reflects the current public-summary classification.
- Top-5 centrality citation table populated from centrality CSV.
- Size budget (3-8 KB sanity band) holds for a representative fixture.
- ``main()`` exits 0, writes a file with deterministic content.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research import methodology_summary as ms_module

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
            "evidence_summary": "",
            "metric_snapshot": "",
            "next_step": "",
            "evidence_refs": "",
            "p_value": None,
            "key_label": kl,
            "key_value": kv,
            "n_obs": n,
            "paper_ids": "",
            "paper_count": 0,
            "track": track,
            "evidence_tier": tier,
        })
    return pd.DataFrame(data, columns=_VERDICTS_COLUMNS)


def _make_public_summary() -> dict:
    return {
        "schema_version": 1,
        "generated_at": "2026-05-17T00:00:00+00:00",
        "literature": {
            "papers_indexed": 16,
            "research_threads": 3,
            "research_thread_names": [
                "price_pressure",
                "demand_curve",
                "identification",
            ],
            "console_scripts_count": 41,
        },
        "pap_baseline": {
            "snapshot_date": "2026-05-16",
            "path_ref": "snapshots/pre-registration-2026-05-16.csv",
            "frozen_for_days": 1,
        },
        "pap_deviation_summary": {
            "all_unchanged": True,
            "flipped_count": 0,
            "tightened_count": 0,
            "weakened_count": 0,
            "unverifiable_count": 0,
            "unchanged_count": 7,
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


def _make_centrality_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"paper_id": "shleifer_1986", "eigenvector": 0.611,
             "in_degree": 14, "out_degree": 0, "betweenness": 0.25},
            {"paper_id": "harris_gurel_1986", "eigenvector": 0.552,
             "in_degree": 12, "out_degree": 0, "betweenness": 0.17},
            {"paper_id": "wurgler_zhuravskaya_2002", "eigenvector": 0.270,
             "in_degree": 7, "out_degree": 3, "betweenness": 0.07},
            {"paper_id": "lynch_mendenhall_1997", "eigenvector": 0.264,
             "in_degree": 6, "out_degree": 2, "betweenness": 0.05},
            {"paper_id": "chang_hong_liskovich_2014", "eigenvector": 0.172,
             "in_degree": 4, "out_degree": 4, "betweenness": 0.04},
            {"paper_id": "kaul_mehrotra_morck_2000", "eigenvector": 0.168,
             "in_degree": 1, "out_degree": 3, "betweenness": 0.003},
        ]
    )


_REAL_EVENTS_CSV_HEAD = (
    "market,index_name,ticker,announce_date,effective_date\n"
)
_REAL_MATCHED_PANEL_CSV_HEAD = (
    "market,ticker,date,ret,event_phase,inclusion,relative_day\n"
)


@pytest.fixture
def fixture_paths(tmp_path: Path) -> dict[str, Path]:
    """Populated tmp project tree with all inputs the summary card reads."""
    real_tables = tmp_path / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _make_verdicts_df().to_csv(
        real_tables / "cma_hypothesis_verdicts.csv", index=False
    )

    public_dir = tmp_path / "data" / "public"
    public_dir.mkdir(parents=True)
    (public_dir / "index_research_summary.json").write_text(
        json.dumps(_make_public_summary()),
        encoding="utf-8",
    )

    lit_dir = tmp_path / "results" / "literature"
    lit_dir.mkdir(parents=True)
    _make_centrality_df().to_csv(
        lit_dir / "citation_centrality.csv", index=False
    )

    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    # Real-events fixture: 893 rows + header (matches current artifact).
    (processed / "real_events_clean.csv").write_text(
        _REAL_EVENTS_CSV_HEAD
        + "\n".join(f"CN,CSI300,T{i},2024-01-01,2024-01-15" for i in range(893))
        + "\n",
        encoding="utf-8",
    )
    # Matched panel fixture: 1500 rows (small enough for fast tmp_path, large
    # enough to verify formatting + count).
    (processed / "real_matched_event_panel.csv").write_text(
        _REAL_MATCHED_PANEL_CSV_HEAD
        + "\n".join(
            f"CN,T{i},2024-01-{(i % 27) + 1:02d},0.01,announce,0,0"
            for i in range(1500)
        )
        + "\n",
        encoding="utf-8",
    )

    # Minimal pyproject.toml fixture exposing a synthetic 41-script project.
    pyproject = tmp_path / "pyproject.toml"
    scripts_block = "\n".join(
        f'  script-{i} = "x.module:f"' for i in range(41)
    )
    pyproject.write_text(
        '[project]\nname = "x"\nversion = "0"\n\n'
        '[project.scripts]\n' + scripts_block + "\n",
        encoding="utf-8",
    )

    return {
        "root": tmp_path,
        "verdicts": real_tables / "cma_hypothesis_verdicts.csv",
        "public_summary": public_dir / "index_research_summary.json",
        "centrality": lit_dir / "citation_centrality.csv",
        "real_events": processed / "real_events_clean.csv",
        "real_matched_panel": processed / "real_matched_event_panel.csv",
        "pyproject": pyproject,
    }


def _render(fixture: dict[str, Path], **overrides) -> str:
    kwargs = dict(
        verdicts_csv=fixture["verdicts"],
        public_summary_json=fixture["public_summary"],
        centrality_csv=fixture["centrality"],
        real_events_csv=fixture["real_events"],
        real_matched_panel_csv=fixture["real_matched_panel"],
        pyproject_path=fixture["pyproject"],
        generated_at=datetime(2026, 5, 17, tzinfo=UTC),
    )
    kwargs.update(overrides)
    return ms_module.build_methodology_summary(**kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_card_has_all_eight_sections(fixture_paths):
    """Every advertised §1..§8 section header appears in the rendered card."""
    rendered = _render(fixture_paths)
    expected_headers = (
        "# 指数纳入效应跨市场不对称研究 · 方法论摘要",
        "## 1. 样本规模",
        "## 2. 估计方法",
        "## 3. 稳健性覆盖",
        "## 4. 裁决基线快照",
        "## 5. 数据契约",
        "## 6. 复现命令",
        "## 7. 关键文献基础",
        "## 8. 工具链",
    )
    for header in expected_headers:
        assert header in rendered, f"missing header: {header!r}"


def test_verdict_table_populated_h1_through_h7(fixture_paths):
    """§1 sample-size table contains every H1..H7 row in canonical order."""
    rendered = _render(fixture_paths)
    assert "| 假说 | 名称 | n_obs | 证据层级 | 主线 |" in rendered
    # Every HID appears as a leading cell and the canonical ordering
    # is preserved (H1 line appears before H2, etc.).
    last_pos = -1
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7"):
        marker = f"| {hid} |"
        pos = rendered.find(marker)
        assert pos != -1, f"missing row marker {marker!r}"
        assert pos > last_pos, (
            f"{hid} appears out of order (pos={pos}, last={last_pos})"
        )
        last_pos = pos
    # Specific sample-size values from the fixture must round-trip.
    assert "| 436 | core | identification |" in rendered  # H1
    assert "| 936 | core | identification |" in rendered  # H5
    assert "| 17 | core | demand_curve |" in rendered  # H2


def test_sensitivity_coverage_auto_derives_from_public_summary(fixture_paths):
    """§3 sensitivity table reflects the public-summary stable/cell counts."""
    rendered = _render(fixture_paths)
    # threshold axis: 4 thresholds, all 7 stable
    assert "| 阈值 | 0.05 / 0.1 / 0.15 / 0.2 | 7/7 |" in rendered
    # AR engine axis: 2 engines, 5 stable
    assert "| AR 引擎 | adjusted / market | 5/7 |" in rendered
    # 2D joint axis: 8 cells = 4 × 2, 5 stable
    assert (
        "| 联合 | 8 cells = 4 阈值 × 2 AR 引擎 | 5/7 |" in rendered
    )


def test_pap_block_reflects_current_deviation_state(fixture_paths):
    """§4 PAP table surfaces the snapshot date and deviation classification."""
    rendered = _render(fixture_paths)
    # Baseline row with snapshot date + path.
    assert "冻结于 2026-05-16" in rendered
    assert "snapshots/pre-registration-2026-05-16.csv" in rendered
    # All-unchanged flag short-circuits to the friendly summary string.
    assert "全部未偏离" in rendered
    # Doctor rows still mention the two monitored checks.
    assert "check_pap_deviation_no_flips" in rendered
    assert "check_pap_snapshot_freshness" in rendered


def test_top_centrality_citations_populated(fixture_paths):
    """§7 lists the top-5 eigenvector-ranked papers in descending order."""
    rendered = _render(fixture_paths)
    # Top-5 should be the 5 highest-eigenvector rows (not the 6th).
    expected_top_5 = (
        "shleifer_1986",
        "harris_gurel_1986",
        "wurgler_zhuravskaya_2002",
        "lynch_mendenhall_1997",
        "chang_hong_liskovich_2014",
    )
    last_pos = -1
    for paper_id in expected_top_5:
        marker = f"`{paper_id}`"
        pos = rendered.find(marker)
        assert pos != -1, f"missing centrality row {marker!r}"
        assert pos > last_pos, (
            f"{paper_id} appears out of expected eigenvector order"
        )
        last_pos = pos
    # 6th-ranked paper must NOT appear in the table.
    assert "`kaul_mehrotra_morck_2000`" not in rendered
    # Heuristic-vs-bibliography disclaimer present.
    assert "启发式相似性" in rendered


def test_size_in_sanity_band(fixture_paths):
    """Rendered card weighs between SUMMARY_MIN_BYTES and SUMMARY_MAX_BYTES."""
    rendered = _render(fixture_paths)
    size_bytes = len(rendered.encode("utf-8"))
    assert (
        ms_module.SUMMARY_MIN_BYTES
        <= size_bytes
        <= ms_module.SUMMARY_MAX_BYTES
    ), (
        f"card size {size_bytes} bytes outside band "
        f"[{ms_module.SUMMARY_MIN_BYTES}, {ms_module.SUMMARY_MAX_BYTES}]"
    )


def test_carries_no_todo_markers(fixture_paths):
    """Unlike paper_skeleton, the methodology card must NEVER emit [TODO]."""
    rendered = _render(fixture_paths)
    assert "[TODO" not in rendered, (
        "methodology summary must be fully deterministic — no [TODO] prose markers"
    )
    # Reproduction commands and CLI badge auto-derive.
    assert "index-inclusion-methodology-summary" in rendered
    assert "41 个 console scripts" in rendered


def test_main_writes_file_and_exits_zero(
    fixture_paths, monkeypatch, tmp_path: Path
):
    """``main()`` writes the default path under the fixture root and exits 0."""
    # Point the module's path resolvers at the fixture tree.
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))

    output_path = fixture_paths["root"] / "paper" / "methodology_summary.md"
    rc = ms_module.main(["--output", str(output_path)])
    assert rc == 0
    assert output_path.exists()
    body = output_path.read_text(encoding="utf-8")
    assert "## 1. 样本规模" in body
    assert "## 8. 工具链" in body
