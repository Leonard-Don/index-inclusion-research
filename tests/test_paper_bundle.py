from __future__ import annotations

from pathlib import Path

import pytest

from index_inclusion_research import paper_bundle


def _seed_minimal_project(root: Path) -> None:
    """Lay down the minimum tree paper_bundle expects so it can copy."""
    (root / "results" / "real_tables").mkdir(parents=True)
    (root / "results" / "real_tables" / "cma_hypothesis_verdicts.tex").write_text(
        "% verdicts table\n", encoding="utf-8"
    )
    (root / "results" / "real_tables" / "data_sources.tex").write_text(
        "% data sources\n", encoding="utf-8"
    )
    # Verdicts CSV — used by bundle_summary
    (root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv").write_text(
        "hid,name_cn,verdict,evidence_tier\n"
        "H1,信息泄露,证据不足,core\n"
        "H5,涨跌停限制,支持,core\n"
        "H7,行业结构,支持,core\n"
        "H3,散户机构,支持,supplementary\n"
        "H4,卖空约束,证据不足,supplementary\n",
        encoding="utf-8",
    )

    (root / "results" / "real_figures").mkdir(parents=True)
    (root / "results" / "real_figures" / "cma_mechanism_heatmap.png").write_bytes(b"png")

    rdd_dir = root / "results" / "literature" / "hs300_rdd"
    rdd_dir.mkdir(parents=True)
    (rdd_dir / "rdd_summary.csv").write_text("outcome\ncar_m1_p1\n", encoding="utf-8")
    (rdd_dir / "rdd_robustness.csv").write_text(
        "spec,spec_kind,tau,p_value,n_obs\n"
        "main · 局部线性,main,0.039,0.048,120\n"
        "donut(±0.01),donut,0.049,0.102,102\n",
        encoding="utf-8",
    )
    (rdd_dir / "rdd_status.csv").write_text(
        "candidate_rows,candidate_batches,as_of_date,coverage_note\n"
        "356,11,2020-11-27 至 2025-11-28,11 个批次覆盖 cutoff 两侧。\n",
        encoding="utf-8",
    )
    (rdd_dir / "figures").mkdir()
    (rdd_dir / "figures" / "car_m1_p1_rdd_main.png").write_bytes(b"png")
    (rdd_dir / "figures" / "rdd_robustness_forest.png").write_bytes(b"png")

    (root / "docs").mkdir()
    (root / "docs" / "paper_outline.md").write_text("# Outline\n", encoding="utf-8")
    (root / "docs" / "pre_registration.md").write_text("# PAP\n", encoding="utf-8")

    (root / "snapshots").mkdir()
    (root / "snapshots" / "pre-registration-2026-05-03.csv").write_text(
        "hid,verdict\nH1,证据不足\n", encoding="utf-8"
    )

    (root / "data" / "raw").mkdir(parents=True)
    (root / "data" / "raw" / "hs300_rdd_candidates.csv").write_text(
        "batch_id,inclusion\ncsi300-2020-11,1\n", encoding="utf-8"
    )


def test_build_paper_bundle_creates_expected_structure(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    result = paper_bundle.build_paper_bundle(root=tmp_path, force=True)

    assert result.dest == tmp_path / "paper"
    assert result.dest.is_dir()
    for sub in ("tables", "figures", "rdd", "narrative", "data"):
        assert (result.dest / sub).is_dir()

    # Tables flatten into paper/tables/
    assert (result.dest / "tables" / "cma_hypothesis_verdicts.tex").exists()
    assert (result.dest / "tables" / "data_sources.tex").exists()
    # Figures
    assert (result.dest / "figures" / "cma_mechanism_heatmap.png").exists()
    # RDD CSVs and figures land flat in paper/rdd/
    assert (result.dest / "rdd" / "rdd_summary.csv").exists()
    assert (result.dest / "rdd" / "rdd_robustness.csv").exists()
    assert (result.dest / "rdd" / "car_m1_p1_rdd_main.png").exists()
    assert (result.dest / "rdd" / "rdd_robustness_forest.png").exists()
    # Narrative + snapshot data
    assert (result.dest / "narrative" / "paper_outline.md").exists()
    assert (result.dest / "narrative" / "pre_registration.md").exists()
    assert (result.dest / "data" / "pre-registration-2026-05-03.csv").exists()
    assert (result.dest / "data" / "hs300_rdd_candidates.csv").exists()


def test_build_paper_bundle_emits_readme_and_summary(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    result = paper_bundle.build_paper_bundle(root=tmp_path, force=True)

    readme = result.readme.read_text(encoding="utf-8")
    assert "Paper Bundle Index" in readme
    assert "tables/" in readme
    assert "rdd/" in readme

    summary = result.summary.read_text(encoding="utf-8")
    assert "研究状态快照" in summary
    # PAP baseline date pulled from snapshot filename
    assert "2026-05-03" in summary
    # Verdict counts from seeded verdicts.csv
    assert "支持" in summary and "证据不足" in summary
    # RDD coverage from rdd_status.csv
    assert "356 行" in summary or "11 批次" in summary
    # RDD main τ from robustness CSV
    assert "3.90%" in summary or "main 局部线性" in summary


def test_build_paper_bundle_force_replaces_existing_dest(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    dest = tmp_path / "paper"
    dest.mkdir()
    stale = dest / "stale_artifact.txt"
    stale.write_text("should be removed", encoding="utf-8")

    paper_bundle.build_paper_bundle(root=tmp_path, dest=dest, force=True)

    assert not stale.exists()
    assert (dest / "tables").is_dir()


def test_build_paper_bundle_handles_missing_rdd_robustness(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    (tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv").unlink()
    result = paper_bundle.build_paper_bundle(root=tmp_path, force=True)
    summary = result.summary.read_text(encoding="utf-8")
    # Without robustness CSV, the RDD main subsection silently disappears
    # but the rest of the summary still renders.
    assert "研究状态快照" in summary
    assert "main 局部线性" not in summary


def test_build_paper_bundle_cli_main_wraps_console_script(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_minimal_project(tmp_path)
    rc = paper_bundle.main(["--dest", str(tmp_path / "paper_out"), "--force"])
    assert rc == 0
    assert (tmp_path / "paper_out" / "README.md").exists()
