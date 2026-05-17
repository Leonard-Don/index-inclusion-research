from __future__ import annotations

import hashlib
import json
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
    # PAP deviation report (commit 48a22f0) — bundled like patell_bmp_summary.
    (root / "results" / "real_tables" / "pap_deviation_report.csv").write_text(
        "hid,classification,baseline_verdict,current_verdict,notes\n"
        "H1,unchanged,证据不足,证据不足,\n"
        "H2,flipped,证据不足,部分支持,verdict 证据不足 → 部分支持\n",
        encoding="utf-8",
    )
    (root / "results" / "real_event_study").mkdir(parents=True)
    (root / "results" / "real_event_study" / "patell_bmp_summary.csv").write_text(
        "market,event_phase,inclusion,window_slug,patell_p,bmp_p\n"
        "CN,announce,1,m1_p1,0.001,0.002\n",
        encoding="utf-8",
    )

    (root / "results" / "real_figures").mkdir(parents=True)
    (root / "results" / "real_figures" / "cma_mechanism_heatmap.png").write_bytes(b"png")

    # Paper-grade forest plots live under results/figures/ (commits d1b70a1
    # and f0c2260). Seed both PNG and PDF so the bundle's PNG glob + PDF
    # explicit_files both find a hit.
    (root / "results" / "figures").mkdir(parents=True)
    (root / "results" / "figures" / "hs300_rdd_robustness_forest.png").write_bytes(b"png")
    (root / "results" / "figures" / "hs300_rdd_robustness_forest.pdf").write_bytes(b"%PDF")
    (root / "results" / "figures" / "cma_verdicts_forest.png").write_bytes(b"png")
    (root / "results" / "figures" / "cma_verdicts_forest.pdf").write_bytes(b"%PDF")
    (root / "results" / "figures" / "cma_verdicts_2d_robustness.png").write_bytes(b"png")
    (root / "results" / "figures" / "cma_verdicts_2d_robustness.pdf").write_bytes(b"%PDF")

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
        "356,11,2020-11-27 至 2025-11-28,11 个批次覆盖断点两侧。\n",
        encoding="utf-8",
    )
    (rdd_dir / "figures").mkdir()
    (rdd_dir / "figures" / "car_m1_p1_rdd_main.png").write_bytes(b"png")
    (rdd_dir / "figures" / "rdd_robustness_forest.png").write_bytes(b"png")

    (root / "docs").mkdir()
    (root / "docs" / "paper_outline.md").write_text("# Outline\n", encoding="utf-8")
    (root / "docs" / "research_delivery_package.md").write_text(
        "# Delivery\n", encoding="utf-8"
    )
    (root / "docs" / "pre_registration.md").write_text("# PAP\n", encoding="utf-8")

    (root / "snapshots").mkdir()
    (root / "snapshots" / "pre-registration-2026-05-03.csv").write_text(
        "hid,verdict\nH1,证据不足\n", encoding="utf-8"
    )

    (root / "data" / "raw").mkdir(parents=True)
    (root / "data" / "raw" / "hs300_rdd_candidates.csv").write_text(
        "batch_id,inclusion\ncsi300-2020-11,1\n", encoding="utf-8"
    )


def _build_no_regen(root: Path, **kwargs: object) -> paper_bundle.BundleResult:
    """Run the bundle without the pre-copy regeneration step.

    The default ``build_paper_bundle`` regenerates forest plots + the
    PAP audit before copying, but the seeded fixtures here use stub
    bytes (``b"png"``) for the forest plots — calling the real plotter
    on those bytes would overwrite them with a valid PNG and change
    the expected hashes. Tests assert behavior of the *copy* layer,
    so we always disable regeneration here.
    """
    return paper_bundle.build_paper_bundle(
        root=root, regenerate=False, **kwargs  # type: ignore[arg-type]
    )


def test_build_paper_bundle_creates_expected_structure(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    result = _build_no_regen(tmp_path, force=True)

    assert result.dest == tmp_path / "paper"
    assert result.dest.is_dir()
    for sub in ("tables", "figures", "rdd", "narrative", "data"):
        assert (result.dest / sub).is_dir()

    # Tables flatten into paper/tables/
    assert (result.dest / "tables" / "cma_hypothesis_verdicts.tex").exists()
    assert (result.dest / "tables" / "data_sources.tex").exists()
    assert (result.dest / "tables" / "patell_bmp_summary.csv").exists()
    # PAP deviation report (commit 48a22f0)
    assert (result.dest / "tables" / "pap_deviation_report.csv").exists()
    # Figures
    assert (result.dest / "figures" / "cma_mechanism_heatmap.png").exists()
    # New paper-grade forest plots from results/figures/
    assert (result.dest / "figures" / "hs300_rdd_robustness_forest.png").exists()
    assert (result.dest / "figures" / "hs300_rdd_robustness_forest.pdf").exists()
    assert (result.dest / "figures" / "cma_verdicts_forest.png").exists()
    assert (result.dest / "figures" / "cma_verdicts_forest.pdf").exists()
    assert (result.dest / "figures" / "cma_verdicts_2d_robustness.png").exists()
    assert (result.dest / "figures" / "cma_verdicts_2d_robustness.pdf").exists()
    # RDD CSVs and figures land flat in paper/rdd/
    assert (result.dest / "rdd" / "rdd_summary.csv").exists()
    assert (result.dest / "rdd" / "rdd_robustness.csv").exists()
    assert (result.dest / "rdd" / "car_m1_p1_rdd_main.png").exists()
    assert (result.dest / "rdd" / "rdd_robustness_forest.png").exists()
    # Narrative + snapshot data
    assert (result.dest / "narrative" / "paper_outline.md").exists()
    assert (result.dest / "narrative" / "research_delivery_package.md").exists()
    assert (result.dest / "narrative" / "pre_registration.md").exists()
    assert (result.dest / "data" / "pre-registration-2026-05-03.csv").exists()
    assert (result.dest / "data" / "hs300_rdd_candidates.csv").exists()


def test_build_paper_bundle_emits_readme_and_summary(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    result = _build_no_regen(tmp_path, force=True)

    readme = result.readme.read_text(encoding="utf-8")
    assert "Paper Bundle Index" in readme
    assert "tables/" in readme
    assert "rdd/" in readme
    # README footer points readers at the new manifest.
    assert "manifest.json" in readme
    # The new forest-plot artifacts must appear in the index, not just
    # on disk — keeps `paper/README.md` accurate when shared standalone.
    assert "hs300_rdd_robustness_forest.png" in readme
    assert "cma_verdicts_forest.png" in readme
    assert "cma_verdicts_2d_robustness.png" in readme
    assert "pap_deviation_report.csv" in readme

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

    _build_no_regen(tmp_path, dest=dest, force=True)

    assert not stale.exists()
    assert (dest / "tables").is_dir()


def test_build_paper_bundle_handles_missing_rdd_robustness(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    (tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv").unlink()
    result = _build_no_regen(tmp_path, force=True)
    summary = result.summary.read_text(encoding="utf-8")
    # Without robustness CSV, the RDD main subsection silently disappears
    # but the rest of the summary still renders.
    assert "研究状态快照" in summary
    assert "main 局部线性" not in summary


def test_build_paper_bundle_cli_main_wraps_console_script(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_minimal_project(tmp_path)
    rc = paper_bundle.main(
        [
            "--dest",
            str(tmp_path / "paper_out"),
            "--force",
            # CLI tests run against synthetic fixtures, so skip the real
            # forest renderer (it would overwrite seeded PNG stubs).
            "--no-regenerate",
        ]
    )
    assert rc == 0
    assert (tmp_path / "paper_out" / "README.md").exists()
    assert (tmp_path / "paper_out" / "manifest.json").exists()


# ── Manifest tests ───────────────────────────────────────────────────


def test_build_paper_bundle_writes_manifest(tmp_path: Path) -> None:
    _seed_minimal_project(tmp_path)
    result = _build_no_regen(tmp_path, force=True)

    assert result.manifest.exists()
    payload = json.loads(result.manifest.read_text(encoding="utf-8"))

    # Top-level schema fields the consumers (paper-audit, CI) rely on.
    assert payload["bundle_label"] == "index-inclusion-paper-bundle"
    assert payload["manifest_schema_version"] == 1
    assert payload["artifact_count"] == len(payload["artifacts"])
    assert payload["artifact_count"] > 0
    # ``regenerated`` is present but empty when called with regenerate=False.
    assert payload["regenerated"] == {}

    # Every artifact entry has the contract fields.
    for entry in payload["artifacts"]:
        assert set(entry.keys()) >= {
            "section",
            "source",
            "target",
            "sha256",
            "size_bytes",
        }
        # sha256 is 64 hex chars.
        assert len(entry["sha256"]) == 64
        assert entry["size_bytes"] >= 0


def test_manifest_hashes_match_copied_files(tmp_path: Path) -> None:
    """Every manifest entry's sha256 must match the byte content of the
    file actually staged in the bundle.

    Guards against the regression where the manifest is written but
    fails to update when the underlying copy layer changes the staged
    bytes (e.g. line-ending normalization, accidental BOM rewrite).
    """
    _seed_minimal_project(tmp_path)
    result = _build_no_regen(tmp_path, force=True)
    payload = json.loads(result.manifest.read_text(encoding="utf-8"))

    for entry in payload["artifacts"]:
        target = result.dest / entry["target"]
        assert target.exists(), f"manifest references missing file: {entry['target']}"
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        assert actual == entry["sha256"], (
            f"hash mismatch for {entry['target']}: "
            f"manifest={entry['sha256']} actual={actual}"
        )


def test_manifest_lists_new_visualization_artifacts(tmp_path: Path) -> None:
    """The manifest must explicitly enumerate the three new artifacts
    introduced in commits 48a22f0 / d1b70a1 / f0c2260."""
    _seed_minimal_project(tmp_path)
    result = _build_no_regen(tmp_path, force=True)
    payload = json.loads(result.manifest.read_text(encoding="utf-8"))

    targets = {entry["target"] for entry in payload["artifacts"]}
    assert "figures/hs300_rdd_robustness_forest.png" in targets
    assert "figures/hs300_rdd_robustness_forest.pdf" in targets
    assert "figures/cma_verdicts_forest.png" in targets
    assert "figures/cma_verdicts_forest.pdf" in targets
    assert "figures/cma_verdicts_2d_robustness.png" in targets
    assert "figures/cma_verdicts_2d_robustness.pdf" in targets
    assert "tables/pap_deviation_report.csv" in targets


# ── Regeneration tests ───────────────────────────────────────────────


def _seed_for_regen(root: Path) -> None:
    """Seed the inputs the regeneration step reads.

    Uses the canonical (real) robustness CSV columns so
    ``build_hs300_rdd_forest_plot`` can render without raising. The
    seeded H1-H7 verdicts CSV has all 7 hypotheses so
    ``build_cma_verdicts_forest_plot`` validates as complete.
    """
    _seed_minimal_project(root)
    # Real robustness CSV (with std_error so the forest plotter has CIs).
    (root / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv").write_text(
        "spec,spec_kind,tau,std_error,p_value,n_obs\n"
        "main · 局部线性,main,0.0392,0.0199,0.048,120\n"
        "donut(±0.01),donut,0.049,0.030,0.102,102\n",
        encoding="utf-8",
    )
    # Full 7-hypothesis verdicts CSV (CMA forest requires all of H1-H7).
    (root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv").write_text(
        "hid,name_cn,verdict,confidence,evidence_tier,n_obs\n"
        "H1,信息泄露与预运行,证据不足,中,core,436\n"
        "H2,被动基金 AUM 差异,部分支持,中,core,17\n"
        "H3,散户 vs 机构结构,支持,高,supplementary,4\n"
        "H4,卖空约束,证据不足,低,supplementary,40\n"
        "H5,涨跌停限制,支持,高,core,180\n"
        "H6,权重变化,部分支持,中,core,93\n"
        "H7,行业结构,支持,高,core,936\n",
        encoding="utf-8",
    )


def _seed_sensitivity_cache(root: Path) -> Path:
    csv = (
        root
        / "results"
        / "sensitivity"
        / "threshold_0_10"
        / "cma_hypothesis_verdicts.csv"
    )
    csv.parent.mkdir(parents=True, exist_ok=True)
    csv.write_text(
        "hid,name_cn,verdict,confidence,evidence_tier,n_obs\n"
        "H1,信息泄露与预运行,证据不足,中,core,436\n"
        "H2,被动基金 AUM 差异,部分支持,中,core,17\n"
        "H3,散户 vs 机构结构,支持,高,supplementary,4\n"
        "H4,卖空约束,证据不足,中,supplementary,40\n"
        "H5,涨跌停限制,支持,高,core,936\n"
        "H6,指数权重可预测性,证据不足,中,supplementary,67\n"
        "H7,行业结构差异,支持,中,core,187\n",
        encoding="utf-8",
    )
    return csv


def test_regenerate_artifacts_refreshes_forest_plots(tmp_path: Path) -> None:
    """When called with regenerate=True (default), the bundle invokes the
    HS300 RDD + CMA verdict forest plot builders so stale figures from a
    prior `make rebuild` run get refreshed against current CSVs.

    We assert the regeneration step reported 'ok' for each entry, and
    that the resulting PNG file is a real PNG (begins with the magic
    bytes \\x89PNG) rather than the seeded stub we wrote earlier.
    """
    _seed_for_regen(tmp_path)
    # Seed a stub PNG that the regeneration must overwrite.
    figures_dir = tmp_path / "results" / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    (figures_dir / "hs300_rdd_robustness_forest.png").write_bytes(b"stub")
    (figures_dir / "cma_verdicts_forest.png").write_bytes(b"stub")

    result = paper_bundle.build_paper_bundle(
        root=tmp_path, force=True, regenerate=True
    )

    assert result.regenerated["hs300_rdd_forest"] == "ok"
    assert result.regenerated["cma_verdicts_forest"] == "ok"
    assert result.regenerated["pap_deviation_report"] == "ok"

    # Regenerated PNGs are real (not the b"stub" we seeded).
    refreshed_hs = (figures_dir / "hs300_rdd_robustness_forest.png").read_bytes()
    assert refreshed_hs.startswith(b"\x89PNG"), "HS300 forest PNG not regenerated"
    refreshed_cma = (figures_dir / "cma_verdicts_forest.png").read_bytes()
    assert refreshed_cma.startswith(b"\x89PNG"), "CMA verdicts forest PNG not regenerated"

    # PAP deviation report was written from baseline + current verdicts.
    pap_report = tmp_path / "results" / "real_tables" / "pap_deviation_report.csv"
    assert pap_report.exists()
    pap_text = pap_report.read_text(encoding="utf-8")
    assert "classification" in pap_text


def test_regenerate_artifacts_rerenders_sensitivity_from_cache_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_for_regen(tmp_path)
    _seed_sensitivity_cache(tmp_path)

    from index_inclusion_research import outputs

    calls: list[dict[str, object]] = []

    def _cache_only_renderer(**kwargs: object) -> Path:
        calls.append(kwargs)
        png_path = Path(kwargs["output_png_path"])  # type: ignore[arg-type]
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_bytes(b"png")
        pdf_path = Path(kwargs["output_pdf_path"])  # type: ignore[arg-type]
        pdf_path.write_bytes(b"%PDF")
        return png_path

    monkeypatch.setattr(
        outputs,
        "build_cma_sensitivity_forest_plot",
        lambda **kwargs: pytest.fail("fresh CMA sensitivity sweep should not run"),
    )
    monkeypatch.setattr(
        outputs,
        "build_cma_sensitivity_forest_plot_from_cache",
        _cache_only_renderer,
    )

    status = paper_bundle._regenerate_artifacts(tmp_path)

    assert status["cma_verdicts_sensitivity_forest"] == "ok"
    assert calls
    assert calls[0]["sensitivity_root"] == tmp_path / "results" / "sensitivity"


def test_regenerate_artifacts_marks_missing_inputs_as_skipped(tmp_path: Path) -> None:
    """Missing inputs (CSV not present) must not error — they should
    skip the step and the rest of the bundle should still succeed."""
    _seed_minimal_project(tmp_path)
    # Remove the inputs the regen step would read.
    (tmp_path / "results" / "literature" / "hs300_rdd" / "rdd_robustness.csv").unlink()
    (tmp_path / "results" / "real_tables" / "cma_hypothesis_verdicts.csv").unlink()

    result = paper_bundle.build_paper_bundle(
        root=tmp_path, force=True, regenerate=True
    )

    assert result.regenerated["hs300_rdd_forest"] == "skipped"
    assert result.regenerated["cma_verdicts_forest"] == "skipped"
    assert result.regenerated["pap_deviation_report"] == "skipped"
    # The bundle still ran end-to-end despite the missing inputs.
    assert result.manifest.exists()


def test_no_regenerate_flag_keeps_regenerated_empty(tmp_path: Path) -> None:
    """``--no-regenerate`` (or ``regenerate=False``) must short-circuit the
    refresh step so existing files aren't touched. The manifest still
    records an empty regenerated block so downstream consumers can tell."""
    _seed_minimal_project(tmp_path)
    result = paper_bundle.build_paper_bundle(
        root=tmp_path, force=True, regenerate=False
    )
    assert result.regenerated == {}
    payload = json.loads(result.manifest.read_text(encoding="utf-8"))
    assert payload["regenerated"] == {}
