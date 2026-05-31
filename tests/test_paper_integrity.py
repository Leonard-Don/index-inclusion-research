"""Tests for ``index_inclusion_research.paper_integrity``.

Each test seeds a tmp project tree with the artifacts the gate inspects,
then runs the cross-document checks (singly or via the orchestrator) to
verify the right scenarios are flagged.

The gate is intentionally read-only, so the fixtures are also disposable
— every test scoped to ``tmp_path`` and ``monkeypatch.setenv`` on
``INDEX_INCLUSION_ROOT`` to keep the real repo untouched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research import paper_integrity

# ---------------------------------------------------------------------------
# Canonical fixture: produces an internally consistent tmp project tree
# whose ``check_paper_integrity`` returns 0 issues. Individual tests then
# mutate one artifact to fabricate the failure scenarios.
# ---------------------------------------------------------------------------


def _make_verdicts_df() -> pd.DataFrame:
    """7-row verdicts CSV with paper_ids covering the whole catalog."""
    # We pack ALL 16 catalog paper_ids across the 7 rows so the references
    # check passes; H1 carries the bulk and the rest get one each.
    catalog_ids = (
        "harris_gurel_1986 | shleifer_1986 | lynch_mendenhall_1997 | "
        "kaul_mehrotra_morck_2000 | denis_et_al_2003 | "
        "wurgler_zhuravskaya_2002 | madhavan_2003 | petajisto_2011 | "
        "kasch_sarkar_2011 | ahn_patatoukas_2022"
    )
    return pd.DataFrame(
        [
            {
                "hid": "H1",
                "name_cn": "信息泄露与预运行",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "core",
                "track": "identification",
                "n_obs": 436,
                "paper_ids": catalog_ids,
                "key_label": "bootstrap p",
                "key_value": 0.8748,
                "p_value": 0.8748,
                "paper_count": 10,
            },
            {
                "hid": "H2",
                "name_cn": "被动基金 AUM 差异",
                "verdict": "部分支持",
                "confidence": "中",
                "evidence_tier": "core",
                "track": "demand_curve",
                "n_obs": 17,
                "paper_ids": "coakley_et_al_2022 | greenwood_sammon_2022",
                "key_label": "US AUM ratio",
                "key_value": 13.48,
                "p_value": "",
                "paper_count": 2,
            },
            {
                "hid": "H3",
                "name_cn": "散户 vs 机构结构",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "supplementary",
                "track": "price_pressure",
                "n_obs": 4,
                "paper_ids": "chang_hong_liskovich_2014",
                "key_label": "双通道命中率",
                "key_value": 0.75,
                "p_value": "",
                "paper_count": 1,
            },
            {
                "hid": "H4",
                "name_cn": "卖空约束",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "supplementary",
                "track": "identification",
                "n_obs": 436,
                "paper_ids": "chu_et_al_2021",
                "key_label": "regression p",
                "key_value": 0.5366,
                "p_value": 0.5366,
                "paper_count": 1,
            },
            {
                "hid": "H5",
                "name_cn": "涨跌停限制",
                "verdict": "支持",
                "confidence": "高",
                "evidence_tier": "core",
                "track": "identification",
                "n_obs": 936,
                "paper_ids": "yao_zhang_li_hs300 | yao_zhou_chen_2022",
                "key_label": "limit_coef p",
                "key_value": 0.008,
                "p_value": 0.008,
                "paper_count": 2,
            },
            {
                "hid": "H6",
                "name_cn": "指数权重可预测性",
                "verdict": "证据不足",
                "confidence": "中",
                "evidence_tier": "supplementary",
                "track": "demand_curve",
                "n_obs": 67,
                "paper_ids": "shleifer_1986",
                "key_label": "heavy−light spread",
                "key_value": -0.019,
                "p_value": "",
                "paper_count": 1,
            },
            {
                "hid": "H7",
                "name_cn": "行业结构差异",
                "verdict": "支持",
                "confidence": "中",
                "evidence_tier": "core",
                "track": "identification",
                "n_obs": 187,
                "paper_ids": "madhavan_2003",
                "key_label": "US sector spread",
                "key_value": 5.95,
                "p_value": "",
                "paper_count": 1,
            },
        ]
    )


def _make_pap_report_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"hid": h, "classification": "unchanged"}
            for h in ("H1", "H2", "H3", "H4", "H5", "H6", "H7")
        ]
    )


def _make_public_summary() -> dict:
    return {
        "pap_deviation_summary": {
            "all_unchanged": True,
            "unchanged_count": 7,
            "flipped_count": 0,
            "tightened_count": 0,
            "weakened_count": 0,
            "unverifiable_count": 0,
        },
        "sensitivity_robustness": {
            "threshold_axis": {
                "cell_count": 4,
                "stable_count": 7,
                "thresholds_tested": [0.05, 0.1, 0.15, 0.2],
            },
            "ar_engine_axis": {
                "cell_count": 2,
                "stable_count": 5,
                "ar_models_tested": ["adjusted", "market"],
            },
            "two_dimensional": {
                "cell_count": 8,
                "stable_count": 5,
            },
        },
    }


def _make_skeleton_text() -> str:
    # All 16 catalog paper_ids appear as "paper_id=<id>" so references check
    # passes. Both H1..H7 verdict table and PAP §7 unchanged table are present.
    from index_inclusion_research.literature_catalog import PAPER_LIBRARY

    refs = "\n".join(f"- paper_id={p.paper_id}" for p in PAPER_LIBRARY)
    return (
        "# Paper\n\n"
        "### 3.3 假说\n\n"
        "| hid | name | verdict |\n"
        "|---|---|---|\n"
        "| H1 | x | x |\n"
        "| H2 | x | x |\n"
        "| H3 | x | x |\n"
        "| H4 | x | x |\n"
        "| H5 | x | x |\n"
        "| H6 | x | x |\n"
        "| H7 | x | x |\n\n"
        "## 4 Figures\n\n"
        "![Fig 1](../results/figures/cma_verdicts_forest.png)\n"
        "![Fig 2](../results/figures/hs300_rdd_robustness_forest.png)\n\n"
        "## 7 PAP\n\n"
        "| 假说 | 名称 | 分类 | baseline | current |\n"
        "|---|---|---|---|---|\n"
        "| H1 | x | unchanged | y | y |\n"
        "| H2 | x | unchanged | y | y |\n"
        "| H3 | x | unchanged | y | y |\n"
        "| H4 | x | unchanged | y | y |\n"
        "| H5 | x | unchanged | y | y |\n"
        "| H6 | x | unchanged | y | y |\n"
        "| H7 | x | unchanged | y | y |\n\n"
        "## References\n\n" + refs + "\n"
    )


def _make_methodology_text() -> str:
    # Sample sizes match the verdicts CSV exactly.
    # Sensitivity stable counts match the public summary exactly.
    return (
        "# Methodology\n\n"
        "## 1. 样本规模\n\n"
        "| 假说 | 名称 | n_obs | 证据层级 | 主线 |\n"
        "|---|---|---:|---|---|\n"
        "| H1 | 信息泄露与预运行 | 436 | core | identification |\n"
        "| H2 | 被动基金 AUM 差异 | 17 | core | demand_curve |\n"
        "| H3 | 散户 vs 机构结构 | 4 | supplementary | price_pressure |\n"
        "| H4 | 卖空约束 | 436 | supplementary | identification |\n"
        "| H5 | 涨跌停限制 | 936 | core | identification |\n"
        "| H6 | 指数权重可预测性 | 67 | supplementary | demand_curve |\n"
        "| H7 | 行业结构差异 | 187 | core | identification |\n\n"
        "## 3. 稳健性覆盖\n\n"
        "| 轴 | 范围 | 假说稳定数 |\n"
        "|---|---|---|\n"
        "| 阈值 | 0.05 / 0.1 / 0.15 / 0.2 | 7/7 |\n"
        "| AR 引擎 | adjusted / market | 5/7 |\n"
        "| 联合 | 8 cells | 5/7 |\n"
    )


def _make_pyproject_text(n_scripts: int = 42) -> str:
    scripts = "\n".join(
        f'script-{i} = "x.module:f"' for i in range(n_scripts)
    )
    return (
        "[project]\nname = \"x\"\nversion = \"0\"\n\n"
        "[project.scripts]\n" + scripts + "\n"
    )


def _make_readme_text(n_scripts: int = 42) -> str:
    return (
        "# Project\n"
        f"![CLI](https://img.shields.io/badge/CLI-{n_scripts}%20commands-2da44e)\n"
    )


def _make_cli_reference_text(n_doctor_checks: int) -> str:
    return f"# CLI reference\n\nDoctor exposes {n_doctor_checks} doctor checks.\n"


@pytest.fixture
def consistent_project(tmp_path: Path, monkeypatch) -> Path:
    """Seed a tmp project tree where every cross-document check passes."""
    # Verdicts CSV
    real_tables = tmp_path / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _make_verdicts_df().to_csv(
        real_tables / "cma_hypothesis_verdicts.csv", index=False
    )
    _make_pap_report_df().to_csv(
        real_tables / "pap_deviation_report.csv", index=False
    )
    # Power analysis report (H3 + H6 minimal rows for the cross-doc gate).
    pd.DataFrame(
        [
            {
                "hid": "H3",
                "name_cn": "散户 vs 机构结构",
                "n_obs": 4,
                "test_family": "binomial_proportion_z_two_sided",
                "power_at_observed": 0.13,
                "mde_at_80_power": 0.50,
                "mde_label": "proportion_gap_p1_minus_p0",
            },
            {
                "hid": "H6",
                "name_cn": "指数权重可预测性",
                "n_obs": 67,
                "test_family": "one_sample_t_two_sided",
                "power_at_observed": 1.00,
                "mde_at_80_power": 0.35,
                "mde_label": "cohens_d_at_target_power",
            },
        ]
    ).to_csv(real_tables / "power_analysis_report.csv", index=False)

    # Paper artifacts
    paper_dir = tmp_path / "paper"
    paper_dir.mkdir()
    (paper_dir / "skeleton.md").write_text(_make_skeleton_text(), encoding="utf-8")
    (paper_dir / "methodology_summary.md").write_text(
        _make_methodology_text(), encoding="utf-8"
    )

    # Figures
    figures_dir = tmp_path / "results" / "figures"
    figures_dir.mkdir(parents=True)
    for name in (
        "cma_verdicts_forest.png",
        "hs300_rdd_robustness_forest.png",
    ):
        (figures_dir / name).write_bytes(b"PNG")

    # Public summary
    public_dir = tmp_path / "data" / "public"
    public_dir.mkdir(parents=True)
    (public_dir / "index_research_summary.json").write_text(
        json.dumps(_make_public_summary()), encoding="utf-8"
    )

    # pyproject + README
    (tmp_path / "pyproject.toml").write_text(
        _make_pyproject_text(42), encoding="utf-8"
    )
    (tmp_path / "README.md").write_text(
        _make_readme_text(42), encoding="utf-8"
    )

    # cli_reference matches actual doctor count
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    from index_inclusion_research.doctor import DEFAULT_CHECKS

    (docs_dir / "cli_reference.md").write_text(
        _make_cli_reference_text(len(DEFAULT_CHECKS)), encoding="utf-8"
    )

    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_consistent_project_returns_zero_issues(consistent_project: Path) -> None:
    """All checks pass on a fabricated mutually-consistent project tree."""
    issues = paper_integrity.check_paper_integrity()
    assert paper_integrity.integrity_summary(issues) == {
        "info": 0,
        "warn": 0,
        "fail": 0,
        "total": 0,
    }
    assert paper_integrity.integrity_exit_code(issues) == 0
    assert paper_integrity.integrity_exit_code(issues, fail_on_warn=True) == 0


def test_six_h_rows_instead_of_seven_fails(consistent_project: Path) -> None:
    """Verdict CSV with 6 H rows triggers a hypothesis_set fail."""
    verdicts_path = (
        consistent_project / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    )
    df = pd.read_csv(verdicts_path)
    df = df.loc[df["hid"] != "H7"].copy()  # drop H7
    df.to_csv(verdicts_path, index=False)

    issues = paper_integrity.check_paper_integrity()
    fails = [i for i in issues if i.severity == "fail"]
    assert any(i.category == "hypothesis_set" for i in fails), (
        f"expected hypothesis_set fail, got: {[(i.category, i.severity) for i in issues]}"
    )
    assert paper_integrity.integrity_exit_code(issues) == 2


def test_missing_referenced_figure_fails(consistent_project: Path) -> None:
    """A skeleton figure reference with no on-disk file triggers figures fail."""
    (consistent_project / "results" / "figures" / "cma_verdicts_forest.png").unlink()

    issues = paper_integrity.check_paper_integrity()
    assert any(
        i.category == "figures" and i.severity == "fail" for i in issues
    ), [
        (i.category, i.severity, i.description) for i in issues
    ]


def test_readme_badge_999_scripts_fails(consistent_project: Path) -> None:
    """README claiming the wrong CLI script count triggers cli_count fail."""
    (consistent_project / "README.md").write_text(
        _make_readme_text(999), encoding="utf-8"
    )

    issues = paper_integrity.check_paper_integrity()
    cli_fails = [
        i for i in issues if i.category == "cli_count" and i.severity == "fail"
    ]
    assert cli_fails
    assert "999" in cli_fails[0].description


def test_mismatched_pap_classification_fails(consistent_project: Path) -> None:
    """Per-row PAP classifications disagreeing with public summary triggers fail."""
    pap_path = (
        consistent_project / "results" / "real_tables" / "pap_deviation_report.csv"
    )
    df = pd.read_csv(pap_path)
    # Flip one row's classification — public summary still claims 7 unchanged.
    df.loc[df["hid"] == "H1", "classification"] = "flipped"
    df.to_csv(pap_path, index=False)

    issues = paper_integrity.check_paper_integrity()
    pap_fails = [
        i for i in issues if i.category == "pap" and i.severity == "fail"
    ]
    assert pap_fails
    # Expect both the public-summary mismatch and skeleton mismatch surfaces.
    descriptions = " ".join(i.description for i in pap_fails)
    assert "pap_deviation_report" in descriptions.lower() or "PAP" in descriptions


def test_paper_count_drift_fails(consistent_project: Path) -> None:
    """A skeleton missing some paper_ids triggers references fail."""
    # Rewrite skeleton dropping the last few paper_ids
    from index_inclusion_research.literature_catalog import PAPER_LIBRARY

    refs = "\n".join(
        f"- paper_id={p.paper_id}" for p in PAPER_LIBRARY[:10]
    )  # drop 6 papers
    skeleton = _make_skeleton_text()
    head = skeleton.split("## References", 1)[0]
    (consistent_project / "paper" / "skeleton.md").write_text(
        head + "## References\n\n" + refs + "\n", encoding="utf-8"
    )

    issues = paper_integrity.check_paper_integrity()
    ref_fails = [
        i for i in issues if i.category == "references" and i.severity == "fail"
    ]
    assert ref_fails


def test_sample_size_mismatch_fails(consistent_project: Path) -> None:
    """Methodology summary n_obs disagreeing with verdicts CSV triggers fail."""
    methodology_path = (
        consistent_project / "paper" / "methodology_summary.md"
    )
    body = methodology_path.read_text(encoding="utf-8")
    # Replace H1's 436 with a wrong number.
    new_body = body.replace(
        "| H1 | 信息泄露与预运行 | 436 |",
        "| H1 | 信息泄露与预运行 | 999 |",
    )
    methodology_path.write_text(new_body, encoding="utf-8")

    issues = paper_integrity.check_paper_integrity()
    sample_fails = [
        i for i in issues if i.category == "sample_sizes" and i.severity == "fail"
    ]
    assert sample_fails
    assert any("H1" in ev for ev in sample_fails[0].evidence)


def test_fail_on_warn_returns_exit_one_for_warn_only(
    consistent_project: Path,
) -> None:
    """A warn-only run should exit 0 normally but 1 under --fail-on-warn."""
    # Mutate cli_reference doctor count to a wrong number -> warn-level issue.
    (consistent_project / "docs" / "cli_reference.md").write_text(
        _make_cli_reference_text(9999), encoding="utf-8"
    )

    issues = paper_integrity.check_paper_integrity()
    warns = [i for i in issues if i.severity == "warn"]
    fails = [i for i in issues if i.severity == "fail"]
    assert warns, "expected at least one warn"
    assert not fails, f"expected no fails, got: {[i.description for i in fails]}"
    assert paper_integrity.integrity_exit_code(issues) == 1
    assert (
        paper_integrity.integrity_exit_code(issues, fail_on_warn=True) == 1
    )


def test_main_exits_zero_on_consistent_project(
    consistent_project: Path, capsys
) -> None:
    """``main()`` returns 0 and prints a 'passed' line on a clean project."""
    rc = paper_integrity.main([])
    captured = capsys.readouterr()
    assert rc == 0
    assert "all cross-document checks passed" in captured.out


def test_main_json_format_emits_parseable_payload(
    consistent_project: Path, capsys
) -> None:
    """``--format json`` emits parseable JSON with the documented schema."""
    rc = paper_integrity.main(["--format", "json"])
    captured = capsys.readouterr()
    assert rc == 0
    payload = json.loads(captured.out)
    assert "summary" in payload
    assert "issues" in payload
    assert payload["summary"]["total"] == 0


def test_real_repo_run_exits_zero() -> None:
    """The real repo passes the integrity gate today (regression guard)."""
    # No env override: this exercises the real artifact tree under the
    # repo root resolved through paths.project_root().
    issues = paper_integrity.check_paper_integrity()
    assert paper_integrity.integrity_exit_code(issues) == 0, (
        "real repo paper-integrity should pass; issues: "
        + repr([(i.severity, i.category, i.description) for i in issues])
    )


# ---------------------------------------------------------------------------
# README front-page verdict table ↔ cma_hypothesis_verdicts.csv
#
# These are fully hermetic: they build a tiny fake repo tree under tmp_path
# (a minimal README.md + a minimal cma_hypothesis_verdicts.csv) and never
# touch the real repo files.
# ---------------------------------------------------------------------------


def _make_readme_verdicts_text(verdicts: dict[str, str]) -> str:
    """Render a front-page 7-hypothesis verdict table for the README.

    ``verdicts`` maps ``hid`` -> README verdict cell. The table mimics the
    real front-page layout (with a header + separator and trailing detail
    columns) so the parser must defensively skip non-H rows.
    """
    rows = "\n".join(
        f"| {hid} | 名称{hid} | {verdict} | 细节 {hid} (n=10) | 正文 core | 制度识别 |"
        for hid, verdict in verdicts.items()
    )
    return (
        "# Project\n\n"
        "## 七大假说裁定\n\n"
        "| 假说 | 名称 | 裁定 | 关键证据 | 位置 | 主线 |\n"
        "|---|---|---|---|---|---|\n"
        f"{rows}\n\n"
        "其它正文。\n"
    )


def _write_readme_verdicts_repo(
    tmp_path: Path,
    *,
    readme_verdicts: dict[str, str],
    csv_verdicts: dict[str, str],
) -> Path:
    """Seed a tiny repo tree with just the README + verdicts CSV under tmp_path."""
    real_tables = tmp_path / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    pd.DataFrame(
        [{"hid": hid, "verdict": verdict} for hid, verdict in csv_verdicts.items()]
    ).to_csv(real_tables / "cma_hypothesis_verdicts.csv", index=False)

    (tmp_path / "README.md").write_text(
        _make_readme_verdicts_text(readme_verdicts), encoding="utf-8"
    )
    return tmp_path


def test_check_readme_verdicts_match_csv_flags_mismatch(tmp_path: Path) -> None:
    """README says H5=支持 but CSV says 证据不足 -> issues list flags H5."""
    repo = _write_readme_verdicts_repo(
        tmp_path,
        readme_verdicts={
            "H1": "证据不足",
            "H5": "支持",  # drifted away from the CSV
        },
        csv_verdicts={
            "H1": "证据不足",
            "H5": "证据不足",
        },
    )

    issues = paper_integrity.check_readme_verdicts_match_csv(repo)
    assert isinstance(issues, list)
    assert any("H5" in issue for issue in issues), issues
    # The non-drifted hid must NOT be reported.
    assert not any("H1" in issue for issue in issues), issues


def test_check_readme_verdicts_match_csv_passes_when_aligned(tmp_path: Path) -> None:
    """When every README verdict equals the CSV verdict, issues == []."""
    repo = _write_readme_verdicts_repo(
        tmp_path,
        readme_verdicts={
            "H1": "证据不足",
            "H2": "部分支持",
            "H5": "证据不足",
        },
        csv_verdicts={
            "H1": "证据不足",
            "H2": "部分支持",
            "H5": "证据不足",
        },
    )

    assert paper_integrity.check_readme_verdicts_match_csv(repo) == []
