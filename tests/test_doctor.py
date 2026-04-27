"""Tests for ``index_inclusion_research.doctor``."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research.doctor import (
    CheckResult,
    check_chart_builders_register,
    check_console_scripts_importable,
    check_hypothesis_paper_ids_resolve,
    check_results_directory_populated,
    check_verdicts_csv_health,
    main,
    render_results,
    run_all_checks,
)

# ── individual checks ────────────────────────────────────────────────


def test_check_hypothesis_paper_ids_resolve_passes_with_real_catalog() -> None:
    result = check_hypothesis_paper_ids_resolve()
    assert result.status == "pass"
    assert "resolve" in result.message


def test_check_hypothesis_paper_ids_resolve_flags_typos() -> None:
    """Inject a fake catalog missing one paper to trigger the failure path."""

    def _fake_catalog():
        # Real catalog drops e.g. 'shleifer_1986'
        from index_inclusion_research.literature_catalog import (
            build_literature_catalog_frame,
        )
        df = build_literature_catalog_frame()
        return df.loc[df["paper_id"] != "shleifer_1986"].copy()

    result = check_hypothesis_paper_ids_resolve(catalog_loader=_fake_catalog)
    assert result.status == "fail"
    # H6 references shleifer_1986
    assert any("shleifer_1986" in d for d in result.details)


def test_check_verdicts_csv_health_warns_when_missing(tmp_path: Path) -> None:
    result = check_verdicts_csv_health(csv_path=tmp_path / "missing.csv")
    assert result.status == "warn"
    assert "not found" in result.message


def test_check_verdicts_csv_health_fails_when_unparseable(tmp_path: Path) -> None:
    bad = tmp_path / "verdicts.csv"
    bad.write_text("not,a,valid\n\"unclosed,quote,here\n")
    result = check_verdicts_csv_health(csv_path=bad)
    assert result.status in ("fail", "warn")


def test_check_verdicts_csv_health_fails_when_hids_missing(tmp_path: Path) -> None:
    csv = tmp_path / "verdicts.csv"
    pd.DataFrame([{"hid": h} for h in ("H1", "H2", "H3")]).to_csv(csv, index=False)
    result = check_verdicts_csv_health(csv_path=csv)
    assert result.status == "fail"
    assert any("missing" in d for d in result.details)


def test_check_verdicts_csv_health_passes_with_seven_hids(tmp_path: Path) -> None:
    csv = tmp_path / "verdicts.csv"
    pd.DataFrame(
        [{"hid": f"H{i}"} for i in range(1, 8)]
    ).to_csv(csv, index=False)
    result = check_verdicts_csv_health(csv_path=csv)
    assert result.status == "pass"


def test_check_results_directory_populated_warns_when_missing(tmp_path: Path) -> None:
    result = check_results_directory_populated(
        results_dir=tmp_path / "no-such-dir",
        expected_files=("foo.csv",),
    )
    assert result.status == "warn"


def test_check_results_directory_populated_warns_when_partial(tmp_path: Path) -> None:
    (tmp_path / "alpha.csv").write_text("x\n")
    result = check_results_directory_populated(
        results_dir=tmp_path,
        expected_files=("alpha.csv", "beta.csv"),
    )
    assert result.status == "warn"
    assert "beta.csv" in result.details


def test_check_results_directory_populated_passes_when_complete(tmp_path: Path) -> None:
    (tmp_path / "alpha.csv").write_text("x\n")
    (tmp_path / "beta.csv").write_text("y\n")
    result = check_results_directory_populated(
        results_dir=tmp_path,
        expected_files=("alpha.csv", "beta.csv"),
    )
    assert result.status == "pass"


def test_check_chart_builders_register_passes_with_real_module() -> None:
    result = check_chart_builders_register(expected_min=12)
    assert result.status == "pass"


def test_check_chart_builders_register_warns_when_too_few() -> None:
    # An impossibly high threshold to force the warn path
    result = check_chart_builders_register(expected_min=999)
    assert result.status == "warn"


def test_check_console_scripts_importable_passes() -> None:
    result = check_console_scripts_importable()
    assert result.status == "pass"


# ── orchestration ────────────────────────────────────────────────────


def test_run_all_checks_returns_one_result_per_check() -> None:
    results = run_all_checks()
    assert len(results) >= 5
    for r in results:
        assert isinstance(r, CheckResult)
        assert r.status in ("pass", "warn", "fail")
        assert r.name and r.message


def test_run_all_checks_swallows_check_exceptions() -> None:
    def _exploding():
        raise RuntimeError("synthetic explosion")

    results = run_all_checks(checks=[_exploding])
    assert len(results) == 1
    assert results[0].status == "fail"
    assert "synthetic explosion" in results[0].message


def test_render_results_includes_glyphs_and_summary_line() -> None:
    results = [
        CheckResult(name="alpha", status="pass", message="ok"),
        CheckResult(name="beta", status="warn", message="hmm", fix="do X"),
        CheckResult(
            name="gamma", status="fail", message="bad",
            fix="run Y", details=("d1", "d2"),
        ),
    ]
    text = render_results(results, color=False)
    assert "alpha" in text and "beta" in text and "gamma" in text
    assert "1 pass · 1 warn · 1 fail" in text
    assert "do X" in text
    assert "run Y" in text
    assert "d1" in text and "d2" in text


def test_render_results_no_color_suppresses_ansi() -> None:
    text = render_results(
        [CheckResult(name="x", status="pass", message="ok")], color=False,
    )
    assert "\033[" not in text


def test_render_results_color_includes_ansi() -> None:
    text = render_results(
        [CheckResult(name="x", status="pass", message="ok")], color=True,
    )
    assert "\033[" in text


# ── CLI ──────────────────────────────────────────────────────────────


def test_main_returns_zero_when_all_checks_pass(capsys) -> None:
    rc = main(["--no-color"])
    captured = capsys.readouterr().out
    assert "doctor" in captured
    # In a healthy repo, all checks should pass and return 0.
    assert rc == 0
