from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

from index_inclusion_research import cli
from index_inclusion_research.dashboard_bootstrap import bootstrap_dashboard_paths
from index_inclusion_research.literature_dashboard import (
    parse_dashboard_args,
    run_dashboard_app,
)

ROOT = Path(__file__).resolve().parents[1]


def test_bootstrap_dashboard_paths_resolves_repo_layout() -> None:
    current_file = Path("/tmp/example/src/index_inclusion_research/dashboard_app.py")
    paths = bootstrap_dashboard_paths(current_file)
    expected_root = current_file.resolve().parents[2]
    expected_web = expected_root / "src" / "index_inclusion_research" / "web"

    assert paths.root == expected_root
    assert paths.src == expected_root / "src"
    assert paths.templates == expected_web / "templates"
    assert paths.static == expected_web / "static"
    assert str(paths.src) in sys.path


def test_parse_dashboard_args_accepts_host_and_port() -> None:
    args = parse_dashboard_args(["--host", "0.0.0.0", "--port", "5012"])

    assert args.host == "0.0.0.0"
    assert args.port == 5012


def test_run_dashboard_app_prints_localhost_tip_and_forwards_host_port(capsys) -> None:
    calls: list[dict[str, object]] = []

    class DummyApp:
        def run(self, **kwargs) -> None:
            calls.append(kwargs)

    run_dashboard_app(DummyApp(), ["--host", "127.0.0.1", "--port", "5013"])

    captured = capsys.readouterr().out
    assert "http://localhost:5013" in captured
    assert "Firefox 对 localhost 的兼容性更稳定" in captured
    assert calls == [{"host": "127.0.0.1", "port": 5013, "debug": False}]


def test_project_metadata_declares_flask_and_console_scripts() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]

    assert "flask>=3.0" in project["dependencies"]
    assert "yfinance>=0.2" in project["dependencies"]
    assert project["scripts"]["index-inclusion-dashboard"] == "index_inclusion_research.cli:run_dashboard_main"
    assert project["scripts"]["index-inclusion-build-event-sample"] == "index_inclusion_research.cli:run_build_event_sample_main"
    assert project["scripts"]["index-inclusion-build-price-panel"] == "index_inclusion_research.cli:run_build_price_panel_main"
    assert project["scripts"]["index-inclusion-match-controls"] == "index_inclusion_research.cli:run_match_controls_main"
    assert (
        project["scripts"]["index-inclusion-match-robustness"]
        == "index_inclusion_research.cli:run_match_robustness_main"
    )
    assert project["scripts"]["index-inclusion-run-event-study"] == "index_inclusion_research.cli:run_event_study_main"
    assert project["scripts"]["index-inclusion-run-regressions"] == "index_inclusion_research.cli:run_regressions_main"
    assert project["scripts"]["index-inclusion-price-pressure"] == "index_inclusion_research.cli:run_price_pressure_track_main"
    assert project["scripts"]["index-inclusion-demand-curve"] == "index_inclusion_research.cli:run_demand_curve_track_main"
    assert project["scripts"]["index-inclusion-identification"] == "index_inclusion_research.cli:run_identification_china_track_main"
    assert project["scripts"]["index-inclusion-hs300-rdd"] == "index_inclusion_research.cli:run_hs300_rdd_main"
    assert (
        project["scripts"]["index-inclusion-prepare-hs300-rdd"]
        == "index_inclusion_research.cli:run_prepare_hs300_rdd_candidates_main"
    )
    assert (
        project["scripts"]["index-inclusion-reconstruct-hs300-rdd"]
        == "index_inclusion_research.cli:run_reconstruct_hs300_rdd_candidates_main"
    )
    assert (
        project["scripts"]["index-inclusion-collect-hs300-rdd-l3"]
        == "index_inclusion_research.cli:run_collect_hs300_rdd_l3_main"
    )
    assert (
        project["scripts"]["index-inclusion-generate-sample-data"]
        == "index_inclusion_research.cli:run_generate_sample_data_main"
    )
    assert (
        project["scripts"]["index-inclusion-download-real-data"]
        == "index_inclusion_research.cli:run_download_real_data_main"
    )
    assert (
        project["scripts"]["index-inclusion-make-figures-tables"]
        == "index_inclusion_research.cli:run_make_figures_tables_main"
    )
    assert (
        project["scripts"]["index-inclusion-generate-research-report"]
        == "index_inclusion_research.cli:run_generate_research_report_main"
    )
    assert (
        project["scripts"]["index-inclusion-refresh-real-evidence"]
        == "index_inclusion_research.cli:run_refresh_real_evidence_main"
    )


def test_console_scripts_count_matches_readme_and_cli_reference_claim() -> None:
    """README and CLI reference both advertise the total console-script count.

    Why: README.md (line 119, 193) and docs/cli_reference.md (line 3) literally
    claim '29 个 console scripts'; if pyproject.toml gains or loses an
    ``index-inclusion-*`` script the docs must move with it, otherwise the
    README's grouped list and the reference doc drift out of sync.
    """
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    expected_count = len(project["scripts"])
    expected_phrase = f"{expected_count} 个 console scripts"
    # Anchor the count with a digit-boundary lookaround so a stale doc that
    # read '129 个 console scripts' could never silently satisfy a substring
    # check for '29 个 console scripts' (and vice versa for trailing digits).
    pattern = re.compile(rf"(?<!\d){expected_count}(?!\d) 个 console scripts")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    cli_reference = (ROOT / "docs" / "cli_reference.md").read_text(encoding="utf-8")

    assert (
        pattern.search(readme) is not None
    ), f"README.md must advertise '{expected_phrase}' (with no adjacent digits) to match pyproject.toml [project.scripts]"
    assert (
        pattern.search(cli_reference) is not None
    ), f"docs/cli_reference.md must advertise '{expected_phrase}' (with no adjacent digits) to match pyproject.toml [project.scripts]"


def test_readme_repo_card_badge_lines_ignores_later_non_leading_badges(
    readme_repo_card_badge_lines,
) -> None:
    readme = """# Title\n\nIntro prose before any repo-card badges.\n\n![CLI](https://img.shields.io/badge/CLI-999%20commands-2da44e)\n"""

    assert readme_repo_card_badge_lines(readme) == []


def test_readme_ci_badge_targets_existing_workflow_file(
    readme_repo_card_badge_lines,
) -> None:
    """README's leading CI badge must stay tied to the actual GitHub Actions workflow.

    Why: README.md (line 3) renders the GitHub Actions badge that most readers
    use as the first health signal for the project. If the workflow file is
    renamed or the clickable URL drifts from the image URL, the repo card can
    quietly advertise a broken or stale CI status while local tests keep
    passing.

    The assertion is scoped to the leading README badge block so a duplicate
    prose or code-block URL later in the file cannot mask a stale repo-card
    badge.
    """
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    ci_badges = [
        line
        for line in readme_repo_card_badge_lines(readme)
        if line.startswith("[![CI](")
    ]

    assert len(ci_badges) == 1, (
        "README.md must render exactly one leading repo-card CI GitHub Actions badge"
    )

    pattern = re.compile(
        r"\[!\[CI\]\("
        r"https://github\.com/(?P<repo>[^/]+/[^/]+)/actions/workflows/"
        r"(?P<badge_workflow>[^/)]+\.ya?ml)/badge\.svg"
        r"\)\]\("
        r"https://github\.com/(?P=repo)/actions/workflows/"
        r"(?P<link_workflow>[^/)]+\.ya?ml)"
        r"\)"
    )
    match = pattern.fullmatch(ci_badges[0])

    assert match is not None, (
        "README.md CI badge must use matching GitHub Actions image and target URLs"
    )
    assert match.group("badge_workflow") == match.group("link_workflow"), (
        "README.md CI badge image URL and clickable URL must reference the same workflow file"
    )

    workflow_relpath = Path(".github") / "workflows" / match.group("badge_workflow")
    workflow_path = ROOT / workflow_relpath
    assert workflow_path.is_file(), (
        f"README.md CI badge must reference an existing workflow file: {workflow_relpath}"
    )

    workflow_text = workflow_path.read_text(encoding="utf-8")
    assert re.search(r"^name:\s*[\"']?CI[\"']?\s*$", workflow_text, re.MULTILINE), (
        f"{workflow_relpath} must keep workflow name 'CI' to match README.md CI badge label"
    )


def test_readme_cli_badge_matches_console_scripts_count(
    readme_repo_card_badge_lines,
) -> None:
    """README's shields.io CLI badge must match pyproject.toml [project.scripts].

    Why: README.md (line 8) renders ``badge/CLI-29%20commands-…`` as the third
    English shields.io badge on the GitHub repo card, sitting next to the
    already-guarded ``literature-16%20papers`` (test_literature_catalog) and
    ``pipeline-10%20steps`` (test_rebuild_all) badges. The existing
    ``test_console_scripts_count_matches_readme_and_cli_reference_claim`` only
    guards the Chinese ``29 个 console scripts`` narrative on lines 119 and
    193; it never reads the URL-encoded English badge. If pyproject.toml
    gained a 30th ``index-inclusion-*`` script and the in-code / Chinese docs
    were updated, the GitHub repo-card badge would silently keep advertising
    the stale ``29 commands`` to every English reader landing on the project
    page — exactly the failure mode the literature and pipeline badge guards
    were added to prevent for their respective counts.

    The ``(?<!\\d)…(?!\\d)`` digit-boundary lookarounds match the literature
    and pipeline badge guards (commit deaa0c5) so a stale ``129%20commands``
    or ``29%20commands9`` rendering can never satisfy a naive substring check.
    The assertion is additionally scoped to the leading README badge block so
    a duplicate prose or code-block URL later in the file cannot mask a stale
    repo-card badge.
    """
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    expected_count = len(project["scripts"])
    pattern = re.compile(
        rf"!\[CLI\]\("
        rf"https://img\.shields\.io/badge/CLI-(?<!\d){expected_count}(?!\d)%20commands-"
        rf"[0-9a-fA-F]+\)"
    )

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    cli_badges = [
        line
        for line in readme_repo_card_badge_lines(readme)
        if line.startswith("![CLI](")
    ]

    assert len(cli_badges) == 1, (
        "README.md must render exactly one leading repo-card CLI shields.io badge"
    )
    assert pattern.fullmatch(cli_badges[0]) is not None, (
        f"README.md must render shields.io badge 'CLI-{expected_count}%20commands' "
        f"(no adjacent digits) to match pyproject.toml [project.scripts] count="
        f"{expected_count}"
    )


def test_readme_python_badge_matches_pyproject_requires_python(
    readme_repo_card_badge_lines,
) -> None:
    """README's shields.io Python badge must match pyproject.toml ``requires-python``.

    Why: README.md (line 4) renders ``badge/python-3.11%2B-…`` as the leading
    English shields.io Python-version badge on the GitHub repo card, sitting
    next to the already-guarded ``literature-16%20papers`` (test_literature_catalog),
    ``pipeline-10%20steps`` (test_rebuild_all) and ``CLI-29%20commands``
    badges. The existing guards cover every other leading repo-card badge
    that carries a numeric or workflow claim, but no test reads the
    URL-encoded ``python-3.11%2B`` token against the actual ``requires-python``
    declaration in pyproject.toml. If pyproject.toml bumped the floor to
    ``>=3.12`` (e.g., once the Python 3.11 trove classifier is retired) and
    the in-code invariants were updated, the README badge would silently
    keep advertising ``python-3.11%2B`` to every English reader landing on
    the project page — exactly the failure mode the literature, pipeline
    and CLI badge guards were added to prevent for their respective claims.

    The ``(?<!\\d)…(?!\\d)`` digit-boundary lookarounds match the literature,
    pipeline and CLI badge guards (commits 490746e/dc8670c/7591f2e) so a
    stale ``python-13.11%2B`` or ``python-3.111%2B`` rendering can never
    satisfy a naive substring check. The assertion is additionally scoped to
    the leading README badge block so a duplicate prose or code-block URL
    later in the file cannot mask a stale repo-card badge.
    """
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    requires_python = project["requires-python"]
    declaration = re.fullmatch(r">=\s*(\d+)\.(\d+)", requires_python)
    assert declaration is not None, (
        "pyproject.toml [project] requires-python must be a '>=X.Y' declaration "
        "so the README Python badge can mirror it; "
        f"got {requires_python!r}"
    )
    expected_min = f"{declaration.group(1)}.{declaration.group(2)}"
    pattern = re.compile(
        rf"!\[Python\]\("
        rf"https://img\.shields\.io/badge/python-"
        rf"(?<!\d){re.escape(expected_min)}(?!\d)%2B-"
        rf"[0-9a-fA-F]+\)"
    )

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    python_badges = [
        line
        for line in readme_repo_card_badge_lines(readme)
        if line.startswith("![Python](")
    ]

    assert len(python_badges) == 1, (
        "README.md must render exactly one leading repo-card Python shields.io badge"
    )
    assert pattern.fullmatch(python_badges[0]) is not None, (
        f"README.md must render shields.io badge 'python-{expected_min}%2B' "
        f"(no adjacent digits) to match pyproject.toml [project] requires-python="
        f"{requires_python!r}"
    )


def test_readme_generated_artifact_paths_resolve_to_committed_fixtures() -> None:
    """README generated-artifact references should stay aligned with repo fixtures."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    prefixes = ("results/", "data/processed/", "docs/screenshots/")
    target_pattern = re.compile(
        r"\[[^\]]+\]\((?P<markdown>[^)#?]+)(?:[#?][^)]+)?\)"
        r"|<img\s+[^>]*\bsrc=\"(?P<img>[^\"]+)\""
        r"|`(?P<inline>(?:results/|data/processed/|docs/screenshots/)[^`]+)`"
    )

    targets: set[str] = set()
    for match in target_pattern.finditer(readme):
        target = next(
            value for value in match.groupdict().values() if value is not None
        )
        if target.startswith("./"):
            target = target[2:]
        if target.startswith(prefixes):
            targets.add(target)

    expected_core_targets = {
        "docs/screenshots/dashboard-home.png",
        "docs/screenshots/paper-brief.png",
        "docs/screenshots/dashboard-mobile.png",
        "docs/screenshots/cma-evidence-tiers.png",
        "results/real_tables/cma_hypothesis_verdicts.csv",
        "results/real_tables/research_summary.md",
    }
    assert expected_core_targets.issubset(targets), (
        "README.md artifact-path guard must keep covering screenshot and "
        f"generated result fixtures; missing from parsed targets: "
        f"{sorted(expected_core_targets - targets)}"
    )

    missing_targets = sorted(
        target for target in targets if not (ROOT / target).exists()
    )
    assert missing_targets == [], (
        "README.md generated/data/screenshot artifact references must resolve "
        f"to committed repo files or fixture directories: {missing_targets}"
    )


def test_readme_generated_artifact_references_are_not_machine_local() -> None:
    """Public README artifact refs must never point at one developer's machine."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    reference_pattern = re.compile(
        r"\[[^\]]+\]\((?P<markdown>[^)#?]+)(?:[#?][^)]+)?\)"
        r"|<img\s+[^>]*\bsrc=\"(?P<img>[^\"]+)\""
        r"|`(?P<inline>(?:results/|data/processed/|docs/screenshots/|/tmp/|/Users/|/home/|~/|file:)[^`]+)`"
    )
    machine_local = re.compile(r"^(?:/tmp/|/Users/|/home/|~/|file:|[A-Za-z]:[\\/])")

    refs: set[str] = set()
    for match in reference_pattern.finditer(readme):
        target = next(
            value for value in match.groupdict().values() if value is not None
        ).strip()
        if target.startswith("./"):
            target = target[2:]
        refs.add(target)

    intended_relative_refs = {
        "docs/screenshots/dashboard-home.png",
        "results/real_tables/research_summary.md",
    }
    assert intended_relative_refs.issubset(refs), (
        "README.md machine-local guard must parse intended relative artifact "
        f"references; missing {sorted(intended_relative_refs - refs)}"
    )

    leaked_refs = sorted(ref for ref in refs if machine_local.match(ref))
    assert leaked_refs == [], (
        "README.md generated artifact references must use repo-relative paths, "
        f"not machine-local paths: {leaked_refs}"
    )


def test_track_console_wrappers_delegate_to_expected_package_modules(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "_run_package_main", lambda module_name: calls.append(module_name))

    cli.run_build_event_sample_main()
    cli.run_build_price_panel_main()
    cli.run_match_controls_main()
    cli.run_match_robustness_main()
    cli.run_event_study_main()
    cli.run_regressions_main()
    cli.run_prepare_hs300_rdd_candidates_main()
    cli.run_reconstruct_hs300_rdd_candidates_main()
    cli.run_collect_hs300_rdd_l3_main()

    assert calls == [
        "index_inclusion_research.build_event_sample",
        "index_inclusion_research.build_price_panel",
        "index_inclusion_research.match_controls",
        "index_inclusion_research.match_robustness",
        "index_inclusion_research.run_event_study",
        "index_inclusion_research.run_regressions",
        "index_inclusion_research.prepare_hs300_rdd_candidates",
        "index_inclusion_research.reconstruct_hs300_rdd_candidates",
        "index_inclusion_research.hs300_rdd_online_sources",
    ]


def test_package_console_wrappers_delegate_to_expected_package_modules(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "_run_package_main", lambda module_name: calls.append(module_name))

    cli.run_dashboard_main()
    cli.run_price_pressure_track_main()
    cli.run_demand_curve_track_main()
    cli.run_identification_china_track_main()
    cli.run_hs300_rdd_main()
    cli.run_generate_sample_data_main()
    cli.run_download_real_data_main()
    cli.run_refresh_real_evidence_main()
    cli.run_make_figures_tables_main()
    cli.run_generate_research_report_main()

    assert calls == [
        "index_inclusion_research.literature_dashboard",
        "index_inclusion_research.price_pressure_track",
        "index_inclusion_research.demand_curve_track",
        "index_inclusion_research.identification_china_track",
        "index_inclusion_research.hs300_rdd",
        "index_inclusion_research.sample_data",
        "index_inclusion_research.real_data",
        "index_inclusion_research.real_evidence_refresh",
        "index_inclusion_research.figures_tables",
        "index_inclusion_research.research_report",
    ]


def test_console_wrapper_propagates_package_main_return_code(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_run_package_main", lambda module_name: 7)

    assert cli.run_doctor_main() == 7


def test_python_module_fallback_targets_are_directly_runnable() -> None:
    module_files = [
        "literature_dashboard.py",
        "price_pressure_track.py",
        "demand_curve_track.py",
        "identification_china_track.py",
        "hs300_rdd.py",
        "build_event_sample.py",
        "build_price_panel.py",
        "match_controls.py",
        "run_event_study.py",
        "run_regressions.py",
        "prepare_hs300_rdd_candidates.py",
        "reconstruct_hs300_rdd_candidates.py",
        "hs300_rdd_online_sources.py",
        "sample_data.py",
        "real_data.py",
        "enrich_cn_sectors.py",
        "real_evidence_refresh.py",
        "figures_tables.py",
        "research_report.py",
    ]

    for module_file in module_files:
        source = (ROOT / "src" / "index_inclusion_research" / module_file).read_text(encoding="utf-8")
        assert 'if __name__ == "__main__":' in source
