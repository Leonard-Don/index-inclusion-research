"""Tests for ``index_inclusion_research.rebuild_all``."""

from __future__ import annotations

import re
from pathlib import Path

from index_inclusion_research.rebuild_all import (
    DEFAULT_STEPS,
    PipelineStep,
    filter_steps,
    main,
    run_pipeline,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_default_steps_count_matches_readme_pipeline_claim() -> None:
    """README and CLI reference both advertise the rebuild step count.

    Why: README.md (line 39, 176) and docs/cli_reference.md (line 11) literally
    claim '10 步' for ``make rebuild``; if DEFAULT_STEPS grows or shrinks the
    docs must move with it or readers will pip-install and see a different
    number than the README promises.

    The '10 步' claim must additionally appear next to a ``rebuild`` token so
    that unrelated narrative counts (e.g. '建议按这 4 步看' on README line 46
    or '流水线 5 步' on cli_reference.md line 52) cannot satisfy the guard
    when DEFAULT_STEPS happens to land on those values.
    """
    expected_count = len(DEFAULT_STEPS)
    expected_phrase = f"{expected_count} 步"
    # (?<!\d)..(?!\d) prevents the count from being matched inside '210 步'
    # or '109 步'; '.{0,40}rebuild|rebuild.{0,40}' anchors to the rebuild
    # context so an unrelated '5 步' navigation tip cannot stand in.
    rebuild_count_pattern = re.compile(
        rf"(?:rebuild[^\n]{{0,40}}(?<!\d){expected_count}(?!\d) 步"
        rf"|(?<!\d){expected_count}(?!\d) 步[^\n]{{0,40}}rebuild)"
    )

    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    cli_reference = (REPO_ROOT / "docs" / "cli_reference.md").read_text(encoding="utf-8")

    assert (
        rebuild_count_pattern.search(readme) is not None
    ), (
        f"README.md must advertise '{expected_phrase}' adjacent to a 'rebuild' "
        f"token to match len(DEFAULT_STEPS)={expected_count}"
    )
    assert (
        rebuild_count_pattern.search(cli_reference) is not None
    ), (
        f"docs/cli_reference.md must advertise '{expected_phrase}' adjacent to a "
        f"'rebuild' token to match len(DEFAULT_STEPS)={expected_count}"
    )


def test_default_steps_are_in_dependency_order() -> None:
    slugs = [s.slug for s in DEFAULT_STEPS]
    # event-sample must precede price-panel which must precede event-study
    assert slugs.index("build-event-sample") < slugs.index("build-price-panel")
    assert slugs.index("build-price-panel") < slugs.index("run-event-study")
    assert slugs.index("match-controls") < slugs.index("build-matched-panel")
    assert slugs.index("build-matched-panel") < slugs.index("run-regressions")
    assert slugs.index("run-event-study") < slugs.index("cma")
    assert slugs.index("cma") < slugs.index("make-figures-tables")


def test_filter_steps_only() -> None:
    steps = filter_steps(DEFAULT_STEPS, only=["cma", "make-figures-tables"])
    assert [s.slug for s in steps] == ["cma", "make-figures-tables"]


def test_filter_steps_start_from() -> None:
    steps = filter_steps(DEFAULT_STEPS, start_from="run-event-study")
    assert steps[0].slug == "run-event-study"
    assert "build-event-sample" not in [s.slug for s in steps]


def test_filter_steps_skip() -> None:
    steps = filter_steps(DEFAULT_STEPS, skip=["hs300-rdd", "make-figures-tables"])
    assert "hs300-rdd" not in [s.slug for s in steps]
    assert "make-figures-tables" not in [s.slug for s in steps]


def test_filter_steps_unknown_start_from_raises() -> None:
    import pytest

    with pytest.raises(ValueError, match="not found"):
        filter_steps(DEFAULT_STEPS, start_from="bogus")


def test_run_pipeline_calls_each_step_in_order() -> None:
    calls: list[tuple[str, list[str]]] = []

    def make_callable(name: str):
        def _step(argv):
            calls.append((name, list(argv) if argv else []))
            return 0
        return _step

    def resolver(path: str):
        return make_callable(path)

    steps = [
        PipelineStep(slug="a", callable_path="modA:main"),
        PipelineStep(slug="b", callable_path="modB:main", argv=("--flag",)),
        PipelineStep(slug="c", callable_path="modC:main"),
    ]
    rc = run_pipeline(steps, callable_resolver=resolver)
    assert rc == 0
    assert [name for name, _ in calls] == ["modA:main", "modB:main", "modC:main"]
    assert calls[1][1] == ["--flag"]


def test_run_pipeline_aborts_on_non_zero_exit() -> None:
    calls: list[str] = []

    def resolver(path: str):
        def _step(_argv):
            calls.append(path)
            if path.startswith("modB"):
                return 7
            return 0
        return _step

    steps = [
        PipelineStep(slug="a", callable_path="modA:main"),
        PipelineStep(slug="b", callable_path="modB:main"),
        PipelineStep(slug="c", callable_path="modC:main"),
    ]
    rc = run_pipeline(steps, callable_resolver=resolver)
    assert rc == 7
    # third step must NOT have run
    assert "modC:main" not in calls


def test_run_pipeline_emits_events() -> None:
    events: list[tuple[str, str, int | None]] = []

    def resolver(_path: str):
        return lambda _argv: 0

    def on_event(slug, phase, rc, _elapsed):
        events.append((slug, phase, rc))

    steps = [
        PipelineStep(slug="a", callable_path="modA:main"),
        PipelineStep(slug="b", callable_path="modB:main"),
    ]
    rc = run_pipeline(steps, callable_resolver=resolver, on_event=on_event)
    assert rc == 0
    phases = [phase for _, phase, _ in events]
    assert phases == ["start", "finish", "start", "finish"]


def test_main_list_mode_does_not_execute(capsys) -> None:
    rc = main(["--list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "build-event-sample" in out
    assert "cma" in out


def test_main_dry_run_does_not_execute(capsys) -> None:
    rc = main(["--dry-run", "--only", "cma"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "cma" in out
    # other steps should NOT be in dry-run output
    assert "build-event-sample" not in out
