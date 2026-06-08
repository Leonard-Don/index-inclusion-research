"""Results-directory, registry, and schema doctor checks."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from ._common import (
    DEFAULT_CITATION_CENTRALITY_CSV,
    DEFAULT_RESULTS_DIR,
    EXPECTED_CMA_OUTPUTS,
    ROOT,
    CheckResult,
    _relative_label,
)


def check_results_directory_populated(
    *,
    results_dir: Path = DEFAULT_RESULTS_DIR,
    expected_files: Sequence[str] = EXPECTED_CMA_OUTPUTS,
) -> CheckResult:
    """At least the canonical CMA output files should be present."""
    if not results_dir.exists():
        return CheckResult(
            name="results_directory_populated",
            status="warn",
            message=f"results directory missing: {results_dir}",
            fix="Run `index-inclusion-cma` (or `make rebuild`) to populate it.",
        )
    missing = [name for name in expected_files if not (results_dir / name).exists()]
    if missing:
        return CheckResult(
            name="results_directory_populated",
            status="warn",
            message=f"{len(missing)} of {len(expected_files)} canonical CMA outputs missing.",
            fix="Run `index-inclusion-cma` to refresh CMA artifacts.",
            details=tuple(missing),
        )
    return CheckResult(
        name="results_directory_populated",
        status="pass",
        message=f"All {len(expected_files)} canonical CMA outputs are present.",
    )


def check_chart_builders_register(
    *,
    expected_min: int = 12,
) -> CheckResult:
    """The chart_data registry should expose at least the documented chart count."""
    try:
        chart_data = importlib.import_module("index_inclusion_research.chart_data")
    except ImportError as exc:
        return CheckResult(
            name="chart_builders_register",
            status="fail",
            message=f"chart_data module fails to import: {exc}",
            fix="Investigate the import error; this likely breaks the dashboard too.",
        )
    builders = getattr(chart_data, "CHART_BUILDERS", None)
    if not isinstance(builders, dict):
        return CheckResult(
            name="chart_builders_register",
            status="fail",
            message="chart_data.CHART_BUILDERS is not a dict.",
            fix="Restore the registry in src/index_inclusion_research/chart_data.py.",
        )
    if len(builders) < expected_min:
        return CheckResult(
            name="chart_builders_register",
            status="warn",
            message=(
                f"chart_data registry has {len(builders)} entries, expected ≥ {expected_min}."
            ),
            fix="Re-run after a regression test; some builders may have been removed.",
            details=tuple(sorted(builders.keys())),
        )
    return CheckResult(
        name="chart_builders_register",
        status="pass",
        message=f"chart_data registry exposes {len(builders)} builders (≥ {expected_min}).",
    )


def check_console_scripts_importable() -> CheckResult:
    """Every console_script declared in pyproject.toml resolves to a callable."""
    pyproject = ROOT / "pyproject.toml"
    if not pyproject.exists():
        return CheckResult(
            name="console_scripts_importable",
            status="fail",
            message="pyproject.toml is missing.",
            fix="Restore pyproject.toml from git.",
        )
    try:
        import tomllib  # py311+
    except ImportError:
        return CheckResult(
            name="console_scripts_importable",
            status="warn",
            message="tomllib unavailable (Python < 3.11); skipped.",
        )
    data = tomllib.loads(pyproject.read_text())
    scripts = data.get("project", {}).get("scripts", {})
    if not scripts:
        return CheckResult(
            name="console_scripts_importable",
            status="fail",
            message="pyproject.toml has no [project.scripts] table.",
            fix="Restore the console-script declarations.",
        )
    failures: list[str] = []
    for name, target in scripts.items():
        module_name, _, attr = str(target).partition(":")
        if not module_name or not attr:
            failures.append(f"{name} → bad target {target!r}")
            continue
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            failures.append(f"{name} → import {module_name}: {exc}")
            continue
        if not callable(getattr(module, attr, None)):
            failures.append(f"{name} → {module_name}:{attr} is not callable")
    if failures:
        return CheckResult(
            name="console_scripts_importable",
            status="fail",
            message=f"{len(failures)} console_script entry points unresolvable.",
            fix="Reinstall in editable mode (`pip install -e .`) and confirm cli.py wiring.",
            details=tuple(failures),
        )
    return CheckResult(
        name="console_scripts_importable",
        status="pass",
        message=f"All {len(scripts)} console_script entry points resolve to callables.",
    )


def check_heuristic_citation_centrality_schema(
    *,
    csv_path: Path = DEFAULT_CITATION_CENTRALITY_CSV,
) -> CheckResult:
    """Guard generated literature-network CSV semantics.

    ``citation_centrality.csv`` is a legacy filename, but its link-list
    columns must stay heuristic: ``top_linked_by`` / ``top_links_to``.
    Old ``top_cited_by`` / ``top_cites`` headers make the generated
    output look like verified bibliography evidence, so doctor fails
    them explicitly.
    """
    name = "heuristic_citation_centrality_schema"
    if not csv_path.exists():
        return CheckResult(
            name=name,
            status="pass",
            message=(
                f"heuristic literature centrality CSV not present at {_relative_label(csv_path)}; "
                "schema guard will validate top_linked_by/top_links_to when the generated file exists."
            ),
        )
    try:
        df = pd.read_csv(csv_path, nrows=0)
    except (OSError, ValueError) as exc:
        return CheckResult(
            name=name,
            status="fail",
            message=f"heuristic literature centrality CSV is unreadable: {exc}",
            fix="Regenerate with `python3 -m index_inclusion_research.citation_graph`.",
        )

    columns = set(df.columns)
    legacy_columns = sorted(columns & {"top_cited_by", "top_cites"})
    if legacy_columns:
        return CheckResult(
            name=name,
            status="fail",
            message=(
                f"{_relative_label(csv_path)} uses legacy citation-language column(s): "
                f"{legacy_columns}."
            ),
            fix=(
                "Regenerate with `python3 -m index_inclusion_research.citation_graph`; heuristic links "
                "must use top_linked_by/top_links_to and must not be represented as "
                "verified citations."
            ),
            details=tuple(legacy_columns),
        )

    required_columns = {
        "paper_id",
        "in_degree",
        "out_degree",
        "betweenness",
        "eigenvector",
        "top_linked_by",
        "top_links_to",
    }
    missing = sorted(required_columns - columns)
    if missing:
        return CheckResult(
            name=name,
            status="fail",
            message=f"{_relative_label(csv_path)} is missing heuristic column(s): {missing}.",
            fix="Regenerate with `python3 -m index_inclusion_research.citation_graph`.",
            details=tuple(missing),
        )

    return CheckResult(
        name=name,
        status="pass",
        message=(
            f"{_relative_label(csv_path)} uses heuristic link columns "
            "top_linked_by/top_links_to."
        ),
    )
