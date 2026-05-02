from __future__ import annotations

from pathlib import Path

from index_inclusion_research import paths


def test_project_root_default_points_at_repo(monkeypatch):
    monkeypatch.delenv("INDEX_INCLUSION_ROOT", raising=False)
    root = paths.project_root()
    assert (root / "pyproject.toml").exists()
    assert (root / "src" / "index_inclusion_research").exists()


def test_project_root_respects_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(tmp_path))
    assert paths.project_root() == tmp_path.resolve()


def test_subpaths_are_under_root(monkeypatch, tmp_path):
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(tmp_path))
    root = tmp_path.resolve()
    assert paths.results_dir() == root / "results"
    assert paths.real_tables_dir() == root / "results" / "real_tables"
    assert paths.real_figures_dir() == root / "results" / "real_figures"
    assert paths.literature_results_dir() == root / "results" / "literature"
    assert paths.data_dir() == root / "data"
    assert paths.processed_data_dir() == root / "data" / "processed"
    assert paths.raw_data_dir() == root / "data" / "raw"
    assert paths.config_path() == root / "config" / "markets.yml"
    assert paths.docs_dir() == root / "docs"


def test_paths_helpers_return_pathlib_path(monkeypatch):
    monkeypatch.delenv("INDEX_INCLUSION_ROOT", raising=False)
    for fn in (
        paths.project_root,
        paths.results_dir,
        paths.real_tables_dir,
        paths.real_figures_dir,
        paths.literature_results_dir,
        paths.data_dir,
        paths.raw_data_dir,
        paths.processed_data_dir,
        paths.config_path,
        paths.docs_dir,
    ):
        assert isinstance(fn(), Path), f"{fn.__name__} must return Path"
