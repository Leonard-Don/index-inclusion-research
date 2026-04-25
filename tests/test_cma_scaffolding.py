from __future__ import annotations


def test_cma_subpackage_imports():
    import index_inclusion_research.analysis.cross_market_asymmetry as cma

    assert hasattr(cma, "paths")
    assert hasattr(cma, "gap_period")
    assert hasattr(cma, "mechanism_panel")
    assert hasattr(cma, "heterogeneity")
    assert hasattr(cma, "time_series")
    assert hasattr(cma, "hypotheses")
    assert hasattr(cma, "verdicts")
    assert hasattr(cma, "orchestrator")


def test_cma_output_path_constants():
    from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator

    assert orchestrator.REAL_TABLES_DIR.name == "real_tables"
    assert orchestrator.REAL_FIGURES_DIR.name == "real_figures"
