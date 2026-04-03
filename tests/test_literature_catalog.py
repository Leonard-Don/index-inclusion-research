from __future__ import annotations

from index_inclusion_research.literature_catalog import (
    build_grouped_literature_frame,
    build_literature_catalog_frame,
    build_literature_dashboard_frame,
    build_literature_review_markdown,
    build_literature_summary_frame,
    build_project_track_frame,
    build_project_track_markdown,
    get_literature_paper,
    list_literature_papers,
)


def test_literature_catalog_has_expected_size_and_stance_counts() -> None:
    catalog = build_literature_catalog_frame()
    assert len(catalog) == 16
    counts = catalog["stance"].value_counts().to_dict()
    assert counts == {"正方": 7, "反方": 6, "中性": 3}


def test_literature_catalog_maps_into_existing_project_modules() -> None:
    catalog = build_literature_catalog_frame()
    assert set(catalog["project_module"].unique()) == {"短期价格压力", "需求曲线效应", "沪深300论文复现"}
    summary = build_literature_summary_frame()
    assert summary["文献数量"].sum() == 16


def test_literature_dashboard_frame_contains_open_links() -> None:
    dashboard = build_literature_dashboard_frame()
    assert len(dashboard) == 16
    assert "PDF" in dashboard.columns
    assert dashboard["PDF"].str.contains('href="/paper/').any()


def test_get_literature_paper_returns_known_record() -> None:
    papers = list_literature_papers()
    assert len(papers) == 16
    paper = get_literature_paper("harris_gurel_1986")
    assert paper is not None
    assert paper.authors == "Lawrence Harris; Eitan Gurel"


def test_grouped_review_frames_follow_user_defined_buckets() -> None:
    assert len(build_grouped_literature_frame("反方")) == 6
    assert len(build_grouped_literature_frame("中性")) == 3
    assert len(build_grouped_literature_frame("正方")) == 7
    markdown = build_literature_review_markdown()
    assert "反方" in markdown and "中性" in markdown and "正方" in markdown


def test_project_tracks_are_built_from_full_16_paper_library() -> None:
    assert len(build_project_track_frame("短期价格压力")) == 7
    assert len(build_project_track_frame("需求曲线效应")) == 3
    assert len(build_project_track_frame("沪深300论文复现")) == 6
    markdown = build_project_track_markdown("短期价格压力")
    assert "这条研究主线并不是只依赖某一篇论文" in markdown
