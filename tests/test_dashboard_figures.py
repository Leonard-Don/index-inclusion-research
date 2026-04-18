from __future__ import annotations

import os
import time
from pathlib import Path

from index_inclusion_research import dashboard_figures


def _write(path: Path, content: str | bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return path


def _set_mtime(path: Path, timestamp: float) -> None:
    os.utime(path, (timestamp, timestamp))


def test_dashboard_figure_dir_creates_expected_target(tmp_path: Path) -> None:
    target = dashboard_figures.dashboard_figure_dir(tmp_path)

    assert target == tmp_path / "results" / "real_figures"
    assert target.exists()


def test_figure_cache_is_fresh_reflects_source_and_target_mtimes(tmp_path: Path) -> None:
    source = _write(tmp_path / "source.csv", "x\n1\n")
    target = _write(tmp_path / "target.png", b"png")
    now = time.time()
    _set_mtime(source, now - 50)
    _set_mtime(target, now - 10)

    assert dashboard_figures.figure_cache_is_fresh([target], [source]) is True

    _set_mtime(source, now)

    assert dashboard_figures.figure_cache_is_fresh([target], [source]) is False


def test_significance_stars_uses_expected_thresholds() -> None:
    assert dashboard_figures.significance_stars(0.009) == "***"
    assert dashboard_figures.significance_stars(0.03) == "**"
    assert dashboard_figures.significance_stars(0.08) == "*"
    assert dashboard_figures.significance_stars(0.12) == ""


def test_create_price_pressure_figures_returns_cached_metadata(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "results" / "real_tables" / "time_series_event_study_summary.csv",
        "market,event_phase,announce_year,mean_car_m1_p1,inclusion\nCN,announce,2020,0.01,1\n",
    )
    target = _write(tmp_path / "results" / "real_figures" / "price_pressure_time_series.png", b"png")
    now = time.time()
    _set_mtime(source, now - 50)
    _set_mtime(target, now - 10)

    figures = dashboard_figures.create_price_pressure_figures(
        tmp_path,
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
    )

    assert figures == [
        {
            "label": "短窗口 CAR 时间变化图",
            "caption": "图意：按公告年份追踪调入事件的 CAR[-1,+1]。阅读重点：观察美股公告日效应是否随时间减弱，以及中国样本是否呈现不同的阶段性变化。",
            "path": "results/real_figures/price_pressure_time_series.png",
            "layout_class": "wide",
        }
    ]


def test_create_identification_figures_skips_when_rdd_not_real(tmp_path: Path) -> None:
    figures = dashboard_figures.create_identification_figures(
        tmp_path,
        load_rdd_status=lambda: {"mode": "missing"},
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
    )

    assert figures == []


def test_create_identification_figures_returns_cached_metadata_for_real_mode(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "results" / "literature" / "hs300_rdd" / "event_level_with_running.csv",
        "event_phase,distance_to_cutoff,car_m1_p1\nannounce,-0.1,0.01\nannounce,0.1,0.02\n",
    )
    target = _write(
        tmp_path / "results" / "literature" / "hs300_rdd" / "figures" / "car_m1_p1_rdd_main.png",
        b"png",
    )
    now = time.time()
    _set_mtime(source, now - 50)
    _set_mtime(target, now - 10)

    figures = dashboard_figures.create_identification_figures(
        tmp_path,
        load_rdd_status=lambda: {"mode": "real"},
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
    )

    assert figures == [
        {
            "path": "results/literature/hs300_rdd/figures/car_m1_p1_rdd_main.png",
            "caption": "中国样本 RDD 主图。图意：以公告日 CAR[-1,+1] 为例展示断点两侧分箱均值与局部拟合线。阅读重点：聚焦 0 附近是否存在离散跳跃，而不是只看两侧散点的总体波动。",
        }
    ]


def test_create_sample_design_figures_returns_cached_metadata(tmp_path: Path) -> None:
    source_paths = [
        _write(tmp_path / "results" / "real_tables" / "event_study_summary.csv", "x\n1\n"),
        _write(tmp_path / "results" / "real_regressions" / "regression_coefficients.csv", "x\n1\n"),
        _write(tmp_path / "results" / "real_regressions" / "match_diagnostics.csv", "x\n1\n"),
        _write(tmp_path / "data" / "raw" / "real_events.csv", "x\n1\n"),
    ]
    target_dir = tmp_path / "results" / "real_figures"
    targets = [
        _write(target_dir / "sample_event_timeline.png", b"png"),
        _write(target_dir / "sample_car_heatmap.png", b"png"),
        _write(target_dir / "main_regression_coefficients.png", b"png"),
        _write(target_dir / "mechanism_regression_coefficients.png", b"png"),
        _write(target_dir / "match_diagnostics_overview.png", b"png"),
    ]
    now = time.time()
    for path in source_paths:
        _set_mtime(path, now - 50)
    for path in targets:
        _set_mtime(path, now - 10)

    figures = dashboard_figures.create_sample_design_figures(
        tmp_path,
        to_relative=lambda path: path.relative_to(tmp_path).as_posix(),
        format_p_value=lambda value: f"p={value:.3f}",
        format_share=lambda value: f"{value:.1%}",
    )

    assert [figure["label"] for figure in figures] == [
        "真实调入调出事件时间线",
        "真实样本短窗口 CAR 热力图",
        "主回归处理组系数图",
        "机制回归系数图",
        "匹配诊断图",
    ]
    assert figures[0]["path"] == "results/real_figures/sample_event_timeline.png"
    assert figures[-1]["path"] == "results/real_figures/match_diagnostics_overview.png"
