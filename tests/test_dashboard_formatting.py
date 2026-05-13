from __future__ import annotations

from pathlib import Path

import pandas as pd

from index_inclusion_research import dashboard_formatting


def test_render_table_applies_column_and_value_labels() -> None:
    frame = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "treatment_group": 0,
                "baseline_ols": "baseline_ols",
                "retention_ratio_valid": True,
            }
        ]
    )

    html = dashboard_formatting.render_table(frame, compact=True)

    assert "compact-table" in html
    assert "市场" in html
    assert "事件阶段" in html
    assert "事件方向" in html
    assert "处理组" in html
    assert "中国 A 股" in html
    assert "公告日" in html
    assert "调入" in html
    assert "对照组" in html
    assert "基准 OLS" in html
    assert "是" in html


def test_render_table_formats_numbers_for_dashboard_reading() -> None:
    frame = pd.DataFrame(
        [
            {
                "n_obs": 1234.0,
                "p_value": 0.012345,
                "tiny_p_value": 0.00001,
                "mean_car": 0.123456,
                "share_of_baseline": 1.0,
                "coverage": 0.0,
                "retention_ratio": 1.0,
                "year": 2026.0,
                "missing": float("nan"),
            }
        ]
    )

    html = dashboard_formatting.render_table(frame, compact=True)

    assert "1,234" in html
    assert "0.012" in html
    assert "&lt;0.0001" in html
    assert "<td><0.0001</td>" not in html
    assert "0.12" in html
    assert "100.00%" in html
    assert "0.00%" in html
    assert "2026" in html
    assert "2,026" not in html
    assert ">nan<" not in html.lower()
    assert "—" in html


def test_build_figure_caption_uses_dashboard_labels() -> None:
    caption = dashboard_formatting.build_figure_caption(
        Path("results/real_figures/sample_event_timeline.png"),
        prefix="图",
    )

    assert caption.startswith("图：")
    assert "真实调入/调出事件" in caption


def test_clean_display_text_drops_heading_output_files_and_local_paths() -> None:
    text = """
    # 标题

    - 第一行
    关键输出文件：results/foo.csv
    - `/Users/leonardodon/index-inclusion-research/results/foo.csv`
    - 第二行
    """

    cleaned = dashboard_formatting.clean_display_text(text)

    assert cleaned == "第一行\n第二行"
