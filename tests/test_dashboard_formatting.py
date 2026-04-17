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
