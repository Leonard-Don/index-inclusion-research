"""Regression guards for CI-safe CJK chart font stacks."""

from __future__ import annotations

from pathlib import Path

from index_inclusion_research.plot_style import CJK_FONT_CANDIDATES

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src" / "index_inclusion_research"


def test_cjk_font_candidates_include_ubuntu_noto_aliases() -> None:
    """Ubuntu ``fonts-noto-cjk`` may register shared CJK coverage as JP."""
    assert "Noto Sans CJK SC" in CJK_FONT_CANDIDATES
    assert "Noto Sans CJK JP" in CJK_FONT_CANDIDATES
    assert "Noto Sans CJK TC" in CJK_FONT_CANDIDATES
    assert "Noto Sans CJK HK" in CJK_FONT_CANDIDATES
    assert "Noto Sans CJK KR" in CJK_FONT_CANDIDATES
    assert CJK_FONT_CANDIDATES[-1] == "DejaVu Sans"


def test_plot_modules_use_shared_cjk_style_helper() -> None:
    """Avoid one-off font stacks drifting back to macOS-only CJK fonts."""
    offenders: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        if path.name == "plot_style.py":
            continue
        text = path.read_text(encoding="utf-8")
        if 'font.sans-serif' in text:
            offenders.append(str(path.relative_to(PROJECT_ROOT)))

    assert offenders == []
