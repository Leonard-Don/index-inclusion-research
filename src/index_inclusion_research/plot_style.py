"""Shared matplotlib styling helpers for project figures."""

from __future__ import annotations

from typing import Any

# Broad enough for macOS, Windows, and Ubuntu GitHub Actions.  The Ubuntu
# ``fonts-noto-cjk`` package commonly registers shared CJK glyph coverage as
# JP/TC/HK/KR family names in matplotlib, so keep those aliases in the stack.
CJK_FONT_CANDIDATES: tuple[str, ...] = (
    "Songti SC",
    "STHeiti",
    "PingFang SC",
    "Heiti SC",
    "Arial Unicode MS",
    "Noto Sans CJK SC",
    "Noto Sans CJK JP",
    "Noto Sans CJK TC",
    "Noto Sans CJK HK",
    "Noto Sans CJK KR",
    "WenQuanYi Zen Hei",
    "SimHei",
    "Microsoft YaHei",
    "DejaVu Sans",
)


def configure_matplotlib_cjk(plt: Any) -> None:
    """Configure a pyplot module for CJK labels and minus signs."""
    plt.rcParams["font.sans-serif"] = list(CJK_FONT_CANDIDATES)
    plt.rcParams["axes.unicode_minus"] = False
