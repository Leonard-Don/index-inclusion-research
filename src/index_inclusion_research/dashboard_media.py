from __future__ import annotations

import logging
from pathlib import Path
from typing import cast

from index_inclusion_research.dashboard_types import FigureEntry, RelativePathBuilder

logger = logging.getLogger(__name__)


def read_image_dimensions(path: Path) -> tuple[int | None, int | None]:
    try:
        from PIL import Image, UnidentifiedImageError
    except ImportError:
        return None, None

    try:
        with Image.open(path) as image:
            width, height = image.size
    except (OSError, UnidentifiedImageError) as exc:
        logger.debug("Could not read image dimensions for %s: %s", path, exc)
        return None, None

    if width <= 0 or height <= 0:
        return None, None
    return int(width), int(height)


def split_figure_caption(caption: str) -> tuple[str, str]:
    text = " ".join(caption.split())
    intro = ""
    lead = text
    focus = ""

    if "图意：" in text:
        intro, _, remainder = text.partition("图意：")
        intro = intro.strip()
        lead = remainder.strip()

    if "阅读重点：" in lead:
        lead, _, focus = lead.partition("阅读重点：")
        lead = lead.strip()
        focus = focus.strip()

    if intro and lead:
        lead = f"{intro}{lead}"
    elif intro and not lead:
        lead = intro

    return lead or text, focus


def build_figure_entry(
    path: Path,
    *,
    to_relative: RelativePathBuilder,
    caption: str,
    label: str | None = None,
    layout_class: str | None = None,
) -> FigureEntry:
    caption_lead, caption_focus = split_figure_caption(caption)
    entry: FigureEntry = {
        "path": to_relative(path),
        "caption": caption,
        "caption_lead": caption_lead,
        "caption_focus": caption_focus,
    }
    if label:
        entry["label"] = label
    if layout_class:
        entry["layout_class"] = layout_class

    width, height = read_image_dimensions(path)
    if width is not None and height is not None:
        entry["width"] = width
        entry["height"] = height
    return entry


def attach_figure_dimensions(entry: FigureEntry, *, path: Path) -> FigureEntry:
    enriched = dict(entry)
    width, height = read_image_dimensions(path)
    if width is not None and height is not None:
        enriched["width"] = width
        enriched["height"] = height
    return cast(FigureEntry, enriched)
