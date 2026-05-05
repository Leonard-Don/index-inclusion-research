"""Hypothesis verdict pipeline for the cross-market-asymmetry track.

Public API (stable; re-exported from sub-modules so callers don't have
to know the internal layout):

- :func:`build_hypothesis_verdicts` — orchestrates ``_h1`` … ``_h7`` and
  returns the verdict :class:`pandas.DataFrame`.
- :func:`export_hypothesis_verdicts` — wraps ``build_…`` and writes the
  CSV under ``output_dir``.
- :func:`export_hypothesis_verdicts_tex` — booktabs LaTeX table for the
  paper.
- :func:`render_paper_verdict_section` — markdown narrative for
  ``docs/paper_outline_verdicts.md``.
- :func:`export_paper_verdict_section` — thin writer for the markdown.

The package was carved out of a 1215-line ``verdicts.py`` so the
hypothesis logic, exports, and paper rendering each live in their own
file. Internal modules (``_core``, ``_h_functions``, ``_exports``,
``_paper``) are private — go through this facade.
"""

from __future__ import annotations

from ._exports import (
    SIGNIFICANCE_LEVEL,
    build_hypothesis_verdicts,
    export_hypothesis_verdicts,
    export_hypothesis_verdicts_tex,
)
from ._paper import export_paper_verdict_section, render_paper_verdict_section

__all__ = [
    "build_hypothesis_verdicts",
    "export_hypothesis_verdicts",
    "export_hypothesis_verdicts_tex",
    "export_paper_verdict_section",
    "render_paper_verdict_section",
    "SIGNIFICANCE_LEVEL",
]
