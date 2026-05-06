"""Builders / writers that turn hypothesis verdicts into shipped artifacts.

This is the integration layer between the per-hypothesis logic in
``_h_functions`` and the on-disk outputs the rest of the pipeline consumes:

- ``build_hypothesis_verdicts`` orchestrates the seven ``_h*`` functions
  and returns the verdict frame (also re-exported from ``__init__``).
- ``export_hypothesis_verdicts`` writes the CSV under ``output_dir``.
- ``export_hypothesis_verdicts_tex`` writes a booktabs LaTeX table for
  the paper.

Kept private (single-leading-underscore module name) because callers
should go through the package facade in ``verdicts/__init__.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from ..hypotheses import HYPOTHESES
from ._core import SIGNIFICANCE_LEVEL
from ._h_functions import _h1, _h2, _h3, _h4, _h5, _h6, _h7


def build_hypothesis_verdicts(
    *,
    gap_summary: pd.DataFrame,
    mechanism_panel: pd.DataFrame,
    heterogeneity_size: pd.DataFrame,
    time_series_rolling: pd.DataFrame,
    aum_frame: pd.DataFrame | None = None,
    pre_runup_bootstrap: Mapping[str, object] | None = None,
    gap_drift_regression: Mapping[str, object] | None = None,
    channel_concentration: pd.DataFrame | None = None,
    limit_regression: Mapping[str, object] | None = None,
    heterogeneity_sector: pd.DataFrame | None = None,
    h7_sector_interaction: pd.DataFrame | None = None,
    weight_change: pd.DataFrame | None = None,
    gap_event_level: pd.DataFrame | None = None,
    h6_weight_robustness: pd.DataFrame | None = None,
    significance_level: float = SIGNIFICANCE_LEVEL,
) -> pd.DataFrame:
    """Build the 7-hypothesis verdict frame.

    ``significance_level`` (default 0.10) is the boundary p threshold:
    p-gated hypotheses (H1 / H4 / H5) use the ``significance_level / 2``
    inner cutoff for "支持" (high confidence) and ``significance_level``
    itself for "部分支持" (medium). At the default 0.10 the inner cutoff
    is 0.05, which exactly reproduces the pre-parameterized behaviour.
    H2 / H3 / H6 / H7 are decided by spread / share / direction and are
    unaffected by this parameter.
    """
    hypotheses = {h.hid: h for h in HYPOTHESES}
    sector_frame = (
        heterogeneity_sector
        if heterogeneity_sector is not None
        else pd.DataFrame()
    )
    rows = [
        _h1(
            hypotheses["H1"],
            gap_summary,
            bootstrap=pre_runup_bootstrap,
            significance_level=significance_level,
        ),
        _h2(
            hypotheses["H2"],
            time_series_rolling,
            aum_frame=aum_frame,
            significance_level=significance_level,
        ),
        _h3(
            hypotheses["H3"],
            mechanism_panel,
            channel_concentration=channel_concentration,
            significance_level=significance_level,
        ),
        _h4(
            hypotheses["H4"],
            gap_summary,
            regression=gap_drift_regression,
            significance_level=significance_level,
        ),
        _h5(
            hypotheses["H5"],
            mechanism_panel,
            limit_regression=limit_regression,
            significance_level=significance_level,
        ),
        _h6(
            hypotheses["H6"],
            heterogeneity_size,
            weight_change=weight_change,
            gap_event_level=gap_event_level,
            h6_weight_robustness=h6_weight_robustness,
            significance_level=significance_level,
        ),
        _h7(
            hypotheses["H7"],
            sector_frame,
            h7_sector_interaction=h7_sector_interaction,
            significance_level=significance_level,
        ),
    ]
    return pd.DataFrame(rows)


_LATEX_VERDICT_HEADER = r"""\begin{tabular}{llllrrl}
\toprule
HID & 名称 & 裁决 & 可信度 & 头条指标 & 值 & n \\
\midrule
"""

_LATEX_VERDICT_FOOTER = "\\bottomrule\n\\end{tabular}\n"


def _latex_escape(text: str) -> str:
    """Minimal LaTeX-safe escaping for the verdict snapshot."""
    if text is None:
        return ""
    return (
        str(text)
        .replace("\\", r"\textbackslash{}")
        .replace("&", r"\&")
        .replace("%", r"\%")
        .replace("_", r"\_")
        .replace("#", r"\#")
        .replace("$", r"\$")
        .replace("{", r"\{")
        .replace("}", r"\}")
    )


def export_hypothesis_verdicts_tex(
    verdicts: pd.DataFrame,
    *,
    output_dir: Path,
) -> Path:
    """Render the verdict frame as a booktabs LaTeX table for paper insertion.

    The output schema matches the markdown verdict block in research_summary
    so the paper can cite either format. The 关键证据 column is omitted from
    the LaTeX version to keep it printable on a single page.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_hypothesis_verdicts.tex"
    lines: list[str] = ["% auto-generated CMA hypothesis verdict table", _LATEX_VERDICT_HEADER]
    for _, row in verdicts.iterrows():
        label = str(row.get("key_label", "") or "")
        value_raw = row.get("key_value")
        try:
            value_f = float(value_raw) if value_raw is not None else float("nan")
        except (TypeError, ValueError):
            value_f = float("nan")
        value_text = f"{value_f:.3f}" if value_f == value_f else "—"
        n_obs_raw = row.get("n_obs")
        try:
            n_obs_int = int(n_obs_raw) if n_obs_raw is not None else 0
        except (TypeError, ValueError):
            n_obs_int = 0
        n_text = str(n_obs_int) if n_obs_int > 0 else "—"
        lines.append(
            f"{_latex_escape(row['hid'])} & "
            f"{_latex_escape(row['name_cn'])} & "
            f"{_latex_escape(row['verdict'])} & "
            f"{_latex_escape(row['confidence'])} & "
            f"{_latex_escape(label) if label else '—'} & "
            f"{value_text} & "
            f"{n_text} \\\\"
        )
    lines.append(_LATEX_VERDICT_FOOTER)
    out_path.write_text("\n".join(lines))
    return out_path


def export_hypothesis_verdicts(
    *,
    output_dir: Path,
    gap_summary: pd.DataFrame,
    mechanism_panel: pd.DataFrame,
    heterogeneity_size: pd.DataFrame,
    time_series_rolling: pd.DataFrame,
    aum_frame: pd.DataFrame | None = None,
    pre_runup_bootstrap: Mapping[str, object] | None = None,
    gap_drift_regression: Mapping[str, object] | None = None,
    channel_concentration: pd.DataFrame | None = None,
    limit_regression: Mapping[str, object] | None = None,
    heterogeneity_sector: pd.DataFrame | None = None,
    h7_sector_interaction: pd.DataFrame | None = None,
    h6_weight_robustness: pd.DataFrame | None = None,
    significance_level: float = SIGNIFICANCE_LEVEL,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "cma_hypothesis_verdicts.csv"
    verdicts = build_hypothesis_verdicts(
        gap_summary=gap_summary,
        mechanism_panel=mechanism_panel,
        heterogeneity_size=heterogeneity_size,
        time_series_rolling=time_series_rolling,
        aum_frame=aum_frame,
        pre_runup_bootstrap=pre_runup_bootstrap,
        gap_drift_regression=gap_drift_regression,
        channel_concentration=channel_concentration,
        limit_regression=limit_regression,
        heterogeneity_sector=heterogeneity_sector,
        h7_sector_interaction=h7_sector_interaction,
        h6_weight_robustness=h6_weight_robustness,
        significance_level=significance_level,
    )
    verdicts.to_csv(out_path, index=False)
    return out_path
