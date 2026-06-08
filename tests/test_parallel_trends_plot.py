from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from index_inclusion_research.analysis.robustness_event_study import (
    compute_parallel_trends_aar,
)
from index_inclusion_research.outputs.parallel_trends import (
    build_parallel_trends_plot,
)

_FIXED_DATE = date(2026, 6, 7)


def _aar_table() -> pd.DataFrame:
    """Two-cell AAR table (CN announce + US effective) with SE columns."""
    rng = np.random.default_rng(3)
    rows: list[dict[str, object]] = []
    for market, phase in (("CN", "announce"), ("US", "effective")):
        for rel in range(-20, 6):
            gap = 0.03 if rel == 0 else 0.0
            treated = float(rng.normal(0.0, 0.001)) + gap
            control = float(rng.normal(0.0, 0.001))
            rows.append(
                {
                    "market": market,
                    "event_phase": phase,
                    "relative_day": rel,
                    "treated_aar": treated,
                    "control_aar": control,
                    "treated_aar_se": 0.002,
                    "control_aar_se": 0.002,
                    "aar_gap": treated - control,
                    "n_treated": 10,
                    "n_control": 30,
                }
            )
    return pd.DataFrame(rows)


def test_build_parallel_trends_plot_empty_table_writes_nothing(tmp_path: Path) -> None:
    written = build_parallel_trends_plot(pd.DataFrame(), tmp_path)
    assert written == []
    assert not list(tmp_path.iterdir())


def test_build_parallel_trends_plot_missing_columns_writes_nothing(tmp_path: Path) -> None:
    bad = pd.DataFrame({"market": ["CN"], "relative_day": [0]})
    written = build_parallel_trends_plot(bad, tmp_path)
    assert written == []


def test_build_parallel_trends_plot_writes_one_png_per_cell(tmp_path: Path) -> None:
    written = build_parallel_trends_plot(
        _aar_table(), tmp_path, write_pdf=True, generated_on=_FIXED_DATE
    )
    assert len(written) == 2
    for png in written:
        assert png.exists()
        assert png.stat().st_size > 0
        assert png.suffix == ".png"
        # PDF companion alongside the PNG.
        assert png.with_suffix(".pdf").exists()
    names = {p.name for p in written}
    assert names == {
        "parallel_trends_aar_cn_announce.png",
        "parallel_trends_aar_us_effective.png",
    }


def test_build_parallel_trends_plot_no_pdf_when_disabled(tmp_path: Path) -> None:
    written = build_parallel_trends_plot(
        _aar_table(), tmp_path, write_pdf=False, generated_on=_FIXED_DATE
    )
    assert written
    for png in written:
        assert png.exists()
        assert not png.with_suffix(".pdf").exists()


def test_build_parallel_trends_plot_consumes_compute_output(tmp_path: Path) -> None:
    """End-to-end: the analysis function output feeds the plot builder."""
    rng = np.random.default_rng(1)
    rows: list[dict[str, object]] = []
    for event_idx in range(8):
        for treat in (1, 0):
            for rel in range(-10, 4):
                ar = float(rng.normal(0.0, 0.005))
                if treat == 1 and rel == 0:
                    ar += 0.05
                rows.append(
                    {
                        "event_id": f"E{event_idx}-{treat}",
                        "market": "CN",
                        "event_phase": "announce",
                        "treatment_group": treat,
                        "relative_day": rel,
                        "ar": ar,
                    }
                )
    aar = compute_parallel_trends_aar(pd.DataFrame(rows))
    written = build_parallel_trends_plot(aar, tmp_path, generated_on=_FIXED_DATE)
    assert len(written) == 1
    assert written[0].exists()
