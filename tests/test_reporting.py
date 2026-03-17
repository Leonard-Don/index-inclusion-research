from __future__ import annotations

import pandas as pd

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from generate_research_report import build_report_text


def test_build_report_text_includes_key_sections() -> None:
    event_summary = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "inclusion": 1,
                "window": "[-1,+1]",
                "window_slug": "m1_p1",
                "n_events": 3,
                "mean_car": 0.021,
                "std_car": 0.031,
                "t_stat": 2.0,
                "p_value": 0.08,
            },
            {
                "market": "US",
                "event_phase": "effective",
                "inclusion": 1,
                "window": "[-1,+1]",
                "window_slug": "m1_p1",
                "n_events": 3,
                "mean_car": 0.018,
                "std_car": 0.029,
                "t_stat": 1.8,
                "p_value": 0.09,
            },
        ]
    )
    regression_models = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "specification": "turnover_mechanism",
                "dependent_variable": "turnover_change",
                "n_obs": 12,
                "r_squared": 0.4,
                "adj_r_squared": 0.3,
            }
        ]
    )
    regression_coefficients = pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "specification": "turnover_mechanism",
                "dependent_variable": "turnover_change",
                "parameter": "inclusion",
                "coefficient": 0.12,
                "std_error": 0.04,
                "t_stat": 2.5,
                "p_value": 0.03,
            }
        ]
    )

    report = build_report_text(event_summary, regression_models, regression_coefficients)
    assert "事件研究主结论" in report
    assert "机制检验摘要" in report
    assert "公告日" in report
    assert "turnover_change" in report
