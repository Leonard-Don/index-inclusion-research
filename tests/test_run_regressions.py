from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research import run_regressions as cli
from index_inclusion_research.analysis import run_regressions as run_regressions_fn


def _write_matched_panel(path: Path) -> None:
    """Build a tiny matched panel with both treated (treatment_group=1) and
    control (treatment_group=0) rows so build_regression_dataset has both
    arms to compare."""
    rows = []
    for event_id, market, ticker, treatment, index_name in [
        (1, "CN", "CN01", 1, "CSI300"),
        (2, "CN", "CN02", 0, "CSI300"),
        (3, "US", "US01", 1, "SP500"),
        (4, "US", "US02", 0, "SP500"),
    ]:
        for phase in ("announce", "effective"):
            for rel in range(-3, 4):
                rows.append(
                    {
                        "event_id": event_id,
                        "matched_to_event_id": 1 if event_id <= 2 else 3,
                        "market": market,
                        "index_name": index_name,
                        "event_ticker": ticker,
                        "ticker": ticker,
                        "security_name": ticker,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 * treatment if rel == 0 else 0.001,
                        "ret": 0.001,
                        "benchmark_ret": 0.0005,
                        "event_date": "2024-02-05",
                        "event_date_raw": "2024-02-05",
                        "mapped_market_date": "2024-02-05",
                        "date": (pd.Timestamp("2024-02-05") + pd.Timedelta(days=rel)).date().isoformat(),
                        "treatment_group": treatment,
                        "inclusion": 1,
                        "sector": "Industrials",
                        "mkt_cap": 1e9,
                        "turnover": 0.02,
                        "volume": 1e6,
                        "close": 100.0,
                    }
                )
    pd.DataFrame(rows).to_csv(path, index=False)


def test_main_writes_regression_outputs(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.csv"
    out_dir = tmp_path / "out"
    _write_matched_panel(panel_path)
    rc = cli.main(
        [
            "--panel",
            str(panel_path),
            "--output-dir",
            str(out_dir),
        ]
    )
    assert rc == 0
    for name in (
        "regression_dataset.csv",
        "regression_coefficients.csv",
        "regression_models.csv",
    ):
        assert (out_dir / name).exists(), f"missing output {name}"


def test_run_regressions_recovers_known_coefficients() -> None:
    """OLS coefficients must be recovered exactly when the dependent
    variable is a noiseless linear function of the regressors — a guard
    that the regression spec wires its design matrix correctly."""
    beta = {
        "const": 0.5,
        "treatment_group": 0.03,
        "log_mkt_cap": -0.001,
        "pre_event_return": 0.20,
    }
    treatment = [0, 0, 0, 0, 1, 1, 1, 1]
    log_mkt_cap = [20.0, 21.0, 22.0, 23.0, 20.5, 21.5, 22.5, 23.5]
    pre_event_return = [-0.02, -0.01, 0.0, 0.01, 0.02, 0.03, -0.015, 0.005]
    car = [
        beta["const"]
        + beta["treatment_group"] * t
        + beta["log_mkt_cap"] * cap
        + beta["pre_event_return"] * ret
        for t, cap, ret in zip(treatment, log_mkt_cap, pre_event_return, strict=True)
    ]
    dataset = pd.DataFrame(
        {
            "market": "CN",
            "event_phase": "announce",
            "treatment_group": treatment,
            "log_mkt_cap": log_mkt_cap,
            "pre_event_return": pre_event_return,
            "car_m1_p1": car,
        }
    )

    coefficients, model_stats = run_regressions_fn(dataset)

    main = coefficients[coefficients["specification"] == "main_car"]
    assert not main.empty, "main_car spec should run on the 8-event group"
    recovered = dict(zip(main["parameter"], main["coefficient"], strict=True))
    for name, expected in beta.items():
        assert recovered[name] == pytest.approx(expected, abs=1e-6), name

    main_model = model_stats[model_stats["specification"] == "main_car"].iloc[0]
    assert int(main_model["n_obs"]) == 8
    assert float(main_model["r_squared"]) == pytest.approx(1.0, abs=1e-6)
