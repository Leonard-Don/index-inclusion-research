from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .event_study import compute_event_level_metrics


def build_regression_dataset(
    panel: pd.DataFrame,
    car_windows: list[list[int]] | list[tuple[int, int]],
) -> pd.DataFrame:
    dataset = compute_event_level_metrics(panel, car_windows)
    if "treatment_group" not in dataset.columns:
        dataset["treatment_group"] = 1
    dataset["treatment_group"] = dataset["treatment_group"].astype(int)
    dataset["inclusion"] = dataset["inclusion"].astype(int)
    return dataset


def run_regressions(
    dataset: pd.DataFrame,
    main_car_slug: str = "m1_p1",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    specs = {
        "main_car": f"car_{main_car_slug}",
        "turnover_mechanism": "turnover_change",
        "volume_mechanism": "volume_change",
        "volatility_mechanism": "volatility_change",
    }
    coefficient_rows: list[dict[str, object]] = []
    model_rows: list[dict[str, object]] = []

    for (market, event_phase), group in dataset.groupby(["market", "event_phase"], dropna=False):
        group = group.copy()
        for spec_name, dependent in specs.items():
            regression_frame = (
                group[[dependent, "treatment_group", "log_mkt_cap", "pre_event_return"]]
                .replace([np.inf, -np.inf], np.nan)
                .dropna()
            )
            if regression_frame.empty or regression_frame["treatment_group"].nunique() < 2 or len(regression_frame) < 4:
                continue
            design_matrix = sm.add_constant(
                regression_frame[["treatment_group", "log_mkt_cap", "pre_event_return"]],
                has_constant="add",
            )
            model = sm.OLS(regression_frame[dependent], design_matrix).fit(cov_type="HC1")
            model_rows.append(
                {
                    "market": market,
                    "event_phase": event_phase,
                    "specification": spec_name,
                    "dependent_variable": dependent,
                    "n_obs": int(model.nobs),
                    "r_squared": float(model.rsquared),
                    "adj_r_squared": float(model.rsquared_adj),
                }
            )
            for parameter, coefficient in model.params.items():
                coefficient_rows.append(
                    {
                        "market": market,
                        "event_phase": event_phase,
                        "specification": spec_name,
                        "dependent_variable": dependent,
                        "parameter": parameter,
                        "coefficient": float(coefficient),
                        "std_error": float(model.bse[parameter]),
                        "t_stat": float(model.tvalues[parameter]),
                        "p_value": float(model.pvalues[parameter]),
                    }
                )

    return pd.DataFrame(coefficient_rows), pd.DataFrame(model_rows)
