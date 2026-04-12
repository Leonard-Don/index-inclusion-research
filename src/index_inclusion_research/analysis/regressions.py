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


def _run_regression_specs(
    dataset: pd.DataFrame,
    *,
    specs: dict[str, str],
    cov_type: str = "HC1",
    estimation: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    coefficient_rows: list[dict[str, object]] = []
    model_rows: list[dict[str, object]] = []

    for (market, event_phase), group in dataset.groupby(["market", "event_phase"], dropna=False):
        group = group.copy()
        for spec_name, dependent in specs.items():
            if dependent not in group.columns:
                continue
            regression_frame = (
                group[[dependent, "treatment_group", "log_mkt_cap", "pre_event_return"]]
                .replace([np.inf, -np.inf], np.nan)
                .dropna()
            )
            min_required_obs = 5
            if (
                regression_frame.empty
                or regression_frame["treatment_group"].nunique() < 2
                or len(regression_frame) < min_required_obs
            ):
                continue
            design_matrix = sm.add_constant(
                regression_frame[["treatment_group", "log_mkt_cap", "pre_event_return"]],
                has_constant="add",
            )
            base_model = sm.OLS(regression_frame[dependent], design_matrix)
            model = base_model.fit() if cov_type in {"nonrobust", "OLS"} else base_model.fit(cov_type=cov_type)
            model_row = {
                "market": market,
                "event_phase": event_phase,
                "specification": spec_name,
                "dependent_variable": dependent,
                "n_obs": int(model.nobs),
                "r_squared": float(model.rsquared),
                "adj_r_squared": float(model.rsquared_adj),
            }
            if estimation is not None:
                model_row["estimation"] = estimation
            model_rows.append(model_row)
            for parameter, coefficient in model.params.items():
                coefficient_row = {
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
                if estimation is not None:
                    coefficient_row["estimation"] = estimation
                coefficient_rows.append(coefficient_row)

    return pd.DataFrame(coefficient_rows), pd.DataFrame(model_rows)


def run_regressions(
    dataset: pd.DataFrame,
    main_car_slug: str = "m1_p1",
    cov_type: str = "HC1",
    estimation: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    specs = {
        "main_car": f"car_{main_car_slug}",
        "turnover_mechanism": "turnover_change",
        "volume_mechanism": "volume_change",
        "volatility_mechanism": "volatility_change",
    }
    return _run_regression_specs(dataset, specs=specs, cov_type=cov_type, estimation=estimation)
