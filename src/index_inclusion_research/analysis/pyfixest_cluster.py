from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class PyfixestClusterResult:
    """Small, serialisable slice of a pyfixest clustered-SE estimate."""

    coefficient: float
    std_error: float
    p_value: float
    n_obs: int
    cluster_count: int

    def as_dict(self) -> dict[str, float | int]:
        return {
            "coefficient": self.coefficient,
            "std_error": self.std_error,
            "p_value": self.p_value,
            "n_obs": self.n_obs,
            "cluster_count": self.cluster_count,
        }


def estimate_announcement_day_cluster_se(
    event_panel: pd.DataFrame,
    *,
    outcome_col: str = "abnormal_return",
    event_id_col: str = "event_id",
    relative_day_col: str = "relative_day",
    treatment_col: str = "announcement_day",
    treatment_day: int = 0,
) -> PyfixestClusterResult:
    """Estimate an announcement-day event-study effect with event-clustered SE.

    This is an optional-methodology smoke helper: it keeps the main event-study
    pipeline unchanged while proving that pyfixest can consume the project's
    event-level panel shape and return a clustered standard error for a small
    announcement-day regression.
    """

    try:
        import pyfixest as pf
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by skip gate
        raise ModuleNotFoundError(
            "pyfixest is optional; install the methods extra or run with "
            "`uv run --with pyfixest ...` to execute the cluster-SE smoke."
        ) from exc

    required = {outcome_col, event_id_col, relative_day_col}
    missing = sorted(required.difference(event_panel.columns))
    if missing:
        raise ValueError(f"event_panel missing required columns: {missing}")

    work = event_panel[[outcome_col, event_id_col, relative_day_col]].copy()
    work = work.dropna(subset=[outcome_col, event_id_col, relative_day_col])
    if work.empty:
        raise ValueError("event_panel must contain at least one complete observation")

    if treatment_col in event_panel.columns:
        raise ValueError(
            f"treatment_col={treatment_col!r} already exists; pass a scratch column name "
            "to avoid overwriting user-provided event-study fields"
        )
    work[treatment_col] = (work[relative_day_col].astype(int) == int(treatment_day)).astype(int)
    if int(work[treatment_col].sum()) == 0:
        raise ValueError(f"event_panel contains no rows for treatment_day={treatment_day}")
    if work[event_id_col].nunique() < 2:
        raise ValueError("clustered standard errors require at least two event clusters")

    fit: Any = pf.feols(
        f"{outcome_col} ~ {treatment_col}",
        data=work,
        vcov={"CRV1": event_id_col},
    )
    tidy = fit.tidy()
    if treatment_col not in tidy.index:
        raise RuntimeError(f"pyfixest result did not include {treatment_col!r}")

    row = tidy.loc[treatment_col]
    return PyfixestClusterResult(
        coefficient=float(row["Estimate"]),
        std_error=float(row["Std. Error"]),
        p_value=float(row["Pr(>|t|)"]),
        n_obs=int(len(work)),
        cluster_count=int(work[event_id_col].nunique()),
    )
