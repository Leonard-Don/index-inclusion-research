"""Core helpers shared by every hypothesis verdict path.

Kept private (single-leading-underscore module name) because callers
should go through ``verdicts.build_hypothesis_verdicts`` /
``verdicts.export_*``; nothing here is part of the public API.
"""

from __future__ import annotations

import pandas as pd

from ..hypotheses import StructuralHypothesis

SIGNIFICANCE_LEVEL = 0.10


def _row(
    frame: pd.DataFrame,
    *,
    market: str,
    metric: str | None = None,
    event_phase: str | None = None,
    outcome: str | None = None,
    spec: str | None = None,
) -> pd.Series | None:
    if frame.empty:
        return None
    sub = frame.copy()
    filters = {
        "market": market,
        "metric": metric,
        "event_phase": event_phase,
        "outcome": outcome,
        "spec": spec,
    }
    for column, value in filters.items():
        if value is None:
            continue
        if column not in sub.columns:
            return None
        sub = sub.loc[sub[column] == value]
    if sub.empty:
        return None
    return sub.iloc[0]


def _num(row: pd.Series | None, column: str) -> float | None:
    if row is None or column not in row:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.2%}"


def _fmt_num(value: float | None, digits: int = 2) -> str:
    return "NA" if value is None else f"{value:.{digits}f}"


def _sig(row: pd.Series | None) -> bool:
    p_value = _num(row, "p_value")
    return p_value is not None and p_value < SIGNIFICANCE_LEVEL


def _make_verdict(
    hypothesis: StructuralHypothesis,
    *,
    verdict: str,
    confidence: str,
    evidence_summary: str,
    metric_snapshot: str,
    next_step: str,
    key_label: str = "",
    key_value: float | None = None,
    n_obs: int | None = None,
) -> dict[str, object]:
    """Build one verdict row.

    The ``key_label`` / ``key_value`` / ``n_obs`` triple is the
    machine-readable headline (e.g. ``"bootstrap p" 0.640 n=436``).
    Downstream consumers — dashboard verdict cards, research_summary
    markdown table — can render the headline number prominently next
    to the human-readable ``metric_snapshot``.
    """
    return {
        "hid": hypothesis.hid,
        "name_cn": hypothesis.name_cn,
        "verdict": verdict,
        "confidence": confidence,
        "evidence_summary": evidence_summary,
        "metric_snapshot": metric_snapshot,
        "next_step": next_step,
        "evidence_refs": " | ".join(hypothesis.evidence_refs),
        "key_label": key_label,
        "key_value": float(key_value) if key_value is not None else float("nan"),
        "n_obs": int(n_obs) if n_obs is not None else 0,
        "paper_ids": " | ".join(hypothesis.paper_ids),
        "paper_count": len(hypothesis.paper_ids),
        "track": hypothesis.track,
    }


def _pending(
    hypothesis: StructuralHypothesis, reason: str, next_step: str
) -> dict[str, object]:
    return _make_verdict(
        hypothesis,
        verdict="待补数据",
        confidence="低",
        evidence_summary=reason,
        metric_snapshot="NA",
        next_step=next_step,
    )
