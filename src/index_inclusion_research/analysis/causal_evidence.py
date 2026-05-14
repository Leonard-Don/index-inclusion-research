"""Causal / robustness evidence contracts for index-inclusion research.

This module is a lightweight, dependency-free layer that lets the project
record *how identified* a headline estimate is, separately from whether
it happens to be statistically significant. The four data contracts:

- :class:`CausalGraphSpec` — frozen statement of the treatment / outcome /
  confounder / mediator / instrument structure plus the identifying
  assumptions. Used to keep refutation reports honest: if the graph is
  invalid you cannot meaningfully refute anything.
- :class:`PlaceboTest` — deterministic record of a placebo / falsification
  exercise (random treatment, future outcome, shifted cutoff, …). Each
  record evaluates ``passed`` from a small, explicit threshold rule so
  downstream consumers do not have to re-derive the decision.
- :class:`SensitivityResult` — deterministic record of an omitted-variable
  / E-value / Rosenbaum-style robustness bound, with the same explicit
  pass / fail rule.
- :class:`RefutationReport` — aggregates a graph + placebos + sensitivities
  + cluster-SE evidence + freshness label and assigns a conservative
  ``evidence_grade`` (worst of the constituent grades) plus a
  dashboard-friendly payload.

Design choices:

- No DoWhy / EconML import is required at runtime. Callers compute their
  placebo / sensitivity numbers themselves (or via an optional adapter)
  and feed deterministic outputs into the contract. This keeps the test
  suite reproducible without optional extras.
- All contracts serialise to plain dictionaries via ``as_dict`` and can
  be re-hydrated via ``from_dict``. Both round-trips are exercised by
  ``tests/test_causal_evidence.py``.
- ``evidence_grade`` deliberately leans pessimistic: any failed placebo,
  any unbounded sensitivity, missing cluster-SE evidence, or a stale /
  missing freshness label downgrades the grade. This matches the
  pre-registration plan's spirit — refutation evidence should override
  optimistic point estimates rather than the other way around.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

EVIDENCE_GRADES: tuple[str, ...] = ("A", "B", "C", "D", "F")
"""Ordered worst-to-best inverse: ``EVIDENCE_GRADES[0]`` is strongest."""

_GRADE_ORDER = {grade: index for index, grade in enumerate(EVIDENCE_GRADES)}


def _worst_grade(grades: Iterable[str]) -> str:
    """Return the most pessimistic grade in ``grades`` (e.g. ``F`` beats ``A``)."""

    materialised = [grade for grade in grades if grade in _GRADE_ORDER]
    if not materialised:
        return "F"
    return max(materialised, key=lambda grade: _GRADE_ORDER[grade])


def _coerce_tuple(values: Iterable[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    cleaned: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"expected str, got {type(value).__name__}: {value!r}")
        text = value.strip()
        if not text:
            raise ValueError("graph nodes must be non-empty strings")
        cleaned.append(text)
    return tuple(cleaned)


def _ensure_unique(role: str, values: tuple[str, ...]) -> None:
    if len(set(values)) != len(values):
        duplicates = sorted({v for v in values if values.count(v) > 1})
        raise ValueError(f"duplicate {role}: {duplicates}")


# --------------------------------------------------------------------------- #
# CausalGraphSpec
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class CausalGraphSpec:
    """Validated treatment / outcome / confounder / mediator / instrument set.

    The class is intentionally narrow: it captures the *identifying claim*
    being made (which variables are confounders, which assumptions are
    being relied on) without trying to encode the full DAG topology. That
    makes it cheap to serialise into result manifests while still catching
    the most common authoring mistakes — empty names, treatment == outcome,
    a variable that is simultaneously a confounder and a mediator, etc.

    ``assumptions`` is a free-form tuple of textual labels such as
    ``"no_unobserved_confounders"``, ``"exclusion_restriction"``, or
    ``"parallel_trends"`` — the canonical list used by the project lives
    in :data:`KNOWN_ASSUMPTIONS` but unknown labels are accepted (with
    :meth:`unknown_assumptions` available for callers that want to warn).
    """

    treatment: str
    outcome: str
    confounders: tuple[str, ...] = ()
    mediators: tuple[str, ...] = ()
    instruments: tuple[str, ...] = ()
    assumptions: tuple[str, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        treatment = (self.treatment or "").strip()
        outcome = (self.outcome or "").strip()
        if not treatment:
            raise ValueError("treatment must be a non-empty string")
        if not outcome:
            raise ValueError("outcome must be a non-empty string")
        if treatment == outcome:
            raise ValueError("treatment and outcome must differ")

        confounders = _coerce_tuple(self.confounders)
        mediators = _coerce_tuple(self.mediators)
        instruments = _coerce_tuple(self.instruments)
        assumptions = _coerce_tuple(self.assumptions)
        _ensure_unique("confounders", confounders)
        _ensure_unique("mediators", mediators)
        _ensure_unique("instruments", instruments)

        role_map: dict[str, str] = {treatment: "treatment", outcome: "outcome"}
        for role_name, members in (
            ("confounder", confounders),
            ("mediator", mediators),
            ("instrument", instruments),
        ):
            for member in members:
                if member in role_map:
                    raise ValueError(
                        f"{member!r} cannot be both {role_map[member]} and {role_name}"
                    )
                role_map[member] = role_name

        object.__setattr__(self, "treatment", treatment)
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "confounders", confounders)
        object.__setattr__(self, "mediators", mediators)
        object.__setattr__(self, "instruments", instruments)
        object.__setattr__(self, "assumptions", assumptions)
        object.__setattr__(self, "description", str(self.description or "").strip())

    # -- public API -----------------------------------------------------

    def variables(self) -> tuple[str, ...]:
        """Every variable mentioned in the spec, in canonical order."""

        return (
            self.treatment,
            self.outcome,
            *self.confounders,
            *self.mediators,
            *self.instruments,
        )

    def has_instrument(self) -> bool:
        return len(self.instruments) > 0

    def unknown_assumptions(self) -> tuple[str, ...]:
        return tuple(label for label in self.assumptions if label not in KNOWN_ASSUMPTIONS)

    def as_dict(self) -> dict[str, Any]:
        return {
            "treatment": self.treatment,
            "outcome": self.outcome,
            "confounders": list(self.confounders),
            "mediators": list(self.mediators),
            "instruments": list(self.instruments),
            "assumptions": list(self.assumptions),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CausalGraphSpec:
        return cls(
            treatment=str(payload.get("treatment", "")),
            outcome=str(payload.get("outcome", "")),
            confounders=tuple(payload.get("confounders", ()) or ()),
            mediators=tuple(payload.get("mediators", ()) or ()),
            instruments=tuple(payload.get("instruments", ()) or ()),
            assumptions=tuple(payload.get("assumptions", ()) or ()),
            description=str(payload.get("description", "") or ""),
        )


KNOWN_ASSUMPTIONS: frozenset[str] = frozenset(
    {
        "no_unobserved_confounders",
        "exclusion_restriction",
        "stable_unit_treatment_value",
        "positivity",
        "parallel_trends",
        "no_anticipation",
        "rdd_continuity",
        "rdd_no_manipulation",
    }
)


# --------------------------------------------------------------------------- #
# PlaceboTest
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PlaceboTest:
    """Deterministic record of a placebo / falsification exercise.

    Parameters
    ----------
    name:
        Human-readable identifier (e.g. ``"future_outcome"``,
        ``"random_treatment"``, ``"placebo_cutoff_+0.05"``).
    kind:
        Coarse class — one of ``"random_treatment"``, ``"random_outcome"``,
        ``"lagged_outcome"``, ``"shifted_cutoff"``, ``"pre_period"``, or
        ``"other"``. Drives dashboard grouping but is otherwise advisory.
    baseline_estimate:
        The actual effect estimate being defended.
    placebo_estimate:
        The effect estimate when the placebo manipulation is applied.
        Should be close to zero (relative to the baseline) for the
        placebo to pass.
    threshold:
        Maximum allowed ``|placebo_estimate| / |baseline_estimate|``
        ratio (default ``0.2``). Setting this to ``0`` makes any non-zero
        placebo a failure; setting it to ``1.0`` would essentially never
        fail and should be used cautiously.
    placebo_p_value:
        Optional p-value for the placebo estimate; included for downstream
        rendering but does not by itself decide pass / fail.
    notes:
        Free-form context shown verbatim on dashboards.
    """

    name: str
    kind: str
    baseline_estimate: float
    placebo_estimate: float
    threshold: float = 0.2
    placebo_p_value: float | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        name = (self.name or "").strip()
        if not name:
            raise ValueError("PlaceboTest.name must be non-empty")
        kind = (self.kind or "other").strip() or "other"
        if self.threshold < 0:
            raise ValueError("PlaceboTest.threshold must be >= 0")
        try:
            baseline = float(self.baseline_estimate)
            placebo = float(self.placebo_estimate)
        except (TypeError, ValueError) as exc:
            raise ValueError("PlaceboTest estimates must be numeric") from exc
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "baseline_estimate", baseline)
        object.__setattr__(self, "placebo_estimate", placebo)
        object.__setattr__(self, "threshold", float(self.threshold))
        object.__setattr__(self, "notes", str(self.notes or "").strip())

    @property
    def ratio(self) -> float:
        """``|placebo_estimate| / |baseline_estimate|``; ``inf`` if baseline=0.

        A baseline of zero is a degenerate case (nothing to defend) and is
        treated as a guaranteed failure unless the placebo is also exactly
        zero — that is, ``0/0`` collapses to ``0`` so trivially-zero
        estimates do not silently "pass".
        """

        if self.baseline_estimate == 0:
            return 0.0 if self.placebo_estimate == 0 else float("inf")
        return abs(self.placebo_estimate) / abs(self.baseline_estimate)

    @property
    def passed(self) -> bool:
        return self.ratio <= self.threshold

    @property
    def borderline(self) -> bool:
        """True when the placebo passed but only just (within 50 % of threshold)."""

        if not self.passed:
            return False
        if self.threshold == 0:
            return False
        return self.ratio > 0.5 * self.threshold

    def grade(self) -> str:
        if not self.passed:
            return "D"
        if self.borderline:
            return "B"
        return "A"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "baseline_estimate": self.baseline_estimate,
            "placebo_estimate": self.placebo_estimate,
            "threshold": self.threshold,
            "placebo_p_value": self.placebo_p_value,
            "notes": self.notes,
            "ratio": self.ratio,
            "passed": self.passed,
            "borderline": self.borderline,
            "grade": self.grade(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> PlaceboTest:
        return cls(
            name=str(payload.get("name", "")),
            kind=str(payload.get("kind", "other") or "other"),
            baseline_estimate=float(payload.get("baseline_estimate", 0.0)),
            placebo_estimate=float(payload.get("placebo_estimate", 0.0)),
            threshold=float(payload.get("threshold", 0.2)),
            placebo_p_value=(
                None
                if payload.get("placebo_p_value") in (None, "")
                else float(payload["placebo_p_value"])
            ),
            notes=str(payload.get("notes", "") or ""),
        )


# --------------------------------------------------------------------------- #
# SensitivityResult
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SensitivityResult:
    """Deterministic record of an omitted-variable / e-value / R² bound.

    Parameters
    ----------
    name:
        Human-readable identifier (e.g. ``"oster_delta"``,
        ``"e_value_h7"``).
    kind:
        One of ``"omitted_variable"``, ``"e_value"``, ``"rosenbaum_gamma"``,
        ``"r2_bound"``, ``"leave_one_out"``, ``"other"``. Advisory; drives
        grouping in the dashboard payload.
    point_estimate:
        The headline effect estimate the sensitivity bound defends.
    robustness_value:
        How strong an unobserved confounder (or other bias source) would
        need to be in order to nullify the result. Same units as the
        underlying sensitivity literature (Cinelli–Hazlett RV, Rosenbaum
        gamma, e-value, …) — interpretation is left to the caller; only
        the comparison with ``threshold`` is used here.
    threshold:
        Minimum acceptable ``robustness_value``. Failing this is the
        primary "refuted" signal.
    breaks_at:
        Optional explicit value at which significance would flip (for
        display only).
    notes:
        Free-form context.
    """

    name: str
    kind: str
    point_estimate: float
    robustness_value: float
    threshold: float
    breaks_at: float | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        name = (self.name or "").strip()
        if not name:
            raise ValueError("SensitivityResult.name must be non-empty")
        kind = (self.kind or "other").strip() or "other"
        try:
            point = float(self.point_estimate)
            robustness = float(self.robustness_value)
            threshold = float(self.threshold)
        except (TypeError, ValueError) as exc:
            raise ValueError("SensitivityResult numeric fields must be numeric") from exc
        if threshold < 0:
            raise ValueError("SensitivityResult.threshold must be >= 0")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "point_estimate", point)
        object.__setattr__(self, "robustness_value", robustness)
        object.__setattr__(self, "threshold", threshold)
        if self.breaks_at is not None:
            object.__setattr__(self, "breaks_at", float(self.breaks_at))
        object.__setattr__(self, "notes", str(self.notes or "").strip())

    @property
    def passed(self) -> bool:
        return self.robustness_value >= self.threshold

    @property
    def margin(self) -> float:
        """``robustness_value - threshold``. Positive ⇒ passes by margin."""

        return self.robustness_value - self.threshold

    def grade(self) -> str:
        if not self.passed:
            return "C"
        # Comfortably above threshold ⇒ A; only just above ⇒ B.
        if self.threshold > 0 and self.robustness_value < 2 * self.threshold:
            return "B"
        return "A"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "point_estimate": self.point_estimate,
            "robustness_value": self.robustness_value,
            "threshold": self.threshold,
            "breaks_at": self.breaks_at,
            "notes": self.notes,
            "margin": self.margin,
            "passed": self.passed,
            "grade": self.grade(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> SensitivityResult:
        breaks_at = payload.get("breaks_at")
        return cls(
            name=str(payload.get("name", "")),
            kind=str(payload.get("kind", "other") or "other"),
            point_estimate=float(payload.get("point_estimate", 0.0)),
            robustness_value=float(payload.get("robustness_value", 0.0)),
            threshold=float(payload.get("threshold", 0.0)),
            breaks_at=None if breaks_at in (None, "") else float(str(breaks_at)),
            notes=str(payload.get("notes", "") or ""),
        )


# --------------------------------------------------------------------------- #
# RefutationReport
# --------------------------------------------------------------------------- #


_FRESHNESS_GRADE = {
    "fresh": "A",
    "recent": "B",
    "stale": "C",
    "missing": "D",
    "invalid": "D",
}


def _freshness_grade(label: str | None) -> str:
    if label is None:
        return "D"
    return _FRESHNESS_GRADE.get(str(label).strip().lower(), "D")


@dataclass(frozen=True)
class RefutationReport:
    """Aggregated placebo / sensitivity / cluster-SE / freshness evidence.

    The report does not run any of the underlying methods itself — it
    simply records pre-computed contracts and assigns a *conservative*
    evidence grade:

    1. Empty report (no placebos AND no sensitivities) → ``F``.
    2. Any failed placebo → at least ``D`` (placebo failure is treated
       as a structural refutation).
    3. Otherwise the grade is the worst of the four component grades:
       placebo, sensitivity, cluster-SE, freshness.

    Component grades:

    - **Placebo** — ``A`` if every placebo passes cleanly (ratio
      ≤ ½ × threshold). ``B`` if any placebo is borderline (passes but
      ratio > ½ × threshold). ``D`` if any placebo fails.
    - **Sensitivity** — ``A`` if every robustness value is ≥ 2 × threshold.
      ``B`` if every robustness value is ≥ threshold but at least one is
      < 2 × threshold. ``C`` if any robustness value falls below
      threshold.
    - **Cluster-SE** — ``A`` if ``cluster_se_present`` else ``B``.
    - **Freshness** — fresh → A, recent → B, stale → C, missing /
      invalid / unknown → D.

    ``cluster_se_summary`` is a free-form text field (e.g.
    ``"event-clustered, 893 clusters"``) that mirrors the
    :class:`PyfixestClusterResult` already emitted elsewhere in the
    project; it is shown verbatim on the dashboard.
    """

    graph: CausalGraphSpec
    hypothesis_id: str = ""
    placebos: tuple[PlaceboTest, ...] = ()
    sensitivities: tuple[SensitivityResult, ...] = ()
    cluster_se_present: bool = False
    cluster_se_summary: str = ""
    freshness: str = "missing"
    freshness_detail: str = ""
    extra_notes: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.graph, CausalGraphSpec):
            raise TypeError("RefutationReport.graph must be a CausalGraphSpec")
        placebos = tuple(self.placebos)
        sensitivities = tuple(self.sensitivities)
        for placebo in placebos:
            if not isinstance(placebo, PlaceboTest):
                raise TypeError("placebos must be PlaceboTest instances")
        for sensitivity in sensitivities:
            if not isinstance(sensitivity, SensitivityResult):
                raise TypeError("sensitivities must be SensitivityResult instances")
        object.__setattr__(self, "placebos", placebos)
        object.__setattr__(self, "sensitivities", sensitivities)
        object.__setattr__(self, "hypothesis_id", str(self.hypothesis_id or "").strip())
        object.__setattr__(self, "cluster_se_summary", str(self.cluster_se_summary or "").strip())
        object.__setattr__(
            self,
            "freshness",
            (str(self.freshness or "missing").strip().lower() or "missing"),
        )
        object.__setattr__(self, "freshness_detail", str(self.freshness_detail or "").strip())
        object.__setattr__(self, "extra_notes", tuple(self.extra_notes or ()))

    # -- helpers --------------------------------------------------------

    @property
    def is_empty(self) -> bool:
        return not self.placebos and not self.sensitivities

    @property
    def failed_placebos(self) -> tuple[PlaceboTest, ...]:
        return tuple(p for p in self.placebos if not p.passed)

    @property
    def failed_sensitivities(self) -> tuple[SensitivityResult, ...]:
        return tuple(s for s in self.sensitivities if not s.passed)

    def placebo_grade(self) -> str:
        if not self.placebos:
            return "F"
        grades = [p.grade() for p in self.placebos]
        return _worst_grade(grades)

    def sensitivity_grade(self) -> str:
        if not self.sensitivities:
            return "F"
        grades = [s.grade() for s in self.sensitivities]
        return _worst_grade(grades)

    def cluster_se_grade(self) -> str:
        return "A" if self.cluster_se_present else "B"

    def freshness_grade(self) -> str:
        return _freshness_grade(self.freshness)

    def evidence_grade(self) -> str:
        """Conservative overall grade (worst of components).

        Returns ``"F"`` if neither placebos nor sensitivities are present;
        otherwise the worst of the per-component grades, with at least
        ``"D"`` when any placebo fails.
        """

        if self.is_empty:
            return "F"
        components = [
            self.placebo_grade(),
            self.sensitivity_grade(),
            self.cluster_se_grade(),
            self.freshness_grade(),
        ]
        # Drop F components contributed by *missing* lists so a present
        # placebo set with no sensitivities does not get a false F if
        # everything else is strong — but only when the report is not
        # globally empty (handled above).
        non_empty = [grade for grade in components if grade != "F"]
        if not non_empty:
            return "F"
        worst = _worst_grade(non_empty)
        if self.failed_placebos and _GRADE_ORDER[worst] < _GRADE_ORDER["D"]:
            return "D"
        return worst

    # -- serialisation --------------------------------------------------

    def as_dict(self) -> dict[str, Any]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "graph": self.graph.as_dict(),
            "placebos": [p.as_dict() for p in self.placebos],
            "sensitivities": [s.as_dict() for s in self.sensitivities],
            "cluster_se_present": self.cluster_se_present,
            "cluster_se_summary": self.cluster_se_summary,
            "freshness": self.freshness,
            "freshness_detail": self.freshness_detail,
            "extra_notes": list(self.extra_notes),
            "components": {
                "placebo_grade": self.placebo_grade(),
                "sensitivity_grade": self.sensitivity_grade(),
                "cluster_se_grade": self.cluster_se_grade(),
                "freshness_grade": self.freshness_grade(),
            },
            "evidence_grade": self.evidence_grade(),
            "failed_placebos": [p.name for p in self.failed_placebos],
            "failed_sensitivities": [s.name for s in self.failed_sensitivities],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> RefutationReport:
        graph_payload = payload.get("graph") or {}
        if isinstance(graph_payload, CausalGraphSpec):
            graph = graph_payload
        else:
            graph = CausalGraphSpec.from_dict(graph_payload)
        placebos = tuple(
            PlaceboTest.from_dict(item) for item in (payload.get("placebos") or ())
        )
        sensitivities = tuple(
            SensitivityResult.from_dict(item)
            for item in (payload.get("sensitivities") or ())
        )
        return cls(
            graph=graph,
            hypothesis_id=str(payload.get("hypothesis_id", "") or ""),
            placebos=placebos,
            sensitivities=sensitivities,
            cluster_se_present=bool(payload.get("cluster_se_present", False)),
            cluster_se_summary=str(payload.get("cluster_se_summary", "") or ""),
            freshness=str(payload.get("freshness", "missing") or "missing"),
            freshness_detail=str(payload.get("freshness_detail", "") or ""),
            extra_notes=tuple(payload.get("extra_notes", ()) or ()),
        )

    def with_freshness(self, freshness: str, *, detail: str = "") -> RefutationReport:
        """Return a copy with the freshness label refreshed (keeps the rest)."""

        return replace(self, freshness=freshness, freshness_detail=detail)


# --------------------------------------------------------------------------- #
# Dashboard-friendly export
# --------------------------------------------------------------------------- #


_GRADE_LABEL_CN = {
    "A": "证据强 (A)",
    "B": "证据良好 (B)",
    "C": "证据偏弱 (C)",
    "D": "已被反驳 (D)",
    "F": "尚无证据 (F)",
}

_GRADE_TONE = {
    "A": "ok",
    "B": "ok",
    "C": "warn",
    "D": "fail",
    "F": "muted",
}


def grade_label(grade: str) -> str:
    return _GRADE_LABEL_CN.get(grade, grade)


def grade_tone(grade: str) -> str:
    return _GRADE_TONE.get(grade, "muted")


def build_dashboard_payload(reports: Sequence[RefutationReport]) -> dict[str, Any]:
    """Render a list of reports into a JSON-safe dashboard payload.

    The payload is consumed by ``tests/test_causal_evidence.py`` and is
    intended to be embedded in the dashboard's CMA / refutation section
    without further reshaping. It has three top-level keys:

    - ``rows`` — one row per report (hypothesis id, grade, tone, label,
      component grades, summary chips).
    - ``summary`` — aggregate counts plus the conservative *overall*
      grade (worst grade among reports).
    - ``generated_grades`` — distinct grades present in ``rows``,
      ordered strongest-first; useful for filter chips.
    """

    rows: list[dict[str, Any]] = []
    grade_counts: dict[str, int] = {grade: 0 for grade in EVIDENCE_GRADES}
    for report in reports:
        grade = report.evidence_grade()
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
        chips = []
        if report.placebos:
            failed = len(report.failed_placebos)
            chips.append(
                {
                    "label": "placebo",
                    "value": f"{len(report.placebos) - failed}/{len(report.placebos)} 通过",
                    "tone": "ok" if failed == 0 else "fail",
                }
            )
        if report.sensitivities:
            failed = len(report.failed_sensitivities)
            chips.append(
                {
                    "label": "sensitivity",
                    "value": f"{len(report.sensitivities) - failed}/{len(report.sensitivities)} 通过",
                    "tone": "ok" if failed == 0 else "warn",
                }
            )
        chips.append(
            {
                "label": "cluster-SE",
                "value": "已聚类" if report.cluster_se_present else "缺失",
                "tone": "ok" if report.cluster_se_present else "warn",
            }
        )
        chips.append(
            {
                "label": "freshness",
                "value": report.freshness,
                "tone": grade_tone(report.freshness_grade()),
            }
        )
        rows.append(
            {
                "hypothesis_id": report.hypothesis_id,
                "grade": grade,
                "grade_label": grade_label(grade),
                "tone": grade_tone(grade),
                "components": {
                    "placebo_grade": report.placebo_grade(),
                    "sensitivity_grade": report.sensitivity_grade(),
                    "cluster_se_grade": report.cluster_se_grade(),
                    "freshness_grade": report.freshness_grade(),
                },
                "treatment": report.graph.treatment,
                "outcome": report.graph.outcome,
                "chips": chips,
                "failed_placebos": [p.name for p in report.failed_placebos],
                "failed_sensitivities": [s.name for s in report.failed_sensitivities],
                "cluster_se_summary": report.cluster_se_summary,
                "freshness_detail": report.freshness_detail,
            }
        )

    distinct_grades = [grade for grade in EVIDENCE_GRADES if grade_counts.get(grade, 0) > 0]
    overall = _worst_grade(row["grade"] for row in rows) if rows else "F"

    return {
        "rows": rows,
        "summary": {
            "count": len(rows),
            "grade_counts": grade_counts,
            "overall_grade": overall,
            "overall_grade_label": grade_label(overall),
            "overall_grade_tone": grade_tone(overall),
        },
        "generated_grades": distinct_grades,
    }


__all__ = [
    "CausalGraphSpec",
    "EVIDENCE_GRADES",
    "KNOWN_ASSUMPTIONS",
    "PlaceboTest",
    "RefutationReport",
    "SensitivityResult",
    "build_dashboard_payload",
    "grade_label",
    "grade_tone",
]
