"""Source freshness / health audit helpers.

The research dashboard and paper bundle already expose a `data_sources.csv`
manifest, but downstream checks need a programmatic contract that answers:
"does the file exist, how old is it, and is the displayed path safe to show?"

This module deliberately stays small and dependency-light so it can be reused
from tests, CLI commands, dashboard loaders, or paper-audit hooks without
pulling pandas into import-time paths.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from . import paths

FRESHNESS_FRESH = "fresh"
FRESHNESS_RECENT = "recent"
FRESHNESS_STALE = "stale"
FRESHNESS_MISSING = "missing"
FRESHNESS_INVALID = "invalid"


@dataclass(frozen=True)
class FreshnessThresholds:
    """Age buckets for generated research artifacts."""

    fresh_max_days: float = 2.0
    recent_max_days: float = 14.0


DEFAULT_THRESHOLDS = FreshnessThresholds()


@dataclass(frozen=True)
class SourceHealthRow:
    """JSON-safe health row for a source or generated artifact."""

    label: str
    path: str
    category: str
    status: str
    ok: bool
    modified_at: str | None
    age_days: float | None
    freshness: str
    reason: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "label": self.label,
            "path": self.path,
            "category": self.category,
            "status": self.status,
            "ok": self.ok,
            "modified_at": self.modified_at,
            "age_days": self.age_days,
            "freshness": self.freshness,
            "reason": self.reason,
        }


def freshness_label(age_days: float | None, thresholds: FreshnessThresholds = DEFAULT_THRESHOLDS) -> str:
    """Map an artifact age to a small freshness vocabulary."""

    if age_days is None:
        return FRESHNESS_MISSING
    if age_days < 0:
        return FRESHNESS_INVALID
    if age_days <= thresholds.fresh_max_days:
        return FRESHNESS_FRESH
    if age_days <= thresholds.recent_max_days:
        return FRESHNESS_RECENT
    return FRESHNESS_STALE


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_project_path(path: Path, root: Path) -> str:
    """Return a project-relative path and avoid leaking absolute home paths."""

    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        return resolved.relative_to(root_resolved).as_posix()
    except ValueError:
        # Keep the basename for out-of-tree paths so UI/test output never leaks
        # /Users/... or CI workspace roots.
        return resolved.name


def _coerce_path(value: object, *, root: Path) -> Path | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = Path(value.strip())
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate


def audit_source_paths(
    sources: Iterable[Mapping[str, object]],
    *,
    root: Path | None = None,
    now: datetime | None = None,
    thresholds: FreshnessThresholds = DEFAULT_THRESHOLDS,
) -> list[SourceHealthRow]:
    """Build source-health rows from label/path/category mappings.

    Recognized keys are `label` (or Chinese `数据集`), `path` (or `文件`), and
    `category` (or `来源`). Invalid/missing path values produce a row with
    `status="invalid"`; missing files produce `status="missing"`.
    """

    project_root = (root or paths.project_root()).resolve()
    reference_now = now or _utc_now()
    if reference_now.tzinfo is None:
        reference_now = reference_now.replace(tzinfo=UTC)

    rows: list[SourceHealthRow] = []
    for index, spec in enumerate(sources):
        label = str(spec.get("label") or spec.get("数据集") or f"source-{index + 1}")
        raw_category = spec.get("category") or spec.get("来源") or "artifact"
        category = str(raw_category)
        candidate = _coerce_path(spec.get("path") or spec.get("文件"), root=project_root)
        if candidate is None:
            rows.append(
                SourceHealthRow(
                    label=label,
                    path="",
                    category=category,
                    status="invalid",
                    ok=False,
                    modified_at=None,
                    age_days=None,
                    freshness=FRESHNESS_INVALID,
                    reason="missing-path",
                )
            )
            continue

        safe_path = _safe_project_path(candidate, project_root)
        if not candidate.exists():
            rows.append(
                SourceHealthRow(
                    label=label,
                    path=safe_path,
                    category=category,
                    status="missing",
                    ok=False,
                    modified_at=None,
                    age_days=None,
                    freshness=FRESHNESS_MISSING,
                    reason="file-not-found",
                )
            )
            continue

        modified = datetime.fromtimestamp(candidate.stat().st_mtime, tz=UTC)
        age_days = round((reference_now - modified).total_seconds() / 86_400, 3)
        freshness = freshness_label(age_days, thresholds)
        if freshness == FRESHNESS_INVALID:
            rows.append(
                SourceHealthRow(
                    label=label,
                    path=safe_path,
                    category=category,
                    status="invalid",
                    ok=False,
                    modified_at=modified.isoformat().replace("+00:00", "Z"),
                    age_days=age_days,
                    freshness=freshness,
                    reason="future-mtime",
                )
            )
            continue
        ok = freshness in {FRESHNESS_FRESH, FRESHNESS_RECENT}
        rows.append(
            SourceHealthRow(
                label=label,
                path=safe_path,
                category=category,
                status="ok" if ok else "stale",
                ok=ok,
                modified_at=modified.isoformat().replace("+00:00", "Z"),
                age_days=age_days,
                freshness=freshness,
                reason=None if ok else "stale-artifact",
            )
        )
    return rows


def audit_data_sources_csv(
    manifest_path: Path | None = None,
    *,
    root: Path | None = None,
    now: datetime | None = None,
    thresholds: FreshnessThresholds = DEFAULT_THRESHOLDS,
) -> list[SourceHealthRow]:
    """Audit the repository's `results/real_tables/data_sources.csv` manifest."""

    project_root = (root or paths.project_root()).resolve()
    manifest = manifest_path or (project_root / "results" / "real_tables" / "data_sources.csv")
    with manifest.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    return audit_source_paths(rows, root=project_root, now=now, thresholds=thresholds)


__all__: Sequence[str] = (
    "DEFAULT_THRESHOLDS",
    "FRESHNESS_FRESH",
    "FRESHNESS_INVALID",
    "FRESHNESS_MISSING",
    "FRESHNESS_RECENT",
    "FRESHNESS_STALE",
    "FreshnessThresholds",
    "SourceHealthRow",
    "audit_data_sources_csv",
    "audit_source_paths",
    "freshness_label",
)
