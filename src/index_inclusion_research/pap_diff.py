"""PAP deviation auditor: ``index-inclusion-pap-diff``.

Compare the current 7-hypothesis verdicts against the frozen
Pre-Analysis Plan (PAP) baseline and classify every hypothesis into one
of five drift classes:

- ``unchanged``    — verdict, confidence, evidence_tier, n_obs, headline
                     metric all match (within tolerance).
- ``tightened``    — verdict unchanged, but confidence rose (低 → 中 → 高)
                     **or** the headline p-value dropped meaningfully.
- ``weakened``     — verdict unchanged, but confidence fell **or** the
                     headline p-value rose meaningfully.
- ``flipped``      — ``verdict`` text changed (e.g. 证据不足 → 支持).
- ``unverifiable`` — baseline or current row missing, or comparable
                     headline metric is absent / NaN on either side.

The auditor is informational (exit 0) by default. ``--strict`` flips it
into a gate that returns exit 1 whenever any hypothesis is classified as
``flipped`` — useful when wiring the check into CI / `make ci` once the
PAP itself is signed.

Outputs:

- A terminal table summarising each H1..H7 row + an aggregate count.
- ``results/real_tables/pap_deviation_report.csv`` (full per-row data,
  written every run unless ``--no-write`` is passed).

The frozen baseline lives at ``snapshots/pre-registration-2026-05-03.csv``
(see ``docs/pre_registration.md`` §冻结日期). Override with
``--baseline PATH`` if you maintain alternative pre-registration
snapshots.
"""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths

# ── Configuration ────────────────────────────────────────────────────

ROOT = paths.project_root()
DEFAULT_BASELINE_GLOB = "pre-registration-*.csv"
DEFAULT_BASELINE_DIR = ROOT / "snapshots"
DEFAULT_CURRENT = paths.real_tables_dir() / "cma_hypothesis_verdicts.csv"
DEFAULT_REPORT = paths.real_tables_dir() / "pap_deviation_report.csv"

# Confidence rank: higher value = stronger evidence.
_CONFIDENCE_RANK: dict[str, int] = {"低": 1, "中": 2, "高": 3}

# 5-class taxonomy
CLASS_UNCHANGED = "unchanged"
CLASS_TIGHTENED = "tightened"
CLASS_WEAKENED = "weakened"
CLASS_FLIPPED = "flipped"
CLASS_UNVERIFIABLE = "unverifiable"

CLASSIFICATIONS: tuple[str, ...] = (
    CLASS_UNCHANGED,
    CLASS_TIGHTENED,
    CLASS_WEAKENED,
    CLASS_FLIPPED,
    CLASS_UNVERIFIABLE,
)

# ANSI colour for terminal table; auto-disabled when stdout isn't a TTY.
_CLASS_COLOR: dict[str, str] = {
    CLASS_UNCHANGED: "\033[32m",      # green
    CLASS_TIGHTENED: "\033[36m",      # cyan
    CLASS_WEAKENED: "\033[33m",       # yellow
    CLASS_FLIPPED: "\033[31m",        # red
    CLASS_UNVERIFIABLE: "\033[90m",   # grey
}
_RESET = "\033[0m"


@dataclass(frozen=True)
class PapDiffConfig:
    """Tunable thresholds for the comparison.

    ``p_delta_threshold``
        Absolute change in headline p-value needed to count as
        tightened / weakened when confidence and verdict are unchanged.
        Defaults to 0.02 — i.e. p drifting ±0.02 is treated as material.

    ``key_value_rel_threshold``
        Relative change in ``key_value`` (when it's *not* a p-value)
        that counts as tightened / weakened. Defaults to 0.10 (10%).

    ``n_obs_rel_threshold``
        Relative change in ``n_obs`` that we tolerate inside ``unchanged``.
        Larger drifts are flagged in ``notes`` but don't on their own
        change the classification — sample size growing is healthy.
    """

    p_delta_threshold: float = 0.02
    key_value_rel_threshold: float = 0.10
    n_obs_rel_threshold: float = 0.05


# ── IO helpers ───────────────────────────────────────────────────────


def _read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except (OSError, ValueError):
        return None


def resolve_default_baseline(
    baseline_dir: Path = DEFAULT_BASELINE_DIR,
    *,
    glob: str = DEFAULT_BASELINE_GLOB,
) -> Path | None:
    """Return the most recent PAP baseline snapshot, or ``None``.

    Snapshots are named ``pre-registration-YYYY-MM-DD.csv``; we sort
    lexicographically (date order is preserved) and return the last
    one. Used when ``--baseline`` isn't specified.
    """
    if not baseline_dir.exists():
        return None
    snapshots = sorted(baseline_dir.glob(glob))
    return snapshots[-1] if snapshots else None


# ── Coercion helpers ─────────────────────────────────────────────────


def _coerce_float(value: object) -> float:
    if value is None:
        return float("nan")
    try:
        v = float(str(value))
    except (TypeError, ValueError):
        return float("nan")
    return v


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _coerce_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


# ── Core diff logic ──────────────────────────────────────────────────


def _row_lookup(df: pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if df is None or df.empty:
        return {}
    return {str(r["hid"]): dict(r) for _, r in df.iterrows()}


def _verdict_changed(before: str, after: str) -> bool:
    """``verdict`` text differs (after stripping whitespace).

    Empty strings on either side are *not* flips — that's the
    unverifiable case.
    """
    if not before or not after:
        return False
    return before != after


def _confidence_delta(before: str, after: str) -> int:
    """Return >0 if confidence rose, <0 if fell, 0 if unchanged/unknown."""
    b = _CONFIDENCE_RANK.get(before, 0)
    a = _CONFIDENCE_RANK.get(after, 0)
    if b == 0 or a == 0:
        return 0
    return a - b


def _key_value_delta(
    before: float,
    after: float,
    *,
    is_p_value: bool,
    config: PapDiffConfig,
) -> str:
    """Classify a key_value drift as 'tightened'/'weakened'/'flat'/'unknown'.

    For p-values: smaller is tighter evidence.
    For other metrics (spreads, ratios, hit rates): a meaningful change
    in *magnitude* counts as drift; we treat magnitude growing in the
    direction of the headline as tightened, shrinking as weakened.
    Because the meaning is hypothesis-specific we conservatively flag
    any rel change > ``key_value_rel_threshold`` as direction-ambiguous
    drift and label it 'weakened' only when |after| < |before| — i.e.
    the effect shrunk.
    """
    if math.isnan(before) or math.isnan(after):
        return "unknown"
    if is_p_value:
        diff = after - before
        if abs(diff) < config.p_delta_threshold:
            return "flat"
        return "tightened" if diff < 0 else "weakened"
    # Non-p key_value: compare on magnitude relative change.
    base = abs(before) if before != 0 else 1e-9
    rel = (abs(after) - abs(before)) / base
    if abs(rel) < config.key_value_rel_threshold:
        return "flat"
    return "tightened" if rel > 0 else "weakened"


def classify_row(
    baseline: dict[str, object] | None,
    current: dict[str, object] | None,
    *,
    config: PapDiffConfig | None = None,
) -> dict[str, object]:
    """Classify a single hypothesis pair.

    Always returns a dict with keys: ``hid``, ``classification``,
    ``baseline_verdict``, ``current_verdict``, ``baseline_confidence``,
    ``current_confidence``, ``baseline_n_obs``, ``current_n_obs``,
    ``baseline_key_label``, ``current_key_label``, ``baseline_key_value``,
    ``current_key_value``, ``baseline_evidence_tier``,
    ``current_evidence_tier``, ``notes``.

    Used both by the CLI and by tests.
    """
    cfg = config or PapDiffConfig()

    if baseline is None and current is None:
        # Defensive — caller would have given us a hid otherwise.
        return {
            "hid": "",
            "classification": CLASS_UNVERIFIABLE,
            "notes": "no baseline or current row",
        }

    if baseline is None:
        hid = _coerce_str(current.get("hid") if current else "")
        return _empty_row(
            hid=hid,
            classification=CLASS_UNVERIFIABLE,
            baseline=None,
            current=current,
            notes="missing baseline row",
        )
    if current is None:
        hid = _coerce_str(baseline.get("hid", ""))
        return _empty_row(
            hid=hid,
            classification=CLASS_UNVERIFIABLE,
            baseline=baseline,
            current=None,
            notes="missing current row",
        )

    hid = _coerce_str(current.get("hid") or baseline.get("hid"))
    base_verdict = _coerce_str(baseline.get("verdict"))
    cur_verdict = _coerce_str(current.get("verdict"))
    base_conf = _coerce_str(baseline.get("confidence"))
    cur_conf = _coerce_str(current.get("confidence"))
    base_kv = _coerce_float(baseline.get("key_value"))
    cur_kv = _coerce_float(current.get("key_value"))
    base_label = _coerce_str(baseline.get("key_label"))
    base_n = _coerce_int(baseline.get("n_obs"))
    cur_n = _coerce_int(current.get("n_obs"))
    base_tier = _coerce_str(baseline.get("evidence_tier"))
    cur_tier = _coerce_str(current.get("evidence_tier"))

    notes: list[str] = []

    # Hard flip: verdict text changed.
    if _verdict_changed(base_verdict, cur_verdict):
        notes.append(f"verdict {base_verdict} → {cur_verdict}")
        return _empty_row(
            hid=hid,
            classification=CLASS_FLIPPED,
            baseline=baseline,
            current=current,
            notes="; ".join(notes),
        )

    # If either verdict is missing we can't safely call it a match.
    if not base_verdict or not cur_verdict:
        return _empty_row(
            hid=hid,
            classification=CLASS_UNVERIFIABLE,
            baseline=baseline,
            current=current,
            notes="verdict missing on baseline or current",
        )

    # Tier shifts: not in themselves a tightening/weakening, but we note.
    if base_tier and cur_tier and base_tier != cur_tier:
        notes.append(f"evidence_tier {base_tier} → {cur_tier}")

    # Confidence delta.
    conf_delta = _confidence_delta(base_conf, cur_conf)

    # key_value delta. We need to know whether key_value is a p-value;
    # the PAP CSV uses ``p_value`` column for p-gated metrics; for others
    # ``p_value`` is empty. Fall back on label heuristic.
    is_p = _looks_like_p_value(baseline, current)
    kv_drift = _key_value_delta(
        base_kv, cur_kv, is_p_value=is_p, config=cfg
    )

    # n_obs drift — for notes only.
    if base_n is not None and cur_n is not None and base_n != 0:
        rel_n = (cur_n - base_n) / abs(base_n)
        if abs(rel_n) > cfg.n_obs_rel_threshold:
            notes.append(
                f"n_obs {base_n} → {cur_n} ({rel_n:+.1%})"
            )

    # Decide tightened vs weakened vs unchanged.
    # Rule: any directional signal (confidence or key_value) wins,
    # unchanged only when both are flat/zero.
    direction_votes = 0  # positive = tightened, negative = weakened
    if conf_delta > 0:
        direction_votes += 1
        notes.append(f"confidence {base_conf} → {cur_conf}")
    elif conf_delta < 0:
        direction_votes -= 1
        notes.append(f"confidence {base_conf} → {cur_conf}")
    if kv_drift == "tightened":
        direction_votes += 1
        notes.append(_kv_note(base_kv, cur_kv, base_label, "tightened"))
    elif kv_drift == "weakened":
        direction_votes -= 1
        notes.append(_kv_note(base_kv, cur_kv, base_label, "weakened"))
    elif kv_drift == "unknown":
        notes.append("key_value comparable as unverifiable (NaN)")
        if direction_votes == 0 and conf_delta == 0:
            return _empty_row(
                hid=hid,
                classification=CLASS_UNVERIFIABLE,
                baseline=baseline,
                current=current,
                notes="; ".join(notes) or "key_value NaN on one side",
            )

    if direction_votes > 0:
        classification = CLASS_TIGHTENED
    elif direction_votes < 0:
        classification = CLASS_WEAKENED
    else:
        classification = CLASS_UNCHANGED

    return _empty_row(
        hid=hid,
        classification=classification,
        baseline=baseline,
        current=current,
        notes="; ".join(notes) if notes else "",
    )


def _kv_note(before: float, after: float, label: str, direction: str) -> str:
    delta = after - before
    lbl = label or "key_value"
    return f"{lbl} {before:.4f} → {after:.4f} (Δ {delta:+.4f}, {direction})"


def _looks_like_p_value(
    baseline: dict[str, object], current: dict[str, object]
) -> bool:
    """Decide whether the headline ``key_value`` is a p-value.

    We look at the explicit ``p_value`` column first — when it's
    populated *and* equals key_value, it's a p. Otherwise we fall back
    on the ``key_label`` text containing 'p' / 'bootstrap' / 'regression'.
    """
    for src in (current, baseline):
        kv = _coerce_float(src.get("key_value"))
        pv = _coerce_float(src.get("p_value"))
        if not math.isnan(kv) and not math.isnan(pv):
            if abs(kv - pv) < 1e-9:
                return True
    label = _coerce_str(
        current.get("key_label") or baseline.get("key_label")
    ).lower()
    return any(token in label for token in ("p", "bootstrap", "regression"))


def _empty_row(
    *,
    hid: str,
    classification: str,
    baseline: dict[str, object] | None,
    current: dict[str, object] | None,
    notes: str,
) -> dict[str, object]:
    """Assemble the per-hid output row regardless of available data."""
    b = baseline or {}
    c = current or {}
    return {
        "hid": hid,
        "name_cn": _coerce_str(
            c.get("name_cn") or b.get("name_cn")
        ),
        "classification": classification,
        "baseline_verdict": _coerce_str(b.get("verdict")),
        "current_verdict": _coerce_str(c.get("verdict")),
        "baseline_confidence": _coerce_str(b.get("confidence")),
        "current_confidence": _coerce_str(c.get("confidence")),
        "baseline_evidence_tier": _coerce_str(b.get("evidence_tier")),
        "current_evidence_tier": _coerce_str(c.get("evidence_tier")),
        "baseline_n_obs": _coerce_int(b.get("n_obs")),
        "current_n_obs": _coerce_int(c.get("n_obs")),
        "baseline_key_label": _coerce_str(b.get("key_label")),
        "current_key_label": _coerce_str(c.get("key_label")),
        "baseline_key_value": _coerce_float(b.get("key_value")),
        "current_key_value": _coerce_float(c.get("key_value")),
        "notes": notes,
    }


def build_pap_diff(
    baseline: pd.DataFrame | None,
    current: pd.DataFrame | None,
    *,
    config: PapDiffConfig | None = None,
) -> pd.DataFrame:
    """Run the full audit and return the per-hypothesis DataFrame.

    Rows present in *either* frame are output (union of hids), sorted by
    hid so H1..H7 render in a stable order. The output schema is the
    same as the dict returned by :func:`classify_row` plus the columns
    are pre-ordered for the CSV report.
    """
    cfg = config or PapDiffConfig()
    base_lookup = _row_lookup(baseline)
    cur_lookup = _row_lookup(current)
    hids = sorted(set(base_lookup) | set(cur_lookup))

    rows: list[dict[str, object]] = []
    for hid in hids:
        row = classify_row(
            base_lookup.get(hid),
            cur_lookup.get(hid),
            config=cfg,
        )
        rows.append(row)

    columns = [
        "hid",
        "name_cn",
        "classification",
        "baseline_verdict",
        "current_verdict",
        "baseline_confidence",
        "current_confidence",
        "baseline_evidence_tier",
        "current_evidence_tier",
        "baseline_n_obs",
        "current_n_obs",
        "baseline_key_label",
        "current_key_label",
        "baseline_key_value",
        "current_key_value",
        "notes",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


# ── Rendering ────────────────────────────────────────────────────────


def _colorize(text: str, classification: str, *, enable: bool) -> str:
    if not enable:
        return text
    code = _CLASS_COLOR.get(classification, "")
    if not code:
        return text
    return f"{code}{text}{_RESET}"


def aggregate_counts(report: pd.DataFrame) -> dict[str, int]:
    """Return the count per classification (always all 5 keys present)."""
    counts: dict[str, int] = {c: 0 for c in CLASSIFICATIONS}
    if report is None or report.empty:
        return counts
    for cls, n in report["classification"].value_counts().items():
        counts[str(cls)] = int(n)
    return counts


def render_report(
    report: pd.DataFrame,
    *,
    baseline_path: Path,
    current_path: Path,
    color: bool = True,
) -> str:
    """Render the per-hypothesis classification as a terminal table."""
    lines: list[str] = []
    lines.append("=" * 78)
    lines.append(" INDEX-INCLUSION · PAP DEVIATION AUDIT")
    lines.append(f"   baseline: {baseline_path}")
    lines.append(f"   current : {current_path}")
    lines.append("=" * 78)

    if report.empty:
        lines.append(
            "(no rows — baseline and current both empty / missing.)"
        )
        return "\n".join(lines) + "\n"

    counts = aggregate_counts(report)
    summary_parts = [f"{counts[c]} {c}" for c in CLASSIFICATIONS]
    lines.append("总览: " + " | ".join(summary_parts))
    lines.append("")

    # Per-row table.
    name_width = max(
        (len(str(r)) for r in report["name_cn"].fillna("")),
        default=0,
    )
    name_width = min(max(name_width, 16), 22)
    header = (
        f"  {'HID':<4}"
        f"{'名称':<{name_width + 2}}"
        f"{'分类':<14}"
        f"{'verdict (base→cur)':<24}"
        f"{'conf':<10}"
        f"{'n_obs':>10}"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for _, row in report.iterrows():
        cls = str(row["classification"])
        cls_text = _colorize(f"{cls:<14}", cls, enable=color)
        verdict_pair = f"{row['baseline_verdict']} → {row['current_verdict']}"
        verdict_pair = verdict_pair[:24]
        conf_pair = (
            f"{row['baseline_confidence']}→{row['current_confidence']}"
        )
        conf_pair = conf_pair[:10]
        b_n = row["baseline_n_obs"]
        c_n = row["current_n_obs"]
        if pd.isna(b_n) or pd.isna(c_n):
            n_text = "—"
        else:
            n_text = f"{int(b_n)}→{int(c_n)}"
        name = str(row["name_cn"])[:name_width]
        lines.append(
            f"  {row['hid']:<4}"
            f"{name:<{name_width + 2}}"
            f"{cls_text}"
            f"{verdict_pair:<24}"
            f"{conf_pair:<10}"
            f"{n_text:>10}"
        )

    # Flag flipped rows verbosely.
    flipped = report.loc[report["classification"] == CLASS_FLIPPED]
    if not flipped.empty:
        lines.append("")
        lines.append("⚠ FLIPPED — these need PAP §7 sign-off:")
        for _, row in flipped.iterrows():
            lines.append(
                f"  {row['hid']} {row['name_cn']}: "
                f"{row['baseline_verdict']} → {row['current_verdict']}"
            )

    # Render notes for non-unchanged rows.
    drifted = report.loc[report["classification"] != CLASS_UNCHANGED]
    if not drifted.empty:
        lines.append("")
        lines.append("Notes:")
        for _, row in drifted.iterrows():
            notes = str(row.get("notes") or "").strip()
            if not notes:
                continue
            lines.append(f"  {row['hid']} · {notes}")

    return "\n".join(lines).rstrip() + "\n"


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit the current 7-hypothesis verdicts against the frozen "
            "Pre-Analysis Plan (PAP) baseline and classify each H1..H7 as "
            "unchanged / tightened / weakened / flipped / unverifiable."
        )
    )
    parser.add_argument(
        "--baseline",
        metavar="PATH",
        default=None,
        help=(
            "Path to the frozen PAP baseline CSV (default: latest "
            "snapshots/pre-registration-*.csv)."
        ),
    )
    parser.add_argument(
        "--current",
        metavar="PATH",
        default=str(DEFAULT_CURRENT),
        help=f"Path to current verdicts CSV (default: {DEFAULT_CURRENT}).",
    )
    parser.add_argument(
        "--report",
        metavar="PATH",
        default=str(DEFAULT_REPORT),
        help=f"Path to write the report CSV (default: {DEFAULT_REPORT}).",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Print the table but do not write the CSV report.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colour escape codes.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Exit with code 1 if any hypothesis is classified 'flipped'. "
            "Default behaviour is informational (always exit 0)."
        ),
    )
    parser.add_argument(
        "--p-delta-threshold",
        type=float,
        default=PapDiffConfig.p_delta_threshold,
        help=(
            "Minimum absolute p-value change to count as tightened/weakened "
            "(default: 0.02)."
        ),
    )
    parser.add_argument(
        "--key-value-rel-threshold",
        type=float,
        default=PapDiffConfig.key_value_rel_threshold,
        help=(
            "Minimum relative change in non-p key_value to count as "
            "tightened/weakened (default: 0.10)."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    baseline_path = (
        Path(args.baseline)
        if args.baseline
        else resolve_default_baseline()
    )
    if baseline_path is None or not baseline_path.exists():
        print(
            "[pap-diff] no PAP baseline found. Pass --baseline PATH or "
            f"create one under {DEFAULT_BASELINE_DIR}/pre-registration-*.csv.",
            file=sys.stderr,
        )
        return 1

    current_path = Path(args.current)
    if not current_path.exists():
        print(
            f"[pap-diff] current verdicts CSV not found: {current_path}",
            file=sys.stderr,
        )
        print(
            "Run `index-inclusion-cma` first to refresh verdicts.",
            file=sys.stderr,
        )
        return 1

    baseline = _read_csv(baseline_path)
    current = _read_csv(current_path)

    config = PapDiffConfig(
        p_delta_threshold=args.p_delta_threshold,
        key_value_rel_threshold=args.key_value_rel_threshold,
    )
    report = build_pap_diff(baseline, current, config=config)

    enable_color = not args.no_color and sys.stdout.isatty()
    print(
        render_report(
            report,
            baseline_path=baseline_path,
            current_path=current_path,
            color=enable_color,
        ),
        end="",
    )

    if not args.no_write:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(report_path, index=False)
        print(f"[pap-diff] wrote {report_path}")

    if args.strict:
        flipped = int((report["classification"] == CLASS_FLIPPED).sum())
        if flipped > 0:
            print(
                f"[pap-diff] --strict: {flipped} hypothesis(es) flipped vs PAP.",
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
