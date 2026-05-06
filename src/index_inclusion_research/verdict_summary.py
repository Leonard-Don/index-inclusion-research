"""Terminal-friendly verdict status: ``index-inclusion-verdict-summary``.

Prints the current 7-hypothesis verdict picture to stdout in 1-2
screens — useful for users who want a research-status check without
launching the dashboard. Reads:

- ``results/real_tables/cma_hypothesis_verdicts.csv``
- ``results/real_tables/cma_track_verdict_summary.csv``

Exit code is 0 if the verdict CSV exists and parses, 1 otherwise.
"""

from __future__ import annotations

import argparse
import math
from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths

ROOT = paths.project_root()
_PAP_SNAPSHOTS_DIR = ROOT / "snapshots"
DEFAULT_VERDICTS = ROOT / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
DEFAULT_TRACK_SUMMARY = ROOT / "results" / "real_tables" / "cma_track_verdict_summary.csv"

VERDICT_TIER_ORDER = ("支持", "部分支持", "证据不足", "待补数据")

# ANSI colour codes; auto-disabled when stdout isn't a TTY.
_TIER_COLOR = {
    "支持": "\033[32m",        # green
    "部分支持": "\033[33m",    # yellow
    "证据不足": "\033[31m",    # red
    "待补数据": "\033[90m",    # bright black / grey
}
_RESET = "\033[0m"


def _colorize(text: str, tier: str, *, enable: bool) -> str:
    if not enable:
        return text
    code = _TIER_COLOR.get(tier, "")
    if not code:
        return text
    return f"{code}{text}{_RESET}"


def _format_value(value: object) -> str:
    if value is None:
        return "—"
    try:
        v = float(str(value))
    except (TypeError, ValueError):
        return str(value) if value else "—"
    if math.isnan(v):
        return "—"
    return f"{v:.3f}"


def _format_n(n_obs: object) -> str:
    try:
        n = int(float(str(n_obs)))
    except (TypeError, ValueError):
        return "—"
    return str(n) if n > 0 else "—"


def render_summary(
    verdicts: pd.DataFrame,
    track_summary: pd.DataFrame | None = None,
    *,
    color: bool = True,
) -> str:
    """Render the verdict picture as a multi-line string."""
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append(" INDEX-INCLUSION RESEARCH · CN/US 不对称机制裁决")
    lines.append(" (论文核心 \"是否产生超额收益\" 见 event_study_summary.csv)")
    lines.append("=" * 72)

    if verdicts.empty:
        lines.append("(verdicts CSV is empty — run `index-inclusion-cma` first.)")
        return "\n".join(lines) + "\n"

    counts: dict[str, int] = {tier: 0 for tier in VERDICT_TIER_ORDER}
    for _, row in verdicts.iterrows():
        tier = str(row["verdict"])
        counts[tier] = counts.get(tier, 0) + 1
    aggregate_parts = [
        f"{counts.get(tier, 0)} {tier}"
        for tier in VERDICT_TIER_ORDER
        if counts.get(tier, 0) > 0
    ]
    lines.append("")
    lines.append("总览: " + " | ".join(aggregate_parts))
    lines.append("")

    # Per-hypothesis table
    label_width = max(len(str(row["name_cn"])) for _, row in verdicts.iterrows())
    label_width = min(max(label_width, 16), 24)
    header = f"  {'HID':<4}{'名称':<{label_width + 2}}{'裁决':<10}{'可信度':<8}{'头条指标':<22}{'值':>10}{'n':>10}"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for _, row in verdicts.iterrows():
        tier = str(row["verdict"])
        verdict_text = _colorize(f"{tier:<10}", tier, enable=color)
        name = str(row["name_cn"])[:label_width]
        key_label_raw = row.get("key_label", "")
        # CSV round-trip turns "" into NaN (float), so guard explicitly.
        if key_label_raw is None or (
            isinstance(key_label_raw, float) and math.isnan(key_label_raw)
        ):
            key_label = "—"
        else:
            key_label_str = str(key_label_raw).strip()
            key_label = key_label_str[:22] if key_label_str else "—"
        value = _format_value(row.get("key_value"))
        n_obs = _format_n(row.get("n_obs"))
        lines.append(
            f"  {row['hid']:<4}{name:<{label_width + 2}}{verdict_text}"
            f"{row['confidence']:<8}{key_label:<22}{value:>10}{n_obs:>10}"
        )

    if track_summary is not None and not track_summary.empty:
        lines.append("")
        lines.append("研究主线分布:")
        lines.append("")
        for _, row in track_summary.iterrows():
            label = str(row.get("track_label", row.get("track", "")))
            hids = str(row.get("hypotheses", ""))
            tier_chips = []
            for tier in VERDICT_TIER_ORDER:
                count = int(row.get(tier, 0) or 0)
                if count > 0:
                    chip = _colorize(f"{count} {tier}", tier, enable=color)
                    tier_chips.append(chip)
            chips_text = " · ".join(tier_chips) if tier_chips else "—"
            lines.append(f"  {label:<22}({hids:<22}) → {chips_text}")

    lines.append("")
    lines.append("详见:")
    lines.append("  results/real_tables/cma_hypothesis_verdicts.csv")
    lines.append("  results/real_tables/cma_hypothesis_verdicts.tex   (论文用 LaTeX 表)")
    lines.append("  docs/paper_outline_verdicts.md                    (论文叙述段落)")
    return "\n".join(lines) + "\n"


def _read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except (OSError, ValueError):
        return None


# ── snapshot + diff ──────────────────────────────────────────────────


def save_verdict_snapshot(verdicts: pd.DataFrame, *, output_path: Path) -> Path:
    """Persist the verdict frame as a snapshot for later --compare-with."""
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    verdicts.to_csv(out_path, index=False)
    return out_path


def _coerce_float(value: object) -> float:
    try:
        v = float(str(value))
    except (TypeError, ValueError):
        return float("nan")
    return v


def _coerce_int(value: object) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def compute_verdict_diff(
    current: pd.DataFrame,
    previous: pd.DataFrame,
    *,
    delta_threshold: float = 1e-6,
) -> list[dict[str, object]]:
    """Compute a list of per-hid diff records.

    Each record is one of:
      - ``{"kind": "added", "hid": ...}`` (in current, not in previous)
      - ``{"kind": "removed", "hid": ...}`` (in previous, not in current)
      - ``{"kind": "changed", "hid": ..., changes: {...}}``
      - ``{"kind": "unchanged", "hid": ...}``

    A "changed" record's ``changes`` dict carries before/after for any of
    verdict / confidence / key_label / key_value / n_obs that differ.
    NaN-vs-NaN is treated as equal; NaN-vs-numeric counts as a change.
    """
    if current is None or current.empty:
        cur_by_hid: dict[str, dict] = {}
    else:
        cur_by_hid = {str(r["hid"]): dict(r) for _, r in current.iterrows()}
    if previous is None or previous.empty:
        prev_by_hid: dict[str, dict] = {}
    else:
        prev_by_hid = {str(r["hid"]): dict(r) for _, r in previous.iterrows()}

    rows: list[dict[str, object]] = []
    all_hids = sorted(set(cur_by_hid) | set(prev_by_hid))
    for hid in all_hids:
        if hid in cur_by_hid and hid not in prev_by_hid:
            rows.append({"kind": "added", "hid": hid, "current": cur_by_hid[hid]})
            continue
        if hid in prev_by_hid and hid not in cur_by_hid:
            rows.append({"kind": "removed", "hid": hid, "previous": prev_by_hid[hid]})
            continue
        cur = cur_by_hid[hid]
        prev = prev_by_hid[hid]
        changes: dict[str, dict[str, object]] = {}
        for field in ("verdict", "confidence", "key_label"):
            cur_val = "" if pd.isna(cur.get(field, "")) else str(cur.get(field, ""))
            prev_val = "" if pd.isna(prev.get(field, "")) else str(prev.get(field, ""))
            if cur_val != prev_val:
                changes[field] = {"before": prev_val, "after": cur_val}
        cur_v = _coerce_float(cur.get("key_value"))
        prev_v = _coerce_float(prev.get("key_value"))
        cur_nan = math.isnan(cur_v)
        prev_nan = math.isnan(prev_v)
        if cur_nan != prev_nan or (
            not cur_nan and not prev_nan and abs(cur_v - prev_v) > delta_threshold
        ):
            changes["key_value"] = {"before": prev_v, "after": cur_v}
        cur_n = _coerce_int(cur.get("n_obs"))
        prev_n = _coerce_int(prev.get("n_obs"))
        if cur_n != prev_n:
            changes["n_obs"] = {"before": prev_n, "after": cur_n}
        if changes:
            rows.append({
                "kind": "changed",
                "hid": hid,
                "name_cn": cur.get("name_cn", prev.get("name_cn", "")),
                "changes": changes,
            })
        else:
            rows.append({"kind": "unchanged", "hid": hid})
    return rows


def render_verdict_diff(
    diff_rows: list[dict[str, object]],
    *,
    color: bool = True,
) -> str:
    """Render diff rows as a multi-line string with tier-coloured arrows."""
    if not diff_rows:
        return "(无 diff 输入)\n"
    changed = [r for r in diff_rows if r["kind"] == "changed"]
    added = [r for r in diff_rows if r["kind"] == "added"]
    removed = [r for r in diff_rows if r["kind"] == "removed"]
    unchanged = [r for r in diff_rows if r["kind"] == "unchanged"]

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append(" VERDICT DIFF · 当前 vs 快照")
    lines.append("=" * 72)
    lines.append(
        f"  changed: {len(changed)}, added: {len(added)},"
        f" removed: {len(removed)}, unchanged: {len(unchanged)}"
    )
    lines.append("")

    if not (changed or added or removed):
        lines.append("  ✓ 所有 verdict 相同 — 没有变化。")
        return "\n".join(lines) + "\n"

    if changed:
        lines.append("已变更:")
        lines.append("")
        for row in changed:
            hid = str(row["hid"])
            name = str(row.get("name_cn", ""))
            lines.append(f"  {hid} · {name}")
            changes = row.get("changes", {})
            if not isinstance(changes, Mapping):
                continue
            for field, beats_raw in changes.items():
                if not isinstance(beats_raw, Mapping):
                    continue
                beats = beats_raw
                before = beats["before"]
                after = beats["after"]
                if field == "verdict":
                    bef_text = _colorize(str(before), str(before), enable=color)
                    aft_text = _colorize(str(after), str(after), enable=color)
                    lines.append(f"    verdict        : {bef_text}  →  {aft_text}")
                elif field == "key_value":
                    bef_str = "—" if isinstance(before, float) and math.isnan(before) else f"{before:.3f}"
                    aft_str = "—" if isinstance(after, float) and math.isnan(after) else f"{after:.3f}"
                    delta = ""
                    if (
                        isinstance(before, float) and isinstance(after, float)
                        and not math.isnan(before) and not math.isnan(after)
                    ):
                        delta = f"  (Δ {after - before:+.3f})"
                    lines.append(f"    key_value      : {bef_str}  →  {aft_str}{delta}")
                else:
                    lines.append(f"    {field:<14} : {before}  →  {after}")
            lines.append("")

    if added:
        lines.append("新增:")
        for row in added:
            current = row.get("current", {})
            cur = current if isinstance(current, Mapping) else {}
            lines.append(
                f"  + {row['hid']} {cur.get('name_cn', '')} → {cur.get('verdict', '')}"
            )
        lines.append("")

    if removed:
        lines.append("移除:")
        for row in removed:
            previous = row.get("previous", {})
            prev = previous if isinstance(previous, Mapping) else {}
            lines.append(
                f"  - {row['hid']} {prev.get('name_cn', '')} (was {prev.get('verdict', '')})"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


# ── Sensitivity sweep ────────────────────────────────────────────────


DEFAULT_SENSITIVITY_THRESHOLDS: tuple[float, ...] = (0.05, 0.10, 0.15)


def _benjamini_hochberg_q(p_values: list[float]) -> list[float]:
    """Benjamini-Hochberg adjusted q-values for the given list of p-values.

    NaN inputs propagate to NaN outputs and are ignored when computing m
    (the number of tested hypotheses). The result is monotone non-decreasing
    in original p-value order after sorting.
    """
    indexed = [(i, p) for i, p in enumerate(p_values) if not math.isnan(p)]
    m = len(indexed)
    out = [float("nan")] * len(p_values)
    if m == 0:
        return out
    indexed.sort(key=lambda kv: kv[1])
    raw_adjusted: list[float] = []
    for rank, (_, p) in enumerate(indexed, start=1):
        raw_adjusted.append(min(1.0, p * m / rank))
    monotone = raw_adjusted[:]
    for i in range(len(monotone) - 2, -1, -1):
        monotone[i] = min(monotone[i], monotone[i + 1])
    for (orig_idx, _), q in zip(indexed, monotone, strict=True):
        out[orig_idx] = q
    return out


def build_sensitivity_table(
    verdicts: pd.DataFrame,
    thresholds: Sequence[float] = DEFAULT_SENSITIVITY_THRESHOLDS,
) -> pd.DataFrame:
    """Return a per-hypothesis significance sweep across p thresholds.

    For each verdict row this returns ``hid``, ``name_cn``, ``p_value``
    plus one boolean column per threshold (``sig_at_<t>``) indicating
    whether ``p_value < t`` at that threshold. Hypotheses without a
    structured ``p_value`` (H2 / H3 / H6 / H7 — headline metric is a
    spread / share / ratio, not a p) get ``None`` in every threshold
    column.

    Two multiple-testing columns are also included: ``bonferroni_p =
    min(1, p * m)`` and ``bh_q`` (Benjamini-Hochberg q-value), where
    m is the number of hypotheses that carry a structured p_value.
    Hypotheses without a p_value get NaN in both columns.

    Downstream consumers (``render_sensitivity_table``,
    ``render_summary_json``) can answer "if I tightened the threshold
    from 0.10 to 0.05, which verdicts flip?" without re-parsing
    ``evidence_summary`` strings.
    """
    if verdicts.empty:
        return pd.DataFrame(columns=["hid", "name_cn", "p_value"])
    threshs = tuple(sorted({float(t) for t in thresholds}))
    p_values: list[float] = []
    base_rows: list[dict[str, object]] = []
    for _, v in verdicts.iterrows():
        raw = v.get("p_value") if "p_value" in v.index else None
        try:
            p = float(raw) if raw is not None else float("nan")
        except (TypeError, ValueError):
            p = float("nan")
        p_values.append(p)
        base_rows.append(
            {
                "hid": str(v.get("hid", "")),
                "name_cn": str(v.get("name_cn", "")),
                "p_value": p,
            }
        )
    m = sum(1 for p in p_values if not math.isnan(p))
    bh_qs = _benjamini_hochberg_q(p_values)
    rows: list[dict[str, object]] = []
    for base, p, bh_q in zip(base_rows, p_values, bh_qs, strict=True):
        has_p = not math.isnan(p)
        row = dict(base)
        for t in threshs:
            row[f"sig_at_{t}"] = (p < t) if has_p else None
        row["bonferroni_p"] = min(1.0, p * m) if has_p and m > 0 else float("nan")
        row["bh_q"] = bh_q
        rows.append(row)
    return pd.DataFrame(rows)


def render_sensitivity_table(
    verdicts: pd.DataFrame,
    thresholds: Sequence[float] = DEFAULT_SENSITIVITY_THRESHOLDS,
) -> str:
    """Render the sensitivity sweep as an aligned text block."""
    threshs = tuple(sorted({float(t) for t in thresholds}))
    table = build_sensitivity_table(verdicts, threshs)

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append(
        f" 假说 verdict p 值灵敏度({len(threshs)} 阈值)"
    )
    lines.append("=" * 72)

    if table.empty:
        lines.append("(verdicts CSV 为空 —— 先跑 `index-inclusion-cma`。)")
        return "\n".join(lines) + "\n"

    p_gated = table.loc[table["p_value"].notna()].copy()
    non_p = table.loc[table["p_value"].isna()].copy()

    if p_gated.empty:
        lines.append("(没有任何假说由单一 p 决定 verdict —— sweep 不适用。)")
    else:
        # Header
        thresh_cols = [f"p<{t}" for t in threshs]
        header = (
            f"  {'hid':<5}{'p_value':>9}{'bonf_p':>9}{'bh_q':>9}  "
            + "  ".join(f"{c:>6}" for c in thresh_cols)
        )
        lines.append(header)
        lines.append("  " + "─" * (len(header) - 2))
        for _, row in p_gated.iterrows():
            cells = []
            for t in threshs:
                flag = row[f"sig_at_{t}"]
                cells.append("✓" if flag else "—")
            bonf = row.get("bonferroni_p", float("nan"))
            bhq = row.get("bh_q", float("nan"))
            bonf_text = f"{float(bonf):.4f}" if pd.notna(bonf) else "    —"
            bhq_text = f"{float(bhq):.4f}" if pd.notna(bhq) else "    —"
            lines.append(
                f"  {str(row['hid']):<5}"
                f"{float(row['p_value']):>9.4f}"
                f"{bonf_text:>9}"
                f"{bhq_text:>9}  "
                + "  ".join(f"{c:>6}" for c in cells)
            )
        # tally: at each threshold, how many sig?
        lines.append("")
        tally_parts = []
        for t in threshs:
            sig_count = int(p_gated[f"sig_at_{t}"].sum())
            total = len(p_gated)
            tally_parts.append(f"p<{t}: {sig_count}/{total} 显著")
        lines.append("  " + " · ".join(tally_parts))
        if "bh_q" in p_gated.columns:
            bh_sig = int((p_gated["bh_q"] < 0.10).fillna(False).sum())
            lines.append(
                f"  Bonferroni (raw·m): bonf_p<0.10 通过 "
                f"{int((p_gated['bonferroni_p'] < 0.10).fillna(False).sum())}/{len(p_gated)};"
                f" Benjamini-Hochberg: bh_q<0.10 通过 {bh_sig}/{len(p_gated)}"
            )

    if not non_p.empty:
        lines.append("")
        non_p_ids = " ".join(str(h) for h in non_p["hid"])
        lines.append(
            f"  注:{non_p_ids} 头条指标不是 p(spread / 命中率 / AUM 比率),"
        )
        lines.append("  其 verdict 不会随 p 阈值变化，因此不在 sweep 范围内。")
    return "\n".join(lines).rstrip() + "\n"


# ── CLI ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Print a terminal-friendly summary of the current 7-hypothesis "
            "verdict picture (drawn from cma_hypothesis_verdicts.csv)."
        )
    )
    parser.add_argument(
        "--verdicts",
        default=str(DEFAULT_VERDICTS),
        help=f"Path to the verdicts CSV (default: {DEFAULT_VERDICTS}).",
    )
    parser.add_argument(
        "--track-summary",
        default=str(DEFAULT_TRACK_SUMMARY),
        help=(
            f"Path to the track summary CSV "
            f"(default: {DEFAULT_TRACK_SUMMARY})."
        ),
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colour escape codes.",
    )
    parser.add_argument(
        "--snapshot",
        metavar="PATH",
        help="Save the current verdicts CSV to PATH for later --compare-with diffs.",
    )
    parser.add_argument(
        "--compare-with",
        metavar="PATH",
        help=(
            "Render a before/after diff against the snapshot at PATH instead of "
            "(or in addition to) the standard summary."
        ),
    )
    parser.add_argument(
        "--vs-pap",
        action="store_true",
        help=(
            "Shortcut for --compare-with against the most recent "
            "snapshots/pre-registration-*.csv (PAP baseline). Mutually "
            "exclusive with --compare-with."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help=(
            "Output format. 'text' (default) is the colourised terminal "
            "summary; 'json' is a machine-readable dump of the same data "
            "(verdicts + track summary + optional diff + optional sensitivity)."
        ),
    )
    parser.add_argument(
        "--sensitivity",
        nargs="*",
        type=float,
        metavar="T",
        default=None,
        help=(
            "Print a p-value sensitivity sweep. Pass thresholds as floats "
            "(e.g. `--sensitivity 0.01 0.05 0.10 0.15`); pass with no "
            "arguments to use the default (0.05 0.10 0.15). H1 / H4 / H5 "
            "(p-gated hypotheses) get ✓ / — per threshold; H2 / H3 / H6 / "
            "H7 are out of scope."
        ),
    )
    return parser


def render_summary_json(
    verdicts: pd.DataFrame,
    track_summary: pd.DataFrame | None = None,
    *,
    diff_rows: list[dict[str, object]] | None = None,
    sensitivity_thresholds: Sequence[float] | None = None,
) -> str:
    """Return a JSON string carrying the full verdict picture.

    Schema is stable for downstream tooling:
    ``{"verdicts": [...], "track_summary": [...], "aggregate": {...},
    "diff": [...]?, "sensitivity": {"thresholds": [...], "rows": [...]}?}``.
    NaN values become ``null``; numeric fields stay numeric. The
    ``sensitivity`` block only appears when ``sensitivity_thresholds``
    is non-None.
    """
    import json

    def _row_to_jsonable(row: pd.Series) -> dict[str, object]:
        out: dict[str, object] = {}
        for col, value in row.items():
            if isinstance(value, float) and math.isnan(value):
                out[col] = None  # type: ignore[index]
            elif isinstance(value, (int, float, str, bool)) or value is None:
                out[col] = value  # type: ignore[index]
            else:
                out[col] = str(value)  # type: ignore[index]
        return out

    payload: dict[str, object] = {}
    verdict_records = (
        [_row_to_jsonable(row) for _, row in verdicts.iterrows()]
        if not verdicts.empty
        else []
    )
    payload["verdicts"] = verdict_records
    aggregate: dict[str, int] = {tier: 0 for tier in VERDICT_TIER_ORDER}
    for row in verdict_records:
        tier = str(row.get("verdict", ""))
        aggregate[tier] = aggregate.get(tier, 0) + 1
    payload["aggregate"] = aggregate
    payload["track_summary"] = (
        [_row_to_jsonable(row) for _, row in track_summary.iterrows()]
        if track_summary is not None and not track_summary.empty
        else []
    )
    if diff_rows is not None:
        payload["diff"] = diff_rows
    if sensitivity_thresholds is not None:
        threshs = tuple(sorted({float(t) for t in sensitivity_thresholds}))
        sens_table = build_sensitivity_table(verdicts, threshs)
        payload["sensitivity"] = {
            "thresholds": list(threshs),
            "rows": [_row_to_jsonable(row) for _, row in sens_table.iterrows()],
        }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    import sys

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    verdicts_path = Path(args.verdicts)
    track_path = Path(args.track_summary)

    verdicts = _read_csv(verdicts_path)
    if verdicts is None:
        print(
            f"[verdict-summary] verdicts CSV not found or unreadable: {verdicts_path}"
        )
        print("Run `index-inclusion-cma` to populate it.")
        return 1
    track_summary = _read_csv(track_path)

    enable_color = not args.no_color and sys.stdout.isatty()

    if args.snapshot:
        snapshot_path = Path(args.snapshot)
        save_verdict_snapshot(verdicts, output_path=snapshot_path)
        print(f"[verdict-summary] saved snapshot to {snapshot_path}")

    if args.vs_pap and args.compare_with:
        print(
            "[verdict-summary] --vs-pap and --compare-with are mutually exclusive; pick one."
        )
        return 1

    compare_path: Path | None = None
    if args.compare_with:
        compare_path = Path(args.compare_with)
    elif args.vs_pap:
        snapshots = sorted(_PAP_SNAPSHOTS_DIR.glob("pre-registration-*.csv"))
        if not snapshots:
            print(
                "[verdict-summary] --vs-pap: no snapshots/pre-registration-*.csv found. "
                "Run `index-inclusion-verdict-summary --snapshot snapshots/pre-registration-YYYY-MM-DD.csv` "
                "first to freeze a PAP baseline."
            )
            return 1
        compare_path = snapshots[-1]
        print(f"[verdict-summary] --vs-pap → comparing against {compare_path}")

    diff_rows: list[dict[str, object]] | None = None
    if compare_path is not None:
        previous = _read_csv(compare_path)
        if previous is None:
            print(
                f"[verdict-summary] snapshot not found / unreadable: {compare_path}"
            )
            return 1
        diff_rows = compute_verdict_diff(verdicts, previous)

    sensitivity_thresholds: tuple[float, ...] | None = None
    if args.sensitivity is not None:
        # `--sensitivity` with no values → use defaults; with values → use those.
        sensitivity_thresholds = (
            tuple(args.sensitivity)
            if args.sensitivity
            else DEFAULT_SENSITIVITY_THRESHOLDS
        )

    if args.format == "json":
        print(
            render_summary_json(
                verdicts,
                track_summary,
                diff_rows=diff_rows,
                sensitivity_thresholds=sensitivity_thresholds,
            ),
            end="",
        )
        return 0

    if diff_rows is not None:
        print(render_verdict_diff(diff_rows, color=enable_color))
        if sensitivity_thresholds is not None:
            print(render_sensitivity_table(verdicts, sensitivity_thresholds))
        return 0

    print(render_summary(verdicts, track_summary, color=enable_color))
    if sensitivity_thresholds is not None:
        print(render_sensitivity_table(verdicts, sensitivity_thresholds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
