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
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
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
        v = float(value)
    except (TypeError, ValueError):
        return str(value) if value else "—"
    if math.isnan(v):
        return "—"
    return f"{v:.3f}"


def _format_n(n_obs: object) -> str:
    try:
        n = int(n_obs)
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
    lines.append(" INDEX-INCLUSION RESEARCH · 假说裁决摘要")
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
    return parser


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
    print(render_summary(verdicts, track_summary, color=enable_color))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
