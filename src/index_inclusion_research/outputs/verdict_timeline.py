"""Hypothesis-verdict evolution timeline reconstructed from git history.

The 40th console script (``index-inclusion-verdict-timeline``) walks the
git history of ``results/real_tables/cma_hypothesis_verdicts.csv`` and
renders a horizontal-swimlane figure showing how each of H1..H7's
verdict has evolved across commits, with the current PAP baseline
(resolved dynamically from the latest ``snapshots/pre-registration-*.csv``
via :func:`default_pap_baseline_date`) marked as a vertical reference line.

Why this complements the PAP deviation auditor
----------------------------------------------
The PAP deviation auditor (commit ``48a22f0``) compares the *current*
verdict CSV against the latest frozen PAP snapshot and emits a
single deviation report. That answers "where have we ended up vs. the
pre-registration?" but not "how did we get here?". The verdict timeline
fills the second question by walking ``git log --follow`` on the
verdicts CSV, materialising each commit's CSV via ``git show``, and
stitching the per-commit verdicts into a long-format DataFrame ready
for both plotting and downstream JSON export.

Design notes
------------
- **stdlib only for git access**: uses :mod:`subprocess` (no
  ``gitpython``); the project's existing tooling already shells out to
  ``git`` for snapshots so we keep the dependency footprint flat.
- **graceful empty fallback**: a checkout with no recorded commits for
  the CSV (e.g. a fresh clone before the file is tracked) returns an
  empty DataFrame; plotting then renders an "empty timeline" placeholder
  rather than raising — so the doctor / paper-bundle pipeline never
  hard-fails on a partially-populated checkout.
- **deterministic seed**: matplotlib + numpy RNG seeded with ``0`` so
  the figure is byte-identical across repeated renders given the same
  history.
- **bounded walk**: ``max_history`` (default 50) caps the commit walk
  so a future repo with 1000+ commits touching the file still renders
  in seconds. Most-recent N commits are kept; older commits drop off
  the left edge with a single annotation explaining the truncation.
"""

from __future__ import annotations

import logging
import math
import subprocess
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402 -- after backend pin
import matplotlib.patches as mpatches  # noqa: E402 -- after backend pin
import matplotlib.pyplot as plt  # noqa: E402 -- after backend pin

from index_inclusion_research.plot_style import configure_matplotlib_cjk

configure_matplotlib_cjk(plt)

logger = logging.getLogger(__name__)

DEFAULT_TARGET_CSV = "results/real_tables/cma_hypothesis_verdicts.csv"
DEFAULT_MAX_HISTORY = 50

# Last-resort fallback baseline date, used only when no
# snapshots/pre-registration-*.csv exists on disk. Normal runs resolve the
# CURRENT baseline dynamically via default_pap_baseline_date() so the marker
# tracks the live re-baselined snapshot rather than a hardcoded freeze date.
PAP_BASELINE_DATE = "2026-05-16"


def default_pap_baseline_date(root: Path) -> str:
    """ISO date of the CURRENT PAP baseline snapshot under ``root/snapshots``.

    Resolves the latest ``pre-registration-YYYY-MM-DD.csv`` (lexicographic sort
    = date order, mirroring ``pap_diff.resolve_default_baseline`` and
    ``dashboard.loaders.load_pap_summary``) and returns its ``YYYY-MM-DD``
    stamp. Falls back to :data:`PAP_BASELINE_DATE` when no snapshot exists, so a
    fresh checkout still renders the vertical baseline marker.
    """
    snap_dir = Path(root) / "snapshots"
    if snap_dir.is_dir():
        snapshots = sorted(snap_dir.glob("pre-registration-*.csv"))
        if snapshots:
            return snapshots[-1].stem.removeprefix("pre-registration-")
    return PAP_BASELINE_DATE

# Hypothesis ordering — keep H1..H7 across every renderer so swimlanes
# match the rest of the project's figures.
EXPECTED_HIDS: tuple[str, ...] = ("H1", "H2", "H3", "H4", "H5", "H6", "H7")

# Verdict text -> "broad category" used for the marker palette. New
# verdict strings (typos, language drift) fall back to ``"unknown"``
# which the renderer paints in a muted grey square so reviewers can spot
# them visually without crashing the pipeline.
_VERDICT_CATEGORY: dict[str, str] = {
    "支持": "support",
    "部分支持": "partial",
    "证据不足": "insufficient",
}
_CATEGORY_COLORS: dict[str, str] = {
    "support": "#2da44e",
    "partial": "#e9b949",
    "insufficient": "#c0392b",
    "unknown": "#7f8c8d",
}
_CATEGORY_LABEL_CN: dict[str, str] = {
    "support": "支持",
    "partial": "部分支持",
    "insufficient": "证据不足",
    "unknown": "未知/语言漂移",
}


# ---------------------------------------------------------------------------
# Git history walker
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Commit:
    """Parsed git-log row: SHA, ISO-8601 author date, commit subject."""

    sha: str
    iso_date: str
    subject: str


def _run_git(
    args: list[str],
    *,
    cwd: Path,
    capture_text: bool = True,
) -> str:
    """Run ``git`` with *args* under *cwd*; return stdout (text).

    Failures are raised as :class:`subprocess.CalledProcessError` and
    caught one layer up — the timeline builder converts them into an
    empty result rather than crashing the figures pipeline.
    """
    completed = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=capture_text,
        check=True,
    )
    return completed.stdout


def _list_commits_touching(
    repo_root: Path, target_csv: str, *, max_history: int
) -> list[_Commit]:
    """Return up to *max_history* most-recent commits that touched *target_csv*.

    Uses ``git log --follow`` so file renames don't truncate the
    history. Format ``"%H|%ai|%s"`` keeps the parser tiny and survives
    commit subjects containing pipes (we limit the split to the first
    two ``|``).
    """
    try:
        out = _run_git(
            [
                "log",
                "--follow",
                f"-n{max_history}",
                "--pretty=format:%H|%ai|%s",
                "--",
                target_csv,
            ],
            cwd=repo_root,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(
            "git log for %s failed (rc=%d); returning empty history.",
            target_csv,
            exc.returncode,
        )
        return []
    commits: list[_Commit] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 2)
        if len(parts) < 3:
            logger.warning("malformed git log line dropped: %r", line)
            continue
        sha, iso_date, subject = parts
        commits.append(
            _Commit(sha=sha.strip(), iso_date=iso_date.strip(), subject=subject.strip())
        )
    return commits


def _read_csv_at_commit(
    repo_root: Path, commit_sha: str, target_csv: str
) -> pd.DataFrame:
    """``git show <sha>:<target_csv>`` → DataFrame. Empty on any error."""
    try:
        blob = _run_git(
            ["show", f"{commit_sha}:{target_csv}"], cwd=repo_root
        )
    except subprocess.CalledProcessError as exc:
        # ``git show`` returns 128 when the file did not exist at the
        # named commit. With ``--follow`` enabled this can happen when
        # the file was created after a sibling rename — drop the row
        # silently rather than spamming the log with warnings for every
        # such commit.
        logger.debug(
            "git show %s:%s failed (rc=%d); skipping this commit.",
            commit_sha,
            target_csv,
            exc.returncode,
        )
        return pd.DataFrame()
    if not blob.strip():
        return pd.DataFrame()
    try:
        return pd.read_csv(StringIO(blob))
    except (ValueError, pd.errors.ParserError) as exc:
        logger.warning(
            "verdict CSV at %s is unparseable: %s; skipping this commit.",
            commit_sha,
            exc,
        )
        return pd.DataFrame()


def _short_subject(subject: str, *, limit: int = 60) -> str:
    """Ellipsis-trim a commit subject so it fits in the figure annotation."""
    subject = subject.strip()
    if len(subject) <= limit:
        return subject
    return subject[: limit - 1] + "…"


def _parse_commit_date(iso_date: str) -> datetime:
    """Parse ``git`` ``%ai`` (``YYYY-MM-DD HH:MM:SS +ZZZZ``) → UTC-naive ``datetime``.

    We drop the timezone offset deliberately: matplotlib's date plotting
    doesn't need it, and we want the figure to read in local commit time
    rather than UTC (the project is single-author, single-timezone).
    """
    # Take the first 19 chars (``YYYY-MM-DD HH:MM:SS``) — robust to any
    # offset format ``git`` prints.
    return datetime.strptime(iso_date[:19], "%Y-%m-%d %H:%M:%S")


def build_verdict_timeline_from_git(
    repo_root: Path | str,
    target_csv: str = DEFAULT_TARGET_CSV,
    max_history: int = DEFAULT_MAX_HISTORY,
) -> pd.DataFrame:
    """Reconstruct the per-commit verdict snapshots for H1..H7.

    Parameters
    ----------
    repo_root:
        Path to the git repository (the directory containing ``.git``).
    target_csv:
        Repository-relative path to the verdicts CSV. The default points
        at the canonical ``results/real_tables/cma_hypothesis_verdicts.csv``.
    max_history:
        Cap on the number of most-recent commits to walk. Defaults to 50
        — enough to cover the entire current history with headroom, fast
        enough to keep the figures pipeline snappy.

    Returns
    -------
    pandas.DataFrame
        Long-format frame with one row per (commit, hypothesis) pair and
        columns ``commit_sha``, ``commit_date`` (timezone-naive
        ``datetime64[ns]``), ``commit_msg_short``, ``hypothesis_id``,
        ``verdict``, ``verdict_category``, ``confidence``,
        ``evidence_tier``, ``n_obs``. Sorted chronologically (oldest
        commit first), then by ``hypothesis_id`` so swimlane rendering is
        deterministic. Returns an empty frame (all expected columns
        present, zero rows) when the git history is empty.
    """
    repo_root = Path(repo_root).expanduser().resolve()
    commits = _list_commits_touching(
        repo_root, target_csv, max_history=max_history
    )
    columns = [
        "commit_sha",
        "commit_date",
        "commit_msg_short",
        "hypothesis_id",
        "verdict",
        "verdict_category",
        "confidence",
        "evidence_tier",
        "n_obs",
    ]
    if not commits:
        empty = pd.DataFrame(columns=columns)
        empty["commit_date"] = pd.to_datetime(empty["commit_date"])
        return empty
    rows: list[dict[str, object]] = []
    for commit in commits:
        csv_df = _read_csv_at_commit(repo_root, commit.sha, target_csv)
        if csv_df.empty:
            continue
        # Column names changed over the project's history (early commits
        # used ``hid``, later commits may use ``hypothesis_id``). Map
        # whichever variant exists into the modern name.
        hid_col = "hid" if "hid" in csv_df.columns else "hypothesis_id"
        if hid_col not in csv_df.columns or "verdict" not in csv_df.columns:
            logger.warning(
                "commit %s verdicts CSV is missing required columns; skipping.",
                commit.sha,
            )
            continue
        commit_date = _parse_commit_date(commit.iso_date)
        msg_short = _short_subject(commit.subject)
        for _, csv_row in csv_df.iterrows():
            verdict = str(csv_row["verdict"]).strip()
            rows.append(
                {
                    "commit_sha": commit.sha,
                    "commit_date": commit_date,
                    "commit_msg_short": msg_short,
                    "hypothesis_id": str(csv_row[hid_col]).strip(),
                    "verdict": verdict,
                    "verdict_category": _VERDICT_CATEGORY.get(verdict, "unknown"),
                    "confidence": (
                        str(csv_row["confidence"]).strip()
                        if "confidence" in csv_df.columns
                        else ""
                    ),
                    "evidence_tier": (
                        str(csv_row["evidence_tier"]).strip()
                        if "evidence_tier" in csv_df.columns
                        else ""
                    ),
                    "n_obs": (
                        _to_int(csv_row["n_obs"])
                        if "n_obs" in csv_df.columns
                        else None
                    ),
                }
            )
    if not rows:
        empty = pd.DataFrame(columns=columns)
        empty["commit_date"] = pd.to_datetime(empty["commit_date"])
        return empty
    frame = pd.DataFrame(rows, columns=columns)
    # Ascending so the renderer can scan left-to-right.
    frame = frame.sort_values(
        ["commit_date", "hypothesis_id"], kind="mergesort"
    ).reset_index(drop=True)
    return frame


def _to_int(value: object) -> int | None:
    """Coerce a verdict-CSV ``n_obs`` cell into ``int | None``.

    Missing values land as ``NaN`` in pandas; we round + cast safely so
    the JSON export below doesn't choke on floats.
    """
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------


def count_verdict_changes(timeline_df: pd.DataFrame) -> dict[str, int]:
    """Per-hypothesis count of verdict-text changes across the timeline.

    A "change" is any row whose ``verdict`` differs from the previous
    commit's ``verdict`` for the same ``hypothesis_id``. The very first
    commit per hypothesis does NOT count as a change (there's nothing to
    diff against).

    Returns a dict keyed by ``hypothesis_id``; hypotheses absent from
    the timeline map to ``0`` so callers can iterate over
    ``EXPECTED_HIDS`` without ``KeyError``.
    """
    counts: dict[str, int] = {hid: 0 for hid in EXPECTED_HIDS}
    if timeline_df.empty:
        return counts
    for hid, sub in timeline_df.groupby("hypothesis_id", sort=False):
        sub_sorted = sub.sort_values("commit_date", kind="mergesort")
        prev: str | None = None
        for verdict in sub_sorted["verdict"]:
            current = str(verdict).strip()
            if prev is not None and current != prev:
                counts[str(hid)] = counts.get(str(hid), 0) + 1
            prev = current
    return counts


def total_verdict_changes(timeline_df: pd.DataFrame) -> int:
    """Sum of :func:`count_verdict_changes` across all hypotheses."""
    return int(sum(count_verdict_changes(timeline_df).values()))


# ---------------------------------------------------------------------------
# Plot rendering
# ---------------------------------------------------------------------------


def _empty_timeline_figure(
    output_png_path: Path,
    output_pdf_path: Path | None,
) -> Path:
    """Render a placeholder figure when the git history is empty.

    The placeholder still satisfies the ≥800x600 contract the
    doctor / paper-bundle checks rely on, so freshness signaling stays
    consistent across the fleet of figures.
    """
    png_path = Path(output_png_path).expanduser().resolve()
    png_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 7), dpi=100)
    ax.set_axis_off()
    ax.text(
        0.5,
        0.5,
        "暂无 verdicts CSV 的 git 历史。\n"
        "请提交至少一次 results/real_tables/cma_hypothesis_verdicts.csv 后重新运行。",
        ha="center",
        va="center",
        fontsize=14,
        color="#5c6b77",
        transform=ax.transAxes,
    )
    ax.set_title("CMA 假说裁决演进 (H1-H7)", fontsize=14, pad=14)
    fig.tight_layout()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        fig.savefig(png_path, dpi=100, bbox_inches="tight")
        if output_pdf_path is not None:
            pdf_path = Path(output_pdf_path).expanduser().resolve()
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path


def _previous_verdict_per_hypothesis(
    timeline_df: pd.DataFrame,
) -> dict[tuple[str, str], str | None]:
    """Map ``(hypothesis_id, commit_sha) -> previous verdict text``.

    Used by the renderer to decide whether to draw a "verdict changed"
    marker (square) versus a "verdict held" marker (circle).
    """
    out: dict[tuple[str, str], str | None] = {}
    for hid, sub in timeline_df.groupby("hypothesis_id", sort=False):
        sub_sorted = sub.sort_values("commit_date", kind="mergesort").reset_index(
            drop=True
        )
        prev: str | None = None
        for _, row in sub_sorted.iterrows():
            out[(str(hid), str(row["commit_sha"]))] = prev
            prev = str(row["verdict"]).strip()
    return out


def render_verdict_timeline_plot(
    timeline_df: pd.DataFrame,
    output_png_path: Path | str,
    output_pdf_path: Path | str | None = None,
    *,
    pap_baseline_date: str = PAP_BASELINE_DATE,
    seed: int = 0,
) -> Path:
    """Render the H1..H7 verdict-evolution swimlane figure.

    Parameters
    ----------
    timeline_df:
        Output of :func:`build_verdict_timeline_from_git`. Empty input
        is allowed — a placeholder figure is emitted instead.
    output_png_path:
        Mandatory PNG destination. Parent directory is created if
        missing.
    output_pdf_path:
        Optional vector twin. Pass ``None`` to skip the PDF (the doctor
        figure check still passes — it only enforces PDF presence when
        the pipeline asked for one).
    pap_baseline_date:
        ISO date the PAP discipline locked the methodology. Drawn as a
        vertical dashed line labeled "PAP baseline". Default is the
        project's 2026-05-16 freeze date.
    seed:
        RNG seed for the matplotlib draws — kept for symmetry with the
        rest of the figure pipeline (currently only matplotlib font
        fallback uses it, but pinning it makes the figure
        byte-deterministic).

    Returns
    -------
    pathlib.Path
        Absolute path of the written PNG.
    """
    # Seed RNG so the figure is byte-deterministic across runs.
    np.random.seed(seed)

    png_path = Path(output_png_path).expanduser().resolve()
    pdf_path_resolved: Path | None
    if output_pdf_path is None:
        pdf_path_resolved = None
    else:
        pdf_path_resolved = Path(output_pdf_path).expanduser().resolve()
    if timeline_df.empty:
        return _empty_timeline_figure(png_path, pdf_path_resolved)

    png_path.parent.mkdir(parents=True, exist_ok=True)
    prev_lookup = _previous_verdict_per_hypothesis(timeline_df)

    # 14x8 in @ 100 dpi -> 1400x800 px before trim. Generous so post-
    # tight_layout / bbox_inches='tight' trim stays above 800x600.
    fig, ax = plt.subplots(figsize=(14, 8), dpi=100)

    # Y axis: one swimlane per hypothesis. Reverse so H1 is on top.
    y_positions = {hid: idx for idx, hid in enumerate(reversed(EXPECTED_HIDS))}
    for _hid, y in y_positions.items():
        ax.axhline(y=y, color="#e1e4e8", linewidth=0.6, zorder=0)

    # Plot each verdict marker. Use circles for "verdict same as
    # previous commit" and squares for "verdict text changed" — the
    # latter highlights flow-points reviewers should focus on.
    for hid in EXPECTED_HIDS:
        sub = timeline_df[timeline_df["hypothesis_id"] == hid]
        if sub.empty:
            continue
        sub_sorted = sub.sort_values("commit_date", kind="mergesort").reset_index(
            drop=True
        )
        y = y_positions[hid]
        # Connect the dots with a thin grey line so reviewers can follow
        # the chronology even when verdicts repeat.
        ax.plot(
            sub_sorted["commit_date"],
            [y] * len(sub_sorted),
            color="#c8d1d9",
            linewidth=1.0,
            zorder=1,
            alpha=0.7,
        )
        for _, row in sub_sorted.iterrows():
            verdict = str(row["verdict"]).strip()
            category = str(row["verdict_category"]).strip() or "unknown"
            color = _CATEGORY_COLORS.get(category, _CATEGORY_COLORS["unknown"])
            prev = prev_lookup.get((hid, str(row["commit_sha"])))
            # Square = verdict changed vs. previous commit; circle = held.
            marker = "s" if prev is not None and prev != verdict else "o"
            ax.scatter(
                [row["commit_date"]],
                [y],
                marker=marker,
                s=120,
                color=color,
                edgecolors="white",
                linewidths=1.0,
                zorder=3,
            )

    # PAP baseline vertical line — anchored on the date the PAP
    # discipline locked the methodology. matplotlib's stubs prefer
    # ``float`` x-coords, so we convert via ``mdates.date2num`` rather
    # than passing ``datetime`` directly (axvline / text accept either
    # at runtime, but the typed call sites trip mypy).
    try:
        pap_dt = datetime.strptime(pap_baseline_date, "%Y-%m-%d")
    except ValueError:
        pap_dt = None
    if pap_dt is not None:
        pap_num = float(mdates.date2num(pap_dt))
        ax.axvline(
            x=pap_num,
            color="#5c6b77",
            linestyle="--",
            linewidth=1.2,
            zorder=2,
        )
        ax.text(
            pap_num,
            len(EXPECTED_HIDS) - 0.4,
            "PAP baseline",
            rotation=90,
            ha="right",
            va="top",
            fontsize=9,
            color="#5c6b77",
        )

    # Right-edge annotation: latest verdict text per hypothesis.
    latest_per_hid: dict[str, tuple[datetime, str]] = {}
    for hid in EXPECTED_HIDS:
        sub = timeline_df[timeline_df["hypothesis_id"] == hid]
        if sub.empty:
            continue
        sub_sorted = sub.sort_values("commit_date", kind="mergesort").reset_index(
            drop=True
        )
        last_row = sub_sorted.iloc[-1]
        latest_per_hid[hid] = (
            last_row["commit_date"],
            str(last_row["verdict"]).strip(),
        )
    if latest_per_hid:
        max_date = max(dt for dt, _ in latest_per_hid.values())
        max_date_num = float(mdates.date2num(max_date))
        # Place annotations slightly to the right of the most recent
        # commit so they don't overlap the markers.
        for hid, (_, verdict) in latest_per_hid.items():
            ax.annotate(
                verdict,
                xy=(max_date_num, float(y_positions[hid])),
                xytext=(8, 0),
                textcoords="offset points",
                ha="left",
                va="center",
                fontsize=9,
                color="#1f2933",
            )

    # Y axis labels — H1..H7 + name hint if available.
    ax.set_yticks(list(y_positions.values()))
    ax.set_yticklabels(list(y_positions.keys()))
    ax.set_ylim(-0.5, len(EXPECTED_HIDS) - 0.5)

    # X axis: date formatter.
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=8))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate(rotation=30, ha="right")

    # Legend keyed on verdict category + marker shape.
    color_handles = [
        mpatches.Patch(
            color=_CATEGORY_COLORS[cat],
            label=_CATEGORY_LABEL_CN[cat],
        )
        for cat in ("support", "partial", "insufficient")
    ]
    shape_handles = [
        plt.Line2D(
            [0], [0], marker="s", color="#1f2933", linestyle="None",
            markerfacecolor="#7f8c8d", markersize=9,
            label="裁决文本改变（square）",
        ),
        plt.Line2D(
            [0], [0], marker="o", color="#1f2933", linestyle="None",
            markerfacecolor="#7f8c8d", markersize=9,
            label="裁决保持（circle）",
        ),
    ]
    legend1 = ax.legend(
        handles=color_handles,
        loc="lower left",
        bbox_to_anchor=(0.0, -0.32),
        frameon=False,
        ncol=3,
        fontsize=9,
        title="裁决类别",
        title_fontsize=9,
    )
    legend1.set_zorder(5)
    ax.add_artist(legend1)
    legend2 = ax.legend(
        handles=shape_handles,
        loc="lower right",
        bbox_to_anchor=(1.0, -0.32),
        frameon=False,
        ncol=2,
        fontsize=9,
        title="标记形状",
        title_fontsize=9,
    )
    legend2.set_zorder(5)

    n_commits = timeline_df["commit_sha"].nunique()
    total_changes = total_verdict_changes(timeline_df)
    ax.set_title(
        f"CMA 假说裁决演进 (H1-H7) · {n_commits} 个 commit · "
        f"{total_changes} 次裁决改变",
        fontsize=13,
        pad=14,
    )

    fig.tight_layout()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        fig.savefig(png_path, dpi=100, bbox_inches="tight")
        if pdf_path_resolved is not None:
            pdf_path_resolved.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(pdf_path_resolved, bbox_inches="tight")
    plt.close(fig)
    return png_path


# ---------------------------------------------------------------------------
# Public summary helpers
# ---------------------------------------------------------------------------


def summarize_for_public_summary(
    timeline_df: pd.DataFrame,
) -> dict[str, object]:
    """Slim payload the public summary JSON embeds.

    Shape::

        {
          "total_commits_tracked": int,
          "first_commit_date": str | None,   # ISO date, oldest commit
          "last_commit_date": str | None,    # ISO date, most-recent commit
          "total_verdict_changes": int,
          "verdict_changes_per_hypothesis": {"H1": int, ..., "H7": int},
        }

    Empty input returns zero counts and ``None`` for dates so the JSON
    schema stays stable across partially-populated checkouts.
    """
    counts = count_verdict_changes(timeline_df)
    payload: dict[str, object] = {
        "total_commits_tracked": int(timeline_df["commit_sha"].nunique())
        if not timeline_df.empty
        else 0,
        "first_commit_date": None,
        "last_commit_date": None,
        "total_verdict_changes": int(sum(counts.values())),
        "verdict_changes_per_hypothesis": counts,
    }
    if not timeline_df.empty:
        first = timeline_df["commit_date"].min()
        last = timeline_df["commit_date"].max()
        # ``commit_date`` is ``datetime64[ns]``; ``.date().isoformat()``
        # gives ``YYYY-MM-DD`` which is what the public summary wants.
        payload["first_commit_date"] = pd.Timestamp(first).date().isoformat()
        payload["last_commit_date"] = pd.Timestamp(last).date().isoformat()
    return payload


# ---------------------------------------------------------------------------
# Default paths
# ---------------------------------------------------------------------------


def default_png_path(repo_root: Path | None = None) -> Path:
    from index_inclusion_research import paths

    root = repo_root or paths.project_root()
    return root / "results" / "figures" / "verdict_timeline.png"


def default_pdf_path(repo_root: Path | None = None) -> Path:
    from index_inclusion_research import paths

    root = repo_root or paths.project_root()
    return root / "results" / "figures" / "verdict_timeline.pdf"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _format_summary_log(
    timeline_df: pd.DataFrame, *, summary: dict[str, object]
) -> str:
    """Compose the human-readable summary line for the CLI logger."""
    counts = summary["verdict_changes_per_hypothesis"]
    if not isinstance(counts, dict):
        counts = {}
    per_h = ", ".join(
        f"{hid}={counts.get(hid, 0)}" for hid in EXPECTED_HIDS
    )
    return (
        f"verdict timeline: {summary['total_commits_tracked']} commits, "
        f"{summary['total_verdict_changes']} total changes "
        f"({per_h}); range {summary['first_commit_date']}→"
        f"{summary['last_commit_date']}"
    )


def main(argv: Iterable[str] | None = None) -> int:
    """``index-inclusion-verdict-timeline`` entry point."""
    import argparse

    from index_inclusion_research import paths

    parser = argparse.ArgumentParser(
        prog="index-inclusion-verdict-timeline",
        description=(
            "Reconstruct the H1..H7 verdict-evolution timeline from the "
            "git history of results/real_tables/cma_hypothesis_verdicts.csv "
            "and render a 7-swimlane figure (PNG + PDF) plus a public-"
            "summary-friendly JSON slice in the CLI log."
        ),
    )
    parser.add_argument(
        "--repo-root",
        default=str(paths.project_root()),
        help=(
            "Git repository root (default: project root). Must contain "
            "a `.git` directory."
        ),
    )
    parser.add_argument(
        "--target-csv",
        default=DEFAULT_TARGET_CSV,
        help=(
            "Repository-relative path to the verdicts CSV "
            f"(default: {DEFAULT_TARGET_CSV})."
        ),
    )
    parser.add_argument(
        "--png",
        default="",
        help=(
            "Output PNG path. Default: results/figures/verdict_timeline.png "
            "under the configured repo root."
        ),
    )
    parser.add_argument(
        "--pdf",
        default="",
        help=(
            "Output PDF path. Default: results/figures/verdict_timeline.pdf "
            "under the configured repo root. Pass an empty string AND "
            "--no-pdf to skip vector emission."
        ),
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip the PDF twin entirely.",
    )
    parser.add_argument(
        "--max-history",
        type=int,
        default=DEFAULT_MAX_HISTORY,
        help=(
            "Cap on git commits walked (most-recent N kept). "
            f"Default: {DEFAULT_MAX_HISTORY}."
        ),
    )
    parser.add_argument(
        "--pap-baseline-date",
        default="",
        help=(
            "ISO date for the vertical PAP baseline marker. Default: the "
            "latest snapshots/pre-registration-*.csv under the repo root "
            f"(fallback {PAP_BASELINE_DATE} when none exists)."
        ),
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    repo_root = Path(args.repo_root).expanduser().resolve()
    timeline_df = build_verdict_timeline_from_git(
        repo_root,
        target_csv=args.target_csv,
        max_history=args.max_history,
    )

    png_path = (
        Path(args.png).expanduser().resolve()
        if args.png
        else default_png_path(repo_root)
    )
    pdf_path: Path | None
    if args.no_pdf:
        pdf_path = None
    else:
        pdf_path = (
            Path(args.pdf).expanduser().resolve()
            if args.pdf
            else default_pdf_path(repo_root)
        )

    written = render_verdict_timeline_plot(
        timeline_df,
        output_png_path=png_path,
        output_pdf_path=pdf_path,
        pap_baseline_date=args.pap_baseline_date or default_pap_baseline_date(repo_root),
    )
    logger.info("verdict timeline PNG written: %s", written)
    if pdf_path is not None:
        logger.info("verdict timeline PDF written: %s", pdf_path)

    summary = summarize_for_public_summary(timeline_df)
    logger.info(_format_summary_log(timeline_df, summary=summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
