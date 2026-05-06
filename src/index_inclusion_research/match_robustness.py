from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from index_inclusion_research import paths
from index_inclusion_research.loaders import load_events, load_prices, save_dataframe
from index_inclusion_research.pipeline import compute_covariate_balance

ROOT = paths.project_root()
DEFAULT_MATCHED_EVENTS_CSV = ROOT / "data" / "processed" / "real_matched_events.csv"
DEFAULT_PRICES_CSV = ROOT / "data" / "raw" / "real_prices.csv"
DEFAULT_GRID_CSV = ROOT / "results" / "real_regressions" / "match_robustness_grid.csv"
DEFAULT_BALANCE_CSV = ROOT / "results" / "real_regressions" / "match_robustness_balance.csv"
DEFAULT_SUMMARY_MD = ROOT / "results" / "real_regressions" / "match_robustness_summary.md"

DEFAULT_REFERENCE_DATE_COLUMNS: tuple[str, ...] = ("announce_date", "effective_date")
DEFAULT_CONTROL_RATIOS: tuple[int, ...] = (1, 2, 3)


def _reference_label(reference_date_column: str) -> str:
    return reference_date_column.removesuffix("_date").replace("_", "-")


def _spec_id(reference_date_column: str, control_ratio: int) -> str:
    return f"{_reference_label(reference_date_column)}_1to{control_ratio}"


def _matched_group_key(frame: pd.DataFrame) -> pd.Series:
    if "matched_to_event_id" in frame.columns:
        key = frame["matched_to_event_id"].astype("string")
    elif "event_id" in frame.columns:
        key = frame["event_id"].astype("string").str.replace(
            r"-ctrl-\d+$", "", regex=True
        )
    else:
        key = pd.Series(range(len(frame)), index=frame.index, dtype="string")  # type: ignore[assignment]
    if "event_id" in frame.columns:
        event_id = frame["event_id"].astype("string")
        key = key.fillna(event_id.str.replace(r"-ctrl-\d+$", "", regex=True))
    return key.fillna(pd.Series(range(len(frame)), index=frame.index, dtype="string"))  # type: ignore[arg-type]


def _with_control_rank(matched_events: pd.DataFrame) -> pd.DataFrame:
    prepared = matched_events.copy()
    treatment = pd.to_numeric(
        prepared.get("treatment_group", pd.Series(1, index=prepared.index)),
        errors="coerce",
    ).fillna(1)
    prepared["_treatment_group_numeric"] = treatment.astype(int)
    prepared["_matched_group_key"] = _matched_group_key(prepared)
    prepared["_control_rank"] = 0
    control_mask = prepared["_treatment_group_numeric"] == 0

    extracted = pd.Series(pd.NA, index=prepared.index, dtype="Float64")
    if "event_id" in prepared.columns:
        extracted = pd.to_numeric(
            prepared["event_id"].astype(str).str.extract(r"-ctrl-(\d+)$")[0],
            errors="coerce",
        )
    prepared.loc[control_mask, "_control_rank"] = extracted.loc[control_mask]
    missing_rank = control_mask & prepared["_control_rank"].isna()
    if missing_rank.any():
        fallback = prepared.loc[control_mask].groupby("_matched_group_key").cumcount() + 1
        prepared.loc[missing_rank, "_control_rank"] = fallback.loc[missing_rank]
    prepared["_control_rank"] = prepared["_control_rank"].fillna(0).astype(int)
    return prepared


def select_control_ratio(matched_events: pd.DataFrame, control_ratio: int) -> pd.DataFrame:
    """Select treated rows plus the first ``control_ratio`` controls per event."""
    if control_ratio < 1:
        raise ValueError("control_ratio must be >= 1")
    prepared = _with_control_rank(matched_events)
    keep = (prepared["_treatment_group_numeric"] == 1) | (
        (prepared["_treatment_group_numeric"] == 0)
        & (prepared["_control_rank"] <= control_ratio)
    )
    selected = prepared.loc[keep].drop(
        columns=["_treatment_group_numeric", "_matched_group_key", "_control_rank"],
        errors="ignore",
    )
    return selected.reset_index(drop=True)


def _sample_counts(sample: pd.DataFrame) -> tuple[int, int]:
    treatment = pd.to_numeric(
        sample.get("treatment_group", pd.Series(1, index=sample.index)),
        errors="coerce",
    ).fillna(1)
    return int((treatment == 1).sum()), int((treatment == 0).sum())


def _balance_counts(balance: pd.DataFrame, fallback_sample: pd.DataFrame) -> tuple[int, int]:
    if not balance.empty and {"market", "n_treated", "n_control"}.issubset(balance.columns):
        market_counts = balance[["market", "n_treated", "n_control"]].drop_duplicates()
        treated = pd.to_numeric(market_counts["n_treated"], errors="coerce").fillna(0)
        control = pd.to_numeric(market_counts["n_control"], errors="coerce").fillna(0)
        return int(treated.sum()), int(control.sum())
    return _sample_counts(fallback_sample)


def _summarise_spec(
    *,
    spec_id: str,
    balance: pd.DataFrame,
    sample: pd.DataFrame,
    reference_date_column: str,
    control_ratio: int,
    smd_threshold: float,
    is_default: bool,
) -> dict[str, object]:
    n_treated, n_control = _balance_counts(balance, sample)
    if balance.empty or "smd" not in balance.columns:
        return {
            "spec_id": spec_id,
            "reference_date_column": reference_date_column,
            "control_ratio": int(control_ratio),
            "n_treated": n_treated,
            "n_control": n_control,
            "balance_rows": int(len(balance)),
            "max_abs_smd": float("nan"),
            "over_threshold_covariates": 0,
            "worst_market": "",
            "worst_covariate": "",
            "worst_smd": float("nan"),
            "passes_threshold": False,
            "is_default": bool(is_default),
        }

    numeric_smd = pd.to_numeric(balance["smd"], errors="coerce")
    abs_smd = numeric_smd.abs()
    finite_abs = abs_smd.dropna()
    over = balance.loc[abs_smd >= smd_threshold]
    worst_market = ""
    worst_covariate = ""
    worst_smd = float("nan")
    if not finite_abs.empty:
        worst_idx = finite_abs.idxmax()
        worst = balance.loc[worst_idx]
        worst_market = str(worst.get("market", ""))
        worst_covariate = str(worst.get("covariate", ""))
        worst_smd = float(numeric_smd.loc[worst_idx])
    return {
        "spec_id": spec_id,
        "reference_date_column": reference_date_column,
        "control_ratio": int(control_ratio),
        "n_treated": n_treated,
        "n_control": n_control,
        "balance_rows": int(len(balance)),
        "max_abs_smd": float(finite_abs.max()) if not finite_abs.empty else float("nan"),
        "over_threshold_covariates": int(len(over)),
        "worst_market": worst_market,
        "worst_covariate": worst_covariate,
        "worst_smd": worst_smd,
        "passes_threshold": bool(len(over) == 0 and not finite_abs.empty),
        "is_default": bool(is_default),
    }


def build_match_robustness_grid(
    *,
    matched_events: pd.DataFrame,
    prices: pd.DataFrame,
    control_ratios: Sequence[int] = DEFAULT_CONTROL_RATIOS,
    reference_date_columns: Sequence[str] = DEFAULT_REFERENCE_DATE_COLUMNS,
    lookback_days: int = 20,
    smd_threshold: float = 0.25,
    default_control_ratio: int = 3,
    default_reference_date_column: str = "announce_date",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build a local-only post-match balance grid.

    The grid never fetches data. It reuses the current matched event sample and
    local price history, then compares balance under first-k controls and
    announce/effective reference dates.
    """
    grid_rows: list[dict[str, object]] = []
    balance_frames: list[pd.DataFrame] = []
    for reference_date_column in reference_date_columns:
        if reference_date_column not in matched_events.columns:
            continue
        for control_ratio in control_ratios:
            ratio = int(control_ratio)
            sample = select_control_ratio(matched_events, ratio)
            sid = _spec_id(reference_date_column, ratio)
            is_default = (
                ratio == int(default_control_ratio)
                and reference_date_column == default_reference_date_column
            )
            balance = compute_covariate_balance(
                sample,
                prices,
                lookback_days=lookback_days,
                reference_date_column=reference_date_column,
                smd_threshold=smd_threshold,
            )
            if not balance.empty:
                annotated = balance.copy()
                annotated.insert(0, "spec_id", sid)
                annotated.insert(1, "reference_date_column", reference_date_column)
                annotated.insert(2, "control_ratio", ratio)
                annotated.insert(3, "is_default", is_default)
                balance_frames.append(annotated)
            grid_rows.append(
                _summarise_spec(
                    spec_id=sid,
                    balance=balance,
                    sample=sample,
                    reference_date_column=reference_date_column,
                    control_ratio=ratio,
                    smd_threshold=smd_threshold,
                    is_default=is_default,
                )
            )
    grid = pd.DataFrame(grid_rows)
    if not grid.empty:
        grid = grid.sort_values(
            ["reference_date_column", "control_ratio"], ignore_index=True
        )
    balance_long = (
        pd.concat(balance_frames, ignore_index=True, sort=False)
        if balance_frames
        else pd.DataFrame()
    )
    return grid, balance_long


def rank_specs(grid: pd.DataFrame) -> pd.DataFrame:
    if grid.empty:
        return grid.copy()
    ranked = grid.copy()
    ranked["_max_abs_sort"] = pd.to_numeric(
        ranked.get("max_abs_smd", pd.Series(index=ranked.index)),
        errors="coerce",
    ).fillna(float("inf"))
    ranked["_over_sort"] = pd.to_numeric(
        ranked.get("over_threshold_covariates", pd.Series(index=ranked.index)),
        errors="coerce",
    ).fillna(float("inf"))
    ranked["_control_sort"] = pd.to_numeric(
        ranked.get("control_ratio", pd.Series(index=ranked.index)),
        errors="coerce",
    ).fillna(float("inf"))
    return ranked.sort_values(
        ["_over_sort", "_max_abs_sort", "_control_sort", "spec_id"],
        ignore_index=True,
    ).drop(columns=["_max_abs_sort", "_over_sort", "_control_sort"], errors="ignore")


def render_summary_markdown(
    *,
    grid: pd.DataFrame,
    smd_threshold: float,
    matched_events_path: Path,
    prices_path: Path,
) -> str:
    lines = [
        "# Match Robustness Grid",
        "",
        "This artifact is local-only. It reads the checked local matched-event sample",
        "and local price history; it does not download or scrape web data.",
        "",
        f"- matched events: `{matched_events_path}`",
        f"- prices: `{prices_path}`",
        f"- threshold: `|SMD| < {smd_threshold:.2f}`",
        "",
    ]
    if grid.empty:
        lines.extend(["No valid robustness specifications were produced.", ""])
        return "\n".join(lines)

    ranked = rank_specs(grid)
    best = ranked.iloc[0]
    default_rows = grid.loc[grid["is_default"].astype(bool)]
    lines.extend(
        [
            "## Best Current Specification",
            "",
            (
                f"- `{best['spec_id']}`: "
                f"{int(best['over_threshold_covariates'])} covariate(s) over threshold; "
                f"max |SMD| = {float(best['max_abs_smd']):.3f}"
            ),
        ]
    )
    if not default_rows.empty:
        default = default_rows.iloc[0]
        lines.append(
            f"- default `{default['spec_id']}`: "
            f"{int(default['over_threshold_covariates'])} covariate(s) over threshold; "
            f"max |SMD| = {float(default['max_abs_smd']):.3f}"
        )
    lines.extend(["", "## Specification Grid", ""])
    for _, row in grid.iterrows():
        default_label = " (default)" if bool(row.get("is_default", False)) else ""
        lines.append(
            f"- `{row['spec_id']}`{default_label}: "
            f"controls=1:{int(row['control_ratio'])}, "
            f"reference={row['reference_date_column']}, "
            f"over={int(row['over_threshold_covariates'])}, "
            f"max_abs_smd={float(row['max_abs_smd']):.3f}, "
            f"worst={row['worst_market']}/{row['worst_covariate']} "
            f"({float(row['worst_smd']):+.3f})"
        )
    lines.append("")
    return "\n".join(lines)


def write_match_robustness_outputs(
    *,
    grid: pd.DataFrame,
    balance: pd.DataFrame,
    grid_path: Path = DEFAULT_GRID_CSV,
    balance_path: Path = DEFAULT_BALANCE_CSV,
    summary_path: Path = DEFAULT_SUMMARY_MD,
    smd_threshold: float = 0.25,
    matched_events_path: Path = DEFAULT_MATCHED_EVENTS_CSV,
    prices_path: Path = DEFAULT_PRICES_CSV,
) -> tuple[Path, Path, Path]:
    save_dataframe(grid, grid_path)
    save_dataframe(balance, balance_path)
    summary_path = Path(summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        render_summary_markdown(
            grid=grid,
            smd_threshold=smd_threshold,
            matched_events_path=Path(matched_events_path),
            prices_path=Path(prices_path),
        ),
        encoding="utf-8",
    )
    return Path(grid_path), Path(balance_path), summary_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a local-only post-match covariate-balance robustness grid."
    )
    parser.add_argument(
        "--matched-events",
        default=str(DEFAULT_MATCHED_EVENTS_CSV),
        help="Matched events CSV produced by index-inclusion-match-controls.",
    )
    parser.add_argument(
        "--prices",
        default=str(DEFAULT_PRICES_CSV),
        help="Local daily prices CSV.",
    )
    parser.add_argument("--output-grid", default=str(DEFAULT_GRID_CSV))
    parser.add_argument("--output-balance", default=str(DEFAULT_BALANCE_CSV))
    parser.add_argument("--output-summary", default=str(DEFAULT_SUMMARY_MD))
    parser.add_argument("--lookback-days", type=int, default=20)
    parser.add_argument("--smd-threshold", type=float, default=0.25)
    parser.add_argument(
        "--control-ratios",
        nargs="+",
        type=int,
        default=list(DEFAULT_CONTROL_RATIOS),
        help="First-k controls to keep from each matched event.",
    )
    parser.add_argument(
        "--reference-date-columns",
        nargs="+",
        default=list(DEFAULT_REFERENCE_DATE_COLUMNS),
        help="Event date columns used for balance snapshots.",
    )
    parser.add_argument("--default-control-ratio", type=int, default=3)
    parser.add_argument("--default-reference-date-column", default="announce_date")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    matched_events_path = Path(args.matched_events)
    prices_path = Path(args.prices)
    matched_events = load_events(matched_events_path)
    prices = load_prices(prices_path)
    grid, balance = build_match_robustness_grid(
        matched_events=matched_events,
        prices=prices,
        control_ratios=args.control_ratios,
        reference_date_columns=args.reference_date_columns,
        lookback_days=args.lookback_days,
        smd_threshold=args.smd_threshold,
        default_control_ratio=args.default_control_ratio,
        default_reference_date_column=args.default_reference_date_column,
    )
    grid_path, balance_path, summary_path = write_match_robustness_outputs(
        grid=grid,
        balance=balance,
        grid_path=Path(args.output_grid),
        balance_path=Path(args.output_balance),
        summary_path=Path(args.output_summary),
        smd_threshold=args.smd_threshold,
        matched_events_path=matched_events_path,
        prices_path=prices_path,
    )
    ranked = rank_specs(grid)
    best_text = "no valid specs"
    if not ranked.empty:
        best = ranked.iloc[0]
        best_text = (
            f"{best['spec_id']} "
            f"(over={int(best['over_threshold_covariates'])}, "
            f"max|SMD|={float(best['max_abs_smd']):.3f})"
        )
    print(
        "Saved match robustness grid "
        f"({len(grid)} specs, best {best_text}) to {grid_path}"
    )
    print(f"Saved balance detail to {balance_path}")
    print(f"Saved local-only summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
