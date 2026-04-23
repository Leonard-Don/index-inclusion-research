from __future__ import annotations

import argparse
from pathlib import Path

from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator


def _require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the cross-market (CN vs US) announce/effective asymmetry "
            "analysis pack."
        ),
    )
    parser.add_argument(
        "--event-panel",
        default=str(orchestrator.REAL_EVENT_PANEL),
        help="Path to real_event_panel.csv",
    )
    parser.add_argument(
        "--matched-panel",
        default=str(orchestrator.REAL_MATCHED_EVENT_PANEL),
        help="Path to real_matched_event_panel.csv",
    )
    parser.add_argument(
        "--events",
        default=str(orchestrator.REAL_EVENTS_CLEAN),
        help="Path to real_events_clean.csv",
    )
    parser.add_argument(
        "--tables-dir",
        default=str(orchestrator.REAL_TABLES_DIR),
    )
    parser.add_argument(
        "--figures-dir",
        default=str(orchestrator.REAL_FIGURES_DIR),
    )
    parser.add_argument(
        "--research-summary",
        default=str(orchestrator.REAL_TABLES_DIR / "research_summary.md"),
    )
    parser.add_argument(
        "--aum",
        default=None,
        help=(
            "Optional passive AUM overlay CSV (columns: market, year, aum_trillion). "
            "If absent, AUM overlay is skipped."
        ),
    )
    parser.add_argument(
        "--tex-only",
        action="store_true",
        help="Skip computation; only regenerate LaTeX tables from existing CMA CSVs.",
    )
    args = parser.parse_args(argv)

    if args.tex_only:
        orchestrator.regenerate_tex_only(tables_dir=Path(args.tables_dir))
        print(f"CMA LaTeX regenerated under {args.tables_dir}")
        return

    event_panel = Path(args.event_panel)
    matched = Path(args.matched_panel)
    events = Path(args.events)
    _require_file(event_panel, "event_panel")
    _require_file(matched, "matched_panel")
    _require_file(events, "events")

    aum_path = Path(args.aum) if args.aum else None
    result = orchestrator.run_cma_pipeline(
        event_panel_path=event_panel,
        matched_panel_path=matched,
        events_path=events,
        tables_dir=Path(args.tables_dir),
        figures_dir=Path(args.figures_dir),
        research_summary_path=Path(args.research_summary),
        aum_path=aum_path,
    )
    print("CMA pipeline finished.")
    print(f"  tables_dir: {result['tables_dir']}")
    print(f"  figures_dir: {result['figures_dir']}")
    print(f"  tables written: {result['tables_count']}")
    print(f"  figures written: {result['figures_count']}")


if __name__ == "__main__":
    main()
