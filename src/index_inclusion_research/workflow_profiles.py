from __future__ import annotations

import argparse
from pathlib import Path

from index_inclusion_research import paths

ROOT = paths.project_root()


_PROFILE_PATHS: dict[str, dict[str, dict[str, str]]] = {
    "sample": {
        "build_event_sample": {
            "input": "data/raw/sample_events.csv",
            "output": "data/processed/events_clean.csv",
        },
        "build_price_panel": {
            "events": "data/processed/events_clean.csv",
            "prices": "data/raw/sample_prices.csv",
            "benchmarks": "data/raw/sample_benchmarks.csv",
            "output": "data/processed/event_panel.csv",
        },
        "match_controls": {
            "events": "data/processed/events_clean.csv",
            "prices": "data/raw/sample_prices.csv",
            "output_events": "data/processed/matched_events.csv",
            "output_diagnostics": "results/regressions/match_diagnostics.csv",
        },
        "run_event_study": {
            "panel": "data/processed/event_panel.csv",
            "output_dir": "results/event_study",
        },
        "run_regressions": {
            "panel": "data/processed/matched_event_panel.csv",
            "output_dir": "results/regressions",
        },
    },
    "real": {
        "build_event_sample": {
            "input": "data/raw/real_events.csv",
            "output": "data/processed/real_events_clean.csv",
        },
        "build_price_panel": {
            "events": "data/processed/real_events_clean.csv",
            "prices": "data/raw/real_prices.csv",
            "benchmarks": "data/raw/real_benchmarks.csv",
            "output": "data/processed/real_event_panel.csv",
        },
        "match_controls": {
            "events": "data/processed/real_events_clean.csv",
            "prices": "data/raw/real_prices.csv",
            "output_events": "data/processed/real_matched_events.csv",
            "output_diagnostics": "results/real_regressions/match_diagnostics.csv",
        },
        "run_event_study": {
            "panel": "data/processed/real_event_panel.csv",
            "output_dir": "results/real_event_study",
        },
        "run_regressions": {
            "panel": "data/processed/real_matched_event_panel.csv",
            "output_dir": "results/real_regressions",
        },
    },
}


_REAL_MARKERS: dict[str, tuple[str, ...]] = {
    "build_event_sample": ("data/raw/real_events.csv",),
    "build_price_panel": (
        "data/processed/real_events_clean.csv",
        "data/raw/real_prices.csv",
        "data/raw/real_benchmarks.csv",
    ),
    "match_controls": (
        "data/processed/real_events_clean.csv",
        "data/raw/real_prices.csv",
    ),
    "run_event_study": ("data/processed/real_event_panel.csv",),
    "run_regressions": ("data/processed/real_matched_event_panel.csv",),
}


def add_profile_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--profile",
        choices=["auto", "sample", "real"],
        default="auto",
        help="Workflow profile. Defaults to auto and prefers the real-data path when inputs are available.",
    )


def detect_profile(workflow: str, *, root: Path = ROOT) -> str:
    markers = _REAL_MARKERS.get(workflow, ())
    if not markers:
        return "sample"
    return "real" if all((root / marker).exists() for marker in markers) else "sample"


def resolve_profile_args(
    args: argparse.Namespace,
    *,
    workflow: str,
    root: Path = ROOT,
) -> argparse.Namespace:
    resolved = argparse.Namespace(**vars(args))
    requested_profile = getattr(resolved, "profile", "auto")
    profile = detect_profile(workflow, root=root) if requested_profile == "auto" else requested_profile
    defaults = _PROFILE_PATHS[profile][workflow]
    for field, relative_path in defaults.items():
        if not getattr(resolved, field):
            setattr(resolved, field, str(root / relative_path))
    resolved.profile = profile
    return resolved
