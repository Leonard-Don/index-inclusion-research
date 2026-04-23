from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REAL_TABLES_DIR = ROOT / "results" / "real_tables"
REAL_FIGURES_DIR = ROOT / "results" / "real_figures"
REAL_EVENT_PANEL = ROOT / "data" / "processed" / "real_event_panel.csv"
REAL_MATCHED_EVENT_PANEL = ROOT / "data" / "processed" / "real_matched_event_panel.csv"
REAL_EVENTS_CLEAN = ROOT / "data" / "processed" / "real_events_clean.csv"
