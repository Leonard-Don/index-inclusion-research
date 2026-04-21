from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import demand_curve_track as _demand_curve_track

main = _demand_curve_track.main
run_analysis = _demand_curve_track.run_analysis


if __name__ == "__main__":
    raise SystemExit(main())
