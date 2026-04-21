from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import price_pressure_track as _price_pressure_track

main = _price_pressure_track.main
run_analysis = _price_pressure_track.run_analysis


if __name__ == "__main__":
    raise SystemExit(main())
