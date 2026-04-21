from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import hs300_rdd as _hs300_rdd

main = _hs300_rdd.main
run_analysis = _hs300_rdd.run_analysis


if __name__ == "__main__":
    raise SystemExit(main())
