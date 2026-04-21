from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import harris_gurel as _harris_gurel

main = _harris_gurel.main
run_analysis = _harris_gurel.run_analysis


if __name__ == "__main__":
    raise SystemExit(main())
