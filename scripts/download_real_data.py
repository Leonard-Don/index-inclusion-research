from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import real_data as _real_data

main = _real_data.main


def __getattr__(name: str):
    return getattr(_real_data, name)


if __name__ == "__main__":
    raise SystemExit(main())
