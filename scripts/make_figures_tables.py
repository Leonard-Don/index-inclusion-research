from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research import figures_tables as _figures_tables

main = _figures_tables.main


def __getattr__(name: str):
    return getattr(_figures_tables, name)


if __name__ == "__main__":
    raise SystemExit(main())
