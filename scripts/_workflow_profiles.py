from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from index_inclusion_research.workflow_profiles import add_profile_argument, detect_profile, resolve_profile_args

__all__ = ["add_profile_argument", "detect_profile", "resolve_profile_args"]
