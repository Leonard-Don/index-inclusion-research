from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import pandas as pd


@dataclass(slots=True)
class _CachedCsvFrame:
    mtime_ns: int
    size: int
    frame: pd.DataFrame


_CSV_CACHE_LOCK = Lock()
_CSV_CACHE: dict[tuple[str, bool], _CachedCsvFrame] = {}


def read_cached_csv(
    path: str | Path,
    *,
    low_memory: bool = False,
    optional: bool = False,
) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        if optional:
            return pd.DataFrame()
        raise FileNotFoundError(f"Missing results snapshot file: {csv_path}")

    resolved = csv_path.resolve()
    stat = resolved.stat()
    cache_key = (str(resolved), low_memory)
    with _CSV_CACHE_LOCK:
        cached = _CSV_CACHE.get(cache_key)
        if cached and cached.mtime_ns == stat.st_mtime_ns and cached.size == stat.st_size:
            return cached.frame.copy()

    frame = pd.read_csv(resolved, low_memory=low_memory)
    with _CSV_CACHE_LOCK:
        _CSV_CACHE[cache_key] = _CachedCsvFrame(
            mtime_ns=stat.st_mtime_ns,
            size=stat.st_size,
            frame=frame,
        )
    return frame.copy()


@dataclass(frozen=True, slots=True)
class ResultsSnapshot:
    root: Path

    def path(self, *relative_parts: str | Path) -> Path:
        return self.root.joinpath(*(str(part) for part in relative_parts))

    def csv(self, *relative_parts: str | Path, low_memory: bool = False) -> pd.DataFrame:
        return read_cached_csv(self.path(*relative_parts), low_memory=low_memory)

    def optional_csv(self, *relative_parts: str | Path, low_memory: bool = False) -> pd.DataFrame:
        return read_cached_csv(
            self.path(*relative_parts),
            low_memory=low_memory,
            optional=True,
        )


def require_first_row(frame: pd.DataFrame, *, context: str):
    if frame.empty:
        raise ValueError(f"Expected at least one row for {context}.")
    return frame.iloc[0]
