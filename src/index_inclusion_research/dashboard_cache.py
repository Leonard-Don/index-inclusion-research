from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from threading import Lock

from index_inclusion_research.dashboard_types import CacheEntry


class AnalysisCacheStore(MutableMapping[str, CacheEntry]):
    def __init__(self, initial: Mapping[str, CacheEntry] | None = None) -> None:
        self._lock = Lock()
        self._data: dict[str, CacheEntry] = dict(initial or {})

    def __getitem__(self, key: str) -> CacheEntry:
        with self._lock:
            return self._data[key]

    def __setitem__(self, key: str, value: CacheEntry) -> None:
        with self._lock:
            self._data[key] = value

    def __delitem__(self, key: str) -> None:
        with self._lock:
            del self._data[key]

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            keys = tuple(self._data)
        return iter(keys)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AnalysisCacheStore):
            return self.snapshot() == other.snapshot()
        return self.snapshot() == other

    def __repr__(self) -> str:
        return repr(self.snapshot())

    def copy(self) -> dict[str, CacheEntry]:
        return self.snapshot()

    def snapshot(self) -> dict[str, CacheEntry]:
        with self._lock:
            return dict(self._data)

    def replace_all(self, next_data: Mapping[str, CacheEntry]) -> None:
        with self._lock:
            self._data = dict(next_data)
