"""CrossRef HTTP client for bibliographic enrichment.

This module wraps the public CrossRef REST API
(`https://api.crossref.org/works`) so the project can resolve
``[TODO: journal]`` placeholders in ``paper/references.bib`` to the real
journal name, volume, issue, pages, and DOI without manual lookup.

Why CrossRef and not Semantic Scholar / Google Scholar?

- CrossRef is the canonical metadata source for DOIs. ~99% of
  peer-reviewed economics papers (RFS, JFE, JFQA, JBF, MS, JF) have a
  registered CrossRef record at the moment of publication.
- The API is free, no key required, and the rate limit is generous
  (50 req/s on the "polite" pool). The project URL + a real email in the
  ``User-Agent`` header is enough to qualify.
- The match heuristic is straightforward: filter by year, then score
  results by author-surname overlap + title token Jaccard similarity.

The client is deliberately defensive:

- Every network call is wrapped in a single ``requests.get`` with a
  short timeout (10 s). Any network exception → ``None`` (caller treats
  as low-confidence, keeps the TODO marker).
- The 50 req/s rate limit is enforced client-side via a sleep between
  consecutive calls. Calling code is also expected to cache responses
  on disk (see :mod:`enrich_bib`).
- The author / title match is a confidence *score* (0.0–1.0), not a
  boolean. Calling code chooses a threshold (default 0.7).

This module has no side effects beyond the HTTP call itself: callers
that want caching wrap :func:`query_crossref`.
"""

from __future__ import annotations

import html
import logging
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CrossRefResult:
    """A single CrossRef match resolved for a BibTeX entry.

    ``confidence_score`` combines author-surname overlap (weight 0.5)
    and title-token Jaccard similarity (weight 0.5). Callers should
    treat ``< 0.5`` as "no match", ``0.5 – 0.7`` as "borderline" (keep
    TODO), and ``≥ 0.7`` as confident enough to fill in the entry.
    """

    doi: str
    journal: str
    volume: str
    issue: str
    pages: str
    year: int | None
    title_matched: str
    author_matched: str
    confidence_score: float


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


CROSSREF_BASE_URL = "https://api.crossref.org/works"

# Polite usage: include a real project URL + maintainer email so CrossRef
# can route us to the "polite pool" with its higher rate limit. Setting
# the email is the only documented switch (see CrossRef "polite pool"
# docs); the project URL is good practice for accountability.
DEFAULT_USER_AGENT = (
    "index-inclusion-research/0.1 "
    "(https://github.com/leonardodon/index-inclusion-research; "
    "mailto:leonarddon@oxxz.site)"
)

# CrossRef "polite pool" promises 50 req/s. We're far more conservative —
# 5 req/s is plenty for a 16-entry bibliography and stays well clear of
# any throttling. (Polite pool limit is per-IP, not per-key.)
DEFAULT_MIN_INTERVAL_SECONDS = 0.2

# Per-request HTTP timeout. CrossRef is fast; 10 s is generous.
DEFAULT_TIMEOUT_SECONDS = 10.0

# How many candidate rows to fetch per query. CrossRef returns rows in
# relevance order; 5 is enough to find a high-confidence match for a
# specific title + year filter.
DEFAULT_ROWS = 5


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class _RateLimiter:
    """Bare-bones client-side rate limiter.

    Maintains the wall-clock time of the last successful request and
    sleeps before the next one if it would arrive too soon. Per-instance
    state — not thread-safe, which is fine for the single-threaded
    enrichment pipeline.
    """

    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = min_interval_seconds
        self._last_request_at: float | None = None

    def wait(self) -> None:
        if self._last_request_at is None:
            self._last_request_at = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_request_at
        wait_for = self.min_interval_seconds - elapsed
        if wait_for > 0:
            time.sleep(wait_for)
        self._last_request_at = time.monotonic()


# Module-level limiter so callers share rate budget across all queries.
_LIMITER = _RateLimiter(DEFAULT_MIN_INTERVAL_SECONDS)


def _reset_rate_limiter_for_tests() -> None:
    """Test hook: reset the module-level rate limiter so unit tests are fast."""
    global _LIMITER
    _LIMITER = _RateLimiter(DEFAULT_MIN_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Author-surname / title-token utilities
# ---------------------------------------------------------------------------


# Surnames that appear in the BibTeX ``author`` field come in BibTeX
# "First Last" or "Last, First" form. We normalize to the last whitespace
# token for matching. CJK names are handled by treating the entire string
# as one surname (no internal whitespace).
def _surname_tokens(author_field: str) -> set[str]:
    """Extract a set of normalized surname tokens from a BibTeX author field.

    Splits on ``" and "`` to separate authors, then takes the last token
    of each name (or the whole token for CJK names) and lower-cases it.
    Trailing commas (e.g. ``"Last, First"``) are stripped.
    """
    out: set[str] = set()
    for raw in re.split(r"\s+and\s+", author_field):
        candidate = raw.strip().rstrip(",")
        if not candidate:
            continue
        if "," in candidate:
            # "Last, First" → surname is everything before the comma.
            surname = candidate.split(",", 1)[0]
        else:
            # "First Middle Last" → surname is last whitespace token.
            parts = candidate.split()
            surname = parts[-1] if parts else candidate
        # Normalize: lower-case, strip diacritics-friendly chars left in.
        out.add(surname.strip().lower())
    return out


# We tokenize titles by lower-casing, stripping punctuation, and keeping
# tokens of length ≥ 3. Short stopwords like "of", "the", "in" carry no
# signal for title matching.
_TITLE_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "from",
        "with",
        "into",
        "that",
        "this",
        "are",
        "its",
        "has",
        "have",
        "new",
        "evidence",
    }
)


def _title_tokens(title: str) -> set[str]:
    """Tokenize a paper title for Jaccard comparison.

    Lower-cases, strips non-alphanumeric chars, drops tokens shorter than
    3 characters and stopwords. Returns the resulting set of tokens.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", title.lower())
    tokens = {tok for tok in cleaned.split() if len(tok) >= 3}
    return tokens - _TITLE_STOPWORDS


def _jaccard(a: set[str], b: set[str]) -> float:
    """Jaccard similarity of two token sets (0.0 if both empty)."""
    if not a and not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


# ---------------------------------------------------------------------------
# Item → CrossRefResult parsing + scoring
# ---------------------------------------------------------------------------


def _item_first_string(item: dict[str, Any], key: str) -> str:
    """Return the first string value at ``item[key]``, or ``""``."""
    value = item.get(key)
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str):
            return first
    if isinstance(value, str):
        return value
    return ""


def _item_authors_str(item: dict[str, Any]) -> str:
    """Reduce CrossRef ``author`` array to a ``"Surname1, Surname2, ..."`` string."""
    authors = item.get("author") or []
    surnames: list[str] = []
    for author in authors:
        if not isinstance(author, dict):
            continue
        family = author.get("family")
        if isinstance(family, str) and family.strip():
            surnames.append(family.strip())
    return ", ".join(surnames)


def _item_year(item: dict[str, Any]) -> int | None:
    """Extract the publication year from CrossRef's ``issued`` field."""
    issued = item.get("issued") or {}
    if not isinstance(issued, dict):
        return None
    date_parts = issued.get("date-parts")
    if not isinstance(date_parts, list) or not date_parts:
        return None
    first = date_parts[0]
    if isinstance(first, list) and first:
        candidate = first[0]
        if isinstance(candidate, int):
            return candidate
        if isinstance(candidate, str) and candidate.isdigit():
            return int(candidate)
    return None


def _score_item(
    item: dict[str, Any],
    author_surnames: set[str],
    title_tokens: set[str],
    expected_year: int | None,
) -> float:
    """Compute a 0.0–1.0 confidence score for one CrossRef candidate.

    Score = ``0.5 * author_overlap + 0.5 * title_jaccard`` with a small
    year-mismatch penalty (capped to keep the score in [0,1]).

    ``author_overlap`` is the fraction of ``author_surnames`` that
    appear in the candidate's CrossRef author list. ``title_jaccard``
    is the standard set Jaccard between tokenized titles.
    """
    crossref_authors = item.get("author") or []
    crossref_surnames = {
        a.get("family", "").strip().lower()
        for a in crossref_authors
        if isinstance(a, dict) and a.get("family")
    }
    crossref_surnames.discard("")
    if author_surnames:
        overlap_n = len(author_surnames & crossref_surnames)
        author_overlap = overlap_n / max(1, len(author_surnames))
    else:
        author_overlap = 0.0

    crossref_title = _item_first_string(item, "title")
    crossref_tokens = _title_tokens(crossref_title)
    title_jaccard = _jaccard(title_tokens, crossref_tokens)

    score = 0.5 * author_overlap + 0.5 * title_jaccard

    # Year penalty: if expected_year is provided and CrossRef year differs
    # by > 1 year, dock 0.15 (papers can be issued the year before / after
    # acceptance, so 1-year drift is tolerated).
    if expected_year is not None:
        cr_year = _item_year(item)
        if cr_year is not None and abs(cr_year - expected_year) > 1:
            score = max(0.0, score - 0.15)

    return min(1.0, max(0.0, score))


def _decode_html_entities(text: str) -> str:
    """Decode HTML entities (``&amp;``, ``&#x2013;``) commonly returned by CrossRef."""
    return html.unescape(text)


def _build_result(
    item: dict[str, Any], score: float
) -> CrossRefResult:
    """Map a CrossRef item dict + score into a :class:`CrossRefResult`."""
    return CrossRefResult(
        doi=str(item.get("DOI", "")),
        journal=_decode_html_entities(_item_first_string(item, "container-title")),
        volume=str(item.get("volume", "")),
        issue=str(item.get("issue", "")),
        pages=str(item.get("page", "")),
        year=_item_year(item),
        title_matched=_decode_html_entities(_item_first_string(item, "title")),
        author_matched=_item_authors_str(item),
        confidence_score=float(round(score, 3)),
    )


# ---------------------------------------------------------------------------
# Public query
# ---------------------------------------------------------------------------


@dataclass
class CrossRefClient:
    """Stateful wrapper around the CrossRef REST API.

    The dataclass form lets callers (and tests) inject a custom ``session``
    (e.g. a ``requests_mock``-backed one) and tune the rate limit /
    timeout. The default values are the production settings.
    """

    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    rows: int = DEFAULT_ROWS
    session: Any = None  # injected for tests; None → use module requests
    base_url: str = CROSSREF_BASE_URL

    _limiter: _RateLimiter = field(
        default_factory=lambda: _LIMITER, repr=False
    )

    # Mutable in tests; default to module limiter.
    def _do_get(self, params: dict[str, str]) -> dict[str, Any] | None:
        """Perform the GET, returning the parsed JSON dict or None on error."""
        self._limiter.wait()
        try:
            getter = self.session.get if self.session is not None else requests.get
            response = getter(
                self.base_url,
                params=params,
                headers={"User-Agent": self.user_agent},
                timeout=self.timeout_seconds,
            )
        except requests.exceptions.RequestException as exc:
            logger.warning("CrossRef request failed: %s", exc)
            return None
        except OSError as exc:
            # Some flaky environments raise OSError (e.g. DNS down).
            logger.warning("CrossRef network error: %s", exc)
            return None
        if response.status_code != 200:
            logger.warning(
                "CrossRef returned HTTP %s for query %r",
                response.status_code,
                params,
            )
            return None
        try:
            payload = response.json()
        except ValueError:
            logger.warning("CrossRef returned non-JSON for query %r", params)
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def query(
        self,
        *,
        authors: Iterable[str] | str,
        year: int | None,
        title: str,
    ) -> CrossRefResult | None:
        """Query CrossRef and return the highest-scoring match (or None).

        Parameters
        ----------
        authors:
            Either a BibTeX author string (``"First Last and First Last"``)
            or an iterable of surname strings.
        year:
            4-digit publication year, used both as a CrossRef filter and
            for the confidence score's year penalty. ``None`` skips the
            filter (rare; mostly for entries with ``[TODO: year]``).
        title:
            The paper title. CrossRef matches the title via its
            ``query.title`` parameter (which uses internal scoring).
        """
        if isinstance(authors, str):
            author_surnames = _surname_tokens(authors)
        else:
            author_surnames = {a.strip().lower() for a in authors if a.strip()}
        title_tokens = _title_tokens(title)

        params: dict[str, str] = {
            "query.title": title,
            "rows": str(self.rows),
        }
        if author_surnames:
            params["query.author"] = " ".join(sorted(author_surnames))
        if year is not None:
            params["filter"] = (
                f"from-pub-date:{year - 1},until-pub-date:{year + 1}"
            )

        payload = self._do_get(params)
        if payload is None:
            return None

        message = payload.get("message")
        if not isinstance(message, dict):
            return None
        items = message.get("items")
        if not isinstance(items, list) or not items:
            return None

        best_score = -1.0
        best_item: dict[str, Any] | None = None
        for item in items:
            if not isinstance(item, dict):
                continue
            score = _score_item(
                item,
                author_surnames=author_surnames,
                title_tokens=title_tokens,
                expected_year=year,
            )
            if score > best_score:
                best_score = score
                best_item = item

        if best_item is None or best_score <= 0.0:
            return None
        return _build_result(best_item, best_score)


def query_crossref(
    authors: Iterable[str] | str,
    year: int | None,
    title: str,
) -> CrossRefResult | None:
    """Module-level convenience wrapper around :class:`CrossRefClient`.

    Constructs a default client and forwards the keyword args. Useful for
    one-off scripts; the enrichment pipeline instantiates the client
    once and reuses it.
    """
    client = CrossRefClient()
    return client.query(authors=authors, year=year, title=title)
