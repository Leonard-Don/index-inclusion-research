"""Tests for ``index_inclusion_research.enrich_bib`` + ``crossref_client``.

The tests mock CrossRef HTTP responses (we never hit the network) and
focus on the contract:

- Confident matches → fields filled in.
- Low-confidence matches → original TODO placeholder kept.
- Cache is read on second invocation (mock HTTP call count drops to 0).
- Network failure → graceful (no overwrite of TODO placeholders).
- Multiple entries enriched in a single run.
- Output BibTeX is still LaTeX-parseable (round-trip stable).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from index_inclusion_research import crossref_client, enrich_bib

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_SAMPLE_BIB = """@article{shleifer_1986,
  author    = {Andrei Shleifer},
  title     = {Do Demand Curves for Stocks Slope Down?},
  year      = {1986},
  journal   = {[TODO: journal]},
  note      = {美国 / S\\&P 500; 需求曲线, 非完全替代; camp=创世之战},
}

@article{harris_gurel_1986,
  author    = {Lawrence Harris and Eitan Gurel},
  title     = {Price and Volume Effects Associated with Changes in the S\\&P 500 List},
  year      = {1986},
  journal   = {[TODO: journal]},
  note      = {美国 / S\\&P 500; 事件研究, 价格压力; camp=创世之战},
}
"""


def _make_mock_response(json_payload: dict, status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_payload
    return response


def _shleifer_crossref_payload() -> dict:
    """Realistic CrossRef payload for Shleifer 1986."""
    return {
        "message": {
            "items": [
                {
                    "DOI": "10.1111/j.1540-6261.1986.tb04518.x",
                    "title": ["Do Demand Curves for Stocks Slope Down?"],
                    "container-title": ["The Journal of Finance"],
                    "volume": "41",
                    "issue": "3",
                    "page": "579-590",
                    "issued": {"date-parts": [[1986]]},
                    "author": [
                        {"family": "Shleifer", "given": "Andrei"},
                    ],
                }
            ]
        }
    }


def _harris_gurel_crossref_payload() -> dict:
    return {
        "message": {
            "items": [
                {
                    "DOI": "10.1111/j.1540-6261.1986.tb04550.x",
                    "title": [
                        "Price and Volume Effects Associated with Changes in the "
                        "S&P 500 List: New Evidence for the Existence of Price Pressures"
                    ],
                    "container-title": ["The Journal of Finance"],
                    "volume": "41",
                    "issue": "4",
                    "page": "815-829",
                    "issued": {"date-parts": [[1986]]},
                    "author": [
                        {"family": "Harris", "given": "Lawrence"},
                        {"family": "Gurel", "given": "Eitan"},
                    ],
                }
            ]
        }
    }


# Quasi-irrelevant CrossRef hit — wrong authors, wrong title — to force a
# low-confidence score.
def _low_confidence_payload() -> dict:
    return {
        "message": {
            "items": [
                {
                    "DOI": "10.0000/wrong",
                    "title": ["Completely Unrelated Macroeconomics Paper"],
                    "container-title": ["Some Journal"],
                    "volume": "7",
                    "issue": "2",
                    "page": "1-10",
                    "issued": {"date-parts": [[2010]]},
                    "author": [
                        {"family": "Smith", "given": "John"},
                    ],
                }
            ]
        }
    }


@pytest.fixture(autouse=True)
def _reset_rate_limiter() -> None:
    """Reset module-level rate limiter so tests don't accumulate sleep."""
    crossref_client._reset_rate_limiter_for_tests()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_query_crossref_scores_confident_match_high() -> None:
    """Real-author + real-title → confidence > 0.7."""
    session = MagicMock()
    session.get.return_value = _make_mock_response(_shleifer_crossref_payload())

    client = crossref_client.CrossRefClient(session=session)
    result = client.query(
        authors="Andrei Shleifer",
        year=1986,
        title="Do Demand Curves for Stocks Slope Down?",
    )

    assert result is not None
    assert result.confidence_score >= 0.7
    assert result.doi == "10.1111/j.1540-6261.1986.tb04518.x"
    assert result.journal == "The Journal of Finance"
    assert result.volume == "41"
    assert result.pages == "579-590"


def test_query_crossref_low_confidence_when_authors_dont_match() -> None:
    """Wrong authors + wrong title → either None or confidence < 0.5."""
    session = MagicMock()
    session.get.return_value = _make_mock_response(_low_confidence_payload())

    client = crossref_client.CrossRefClient(session=session)
    result = client.query(
        authors="Andrei Shleifer",
        year=1986,
        title="Do Demand Curves for Stocks Slope Down?",
    )

    # An obviously wrong match should either be discarded outright (None)
    # or returned with a sub-threshold score — both convey the same
    # downstream meaning: do not enrich.
    if result is not None:
        assert result.confidence_score < 0.5


def test_query_crossref_network_failure_returns_none() -> None:
    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError("offline")

    client = crossref_client.CrossRefClient(session=session)
    result = client.query(authors="x", year=2000, title="y")

    assert result is None


def test_enrich_references_bib_fills_high_confidence_entries(
    tmp_path: Path,
) -> None:
    input_bib = tmp_path / "in.bib"
    output_bib = tmp_path / "out.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    session = MagicMock()
    session.get.side_effect = [
        _make_mock_response(_shleifer_crossref_payload()),
        _make_mock_response(_harris_gurel_crossref_payload()),
    ]
    client = crossref_client.CrossRefClient(session=session)

    report = enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib,
        cache_path=cache_path,
        client=client,
        min_confidence=0.5,
    )

    assert report.enriched_count() == 2
    assert report.kept_todo_count() == 0
    output_text = output_bib.read_text(encoding="utf-8")
    assert "The Journal of Finance" in output_text
    assert "10.1111/j.1540-6261.1986.tb04518.x" in output_text
    assert "[TODO: journal]" not in output_text
    # Original title preserved.
    assert "Do Demand Curves for Stocks Slope Down?" in output_text
    # Pages normalized to BibTeX double-hyphen.
    assert "579--590" in output_text


def test_enrich_references_bib_keeps_todo_for_low_confidence(
    tmp_path: Path,
) -> None:
    input_bib = tmp_path / "in.bib"
    output_bib = tmp_path / "out.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    session = MagicMock()
    session.get.return_value = _make_mock_response(_low_confidence_payload())
    client = crossref_client.CrossRefClient(session=session)

    report = enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib,
        cache_path=cache_path,
        client=client,
        min_confidence=0.7,
    )

    assert report.enriched_count() == 0
    assert report.kept_todo_count() == 2
    output_text = output_bib.read_text(encoding="utf-8")
    assert output_text.count("[TODO: journal]") == 2


def test_enrich_references_bib_cache_roundtrip_avoids_second_http_call(
    tmp_path: Path,
) -> None:
    input_bib = tmp_path / "in.bib"
    output_bib_a = tmp_path / "out_a.bib"
    output_bib_b = tmp_path / "out_b.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    session = MagicMock()
    session.get.side_effect = [
        _make_mock_response(_shleifer_crossref_payload()),
        _make_mock_response(_harris_gurel_crossref_payload()),
    ]
    client = crossref_client.CrossRefClient(session=session)

    report_a = enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib_a,
        cache_path=cache_path,
        client=client,
        min_confidence=0.5,
    )
    assert report_a.enriched_count() == 2
    assert session.get.call_count == 2

    # Cache should be on disk now.
    assert cache_path.exists()
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert len(payload) == 2

    # Second run: no further HTTP calls.
    session.get.reset_mock()
    session.get.side_effect = AssertionError(
        "should not hit network on cache hit"
    )
    report_b = enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib_b,
        cache_path=cache_path,
        client=client,
        min_confidence=0.5,
    )
    assert report_b.enriched_count() == 2
    assert session.get.call_count == 0
    # Output of second run should match first.
    assert output_bib_a.read_text(encoding="utf-8") == output_bib_b.read_text(
        encoding="utf-8"
    )


def test_enrich_references_bib_network_failure_keeps_todos(
    tmp_path: Path,
) -> None:
    input_bib = tmp_path / "in.bib"
    output_bib = tmp_path / "out.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    session = MagicMock()
    session.get.side_effect = requests.exceptions.ConnectionError(
        "CrossRef offline"
    )
    client = crossref_client.CrossRefClient(session=session)

    report = enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib,
        cache_path=cache_path,
        client=client,
        min_confidence=0.7,
    )

    assert report.enriched_count() == 0
    assert report.kept_todo_count() == 2
    output_text = output_bib.read_text(encoding="utf-8")
    # Original title / author / year survived intact.
    assert "Andrei Shleifer" in output_text
    assert "Do Demand Curves for Stocks Slope Down?" in output_text
    # TODO markers preserved verbatim.
    assert output_text.count("[TODO: journal]") == 2


def test_enrich_references_bib_dry_run_does_not_write(tmp_path: Path) -> None:
    input_bib = tmp_path / "in.bib"
    output_bib = tmp_path / "out.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    session = MagicMock()
    session.get.side_effect = [
        _make_mock_response(_shleifer_crossref_payload()),
        _make_mock_response(_harris_gurel_crossref_payload()),
    ]
    client = crossref_client.CrossRefClient(session=session)

    report = enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib,
        cache_path=cache_path,
        client=client,
        min_confidence=0.5,
        write_output=False,
    )

    assert report.enriched_count() == 2
    assert not output_bib.exists()


def test_enriched_output_is_valid_bibtex_structure(tmp_path: Path) -> None:
    """Round-trip: enriched bib re-parses to the same entry count + keys."""
    input_bib = tmp_path / "in.bib"
    output_bib = tmp_path / "out.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    session = MagicMock()
    session.get.side_effect = [
        _make_mock_response(_shleifer_crossref_payload()),
        _make_mock_response(_harris_gurel_crossref_payload()),
    ]
    client = crossref_client.CrossRefClient(session=session)

    enrich_bib.enrich_references_bib(
        input_bib_path=input_bib,
        output_bib_path=output_bib,
        cache_path=cache_path,
        client=client,
        min_confidence=0.5,
    )

    output_text = output_bib.read_text(encoding="utf-8")
    parsed = enrich_bib._parse_bib(output_text)
    assert len(parsed) == 2
    keys = {e.citation_key for e in parsed}
    assert keys == {"shleifer_1986", "harris_gurel_1986"}
    for entry in parsed:
        assert entry.fields.get("doi", "").startswith("10.")
        assert "Journal of Finance" in entry.fields.get("journal", "")


def test_cli_main_invokes_enrichment_with_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Smoke-test the CLI entry point handles --dry-run end-to-end."""
    input_bib = tmp_path / "in.bib"
    output_bib = tmp_path / "out.bib"
    cache_path = tmp_path / "cache.json"
    input_bib.write_text(_SAMPLE_BIB, encoding="utf-8")

    # Patch the module-level requests so the dry-run doesn't hit network.
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"message": {"items": []}}

    def fake_get(*args: object, **kwargs: object) -> MagicMock:
        return fake_response

    monkeypatch.setattr(
        "index_inclusion_research.crossref_client.requests.get", fake_get
    )

    rc = enrich_bib.main(
        [
            "--input",
            str(input_bib),
            "--output",
            str(output_bib),
            "--cache-path",
            str(cache_path),
            "--min-confidence",
            "0.7",
            "--dry-run",
        ]
    )
    capsys.readouterr()
    assert rc == 0
    assert not output_bib.exists()
