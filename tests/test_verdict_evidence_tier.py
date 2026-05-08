from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.verdicts._core import (
    EVIDENCE_TIER,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_evidence_tier_mapping_covers_all_seven_hypotheses() -> None:
    expected = {"H1", "H2", "H3", "H4", "H5", "H6", "H7"}
    assert set(EVIDENCE_TIER.keys()) == expected
    assert all(v in {"core", "supplementary"} for v in EVIDENCE_TIER.values())


def test_evidence_tier_marks_h1_h5_h7_as_core() -> None:
    assert EVIDENCE_TIER["H1"] == "core"
    assert EVIDENCE_TIER["H5"] == "core"
    assert EVIDENCE_TIER["H7"] == "core"


def test_evidence_tier_marks_low_n_hypotheses_as_supplementary() -> None:
    for hid in ("H2", "H3", "H4", "H6"):
        assert EVIDENCE_TIER[hid] == "supplementary"


def test_readme_hypothesis_count_claim_matches_evidence_tier() -> None:
    """README's ``N 条机制假说`` narrative must match ``len(EVIDENCE_TIER)``.

    Why: README.md (lines 32, 37) literally claims ``7 条机制假说裁决`` for
    the CMA pipeline's hypothesis count, rendered just above the H1..H7
    verdict table that GitHub readers see on the repo home page. If
    ``EVIDENCE_TIER`` (the canonical hypothesis registry in
    ``verdicts._core``) gains or loses an entry, the README narrative
    must move with it, otherwise the prose count would silently
    contradict the H1..H7 table directly beneath it.

    The existing ``test_evidence_tier_mapping_covers_all_seven_hypotheses``
    only pins ``set(EVIDENCE_TIER) == {H1..H7}`` in code; it never reads
    the README's ``7 条`` phrase, so a hypothesis added in code with the
    README left stale would still pass that test — exactly the failure
    mode the README CLI / Python / pipeline / literature badge guards
    were added to prevent for their respective counts.

    The ``(?<!\\d)…(?!\\d)`` digit-boundary lookarounds match the
    badge-count guards in ``test_dashboard_bootstrap_cli`` and
    ``test_rebuild_all`` so a stale ``17 条机制假说`` or ``70 条机制假说``
    rendering can never satisfy a naive substring check. The phrase is
    anchored to ``机制假说`` so that an unrelated narrative count (e.g.
    ``7 条假说`` on README line 187 or ``7 条 CMA 假说`` on line 280)
    cannot stand in for the H1..H7 table caption.
    """
    expected_count = len(EVIDENCE_TIER)
    pattern = re.compile(rf"(?<!\d){expected_count}(?!\d) 条机制假说")

    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert pattern.search(readme) is not None, (
        f"README.md must advertise '{expected_count} 条机制假说' "
        f"(with no adjacent digits) to match len(EVIDENCE_TIER)="
        f"{expected_count}"
    )


def test_existing_verdicts_csv_has_tier_after_pipeline_run() -> None:
    """If the live CSV exists, it should carry the tier column."""
    from index_inclusion_research.paths import results_dir

    path = results_dir() / "real_tables" / "cma_hypothesis_verdicts.csv"
    if not path.exists():
        return  # pipeline hasn't been run in this checkout
    df = pd.read_csv(path)
    if "evidence_tier" not in df.columns:
        return  # CSV pre-dates this column; will refresh on next make rebuild
    assert set(df["evidence_tier"].unique()).issubset({"core", "supplementary"})
    by_hid = dict(zip(df["hid"], df["evidence_tier"], strict=False))
    for hid, tier in EVIDENCE_TIER.items():
        if hid in by_hid:
            assert by_hid[hid] == tier
