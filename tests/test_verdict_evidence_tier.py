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


def test_readme_h1_h7_verdict_table_tier_column_matches_evidence_tier() -> None:
    """README's H1..H7 verdict table 写作层级 column must mirror EVIDENCE_TIER per row.

    Why: README.md (lines 39-47) renders the H1..H7 verdict table as the
    first concrete result GitHub readers see for the CMA pipeline. The
    table's ``写作层级`` column maps each hypothesis to ``正文 core``
    (paper main table) or ``附录 supplementary`` (appendix). This is the
    same per-hypothesis decision encoded in ``EVIDENCE_TIER``
    (``verdicts._core``), referenced by ``docs/limitations.md §7`` and
    ``docs/paper_outline_verdicts.md`` as the canonical writing-tier
    registry.

    The existing ``test_readme_hypothesis_count_claim_matches_evidence_tier``
    above only pins the COUNT ``7 条机制假说``, not per-row tier values,
    so a silent mismatch like H7 marked ``supplementary`` in code but
    ``正文 core`` in the README table — or vice versa — would still pass
    that test. The leading badge guards (CLI / literature / pipeline /
    Python / CI) likewise only pin counts or workflow targets, not
    per-hypothesis tier assignments. This guard pins each row's tier
    cell to the canonical EVIDENCE_TIER value so the table cannot drift
    from code.

    Anchoring to ``^\\|\\s*Hn\\s*\\|`` (with ``re.MULTILINE``) and
    requiring the row to terminate with ``|`` ensures an unrelated
    ``H1/H5/H7`` mention elsewhere — e.g. the ``core (H1/H5/H7) vs
    supplementary (H2/H3/H4/H6)`` evidence-strength bullet on README
    line 281 — cannot masquerade as a verdict-table row.
    """
    tier_label = {"core": "正文 core", "supplementary": "附录 supplementary"}
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for hid, tier in EVIDENCE_TIER.items():
        expected_label = tier_label[tier]
        opposite_label = tier_label[
            "supplementary" if tier == "core" else "core"
        ]
        row_pattern = re.compile(
            rf"^\|\s*{re.escape(hid)}\s*\|.*\|\s*$",
            re.MULTILINE,
        )
        rows = row_pattern.findall(readme)
        assert len(rows) == 1, (
            f"README.md must contain exactly one H1..H7 verdict-table row "
            f"for {hid}; found {len(rows)}: {rows!r}"
        )
        row = rows[0]
        assert expected_label in row, (
            f"README.md H1..H7 verdict-table row for {hid} must contain "
            f"'{expected_label}' (写作层级 cell) to match "
            f"EVIDENCE_TIER[{hid!r}]={tier!r}; row: {row!r}"
        )
        assert opposite_label not in row, (
            f"README.md H1..H7 verdict-table row for {hid} must NOT contain "
            f"'{opposite_label}' (would contradict "
            f"EVIDENCE_TIER[{hid!r}]={tier!r}); row: {row!r}"
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
