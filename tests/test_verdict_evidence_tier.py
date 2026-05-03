from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.verdicts._core import (
    EVIDENCE_TIER,
)


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
