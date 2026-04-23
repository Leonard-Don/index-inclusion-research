from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
    HYPOTHESES,
    StructuralHypothesis,
    export_hypothesis_map,
)


def test_hypotheses_registry_has_six_entries():
    assert len(HYPOTHESES) == 6
    assert [h.hid for h in HYPOTHESES] == ["H1", "H2", "H3", "H4", "H5", "H6"]


def test_hypothesis_shape():
    for h in HYPOTHESES:
        assert isinstance(h, StructuralHypothesis)
        assert h.name_cn and h.mechanism and h.verdict_logic
        assert h.evidence_refs, f"{h.hid} has no evidence refs"
        assert h.implications, f"{h.hid} has no implications"


def test_export_hypothesis_map_writes_csv(tmp_path):
    out = export_hypothesis_map(output_dir=tmp_path)
    assert out.exists()
    df = pd.read_csv(out)
    assert {"hid", "name_cn", "mechanism", "evidence_refs", "verdict_logic"}.issubset(
        df.columns
    )
    assert len(df) == 6
