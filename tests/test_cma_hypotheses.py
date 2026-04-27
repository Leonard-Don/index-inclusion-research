from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
    HYPOTHESES,
    StructuralHypothesis,
    export_hypothesis_map,
)


def test_hypotheses_registry_has_seven_entries():
    assert len(HYPOTHESES) == 7
    assert [h.hid for h in HYPOTHESES] == ["H1", "H2", "H3", "H4", "H5", "H6", "H7"]


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
    assert {"hid", "name_cn", "mechanism", "evidence_refs", "verdict_logic", "track"}.issubset(
        df.columns
    )
    assert len(df) == 7
    # all 7 should have non-empty track assignments
    assert df["track"].notna().all()
    assert (df["track"].astype(str).str.len() > 0).all()


def test_compute_track_verdict_summary_groups_by_track():
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        compute_track_verdict_summary,
    )

    verdicts = pd.DataFrame(
        [
            {"hid": "H1", "verdict": "证据不足"},
            {"hid": "H2", "verdict": "待补数据"},
            {"hid": "H3", "verdict": "支持"},
            {"hid": "H4", "verdict": "证据不足"},
            {"hid": "H5", "verdict": "证据不足"},
            {"hid": "H6", "verdict": "部分支持"},
            {"hid": "H7", "verdict": "部分支持"},
        ]
    )
    summary = compute_track_verdict_summary(verdicts)
    assert {"track", "track_label", "hypotheses", "支持", "部分支持",
            "证据不足", "待补数据", "total"}.issubset(summary.columns)
    by_track = summary.set_index("track")
    # price_pressure: H3 (支持)
    assert by_track.loc["price_pressure", "支持"] == 1
    assert by_track.loc["price_pressure", "total"] == 1
    # demand_curve: H2 (待补) + H6 (部分支持)
    assert by_track.loc["demand_curve", "待补数据"] == 1
    assert by_track.loc["demand_curve", "部分支持"] == 1
    # identification: H1 + H4 + H5 (3 证据不足) + H7 (部分支持)
    assert by_track.loc["identification", "证据不足"] == 3
    assert by_track.loc["identification", "部分支持"] == 1


def test_export_track_verdict_summary_writes_csv(tmp_path):
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        export_track_verdict_summary,
    )

    verdicts = pd.DataFrame(
        [
            {"hid": "H1", "verdict": "证据不足"},
            {"hid": "H2", "verdict": "待补数据"},
            {"hid": "H3", "verdict": "支持"},
            {"hid": "H4", "verdict": "证据不足"},
            {"hid": "H5", "verdict": "证据不足"},
            {"hid": "H6", "verdict": "部分支持"},
            {"hid": "H7", "verdict": "部分支持"},
        ]
    )
    out = export_track_verdict_summary(verdicts, output_dir=tmp_path)
    assert out.name == "cma_track_verdict_summary.csv"
    df = pd.read_csv(out)
    assert len(df) == 3
