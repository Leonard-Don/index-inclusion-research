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


def test_each_hypothesis_has_at_least_one_supporting_paper():
    """Every H1..H7 should reference at least one literature paper."""
    for h in HYPOTHESES:
        assert h.paper_ids, f"{h.hid} has no supporting papers in literature catalog"


def test_hypothesis_paper_ids_resolve_to_real_catalog_entries():
    """Every paper_id should match a paper in build_literature_catalog_frame."""
    from index_inclusion_research.literature_catalog import (
        build_literature_catalog_frame,
    )
    catalog_ids = set(build_literature_catalog_frame()["paper_id"].tolist())
    for h in HYPOTHESES:
        for pid in h.paper_ids:
            assert pid in catalog_ids, (
                f"{h.hid} references unknown paper_id {pid!r}; "
                f"valid IDs: {sorted(catalog_ids)}"
            )


def test_export_hypothesis_map_writes_csv(tmp_path):
    out = export_hypothesis_map(output_dir=tmp_path)
    assert out.exists()
    df = pd.read_csv(out)
    assert {
        "hid", "name_cn", "mechanism", "evidence_refs", "verdict_logic",
        "track", "paper_ids", "paper_count",
    }.issubset(df.columns)
    assert len(df) == 7
    # all 7 should have non-empty track assignments
    assert df["track"].notna().all()
    assert (df["track"].astype(str).str.len() > 0).all()
    # all 7 should have paper_count >= 1
    assert (df["paper_count"] >= 1).all()


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


def test_compute_paper_verdict_citations_returns_static_when_verdicts_absent():
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        compute_paper_verdict_citations,
    )

    cits = compute_paper_verdict_citations("harris_gurel_1986")
    # H1 + H3 cite this paper
    hids = sorted(c["hid"] for c in cits)
    assert hids == ["H1", "H3"]
    # Static fields populated
    for c in cits:
        assert c["name_cn"]
        assert c["track"]
        assert c["track_label"]
    # Without a verdicts frame, live fields are blank
    assert all(c["verdict"] == "" for c in cits)


def test_compute_paper_verdict_citations_returns_empty_for_unknown_paper():
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        compute_paper_verdict_citations,
    )

    assert compute_paper_verdict_citations("not-a-paper") == []


def test_compute_paper_verdict_citations_merges_live_verdict_when_provided():
    from index_inclusion_research.analysis.cross_market_asymmetry.hypotheses import (
        compute_paper_verdict_citations,
    )

    verdicts = pd.DataFrame(
        [
            {"hid": "H1", "verdict": "证据不足", "confidence": "中",
             "key_label": "bootstrap p", "key_value": 0.640, "n_obs": 436},
            {"hid": "H3", "verdict": "部分支持", "confidence": "中",
             "key_label": "双通道命中率", "key_value": 0.500, "n_obs": 4},
        ]
    )
    cits = compute_paper_verdict_citations("harris_gurel_1986", verdicts=verdicts)
    by_hid = {c["hid"]: c for c in cits}
    assert by_hid["H1"]["verdict"] == "证据不足"
    assert abs(by_hid["H1"]["key_value"] - 0.640) < 1e-9
    assert by_hid["H3"]["verdict"] == "部分支持"
    assert by_hid["H3"]["n_obs"] == 4


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
