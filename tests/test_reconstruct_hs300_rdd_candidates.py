from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import reconstruct_hs300_rdd_candidates as reconstruction_cli
from index_inclusion_research.analysis.rdd_reconstruction import ReconstructionBatch


def _sample_batches() -> list[ReconstructionBatch]:
    return [
        ReconstructionBatch(
            announce_date="2020-06-01",
            effective_date="2020-06-15",
            batch_id="csi300-2020-06",
            additions=frozenset({"000001"}),
            deletions=frozenset({"000002"}),
        ),
        ReconstructionBatch(
            announce_date="2024-05-31",
            effective_date="2024-06-14",
            batch_id="csi300-2024-05",
            additions=frozenset({"000003"}),
            deletions=frozenset({"000004"}),
        ),
        ReconstructionBatch(
            announce_date="2025-05-30",
            effective_date="2025-06-13",
            batch_id="csi300-2025-05",
            additions=frozenset({"000005"}),
            deletions=frozenset({"000006"}),
        ),
    ]


def test_select_reconstruction_batches_supports_all_batches_and_deduped_dates() -> None:
    batches = _sample_batches()

    selected_all = reconstruction_cli._select_reconstruction_batches(
        batches,
        announce_dates=None,
        all_batches=True,
    )
    selected_dates = reconstruction_cli._select_reconstruction_batches(
        batches,
        announce_dates=["2025-05-30,2024-05-31", "2024-05-31"],
        all_batches=False,
    )

    assert [batch.announce_date for batch in selected_all] == ["2020-06-01", "2024-05-31", "2025-05-30"]
    assert [batch.announce_date for batch in selected_dates] == ["2025-05-30", "2024-05-31"]


def test_resolve_selected_batches_keeps_reconstructable_suffix_for_all_batches() -> None:
    batches = _sample_batches()
    current = {"000001", "000002", "000003"}

    original = reconstruction_cli.reconstruct_batch_membership

    def _fake_reconstruct(current_constituents, available_batches, *, target_announce_date: str, expected_size: int = 300):
        del current_constituents, available_batches, expected_size
        if target_announce_date == "2020-06-01":
            raise ValueError("coverage gap")
        target = next(batch for batch in batches if batch.announce_date == target_announce_date)
        return target, {"000001", "000002", "000003"}, {"000001", "000002", "000003"}

    reconstruction_cli.reconstruct_batch_membership = _fake_reconstruct
    try:
        selected, skipped = reconstruction_cli._resolve_selected_batches(
            current,
            batches,
            announce_dates=None,
            all_batches=True,
        )
    finally:
        reconstruction_cli.reconstruct_batch_membership = original

    assert [batch.announce_date for batch in selected] == ["2024-05-31", "2025-05-30"]
    assert "2020-06-01" in skipped


def test_build_summary_text_reports_multi_batch_range() -> None:
    summary = reconstruction_cli._build_summary_text(
        selected_batches=_sample_batches()[1:],
        output_path=ROOT / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv",
        audit_path=ROOT / "results" / "literature" / "hs300_rdd_reconstruction" / "candidate_batch_audit.csv",
        audit_summary={"candidate_batches": 2, "treated_rows": 598, "control_rows": 24, "crossing_batches": 2},
        candidate_count=622,
        missing_after_fetch={"2024-05-31": ["600930"], "2025-05-30": ["000043", "000022"]},
        skipped_batches={"2022-05-27": "coverage gap"},
    )

    assert "重建批次数：`2`" in summary
    assert "公告日期范围：`2024-05-31` 至 `2025-05-30`" in summary
    assert "批次列表：`2024-05-31, 2025-05-30`" in summary
    assert "`2022-05-27`：coverage gap" in summary
    assert "`2024-05-31`：`600930`" in summary
    assert "`2025-05-30`：`000043, 000022`" in summary
