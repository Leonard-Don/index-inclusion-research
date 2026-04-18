from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.rdd_reconstruction import (
    ReconstructionBatch,
    build_reconstructed_candidate_frame,
    load_cn_reconstruction_batches,
    reconstruct_batch_membership,
)


def test_load_cn_reconstruction_batches_groups_additions_and_deletions() -> None:
    events = pd.DataFrame(
        [
            {"market": "CN", "announce_date": "2024-05-31", "effective_date": "2024-06-14", "batch_id": "csi300-2024-05", "ticker": "000001", "inclusion": 1},
            {"market": "CN", "announce_date": "2024-05-31", "effective_date": "2024-06-14", "batch_id": "csi300-2024-05", "ticker": "000002", "inclusion": 0},
            {"market": "CN", "announce_date": "2025-05-30", "effective_date": "2025-06-13", "batch_id": "csi300-2025-05", "ticker": "000003", "inclusion": 1},
        ]
    )

    batches = load_cn_reconstruction_batches(events)

    assert [batch.announce_date for batch in batches] == ["2024-05-31", "2025-05-30"]
    assert batches[0].additions == frozenset({"000001"})
    assert batches[0].deletions == frozenset({"000002"})


def test_reconstruct_batch_membership_rolls_back_future_changes() -> None:
    current = {"000001", "000002", "000003"}
    batches = [
        ReconstructionBatch(
            announce_date="2024-05-31",
            effective_date="2024-06-14",
            batch_id="csi300-2024-05",
            additions=frozenset({"000002"}),
            deletions=frozenset({"000004"}),
        ),
        ReconstructionBatch(
            announce_date="2025-05-30",
            effective_date="2025-06-13",
            batch_id="csi300-2025-05",
            additions=frozenset({"000003"}),
            deletions=frozenset({"000005"}),
        ),
    ]

    target, pre_review, post_review = reconstruct_batch_membership(
        current,
        batches,
        target_announce_date="2024-05-31",
        expected_size=3,
    )

    assert target.batch_id == "csi300-2024-05"
    assert post_review == {"000001", "000002", "000005"}
    assert pre_review == {"000001", "000004", "000005"}


def test_build_reconstructed_candidate_frame_ranks_and_marks_post_review_members() -> None:
    candidate_caps = pd.DataFrame(
        [
            {"ticker": "000001", "security_name": "A", "proxy_market_cap": 300.0},
            {"ticker": "000002", "security_name": "B", "proxy_market_cap": 200.0},
            {"ticker": "000003", "security_name": "C", "proxy_market_cap": 100.0},
        ]
    )
    batch = ReconstructionBatch(
        announce_date="2024-05-31",
        effective_date="2024-06-14",
        batch_id="csi300-2024-05",
        additions=frozenset({"000001"}),
        deletions=frozenset({"000003"}),
    )

    frame = build_reconstructed_candidate_frame(
        candidate_caps,
        batch=batch,
        post_review_membership={"000001", "000002"},
        cutoff=2,
    )

    assert frame["ticker"].tolist() == ["000001", "000002", "000003"]
    assert frame["descending_rank"].tolist() == [1, 2, 3]
    assert frame["running_variable"].tolist() == [4, 3, 2]
    assert frame["inclusion"].tolist() == [1, 1, 0]
    assert frame.loc[0, "event_type"] == "reconstructed_post_member"
    assert frame.loc[2, "event_type"] == "reconstructed_pre_only_member"
