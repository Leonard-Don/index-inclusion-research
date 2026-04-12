from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import start_hs300_rdd as hs300_rdd


def _valid_candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "batch_id": "2024-11-29",
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "000686",
                "security_name": "东北证券",
                "announce_date": "2024-11-29",
                "effective_date": "2024-12-16",
                "running_variable": 300.22,
                "cutoff": 300,
                "inclusion": 1,
                "event_type": "addition",
            },
            {
                "batch_id": "2024-11-29",
                "market": "CN",
                "index_name": "CSI300",
                "ticker": "000001",
                "security_name": "平安银行",
                "announce_date": "2024-11-29",
                "effective_date": "2024-12-16",
                "running_variable": 299.91,
                "cutoff": 300,
                "inclusion": 0,
                "event_type": "borderline",
            },
        ]
    )


def test_validate_candidate_frame_requires_required_columns() -> None:
    frame = _valid_candidate_frame().drop(columns=["batch_id"])
    with pytest.raises(ValueError, match="missing required columns: batch_id"):
        hs300_rdd._validate_candidate_frame(frame)


def test_validate_candidate_frame_rejects_invalid_dates() -> None:
    frame = _valid_candidate_frame()
    frame.loc[0, "announce_date"] = "not-a-date"
    with pytest.raises(ValueError, match="announce_date"):
        hs300_rdd._validate_candidate_frame(frame)


def test_validate_candidate_frame_rejects_non_binary_inclusion() -> None:
    frame = _valid_candidate_frame()
    frame.loc[0, "inclusion"] = 2
    with pytest.raises(ValueError, match="0/1"):
        hs300_rdd._validate_candidate_frame(frame)


def test_load_candidate_file_returns_missing_when_real_input_absent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hs300_rdd, "REAL_INPUT", tmp_path / "hs300_rdd_candidates.csv")
    monkeypatch.setattr(hs300_rdd, "DEMO_INPUT", tmp_path / "hs300_rdd_demo.csv")

    frame, mode, message = hs300_rdd._load_candidate_file(allow_demo=False, strict_validation=False)

    assert frame.empty
    assert mode == "missing"
    assert "等待真实候选样本文件" in message


def test_load_candidate_file_uses_demo_only_when_explicit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    demo_path = tmp_path / "hs300_rdd_demo.csv"
    demo_frame = _valid_candidate_frame().drop(columns=["security_name"])
    demo_frame.to_csv(demo_path, index=False)

    monkeypatch.setattr(hs300_rdd, "REAL_INPUT", tmp_path / "hs300_rdd_candidates.csv")
    monkeypatch.setattr(hs300_rdd, "DEMO_INPUT", demo_path)

    frame, mode, message = hs300_rdd._load_candidate_file(allow_demo=True, strict_validation=False)

    assert not frame.empty
    assert "security_name" in frame.columns
    assert mode == "demo"
    assert "--demo" in message


def test_load_candidate_file_raises_on_invalid_real_input_when_strict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_path = tmp_path / "hs300_rdd_candidates.csv"
    invalid = _valid_candidate_frame().drop(columns=["running_variable"])
    invalid.to_csv(real_path, index=False)

    monkeypatch.setattr(hs300_rdd, "REAL_INPUT", real_path)
    monkeypatch.setattr(hs300_rdd, "DEMO_INPUT", tmp_path / "hs300_rdd_demo.csv")

    with pytest.raises(ValueError, match="running_variable"):
        hs300_rdd._load_candidate_file(allow_demo=False, strict_validation=True)
