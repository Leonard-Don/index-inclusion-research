from __future__ import annotations

import pandas as pd

from index_inclusion_research.analysis.cross_market_asymmetry.h6_robustness import (
    build_h6_weight_explanation,
    build_h6_weight_joined_frame,
    compute_h6_weight_robustness,
    export_h6_weight_explanation,
    export_h6_weight_robustness,
)


def test_h6_weight_join_uses_announce_date_to_avoid_cross_batch_duplicates() -> None:
    weights = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "1",
                "announce_date": "2020-01-01",
                "weight_proxy": 0.01,
            },
            {
                "market": "CN",
                "ticker": "1",
                "announce_date": "2021-01-01",
                "weight_proxy": 0.02,
            },
        ]
    )
    gap = pd.DataFrame(
        [
            {
                "market": "CN",
                "ticker": "000001",
                "announce_date": "2020-01-01",
                "announce_jump": 0.01,
            },
            {
                "market": "CN",
                "ticker": "000001",
                "announce_date": "2021-01-01",
                "announce_jump": 0.02,
            },
        ]
    )

    joined = build_h6_weight_joined_frame(weights, gap)

    assert len(joined) == 2
    assert joined["weight_proxy"].tolist() == [0.01, 0.02]


def test_compute_h6_weight_robustness_emits_core_specs() -> None:
    rows = []
    gap_rows = []
    for i in range(1, 17):
        ticker = f"{i:06d}"
        sector = "Tech" if i <= 8 else "Finance"
        rows.append(
            {
                "market": "CN",
                "ticker": ticker,
                "announce_date": "2020-01-01",
                "weight_proxy": i / 1000,
            }
        )
        gap_rows.append(
            {
                "market": "CN",
                "ticker": ticker,
                "announce_date": "2020-01-01",
                "announce_jump": i / 1000,
                "sector": sector,
            }
        )
    table = compute_h6_weight_robustness(pd.DataFrame(rows), pd.DataFrame(gap_rows))
    by_test = table.set_index("test")

    assert {
        "coverage",
        "quartile_spread",
        "ols_weight",
        "sector_fe_weight",
        "permutation_quartile_spread",
    }.issubset(by_test.index)
    assert by_test.loc["quartile_spread", "coefficient"] > 0
    assert by_test.loc["ols_weight", "status"] == "pass"
    assert by_test.loc["sector_fe_weight", "status"] == "pass"
    # Permutation should agree with the parametric Welch test on this
    # synthetic monotone fixture: large positive spread + small p.
    assert by_test.loc["permutation_quartile_spread", "status"] == "pass"
    assert by_test.loc["permutation_quartile_spread", "coefficient"] > 0
    assert by_test.loc["permutation_quartile_spread", "p_value"] < 0.05


def test_compute_h6_weight_robustness_handles_missing_inputs() -> None:
    table = compute_h6_weight_robustness(None, None)

    assert table.iloc[0]["test"] == "coverage"
    assert table.iloc[0]["status"] == "missing"
    assert table.iloc[0]["n_obs"] == 0


def test_export_h6_weight_robustness_writes_csv(tmp_path) -> None:
    table = compute_h6_weight_robustness(None, None)

    out = export_h6_weight_robustness(table, output_dir=tmp_path)

    assert out.name == "cma_h6_weight_robustness.csv"
    assert pd.read_csv(out).iloc[0]["test"] == "coverage"


def test_build_h6_weight_explanation_summarizes_direction_and_verdict() -> None:
    robustness = pd.DataFrame(
        [
            {
                "test": "coverage",
                "status": "pass",
                "coefficient": float("nan"),
                "p_value": float("nan"),
                "n_obs": 67,
                "detail": "matched events=67",
            },
            {
                "test": "quartile_spread",
                "status": "pass",
                "coefficient": -0.01,
                "p_value": 0.4,
                "n_obs": 67,
                "detail": "Q4 mean=-1.000%; Q1 mean=+0.000%",
            },
            {
                "test": "ols_weight",
                "status": "pass",
                "coefficient": -0.02,
                "p_value": 0.2,
                "n_obs": 67,
                "detail": "HC3 OLS",
            },
        ]
    )

    table = build_h6_weight_explanation(
        h6_verdict={
            "verdict": "证据不足",
            "confidence": "medium",
            "evidence_summary": "权重 proxy 未给出显著正向信号。",
        },
        robustness=robustness,
    )
    by_topic = table.set_index("topic")

    assert by_topic.loc["sample_coverage", "value"] == 67
    assert by_topic.loc["direction", "status"] == "warn"
    assert "未显示更强公告日跳涨" in by_topic.loc["direction", "headline"]
    assert by_topic.loc["final_read", "headline"] == "当前 H6 裁决: 证据不足"


def test_export_h6_weight_explanation_writes_csv(tmp_path) -> None:
    table = build_h6_weight_explanation(robustness=compute_h6_weight_robustness(None, None))

    out = export_h6_weight_explanation(table, output_dir=tmp_path)

    assert out.name == "cma_h6_weight_explanation.csv"
    assert pd.read_csv(out).iloc[0]["topic"] == "sample_coverage"


def test_permutation_quartile_spread_returns_high_p_when_no_signal() -> None:
    """Random weight_proxy with constant announce_jump → permutation p ≈ 1."""
    import numpy as np
    rng = np.random.default_rng(0)
    n = 60
    rows = []
    gap_rows = []
    for i in range(n):
        rows.append(
            {
                "market": "CN",
                "ticker": f"{i:06d}",
                "announce_date": "2020-01-01",
                "weight_proxy": float(rng.uniform()),
            }
        )
        gap_rows.append(
            {
                "market": "CN",
                "ticker": f"{i:06d}",
                "announce_date": "2020-01-01",
                "announce_jump": 0.01,  # constant outcome → no real signal
                "sector": "Tech",
            }
        )
    table = compute_h6_weight_robustness(pd.DataFrame(rows), pd.DataFrame(gap_rows))
    perm = table.loc[table["test"] == "permutation_quartile_spread"].iloc[0]
    # With no signal, observed spread is 0 and every permuted spread is also 0,
    # so p = (B+1)/(B+1) = 1.0
    assert float(perm["coefficient"]) == 0.0
    assert float(perm["p_value"]) >= 0.99


def test_permutation_quartile_spread_recovers_real_negative_signal() -> None:
    """Heavy-weight stocks have systematically lower announce_jump →
    permutation should detect a significant negative spread (the real
    HS300 H6 finding pattern)."""
    n_per_q = 15
    rows = []
    gap_rows = []
    for q, jump in enumerate([0.04, 0.03, 0.02, 0.005]):
        for j in range(n_per_q):
            ticker = f"{q:02d}{j:04d}"
            rows.append(
                {
                    "market": "CN",
                    "ticker": ticker,
                    "announce_date": "2020-01-01",
                    "weight_proxy": float(q + 1) + j * 1e-3,
                }
            )
            gap_rows.append(
                {
                    "market": "CN",
                    "ticker": ticker,
                    "announce_date": "2020-01-01",
                    "announce_jump": jump,
                    "sector": "Tech",
                }
            )
    table = compute_h6_weight_robustness(pd.DataFrame(rows), pd.DataFrame(gap_rows))
    perm = table.loc[table["test"] == "permutation_quartile_spread"].iloc[0]
    # Q4 (highest weight) has jump=0.005, Q1 (lowest) has 0.04 → spread strongly negative.
    assert float(perm["coefficient"]) < -0.02
    assert float(perm["p_value"]) < 0.05
