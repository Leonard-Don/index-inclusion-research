from __future__ import annotations

import pandas as pd
import pytest

from index_inclusion_research.analysis.cross_market_asymmetry import orchestrator


def _make_min_event_panel():
    rows = []
    for event_id in (1, 2, 3, 4):
        market = "CN" if event_id <= 2 else "US"
        for phase in ("announce", "effective"):
            for rel in range(-20, 21):
                rows.append(
                    {
                        "event_id": event_id,
                        "market": market,
                        "event_type": "addition",
                        "event_phase": phase,
                        "relative_day": rel,
                        "ar": 0.01 if rel == 0 else 0.001,
                        "ret": 0.005 if rel == 0 else 0.0005,
                        "turnover": 0.02 if rel >= 0 else 0.015,
                        "volume": 110 if rel >= 0 else 100,
                        "mkt_cap": 1e9 * event_id,
                        "treatment_group": 1 if event_id in (1, 3) else 0,
                        "sector": "Tech",
                        "event_date": "2020-06-01",
                    }
                )
    return pd.DataFrame(rows)


def _make_min_events():
    return pd.DataFrame(
        [
            {
                "event_id": i,
                "market": "CN" if i <= 2 else "US",
                "ticker": f"T{i}",
                "event_type": "addition",
                "announce_date": "2020-05-15",
                "effective_date": "2020-06-01",
            }
            for i in (1, 2, 3, 4)
        ]
    )


def test_orchestrator_runs_on_toy_data(tmp_path):
    event_panel = _make_min_event_panel()
    matched_panel = event_panel.copy()
    events = _make_min_events()
    event_panel_path = tmp_path / "event_panel.csv"
    matched_path = tmp_path / "matched.csv"
    events_path = tmp_path / "events.csv"
    event_panel.to_csv(event_panel_path, index=False)
    matched_panel.to_csv(matched_path, index=False)
    events.to_csv(events_path, index=False)

    result = orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tmp_path / "tables",
        figures_dir=tmp_path / "figures",
        research_summary_path=tmp_path / "summary.md",
    )

    expected_tables = [
        "cma_ar_path.csv",
        "cma_car_path.csv",
        "cma_window_summary.csv",
        "cma_gap_event_level.csv",
        "cma_gap_summary.csv",
        "cma_pre_runup_bootstrap.csv",
        "cma_gap_drift_market_regression.csv",
        "cma_h3_channel_concentration.csv",
        "cma_h5_limit_predictive_regression.csv",
        "cma_mechanism_panel.csv",
        "cma_mechanism_panel.tex",
        "cma_heterogeneity_size.csv",
        "cma_heterogeneity_liquidity.csv",
        "cma_heterogeneity_sector.csv",
        "cma_heterogeneity_gap_bucket.csv",
        "cma_time_series_rolling.csv",
        "cma_time_series_break.csv",
        "cma_hypothesis_map.csv",
        "cma_hypothesis_verdicts.csv",
        "cma_hypothesis_verdicts.tex",
        "cma_track_verdict_summary.csv",
    ]
    for name in expected_tables:
        assert (tmp_path / "tables" / name).exists(), f"missing: {name}"

    expected_figures = [
        "cma_ar_path_comparison.png",
        "cma_car_path_comparison.png",
        "cma_gap_length_distribution.png",
        "cma_gap_decomposition.png",
        "cma_mechanism_heatmap.png",
        "cma_heterogeneity_matrix_size.png",
        "cma_time_series_rolling.png",
    ]
    for name in expected_figures:
        assert (tmp_path / "figures" / name).exists(), f"missing figure: {name}"

    assert (tmp_path / "tables" / "paper_outline_verdicts.md").exists()
    assert result["paper_verdict_path"] == tmp_path / "tables" / "paper_outline_verdicts.md"
    summary = (tmp_path / "summary.md").read_text()
    assert "六、美股 vs A股 不对称" in summary
    assert "假说裁决摘要" in summary
    assert "announce" in summary or "effective" in summary
    assert result["tables_count"] == len(expected_tables)


def test_regenerate_tex_only_uses_existing_csv(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()
    pd.DataFrame(
        [
            {
                "market": "CN",
                "event_phase": "announce",
                "outcome": "car_1_1",
                "spec": "no_fe",
                "coef": 0.01,
                "se": 0.002,
                "t": 5.0,
                "p_value": 0.0,
                "n_obs": 100,
                "r_squared": 0.1,
            }
        ]
    ).to_csv(tables / "cma_mechanism_panel.csv", index=False)
    orchestrator.regenerate_tex_only(tables_dir=tables)
    assert (tables / "cma_mechanism_panel.tex").exists()


def test_regenerate_tex_only_errors_when_csv_missing(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()
    with pytest.raises(FileNotFoundError):
        orchestrator.regenerate_tex_only(tables_dir=tables)


def test_research_summary_append_is_idempotent(tmp_path):
    event_panel = _make_min_event_panel()
    events = _make_min_events()
    event_panel_path = tmp_path / "ep.csv"
    matched_path = tmp_path / "mp.csv"
    events_path = tmp_path / "ev.csv"
    event_panel.to_csv(event_panel_path, index=False)
    event_panel.to_csv(matched_path, index=False)
    events.to_csv(events_path, index=False)
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("# 现有内容\n\n前面的章节。\n")

    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tmp_path / "tables",
        figures_dir=tmp_path / "figures",
        research_summary_path=summary_path,
    )
    content1 = summary_path.read_text()
    assert "前面的章节" in content1
    assert content1.count("六、美股 vs A股 不对称") == 1

    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tmp_path / "tables",
        figures_dir=tmp_path / "figures",
        research_summary_path=summary_path,
    )
    content2 = summary_path.read_text()
    assert content2.count("六、美股 vs A股 不对称") == 1
    # verdict block is rendered as a markdown table, one row per H1..H6
    assert "| 假说 | 名称 | 裁决 | 可信度 | 头条指标 | 值 | n | 关键证据 |" in content2
    assert "|---|---|---|---|---|---|---|---|" in content2
    for hid in ("H1", "H2", "H3", "H4", "H5", "H6"):
        assert f"| {hid} |" in content2


def test_run_cma_pipeline_writes_previous_snapshot_on_second_run(tmp_path):
    event_panel = _make_min_event_panel()
    events = _make_min_events()
    event_panel_path = tmp_path / "ep.csv"
    matched_path = tmp_path / "mp.csv"
    events_path = tmp_path / "ev.csv"
    event_panel.to_csv(event_panel_path, index=False)
    event_panel.to_csv(matched_path, index=False)
    events.to_csv(events_path, index=False)
    tables_dir = tmp_path / "tables"

    # first run: no .previous.csv expected
    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tables_dir,
        figures_dir=tmp_path / "figures",
    )
    current = tables_dir / "cma_hypothesis_verdicts.csv"
    previous = tables_dir / "cma_hypothesis_verdicts.previous.csv"
    assert current.exists()
    assert not previous.exists()

    # second run: .previous.csv should now mirror the first run's CSV
    first_content = current.read_bytes()
    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tables_dir,
        figures_dir=tmp_path / "figures",
    )
    assert previous.exists()
    assert previous.read_bytes() == first_content


def test_run_cma_pipeline_autoloads_default_passive_aum(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    event_panel = _make_min_event_panel()
    events = _make_min_events()
    event_panel_path = tmp_path / "ep.csv"
    matched_path = tmp_path / "mp.csv"
    events_path = tmp_path / "ev.csv"
    event_panel.to_csv(event_panel_path, index=False)
    event_panel.to_csv(matched_path, index=False)
    events.to_csv(events_path, index=False)
    aum_path = tmp_path / "passive_aum.csv"
    pd.DataFrame(
        {
            "market": ["US", "US"],
            "year": [2014, 2020],
            "aum_trillion": [2.0, 7.0],
        }
    ).to_csv(aum_path, index=False)
    monkeypatch.setattr(orchestrator, "DEFAULT_PASSIVE_AUM_PATH", aum_path)
    monkeypatch.setattr(
        orchestrator.time_series,
        "build_rolling_car",
        lambda _panel: pd.DataFrame(
            [
                {
                    "market": "US",
                    "event_phase": "effective",
                    "window_start_year": 2010,
                    "window_end_year": 2014,
                    "car_mean": 0.004,
                    "car_se": 0.001,
                    "car_t": 4.0,
                    "n_events": 2,
                },
                {
                    "market": "US",
                    "event_phase": "effective",
                    "window_start_year": 2016,
                    "window_end_year": 2020,
                    "car_mean": -0.002,
                    "car_se": 0.001,
                    "car_t": -2.0,
                    "n_events": 2,
                },
            ]
        ),
    )

    orchestrator.run_cma_pipeline(
        event_panel_path=event_panel_path,
        matched_panel_path=matched_path,
        events_path=events_path,
        tables_dir=tmp_path / "tables",
        figures_dir=tmp_path / "figures",
    )

    verdicts = pd.read_csv(tmp_path / "tables" / "cma_hypothesis_verdicts.csv")
    h2 = verdicts.set_index("hid").loc["H2"]
    assert h2["verdict"] != "待补数据"
    assert "US AUM" in h2["metric_snapshot"]
