"""Unit tests for Phase F1 ``export_public_summary``.

Covers:

- All required top-level keys land in the output when every input exists.
- Missing input CSV is gracefully omitted (key absent, no synthetic data).
- ``schema_version`` is surfaced at the top level and matches the constant.
- Determinism: same input + fixed ``generated_at`` → byte-identical output.
- Atomic write leaves no half-written file behind on failure.
- Figure path verification — missing files drop out, present files survive.
- Per-section sanity (verdicts shape, PAP deviation counts, RDD headline).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from index_inclusion_research import export_public_summary as export_module

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_VERDICTS_COLUMNS = [
    "hid",
    "name_cn",
    "verdict",
    "confidence",
    "evidence_summary",
    "metric_snapshot",
    "next_step",
    "evidence_refs",
    "p_value",
    "key_label",
    "key_value",
    "n_obs",
    "paper_ids",
    "paper_count",
    "track",
    "evidence_tier",
]


def _make_verdicts_df() -> pd.DataFrame:
    """A minimal but realistic 7-row verdicts table (mirrors the live CSV)."""
    rows = [
        ("H1", "信息泄露与预运行", "证据不足", "中", "core", "identification",
         "bootstrap p", 0.8748, 436),
        ("H2", "被动基金 AUM 差异", "部分支持", "中", "core", "demand_curve",
         "US AUM ratio", 13.48, 17),
        ("H3", "散户 vs 机构结构", "支持", "高", "supplementary",
         "price_pressure", "双通道命中率", 0.75, 4),
        ("H4", "卖空约束", "证据不足", "中", "supplementary",
         "identification", "regression p", 0.5366, 436),
        ("H5", "涨跌停限制", "支持", "高", "core", "identification",
         "limit_coef p", 0.0082, 936),
        ("H6", "指数权重可预测性", "证据不足", "中", "supplementary",
         "demand_curve", "heavy−light spread", -0.019, 67),
        ("H7", "行业结构差异", "支持", "中", "core", "identification",
         "US sector spread", 5.95, 187),
    ]
    data = []
    for hid, name, verdict, confidence, tier, track, kl, kv, n in rows:
        data.append({
            "hid": hid,
            "name_cn": name,
            "verdict": verdict,
            "confidence": confidence,
            "evidence_summary": "long summary text omitted from public output",
            "metric_snapshot": "multi-line snapshot text omitted",
            "next_step": "next-step text omitted",
            "evidence_refs": "M1:cma_ar_path.csv",
            "p_value": kv if "p" in kl else None,
            "key_label": kl,
            "key_value": kv,
            "n_obs": n,
            "paper_ids": "paper_a | paper_b",
            "paper_count": 2,
            "track": track,
            "evidence_tier": tier,
        })
    return pd.DataFrame(data, columns=_VERDICTS_COLUMNS)


def _make_pap_df(classification: str = "unchanged") -> pd.DataFrame:
    """A minimal 7-row PAP deviation report with all rows of one classification."""
    return pd.DataFrame(
        [
            {
                "hid": hid,
                "name_cn": "name",
                "classification": classification,
                "baseline_verdict": "v",
                "current_verdict": "v",
            }
            for hid in ("H1", "H2", "H3", "H4", "H5", "H6", "H7")
        ]
    )


def _make_rdd_df() -> pd.DataFrame:
    """A minimal 5-row rdd_robustness CSV (mirrors live file)."""
    return pd.DataFrame(
        [
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 120,
                "tau": 0.039,
                "p_value": 0.048,
                "spec": "main",
                "spec_kind": "main",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 102,
                "tau": 0.049,
                "p_value": 0.10,
                "spec": "donut(±0.01)",
                "spec_kind": "donut",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 72,
                "tau": -0.024,
                "p_value": 0.26,
                "spec": "placebo -0.05",
                "spec_kind": "placebo",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 130,
                "tau": -0.020,
                "p_value": 0.18,
                "spec": "placebo +0.05",
                "spec_kind": "placebo",
            },
            {
                "outcome": "car_m1_p1",
                "bandwidth": 0.06,
                "n_obs": 120,
                "tau": 0.004,
                "p_value": 0.92,
                "spec": "polynomial=2",
                "spec_kind": "polynomial",
            },
        ]
    )


@pytest.fixture
def fixture_paths(tmp_path: Path) -> dict[str, Path]:
    """Populated tmp project tree with all the CSVs the export script reads."""
    real_tables = tmp_path / "results" / "real_tables"
    real_tables.mkdir(parents=True)
    _make_verdicts_df().to_csv(real_tables / "cma_hypothesis_verdicts.csv", index=False)
    _make_pap_df().to_csv(real_tables / "pap_deviation_report.csv", index=False)

    rdd_dir = tmp_path / "results" / "literature" / "hs300_rdd"
    rdd_dir.mkdir(parents=True)
    _make_rdd_df().to_csv(rdd_dir / "rdd_robustness.csv", index=False)

    snapshots_dir = tmp_path / "snapshots"
    snapshots_dir.mkdir()
    snap = snapshots_dir / "pre-registration-2026-05-16.csv"
    _make_verdicts_df().to_csv(snap, index=False)

    figures_dir = tmp_path / "results" / "figures"
    figures_dir.mkdir(parents=True)
    for relpath in export_module.PUBLISHED_FIGURE_RELPATHS:
        figpath = tmp_path / relpath
        figpath.parent.mkdir(parents=True, exist_ok=True)
        figpath.write_bytes(b"fake-png")

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "test"\nversion = "9.9.9"\n\n[project.scripts]\n'
        'index-inclusion-foo = "x:y"\n'
        'index-inclusion-bar = "x:z"\n',
        encoding="utf-8",
    )

    # Empty sensitivity dir so the section is omitted by default. Tests
    # that need it can populate further.
    return {
        "root": tmp_path,
        "verdicts": real_tables / "cma_hypothesis_verdicts.csv",
        "pap": real_tables / "pap_deviation_report.csv",
        "rdd": rdd_dir / "rdd_robustness.csv",
        "snapshots": snapshots_dir,
        "sensitivity": tmp_path / "results" / "sensitivity",
        "figures": figures_dir,
        "pyproject": pyproject,
    }


def _build(fixture: dict[str, Path], **overrides) -> dict:
    """Helper that calls ``build_public_summary`` with the fixture paths."""
    kwargs = dict(
        verdicts_csv=fixture["verdicts"],
        pap_csv=fixture["pap"],
        rdd_robustness_csv=fixture["rdd"],
        snapshots_dir=fixture["snapshots"],
        sensitivity_root=fixture["sensitivity"],
        figures_dir=fixture["figures"],
        pyproject_path=fixture["pyproject"],
        generated_at="2026-05-17T00:00:00+00:00",
        today=date(2026, 5, 17),
    )
    kwargs.update(overrides)
    return export_module.build_public_summary(**kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_export_contains_required_top_level_keys(fixture_paths, monkeypatch):
    """All advertised top-level sections show up when every input exists."""
    # Pin INDEX_INCLUSION_ROOT so ``paths.project_root()`` (used by
    # ``_relative_to_project_root`` and ``_build_figures_published``) resolves
    # against our fixture tree.
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))

    payload = _build(fixture_paths)
    required = {
        "schema_version",
        "generated_at",
        "source_codebase_version",
        "pap_baseline",
        "pap_deviation_summary",
        "verdicts",
        "hs300_rdd",
        "literature",
        "figures_published",
    }
    assert required.issubset(payload.keys()), (
        f"missing required keys: {required - payload.keys()}"
    )
    # Schema version surfaces and matches the module constant.
    assert payload["schema_version"] == export_module.SCHEMA_VERSION
    assert payload["schema_version"] == 1
    # Source codebase version is read from the synthetic pyproject.
    assert payload["source_codebase_version"] == "9.9.9"
    # All 7 hypotheses round-trip in EXPECTED_HIDS order.
    assert list(payload["verdicts"].keys()) == [
        "H1", "H2", "H3", "H4", "H5", "H6", "H7"
    ]


def test_determinism_same_input_same_output(fixture_paths, monkeypatch):
    """Same input + fixed ``generated_at`` → byte-identical JSON output."""
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))

    first = _build(fixture_paths)
    second = _build(fixture_paths)
    assert first == second
    assert json.dumps(first, ensure_ascii=False, indent=2, sort_keys=True) == (
        json.dumps(second, ensure_ascii=False, indent=2, sort_keys=True)
    )


def test_missing_input_csv_results_in_key_absent(fixture_paths, monkeypatch):
    """A missing input CSV gracefully drops the corresponding key.

    The script must NOT synthesize fake data, and must NOT crash.
    """
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))
    # Remove every optional input.
    fixture_paths["rdd"].unlink()
    fixture_paths["pap"].unlink()
    for snap in fixture_paths["snapshots"].glob("*.csv"):
        snap.unlink()

    payload = _build(fixture_paths)
    assert "hs300_rdd" not in payload
    assert "pap_deviation_summary" not in payload
    # ``pap_baseline`` is absent because no snapshot file remains.
    assert "pap_baseline" not in payload
    # Required artifacts still present.
    assert "verdicts" in payload
    assert "figures_published" in payload


def test_pap_deviation_summary_counts_classifications(fixture_paths, monkeypatch):
    """Classification counters reflect the actual PAP row distribution."""
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))
    mixed = pd.DataFrame(
        [
            {"hid": "H1", "classification": "unchanged"},
            {"hid": "H2", "classification": "flipped"},
            {"hid": "H3", "classification": "tightened"},
            {"hid": "H4", "classification": "weakened"},
            {"hid": "H5", "classification": "unverifiable"},
            {"hid": "H6", "classification": "unchanged"},
            {"hid": "H7", "classification": "tightened"},
        ]
    )
    mixed.to_csv(fixture_paths["pap"], index=False)

    payload = _build(fixture_paths)
    summary = payload["pap_deviation_summary"]
    assert summary["flipped_count"] == 1
    assert summary["tightened_count"] == 2
    assert summary["weakened_count"] == 1
    assert summary["unverifiable_count"] == 1
    assert summary["unchanged_count"] == 2
    assert summary["all_unchanged"] is False  # any deviation flips this


def test_atomic_write_no_tempfile_leftover(fixture_paths, tmp_path, monkeypatch):
    """Successful write should leave the destination file and no .tmp leftovers."""
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))
    payload = _build(fixture_paths)
    output_path = tmp_path / "public_out" / "index_research_summary.json"
    export_module.write_public_summary_atomic(payload, output_path)

    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written == payload

    leftovers = list(
        output_path.parent.glob(f"{output_path.stem}-*.json.tmp")
    )
    assert leftovers == [], f"Tempfile leftover: {leftovers}"


def test_atomic_write_cleans_up_on_write_failure(monkeypatch, tmp_path):
    """If ``json.dump`` blows up, the original file stays untouched and no
    tempfile is left behind."""
    output_path = tmp_path / "index_research_summary.json"
    output_path.write_text('{"prior":"state"}', encoding="utf-8")

    def _explode(*_args, **_kwargs):
        raise RuntimeError("simulated serialization failure")

    monkeypatch.setattr(export_module.json, "dump", _explode)
    with pytest.raises(RuntimeError, match="simulated"):
        export_module.write_public_summary_atomic({"x": "y"}, output_path)

    assert output_path.read_text(encoding="utf-8") == '{"prior":"state"}'
    leftovers = list(tmp_path.glob("index_research_summary-*.json.tmp"))
    assert leftovers == []


def test_figures_published_filters_to_existing_files(fixture_paths, monkeypatch):
    """Advertised figure relpaths that don't exist drop out of the manifest.

    Down-stream consumers can trust every entry is a real file at
    publication time.
    """
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))
    # Delete one of the figures so it should disappear from the manifest.
    doomed = fixture_paths["root"] / export_module.PUBLISHED_FIGURE_RELPATHS[0]
    doomed.unlink()

    payload = _build(fixture_paths)
    surviving = set(payload["figures_published"])
    assert export_module.PUBLISHED_FIGURE_RELPATHS[0] not in surviving
    assert len(surviving) == len(export_module.PUBLISHED_FIGURE_RELPATHS) - 1


def test_no_absolute_paths_or_debug_fields_in_output(fixture_paths, monkeypatch):
    """The public JSON must never contain ``/Users/``-style absolute paths
    or non-public keys leaking in from the source CSVs."""
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))

    payload = _build(fixture_paths)
    blob = json.dumps(payload, ensure_ascii=False)

    # Absolute filesystem paths must never leak (covers Linux + macOS).
    assert "/Users/" not in blob
    assert str(fixture_paths["root"]) not in blob
    # Multi-line CSV narrative fields must never leak (we keep only
    # ``headline_metric``).
    assert "long summary text" not in blob
    assert "multi-line snapshot text" not in blob
    assert "next-step text" not in blob


def test_verdicts_carry_only_safe_subset(fixture_paths, monkeypatch):
    """Each verdict entry exposes only the public keys (no narrative leak)."""
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))

    payload = _build(fixture_paths)
    expected_keys = {
        "name",
        "verdict",
        "confidence",
        "evidence_tier",
        "n_obs",
        "headline_metric",
        "track",
    }
    for hid, entry in payload["verdicts"].items():
        assert set(entry.keys()) == expected_keys, (
            f"{hid} leaked extra keys: {set(entry.keys()) - expected_keys}"
        )
        # Make sure ``headline_metric`` is a short single-line string, not a
        # whole CSV row.
        assert "\n" not in entry["headline_metric"]
        assert len(entry["headline_metric"]) < 80


def test_size_is_bounded_under_20kb(fixture_paths, tmp_path, monkeypatch):
    """The committed artifact stays inside the 5-20 KB budget on realistic
    fixture data (the live data point is even smaller)."""
    monkeypatch.setenv("INDEX_INCLUSION_ROOT", str(fixture_paths["root"]))

    payload = _build(fixture_paths)
    output_path = tmp_path / "out.json"
    export_module.write_public_summary_atomic(payload, output_path)
    size = output_path.stat().st_size
    # Lower bound is loose -- empty fixture without sensitivity is small.
    assert 500 < size < 20_000, f"summary out of budget: {size} bytes"
