from __future__ import annotations

import csv
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from threading import Thread

from index_inclusion_research.dashboard_types import (
    AnchorNormalizer,
    HomeUrlBuilder,
    ModeName,
    NavSection,
    OpenPanelsNormalizer,
    RddContractCheck,
    RefreshArtifact,
    RefreshFailureHandler,
    RefreshJobRunner,
    RefreshRedirectUrlBuilder,
    RefreshRunner,
    RefreshState,
    RefreshStatus,
    RefreshStatusPayload,
    RefreshSuccessHandler,
    RefreshWorkerSpawner,
    RelativePathBuilder,
    ResultHealth,
    ResultHealthCheck,
    SnapshotMeta,
)


def default_refresh_state() -> RefreshState:
    return {
        "status": "idle",
        "message": "页面已就绪，刷新完成后会自动同步本次更新。",
        "scope_label": "全部材料",
        "scope_key": "all",
        "started_at": "",
        "finished_at": "",
        "started_ts": 0.0,
        "finished_ts": 0.0,
        "error": "",
        "snapshot_label": "",
        "snapshot_copy": "",
        "snapshot_source_path": "",
        "snapshot_source_count": 0,
        "contract_status_label": "",
        "contract_status_copy": "",
        "artifact_summary_label": "",
        "artifact_summary_copy": "",
        "updated_artifacts": [],
        "baseline_artifact_mtimes": {},
    }


def dashboard_snapshot_sources(root: Path) -> list[Path]:
    tracked = [
        root / "results" / "real_tables" / "event_study_summary.csv",
        root / "results" / "real_tables" / "long_window_event_study_summary.csv",
        root / "results" / "real_tables" / "regression_coefficients.csv",
        root / "results" / "real_tables" / "identification_scope.csv",
        root / "results" / "real_tables" / "results_manifest.csv",
        root / "results" / "real_tables" / "research_summary.md",
        root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
        root / "results" / "literature" / "harris_gurel" / "summary.md",
        root / "results" / "literature" / "shleifer" / "summary.md",
        root / "results" / "literature" / "hs300_style" / "summary.md",
        root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv",
    ]
    return [path for path in tracked if path.exists()]


def default_result_health() -> ResultHealth:
    return {
        "health_status_label": "未校验",
        "health_status_copy": "当前 payload 未附带结果健康检查。",
        "health_checks": [],
        "health_commands": [],
    }


def _health_check(label: str, status: str, copy: str, command: str = "") -> ResultHealthCheck:
    return {
        "label": label,
        "status": status,
        "copy": copy,
        "command": command,
    }


def _read_first_csv_row(path: Path) -> dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return next(reader, {}) or {}


def _path_label(root: Path, path: Path, to_relative: RelativePathBuilder) -> str:
    try:
        return to_relative(path)
    except ValueError:
        return str(path.relative_to(root)) if path.is_relative_to(root) else str(path)


def _unique_commands(checks: list[ResultHealthCheck]) -> list[str]:
    seen: set[str] = set()
    commands: list[str] = []
    for check in checks:
        command = check.get("command", "").strip()
        if not command or command in seen:
            continue
        seen.add(command)
        commands.append(command)
    return commands


def _rdd_l3_candidate_health(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
    contract_check: RddContractCheck | None,
) -> ResultHealthCheck:
    formal_path = root / "data" / "raw" / "hs300_rdd_candidates.csv"
    reconstructed_path = root / "data" / "raw" / "hs300_rdd_candidates.reconstructed.csv"
    collection_plan_path = root / "results" / "literature" / "hs300_rdd_l3_collection" / "collection_plan.md"
    collection_checklist_path = root / "results" / "literature" / "hs300_rdd_l3_collection" / "batch_collection_checklist.csv"
    collection_template_path = root / "results" / "literature" / "hs300_rdd_l3_collection" / "formal_candidate_template.csv"
    collection_boundary_path = root / "results" / "literature" / "hs300_rdd_l3_collection" / "boundary_reference.csv"
    formal_label = _path_label(root, formal_path, to_relative)
    reconstructed_label = _path_label(root, reconstructed_path, to_relative)
    collection_plan_label = _path_label(root, collection_plan_path, to_relative)
    collection_checklist_label = _path_label(root, collection_checklist_path, to_relative)
    collection_template_label = _path_label(root, collection_template_path, to_relative)
    collection_boundary_label = _path_label(root, collection_boundary_path, to_relative)
    live_status = contract_check["live_status"] if contract_check else {}
    mode = str(live_status.get("mode", "") or "")
    source_kind = str(live_status.get("source_kind", "") or "")
    rows = live_status.get("candidate_rows")
    batches = live_status.get("candidate_batches")
    refresh_command = "index-inclusion-hs300-rdd && index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma"
    import_command = "index-inclusion-prepare-hs300-rdd --input /path/to/raw_candidates.xlsx --check-only"
    collection_command = "index-inclusion-plan-hs300-rdd-l3"

    if formal_path.exists():
        if mode == "real" or source_kind == "real":
            suffix = f"当前 RDD 已使用正式样本：{rows} 条候选、{batches} 个批次。" if rows and batches else "当前 RDD 已使用正式样本。"
            return _health_check("RDD L3 正式样本", "ok", f"{formal_label} 可用，{suffix}")
        return _health_check(
            "RDD L3 正式样本",
            "warning",
            f"{formal_label} 已存在，但当前 RDD 状态仍是 {mode or source_kind or '未知'}；需要重跑识别和主结果。",
            refresh_command,
        )

    if reconstructed_path.exists():
        collection_ready = (
            collection_plan_path.exists()
            and collection_checklist_path.exists()
            and collection_template_path.exists()
            and collection_boundary_path.exists()
        )
        if collection_ready:
            return _health_check(
                "RDD L3 正式样本",
                "warning",
                (
                    f"未找到 {formal_label}；当前只能使用 {reconstructed_label} 的 L2 公开重建样本。"
                    f"L3 采集包已就绪：{collection_plan_label}、{collection_checklist_label}、"
                    f"{collection_template_label}、{collection_boundary_label}。"
                ),
                import_command,
            )
        return _health_check(
            "RDD L3 正式样本",
            "warning",
            f"未找到 {formal_label}；当前只能使用 {reconstructed_label} 的 L2 公开重建样本。",
            collection_command,
        )

    return _health_check(
        "RDD L3 正式样本",
        "missing",
        f"未找到 {formal_label}，也未找到 {reconstructed_label}。",
        import_command,
    )


def build_result_health(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
    contract_check: RddContractCheck | None,
) -> ResultHealth:
    required = [
        (
            "事件研究摘要",
            root / "results" / "real_tables" / "event_study_summary.csv",
            "index-inclusion-make-figures-tables",
        ),
        (
            "回归系数表",
            root / "results" / "real_tables" / "regression_coefficients.csv",
            "index-inclusion-make-figures-tables",
        ),
        (
            "结果状态 manifest",
            root / "results" / "real_tables" / "results_manifest.csv",
            "index-inclusion-make-figures-tables",
        ),
        (
            "RDD 状态",
            root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv",
            "index-inclusion-hs300-rdd",
        ),
        (
            "CMA 假说裁决",
            root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv",
            "index-inclusion-cma",
        ),
        (
            "研究摘要",
            root / "results" / "real_tables" / "research_summary.md",
            "index-inclusion-generate-research-report && index-inclusion-cma",
        ),
    ]
    checks: list[ResultHealthCheck] = []
    for label, path, command in required:
        relative = _path_label(root, path, to_relative)
        if path.exists():
            modified = refresh_artifact_modified_at(path)
            checks.append(_health_check(label, "ok", f"{relative} 可用，最近修改：{modified}。"))
        else:
            checks.append(_health_check(label, "missing", f"未找到 {relative}。", command))

    if contract_check is None:
        checks.append(_health_check("RDD/manifest 契约", "warning", "本次状态未附带 RDD 契约检查。"))
    elif not contract_check["manifest_exists"]:
        checks.append(
            _health_check(
                "RDD/manifest 契约",
                "missing",
                f"未找到 {contract_check['manifest_path']}，无法确认 RDD 状态是否同步。",
                "index-inclusion-make-figures-tables",
            )
        )
    elif not contract_check["matches"]:
        mismatch_labels = "、".join(_contract_field_label(field) for field in contract_check["mismatched_fields"])
        checks.append(
            _health_check(
                "RDD/manifest 契约",
                "warning",
                f"manifest 与 live RDD 状态在 {mismatch_labels} 上不一致。",
                "index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma",
            )
        )
    else:
        checks.append(_health_check("RDD/manifest 契约", "ok", "manifest 与当前 RDD 识别状态一致。"))

    checks.append(_rdd_l3_candidate_health(root, to_relative=to_relative, contract_check=contract_check))

    if contract_check and contract_check["manifest_exists"]:
        live_generated = str(contract_check["live_status"].get("generated_at", "") or "")
        manifest_generated = str(contract_check["manifest"].get("rdd_generated_at", "") or "")
        if live_generated and manifest_generated and live_generated != manifest_generated:
            checks.append(
                _health_check(
                    "RDD 状态新鲜度",
                    "warning",
                    f"rdd_status.csv 的生成时间为 {live_generated}，manifest 仍记录 {manifest_generated}。",
                    "index-inclusion-make-figures-tables && index-inclusion-generate-research-report && index-inclusion-cma",
                )
            )
        elif live_generated:
            checks.append(_health_check("RDD 状态新鲜度", "ok", f"manifest 已记录最新 RDD 生成时间：{live_generated}。"))

    verdict_path = root / "results" / "real_tables" / "cma_hypothesis_verdicts.csv"
    if verdict_path.exists():
        with verdict_path.open(newline="", encoding="utf-8") as fh:
            verdict_rows = max(0, len(list(csv.DictReader(fh))))
        status = "ok" if verdict_rows >= 6 else "warning"
        copy = f"CMA 假说裁决表包含 {verdict_rows} 条记录。"
        command = "" if status == "ok" else "index-inclusion-cma"
        checks.append(_health_check("CMA 裁决覆盖", status, copy, command))

    severity = [check["status"] for check in checks]
    commands = _unique_commands(checks)
    if "missing" in severity:
        label = "结果健康缺项"
        missing_count = sum(1 for status in severity if status == "missing")
        copy = f"缺少 {missing_count} 项核心产物；建议按提示命令补生成后刷新页面。"
    elif "warning" in severity:
        label = "结果健康需关注"
        warning_count = sum(1 for status in severity if status == "warning")
        copy = f"{warning_count} 项结果状态需要同步或补充；核心页面仍可阅读。"
    else:
        label = "结果健康良好"
        copy = "核心结果、RDD 状态和 CMA 裁决表都可用。"
    return {
        "health_status_label": label,
        "health_status_copy": copy,
        "health_checks": checks,
        "health_commands": commands,
    }


def _contract_field_label(field: str) -> str:
    labels = {
        "mode": "模式",
        "evidence_tier": "证据等级",
        "evidence_status": "证据状态",
        "source_kind": "来源类型",
        "source_label": "来源摘要",
        "source_file": "来源文件",
        "coverage_note": "覆盖说明",
        "candidate_rows": "候选样本数",
        "candidate_batches": "候选批次数",
        "treated_rows": "调入样本数",
        "control_rows": "对照样本数",
        "crossing_batches": "跨 cutoff 批次数",
    }
    return labels.get(field, field)


def refresh_contract_status(contract_check: RddContractCheck | None) -> tuple[str, str]:
    if contract_check is None:
        return ("未校验", "本次刷新未附带结果状态校验。")
    manifest_path = contract_check["manifest_path"] or "results_manifest.csv"
    if not contract_check["manifest_exists"]:
        return ("缺少结果状态文件", f"未找到 {manifest_path}；刷新完成后请补生成结构化结果状态文件。")
    if contract_check["matches"]:
        return ("结果状态已同步", f"{manifest_path} 已与当前识别状态保持一致。")
    mismatch_labels = "、".join(_contract_field_label(field) for field in contract_check["mismatched_fields"])
    return (
        "结果状态待同步",
        f"{manifest_path} 与当前识别状态在 {mismatch_labels} 上不一致；建议重跑 index-inclusion-make-figures-tables 和 index-inclusion-generate-research-report。",
    )


def refresh_artifact_summary(
    *,
    status: RefreshStatus,
    updated_artifacts: list[RefreshArtifact],
    contract_check: RddContractCheck | None,
) -> tuple[str, str]:
    contract_status_label, contract_status_copy = refresh_contract_status(contract_check)
    if status == "running":
        return ("本次刷新进行中", "正在生成最新核心产物；结果状态会在刷新完成后重新校验。")
    if status == "failed":
        return ("本次刷新未完成", f"页面仍显示上一个成功快照；结果状态：{contract_status_label}。{contract_status_copy}")
    if status != "succeeded":
        return ("最近结果概览", f"结果状态：{contract_status_label}。{contract_status_copy}")

    artifact_count = len(updated_artifacts)
    if artifact_count <= 0:
        return (
            "本次未发现新的核心产物",
            f"本次刷新未检测到新的核心结果文件变化；结果状态：{contract_status_label}。{contract_status_copy}",
        )

    preview_paths = [str(item.get("path", "") or "") for item in updated_artifacts[:2] if item.get("path")]
    preview = "、".join(preview_paths)
    if preview and artifact_count > len(preview_paths):
        preview = f"{preview} 等 {artifact_count} 项"
    elif not preview:
        preview = f"共 {artifact_count} 项"
    return (
        f"本次更新 {artifact_count} 项核心产物",
        f"最近变更：{preview}；结果状态：{contract_status_label}。{contract_status_copy}",
    )


def build_dashboard_snapshot_meta(
    root: Path,
    *,
    to_relative: RelativePathBuilder,
    snapshot_files: list[Path] | None = None,
) -> SnapshotMeta:
    files = dashboard_snapshot_sources(root) if snapshot_files is None else [path for path in snapshot_files if path.exists()]
    if not files:
        return {
            "label": "尚未生成",
            "copy": "页面暂未读到核心结果文件；首次刷新后这里会显示最新快照时间。",
            "source_path": "",
            "source_count": 0,
        }
    latest = max(files, key=lambda path: path.stat().st_mtime)
    latest_dt = datetime.fromtimestamp(latest.stat().st_mtime).astimezone()
    latest_path = to_relative(latest)
    return {
        "label": latest_dt.strftime("%Y-%m-%d %H:%M"),
        "copy": f"页面目前读取的是 {len(files)} 个核心结果文件中的最新快照，最近更新文件：{latest_path}。",
        "source_path": latest_path,
        "source_count": len(files),
    }


def refresh_artifact_modified_at(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone().strftime("%Y-%m-%d %H:%M")


def snapshot_artifact_mtimes(
    snapshot_files: list[Path],
    *,
    to_relative: RelativePathBuilder,
) -> dict[str, float]:
    mtimes: dict[str, float] = {}
    for path in snapshot_files:
        if not path.exists():
            continue
        mtimes[to_relative(path)] = path.stat().st_mtime
    return mtimes


def build_updated_artifacts(
    snapshot_files: list[Path],
    *,
    baseline_artifact_mtimes: Mapping[str, float],
    to_relative: RelativePathBuilder,
) -> list[RefreshArtifact]:
    updated: list[tuple[float, RefreshArtifact]] = []
    for path in snapshot_files:
        if not path.exists():
            continue
        relative_path = to_relative(path)
        current_mtime = path.stat().st_mtime
        previous_mtime = baseline_artifact_mtimes.get(relative_path)
        if previous_mtime is not None and current_mtime <= previous_mtime:
            continue
        updated.append(
            (
                current_mtime,
                {
                    "path": relative_path,
                    "modified_at": refresh_artifact_modified_at(path),
                },
            )
        )
    updated.sort(key=lambda item: item[0], reverse=True)
    return [artifact for _, artifact in updated]


def refresh_timestamp(now: datetime | None = None) -> str:
    current = now.astimezone() if now is not None else datetime.now().astimezone()
    return current.strftime("%Y-%m-%d %H:%M")


def resolve_dashboard_mode(raw_mode: str | None) -> ModeName:
    mode = raw_mode or "demo"
    return mode if mode in {"brief", "demo", "full"} else "demo"


def normalize_open_panels(raw: str | None, *, allowed_keys: frozenset[str] | set[str]) -> str:
    if raw is None:
        return ""
    seen: set[str] = set()
    keys: list[str] = []
    for item in raw.split(","):
        key = item.strip()
        if not key or key not in allowed_keys or key in seen:
            continue
        seen.add(key)
        keys.append(key)
    return ",".join(keys)


def normalize_anchor_for_mode(
    mode: ModeName,
    anchor: str | None,
    *,
    nav_sections: list[NavSection],
    track_anchors: set[str],
) -> str:
    raw = (anchor or "").strip().lstrip("#")
    allowed = {item["anchor"] for item in nav_sections} | track_anchors
    if not raw:
        return "overview"
    if raw in allowed:
        return raw
    if raw in {"framework", "supplement", "robustness"} and "tracks" in allowed:
        return "tracks"
    return "overview"


def refresh_redirect_url(
    mode: ModeName,
    anchor: str,
    *,
    open_panels: str | None,
    details_query_param: str,
    url_builder: HomeUrlBuilder,
    normalize_anchor: AnchorNormalizer,
    normalize_open_panels: OpenPanelsNormalizer,
) -> str:
    redirect_kwargs: dict[str, str] = {"mode": mode}
    normalized = normalize_anchor(mode, anchor)
    if open_panels is not None:
        redirect_kwargs[details_query_param] = normalize_open_panels(open_panels)
    if normalized:
        redirect_kwargs["_anchor"] = normalized
    return url_builder(**redirect_kwargs)


def refresh_poll_after_ms(status: RefreshStatus, started_ts: float, *, now_ts: float) -> int:
    if status != "running" or started_ts <= 0:
        return 1500
    elapsed = max(0.0, now_ts - started_ts)
    if elapsed < 12:
        return 1200
    if elapsed < 45:
        return 2500
    return 5000


def refresh_duration_seconds(
    started_ts: float,
    finished_ts: float,
    status: RefreshStatus,
    *,
    now_ts: float,
) -> int | None:
    if started_ts <= 0:
        return None
    if status == "running":
        return int(max(0.0, now_ts - started_ts))
    if finished_ts > 0:
        return int(max(0.0, finished_ts - started_ts))
    return None


def refresh_status_payload(
    state: RefreshState,
    *,
    mode: ModeName,
    anchor: str,
    open_panels: str | None,
    snapshot_meta: SnapshotMeta,
    contract_check: RddContractCheck | None,
    result_health: ResultHealth | None = None,
    redirect_url_builder: RefreshRedirectUrlBuilder,
    now_ts: float,
    accepted: bool = True,
) -> RefreshStatusPayload:
    status = state.get("status", "idle")
    message = str(state.get("message", "") or "页面已就绪，刷新完成后会自动同步本次更新。")
    scope_label = str(state.get("scope_label", "全部材料") or "全部材料")
    scope_key = str(state.get("scope_key", "all") or "all")
    error = str(state.get("error", "") or "")
    started_ts = float(state.get("started_ts", 0.0) or 0.0)
    finished_ts = float(state.get("finished_ts", 0.0) or 0.0)
    snapshot_label = str(state.get("snapshot_label", "") or snapshot_meta["label"])
    snapshot_copy = str(state.get("snapshot_copy", "") or snapshot_meta["copy"])
    snapshot_source_path = str(state.get("snapshot_source_path", "") or snapshot_meta["source_path"])
    snapshot_source_count = int(state.get("snapshot_source_count", 0) or snapshot_meta["source_count"])
    contract_status_label, contract_status_copy = refresh_contract_status(contract_check)
    contract_status_label = str(state.get("contract_status_label", "") or contract_status_label)
    contract_status_copy = str(state.get("contract_status_copy", "") or contract_status_copy)
    updated_artifacts = list(state.get("updated_artifacts", []) or [])
    artifact_summary_label, artifact_summary_copy = refresh_artifact_summary(
        status=status,
        updated_artifacts=updated_artifacts,
        contract_check=contract_check,
    )
    artifact_summary_label = str(state.get("artifact_summary_label", "") or artifact_summary_label)
    artifact_summary_copy = str(state.get("artifact_summary_copy", "") or artifact_summary_copy)
    health = result_health or default_result_health()
    duration_seconds = refresh_duration_seconds(started_ts, finished_ts, status, now_ts=now_ts)
    if status == "failed" and error:
        message = f"{message} {error}"
    return {
        "accepted": accepted,
        "status": status,
        "message": message,
        "error": error,
        "scope_label": scope_label,
        "scope_key": scope_key,
        "started_at": str(state.get("started_at", "")),
        "finished_at": str(state.get("finished_at", "")),
        "started_ts": started_ts,
        "finished_ts": finished_ts,
        "duration_seconds": duration_seconds,
        "poll_after_ms": refresh_poll_after_ms(status, started_ts, now_ts=now_ts),
        "redirect_url": redirect_url_builder(mode, anchor, open_panels) if status == "succeeded" else "",
        "snapshot_label": snapshot_label,
        "snapshot_copy": snapshot_copy,
        "snapshot_source_path": snapshot_source_path,
        "snapshot_source_count": snapshot_source_count,
        "contract_status_label": contract_status_label,
        "contract_status_copy": contract_status_copy,
        "artifact_summary_label": artifact_summary_label,
        "artifact_summary_copy": artifact_summary_copy,
        "health_status_label": health["health_status_label"],
        "health_status_copy": health["health_status_copy"],
        "health_checks": health["health_checks"],
        "health_commands": health["health_commands"],
        "updated_artifacts": updated_artifacts,
    }


def set_refresh_succeeded(
    refresh_lock,
    refresh_state: RefreshState,
    *,
    scope_label: str,
    scope_key: str,
    snapshot_meta: SnapshotMeta,
    contract_check: RddContractCheck | None,
    updated_artifacts: list[RefreshArtifact],
    finished_at: str,
    finished_ts: float,
) -> None:
    contract_status_label, contract_status_copy = refresh_contract_status(contract_check)
    artifact_summary_label, artifact_summary_copy = refresh_artifact_summary(
        status="succeeded",
        updated_artifacts=updated_artifacts,
        contract_check=contract_check,
    )
    with refresh_lock:
        refresh_state.update(
            {
                "status": "succeeded",
                "scope_label": scope_label,
                "scope_key": scope_key,
                "message": f'“{scope_label}”刷新完成，本次更新已同步。',
                "finished_at": finished_at,
                "finished_ts": finished_ts,
                "error": "",
                "snapshot_label": snapshot_meta["label"],
                "snapshot_copy": snapshot_meta["copy"],
                "snapshot_source_path": snapshot_meta["source_path"],
                "snapshot_source_count": snapshot_meta["source_count"],
                "contract_status_label": contract_status_label,
                "contract_status_copy": contract_status_copy,
                "artifact_summary_label": artifact_summary_label,
                "artifact_summary_copy": artifact_summary_copy,
                "updated_artifacts": updated_artifacts,
                "baseline_artifact_mtimes": {},
            }
        )


def set_refresh_failed(
    refresh_lock,
    refresh_state: RefreshState,
    *,
    scope_label: str,
    scope_key: str,
    error: str,
    finished_at: str,
    finished_ts: float,
) -> None:
    with refresh_lock:
        refresh_state.update(
            {
                "status": "failed",
                "scope_label": scope_label,
                "scope_key": scope_key,
                "message": f'“{scope_label}”刷新失败，请检查后台日志或稍后重试。',
                "finished_at": finished_at,
                "finished_ts": finished_ts,
                "error": error,
                "contract_status_label": "",
                "contract_status_copy": "",
                "artifact_summary_label": "",
                "artifact_summary_copy": "",
                "updated_artifacts": [],
                "baseline_artifact_mtimes": {},
            }
        )


def run_refresh_job(
    runner: RefreshRunner,
    scope_label: str,
    scope_key: str,
    *,
    mark_refresh_succeeded: RefreshSuccessHandler,
    mark_refresh_failed: RefreshFailureHandler,
) -> None:
    try:
        runner()
    except Exception as exc:
        mark_refresh_failed(scope_label, scope_key, exc)
        return
    mark_refresh_succeeded(scope_label, scope_key)


def spawn_refresh_worker(
    runner: RefreshRunner,
    scope_label: str,
    scope_key: str,
    *,
    run_refresh_job: RefreshJobRunner,
) -> None:
    Thread(target=run_refresh_job, args=(runner, scope_label, scope_key), daemon=True).start()


def queue_refresh_job(
    refresh_lock,
    refresh_state: RefreshState,
    *,
    runner: RefreshRunner,
    scope_label: str,
    scope_key: str,
    started_at: str,
    started_ts: float,
    snapshot_meta: SnapshotMeta,
    baseline_artifact_mtimes: dict[str, float],
    spawn_refresh_worker: RefreshWorkerSpawner,
) -> bool:
    with refresh_lock:
        if refresh_state.get("status") == "running":
            return False
        refresh_state.update(
            {
                "status": "running",
                "scope_label": scope_label,
                "scope_key": scope_key,
                "message": f'正在刷新“{scope_label}”，完成后会自动同步本次更新。',
                "started_at": started_at,
                "started_ts": started_ts,
                "finished_at": "",
                "finished_ts": 0.0,
                "error": "",
                "snapshot_label": snapshot_meta["label"],
                "snapshot_copy": snapshot_meta["copy"],
                "snapshot_source_path": snapshot_meta["source_path"],
                "snapshot_source_count": snapshot_meta["source_count"],
                "contract_status_label": "",
                "contract_status_copy": "",
                "artifact_summary_label": "",
                "artifact_summary_copy": "",
                "updated_artifacts": [],
                "baseline_artifact_mtimes": baseline_artifact_mtimes,
            }
        )
    spawn_refresh_worker(runner, scope_label, scope_key)
    return True


def wants_async_refresh(headers: Mapping[str, str]) -> bool:
    requested_with = headers.get("X-Requested-With", "")
    accept = headers.get("Accept", "")
    return requested_with in {"fetch", "XMLHttpRequest"} or "application/json" in accept
