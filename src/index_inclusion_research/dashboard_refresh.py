from __future__ import annotations

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
    RefreshArtifact,
    RefreshFailureHandler,
    RefreshJobRunner,
    RefreshRunner,
    RefreshRedirectUrlBuilder,
    RefreshState,
    RefreshStatus,
    RefreshStatusPayload,
    RefreshSuccessHandler,
    RefreshWorkerSpawner,
    RelativePathBuilder,
    SnapshotMeta,
)


def default_refresh_state() -> RefreshState:
    return {
        "status": "idle",
        "message": "页面已就绪，可在不离开当前位置的情况下刷新最新结果。",
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
        "updated_artifacts": [],
        "baseline_artifact_mtimes": {},
    }


def dashboard_snapshot_sources(root: Path) -> list[Path]:
    tracked = [
        root / "results" / "real_tables" / "event_study_summary.csv",
        root / "results" / "real_tables" / "long_window_event_study_summary.csv",
        root / "results" / "real_tables" / "regression_coefficients.csv",
        root / "results" / "real_tables" / "identification_scope.csv",
        root / "results" / "literature" / "harris_gurel" / "summary.md",
        root / "results" / "literature" / "shleifer" / "summary.md",
        root / "results" / "literature" / "hs300_style" / "summary.md",
        root / "results" / "literature" / "hs300_rdd" / "rdd_status.csv",
    ]
    return [path for path in tracked if path.exists()]


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
            "copy": "当前页面尚未读到核心结果文件；首次刷新后这里会显示最新快照时间。",
            "source_path": "",
            "source_count": 0,
        }
    latest = max(files, key=lambda path: path.stat().st_mtime)
    latest_dt = datetime.fromtimestamp(latest.stat().st_mtime).astimezone()
    latest_path = to_relative(latest)
    return {
        "label": latest_dt.strftime("%Y-%m-%d %H:%M"),
        "copy": f"当前页面读取的是 {len(files)} 个核心结果文件中的最新快照，最近更新文件：{latest_path}。",
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
    redirect_url_builder: RefreshRedirectUrlBuilder,
    now_ts: float,
    accepted: bool = True,
) -> RefreshStatusPayload:
    status = state.get("status", "idle")
    message = str(state.get("message", "") or "页面已就绪，可在不离开当前位置的情况下刷新最新结果。")
    scope_label = str(state.get("scope_label", "全部材料") or "全部材料")
    scope_key = str(state.get("scope_key", "all") or "all")
    error = str(state.get("error", "") or "")
    started_ts = float(state.get("started_ts", 0.0) or 0.0)
    finished_ts = float(state.get("finished_ts", 0.0) or 0.0)
    snapshot_label = str(state.get("snapshot_label", "") or snapshot_meta["label"])
    snapshot_copy = str(state.get("snapshot_copy", "") or snapshot_meta["copy"])
    snapshot_source_path = str(state.get("snapshot_source_path", "") or snapshot_meta["source_path"])
    snapshot_source_count = int(state.get("snapshot_source_count", 0) or snapshot_meta["source_count"])
    updated_artifacts = list(state.get("updated_artifacts", []) or [])
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
        "updated_artifacts": updated_artifacts,
    }


def set_refresh_succeeded(
    refresh_lock,
    refresh_state: RefreshState,
    *,
    scope_label: str,
    scope_key: str,
    snapshot_meta: SnapshotMeta,
    updated_artifacts: list[RefreshArtifact],
    finished_at: str,
    finished_ts: float,
) -> None:
    with refresh_lock:
        refresh_state.update(
            {
                "status": "succeeded",
                "scope_label": scope_label,
                "scope_key": scope_key,
                "message": f'“{scope_label}”刷新完成，最新快照时间为 {snapshot_meta["label"]}。',
                "finished_at": finished_at,
                "finished_ts": finished_ts,
                "error": "",
                "snapshot_label": snapshot_meta["label"],
                "snapshot_copy": snapshot_meta["copy"],
                "snapshot_source_path": snapshot_meta["source_path"],
                "snapshot_source_count": snapshot_meta["source_count"],
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
                "message": f'正在后台刷新“{scope_label}”，页面会在完成后自动回到当前位置。',
                "started_at": started_at,
                "started_ts": started_ts,
                "finished_at": "",
                "finished_ts": 0.0,
                "error": "",
                "snapshot_label": snapshot_meta["label"],
                "snapshot_copy": snapshot_meta["copy"],
                "snapshot_source_path": snapshot_meta["source_path"],
                "snapshot_source_count": snapshot_meta["source_count"],
                "baseline_artifact_mtimes": baseline_artifact_mtimes,
            }
        )
    spawn_refresh_worker(runner, scope_label, scope_key)
    return True


def wants_async_refresh(headers: Mapping[str, str]) -> bool:
    requested_with = headers.get("X-Requested-With", "")
    accept = headers.get("Accept", "")
    return requested_with in {"fetch", "XMLHttpRequest"} or "application/json" in accept
