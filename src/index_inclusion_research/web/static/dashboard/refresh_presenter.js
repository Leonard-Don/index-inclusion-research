export function refreshRuntimeCopy(ctx, payload) {
  const status = (payload && payload.status) || "idle";
  const message =
    (payload && payload.message) ||
    "页面已就绪，刷新完成后会自动同步本次更新。";
  const durationSeconds = payload && payload.duration_seconds;
  if (status === "running" && durationSeconds != null) {
    return `${message} 已运行 ${ctx.formatDuration(durationSeconds)}。`;
  }
  if (status === "failed" && durationSeconds != null) {
    return `${message} 总耗时约 ${ctx.formatDuration(durationSeconds)}。`;
  }
  return message;
}

export function refreshStateText(payload) {
  const status = (payload && payload.status) || "idle";
  if (status === "running") {
    return "刷新中";
  }
  if (status === "succeeded") {
    return "已完成";
  }
  if (status === "failed") {
    return "刷新失败";
  }
  return "已就绪";
}

export function refreshDurationText(ctx, payload) {
  const durationSeconds = payload && payload.duration_seconds;
  if (durationSeconds == null) {
    return "—";
  }
  return ctx.formatDuration(durationSeconds);
}

export function refreshSnapshotSourceText(payload) {
  const sourcePath = (payload && payload.snapshot_source_path) || "";
  const sourceCount = Number((payload && payload.snapshot_source_count) || 0);
  if (!sourcePath) {
    return sourceCount > 0 ? `${sourceCount} 个核心文件` : "—";
  }
  return `${sourcePath} · ${sourceCount} 个核心文件`;
}

export function refreshContractSummaryText(payload) {
  const status = (payload && payload.status) || "idle";
  if (status === "running") {
    return "待完成后校验";
  }
  return (payload && payload.contract_status_label) || "—";
}

export function renderRefreshArtifacts(ctx, payload) {
  if (!ctx.refreshArtifacts || !ctx.refreshArtifactList) {
    return;
  }
  if (ctx.refreshArtifactSummaryLabel) {
    ctx.refreshArtifactSummaryLabel.textContent =
      (payload && payload.artifact_summary_label) || "—";
  }
  if (ctx.refreshArtifactSummaryCopy) {
    ctx.refreshArtifactSummaryCopy.textContent =
      (payload && payload.artifact_summary_copy) || "—";
  }
  const artifacts = Array.isArray(payload && payload.updated_artifacts)
    ? payload.updated_artifacts
    : [];
  ctx.refreshArtifactList.textContent = "";
  artifacts.forEach((artifact) => {
    const item = document.createElement("li");
    item.className = "refresh-status-artifact-item";
    const path = document.createElement("span");
    path.textContent = artifact && artifact.path ? artifact.path : "";
    const time = document.createElement("time");
    time.textContent = artifact && artifact.modified_at ? artifact.modified_at : "";
    item.append(path, time);
    ctx.refreshArtifactList.append(item);
  });
  ctx.refreshArtifactList.hidden = artifacts.length === 0;
  const hasSummary =
    Boolean((payload && payload.artifact_summary_label) || "") ||
    Boolean((payload && payload.artifact_summary_copy) || "");
  ctx.refreshArtifacts.hidden = artifacts.length === 0 && !hasSummary;
}

export function renderRefreshHealth(ctx, payload) {
  if (ctx.refreshHealthSummary) {
    ctx.refreshHealthSummary.textContent =
      (payload && payload.health_status_label) || "—";
  }
  if (!ctx.refreshHealth || !ctx.refreshHealthList) {
    return;
  }
  if (ctx.refreshHealthLabel) {
    ctx.refreshHealthLabel.textContent =
      (payload && payload.health_status_label) || "—";
  }
  if (ctx.refreshHealthCopy) {
    ctx.refreshHealthCopy.textContent =
      (payload && payload.health_status_copy) || "—";
  }
  const checks = Array.isArray(payload && payload.health_checks)
    ? payload.health_checks
    : [];
  ctx.refreshHealthList.textContent = "";
  checks.forEach((check) => {
    const item = document.createElement("li");
    item.className = "refresh-status-artifact-item";
    item.dataset.healthStatus = (check && check.status) || "";
    const copy = document.createElement("span");
    copy.textContent = check && check.label ? `${check.label} · ${check.copy || ""}` : "";
    item.append(copy);
    if (check && check.command) {
      const command = document.createElement("code");
      command.textContent = check.command;
      item.append(command);
    }
    ctx.refreshHealthList.append(item);
  });
  ctx.refreshHealthList.hidden = checks.length === 0;
  const hasHealth =
    Boolean((payload && payload.health_status_label) || "") ||
    Boolean((payload && payload.health_status_copy) || "");
  ctx.refreshHealth.hidden = checks.length === 0 && !hasHealth;
}

export function applyRefreshStateToDom(
  ctx,
  refreshState,
  payload,
  { startRefreshRuntimeTimer, stopRefreshRuntimeTimer },
) {
  const status = (payload && payload.status) || "idle";
  refreshState.lastPayload = payload || null;
  const scopeKey = (payload && payload.scope_key) || "all";
  const accepted = !(payload && payload.accepted === false);
  if (ctx.refreshPanel) {
    ctx.refreshPanel.dataset.state = status;
  }
  if (ctx.refreshStateLabel) {
    ctx.refreshStateLabel.textContent = refreshStateText(payload);
  }
  if (ctx.refreshSnapshotLabel && payload && payload.snapshot_label) {
    ctx.refreshSnapshotLabel.textContent = payload.snapshot_label;
  }
  if (ctx.refreshSnapshotCopy && payload && payload.snapshot_copy) {
    ctx.refreshSnapshotCopy.textContent = payload.snapshot_copy;
  }
  if (ctx.refreshScopeLabel) {
    ctx.refreshScopeLabel.textContent = (payload && payload.scope_label) || "全部材料";
  }
  if (ctx.refreshStartedAt) {
    ctx.refreshStartedAt.textContent = (payload && payload.started_at) || "—";
  }
  if (ctx.refreshFinishedAt) {
    ctx.refreshFinishedAt.textContent = (payload && payload.finished_at) || "—";
  }
  if (ctx.refreshDuration) {
    ctx.refreshDuration.textContent = refreshDurationText(ctx, payload);
  }
  if (ctx.refreshSnapshotSource) {
    ctx.refreshSnapshotSource.textContent = refreshSnapshotSourceText(payload);
  }
  if (ctx.refreshArtifactSummaryInline) {
    ctx.refreshArtifactSummaryInline.textContent =
      (payload && payload.artifact_summary_label) || "—";
  }
  if (ctx.refreshContractSummary) {
    ctx.refreshContractSummary.textContent = refreshContractSummaryText(payload);
  }
  renderRefreshArtifacts(ctx, payload);
  renderRefreshHealth(ctx, payload);
  if (ctx.refreshNote) {
    ctx.refreshNote.textContent = refreshRuntimeCopy(ctx, payload);
  }
  const running = status === "running";
  if (running) {
    startRefreshRuntimeTimer();
  } else {
    stopRefreshRuntimeTimer();
  }
  ctx.refreshButtons.forEach((button) => {
    if (!button.dataset.defaultLabel) {
      button.dataset.defaultLabel = button.textContent.trim();
    }
    button.disabled = running;
    const buttonScopeKey = button.dataset.scopeKey || "all";
    const isActiveButton = running && buttonScopeKey === scopeKey;
    if (isActiveButton) {
      button.textContent = button.dataset.runningLabel || "刷新中…";
      return;
    }
    if (running && !accepted) {
      button.textContent = "已有刷新在进行";
      return;
    }
    button.textContent = button.dataset.defaultLabel;
  });
}
