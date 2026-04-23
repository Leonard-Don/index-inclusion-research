export function collectWaypoints(ctx) {
  return ctx.waypointElements
    .filter((element) => element.id)
    .map((element) => ({
      element,
      hash: `#${element.id}`,
      label: element.dataset.waypointLabel || element.id,
      kind: element.dataset.waypointKind || "section",
      parent: element.dataset.waypointParent || "",
    }));
}

export function computeReadingProgress() {
  const root = document.documentElement;
  const maxScroll = Math.max(0, root.scrollHeight - window.innerHeight);
  const progress = maxScroll <= 0 ? 0 : Math.min(1, Math.max(0, window.scrollY / maxScroll));
  return {
    progress,
    progressValue: Math.round(progress * 100),
  };
}

export function applyReadingProgress(ctx, progress, progressValue) {
  if (ctx.readingProgressBar) {
    ctx.readingProgressBar.style.transform = `scaleX(${progress})`;
  }
  if (ctx.readingProgress) {
    ctx.readingProgress.setAttribute("aria-valuenow", String(progressValue));
    ctx.readingProgress.setAttribute("aria-valuetext", `已浏览 ${progressValue}%`);
  }
  ctx.readingProgressLabels.forEach((label) => {
    label.textContent = `${progressValue}%`;
  });
}

export function waypointIndex(waypoints, hash) {
  return waypoints.findIndex((item) => item.hash === hash);
}

export function currentWaypointForHash(waypoints, hash) {
  const index = waypointIndex(waypoints, hash);
  if (index >= 0) {
    return { item: waypoints[index], index };
  }
  return { item: waypoints[0] || null, index: 0 };
}

export function waypointTitleText(item) {
  if (!item) {
    return "总览";
  }
  if (item.kind === "track" && item.parent) {
    return `${item.parent} / ${item.label}`;
  }
  return item.label;
}

export function candidateWaypointFromScroll(waypoints, threshold = 168) {
  let candidate = waypoints[0];
  for (const item of waypoints) {
    const top = item.element.getBoundingClientRect().top;
    if (top - threshold <= 0) {
      candidate = item;
      continue;
    }
    break;
  }
  if (window.scrollY < 48) {
    candidate = waypoints[0];
  }
  return candidate;
}

export function normalizeHashForAllowedSet(hash, allowedHashes, defaultHash) {
  if (!allowedHashes.length || allowedHashes.includes(hash)) {
    return hash;
  }
  return ["#framework", "#supplement", "#robustness"].includes(hash) &&
    allowedHashes.includes("#tracks")
    ? "#tracks"
    : defaultHash;
}
