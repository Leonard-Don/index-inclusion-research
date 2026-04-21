import { waypointTitleText } from "./navigation_helpers.js";

export function setWaypointMenuOpen(ctx, nextOpen) {
  const open = Boolean(nextOpen);
  document.body.dataset.waypointMenuOpen = open ? "true" : "false";
  if (ctx.waypointMenu) {
    ctx.waypointMenu.dataset.open = open ? "true" : "false";
    ctx.waypointMenu.setAttribute("aria-hidden", open ? "false" : "true");
  }
  if (ctx.waypointMenuBackdrop) {
    ctx.waypointMenuBackdrop.hidden = !open;
  }
  ctx.waypointMenuToggles.forEach((button) => {
    button.setAttribute("aria-expanded", open ? "true" : "false");
  });
  if (!open) {
    return;
  }
  const activeLink =
    ctx.waypointMenuLinks.find((link) => link.classList.contains("active")) ||
    ctx.waypointMenuLinks[0];
  if (activeLink) {
    requestAnimationFrame(() => {
      activeLink.focus();
    });
  }
}

export function syncWaypointMenuState(ctx, hash, sectionHash) {
  ctx.waypointMenuLinks.forEach((link) => {
    const linkHash = link.getAttribute("href") || "";
    const kind = link.dataset.waypointMenuKind || "section";
    const active = kind === "track" ? linkHash === hash : linkHash === sectionHash;
    link.classList.toggle("active", active);
    if (active) {
      link.setAttribute("aria-current", kind === "track" ? "step" : "location");
    } else {
      link.removeAttribute("aria-current");
    }
  });
}

export function updateWaypointDock(ctx, waypoints, currentWaypoint, scrollY) {
  if (!ctx.waypointDock) {
    return;
  }
  const { item, index } = currentWaypoint;
  const previous = index > 0 ? waypoints[index - 1] : null;
  const next = index >= 0 && index < waypoints.length - 1 ? waypoints[index + 1] : null;
  if (ctx.waypointTitle) {
    ctx.waypointTitle.textContent = waypointTitleText(item);
  }
  ctx.waypointTopLabels.forEach((label) => {
    label.textContent = waypointTitleText(item);
  });
  if (ctx.waypointCopy) {
    ctx.waypointCopy.textContent =
      item && item.kind === "track"
        ? "当前停留在某条研究主线内部，切换展示模式、刷新或继续滚动时都会尽量保留这里的位置。"
        : "章节导航会跟着滚动自动同步，长页面里可以直接从这里继续往前或回到顶部。";
  }
  if (ctx.waypointPrevButton) {
    ctx.waypointPrevButton.disabled = !previous;
    ctx.waypointPrevButton.title = previous ? `上一节：${waypointTitleText(previous)}` : "已经到顶部";
  }
  if (ctx.waypointNextButton) {
    ctx.waypointNextButton.disabled = !next;
    ctx.waypointNextButton.title = next ? `下一节：${waypointTitleText(next)}` : "已经到最后一节";
  }
  ctx.waypointDock.dataset.visible = scrollY > 300 ? "true" : "false";
}
