function queryOne(selector) {
  return document.querySelector(selector);
}

function queryAll(selector) {
  return Array.from(document.querySelectorAll(selector));
}

export function createNavigationContext() {
  return {
    sectionLinks: queryAll("[data-section-link]"),
    modeLinks: queryAll("[data-mode-link]"),
    anchorInputs: queryAll("[data-anchor-input]"),
    waypointElements: queryAll("[data-waypoint]"),
    waypointDock: queryOne("[data-waypoint-dock]"),
    waypointTitle: queryOne("[data-waypoint-title]"),
    waypointTopLabels: queryAll("[data-waypoint-top-label]"),
    waypointCopy: queryOne("[data-waypoint-copy]"),
    waypointPrevButton: queryOne("[data-waypoint-prev]"),
    waypointNextButton: queryOne("[data-waypoint-next]"),
    waypointTopButton: queryOne("[data-waypoint-top]"),
    waypointMenu: queryOne("[data-waypoint-menu]"),
    waypointMenuBackdrop: queryOne("[data-waypoint-menu-backdrop]"),
    waypointMenuToggles: queryAll("[data-waypoint-menu-toggle]"),
    waypointMenuCloseButtons: queryAll("[data-waypoint-menu-close]"),
    waypointMenuLinks: queryAll("[data-waypoint-menu-link]"),
    readingProgress: queryOne("[data-reading-progress]"),
    readingProgressBar: queryOne("[data-reading-progress-bar]"),
    readingProgressLabels: queryAll("[data-reading-progress-label]"),
  };
}

export function createRefreshContext() {
  return {
    refreshForms: queryAll("[data-refresh-form]"),
    refreshButtons: queryAll("[data-refresh-button]"),
    refreshPanel: queryOne("[data-refresh-panel]"),
    refreshStateLabel: queryOne("[data-refresh-state-label]"),
    refreshSnapshotLabel: queryOne("[data-refresh-snapshot-label]"),
    refreshSnapshotCopy: queryOne("[data-refresh-snapshot-copy]"),
    refreshNote: queryOne("[data-refresh-note]"),
    refreshArtifactSummaryInline: queryOne("[data-refresh-artifact-summary-inline]"),
    refreshContractSummary: queryOne("[data-refresh-contract-summary]"),
    refreshScopeLabel: queryOne("[data-refresh-scope-label]"),
    refreshStartedAt: queryOne("[data-refresh-started-at]"),
    refreshFinishedAt: queryOne("[data-refresh-finished-at]"),
    refreshDuration: queryOne("[data-refresh-duration]"),
    refreshSnapshotSource: queryOne("[data-refresh-snapshot-source]"),
    refreshArtifacts: queryOne("[data-refresh-artifacts]"),
    refreshArtifactSummaryLabel: queryOne("[data-refresh-artifact-summary-label]"),
    refreshArtifactSummaryCopy: queryOne("[data-refresh-artifact-summary-copy]"),
    refreshArtifactList: queryOne("[data-refresh-artifact-list]"),
  };
}

export function createSurfaceContext() {
  return {
    detailsOpenInputs: queryAll("[data-open-input]"),
    detailsPanels: queryAll("[data-details-key]"),
    tableDensityButtons: queryAll("[data-table-density-button]"),
    tableWraps: queryAll(".table-wrap"),
  };
}

export function createRuntimeContext() {
  return {
    refreshStatusUrl: document.body.dataset.refreshStatusUrl || "",
    tableDensityStorageKey: "index-inclusion:table-density",
    detailsQueryParam: "open",
    aliasMap: new Map([
      ["#price_pressure_track", "#tracks"],
      ["#demand_curve_track", "#tracks"],
      ["#identification_china_track", "#tracks"],
    ]),
    currentMode() {
      const params = new URLSearchParams(window.location.search);
      return params.get("mode") || "demo";
    },
    formatDuration(seconds) {
      const total = Math.max(0, Math.floor(Number(seconds) || 0));
      const minutes = Math.floor(total / 60);
      const remainder = total % 60;
      if (minutes <= 0) {
        return `${remainder} 秒`;
      }
      return `${minutes} 分 ${remainder.toString().padStart(2, "0")} 秒`;
    },
  };
}
