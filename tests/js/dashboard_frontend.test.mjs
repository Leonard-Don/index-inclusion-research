import test from "node:test";
import assert from "node:assert/strict";

import { bootstrapDashboard } from "../../src/index_inclusion_research/web/static/dashboard/bootstrap.js";
import {
  createNavigationContext,
  createRefreshContext,
  createRuntimeContext,
  createSurfaceContext,
} from "../../src/index_inclusion_research/web/static/dashboard/context_groups.js";
import {
  currentWaypointForHash,
  normalizeHashForAllowedSet,
  waypointTitleText,
} from "../../src/index_inclusion_research/web/static/dashboard/navigation_helpers.js";
import {
  setWaypointMenuOpen,
  syncWaypointMenuState,
  updateWaypointDock,
} from "../../src/index_inclusion_research/web/static/dashboard/navigation_ui.js";
import {
  refreshRuntimeCopy,
  refreshSnapshotSourceText,
  refreshStateText,
} from "../../src/index_inclusion_research/web/static/dashboard/refresh_presenter.js";
import { fetchRefreshStatus, postRefreshRequest } from "../../src/index_inclusion_research/web/static/dashboard/refresh_requests.js";
import { createDetailsSurface } from "../../src/index_inclusion_research/web/static/dashboard/surface_details.js";
import { createTableSurface } from "../../src/index_inclusion_research/web/static/dashboard/surface_tables.js";
import { createSensitivityThresholdController } from "../../src/index_inclusion_research/web/static/dashboard/sensitivity_threshold.js";
import { createVerdictFilterController } from "../../src/index_inclusion_research/web/static/dashboard/verdict_filter.js";

function createClassList(initialValues = []) {
  const values = new Set(initialValues);
  return {
    contains(name) {
      return values.has(name);
    },
    toggle(name, force) {
      if (force === undefined) {
        if (values.has(name)) {
          values.delete(name);
          return false;
        }
        values.add(name);
        return true;
      }
      if (force) {
        values.add(name);
        return true;
      }
      values.delete(name);
      return false;
    },
  };
}

function withFakeDom(
  {
    selectors = new Map(),
    search = "?mode=demo",
    href = `http://localhost/${search.replace(/^\?/, "?")}`,
    origin = "http://localhost",
    bodyDataset = {},
    localStorage = {
      getItem() {
        return null;
      },
      setItem() {},
    },
    history = {
      replaceState() {},
    },
    requestAnimationFrameImpl = (callback) => {
      callback();
      return 1;
    },
    resizeObserverImpl = undefined,
    fetchImpl = undefined,
    addWindowListener = () => {},
  },
  callback,
) {
  const previousDocument = globalThis.document;
  const previousWindow = globalThis.window;
  const previousHistory = globalThis.history;
  const previousRequestAnimationFrame = globalThis.requestAnimationFrame;
  const previousResizeObserver = globalThis.ResizeObserver;
  const previousFetch = globalThis.fetch;
  const restore = () => {
    globalThis.document = previousDocument;
    globalThis.window = previousWindow;
    globalThis.history = previousHistory;
    globalThis.requestAnimationFrame = previousRequestAnimationFrame;
    globalThis.ResizeObserver = previousResizeObserver;
    globalThis.fetch = previousFetch;
  };
  globalThis.document = {
    body: {
      dataset: {
        refreshStatusUrl: "/refresh/status",
        ...bodyDataset,
      },
    },
    querySelector(selector) {
      const value = selectors.get(selector);
      if (Array.isArray(value)) {
        return value[0] ?? null;
      }
      return value ?? null;
    },
    querySelectorAll(selector) {
      const value = selectors.get(selector);
      if (Array.isArray(value)) {
        return value;
      }
      return value ? [value] : [];
    },
  };
  globalThis.window = {
    location: {
      search,
      href,
      origin,
    },
    localStorage,
    addEventListener: addWindowListener,
    ResizeObserver: resizeObserverImpl,
  };
  globalThis.history = history;
  globalThis.requestAnimationFrame = requestAnimationFrameImpl;
  globalThis.ResizeObserver = resizeObserverImpl;
  if (fetchImpl !== undefined) {
    globalThis.fetch = fetchImpl;
  }
  try {
    const result = callback();
    if (result && typeof result.then === "function") {
      return result.finally(restore);
    }
    restore();
    return result;
  } catch (error) {
    restore();
    throw error;
  }
}

test("bootstrapDashboard wires controllers in order and returns handles", () => {
  const calls = [];
  const context = { id: "ctx" };
  let detailsCallback = null;
  const surface = {
    initialize(options) {
      calls.push("surface.initialize");
      detailsCallback = options.onDetailsStateChange;
    },
  };
  const navigation = {
    initialize() {
      calls.push("navigation.initialize");
    },
    syncTopbarState() {
      calls.push("navigation.syncTopbarState");
    },
  };
  const refresh = {
    initialize() {
      calls.push("refresh.initialize");
    },
  };
  const verdictFilter = {
    initialize() {
      calls.push("verdictFilter.initialize");
    },
  };
  const sensitivityThreshold = {
    initialize() {
      calls.push("sensitivityThreshold.initialize");
    },
  };

  const result = bootstrapDashboard({
    createDashboardContext() {
      calls.push("createDashboardContext");
      return context;
    },
    createSurfaceController(receivedContext) {
      calls.push("createSurfaceController");
      assert.equal(receivedContext, context);
      return surface;
    },
    createNavigationController(receivedContext, receivedSurface) {
      calls.push("createNavigationController");
      assert.equal(receivedContext, context);
      assert.equal(receivedSurface, surface);
      return navigation;
    },
    createRefreshController(receivedContext, receivedSurface, receivedNavigation) {
      calls.push("createRefreshController");
      assert.equal(receivedContext, context);
      assert.equal(receivedSurface, surface);
      assert.equal(receivedNavigation, navigation);
      return refresh;
    },
    createVerdictFilterController() {
      calls.push("createVerdictFilterController");
      return verdictFilter;
    },
    createSensitivityThresholdController() {
      calls.push("createSensitivityThresholdController");
      return sensitivityThreshold;
    },
  });

  assert.deepEqual(calls, [
    "createDashboardContext",
    "createSurfaceController",
    "createNavigationController",
    "createRefreshController",
    "createVerdictFilterController",
    "createSensitivityThresholdController",
    "surface.initialize",
    "navigation.initialize",
    "refresh.initialize",
    "verdictFilter.initialize",
    "sensitivityThreshold.initialize",
  ]);
  assert.equal(result.context, context);
  assert.equal(result.surface, surface);
  assert.equal(result.navigation, navigation);
  assert.equal(result.refresh, refresh);
  assert.equal(result.verdictFilter, verdictFilter);
  assert.equal(result.sensitivityThreshold, sensitivityThreshold);

  assert.equal(typeof detailsCallback, "function");
  detailsCallback();
  assert.equal(calls.at(-1), "navigation.syncTopbarState");
});

test("context group builders query selectors by concern and preserve runtime helpers", () => {
  const selectors = new Map([
    ["[data-section-link]", [{ id: "section-a" }, { id: "section-b" }]],
    ["[data-mode-link]", [{ id: "mode-a" }]],
    ["[data-anchor-input]", [{ id: "anchor-a" }]],
    ["[data-waypoint]", [{ id: "overview" }]],
    ["[data-waypoint-dock]", { id: "dock" }],
    ["[data-waypoint-title]", { id: "title" }],
    ["[data-waypoint-top-label]", [{ id: "top-label" }]],
    ["[data-waypoint-copy]", { id: "copy" }],
    ["[data-waypoint-prev]", { id: "prev" }],
    ["[data-waypoint-next]", { id: "next" }],
    ["[data-waypoint-top]", { id: "top" }],
    ["[data-waypoint-menu]", { id: "menu" }],
    ["[data-waypoint-menu-backdrop]", { id: "backdrop" }],
    ["[data-waypoint-menu-toggle]", [{ id: "toggle" }]],
    ["[data-waypoint-menu-close]", [{ id: "close" }]],
    ["[data-waypoint-menu-link]", [{ id: "menu-link" }]],
    ["[data-reading-progress]", { id: "progress" }],
    ["[data-reading-progress-bar]", { id: "progress-bar" }],
    ["[data-reading-progress-label]", [{ id: "progress-label" }]],
    ["[data-refresh-form]", [{ id: "refresh-form" }]],
    ["[data-refresh-button]", [{ id: "refresh-button" }]],
    ["[data-refresh-panel]", { id: "refresh-panel" }],
    ["[data-refresh-state-label]", { id: "refresh-state" }],
    ["[data-refresh-snapshot-label]", { id: "refresh-snapshot" }],
    ["[data-refresh-snapshot-copy]", { id: "refresh-copy" }],
    ["[data-refresh-note]", { id: "refresh-note" }],
    ["[data-refresh-artifact-summary-inline]", { id: "artifact-inline" }],
    ["[data-refresh-contract-summary]", { id: "contract-summary" }],
    ["[data-refresh-scope-label]", { id: "scope-label" }],
    ["[data-refresh-started-at]", { id: "started-at" }],
    ["[data-refresh-finished-at]", { id: "finished-at" }],
    ["[data-refresh-duration]", { id: "duration" }],
    ["[data-refresh-snapshot-source]", { id: "snapshot-source" }],
    ["[data-refresh-artifacts]", { id: "artifacts" }],
    ["[data-refresh-artifact-summary-label]", { id: "artifact-label" }],
    ["[data-refresh-artifact-summary-copy]", { id: "artifact-copy" }],
    ["[data-refresh-artifact-list]", { id: "artifact-list" }],
    ["[data-open-input]", [{ id: "open-input" }]],
    ["[data-details-key]", [{ id: "details-panel" }]],
    ["[data-table-density-button]", [{ id: "density-button" }]],
    [".table-wrap", [{ id: "table-wrap" }]],
  ]);

  withFakeDom({ selectors, search: "?mode=full" }, () => {
    const navigation = createNavigationContext();
    const refresh = createRefreshContext();
    const surface = createSurfaceContext();
    const runtime = createRuntimeContext();

    assert.equal(navigation.sectionLinks.length, 2);
    assert.equal(navigation.waypointMenuLinks.length, 1);
    assert.equal(refresh.refreshForms.length, 1);
    assert.equal(refresh.refreshPanel.id, "refresh-panel");
    assert.equal(surface.detailsPanels.length, 1);
    assert.equal(surface.tableWraps.length, 1);
    assert.equal(runtime.currentMode(), "full");
    assert.equal(runtime.formatDuration(125), "2 分 05 秒");
    assert.equal(runtime.aliasMap.get("#price_pressure_track"), "#tracks");
  });
});

test("navigation helpers keep track fallback and titles stable", () => {
  assert.equal(
    normalizeHashForAllowedSet("#framework", ["#overview", "#tracks"], "#overview"),
    "#tracks",
  );
  assert.equal(
    normalizeHashForAllowedSet("#appendix", ["#overview"], "#overview"),
    "#overview",
  );

  const waypoints = [
    { hash: "#overview", label: "总览", kind: "section", parent: "" },
    { hash: "#price_pressure_track", label: "短期价格压力", kind: "track", parent: "主线结果" },
  ];
  assert.deepEqual(currentWaypointForHash(waypoints, "#price_pressure_track"), {
    item: waypoints[1],
    index: 1,
  });
  assert.deepEqual(currentWaypointForHash(waypoints, "#missing"), {
    item: waypoints[0],
    index: 0,
  });
  assert.equal(waypointTitleText(waypoints[1]), "主线结果 / 短期价格压力");
});

test("refresh presenter helpers render stable labels and runtime copy", () => {
  assert.equal(refreshStateText({ status: "running" }), "刷新中");
  assert.equal(refreshStateText({ status: "failed" }), "刷新失败");
  assert.equal(refreshStateText({ status: "idle" }), "已就绪");

  assert.equal(
    refreshRuntimeCopy(
      { formatDuration: (seconds) => `${seconds} 秒` },
      { status: "running", message: "正在刷新", duration_seconds: 5 },
    ),
    "正在刷新 已运行 5 秒。",
  );
  assert.equal(
    refreshSnapshotSourceText({
      snapshot_source_path: "results/real_tables/results_manifest.csv",
      snapshot_source_count: 3,
    }),
    "results/real_tables/results_manifest.csv · 3 个核心文件",
  );
  assert.equal(refreshSnapshotSourceText({ snapshot_source_count: 0 }), "—");
});

test("surface details keeps query state, storage state, and callbacks aligned", () => {
  const input = { value: "" };
  const toggle = { textContent: "展开 Alpha", dataset: {} };
  const panelListeners = {};
  const panel = {
    dataset: { detailsKey: "alpha" },
    open: false,
    querySelector(selector) {
      return selector === "[data-details-toggle]" ? toggle : null;
    },
    addEventListener(eventName, callback) {
      panelListeners[eventName] = callback;
    },
  };
  const storage = new Map();
  const historyCalls = [];
  let detailsChanged = 0;
  let wrapSyncCount = 0;

  withFakeDom(
    {
      search: "?mode=demo&open=alpha",
      href: "http://localhost/?mode=demo&open=alpha#overview",
      localStorage: {
        getItem(key) {
          return storage.get(key) ?? null;
        },
        setItem(key, value) {
          storage.set(key, value);
        },
      },
      history: {
        replaceState(_state, _title, url) {
          historyCalls.push(url);
        },
      },
    },
    () => {
      const surface = createDetailsSurface(
        {
          detailsPanels: [panel],
          detailsOpenInputs: [input],
          detailsQueryParam: "open",
        },
        {
          syncAllTableWraps() {
            wrapSyncCount += 1;
          },
        },
      );

      surface.initialize(() => {
        detailsChanged += 1;
      });

      assert.equal(panel.open, true);
      assert.equal(toggle.textContent, "收起内容");
      assert.equal(input.value, "alpha");
      assert.equal(surface.shouldCarryDetailsState(), true);
      assert.equal(surface.detailsValueForNavigation(), "alpha");
      assert.match(historyCalls[0], /open=alpha/);

      panel.open = false;
      panelListeners.toggle();

      assert.equal(toggle.textContent, "展开 Alpha");
      assert.equal(input.value, "");
      assert.equal(storage.get("iidashboard:alpha"), "closed");
      assert.equal(detailsChanged, 1);
      assert.equal(wrapSyncCount, 1);
      assert.match(historyCalls.at(-1), /open=/);
    },
  );
});

test("surface tables applies saved density and scroll affordances", () => {
  const buttonListeners = {};
  const cozyButton = {
    dataset: { tableDensityButton: "cozy" },
    classList: createClassList(),
    attributes: {},
    addEventListener(eventName, callback) {
      buttonListeners[`cozy:${eventName}`] = callback;
    },
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
  };
  const compactButton = {
    dataset: { tableDensityButton: "compact" },
    classList: createClassList(),
    attributes: {},
    addEventListener(eventName, callback) {
      buttonListeners[`compact:${eventName}`] = callback;
    },
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
  };
  const note = { hidden: true };
  const wrapListeners = {};
  const table = { id: "table" };
  const wrap = {
    scrollWidth: 220,
    clientWidth: 100,
    scrollLeft: 0,
    dataset: {},
    parentElement: {
      querySelector(selector) {
        return selector === "[data-table-scroll-note]" ? note : null;
      },
    },
    querySelector(selector) {
      return selector === "table" ? table : null;
    },
    addEventListener(eventName, callback) {
      wrapListeners[eventName] = callback;
    },
  };
  const storage = new Map([["density-key", "compact"]]);
  const observedTargets = [];
  const windowListeners = {};

  class FakeResizeObserver {
    constructor(callback) {
      this.callback = callback;
    }
    observe(target) {
      observedTargets.push(target);
    }
  }

  withFakeDom(
    {
      bodyDataset: { tableDensity: "cozy" },
      localStorage: {
        getItem(key) {
          return storage.get(key) ?? null;
        },
        setItem(key, value) {
          storage.set(key, value);
        },
      },
      resizeObserverImpl: FakeResizeObserver,
      addWindowListener(eventName, callback) {
        windowListeners[eventName] = callback;
      },
    },
    () => {
      const surface = createTableSurface({
        tableDensityButtons: [cozyButton, compactButton],
        tableWraps: [wrap],
        tableDensityStorageKey: "density-key",
      });

      surface.initialize();

      assert.equal(document.body.dataset.tableDensity, "compact");
      assert.equal(compactButton.attributes["aria-pressed"], "true");
      assert.equal(wrap.dataset.scrollable, "true");
      assert.equal(wrap.dataset.canScrollRight, "true");
      assert.equal(note.hidden, false);
      assert.deepEqual(observedTargets, [wrap, table]);
      assert.equal(typeof windowListeners.resize, "function");

      wrap.scrollLeft = 20;
      wrapListeners.scroll();
      assert.equal(wrap.dataset.canScrollLeft, "true");

      buttonListeners["cozy:click"]();
      assert.equal(document.body.dataset.tableDensity, "cozy");
      assert.equal(storage.get("density-key"), "cozy");
    },
  );
});

test("navigation ui helpers keep menu and dock state in sync", () => {
  const menuAttributes = {};
  const toggleAttributes = {};
  const sectionLink = {
    dataset: { waypointMenuKind: "section" },
    classList: createClassList(),
    attributes: {},
    getAttribute(name) {
      return name === "href" ? "#tracks" : null;
    },
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
    removeAttribute(name) {
      delete this.attributes[name];
    },
  };
  const trackLink = {
    dataset: { waypointMenuKind: "track" },
    classList: createClassList(["active"]),
    attributes: {},
    focused: false,
    getAttribute(name) {
      return name === "href" ? "#price_pressure_track" : null;
    },
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
    removeAttribute(name) {
      delete this.attributes[name];
    },
    focus() {
      this.focused = true;
    },
  };

  withFakeDom({ bodyDataset: {} }, () => {
    const ctx = {
      waypointMenu: {
        dataset: {},
        setAttribute(name, value) {
          menuAttributes[name] = value;
        },
      },
      waypointMenuBackdrop: { hidden: true },
      waypointMenuToggles: [
        {
          setAttribute(name, value) {
            toggleAttributes[name] = value;
          },
        },
      ],
      waypointMenuLinks: [sectionLink, trackLink],
      waypointDock: { dataset: {} },
      waypointTitle: { textContent: "" },
      waypointTopLabels: [{ textContent: "" }],
      waypointCopy: { textContent: "" },
      waypointPrevButton: { disabled: false, title: "" },
      waypointNextButton: { disabled: false, title: "" },
    };
    const waypoints = [
      { hash: "#overview", label: "总览", kind: "section", parent: "" },
      { hash: "#price_pressure_track", label: "价格压力", kind: "track", parent: "主线结果" },
      { hash: "#limits", label: "研究边界", kind: "section", parent: "" },
    ];

    setWaypointMenuOpen(ctx, true);
    assert.equal(document.body.dataset.waypointMenuOpen, "true");
    assert.equal(menuAttributes["aria-hidden"], "false");
    assert.equal(ctx.waypointMenuBackdrop.hidden, false);
    assert.equal(toggleAttributes["aria-expanded"], "true");
    assert.equal(trackLink.focused, true);

    syncWaypointMenuState(ctx, "#price_pressure_track", "#tracks");
    assert.equal(trackLink.attributes["aria-current"], "step");

    updateWaypointDock(
      ctx,
      waypoints,
      { item: waypoints[1], index: 1 },
      420,
    );
    assert.equal(ctx.waypointTitle.textContent, "主线结果 / 价格压力");
    assert.equal(ctx.waypointTopLabels[0].textContent, "主线结果 / 价格压力");
    assert.match(ctx.waypointCopy.textContent, /主线内部/);
    assert.equal(ctx.waypointPrevButton.disabled, false);
    assert.equal(ctx.waypointNextButton.disabled, false);
    assert.equal(ctx.waypointDock.dataset.visible, "true");
  });
});

test("refresh request helpers build urls and post payloads with dashboard headers", async () => {
  const fetchCalls = [];
  await withFakeDom(
    {
      origin: "http://localhost:5001",
      fetchImpl: async (url, options = {}) => {
        fetchCalls.push({ url, options });
        return {
          ok: true,
          async json() {
            return { status: "running" };
          },
        };
      },
    },
    async () => {
      const payload = await fetchRefreshStatus(
        {
          refreshStatusUrl: "/refresh/status",
          currentMode() {
            return "demo";
          },
          detailsQueryParam: "open",
        },
        {
          shouldCarryDetailsState() {
            return true;
          },
          detailsValueForNavigation() {
            return "alpha,beta";
          },
        },
        "framework",
      );

      assert.deepEqual(payload, { status: "running" });
      const statusUrl = new URL(fetchCalls[0].url);
      assert.equal(statusUrl.pathname, "/refresh/status");
      assert.equal(statusUrl.searchParams.get("mode"), "demo");
      assert.equal(statusUrl.searchParams.get("anchor"), "framework");
      assert.equal(statusUrl.searchParams.get("open"), "alpha,beta");
      assert.equal(fetchCalls[0].options.credentials, "same-origin");

      const formData = new FormData();
      formData.append("anchor", "overview");
      await postRefreshRequest({ action: "/refresh?mode=demo" }, formData);

      assert.equal(fetchCalls[1].url, "/refresh?mode=demo");
      assert.equal(fetchCalls[1].options.method, "POST");
      assert.equal(fetchCalls[1].options.headers["X-Requested-With"], "fetch");
      assert.equal(fetchCalls[1].options.credentials, "same-origin");
    },
  );
});

// ── verdict tier filter ──────────────────────────────────────────────


function makeChip(filter) {
  const listeners = new Map();
  const attrs = new Map([["data-filter", filter]]);
  const classes = new Set(filter === "all" ? ["is-active"] : []);
  return {
    listeners,
    attrs,
    classes,
    getAttribute(name) {
      return attrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      attrs.set(name, value);
    },
    addEventListener(event, handler) {
      listeners.set(event, handler);
    },
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
    },
    click() {
      const handler = listeners.get("click");
      if (handler) handler({ preventDefault: () => {} });
    },
  };
}


function makeGridAndNav(filterChipKinds = ["all", "支持", "证据不足"]) {
  const chips = filterChipKinds.map((kind) => makeChip(kind));
  const gridAttrs = new Map();
  const grid = {
    getAttribute(name) {
      return gridAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      gridAttrs.set(name, value);
    },
    removeAttribute(name) {
      gridAttrs.delete(name);
    },
  };
  const navAttrs = new Map();
  const nav = {
    getAttribute(name) {
      return navAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      navAttrs.set(name, value);
    },
    parentElement: {
      querySelector(selector) {
        return selector === ".cma-verdict-grid" ? grid : null;
      },
    },
    querySelectorAll(selector) {
      return selector === ".cma-verdict-filter-chip" ? chips : [];
    },
  };
  return { nav, chips, grid };
}


test("verdict filter chip click sets data-filter on grid and toggles active", () => {
  const { nav, chips, grid } = makeGridAndNav();
  const fakeDoc = {
    querySelectorAll(selector) {
      return selector === ".cma-verdict-filter" ? [nav] : [];
    },
    querySelector() {
      return null;
    },
  };
  const controller = createVerdictFilterController({ doc: fakeDoc });
  controller.initialize();

  // initial state — "all" chip is active, no filter on grid
  assert.equal(grid.getAttribute("data-filter"), null);
  assert.ok(chips[0].classList.contains("is-active"));

  // click "支持" chip
  chips[1].click();
  assert.equal(grid.getAttribute("data-filter"), "支持");
  assert.equal(nav.getAttribute("data-active"), "支持");
  assert.ok(!chips[0].classList.contains("is-active"));
  assert.ok(chips[1].classList.contains("is-active"));
  assert.ok(!chips[2].classList.contains("is-active"));

  // click "证据不足" chip
  chips[2].click();
  assert.equal(grid.getAttribute("data-filter"), "证据不足");
  assert.ok(chips[2].classList.contains("is-active"));
  assert.ok(!chips[1].classList.contains("is-active"));

  // click "all" chip — filter cleared
  chips[0].click();
  assert.equal(grid.getAttribute("data-filter"), null);
  assert.ok(chips[0].classList.contains("is-active"));
});


test("verdict filter initialize is a no-op when doc is null", () => {
  const controller = createVerdictFilterController({ doc: null });
  // Should not throw.
  controller.initialize();
});


test("verdict filter initialize is a no-op when no nav element exists", () => {
  const fakeDoc = {
    querySelectorAll() {
      return [];
    },
    querySelector() {
      return null;
    },
  };
  const controller = createVerdictFilterController({ doc: fakeDoc });
  controller.initialize();  // should not throw
});


function makeTrackCard(track) {
  const listeners = new Map();
  const attrs = new Map([["data-filter-track", track]]);
  const classes = new Set();
  const card = {
    listeners,
    attrs,
    classes,
    parentElement: null,  // assigned later when collecting siblings
    getAttribute(name) {
      return attrs.get(name) ?? null;
    },
    addEventListener(event, handler) {
      listeners.set(event, handler);
    },
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
    },
    click() {
      const handler = listeners.get("click");
      if (handler) handler({ preventDefault: () => {} });
    },
  };
  return card;
}


function makeTrackCardsWithGrid() {
  const cards = [
    makeTrackCard("price_pressure"),
    makeTrackCard("demand_curve"),
    makeTrackCard("identification"),
  ];
  const parent = {
    querySelectorAll(selector) {
      return selector === ".cma-track-card" ? cards : [];
    },
  };
  cards.forEach((card) => {
    card.parentElement = parent;
  });
  const gridAttrs = new Map();
  const grid = {
    getAttribute(name) {
      return gridAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      gridAttrs.set(name, value);
    },
    removeAttribute(name) {
      gridAttrs.delete(name);
    },
  };
  return { cards, grid };
}


test("track summary card click sets data-filter-track on grid + activates that card only", () => {
  const { cards, grid } = makeTrackCardsWithGrid();
  const fakeDoc = {
    querySelectorAll(selector) {
      if (selector === ".cma-verdict-filter") return [];
      if (selector === ".cma-track-card") return cards;
      return [];
    },
    querySelector(selector) {
      if (selector === ".cma-verdict-grid") return grid;
      return null;
    },
  };
  const controller = createVerdictFilterController({ doc: fakeDoc });
  controller.initialize();

  // initial state: no track filter
  assert.equal(grid.getAttribute("data-filter-track"), null);

  // click identification
  cards[2].click();
  assert.equal(grid.getAttribute("data-filter-track"), "identification");
  assert.ok(cards[2].classList.contains("is-active"));
  assert.ok(!cards[0].classList.contains("is-active"));
  assert.ok(!cards[1].classList.contains("is-active"));

  // click price_pressure switches active card
  cards[0].click();
  assert.equal(grid.getAttribute("data-filter-track"), "price_pressure");
  assert.ok(cards[0].classList.contains("is-active"));
  assert.ok(!cards[2].classList.contains("is-active"));

  // click the same card again toggles off
  cards[0].click();
  assert.equal(grid.getAttribute("data-filter-track"), null);
  assert.ok(!cards[0].classList.contains("is-active"));
});


test("track filter and tier filter coexist independently on the same grid", () => {
  // Verify both filters can be active simultaneously and CSS resolves
  // their intersection (we can't test CSS here, just the attribute
  // state), demonstrating the two controllers don't interfere.
  const { cards, grid } = makeTrackCardsWithGrid();
  const tierChip = makeChip("证据不足");
  const allChip = makeChip("all");
  const tierNavAttrs = new Map();
  const tierNav = {
    getAttribute(name) {
      return tierNavAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      tierNavAttrs.set(name, value);
    },
    parentElement: {
      querySelector(selector) {
        return selector === ".cma-verdict-grid" ? grid : null;
      },
    },
    querySelectorAll(selector) {
      return selector === ".cma-verdict-filter-chip"
        ? [allChip, tierChip]
        : [];
    },
  };
  const fakeDoc = {
    querySelectorAll(selector) {
      if (selector === ".cma-verdict-filter") return [tierNav];
      if (selector === ".cma-track-card") return cards;
      return [];
    },
    querySelector(selector) {
      return selector === ".cma-verdict-grid" ? grid : null;
    },
  };
  const controller = createVerdictFilterController({ doc: fakeDoc });
  controller.initialize();

  cards[2].click();  // identification track active
  tierChip.click();  // 证据不足 tier active
  assert.equal(grid.getAttribute("data-filter-track"), "identification");
  assert.equal(grid.getAttribute("data-filter"), "证据不足");

  // toggle track off — tier filter remains
  cards[2].click();
  assert.equal(grid.getAttribute("data-filter-track"), null);
  assert.equal(grid.getAttribute("data-filter"), "证据不足");
});


// ── sensitivity threshold filter ─────────────────────────────────────


function makeSensitivityChip(threshold, isInitiallyActive = false) {
  const listeners = new Map();
  const attrs = new Map([["data-threshold", threshold]]);
  const classes = new Set(isInitiallyActive ? ["is-active"] : []);
  return {
    listeners,
    attrs,
    classes,
    getAttribute(name) {
      return attrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      attrs.set(name, value);
    },
    addEventListener(event, handler) {
      listeners.set(event, handler);
    },
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
    },
    click() {
      const handler = listeners.get("click");
      if (handler) handler({ preventDefault: () => {} });
    },
  };
}


function makeVerdictCard({ pValue }) {
  // pValue: a string like "0.012", or "" for NaN/non-p hypotheses.
  const stripAttrs = new Map([["data-sensitivity", "not_sig"]]);
  const iconEl = { textContent: "—" };
  const textEl = { textContent: "" };
  const strip = {
    getAttribute(name) {
      return stripAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      stripAttrs.set(name, value);
    },
    querySelector(selector) {
      if (selector === ".cma-verdict-sensitivity-icon") return iconEl;
      if (selector === ".cma-verdict-sensitivity-text") return textEl;
      return null;
    },
  };
  const cardAttrs = new Map([["data-p-value", pValue]]);
  return {
    cardAttrs,
    strip,
    stripAttrs,
    iconEl,
    textEl,
    getAttribute(name) {
      return cardAttrs.get(name) ?? null;
    },
    querySelector(selector) {
      return selector === ".cma-verdict-sensitivity" ? strip : null;
    },
  };
}


function makeSensitivityFixture(pValues = ["0.012", "0.6396", ""]) {
  const chips = [
    makeSensitivityChip("0.05"),
    makeSensitivityChip("0.10", true),  // initial server-rendered active
    makeSensitivityChip("0.20"),
  ];
  const cards = pValues.map((p) => makeVerdictCard({ pValue: p }));
  const gridAttrs = new Map([["data-sensitivity-threshold", "0.10"]]);
  const grid = {
    getAttribute(name) {
      return gridAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      gridAttrs.set(name, value);
    },
    querySelectorAll(selector) {
      return selector === ".cma-verdict-card" ? cards : [];
    },
  };
  const navAttrs = new Map([["data-active", "0.10"]]);
  const nav = {
    getAttribute(name) {
      return navAttrs.get(name) ?? null;
    },
    setAttribute(name, value) {
      navAttrs.set(name, value);
    },
    parentElement: {
      querySelector(selector) {
        return selector === ".cma-verdict-grid" ? grid : null;
      },
    },
    querySelectorAll(selector) {
      return selector === ".cma-sensitivity-threshold-chip" ? chips : [];
    },
  };
  const fakeDoc = {
    querySelectorAll(selector) {
      return selector === ".cma-sensitivity-threshold-filter" ? [nav] : [];
    },
    querySelector() {
      return null;
    },
  };
  return { nav, chips, grid, cards, fakeDoc };
}


test("sensitivity threshold chip click flips p-gated cards between sig and not_sig", () => {
  const { fakeDoc, chips, grid, cards } = makeSensitivityFixture();
  const controller = createSensitivityThresholdController({ doc: fakeDoc });
  controller.initialize();

  // Click 0.05 — H1 (p=0.012) flips to sig, H4 (p=0.6396) stays not_sig,
  // NaN card stays na.
  chips[0].click();
  assert.equal(grid.getAttribute("data-sensitivity-threshold"), "0.05");
  assert.ok(chips[0].classList.contains("is-active"));
  assert.ok(!chips[1].classList.contains("is-active"));

  assert.equal(cards[0].stripAttrs.get("data-sensitivity"), "sig");
  assert.equal(cards[0].iconEl.textContent, "✓");
  assert.match(cards[0].textEl.textContent, /显著.*p=0\.012/);

  assert.equal(cards[1].stripAttrs.get("data-sensitivity"), "not_sig");
  assert.equal(cards[1].iconEl.textContent, "—");
  assert.match(cards[1].textEl.textContent, /不显著.*p=0\.640/);

  assert.equal(cards[2].stripAttrs.get("data-sensitivity"), "na");
  assert.equal(cards[2].iconEl.textContent, "·");
  assert.match(cards[2].textEl.textContent, /不在 sweep 范围/);
});


test("sensitivity threshold click to 0.20 marks H1 + H4 sig but H4 still not at p=0.6396", () => {
  // H4 has p=0.6396 → still > 0.20 so not sig.
  const { fakeDoc, chips, cards } = makeSensitivityFixture();
  const controller = createSensitivityThresholdController({ doc: fakeDoc });
  controller.initialize();

  chips[2].click();  // 0.20
  assert.equal(cards[0].stripAttrs.get("data-sensitivity"), "sig");
  assert.equal(cards[1].stripAttrs.get("data-sensitivity"), "not_sig");
  assert.equal(cards[2].stripAttrs.get("data-sensitivity"), "na");
});


test("sensitivity threshold initialize is a no-op when doc is null", () => {
  const controller = createSensitivityThresholdController({ doc: null });
  controller.initialize();  // should not throw
});


test("sensitivity threshold initialize is a no-op when no nav element exists", () => {
  const fakeDoc = {
    querySelectorAll() {
      return [];
    },
    querySelector() {
      return null;
    },
  };
  const controller = createSensitivityThresholdController({ doc: fakeDoc });
  controller.initialize();  // should not throw
});


test("sensitivity threshold marks malformed data-p-value as na rather than silently sig", () => {
  // Defensive case: data-p-value = "abc" → can't parse → strip should
  // be na, not silently flipped to sig.
  const { fakeDoc, chips, cards } = makeSensitivityFixture(["abc"]);
  const controller = createSensitivityThresholdController({ doc: fakeDoc });
  controller.initialize();
  chips[0].click();
  assert.equal(cards[0].stripAttrs.get("data-sensitivity"), "na");
});
