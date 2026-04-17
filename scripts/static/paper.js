const viewButtons = document.querySelectorAll("[data-view-target]");
const viewPanels = document.querySelectorAll("[data-view-name]");
const tableDensityButtons = document.querySelectorAll("[data-table-density-button]");
const tableWraps = document.querySelectorAll(".table-wrap");
const tableDensityStorageKey = "index-inclusion:table-density";
const availableViews = Array.from(viewPanels, (panel) => panel.getAttribute("data-view-name")).filter(Boolean);
const defaultView = availableViews[0] || "camp";

function setTableDensity(nextDensity, persist = true) {
  const density = nextDensity === "compact" ? "compact" : "cozy";
  document.body.dataset.tableDensity = density;
  tableDensityButtons.forEach((button) => {
    const active = button.getAttribute("data-table-density-button") === density;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
  if (!persist) {
    return;
  }
  try {
    window.localStorage.setItem(tableDensityStorageKey, density);
  } catch (error) {
    // Ignore persistence failures in restricted storage environments.
  }
}

function initializeTableDensity() {
  let density = document.body.dataset.tableDensity || "cozy";
  try {
    const savedDensity = window.localStorage.getItem(tableDensityStorageKey);
    if (savedDensity === "compact" || savedDensity === "cozy") {
      density = savedDensity;
    }
  } catch (error) {
    // Ignore storage failures and keep the default.
  }
  setTableDensity(density, false);
  tableDensityButtons.forEach((button) => {
    button.addEventListener("click", () => {
      setTableDensity(button.getAttribute("data-table-density-button") || "cozy");
      requestAnimationFrame(() => {
        syncAllTableWraps();
      });
    });
  });
}

function syncTableWrapState(wrap) {
  const scrollable = wrap.scrollWidth > wrap.clientWidth + 4;
  const canScrollLeft = wrap.scrollLeft > 8;
  const canScrollRight = scrollable && wrap.scrollLeft < wrap.scrollWidth - wrap.clientWidth - 8;
  wrap.dataset.scrollable = scrollable ? "true" : "false";
  wrap.dataset.canScrollLeft = canScrollLeft ? "true" : "false";
  wrap.dataset.canScrollRight = canScrollRight ? "true" : "false";
  const note = wrap.parentElement?.querySelector("[data-table-scroll-note]");
  if (note) {
    note.hidden = !scrollable;
  }
}

function syncAllTableWraps() {
  tableWraps.forEach((wrap) => {
    syncTableWrapState(wrap);
  });
}

function initializeTableWraps() {
  if (!tableWraps.length) {
    return;
  }
  tableWraps.forEach((wrap) => {
    wrap.addEventListener("scroll", () => {
      syncTableWrapState(wrap);
    }, { passive: true });
  });
  window.addEventListener("resize", syncAllTableWraps);
  window.addEventListener("load", syncAllTableWraps);
  if ("ResizeObserver" in window) {
    const observer = new ResizeObserver(() => {
      syncAllTableWraps();
    });
    tableWraps.forEach((wrap) => {
      observer.observe(wrap);
      const table = wrap.querySelector("table");
      if (table) {
        observer.observe(table);
      }
    });
  }
  requestAnimationFrame(() => {
    syncAllTableWraps();
  });
}

function activateView(target, updateUrl = true) {
  const nextView = availableViews.includes(target) ? target : defaultView;

  viewButtons.forEach((button) => {
    const active = button.getAttribute("data-view-target") === nextView;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });

  viewPanels.forEach((panel) => {
    const active = panel.getAttribute("data-view-name") === nextView;
    panel.classList.toggle("active", active);
    panel.hidden = !active;
    panel.setAttribute("aria-hidden", active ? "false" : "true");
  });

  if (!updateUrl) {
    return;
  }

  const url = new URL(window.location.href);
  if (nextView === defaultView) {
    url.searchParams.delete("view");
  } else {
    url.searchParams.set("view", nextView);
  }
  window.history.replaceState(null, "", url);
}

const initialView = new URLSearchParams(window.location.search).get("view") || defaultView;
initializeTableDensity();
initializeTableWraps();
activateView(initialView, false);

viewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.getAttribute("data-view-target");
    activateView(target);
  });
});

window.addEventListener("popstate", () => {
  const target = new URLSearchParams(window.location.search).get("view") || defaultView;
  activateView(target, false);
});
