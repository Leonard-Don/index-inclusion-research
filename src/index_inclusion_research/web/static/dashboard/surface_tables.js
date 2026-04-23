export function createTableSurface(ctx) {
  let syncAllTableWraps = () => {};

  function setTableDensity(nextDensity, persist = true) {
    const density = nextDensity === "compact" ? "compact" : "cozy";
    document.body.dataset.tableDensity = density;
    ctx.tableDensityButtons.forEach((button) => {
      const active = button.dataset.tableDensityButton === density;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    if (!persist) {
      return;
    }
    try {
      window.localStorage.setItem(ctx.tableDensityStorageKey, density);
    } catch (error) {
      void error;
    }
  }

  function initializeTableDensity() {
    let density = document.body.dataset.tableDensity || "cozy";
    try {
      const savedDensity = window.localStorage.getItem(ctx.tableDensityStorageKey);
      if (savedDensity === "compact" || savedDensity === "cozy") {
        density = savedDensity;
      }
    } catch (error) {
      void error;
    }
    setTableDensity(density, false);
    ctx.tableDensityButtons.forEach((button) => {
      button.addEventListener("click", () => {
        setTableDensity(button.dataset.tableDensityButton || "cozy");
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

  function initializeTableWraps() {
    syncAllTableWraps = () => {
      ctx.tableWraps.forEach((wrap) => {
        syncTableWrapState(wrap);
      });
    };
    if (!ctx.tableWraps.length) {
      return;
    }
    ctx.tableWraps.forEach((wrap) => {
      wrap.addEventListener(
        "scroll",
        () => {
          syncTableWrapState(wrap);
        },
        { passive: true },
      );
    });
    window.addEventListener("resize", syncAllTableWraps);
    window.addEventListener("load", syncAllTableWraps);
    if ("ResizeObserver" in window) {
      const observer = new ResizeObserver(() => {
        syncAllTableWraps();
      });
      ctx.tableWraps.forEach((wrap) => {
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

  return {
    initialize() {
      initializeTableDensity();
      initializeTableWraps();
    },
    syncAllTableWraps() {
      syncAllTableWraps();
    },
  };
}
