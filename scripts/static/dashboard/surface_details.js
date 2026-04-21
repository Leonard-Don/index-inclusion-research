export function createDetailsSurface(ctx, tableSurface) {
  const detailsKeySet = new Set(
    ctx.detailsPanels
      .map((panel) => panel.dataset.detailsKey || "")
      .filter(Boolean),
  );

  function detailsStorageKey(panel) {
    return `iidashboard:${panel.dataset.detailsKey}`;
  }

  function parseDetailsKeys(rawValue) {
    const seen = new Set();
    return String(rawValue || "")
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item && detailsKeySet.has(item) && !seen.has(item) && seen.add(item));
  }

  function currentOpenDetailsKeys() {
    return ctx.detailsPanels
      .filter((panel) => panel.open && panel.dataset.detailsKey)
      .map((panel) => panel.dataset.detailsKey);
  }

  function serializedOpenDetailsKeys(keys = currentOpenDetailsKeys()) {
    return keys.join(",");
  }

  function currentSearchDetailsValue() {
    const params = new URLSearchParams(window.location.search);
    if (!params.has(ctx.detailsQueryParam)) {
      return null;
    }
    return params.get(ctx.detailsQueryParam) || "";
  }

  function shouldCarryDetailsState() {
    return currentSearchDetailsValue() !== null || currentOpenDetailsKeys().length > 0;
  }

  function detailsValueForNavigation() {
    if (ctx.detailsPanels.length) {
      return serializedOpenDetailsKeys();
    }
    const currentValue = currentSearchDetailsValue();
    return currentValue === null ? "" : currentValue;
  }

  function syncDetailsInputs() {
    const currentValue = ctx.detailsPanels.length
      ? serializedOpenDetailsKeys()
      : (currentSearchDetailsValue() || "");
    ctx.detailsOpenInputs.forEach((input) => {
      input.value = currentValue;
    });
  }

  function syncDetailsQuery(forcePresence = false) {
    syncDetailsInputs();
    if (!ctx.detailsPanels.length) {
      return;
    }
    if (!forcePresence && currentSearchDetailsValue() === null) {
      return;
    }
    const url = new URL(window.location.href);
    url.searchParams.set(ctx.detailsQueryParam, serializedOpenDetailsKeys());
    history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
  }

  function updateDetailsToggle(panel) {
    const toggle = panel.querySelector("[data-details-toggle]");
    if (!toggle) {
      return;
    }
    if (!toggle.dataset.closedLabel) {
      toggle.dataset.closedLabel = toggle.textContent.trim();
    }
    toggle.textContent = panel.open ? "收起内容" : toggle.dataset.closedLabel;
  }

  function initializeDetailsPanels(onDetailsStateChange) {
    const currentSearchValue = currentSearchDetailsValue();
    const hasSearchDetailsState = currentSearchValue !== null;
    const openKeysFromSearch = new Set(parseDetailsKeys(currentSearchValue || ""));
    ctx.detailsPanels.forEach((panel) => {
      const toggle = panel.querySelector("[data-details-toggle]");
      const detailsKey = panel.dataset.detailsKey || "";
      if (toggle && !toggle.dataset.closedLabel) {
        toggle.dataset.closedLabel = toggle.textContent.trim();
      }
      if (hasSearchDetailsState) {
        panel.open = detailsKey ? openKeysFromSearch.has(detailsKey) : false;
      } else {
        try {
          const saved = window.localStorage.getItem(detailsStorageKey(panel));
          if (saved === "open") {
            panel.open = true;
          }
          if (saved === "closed") {
            panel.open = false;
          }
        } catch (error) {
          void error;
        }
      }
      updateDetailsToggle(panel);
      panel.addEventListener("toggle", () => {
        updateDetailsToggle(panel);
        try {
          window.localStorage.setItem(detailsStorageKey(panel), panel.open ? "open" : "closed");
        } catch (error) {
          void error;
        }
        syncDetailsQuery(true);
        onDetailsStateChange();
        requestAnimationFrame(() => {
          tableSurface.syncAllTableWraps();
        });
      });
    });
    syncDetailsQuery(hasSearchDetailsState);
  }

  return {
    initialize(onDetailsStateChange) {
      initializeDetailsPanels(onDetailsStateChange || (() => {}));
    },
    shouldCarryDetailsState,
    detailsValueForNavigation,
    syncDetailsInputs,
  };
}
