import {
  applyReadingProgress,
  candidateWaypointFromScroll,
  collectWaypoints,
  computeReadingProgress,
  currentWaypointForHash,
  normalizeHashForAllowedSet,
} from "./navigation_helpers.js";
import {
  setWaypointMenuOpen as setWaypointMenuOpenUi,
  syncWaypointMenuState as syncWaypointMenuStateUi,
  updateWaypointDock as updateWaypointDockUi,
} from "./navigation_ui.js";

export function createNavigationController(ctx, surface) {
  const waypoints = collectWaypoints(ctx);

  let liveHash = window.location.hash || "#overview";
  let pendingHash = "";
  let pendingHashTimer = null;
  let waypointScrollFrame = null;

  function currentHash() {
    return liveHash || window.location.hash || "#overview";
  }

  function currentSectionHash() {
    const hash = currentHash();
    return ctx.aliasMap.get(hash) || hash;
  }

  function updateReadingProgress() {
    const { progress, progressValue } = computeReadingProgress();
    applyReadingProgress(ctx, progress, progressValue);
  }

  function setWaypointMenuOpen(nextOpen) {
    setWaypointMenuOpenUi(ctx, nextOpen);
  }

  function syncWaypointMenuState() {
    syncWaypointMenuStateUi(ctx, currentHash(), currentSectionHash());
  }

  function clearPendingHash() {
    if (pendingHashTimer !== null) {
      window.clearTimeout(pendingHashTimer);
      pendingHashTimer = null;
    }
    pendingHash = "";
  }

  function setPendingHash(hash) {
    clearPendingHash();
    pendingHash = hash;
    pendingHashTimer = window.setTimeout(() => {
      pendingHash = "";
      pendingHashTimer = null;
      scheduleWaypointSync();
    }, 1800);
  }

  function updateWaypointDock() {
    updateWaypointDockUi(
      ctx,
      waypoints,
      currentWaypointForHash(waypoints, currentHash()),
      window.scrollY,
    );
  }

  function scrollToHash(hash) {
    const target = hash ? document.querySelector(hash) : null;
    if (!target) {
      return;
    }
    setPendingHash(hash);
    liveHash = hash;
    history.replaceState(null, "", `${window.location.pathname}${window.location.search}${hash}`);
    syncTopbarState();
    target.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  function syncWaypointFromScroll() {
    waypointScrollFrame = null;
    updateReadingProgress();
    if (!waypoints.length) {
      updateWaypointDock();
      return;
    }
    const candidate = candidateWaypointFromScroll(waypoints);
    if (pendingHash) {
      if (candidate && candidate.hash === pendingHash) {
        clearPendingHash();
      } else {
        updateWaypointDock();
        return;
      }
    }
    if (candidate && candidate.hash !== currentHash()) {
      liveHash = candidate.hash;
      history.replaceState(
        null,
        "",
        `${window.location.pathname}${window.location.search}${candidate.hash}`,
      );
      syncTopbarState();
      return;
    }
    updateWaypointDock();
  }

  function scheduleWaypointSync() {
    if (waypointScrollFrame !== null) {
      return;
    }
    waypointScrollFrame = window.requestAnimationFrame(syncWaypointFromScroll);
  }

  function stabilizeInitialHash() {
    const initialHash = window.location.hash;
    const target = initialHash ? document.querySelector(initialHash) : null;
    if (!initialHash || !target) {
      return;
    }
    setPendingHash(initialHash);
    liveHash = initialHash;
    window.requestAnimationFrame(() => {
      target.scrollIntoView({ block: "start" });
      scheduleWaypointSync();
    });
  }

  function syncTopbarState() {
    const activeModeLink =
      ctx.modeLinks.find((link) => link.classList.contains("active")) ||
      ctx.modeLinks[0];
    const activeAllowedHashes = ((activeModeLink && activeModeLink.dataset.allowedHashes) || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    const activeDefaultHash = (activeModeLink && activeModeLink.dataset.defaultHash) || "#overview";
    const rawHash = currentHash();
    let hash = normalizeHashForAllowedSet(rawHash, activeAllowedHashes, activeDefaultHash);
    if (hash !== rawHash) {
      liveHash = hash;
      history.replaceState(null, "", `${window.location.pathname}${window.location.search}${hash}`);
      const target = document.querySelector(hash);
      if (target) {
        requestAnimationFrame(() => {
          target.scrollIntoView({ block: "start" });
        });
      }
    }

    ctx.sectionLinks.forEach((link) => {
      const linkHash = link.getAttribute("href");
      const normalizedSectionHash = ctx.aliasMap.get(hash) || hash;
      const active = linkHash === normalizedSectionHash;
      link.classList.toggle("active", active);
      if (active) {
        link.setAttribute("aria-current", "location");
      } else {
        link.removeAttribute("aria-current");
      }
    });

    ctx.modeLinks.forEach((link) => {
      if (link.classList.contains("active")) {
        link.setAttribute("aria-current", "page");
      } else {
        link.removeAttribute("aria-current");
      }
      const baseHref = link.dataset.baseHref || "/";
      const nextUrl = new URL(baseHref, window.location.origin);
      const allowedHashes = (link.dataset.allowedHashes || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
      const defaultHash = link.dataset.defaultHash || "#overview";
      const nextHash = normalizeHashForAllowedSet(hash, allowedHashes, defaultHash);
      if (surface.shouldCarryDetailsState()) {
        nextUrl.searchParams.set(ctx.detailsQueryParam, surface.detailsValueForNavigation());
      } else {
        nextUrl.searchParams.delete(ctx.detailsQueryParam);
      }
      link.setAttribute("href", `${nextUrl.pathname}${nextUrl.search}${nextHash}`);
    });

    ctx.anchorInputs.forEach((input) => {
      input.value = hash.replace(/^#/, "") || "overview";
    });
    surface.syncDetailsInputs();
    syncWaypointMenuState();
    updateWaypointDock();
  }

  function bindHashLink(link, callback) {
    link.addEventListener("click", (event) => {
      const hash = link.getAttribute("href") || "";
      if (
        event.button !== 0 ||
        event.metaKey ||
        event.ctrlKey ||
        event.shiftKey ||
        event.altKey ||
        !hash.startsWith("#")
      ) {
        return;
      }
      event.preventDefault();
      callback(hash);
    });
  }

  return {
    currentHash,
    currentSectionHash,
    setWaypointMenuOpen,
    syncTopbarState,
    scrollToHash,
    initialize() {
      ctx.sectionLinks.forEach((link) => {
        bindHashLink(link, scrollToHash);
      });

      ctx.waypointMenuToggles.forEach((button) => {
        button.addEventListener("click", () => {
          const isOpen = (ctx.waypointMenu && ctx.waypointMenu.dataset.open === "true") || false;
          setWaypointMenuOpen(!isOpen);
        });
      });

      ctx.waypointMenuCloseButtons.forEach((button) => {
        button.addEventListener("click", () => {
          setWaypointMenuOpen(false);
        });
      });

      if (ctx.waypointMenuBackdrop) {
        ctx.waypointMenuBackdrop.addEventListener("click", () => {
          setWaypointMenuOpen(false);
        });
      }

      ctx.waypointMenuLinks.forEach((link) => {
        bindHashLink(link, (hash) => {
          setWaypointMenuOpen(false);
          scrollToHash(hash);
        });
      });

      if (ctx.waypointPrevButton) {
        ctx.waypointPrevButton.addEventListener("click", () => {
          const { index } = currentWaypointForHash(waypoints, currentHash());
          if (index > 0) {
            scrollToHash(waypoints[index - 1].hash);
          }
        });
      }

      if (ctx.waypointNextButton) {
        ctx.waypointNextButton.addEventListener("click", () => {
          const { index } = currentWaypointForHash(waypoints, currentHash());
          if (index >= 0 && index < waypoints.length - 1) {
            scrollToHash(waypoints[index + 1].hash);
          }
        });
      }

      if (ctx.waypointTopButton) {
        ctx.waypointTopButton.addEventListener("click", () => {
          scrollToHash("#overview");
        });
      }

      updateReadingProgress();
      syncTopbarState();
      stabilizeInitialHash();
      scheduleWaypointSync();

      window.addEventListener("hashchange", () => {
        setWaypointMenuOpen(false);
        clearPendingHash();
        liveHash = window.location.hash || "#overview";
        syncTopbarState();
        scheduleWaypointSync();
      });
      window.addEventListener("scroll", scheduleWaypointSync, { passive: true });
      window.addEventListener("resize", scheduleWaypointSync);
      window.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          setWaypointMenuOpen(false);
        }
      });
    },
  };
}
