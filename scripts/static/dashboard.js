    (() => {
      const sectionLinks = Array.from(document.querySelectorAll("[data-section-link]"));
      const modeLinks = Array.from(document.querySelectorAll("[data-mode-link]"));
      const anchorInputs = Array.from(document.querySelectorAll("[data-anchor-input]"));
      const refreshForms = Array.from(document.querySelectorAll("[data-refresh-form]"));
      const refreshButtons = Array.from(document.querySelectorAll("[data-refresh-button]"));
      const detailsOpenInputs = Array.from(document.querySelectorAll("[data-open-input]"));
      const refreshPanel = document.querySelector("[data-refresh-panel]");
      const refreshSnapshotLabel = document.querySelector("[data-refresh-snapshot-label]");
      const refreshSnapshotCopy = document.querySelector("[data-refresh-snapshot-copy]");
      const refreshNote = document.querySelector("[data-refresh-note]");
      const detailsPanels = Array.from(document.querySelectorAll("[data-details-key]"));
      const waypointElements = Array.from(document.querySelectorAll("[data-waypoint]"));
      const waypointDock = document.querySelector("[data-waypoint-dock]");
      const waypointTitle = document.querySelector("[data-waypoint-title]");
      const waypointCopy = document.querySelector("[data-waypoint-copy]");
      const waypointPrevButton = document.querySelector("[data-waypoint-prev]");
      const waypointNextButton = document.querySelector("[data-waypoint-next]");
      const waypointTopButton = document.querySelector("[data-waypoint-top]");
      const waypointMenu = document.querySelector("[data-waypoint-menu]");
      const waypointMenuBackdrop = document.querySelector("[data-waypoint-menu-backdrop]");
      const waypointMenuToggles = Array.from(document.querySelectorAll("[data-waypoint-menu-toggle]"));
      const waypointMenuCloseButtons = Array.from(document.querySelectorAll("[data-waypoint-menu-close]"));
      const waypointMenuLinks = Array.from(document.querySelectorAll("[data-waypoint-menu-link]"));
      const readingProgress = document.querySelector("[data-reading-progress]");
      const readingProgressBar = document.querySelector("[data-reading-progress-bar]");
      const readingProgressLabels = Array.from(document.querySelectorAll("[data-reading-progress-label]"));
      const refreshStatusUrl = document.body.dataset.refreshStatusUrl || "";
      const tableDensityButtons = Array.from(document.querySelectorAll("[data-table-density-button]"));
      const tableWraps = Array.from(document.querySelectorAll(".table-wrap"));
      const tableDensityStorageKey = "index-inclusion:table-density";
      const detailsQueryParam = "open";
      const detailsKeySet = new Set(
        detailsPanels
          .map((panel) => panel.dataset.detailsKey || "")
          .filter(Boolean),
      );
      const aliasMap = new Map([
        ["#price_pressure_track", "#tracks"],
        ["#demand_curve_track", "#tracks"],
        ["#identification_china_track", "#tracks"],
      ]);
      let refreshPollTimer = null;
      let refreshRuntimeTimer = null;
      let lastRefreshPayload = null;
      let liveHash = window.location.hash || "#overview";
      let pendingHash = "";
      let pendingHashTimer = null;
      let waypointScrollFrame = null;
      let syncAllTableWraps = () => {};

      function currentHash() {
        return liveHash || window.location.hash || "#overview";
      }

      function currentSectionHash() {
        const hash = currentHash();
        return aliasMap.get(hash) || hash;
      }

      function updateReadingProgress() {
        const root = document.documentElement;
        const maxScroll = Math.max(0, root.scrollHeight - window.innerHeight);
        const progress = maxScroll <= 0 ? 0 : Math.min(1, Math.max(0, window.scrollY / maxScroll));
        const progressValue = Math.round(progress * 100);
        if (readingProgressBar) {
          readingProgressBar.style.transform = `scaleX(${progress})`;
        }
        if (readingProgress) {
          readingProgress.setAttribute("aria-valuenow", String(progressValue));
          readingProgress.setAttribute("aria-valuetext", `已浏览 ${progressValue}%`);
        }
        readingProgressLabels.forEach((label) => {
          label.textContent = `${progressValue}%`;
        });
      }

      function setWaypointMenuOpen(nextOpen) {
        const open = Boolean(nextOpen);
        document.body.dataset.waypointMenuOpen = open ? "true" : "false";
        if (waypointMenu) {
          waypointMenu.dataset.open = open ? "true" : "false";
          waypointMenu.setAttribute("aria-hidden", open ? "false" : "true");
        }
        if (waypointMenuBackdrop) {
          waypointMenuBackdrop.hidden = !open;
        }
        waypointMenuToggles.forEach((button) => {
          button.setAttribute("aria-expanded", open ? "true" : "false");
        });
        if (!open) {
          return;
        }
        const activeLink =
          waypointMenuLinks.find((link) => link.classList.contains("active")) ||
          waypointMenuLinks[0];
        if (activeLink) {
          requestAnimationFrame(() => {
            activeLink.focus();
          });
        }
      }

      function syncWaypointMenuState() {
        const hash = currentHash();
        const sectionHash = currentSectionHash();
        waypointMenuLinks.forEach((link) => {
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

      function currentMode() {
        const params = new URLSearchParams(window.location.search);
        return params.get("mode") || "demo";
      }

      function formatDuration(seconds) {
        const total = Math.max(0, Math.floor(Number(seconds) || 0));
        const minutes = Math.floor(total / 60);
        const remainder = total % 60;
        if (minutes <= 0) {
          return `${remainder} 秒`;
        }
        return `${minutes} 分 ${remainder.toString().padStart(2, "0")} 秒`;
      }

      function refreshRuntimeCopy(payload) {
        const status = (payload && payload.status) || "idle";
        const message =
          (payload && payload.message) ||
          "页面已就绪，可在不离开当前位置的情况下刷新最新结果。";
        const durationSeconds = payload && payload.duration_seconds;
        if (status === "running" && durationSeconds != null) {
          return `${message} 已运行 ${formatDuration(durationSeconds)}。`;
        }
        if ((status === "succeeded" || status === "failed") && durationSeconds != null) {
          return `${message} 总耗时约 ${formatDuration(durationSeconds)}。`;
        }
        return message;
      }

      function stopRefreshRuntimeTimer() {
        if (refreshRuntimeTimer !== null) {
          window.clearInterval(refreshRuntimeTimer);
          refreshRuntimeTimer = null;
        }
      }

      function startRefreshRuntimeTimer() {
        stopRefreshRuntimeTimer();
        refreshRuntimeTimer = window.setInterval(() => {
          if (!lastRefreshPayload || lastRefreshPayload.status !== "running") {
            stopRefreshRuntimeTimer();
            return;
          }
          const startedTs = Number(lastRefreshPayload.started_ts || 0);
          if (startedTs > 0) {
            lastRefreshPayload.duration_seconds = Math.max(
              0,
              Math.floor(Date.now() / 1000 - startedTs),
            );
          }
          if (refreshNote) {
            refreshNote.textContent = refreshRuntimeCopy(lastRefreshPayload);
          }
        }, 1000);
      }

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
        return detailsPanels
          .filter((panel) => panel.open && panel.dataset.detailsKey)
          .map((panel) => panel.dataset.detailsKey);
      }

      function serializedOpenDetailsKeys(keys = currentOpenDetailsKeys()) {
        return keys.join(",");
      }

      function currentSearchDetailsValue() {
        const params = new URLSearchParams(window.location.search);
        if (!params.has(detailsQueryParam)) {
          return null;
        }
        return params.get(detailsQueryParam) || "";
      }

      function shouldCarryDetailsState() {
        return currentSearchDetailsValue() !== null || currentOpenDetailsKeys().length > 0;
      }

      function detailsValueForNavigation() {
        if (detailsPanels.length) {
          return serializedOpenDetailsKeys();
        }
        const currentValue = currentSearchDetailsValue();
        return currentValue === null ? "" : currentValue;
      }

      function syncDetailsInputs() {
        const currentValue = detailsPanels.length
          ? serializedOpenDetailsKeys()
          : (currentSearchDetailsValue() || "");
        detailsOpenInputs.forEach((input) => {
          input.value = currentValue;
        });
      }

      function syncDetailsQuery(forcePresence = false) {
        syncDetailsInputs();
        if (!detailsPanels.length) {
          return;
        }
        if (!forcePresence && currentSearchDetailsValue() === null) {
          return;
        }
        const url = new URL(window.location.href);
        url.searchParams.set(detailsQueryParam, serializedOpenDetailsKeys());
        history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
      }

      function setTableDensity(nextDensity, persist = true) {
        const density = nextDensity === "compact" ? "compact" : "cozy";
        document.body.dataset.tableDensity = density;
        tableDensityButtons.forEach((button) => {
          const active = button.dataset.tableDensityButton === density;
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
          // Ignore storage failures and keep the server default.
        }
        setTableDensity(density, false);
        tableDensityButtons.forEach((button) => {
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
          tableWraps.forEach((wrap) => {
            syncTableWrapState(wrap);
          });
        };
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

      function initializeDetailsPanels() {
        const currentSearchValue = currentSearchDetailsValue();
        const hasSearchDetailsState = currentSearchValue !== null;
        const openKeysFromSearch = new Set(parseDetailsKeys(currentSearchValue || ""));
        detailsPanels.forEach((panel) => {
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
              // Ignore localStorage failures and fall back to server defaults.
            }
          }
          updateDetailsToggle(panel);
          panel.addEventListener("toggle", () => {
            updateDetailsToggle(panel);
            try {
              window.localStorage.setItem(detailsStorageKey(panel), panel.open ? "open" : "closed");
            } catch (error) {
              // Ignore persistence failures for private browsing / restricted storage.
            }
            syncDetailsQuery(true);
            syncTopbarState();
            requestAnimationFrame(() => {
              syncAllTableWraps();
            });
          });
        });
        syncDetailsQuery(hasSearchDetailsState);
      }

      const waypoints = waypointElements
        .filter((element) => element.id)
        .map((element) => ({
          element,
          hash: `#${element.id}`,
          label: element.dataset.waypointLabel || element.id,
          kind: element.dataset.waypointKind || "section",
          parent: element.dataset.waypointParent || "",
        }));

      function waypointIndex(hash) {
        return waypoints.findIndex((item) => item.hash === hash);
      }

      function currentWaypoint() {
        const hash = currentHash();
        const index = waypointIndex(hash);
        if (index >= 0) {
          return { item: waypoints[index], index };
        }
        return { item: waypoints[0] || null, index: 0 };
      }

      function waypointTitleText(item) {
        if (!item) {
          return "总览";
        }
        if (item.kind === "track" && item.parent) {
          return `${item.parent} / ${item.label}`;
        }
        return item.label;
      }

      function updateWaypointDock() {
        if (!waypointDock) {
          return;
        }
        const { item, index } = currentWaypoint();
        const previous = index > 0 ? waypoints[index - 1] : null;
        const next = index >= 0 && index < waypoints.length - 1 ? waypoints[index + 1] : null;
        if (waypointTitle) {
          waypointTitle.textContent = waypointTitleText(item);
        }
        if (waypointCopy) {
          waypointCopy.textContent =
            item && item.kind === "track"
              ? "当前停留在某条研究主线内部，切换展示模式、刷新或继续滚动时都会尽量保留这里的位置。"
              : "章节导航会跟着滚动自动同步，长页面里可以直接从这里继续往前或回到顶部。";
        }
        if (waypointPrevButton) {
          waypointPrevButton.disabled = !previous;
          waypointPrevButton.title = previous ? `上一节：${waypointTitleText(previous)}` : "已经到顶部";
        }
        if (waypointNextButton) {
          waypointNextButton.disabled = !next;
          waypointNextButton.title = next ? `下一节：${waypointTitleText(next)}` : "已经到最后一节";
        }
        waypointDock.dataset.visible = window.scrollY > 300 ? "true" : "false";
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
        const threshold = 168;
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

      function syncTopbarState() {
        const activeModeLink =
          modeLinks.find((link) => link.classList.contains("active")) ||
          modeLinks[0];
        const activeAllowedHashes = ((activeModeLink && activeModeLink.dataset.allowedHashes) || "")
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean);
        const activeDefaultHash = (activeModeLink && activeModeLink.dataset.defaultHash) || "#overview";
        const rawHash = currentHash();
        let hash = rawHash;
        if (activeAllowedHashes.length && !activeAllowedHashes.includes(hash)) {
          hash = ["#framework", "#supplement", "#robustness"].includes(hash) && activeAllowedHashes.includes("#tracks")
            ? "#tracks"
            : activeDefaultHash;
        }
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

        sectionLinks.forEach((link) => {
          const linkHash = link.getAttribute("href");
          const normalizedSectionHash = aliasMap.get(hash) || hash;
          const active = linkHash === normalizedSectionHash;
          link.classList.toggle("active", active);
          if (active) {
            link.setAttribute("aria-current", "location");
          } else {
            link.removeAttribute("aria-current");
          }
        });

        modeLinks.forEach((link) => {
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
          let nextHash = hash;
          if (allowedHashes.length && !allowedHashes.includes(hash)) {
            nextHash = ["#framework", "#supplement", "#robustness"].includes(hash) && allowedHashes.includes("#tracks")
              ? "#tracks"
              : defaultHash;
          }
          if (shouldCarryDetailsState()) {
            nextUrl.searchParams.set(detailsQueryParam, detailsValueForNavigation());
          } else {
            nextUrl.searchParams.delete(detailsQueryParam);
          }
          link.setAttribute("href", `${nextUrl.pathname}${nextUrl.search}${nextHash}`);
        });

        anchorInputs.forEach((input) => {
          input.value = hash.replace(/^#/, "") || "overview";
        });
        syncDetailsInputs();
        syncWaypointMenuState();
        updateWaypointDock();
      }

      function applyRefreshState(payload) {
        const status = (payload && payload.status) || "idle";
        lastRefreshPayload = payload || null;
        const scopeKey = (payload && payload.scope_key) || "all";
        if (refreshPanel) {
          refreshPanel.dataset.state = status;
        }
        if (refreshSnapshotLabel && payload && payload.snapshot_label) {
          refreshSnapshotLabel.textContent = payload.snapshot_label;
        }
        if (refreshSnapshotCopy && payload && payload.snapshot_copy) {
          refreshSnapshotCopy.textContent = payload.snapshot_copy;
        }
        if (refreshNote) {
          refreshNote.textContent = refreshRuntimeCopy(payload);
        }
        const running = status === "running";
        if (running) {
          startRefreshRuntimeTimer();
        } else {
          stopRefreshRuntimeTimer();
        }
        refreshButtons.forEach((button) => {
          if (!button.dataset.defaultLabel) {
            button.dataset.defaultLabel = button.textContent.trim();
          }
          button.disabled = running;
          const buttonScopeKey = button.dataset.scopeKey || "all";
          const isActiveButton = running && buttonScopeKey === scopeKey;
          button.textContent = isActiveButton ? (button.dataset.runningLabel || "刷新中…") : button.dataset.defaultLabel;
        });
      }

      function stopRefreshPolling() {
        if (refreshPollTimer !== null) {
          window.clearTimeout(refreshPollTimer);
          refreshPollTimer = null;
        }
      }

      async function fetchRefreshStatus(anchorKey) {
        const url = new URL(refreshStatusUrl, window.location.origin);
        url.searchParams.set("mode", currentMode());
        url.searchParams.set("anchor", anchorKey || "overview");
        if (shouldCarryDetailsState()) {
          url.searchParams.set(detailsQueryParam, detailsValueForNavigation());
        }
        const response = await fetch(url.toString(), {
          headers: { Accept: "application/json" },
          credentials: "same-origin",
        });
        if (!response.ok) {
          throw new Error(`status ${response.status}`);
        }
        return response.json();
      }

      async function pollRefreshStatus(anchorKey) {
        stopRefreshPolling();
        try {
          const payload = await fetchRefreshStatus(anchorKey);
          applyRefreshState(payload);
          if (payload.status === "running") {
            refreshPollTimer = window.setTimeout(() => {
              pollRefreshStatus(anchorKey);
            }, Number(payload.poll_after_ms || 1500));
            return;
          }
          if (payload.status === "succeeded" && payload.redirect_url) {
            window.location.href = payload.redirect_url;
          }
        } catch (error) {
          applyRefreshState({
            status: "failed",
            message: "刷新状态轮询失败，请稍后重试或直接重新加载页面。",
            snapshot_label: refreshSnapshotLabel ? refreshSnapshotLabel.textContent : "",
            snapshot_copy: refreshSnapshotCopy ? refreshSnapshotCopy.textContent : "",
          });
        }
      }

      refreshButtons.forEach((button) => {
        button.dataset.defaultLabel = button.textContent.trim();
      });

      refreshForms.forEach((form) => {
        form.addEventListener("submit", async (event) => {
          if (!window.fetch) {
            return;
          }
          event.preventDefault();
          const formData = new FormData(form);
          const anchorKey = String(formData.get("anchor") || "overview");
          applyRefreshState({
            status: "running",
            message: "正在发起后台刷新，请稍候。",
            snapshot_label: refreshSnapshotLabel ? refreshSnapshotLabel.textContent : "",
            snapshot_copy: refreshSnapshotCopy ? refreshSnapshotCopy.textContent : "",
          });
          try {
            const response = await fetch(form.action, {
              method: "POST",
              body: formData,
              headers: {
                "X-Requested-With": "fetch",
                Accept: "application/json",
              },
              credentials: "same-origin",
            });
            if (!response.ok) {
              throw new Error(`refresh ${response.status}`);
            }
            const payload = await response.json();
            applyRefreshState(payload);
            if (payload.status === "succeeded" && payload.redirect_url) {
              window.location.href = payload.redirect_url;
              return;
            }
            if (payload.status === "running") {
              pollRefreshStatus(anchorKey);
            }
          } catch (error) {
            stopRefreshPolling();
            applyRefreshState({
              status: "failed",
              message: "刷新请求失败，请稍后重试；如需立即刷新，也可以直接重新加载页面。",
              snapshot_label: refreshSnapshotLabel ? refreshSnapshotLabel.textContent : "",
              snapshot_copy: refreshSnapshotCopy ? refreshSnapshotCopy.textContent : "",
            });
          }
        });
      });

      sectionLinks.forEach((link) => {
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
          scrollToHash(hash);
        });
      });

      waypointMenuToggles.forEach((button) => {
        button.addEventListener("click", () => {
          const isOpen = (waypointMenu && waypointMenu.dataset.open === "true") || false;
          setWaypointMenuOpen(!isOpen);
        });
      });

      waypointMenuCloseButtons.forEach((button) => {
        button.addEventListener("click", () => {
          setWaypointMenuOpen(false);
        });
      });

      if (waypointMenuBackdrop) {
        waypointMenuBackdrop.addEventListener("click", () => {
          setWaypointMenuOpen(false);
        });
      }

      waypointMenuLinks.forEach((link) => {
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
          setWaypointMenuOpen(false);
          scrollToHash(hash);
        });
      });

      if (waypointPrevButton) {
        waypointPrevButton.addEventListener("click", () => {
          const { index } = currentWaypoint();
          if (index > 0) {
            scrollToHash(waypoints[index - 1].hash);
          }
        });
      }

      if (waypointNextButton) {
        waypointNextButton.addEventListener("click", () => {
          const { index } = currentWaypoint();
          if (index >= 0 && index < waypoints.length - 1) {
            scrollToHash(waypoints[index + 1].hash);
          }
        });
      }

      if (waypointTopButton) {
        waypointTopButton.addEventListener("click", () => {
          scrollToHash("#overview");
        });
      }

      initializeTableDensity();
      initializeTableWraps();
      initializeDetailsPanels();
      updateReadingProgress();
      syncTopbarState();
      scheduleWaypointSync();
      if (refreshPanel && refreshPanel.dataset.state === "running") {
        pollRefreshStatus(currentSectionHash().replace(/^#/, "") || "overview");
      }
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
    })();
