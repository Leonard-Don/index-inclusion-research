import { fetchRefreshStatus, postRefreshRequest } from "./refresh_requests.js";
import {
  applyRefreshStateToDom,
  refreshRuntimeCopy,
} from "./refresh_presenter.js";

export function createRefreshController(ctx, surface, navigation) {
  let refreshPollTimer = null;
  let refreshRuntimeTimer = null;
  const refreshState = { lastPayload: null };

  function stopRefreshRuntimeTimer() {
    if (refreshRuntimeTimer !== null) {
      window.clearInterval(refreshRuntimeTimer);
      refreshRuntimeTimer = null;
    }
  }

  function startRefreshRuntimeTimer() {
    stopRefreshRuntimeTimer();
    refreshRuntimeTimer = window.setInterval(() => {
      if (!refreshState.lastPayload || refreshState.lastPayload.status !== "running") {
        stopRefreshRuntimeTimer();
        return;
      }
      const startedTs = Number(refreshState.lastPayload.started_ts || 0);
      if (startedTs > 0) {
        refreshState.lastPayload.duration_seconds = Math.max(
          0,
          Math.floor(Date.now() / 1000 - startedTs),
        );
      }
      if (ctx.refreshNote) {
        ctx.refreshNote.textContent = refreshRuntimeCopy(ctx, refreshState.lastPayload);
      }
    }, 1000);
  }

  function applyRefreshState(payload) {
    applyRefreshStateToDom(ctx, refreshState, payload, {
      startRefreshRuntimeTimer,
      stopRefreshRuntimeTimer,
    });
  }

  function stopRefreshPolling() {
    if (refreshPollTimer !== null) {
      window.clearTimeout(refreshPollTimer);
      refreshPollTimer = null;
    }
  }

  async function pollRefreshStatus(anchorKey) {
    stopRefreshPolling();
    try {
      const payload = await fetchRefreshStatus(ctx, surface, anchorKey);
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
      void error;
      applyRefreshState({
        status: "failed",
        message: "刷新状态轮询失败，请稍后重试或直接重新加载页面。",
        snapshot_label: ctx.refreshSnapshotLabel ? ctx.refreshSnapshotLabel.textContent : "",
        snapshot_copy: ctx.refreshSnapshotCopy ? ctx.refreshSnapshotCopy.textContent : "",
      });
    }
  }

  function bindRefreshForms() {
    ctx.refreshButtons.forEach((button) => {
      button.dataset.defaultLabel = button.textContent.trim();
    });

    ctx.refreshForms.forEach((form) => {
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
          snapshot_label: ctx.refreshSnapshotLabel ? ctx.refreshSnapshotLabel.textContent : "",
          snapshot_copy: ctx.refreshSnapshotCopy ? ctx.refreshSnapshotCopy.textContent : "",
        });
        try {
          const response = await postRefreshRequest(form, formData);
          if (!response.ok && response.status !== 409) {
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
          void error;
          stopRefreshPolling();
          applyRefreshState({
            status: "failed",
            message: "刷新请求失败，请稍后重试；如需立即刷新，也可以直接重新加载页面。",
            snapshot_label: ctx.refreshSnapshotLabel ? ctx.refreshSnapshotLabel.textContent : "",
            snapshot_copy: ctx.refreshSnapshotCopy ? ctx.refreshSnapshotCopy.textContent : "",
          });
        }
      });
    });
  }

  return {
    initialize() {
      bindRefreshForms();
      if (ctx.refreshPanel && ctx.refreshPanel.dataset.state === "running") {
        pollRefreshStatus(navigation.currentSectionHash().replace(/^#/, "") || "overview");
      }
    },
  };
}
