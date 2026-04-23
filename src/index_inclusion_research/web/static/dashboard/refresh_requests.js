export async function fetchRefreshStatus(ctx, surface, anchorKey) {
  const url = new URL(ctx.refreshStatusUrl, window.location.origin);
  url.searchParams.set("mode", ctx.currentMode());
  url.searchParams.set("anchor", anchorKey || "overview");
  if (surface.shouldCarryDetailsState()) {
    url.searchParams.set(ctx.detailsQueryParam, surface.detailsValueForNavigation());
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

export function postRefreshRequest(form, formData) {
  return fetch(form.action, {
    method: "POST",
    body: formData,
    headers: {
      "X-Requested-With": "fetch",
      Accept: "application/json",
    },
    credentials: "same-origin",
  });
}
