/**
 * Verdict tier filter for the CMA verdict grid.
 *
 * Wires click handlers on every ``.cma-verdict-filter-chip`` so that
 * pressing one toggles the matching ``.cma-verdict-grid``'s ``data-filter``
 * attribute. CSS rules in dashboard.css then hide cards whose
 * ``data-verdict`` doesn't match the active tier; pressing "全部" clears
 * the attribute and shows everything again.
 *
 * Stateless beyond the DOM — no localStorage / URL persistence in this
 * cut. Tests inject a fake document via ``createVerdictFilterController(
 * { doc })`` so initialization can be exercised without a real browser.
 */

const FILTER_CHIP_SELECTOR = ".cma-verdict-filter-chip";
const FILTER_NAV_SELECTOR = ".cma-verdict-filter";
const VERDICT_GRID_SELECTOR = ".cma-verdict-grid";
const ACTIVE_CLASS = "is-active";

export function createVerdictFilterController(options = {}) {
  const doc = options.doc ?? (typeof document === "undefined" ? null : document);

  function initialize() {
    if (!doc) return;
    const navs = doc.querySelectorAll(FILTER_NAV_SELECTOR);
    navs.forEach(setupNav);
  }

  function setupNav(nav) {
    const chips = nav.querySelectorAll(FILTER_CHIP_SELECTOR);
    if (!chips.length) return;
    // Resolve the verdict grid that follows this nav. We assume the nav
    // and the grid are siblings in the same parent (they always are in
    // _dashboard_content_macros.html); fall back to any grid in the
    // document if the structure changes.
    const grid =
      nav.parentElement?.querySelector(VERDICT_GRID_SELECTOR) ??
      doc.querySelector(VERDICT_GRID_SELECTOR);
    if (!grid) return;

    chips.forEach((chip) => {
      chip.addEventListener("click", (event) => {
        event.preventDefault();
        const filter = chip.getAttribute("data-filter") || "all";
        applyFilter({ chips, grid, nav, filter });
      });
    });
  }

  function applyFilter({ chips, grid, nav, filter }) {
    nav.setAttribute("data-active", filter);
    chips.forEach((chip) => {
      if ((chip.getAttribute("data-filter") || "") === filter) {
        chip.classList.add(ACTIVE_CLASS);
      } else {
        chip.classList.remove(ACTIVE_CLASS);
      }
    });
    if (filter === "all") {
      grid.removeAttribute("data-filter");
    } else {
      grid.setAttribute("data-filter", filter);
    }
  }

  return { initialize, applyFilter };
}
