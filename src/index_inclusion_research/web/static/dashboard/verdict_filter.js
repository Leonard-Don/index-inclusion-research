/**
 * Verdict tier filter for the CMA verdict grid.
 *
 * Wires click handlers on every ``.cma-verdict-filter-chip`` so that
 * pressing one toggles the matching ``.cma-verdict-grid``'s ``data-filter``
 * or ``data-filter-tier`` attribute. CSS rules in dashboard.css then hide
 * cards whose ``data-verdict`` or ``data-evidence-tier`` doesn't match the
 * active filter; pressing "全部" clears the relevant attribute and shows
 * everything in that dimension again.
 *
 * Stateless beyond the DOM — no localStorage / URL persistence in this
 * cut. Tests inject a fake document via ``createVerdictFilterController(
 * { doc })`` so initialization can be exercised without a real browser.
 */

const FILTER_CHIP_SELECTOR = ".cma-verdict-filter-chip";
const FILTER_NAV_SELECTOR = ".cma-verdict-filter";
const VERDICT_GRID_SELECTOR = ".cma-verdict-grid";
const TRACK_CARD_SELECTOR = ".cma-track-card";
const ACTIVE_CLASS = "is-active";

export function createVerdictFilterController(options = {}) {
  const doc = options.doc ?? (typeof document === "undefined" ? null : document);

  function initialize() {
    if (!doc) return;
    // tier filter chips
    const navs = doc.querySelectorAll(FILTER_NAV_SELECTOR);
    navs.forEach(setupNav);
    // track filter via track summary cards (toggle-on-click)
    const trackCards = doc.querySelectorAll(TRACK_CARD_SELECTOR);
    trackCards.forEach(setupTrackCard);
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
        const isEvidenceTierFilter = chip.getAttribute("data-filter-tier") !== null;
        const filterAttribute = isEvidenceTierFilter
          ? "data-filter-tier"
          : "data-filter";
        const filter = chip.getAttribute(filterAttribute) || "all";
        applyFilter({ chips, grid, nav, filter, filterAttribute });
      });
    });
  }

  function applyFilter({ chips, grid, nav, filter, filterAttribute = "data-filter" }) {
    nav.setAttribute("data-active", filter);
    chips.forEach((chip) => {
      if ((chip.getAttribute(filterAttribute) || "") === filter) {
        chip.classList.add(ACTIVE_CLASS);
      } else {
        chip.classList.remove(ACTIVE_CLASS);
      }
    });
    if (filter === "all") {
      grid.removeAttribute(filterAttribute);
    } else {
      grid.setAttribute(filterAttribute, filter);
    }
  }

  function setupTrackCard(card) {
    const track = card.getAttribute("data-filter-track");
    if (!track) return;
    const grid = doc.querySelector(VERDICT_GRID_SELECTOR);
    if (!grid) return;
    card.addEventListener("click", (event) => {
      event.preventDefault();
      toggleTrackFilter({ card, grid, track });
    });
  }

  function toggleTrackFilter({ card, grid, track }) {
    const currentTrack = grid.getAttribute("data-filter-track");
    // sibling cards inside the same .cma-track-summary container
    const siblings =
      card.parentElement?.querySelectorAll(TRACK_CARD_SELECTOR) ?? [];
    if (currentTrack === track) {
      // toggle off
      grid.removeAttribute("data-filter-track");
      siblings.forEach((sib) => sib.classList.remove(ACTIVE_CLASS));
    } else {
      grid.setAttribute("data-filter-track", track);
      siblings.forEach((sib) => {
        if (sib === card) {
          sib.classList.add(ACTIVE_CLASS);
        } else {
          sib.classList.remove(ACTIVE_CLASS);
        }
      });
    }
  }

  return { initialize, applyFilter, toggleTrackFilter };
}
