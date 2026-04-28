/**
 * Sensitivity threshold controller for the CMA verdict grid.
 *
 * Wires click handlers on every ``.cma-sensitivity-threshold-chip`` so
 * that pressing one re-runs the per-card significance check against the
 * chosen threshold. Cards with a numeric ``data-p-value`` (the p-gated
 * hypotheses H1 / H4 / H5) are flipped between ``sig`` / ``not_sig``;
 * cards without a structured p (data-p-value="") stay ``na``.
 *
 * The server renders an initial state at p<0.10 (matching the backend
 * SIGNIFICANCE_LEVEL), so this module only kicks in when the user picks
 * a different chip. Tests inject a fake document via
 * ``createSensitivityThresholdController({ doc })``.
 *
 * Stateless beyond the DOM — no localStorage / URL persistence.
 */

const FILTER_NAV_SELECTOR = ".cma-sensitivity-threshold-filter";
const CHIP_SELECTOR = ".cma-sensitivity-threshold-chip";
const VERDICT_GRID_SELECTOR = ".cma-verdict-grid";
const VERDICT_CARD_SELECTOR = ".cma-verdict-card";
const STRIP_SELECTOR = ".cma-verdict-sensitivity";
const STRIP_ICON_SELECTOR = ".cma-verdict-sensitivity-icon";
const STRIP_TEXT_SELECTOR = ".cma-verdict-sensitivity-text";
const ACTIVE_CLASS = "is-active";

export function createSensitivityThresholdController(options = {}) {
  const doc =
    options.doc ?? (typeof document === "undefined" ? null : document);

  function initialize() {
    if (!doc) return;
    const navs = doc.querySelectorAll(FILTER_NAV_SELECTOR);
    navs.forEach(setupNav);
  }

  function setupNav(nav) {
    const chips = nav.querySelectorAll(CHIP_SELECTOR);
    if (!chips.length) return;
    // Resolve the verdict grid that follows this nav. Same heuristic as
    // verdict_filter.js: the nav and the grid are siblings in the same
    // parent (they always are in _dashboard_content_macros.html); fall
    // back to any grid in the document.
    const grid =
      nav.parentElement?.querySelector(VERDICT_GRID_SELECTOR) ??
      doc.querySelector(VERDICT_GRID_SELECTOR);
    if (!grid) return;

    chips.forEach((chip) => {
      chip.addEventListener("click", (event) => {
        event.preventDefault();
        const threshold = chip.getAttribute("data-threshold") || "0.10";
        applyThreshold({ chips, grid, nav, threshold });
      });
    });
  }

  function applyThreshold({ chips, grid, nav, threshold }) {
    nav.setAttribute("data-active", threshold);
    chips.forEach((chip) => {
      if ((chip.getAttribute("data-threshold") || "") === threshold) {
        chip.classList.add(ACTIVE_CLASS);
      } else {
        chip.classList.remove(ACTIVE_CLASS);
      }
    });
    grid.setAttribute("data-sensitivity-threshold", threshold);

    const cards = grid.querySelectorAll(VERDICT_CARD_SELECTOR);
    const tNum = parseFloat(threshold);
    cards.forEach((card) => updateCardStrip(card, threshold, tNum));
  }

  function updateCardStrip(card, thresholdLabel, thresholdNum) {
    const strip = card.querySelector(STRIP_SELECTOR);
    if (!strip) return;
    const pAttr = card.getAttribute("data-p-value") || "";
    const iconEl = strip.querySelector(STRIP_ICON_SELECTOR);
    const textEl = strip.querySelector(STRIP_TEXT_SELECTOR);
    if (pAttr === "") {
      strip.setAttribute("data-sensitivity", "na");
      if (iconEl) iconEl.textContent = "·";
      if (textEl) textEl.textContent = "头条指标不是 p，不在 sweep 范围";
      return;
    }
    const p = parseFloat(pAttr);
    if (Number.isNaN(p) || Number.isNaN(thresholdNum)) {
      // Defensive: malformed input, don't silently mark sig.
      strip.setAttribute("data-sensitivity", "na");
      if (iconEl) iconEl.textContent = "·";
      if (textEl) textEl.textContent = "p 值无效";
      return;
    }
    const sig = p < thresholdNum;
    strip.setAttribute("data-sensitivity", sig ? "sig" : "not_sig");
    if (iconEl) iconEl.textContent = sig ? "✓" : "—";
    if (textEl) {
      textEl.textContent = sig
        ? `在 p<${thresholdLabel} 下显著(p=${p.toFixed(3)})`
        : `在 p<${thresholdLabel} 下不显著(p=${p.toFixed(3)})`;
    }
  }

  return { initialize, applyThreshold, updateCardStrip };
}
