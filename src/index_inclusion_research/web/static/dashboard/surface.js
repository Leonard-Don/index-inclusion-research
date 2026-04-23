import { createDetailsSurface } from "./surface_details.js";
import { createTableSurface } from "./surface_tables.js";

export function createSurfaceController(ctx) {
  const tableSurface = createTableSurface(ctx);
  const detailsSurface = createDetailsSurface(ctx, tableSurface);

  return {
    initialize({ onDetailsStateChange }) {
      tableSurface.initialize();
      detailsSurface.initialize(onDetailsStateChange || (() => {}));
    },
    shouldCarryDetailsState: detailsSurface.shouldCarryDetailsState,
    detailsValueForNavigation: detailsSurface.detailsValueForNavigation,
    syncDetailsInputs: detailsSurface.syncDetailsInputs,
    syncAllTableWraps: () => {
      tableSurface.syncAllTableWraps();
    },
  };
}
