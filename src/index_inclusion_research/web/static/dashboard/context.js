import {
  createNavigationContext,
  createRefreshContext,
  createRuntimeContext,
  createSurfaceContext,
} from "./context_groups.js";

export function createDashboardContext() {
  return {
    ...createNavigationContext(),
    ...createRefreshContext(),
    ...createSurfaceContext(),
    ...createRuntimeContext(),
  };
}
