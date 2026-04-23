import { createDashboardContext } from "./context.js";
import { createSurfaceController } from "./surface.js";
import { createNavigationController } from "./navigation.js";
import { createRefreshController } from "./refresh.js";

const defaultDependencies = {
  createDashboardContext,
  createSurfaceController,
  createNavigationController,
  createRefreshController,
};

export function bootstrapDashboard(dependencies = defaultDependencies) {
  const {
    createDashboardContext: buildContext,
    createSurfaceController: buildSurfaceController,
    createNavigationController: buildNavigationController,
    createRefreshController: buildRefreshController,
  } = dependencies;
  const context = buildContext();
  const surface = buildSurfaceController(context);
  const navigation = buildNavigationController(context, surface);
  const refresh = buildRefreshController(context, surface, navigation);

  surface.initialize({
    onDetailsStateChange: () => {
      navigation.syncTopbarState();
    },
  });
  navigation.initialize();
  refresh.initialize();

  return {
    context,
    surface,
    navigation,
    refresh,
  };
}
