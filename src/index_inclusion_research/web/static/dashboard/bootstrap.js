import { createDashboardContext } from "./context.js";
import { createNavigationController } from "./navigation.js";
import { createRefreshController } from "./refresh.js";
import { createSensitivityThresholdController } from "./sensitivity_threshold.js";
import { createSurfaceController } from "./surface.js";
import { createVerdictFilterController } from "./verdict_filter.js";

const defaultDependencies = {
  createDashboardContext,
  createSurfaceController,
  createNavigationController,
  createRefreshController,
  createVerdictFilterController,
  createSensitivityThresholdController,
};

export function bootstrapDashboard(dependencies = defaultDependencies) {
  const {
    createDashboardContext: buildContext,
    createSurfaceController: buildSurfaceController,
    createNavigationController: buildNavigationController,
    createRefreshController: buildRefreshController,
    createVerdictFilterController: buildVerdictFilterController,
    createSensitivityThresholdController: buildSensitivityThresholdController,
  } = dependencies;
  const context = buildContext();
  const surface = buildSurfaceController(context);
  const navigation = buildNavigationController(context, surface);
  const refresh = buildRefreshController(context, surface, navigation);
  const verdictFilter = buildVerdictFilterController();
  const sensitivityThreshold = buildSensitivityThresholdController();

  surface.initialize({
    onDetailsStateChange: () => {
      navigation.syncTopbarState();
    },
  });
  navigation.initialize();
  refresh.initialize();
  verdictFilter.initialize();
  sensitivityThreshold.initialize();

  return {
    context,
    surface,
    navigation,
    refresh,
    verdictFilter,
    sensitivityThreshold,
  };
}
