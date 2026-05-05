(() => {
  const instances = new WeakMap();

  function asArray(value) {
    if (value == null || Array.isArray(value)) return value;
    return [value];
  }

  function normalizeOption(option) {
    const normalized = { ...(option || {}) };
    for (const key of ["title", "legend", "grid", "xAxis", "yAxis", "tooltip"]) {
      if (key in normalized) normalized[key] = asArray(normalized[key]);
    }
    normalized.series = Array.isArray(normalized.series)
      ? normalized.series
      : (normalized.series ? [normalized.series] : []);
    return normalized;
  }

  class Chart {
    constructor(dom) {
      this.dom = dom;
      this.option = {};
    }

    setOption(option) {
      this.option = normalizeOption(option);
    }

    getOption() {
      return this.option;
    }

    resize() {}

    dispose() {
      instances.delete(this.dom);
    }

    on() {}

    off() {}

    showLoading() {}

    hideLoading() {}
  }

  window.echarts = {
    init(dom) {
      const chart = new Chart(dom);
      instances.set(dom, chart);
      return chart;
    },
    getInstanceByDom(dom) {
      return instances.get(dom) || null;
    },
    registerTheme() {},
    dispose(dom) {
      const chart = instances.get(dom);
      if (chart) chart.dispose();
    },
  };
})();
