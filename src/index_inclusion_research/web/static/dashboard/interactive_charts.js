/**
 * Interactive chart controller — initialises ECharts instances for
 * elements marked with ``[data-echart]``.
 *
 * Charts are lazily loaded via IntersectionObserver: the fetch + init
 * only fires once the container scrolls into view.
 */

import { DASHBOARD_THEME } from './echarts_theme.js';

// ── chart builders ──────────────────────────────────────────────────

function buildCarPathOption(payload) {
  const arSeries = payload.series.filter(s => s.metric === 'AR');
  const carSeries = payload.series.filter(s => s.metric === 'CAR');
  const activeSeries = carSeries.length ? carSeries : arSeries;

  const allSeries = payload.series.map(s => ({
    name: s.name,
    type: 'line',
    data: s.data,
    itemStyle: { color: s.color },
    lineStyle: { type: s.lineStyle?.type || 'solid', width: s.lineStyle?.width || 2 },
    symbol: s.symbol || 'none',
    symbolSize: s.symbolSize || 4,
    emphasis: { lineStyle: { width: 3 } },
  }));

  const defaultSelected = {};
  payload.series.forEach(s => {
    defaultSelected[s.name] = activeSeries.some(a => a.name === s.name);
  });

  return {
    title: { text: '日度 AR / CAR 路径（CN vs US × announce / effective）', left: 'center' },
    tooltip: {
      trigger: 'axis',
      formatter: params => params.map(p =>
        `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${(p.value[1] * 100).toFixed(3)}%`
      ).join('<br>'),
    },
    legend: {
      bottom: 0,
      type: 'scroll',
      selected: defaultSelected,
    },
    grid: { left: 60, right: 30, top: 50, bottom: 60 },
    xAxis: {
      type: 'value',
      name: '事件日',
      nameLocation: 'center',
      nameGap: 28,
      axisTick: { alignWithLabel: true },
    },
    yAxis: {
      type: 'value',
      name: 'CAR',
      axisLabel: { formatter: v => (v * 100).toFixed(1) + '%' },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0 },
      { type: 'slider', xAxisIndex: 0, height: 20, bottom: 35 },
    ],
    series: allSeries,
  };
}

function buildPricePressureOption(payload) {
  const ecSeries = [];
  for (const s of payload.series) {
    // confidence-interval band
    if (s.ci_low?.length && s.ci_high?.length) {
      const bandData = s.ci_low.map((lo, i) => [lo[0], lo[1], s.ci_high[i][1]]);
      ecSeries.push({
        name: s.name + ' CI',
        type: 'custom',
        renderItem: (params, api) => {
          const xValue = api.value(0);
          const loValue = api.value(1);
          const hiValue = api.value(2);
          const lo = api.coord([xValue, loValue]);
          const hi = api.coord([xValue, hiValue]);
          const halfWidth = 8;
          return {
            type: 'polygon',
            shape: {
              points: [[lo[0] - halfWidth, lo[1]], [lo[0] + halfWidth, lo[1]],
                       [hi[0] + halfWidth, hi[1]], [hi[0] - halfWidth, hi[1]]],
            },
            style: { fill: s.color, opacity: 0.12 },
          };
        },
        encode: { x: 0, y: [1, 2] },
        data: bandData,
        silent: true,
        z: -1,
      });
    }
    // main line
    ecSeries.push({
      name: s.name,
      type: 'line',
      data: s.data,
      itemStyle: { color: s.color },
      lineStyle: { type: s.lineStyle?.type || 'solid', width: s.lineStyle?.width || 2.2 },
      symbol: s.symbol || 'circle',
      symbolSize: s.symbolSize || 7,
      emphasis: { lineStyle: { width: 3.5 } },
    });
  }

  return {
    title: { text: '短窗口 CAR 的时间变化', left: 'center' },
    tooltip: {
      trigger: 'axis',
      formatter: params => {
        const items = params.filter(p => p.seriesName && !p.seriesName.endsWith(' CI'));
        const year = items[0]?.value?.[0] ?? '';
        return `<strong>${year}</strong><br>` + items.map(p =>
          `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${(p.value[1] * 100).toFixed(3)}%`
        ).join('<br>');
      },
    },
    legend: {
      bottom: 0,
      data: payload.series.map(s => s.name),
    },
    grid: { left: 60, right: 30, top: 50, bottom: 40 },
    xAxis: {
      type: 'value',
      name: '公告年份',
      nameLocation: 'center',
      nameGap: 28,
      axisLabel: { formatter: v => String(v) },
      min: payload.years?.[0],
      max: payload.years?.[payload.years.length - 1],
    },
    yAxis: {
      type: 'value',
      name: '平均 CAR[-1,+1]',
      axisLabel: { formatter: v => (v * 100).toFixed(1) + '%' },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: ecSeries,
  };
}

function buildCarHeatmapOption(payload) {
  return {
    title: { text: '真实样本短窗口 CAR 热力图', left: 'center' },
    tooltip: {
      formatter: params => {
        const ann = payload.annotations.find(
          a => a.col === params.value[0] && a.row === params.value[1]
        );
        if (!ann) return '';
        return `<strong>${payload.row_labels[ann.row]}</strong><br>` +
          `窗口 ${payload.col_labels[ann.col]}<br>` +
          `CAR: ${ann.car_pct} ${ann.stars}<br>` +
          `p = ${ann.p_value.toFixed(4)}`;
      },
    },
    grid: { left: 140, right: 80, top: 50, bottom: 30 },
    xAxis: {
      type: 'category',
      data: payload.col_labels,
      splitArea: { show: true },
    },
    yAxis: {
      type: 'category',
      data: payload.row_labels,
      splitArea: { show: true },
    },
    visualMap: {
      min: -payload.vmax,
      max: payload.vmax,
      calculable: true,
      orient: 'vertical',
      right: 5,
      top: 'center',
      inRange: {
        color: ['#9c2f55', '#f7f2ea', '#0f5c6e'],
      },
      formatter: v => (v * 100).toFixed(2) + '%',
    },
    series: [{
      type: 'heatmap',
      data: payload.data,
      label: {
        show: true,
        formatter: params => {
          const ann = payload.annotations.find(
            a => a.col === params.value[0] && a.row === params.value[1]
          );
          return ann ? `${ann.car_pct}\n${ann.stars}` : '';
        },
        fontSize: 13,
        fontWeight: 'bold',
        color: '#18212b',
      },
      emphasis: {
        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.3)' },
      },
    }],
  };
}

function buildGapDecompositionOption(payload) {
  const colors = ['#a63b28', '#c36a2d', '#1f7a8c', '#5c6b77'];
  const ecSeries = payload.series.map((s, i) => ({
    name: s.name,
    type: 'bar',
    data: s.data,
    itemStyle: { color: colors[i % colors.length] },
    emphasis: { focus: 'series' },
    label: {
      show: true,
      formatter: params => (params.value * 100).toFixed(2) + '%',
      fontSize: 11,
      color: '#18212b',
    },
  }));
  return {
    title: { text: '空窗期分段：公告→空窗→生效→反转', left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: params => {
        const market = params[0]?.axisValue ?? '';
        return `<strong>${market}</strong><br>` + params.map(p =>
          `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${(p.value * 100).toFixed(3)}%`
        ).join('<br>');
      },
    },
    legend: { bottom: 0, data: payload.series.map(s => s.name) },
    grid: { left: 60, right: 30, top: 50, bottom: 60 },
    xAxis: { type: 'category', data: payload.markets },
    yAxis: {
      type: 'value',
      name: '平均 AR',
      axisLabel: { formatter: v => (v * 100).toFixed(1) + '%' },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: ecSeries,
  };
}


function buildHeterogeneitySizeOption(payload) {
  const ecSeries = payload.series.map(s => ({
    name: s.name,
    type: 'bar',
    data: s.data,
    itemStyle: { color: s.color },
    emphasis: { focus: 'series' },
    label: {
      show: true,
      formatter: params => params.value != null ? params.value.toFixed(2) : '',
      fontSize: 11,
      color: '#18212b',
    },
  }));
  return {
    title: { text: '市值五分位的不对称指数（CN vs US）', left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: params => {
        const bucket = params[0]?.axisValue ?? '';
        return `<strong>${bucket}</strong><br>` + params.map(p => {
          const seriesIdx = payload.series.findIndex(s => s.name === p.seriesName);
          const n = seriesIdx >= 0 ? payload.series[seriesIdx].n_events[p.dataIndex] : '';
          return `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${p.value?.toFixed(3) ?? '—'} (n=${n})`;
        }).join('<br>');
      },
    },
    legend: { bottom: 0, data: payload.series.map(s => s.name) },
    grid: { left: 60, right: 30, top: 50, bottom: 60 },
    xAxis: { type: 'category', data: payload.buckets, name: '市值分组' },
    yAxis: {
      type: 'value',
      name: '不对称指数',
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: ecSeries,
  };
}


function buildTimeSeriesRollingOption(payload) {
  const ecSeries = payload.series.map(s => ({
    name: s.name,
    type: 'line',
    data: s.data,
    itemStyle: { color: s.color },
    lineStyle: { type: s.lineStyle?.type || 'solid', width: s.lineStyle?.width || 2.2 },
    symbol: s.symbol || 'circle',
    symbolSize: s.symbolSize || 7,
    emphasis: { lineStyle: { width: 3.5 } },
  }));
  return {
    title: { text: 'Rolling CAR：5 年滚动窗口（CN vs US × announce / effective）', left: 'center' },
    tooltip: {
      trigger: 'axis',
      formatter: params => {
        const year = params[0]?.value?.[0] ?? '';
        return `<strong>${year}（窗口结束年）</strong><br>` + params.map(p =>
          `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${(p.value[1] * 100).toFixed(3)}%`
        ).join('<br>');
      },
    },
    legend: { bottom: 0, data: payload.series.map(s => s.name) },
    grid: { left: 60, right: 30, top: 50, bottom: 40 },
    xAxis: {
      type: 'value',
      name: '窗口结束年',
      nameLocation: 'center',
      nameGap: 28,
      axisLabel: { formatter: v => String(v) },
      min: payload.years?.[0],
      max: payload.years?.[payload.years.length - 1],
    },
    yAxis: {
      type: 'value',
      name: '平均 CAR',
      axisLabel: { formatter: v => (v * 100).toFixed(1) + '%' },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: ecSeries,
  };
}


const CHART_OPTION_BUILDERS = {
  car_path: buildCarPathOption,
  price_pressure: buildPricePressureOption,
  car_heatmap: buildCarHeatmapOption,
  gap_decomposition: buildGapDecompositionOption,
  heterogeneity_size: buildHeterogeneitySizeOption,
  time_series_rolling: buildTimeSeriesRollingOption,
};

// ── controller ──────────────────────────────────────────────────────

const instances = new Map();

function initChart(container) {
  const chartId = container.dataset.echart;
  const optionBuilder = CHART_OPTION_BUILDERS[chartId];
  if (!optionBuilder) return;

  const apiUrl = `/api/chart/${chartId}`;
  container.classList.add('echart-loading');

  fetch(apiUrl)
    .then(r => {
      if (!r.ok) throw new Error(`Chart API ${r.status}`);
      return r.json();
    })
    .then(payload => {
      if (payload.error || (!payload.series && !payload.data)) {
        container.classList.remove('echart-loading');
        container.classList.add('echart-empty');
        return;
      }
      const chart = echarts.init(container, 'dashboard');
      chart.setOption(optionBuilder(payload));
      instances.set(container, chart);
      container.classList.remove('echart-loading');

      // hide the static fallback img if present
      const fallback = container.parentElement?.querySelector('.echart-fallback');
      if (fallback) fallback.hidden = true;
    })
    .catch(err => {
      console.warn(`[interactive_charts] Failed to load ${chartId}:`, err);
      container.classList.remove('echart-loading');
      container.classList.add('echart-error');
    });
}

function handleResize() {
  for (const chart of instances.values()) {
    chart.resize();
  }
}

export function createInteractiveChartsController() {
  let resizeHandler = null;

  function initialize() {
    // do nothing if ECharts is not loaded
    if (typeof echarts === 'undefined') return;

    echarts.registerTheme('dashboard', DASHBOARD_THEME);

    const containers = document.querySelectorAll('[data-echart]');
    if (!containers.length) return;

    // lazy-init via IntersectionObserver
    if ('IntersectionObserver' in window) {
      const observer = new IntersectionObserver((entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            observer.unobserve(entry.target);
            initChart(entry.target);
          }
        }
      }, { rootMargin: '200px' });
      containers.forEach(el => observer.observe(el));
    } else {
      containers.forEach(el => initChart(el));
    }

    resizeHandler = handleResize;
    window.addEventListener('resize', resizeHandler);
  }

  function dispose() {
    if (resizeHandler) {
      window.removeEventListener('resize', resizeHandler);
    }
    for (const chart of instances.values()) {
      chart.dispose();
    }
    instances.clear();
  }

  return { initialize, dispose };
}
