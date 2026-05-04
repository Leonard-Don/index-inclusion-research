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


function buildMainRegressionOption(payload) {
  const rows = payload.rows || [];
  const labels = rows.map(r => r.label);
  // point series — coefficient
  const pointData = rows.map((r, i) => ({
    value: [r.coef, i],
    itemStyle: { color: r.color },
    p_value: r.p_value,
    stars: r.stars,
    ci_lo: r.ci_lo,
    ci_hi: r.ci_hi,
  }));
  // CI bars rendered via custom series
  const ciSeries = {
    name: '95% CI',
    type: 'custom',
    renderItem: (params, api) => {
      const idx = api.value(0);
      const lo = api.value(1);
      const hi = api.value(2);
      const yPx = api.coord([0, idx])[1];
      const loPx = api.coord([lo, idx])[0];
      const hiPx = api.coord([hi, idx])[0];
      return {
        type: 'group',
        children: [
          { type: 'line', shape: { x1: loPx, y1: yPx, x2: hiPx, y2: yPx },
            style: { stroke: rows[idx]?.color || '#30424f', lineWidth: 2 } },
          { type: 'line', shape: { x1: loPx, y1: yPx - 6, x2: loPx, y2: yPx + 6 },
            style: { stroke: rows[idx]?.color || '#30424f', lineWidth: 2 } },
          { type: 'line', shape: { x1: hiPx, y1: yPx - 6, x2: hiPx, y2: yPx + 6 },
            style: { stroke: rows[idx]?.color || '#30424f', lineWidth: 2 } },
        ],
      };
    },
    data: rows.map((r, i) => [i, r.ci_lo, r.ci_hi]),
    silent: true,
    z: -1,
  };
  return {
    title: { text: '主回归 treatment_group 系数（CAR[-1,+1] × 4 象限，带 95% CI）', left: 'center' },
    tooltip: {
      trigger: 'item',
      formatter: params => {
        const d = params.data;
        if (!d || d.value == null) return '';
        const coef = d.value[0];
        return `<strong>${labels[d.value[1]] ?? ''}</strong><br>` +
          `coef: ${(coef * 100).toFixed(3)}% ${d.stars ?? ''}<br>` +
          `95% CI: [${(d.ci_lo * 100).toFixed(3)}%, ${(d.ci_hi * 100).toFixed(3)}%]<br>` +
          `p = ${d.p_value?.toFixed(4) ?? '—'}`;
      },
    },
    grid: { left: 140, right: 30, top: 50, bottom: 60 },
    xAxis: {
      type: 'value',
      name: 'treatment_group 系数',
      nameLocation: 'center',
      nameGap: 28,
      axisLabel: { formatter: v => (v * 100).toFixed(1) + '%' },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    yAxis: {
      type: 'category',
      data: labels,
      inverse: true,
      axisTick: { show: false },
    },
    series: [
      ciSeries,
      {
        name: '系数估计',
        type: 'scatter',
        data: pointData,
        symbolSize: 12,
        emphasis: { scale: 1.4 },
      },
      {
        // zero-line marker
        type: 'line',
        data: [],
        markLine: {
          symbol: 'none',
          silent: true,
          lineStyle: { color: '#9ba3ad', type: 'dashed' },
          data: [{ xAxis: 0 }],
          label: { show: false },
        },
      },
    ],
  };
}


const CHART_OPTION_BUILDERS = {
  car_path: buildCarPathOption,
  price_pressure: buildPricePressureOption,
  car_heatmap: buildCarHeatmapOption,
  gap_decomposition: buildGapDecompositionOption,
  heterogeneity_size: buildHeterogeneitySizeOption,
  time_series_rolling: buildTimeSeriesRollingOption,
  main_regression: buildMainRegressionOption,
  // mechanism_regression reuses the forest-plot option builder; the
  // chart title and series dataset come straight from the payload.
  mechanism_regression: payload => {
    const opt = buildMainRegressionOption(payload);
    opt.title = { text: '机制回归 turnover_mechanism 系数（× 4 象限，带 95% CI）', left: 'center' };
    return opt;
  },
  // rdd_robustness reuses the same forest-plot option builder. Each row
  // is a different RDD specification (main / donut / placebo / polynomial)
  // rather than a quadrant; the bandwidth-locked τ + 95% CI lets reviewers
  // see how the headline result moves under specification changes.
  rdd_robustness: payload => {
    const opt = buildMainRegressionOption(payload);
    opt.title = { text: 'HS300 RDD 稳健性 · main / donut / placebo / polynomial（τ ± 95% CI）', left: 'center' };
    return opt;
  },
  event_counts: buildEventCountsOption,
  cma_mechanism_heatmap: buildCmaMechanismHeatmapOption,
  cma_gap_length_distribution: buildCmaGapLengthDistributionOption,
  rdd_scatter: buildRddScatterOption,
};


function buildRddScatterOption(payload) {
  const fits = Array.isArray(payload.fits) ? payload.fits : [];
  const defaultBw = payload.default_bandwidth;
  const fitColor = bw => {
    // Highlight the default bandwidth; fade the others so the plot
    // stays readable when multiple bandwidths are toggled on.
    if (defaultBw != null && Math.abs(bw - defaultBw) < 1e-9) return '#1f6feb';
    const palette = ['#5c6b77', '#8a6d3b', '#247346', '#a63b28', '#3a4554', '#5d4f8a', '#7a8b3f', '#5f676d'];
    const ix = fits.findIndex(f => Math.abs(f.bandwidth - bw) < 1e-9);
    return palette[(ix >= 0 ? ix : 0) % palette.length];
  };

  const ecSeries = payload.series.map(s => ({
    name: s.name,
    type: 'scatter',
    data: s.data,
    itemStyle: { color: s.color, opacity: 0.55 },
    symbolSize: 6,
    emphasis: { focus: 'series', itemStyle: { opacity: 1 } },
  }));

  // One fit-line series per bandwidth — connects (cutoff − bw, predicted)
  // → (cutoff, predicted from below) and (cutoff, predicted from above) →
  // (cutoff + bw, predicted) so the slope on each side is visible. Legend
  // toggles which bandwidth is shown.
  const fitSeriesNames = [];
  for (const fit of fits) {
    const name = `bw=${fit.bandwidth} (τ=${(fit.tau * 100).toFixed(2)}%, p=${fit.p_value.toFixed(3)}, n=${fit.n_obs})`;
    fitSeriesNames.push(name);
    const isDefault = defaultBw != null && Math.abs(fit.bandwidth - defaultBw) < 1e-9;
    const color = fitColor(fit.bandwidth);
    const baseSeries = {
      name,
      type: 'line',
      symbol: 'none',
      smooth: false,
      itemStyle: { color },
      lineStyle: { color, width: isDefault ? 2.5 : 1.6, type: isDefault ? 'solid' : 'dashed' },
      emphasis: { focus: 'series', lineStyle: { width: 3 } },
      bandwidth: fit.bandwidth,
      tau: fit.tau,
      p_value: fit.p_value,
      n_obs: fit.n_obs,
    };
    ecSeries.push({ ...baseSeries, side: 'left', data: fit.line_left });
    ecSeries.push({ ...baseSeries, side: 'right', data: fit.line_right });
  }

  if (payload.cutoff != null) {
    ecSeries.push({
      name: '__cutoff_marker__',
      type: 'line',
      data: [],
      markLine: {
        symbol: 'none',
        silent: true,
        lineStyle: { color: '#9ba3ad', type: 'dashed', width: 1.5 },
        label: { formatter: 'cutoff = ' + payload.cutoff },
        data: [{ xAxis: payload.cutoff }],
      },
    });
  }

  // Default legend selection: scatter series + only the default-bandwidth
  // fit. Legend click toggles other bandwidths on/off — that's the de-facto
  // bandwidth slider.
  const legendData = [...payload.series.map(s => s.name), ...fitSeriesNames];
  const legendSelected = {};
  for (const name of payload.series.map(s => s.name)) legendSelected[name] = true;
  const defaultFitName = (() => {
    if (defaultBw == null) return null;
    const fit = fits.find(f => Math.abs(f.bandwidth - defaultBw) < 1e-9);
    if (!fit) return null;
    return `bw=${fit.bandwidth} (τ=${(fit.tau * 100).toFixed(2)}%, p=${fit.p_value.toFixed(3)}, n=${fit.n_obs})`;
  })();
  for (const name of fitSeriesNames) {
    legendSelected[name] = name === defaultFitName;
  }

  const subtitleText = (() => {
    if (defaultBw == null || fits.length === 0) {
      return '点击图例切换 bandwidth · 默认隐藏其它带宽';
    }
    const fit = fits.find(f => Math.abs(f.bandwidth - defaultBw) < 1e-9) || fits[0];
    return (
      `默认 bandwidth=${fit.bandwidth} · τ=${(fit.tau * 100).toFixed(3)}%, ` +
      `p=${fit.p_value.toFixed(3)}, n=${fit.n_obs} · 点击图例切换其它带宽`
    );
  })();

  return {
    title: {
      text: 'HS300 RDD 散点 · 运行变量 × CAR[-1,+1] · 多 bandwidth 拟合',
      subtext: subtitleText,
      left: 'center',
      subtextStyle: { fontSize: 11, color: '#5c6b77' },
    },
    tooltip: {
      trigger: 'item',
      formatter: params => {
        if (params.seriesName === '__cutoff_marker__') return '';
        if (params.seriesType === 'line') {
          // Series name carries bandwidth + tau + p + n already; just show
          // it together with the running-variable position the user hovered.
          const x = Array.isArray(params.value) ? params.value[0] : null;
          const y = Array.isArray(params.value) ? params.value[1] : null;
          const lines = [`<strong>RDD 拟合</strong>`, params.seriesName];
          if (x != null) lines.push(`running_variable: ${x.toFixed(2)}`);
          if (y != null) lines.push(`predicted CAR[-1,+1]: ${(y * 100).toFixed(3)}%`);
          return lines.join('<br>');
        }
        if (!params.value || params.value.length < 2) return '';
        const x = params.value[0];
        const y = params.value[1];
        const data = params.data || {};
        const lines = [
          `<strong>${params.seriesName}</strong>`,
          `running_variable: ${x.toFixed(2)}`,
          `CAR[-1,+1]: ${(y * 100).toFixed(3)}%`,
        ];
        if (data.batch_id) lines.push(`batch: ${data.batch_id}`);
        if (data.ticker) lines.push(`ticker: ${data.ticker}`);
        if (data.security_name) lines.push(`证券: ${data.security_name}`);
        return lines.join('<br>');
      },
    },
    legend: {
      bottom: 0,
      type: 'scroll',
      data: legendData,
      selected: legendSelected,
      textStyle: { fontSize: 11 },
    },
    grid: { left: 60, right: 30, top: 70, bottom: 70 },
    xAxis: {
      type: 'value',
      name: 'running_variable',
      nameLocation: 'center',
      nameGap: 28,
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    yAxis: {
      type: 'value',
      name: 'CAR[-1,+1]',
      axisLabel: { formatter: v => (v * 100).toFixed(1) + '%' },
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: ecSeries,
  };
}


function buildCmaMechanismHeatmapOption(payload) {
  return {
    title: { text: 'CMA 机制系数 t 值热力图(no_fe spec)', left: 'center' },
    tooltip: {
      formatter: params => {
        const ann = payload.annotations.find(
          a => a.col === params.value[0] && a.row === params.value[1]
        );
        if (!ann) return '';
        return `<strong>${payload.row_labels[ann.row]}</strong><br>` +
          `${payload.col_labels[ann.col]}<br>` +
          `t = ${ann.t.toFixed(2)} ${ann.stars ?? ''}<br>` +
          (ann.p_value != null ? `p = ${ann.p_value.toFixed(4)}` : '');
      },
    },
    grid: { left: 140, right: 80, top: 50, bottom: 60 },
    xAxis: { type: 'category', data: payload.col_labels, splitArea: { show: true } },
    yAxis: { type: 'category', data: payload.row_labels, splitArea: { show: true } },
    visualMap: {
      min: -payload.vmax,
      max: payload.vmax,
      calculable: true,
      orient: 'vertical',
      right: 5,
      top: 'center',
      inRange: { color: ['#9c2f55', '#f7f2ea', '#0f5c6e'] },
      formatter: v => v.toFixed(2),
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
          return ann ? `${ann.t.toFixed(2)}\n${ann.stars}` : '';
        },
        fontSize: 12,
        fontWeight: 'bold',
        color: '#18212b',
      },
      emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.3)' } },
    }],
  };
}


function buildCmaGapLengthDistributionOption(payload) {
  const ecSeries = payload.series.map(s => ({
    name: s.name,
    type: 'bar',
    data: s.data,
    itemStyle: { color: s.color },
    barGap: 0,
    emphasis: { focus: 'series' },
  }));
  return {
    title: { text: 'Announce → Effective 窗口长度分布(天)', left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: params => {
        const days = params[0]?.axisValue ?? '';
        return `<strong>${days} 天</strong><br>` + params
          .filter(p => p.value > 0)
          .map(p => `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${p.value} events`)
          .join('<br>');
      },
    },
    legend: { bottom: 0, data: payload.series.map(s => s.name) },
    grid: { left: 60, right: 30, top: 50, bottom: 60 },
    xAxis: { type: 'category', data: payload.lengths.map(String), name: 'gap_length_days' },
    yAxis: { type: 'value', name: '事件数', splitLine: { lineStyle: { type: 'dashed' } } },
    series: ecSeries,
  };
}


function buildEventCountsOption(payload) {
  const ecSeries = payload.series.map(s => ({
    name: s.name,
    type: 'bar',
    data: s.data,
    itemStyle: { color: s.color },
    emphasis: { focus: 'series' },
    label: {
      show: true,
      position: 'top',
      formatter: params => params.value > 0 ? params.value : '',
      fontSize: 11,
      color: '#18212b',
    },
  }));
  return {
    title: { text: '真实调入事件按公告年分布(CN vs US)', left: 'center' },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: params => {
        const year = params[0]?.axisValue ?? '';
        return `<strong>${year}</strong><br>` + params.map(p =>
          `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px"></span>${p.seriesName}: ${p.value} events`
        ).join('<br>');
      },
    },
    legend: { bottom: 0, data: payload.series.map(s => s.name) },
    grid: { left: 60, right: 30, top: 50, bottom: 60 },
    xAxis: { type: 'category', data: payload.years.map(String), name: '公告年份' },
    yAxis: {
      type: 'value',
      name: '事件数',
      splitLine: { lineStyle: { type: 'dashed' } },
    },
    series: ecSeries,
  };
}

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
      // empty / unrenderable payload — keep the static fallback img
      const hasContent = !!(payload.series || payload.data || payload.rows);
      if (payload.error || !hasContent) {
        container.classList.remove('echart-loading');
        container.classList.add('echart-empty');
        return;
      }
      const chart = echarts.init(container, 'dashboard');
      chart.setOption(optionBuilder(payload));
      instances.set(container, chart);
      container.classList.remove('echart-loading');

      // hide the static fallback img if present. .echart-fallback is a
      // sibling of .echart-panel (the container's direct parent), so we
      // walk up one level before searching.
      const echartPanel = container.closest('.echart-panel');
      const fallback = echartPanel?.parentElement?.querySelector(':scope > .echart-fallback');
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
