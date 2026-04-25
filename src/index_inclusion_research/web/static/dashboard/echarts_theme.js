/**
 * Custom ECharts theme matching the dashboard colour system.
 *
 * Register with:  echarts.registerTheme('dashboard', DASHBOARD_THEME);
 */

export const DASHBOARD_THEME = {
  color: [
    '#a63b28', '#0f5c6e', '#c36a2d', '#1f7a8c',
    '#d7b49e', '#9cc7cf', '#5c6b77', '#92a0aa',
  ],
  backgroundColor: 'transparent',
  textStyle: { fontFamily: "'Songti SC', 'STHeiti', 'Arial Unicode MS', system-ui, sans-serif" },
  title: {
    textStyle: { color: '#18212b', fontSize: 16, fontWeight: 600 },
    subtextStyle: { color: '#5c6b77', fontSize: 12 },
  },
  legend: {
    textStyle: { color: '#445463', fontSize: 12 },
  },
  tooltip: {
    backgroundColor: 'rgba(24, 33, 43, 0.92)',
    borderColor: 'rgba(255, 255, 255, 0.08)',
    textStyle: { color: '#f7f2ea', fontSize: 13 },
    extraCssText: 'backdrop-filter: blur(8px); border-radius: 6px; box-shadow: 0 4px 20px rgba(0,0,0,.25);',
  },
  xAxis: {
    axisLine: { lineStyle: { color: '#c8d0d9' } },
    axisTick: { lineStyle: { color: '#c8d0d9' } },
    axisLabel: { color: '#5c6b77' },
    splitLine: { lineStyle: { color: '#e8ecf0', type: 'dashed' } },
  },
  yAxis: {
    axisLine: { lineStyle: { color: '#c8d0d9' } },
    axisTick: { lineStyle: { color: '#c8d0d9' } },
    axisLabel: { color: '#5c6b77' },
    splitLine: { lineStyle: { color: '#e8ecf0', type: 'dashed' } },
  },
  line: {
    smooth: false,
    symbolSize: 6,
    lineStyle: { width: 2.2 },
  },
};
