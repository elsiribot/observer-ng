import type { EcashAnonScatter } from '../types/api';
import { formatSi } from './anonSet';
import { formatTimestamp } from './format';

// Scatter dots (individual sampled transactions) vs. the rolling-7d
// percentile lines. p50 is drawn thicker/darker than p10/p90 so the median
// trend reads first.
export const SCATTER_COLOR = '#3b82f6';
export const P10_COLOR = '#f59e0b';
export const P50_COLOR = '#ef4444';
export const P90_COLOR = '#10b981';

interface TooltipParam {
  seriesName: string;
  value: [number, number];
  marker: string;
}

// Builds the ECharts option for the ecash-spend anonymity scatter chart: a
// random sample of per-transaction points (x = time, y = anon bits) plus
// rolling-7d p10/p50/p90 percentile lines. Pure (no DOM) so it can be
// unit-tested; the Y axis is expressed in bits but labeled/tooltipped as the
// implied crowd size (2^bits) via `formatSi`, which is what a reader actually
// cares about.
export function buildAnonScatterOption(data: EcashAnonScatter) {
  const scatterData = data.points.map((p) => [p.t * 1000, p.bits]);
  const p10Data = data.percentiles.map((p) => [p.t * 1000, p.p10]);
  const p50Data = data.percentiles.map((p) => [p.t * 1000, p.p50]);
  const p90Data = data.percentiles.map((p) => [p.t * 1000, p.p90]);

  return {
    grid: { left: '3%', right: '4%', bottom: '16%', top: '12%', containLabel: true },
    legend: {
      type: 'scroll' as const,
      top: 0,
      textStyle: { color: '#9ca3af', fontSize: 11 },
      inactiveColor: '#6b7280',
    },
    tooltip: {
      trigger: 'item' as const,
      backgroundColor: '#1f2937',
      borderColor: '#374151',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: TooltipParam) => {
        const [t, bits] = params.value;
        return `${params.marker}${params.seriesName}<br/>${formatTimestamp(
          Math.round(t / 1000)
        )}<br/>≈ ${formatSi(2 ** bits)} crowd`;
      },
    },
    xAxis: {
      type: 'time' as const,
      axisLabel: { fontSize: 10, color: '#9ca3af' },
      axisLine: { lineStyle: { color: '#374151' } },
    },
    yAxis: {
      type: 'value' as const,
      name: 'anonymity set',
      nameTextStyle: { color: '#9ca3af', fontSize: 10 },
      axisLabel: {
        fontSize: 10,
        color: '#9ca3af',
        formatter: (v: number) => formatSi(2 ** v),
      },
      axisLine: { lineStyle: { color: '#374151' } },
      splitLine: { lineStyle: { color: '#374151', type: 'dashed' as const } },
    },
    dataZoom: [
      { type: 'inside' as const },
      {
        type: 'slider' as const,
        height: 22,
        bottom: 8,
        borderColor: '#3b82f6',
        fillerColor: 'rgba(59, 130, 246, 0.2)',
        handleStyle: { color: '#3b82f6' },
        textStyle: { color: '#9ca3af', fontSize: 10 },
      },
    ],
    series: [
      {
        name: 'Transactions',
        type: 'scatter' as const,
        symbolSize: 5,
        itemStyle: { color: SCATTER_COLOR, opacity: 0.35 },
        data: scatterData,
      },
      {
        name: 'p10',
        type: 'line' as const,
        smooth: true,
        showSymbol: false,
        lineStyle: { color: P10_COLOR, width: 1.5 },
        itemStyle: { color: P10_COLOR },
        data: p10Data,
      },
      {
        name: 'p50 (median)',
        type: 'line' as const,
        smooth: true,
        showSymbol: false,
        z: 10,
        lineStyle: { color: P50_COLOR, width: 2.5 },
        itemStyle: { color: P50_COLOR },
        data: p50Data,
      },
      {
        name: 'p90',
        type: 'line' as const,
        smooth: true,
        showSymbol: false,
        lineStyle: { color: P90_COLOR, width: 1.5 },
        itemStyle: { color: P90_COLOR },
        data: p90Data,
      },
    ],
  };
}
