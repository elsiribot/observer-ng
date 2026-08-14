import type { MintDenomination } from '../types/api';

// Ecash note denominations are exact powers of two in *millisatoshis*, so no
// denomination above 1 sat is a whole number of sats. We therefore label the
// axis in msat (compact) and give the exact value + sat-equivalent in the
// tooltip.

// Compact msat label for an axis tick (the unit lives in the axis name).
export function formatDenominationMsat(msat: number): string {
  if (msat < 1000) {
    return `${msat}`;
  }
  if (msat < 1_000_000) {
    const k = msat / 1000;
    return `${Number.isInteger(k) ? k : k.toFixed(2)}K`;
  }
  if (msat < 1_000_000_000) {
    return `${(msat / 1_000_000).toFixed(2)}M`;
  }
  return `${(msat / 1_000_000_000).toFixed(2)}G`;
}

// Exact tooltip label: grouped msat plus the sat equivalent.
export function formatDenominationLong(msat: number): string {
  const sat = msat / 1000;
  return `${msat.toLocaleString()} msat (${sat.toLocaleString(undefined, {
    maximumFractionDigits: 3,
  })} sat)`;
}

// Ever-issued sits on the left axis, in-circulation on the right, each with its
// own scale so the (usually large) magnitude gap stays readable.
export const ISSUED_COLOR = '#3b82f6';
export const CIRCULATION_COLOR = '#10b981';

// Builds the ECharts option for the grouped-bar, dual-Y denomination histogram.
// Pure (no DOM) so it can be unit-tested. `data` is assumed sorted ascending by
// denomination (the backend orders it).
export function buildDenominationChartOption(data: MintDenomination[]) {
  const categories = data.map((d) => formatDenominationMsat(d.denomination_msat));
  const longLabels = data.map((d) => formatDenominationLong(d.denomination_msat));

  return {
    grid: { left: '3%', right: '4%', bottom: '3%', top: '14%', containLabel: true },
    legend: {
      top: 0,
      textStyle: { color: '#9ca3af', fontSize: 11 },
      inactiveColor: '#6b7280',
    },
    tooltip: {
      trigger: 'axis' as const,
      axisPointer: { type: 'shadow' as const },
      backgroundColor: '#1f2937',
      borderColor: '#374151',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: Array<{ dataIndex: number; value: number; seriesName: string; marker: string }>) => {
        if (!params.length) {
          return '';
        }
        const header = longLabels[params[0].dataIndex] ?? '';
        const lines = params
          .map((p) => `${p.marker}${p.seriesName}: ${Number(p.value).toLocaleString()}`)
          .join('<br/>');
        return `${header}<br/>${lines}`;
      },
    },
    xAxis: {
      type: 'category' as const,
      data: categories,
      name: 'denomination (msat)',
      nameLocation: 'middle' as const,
      nameGap: 30,
      nameTextStyle: { color: '#9ca3af', fontSize: 10 },
      axisLabel: { fontSize: 10, color: '#9ca3af', rotate: categories.length > 12 ? 45 : 0 },
      axisLine: { lineStyle: { color: '#374151' } },
    },
    yAxis: [
      {
        type: 'value' as const,
        name: 'ever issued',
        position: 'left' as const,
        nameTextStyle: { color: ISSUED_COLOR, fontSize: 10 },
        axisLabel: { fontSize: 10, color: '#9ca3af' },
        axisLine: { show: true, lineStyle: { color: '#374151' } },
        splitLine: { lineStyle: { color: '#374151', type: 'dashed' as const } },
      },
      {
        type: 'value' as const,
        name: 'in circulation',
        position: 'right' as const,
        nameTextStyle: { color: CIRCULATION_COLOR, fontSize: 10 },
        axisLabel: { fontSize: 10, color: '#9ca3af' },
        axisLine: { show: true, lineStyle: { color: '#374151' } },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: 'Ever issued',
        type: 'bar' as const,
        yAxisIndex: 0,
        itemStyle: { color: ISSUED_COLOR },
        emphasis: { focus: 'series' as const },
        data: data.map((d) => d.issued),
      },
      {
        name: 'In circulation',
        type: 'bar' as const,
        yAxisIndex: 1,
        itemStyle: { color: CIRCULATION_COLOR },
        emphasis: { focus: 'series' as const },
        data: data.map((d) => d.in_circulation),
      },
    ],
  };
}
