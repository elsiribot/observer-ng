import type { GlobalActivityPoint } from '../types/api';

// Fleet-wide daily activity: volume (bars, left axis, BTC) + transaction count
// (line, right axis). Pure so it can be unit-tested; the component is just the
// ECharts wrapper. Mirrors the federation-page activity chart, but aggregated
// across all federations and showing both metrics at once.
export const VOLUME_COLOR = '#3b82f6';
export const COUNT_COLOR = '#f59e0b';

const AXIS_LABEL = { fontSize: 10, color: '#9ca3af' } as const;
const AXIS_LINE = { lineStyle: { color: '#374151' } } as const;

interface TooltipParam {
  axisValue: string;
  seriesName: string;
  value: number;
  marker: string;
}

export function buildGlobalActivityOption(points: GlobalActivityPoint[]) {
  const dates = points.map((p) => p.date);
  const volumeBtc = points.map((p) => p.volume_msat / 100_000_000_000);
  const counts = points.map((p) => p.tx_count);

  return {
    grid: { left: '3%', right: '3%', bottom: '10%', top: '14%', containLabel: true },
    legend: {
      top: 0,
      data: ['Volume (BTC)', 'Transactions'],
      textStyle: { color: '#9ca3af', fontSize: 11 },
      inactiveColor: '#6b7280',
    },
    tooltip: {
      trigger: 'axis' as const,
      backgroundColor: '#1f2937',
      borderColor: '#374151',
      textStyle: { color: '#fff', fontSize: 12 },
      formatter: (params: TooltipParam[]) => {
        if (params.length === 0) return '';
        const lines = params.map((p) => {
          const value =
            p.seriesName === 'Volume (BTC)'
              ? `${p.value.toLocaleString('en-US', { maximumFractionDigits: 4 })} BTC`
              : `${p.value.toLocaleString('en-US')} txs`;
          return `${p.marker}${p.seriesName}: ${value}`;
        });
        return `${params[0].axisValue}<br/>${lines.join('<br/>')}`;
      },
    },
    xAxis: {
      type: 'category' as const,
      data: dates,
      axisLabel: { ...AXIS_LABEL, hideOverlap: true },
      axisLine: AXIS_LINE,
    },
    yAxis: [
      {
        type: 'value' as const,
        name: 'Volume (BTC)',
        nameTextStyle: { color: '#9ca3af', fontSize: 10 },
        axisLabel: AXIS_LABEL,
        splitLine: { lineStyle: { color: '#37415133' } },
      },
      {
        type: 'value' as const,
        name: 'Transactions',
        nameTextStyle: { color: '#9ca3af', fontSize: 10 },
        axisLabel: AXIS_LABEL,
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: 'Volume (BTC)',
        type: 'bar' as const,
        yAxisIndex: 0,
        itemStyle: { color: VOLUME_COLOR },
        data: volumeBtc,
      },
      {
        name: 'Transactions',
        type: 'line' as const,
        yAxisIndex: 1,
        smooth: false,
        showSymbol: false,
        lineStyle: { color: COUNT_COLOR, width: 2 },
        itemStyle: { color: COUNT_COLOR },
        data: counts,
      },
    ],
  };
}
