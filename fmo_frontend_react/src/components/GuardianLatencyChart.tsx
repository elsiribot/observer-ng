import { useEffect, useMemo, useState } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { LineChart } from 'echarts/charts';
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { api } from '../services/api';
import type { GuardianLatencySeries } from '../types/api';

echarts.use([
  LineChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  CanvasRenderer,
]);

// Distinct colors for guardian lines, cycled by peer-id index.
const GUARDIAN_COLORS = [
  '#3b82f6',
  '#10b981',
  '#f59e0b',
  '#8b5cf6',
  '#ec4899',
  '#14b8a6',
  '#f97316',
  '#6366f1',
  '#84cc16',
  '#06b6d4',
];
// The quorum line stands apart from the per-guardian palette.
const QUORUM_COLOR = '#ef4444';

const WINDOWS: { label: string; value: string }[] = [
  { label: '7 days', value: '7d' },
  { label: '30 days', value: '30d' },
];

interface GuardianLatencyChartProps {
  federationId: string;
}

export function GuardianLatencyChart({ federationId }: GuardianLatencyChartProps) {
  const [window, setWindow] = useState('30d');
  const [data, setData] = useState<GuardianLatencySeries | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .getGuardianLatency(federationId, window)
      .then((series) => {
        if (!cancelled) {
          setData(series);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load latency');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId, window]);

  const chartOption = useMemo(() => {
    if (!data) {
      return null;
    }

    // Each series is an array of [epoch_ms, latency_ms | null] pairs; nulls
    // break the line at buckets where the guardian produced no sample.
    const guardianSeries = data.guardians.map((g, idx) => ({
      name: g.name,
      type: 'line' as const,
      showSymbol: false,
      connectNulls: false,
      lineStyle: { color: GUARDIAN_COLORS[idx % GUARDIAN_COLORS.length], width: 1.5 },
      itemStyle: { color: GUARDIAN_COLORS[idx % GUARDIAN_COLORS.length] },
      emphasis: { focus: 'series' as const },
      data: data.buckets.map((b) => [b.time * 1000, b.latencies[idx]]),
    }));

    const quorumSeries = {
      name: `Quorum (${data.threshold}/${data.num_guardians})`,
      type: 'line' as const,
      showSymbol: false,
      connectNulls: false,
      z: 10,
      lineStyle: { color: QUORUM_COLOR, width: 3 },
      itemStyle: { color: QUORUM_COLOR },
      emphasis: { focus: 'series' as const },
      data: data.buckets.map((b) => [b.time * 1000, b.quorum_ms]),
    };

    return {
      grid: { left: '3%', right: '4%', bottom: '16%', top: '12%', containLabel: true },
      legend: {
        type: 'scroll',
        top: 0,
        textStyle: { color: '#9ca3af', fontSize: 11 },
        inactiveColor: '#6b7280',
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1f2937',
        borderColor: '#374151',
        textStyle: { color: '#fff', fontSize: 12 },
        valueFormatter: (v: number | null) => (v == null ? '—' : `${Math.round(v)} ms`),
      },
      xAxis: {
        type: 'time',
        axisLabel: { fontSize: 10, color: '#9ca3af' },
        axisLine: { lineStyle: { color: '#374151' } },
      },
      yAxis: {
        type: 'value',
        name: 'ms',
        nameTextStyle: { color: '#9ca3af', fontSize: 10 },
        axisLabel: {
          fontSize: 10,
          color: '#9ca3af',
          formatter: (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(1)}s` : `${v}`),
        },
        axisLine: { lineStyle: { color: '#374151' } },
        splitLine: { lineStyle: { color: '#374151', type: 'dashed' } },
      },
      dataZoom: [
        { type: 'inside' },
        {
          type: 'slider',
          height: 22,
          bottom: 8,
          borderColor: '#3b82f6',
          fillerColor: 'rgba(59, 130, 246, 0.2)',
          handleStyle: { color: '#3b82f6' },
          textStyle: { color: '#9ca3af', fontSize: 10 },
        },
      ],
      // Quorum drawn last so it sits on top of the guardian lines.
      series: [...guardianSeries, quorumSeries],
    };
  }, [data]);

  const windowSelector = (
    <div className="flex gap-1" role="group" aria-label="Latency window">
      {WINDOWS.map((w) => (
        <button
          key={w.value}
          onClick={() => setWindow(w.value)}
          className={`px-2.5 py-1 text-xs rounded-md border ${
            window === w.value
              ? 'bg-blue-600 border-blue-600 text-white'
              : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600'
          }`}
        >
          {w.label}
        </button>
      ))}
    </div>
  );

  const header = (
    <div className="flex items-center justify-between gap-3 mb-3 flex-wrap">
      <div className="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        API latency per guardian. The <span className="font-medium text-red-500">quorum</span> line is
        the slowest of the {data ? data.threshold : ''} fastest guardians at each poll — the latency
        to reach consensus.
      </div>
      {windowSelector}
    </div>
  );

  if (loading) {
    return (
      <div>
        {header}
        <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
          Loading latency…
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div>
        {header}
        <div className="py-10 text-center text-sm text-red-500">Error: {error}</div>
      </div>
    );
  }

  if (!data || data.buckets.length === 0 || !chartOption) {
    return (
      <div>
        {header}
        <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
          No latency data available
        </div>
      </div>
    );
  }

  return (
    <div>
      {header}
      <ReactEChartsCore
        echarts={echarts}
        option={chartOption}
        notMerge={true}
        lazyUpdate={true}
        style={{ height: '360px', width: '100%' }}
        opts={{ renderer: 'canvas' }}
      />
    </div>
  );
}
