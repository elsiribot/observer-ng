import { useMemo } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { ScatterChart, LineChart } from 'echarts/charts';
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import type { EcashAnonScatter } from '../types/api';
import { buildAnonScatterOption } from '../utils/anonScatter';

echarts.use([
  ScatterChart,
  LineChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  CanvasRenderer,
]);

interface EcashAnonScatterChartProps {
  data: EcashAnonScatter;
}

// Scatter of individual ecash-spending transactions' anonymity-set estimate
// (bits) over time, overlaid with rolling-7d p10/p50/p90 percentile lines.
// The option construction is a pure, unit-tested function; this component is
// just the ECharts wrapper.
export function EcashAnonScatterChart({ data }: EcashAnonScatterChartProps) {
  const option = useMemo(() => buildAnonScatterOption(data), [data]);

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      notMerge={true}
      lazyUpdate={true}
      style={{ height: '380px', width: '100%' }}
      opts={{ renderer: 'canvas' }}
    />
  );
}
