import { useMemo } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { LineChart } from 'echarts/charts';
import { GridComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import type { SpAccountTx, SpSeriesPoint } from '../types/api';
import { buildAccountNetOption, buildNetFlowOption, buildPriceOption } from '../utils/spCharts';

echarts.use([LineChart, GridComponent, TooltipComponent, CanvasRenderer]);

const STYLE = { height: '260px', width: '100%' } as const;

// Thin ECharts wrappers; all option construction lives in the pure, unit-tested
// builders in utils/spCharts.ts.

export function SpPriceChart({ series }: { series: SpSeriesPoint[] }) {
  const option = useMemo(() => buildPriceOption(series), [series]);
  return <ReactEChartsCore echarts={echarts} option={option} notMerge lazyUpdate style={STYLE} />;
}

export function SpNetFlowChart({ series }: { series: SpSeriesPoint[] }) {
  const option = useMemo(() => buildNetFlowOption(series), [series]);
  return <ReactEChartsCore echarts={echarts} option={option} notMerge lazyUpdate style={STYLE} />;
}

export function SpAccountNetChart({ txs }: { txs: SpAccountTx[] }) {
  const option = useMemo(() => buildAccountNetOption(txs), [txs]);
  return <ReactEChartsCore echarts={echarts} option={option} notMerge lazyUpdate style={STYLE} />;
}
