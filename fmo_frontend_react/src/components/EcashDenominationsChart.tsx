import { useMemo } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { BarChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import type { MintDenomination } from '../types/api';
import { buildDenominationChartOption } from '../utils/denominations';

echarts.use([BarChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer]);

interface EcashDenominationsChartProps {
  data: MintDenomination[];
}

// Grouped-bar, dual-Y histogram of ecash note denominations: ever-issued (left
// axis) vs. currently in circulation (right axis). The option construction is a
// pure, unit-tested function; this component is just the ECharts wrapper.
export function EcashDenominationsChart({ data }: EcashDenominationsChartProps) {
  const option = useMemo(() => buildDenominationChartOption(data), [data]);

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
