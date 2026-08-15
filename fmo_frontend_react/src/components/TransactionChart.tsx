import { useMemo } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { LineChart } from 'echarts/charts';
import {
  GridComponent,
  TooltipComponent,
  DataZoomComponent,
  LegendComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

// Register only the components we need
echarts.use([
  LineChart,
  GridComponent,
  TooltipComponent,
  DataZoomComponent,
  LegendComponent,
  CanvasRenderer,
]);

export interface ChartSeries {
  name: string;
  color: string;
  data: number[];
}

interface TransactionChartProps {
  dates: string[];
  series: ChartSeries[];
  chartMetric: 'volume' | 'count';
  zoomStart: number;
  zoomEnd: number;
  onZoomChange: (start: number, end: number) => void;
}

function formatValue(value: number, metric: 'volume' | 'count'): string {
  if (metric === 'volume') {
    return value.toFixed(8) + ' BTC';
  }
  return Math.round(value) + ' transactions';
}

// Stacked area chart of daily activity, one filled band per transaction-type
// layer. The layers stack to the day's total; the tooltip lists each layer plus
// a total row.
export function TransactionChart({
  dates,
  series,
  chartMetric,
  zoomStart,
  zoomEnd,
  onZoomChange,
}: TransactionChartProps) {
  const baseChartOption = useMemo(() => {
    return {
      grid: {
        left: '3%',
        right: '4%',
        bottom: '15%',
        top: '15%',
        containLabel: true,
      },
      legend: {
        top: 0,
        textStyle: { color: '#9ca3af', fontSize: 11 },
        icon: 'roundRect',
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: {
          rotate: 45,
          fontSize: 10,
          color: '#9ca3af',
        },
        axisLine: {
          lineStyle: { color: '#374151' },
        },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          fontSize: 10,
          color: '#9ca3af',
          formatter: (value: number) => {
            if (chartMetric === 'volume') {
              return value < 0.001 ? value.toExponential(1) : value.toFixed(3);
            }
            return value < 1 ? value.toFixed(1) : Math.round(value).toString();
          },
        },
        axisLine: {
          lineStyle: { color: '#374151' },
        },
        splitLine: {
          lineStyle: { color: '#374151', type: 'dashed' },
        },
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1f2937',
        borderColor: '#374151',
        textStyle: { color: '#fff', fontSize: 12 },
        formatter: (
          params: { axisValue: string; marker: string; seriesName: string; value: number }[],
        ) => {
          const date = params[0].axisValue;
          let result = `${date}<br/>`;
          let total = 0;
          params.forEach((param) => {
            const value = param.value ?? 0;
            total += value;
            result += `${param.marker} ${param.seriesName}: ${formatValue(value, chartMetric)}<br/>`;
          });
          result += `<span style="opacity:0.7">Total: ${formatValue(total, chartMetric)}</span>`;
          return result;
        },
      },
      series: series.map((s) => ({
        name: s.name,
        type: 'line',
        stack: 'total',
        data: s.data,
        smooth: false,
        symbol: 'none',
        lineStyle: { color: s.color, width: 1 },
        itemStyle: { color: s.color },
        areaStyle: { color: s.color, opacity: 0.75 },
        emphasis: { focus: 'series' },
      })),
    };
  }, [dates, series, chartMetric]);

  const chartOption = useMemo(
    () => ({
      ...baseChartOption,
      dataZoom: [
        {
          type: 'slider',
          start: zoomStart,
          end: zoomEnd,
          height: 25,
          bottom: 10,
          borderColor: '#3b82f6',
          fillerColor: 'rgba(59, 130, 246, 0.2)',
          handleStyle: {
            color: '#3b82f6',
          },
          moveHandleSize: 10,
          textStyle: { color: '#9ca3af', fontSize: 10 },
        },
      ],
    }),
    [baseChartOption, zoomStart, zoomEnd],
  );

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={chartOption}
      notMerge={true}
      lazyUpdate={true}
      style={{ height: '400px', width: '100%' }}
      opts={{ renderer: 'canvas' }}
      onEvents={{
        dataZoom: (params: { batch?: { start: number; end: number }[]; start?: number; end?: number }) => {
          // Handle both batch and direct dataZoom events
          if (params.batch && params.batch[0]) {
            const start = params.batch[0].start;
            const end = params.batch[0].end;
            if (start !== undefined && end !== undefined) {
              onZoomChange(start, end);
            }
          } else if (params.start !== undefined && params.end !== undefined) {
            onZoomChange(params.start, params.end);
          }
        },
      }}
    />
  );
}
