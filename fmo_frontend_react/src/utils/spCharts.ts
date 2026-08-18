import type { SpAccountTx, SpSeriesPoint } from '../types/api';
import { formatFiat, formatTimestamp } from './format';

// Shared palette / axis styling, matching the other observer charts.
export const PRICE_COLOR = '#3b82f6';
export const NETFLOW_COLOR = '#10b981';
export const ACCOUNT_NET_COLOR = '#8b5cf6';

const AXIS_LABEL = { fontSize: 10, color: '#9ca3af' } as const;
const AXIS_LINE = { lineStyle: { color: '#374151' } } as const;
const TOOLTIP = {
  backgroundColor: '#1f2937',
  borderColor: '#374151',
  textStyle: { color: '#fff', fontSize: 12 },
} as const;

interface LineTooltipParam {
  value: [number, number];
  marker: string;
}

function baseGrid() {
  return { left: '3%', right: '4%', bottom: '10%', top: '12%', containLabel: true };
}

function timeAxis() {
  return { type: 'time' as const, axisLabel: AXIS_LABEL, axisLine: AXIS_LINE };
}

/// Cycle BTC→fiat price over time (points that carry a start time). y is in
/// dollars (base units / 100). Pure so it can be unit-tested.
export function buildPriceOption(series: SpSeriesPoint[]) {
  const data = series
    .filter((p) => p.start_time !== null)
    .map((p) => [(p.start_time as number) * 1000, p.price_fiat / 100]);

  return {
    grid: baseGrid(),
    tooltip: {
      trigger: 'axis' as const,
      ...TOOLTIP,
      formatter: (params: LineTooltipParam[]) => {
        const [t, price] = params[0].value;
        return `${formatTimestamp(Math.round(t / 1000))}<br/>${params[0].marker}$${price.toLocaleString(
          'en-US',
          { minimumFractionDigits: 2, maximumFractionDigits: 2 }
        )} / BTC`;
      },
    },
    xAxis: timeAxis(),
    yAxis: {
      type: 'value' as const,
      name: 'price / BTC',
      nameTextStyle: { color: '#9ca3af', fontSize: 10 },
      scale: true,
      axisLabel: { ...AXIS_LABEL, formatter: (v: number) => `$${v.toLocaleString('en-US')}` },
      splitLine: { lineStyle: { color: '#37415133' } },
    },
    series: [
      {
        name: 'price',
        type: 'line' as const,
        showSymbol: false,
        smooth: false,
        lineStyle: { color: PRICE_COLOR, width: 2 },
        itemStyle: { color: PRICE_COLOR },
        data,
      },
    ],
  };
}

/// Cumulative net-contributed pool (deposits − withdrawals) over time, in
/// dollars. Only cycles with ledger activity carry a cumulative value.
export function buildNetFlowOption(series: SpSeriesPoint[]) {
  const data = series
    .filter((p) => p.start_time !== null && p.cumulative_fiat !== null)
    .map((p) => [(p.start_time as number) * 1000, (p.cumulative_fiat as number) / 100]);

  return {
    grid: baseGrid(),
    tooltip: {
      trigger: 'axis' as const,
      ...TOOLTIP,
      formatter: (params: LineTooltipParam[]) => {
        const [t, net] = params[0].value;
        return `${formatTimestamp(Math.round(t / 1000))}<br/>${params[0].marker}net ${formatFiat(
          Math.round(net * 100)
        )}`;
      },
    },
    xAxis: timeAxis(),
    yAxis: {
      type: 'value' as const,
      name: 'net contributed',
      nameTextStyle: { color: '#9ca3af', fontSize: 10 },
      axisLabel: { ...AXIS_LABEL, formatter: (v: number) => `$${v.toLocaleString('en-US')}` },
      splitLine: { lineStyle: { color: '#37415133' } },
    },
    series: [
      {
        name: 'net',
        type: 'line' as const,
        showSymbol: false,
        areaStyle: { color: `${NETFLOW_COLOR}33` },
        lineStyle: { color: NETFLOW_COLOR, width: 2 },
        itemStyle: { color: NETFLOW_COLOR },
        data,
      },
    ],
  };
}

/// Signed fiat delta of one folded account operation: deposits/incoming
/// transfers add, withdrawals/outgoing transfers subtract. Exported for tests.
export function signedFiatDelta(tx: SpAccountTx): number {
  const fiat = tx.fiat_amount ?? 0;
  if (fiat === 0) {
    return 0;
  }
  // withdrawals and outgoing transfers reduce the position; deposits and
  // incoming transfers increase it.
  return tx.kind === 'withdraw' || tx.kind === 'transfer_out' ? -fiat : fiat;
}

/// Cumulative net position of a single account over time, built from its folded
/// transaction list (any order). Points without a timestamp are skipped for the
/// time axis but still counted in the running total.
export function buildAccountNetOption(txs: SpAccountTx[]) {
  const ordered = [...txs].sort((a, b) => a.session_index - b.session_index);
  let running = 0;
  const data: [number, number][] = [];
  for (const tx of ordered) {
    running += signedFiatDelta(tx);
    if (tx.timestamp !== null) {
      data.push([tx.timestamp * 1000, running / 100]);
    }
  }

  return {
    grid: baseGrid(),
    tooltip: {
      trigger: 'axis' as const,
      ...TOOLTIP,
      formatter: (params: LineTooltipParam[]) => {
        const [t, net] = params[0].value;
        return `${formatTimestamp(Math.round(t / 1000))}<br/>${params[0].marker}${formatFiat(
          Math.round(net * 100),
          true
        )}`;
      },
    },
    xAxis: timeAxis(),
    yAxis: {
      type: 'value' as const,
      name: 'net position',
      nameTextStyle: { color: '#9ca3af', fontSize: 10 },
      axisLabel: { ...AXIS_LABEL, formatter: (v: number) => `$${v.toLocaleString('en-US')}` },
      splitLine: { lineStyle: { color: '#37415133' } },
    },
    series: [
      {
        name: 'net position',
        type: 'line' as const,
        step: 'end' as const,
        showSymbol: true,
        symbolSize: 5,
        lineStyle: { color: ACCOUNT_NET_COLOR, width: 2 },
        itemStyle: { color: ACCOUNT_NET_COLOR },
        data,
      },
    ],
  };
}
