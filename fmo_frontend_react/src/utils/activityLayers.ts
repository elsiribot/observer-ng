// Maps the fine-grained gold/matview transaction `kind` strings onto the small
// set of display layers used by the stacked activity chart, and assembles the
// per-day stacked series for a given grain (user vs fedimint) and metric
// (volume vs count). Pure and unit-tested independently of the chart.

// One inner cell of the stacked histogram response: a (day, kind) aggregate.
// `amount_transferred` is in millisatoshis (fedimint `Amount` serializes as a
// bare integer).
export interface ActivityCell {
  num_transactions: number;
  amount_transferred: number;
}

// date (ISO "YYYY-MM-DD") -> kind -> aggregate.
export type ActivityByDay = Record<string, Record<string, ActivityCell>>;

// The /transactions/histogram/stacked response: both grains in one payload.
export interface StackedActivityResponse {
  user: ActivityByDay;
  fedimint: ActivityByDay;
}

export interface DisplayLayer {
  key: string;
  label: string;
  color: string;
}

// Display layers, ordered bottom -> top in the stack. Colors are chosen to read
// on both themes and to echo the existing blue used elsewhere for ecash.
export const ACTIVITY_LAYERS: DisplayLayer[] = [
  { key: 'peg_in', label: 'Peg-in', color: '#10b981' },
  { key: 'peg_out', label: 'Peg-out', color: '#f59e0b' },
  { key: 'ecash', label: 'Ecash', color: '#3b82f6' },
  { key: 'lightning', label: 'Lightning', color: '#8b5cf6' },
  { key: 'stability_pool', label: 'Stability Pool', color: '#ec4899' },
  { key: 'other', label: 'Other', color: '#6b7280' },
];

const KIND_TO_LAYER: Record<string, string> = {
  peg_in: 'peg_in',
  peg_in_v2: 'peg_in',
  peg_out: 'peg_out',
  peg_out_v2: 'peg_out',
  ecash_transfer: 'ecash',
  ecash_transfer_v2: 'ecash',
  ln_send: 'lightning',
  ln_receive: 'lightning',
  lnv2_send: 'lightning',
  lnv2_receive: 'lightning',
  lightning: 'lightning',
  stability_pool: 'stability_pool',
  other: 'other',
};

// Maps a raw kind to its display-layer key; anything unrecognized falls to
// 'other' so a new backend kind never silently vanishes from the total.
export function kindToLayer(kind: string): string {
  return KIND_TO_LAYER[kind] ?? 'other';
}

export interface LayerSeries {
  key: string;
  label: string;
  color: string;
  data: number[];
}

export interface StackedSeries {
  // Display-formatted date labels (category axis), one per day, ascending.
  dates: string[];
  // Corresponding ISO date strings and epoch-ms timestamps (for zoom math).
  isoDates: string[];
  timestamps: number[];
  // One entry per display layer that has any nonzero value in range, in
  // ACTIVITY_LAYERS order.
  series: LayerSeries[];
}

const MSAT_PER_BTC = 100_000_000_000;

function formatDay(isoDate: string): string {
  const d = new Date(isoDate);
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

// Builds the stacked series for one grain. `metric` selects the value:
// 'volume' -> BTC (millisats / 1e11), 'count' -> transaction count. Layers with
// a zero total across all days are omitted (so e.g. Stability Pool only shows
// for federations that have any).
export function buildStackedSeries(
  byDay: ActivityByDay,
  metric: 'volume' | 'count',
): StackedSeries {
  const isoDates = Object.keys(byDay).sort();
  const dates = isoDates.map(formatDay);
  const timestamps = isoDates.map((iso) => new Date(iso).getTime());

  // layer key -> per-day values.
  const perLayer: Record<string, number[]> = {};
  for (const layer of ACTIVITY_LAYERS) {
    perLayer[layer.key] = new Array(isoDates.length).fill(0);
  }

  isoDates.forEach((iso, dayIdx) => {
    const cells = byDay[iso];
    for (const [kind, cell] of Object.entries(cells)) {
      const layerKey = kindToLayer(kind);
      const value =
        metric === 'volume'
          ? cell.amount_transferred / MSAT_PER_BTC
          : cell.num_transactions;
      perLayer[layerKey][dayIdx] += value;
    }
  });

  const series: LayerSeries[] = ACTIVITY_LAYERS.filter((layer) =>
    perLayer[layer.key].some((v) => v > 0),
  ).map((layer) => ({
    key: layer.key,
    label: layer.label,
    color: layer.color,
    data: perLayer[layer.key],
  }));

  return { dates, isoDates, timestamps, series };
}
