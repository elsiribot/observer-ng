# Federation Activity Chart: User/Fedimint modes + stacked-by-type — Design

**Goal:** Turn the federation-detail activity chart (volume / tx-count over time) into a
stacked chart broken down by transaction type, with a toggle between two grains:
"User transactions" (deduplicated, gold layer) and "Fedimint transactions" (raw).

## Decisions (confirmed with user)

- **Layers = transaction type**, one taxonomy for both modes, each tx in exactly one
  layer: **Peg-in · Peg-out · Ecash · Lightning · (Stability Pool) · Other**.
- **Stacked area** chart (keeps the current smooth-area visual style).
- Both the existing **volume** and **count** metric toggles are retained.

## Data model

Two backing rollups, one per mode, both grouped `(federation, day, kind)`:

- **User transactions** — already exists: the gold `user_tx_daily` matview
  (`federation_id, day, kind, direction → tx_count, volume_msat`). We sum over
  `direction`. Its `kind` values: `peg_in`, `peg_out`, `peg_in_v2`, `peg_out_v2`,
  `ecash_transfer`, `ecash_transfer_v2`, `ln_send`, `ln_receive`, `lnv2_send`,
  `lnv2_receive`, `stability_pool`, `other`.

- **Fedimint transactions** — new matview `federation_tx_kind_daily` (core schema
  v11). Classifies each raw fedimint tx into a single `kind` with the same taxonomy
  as the gold `fold_standalone` CASE, plus a `lightning` bucket for any tx touching
  `ln`/`lnv2` (matching gold's `AND NOT (i.kinds && ARRAY['ln','lnv2'])` guard on the
  peg cases). Grain/columns mirror `federation_tx_daily`: `tx_count` = distinct txids,
  `volume_msat` = summed input amounts. Excludes days with no session timestamp yet
  (same as `federation_tx_daily`). Unique index `(federation_id, day, kind)` enables
  `REFRESH ... CONCURRENTLY`; refreshed in the same loop as `federation_tx_daily`.

Fedimint `kind` values: `peg_in`, `peg_out`, `peg_in_v2`, `peg_out_v2`, `lightning`,
`stability_pool`, `ecash_transfer`, `ecash_transfer_v2`, `other`.

## Display taxonomy mapping (frontend)

The finer gold/raw kinds collapse to the 5–6 display layers, shared by both modes:

| display layer   | source kinds                                             |
|-----------------|----------------------------------------------------------|
| Peg-in          | `peg_in`, `peg_in_v2`                                     |
| Peg-out         | `peg_out`, `peg_out_v2`                                   |
| Ecash           | `ecash_transfer`, `ecash_transfer_v2`                    |
| Lightning       | `ln_send`, `ln_receive`, `lnv2_send`, `lnv2_receive`, `lightning` |
| Stability Pool  | `stability_pool`                                          |
| Other           | `other`, anything unmapped                               |

Stable per-layer colors; the "Stability Pool" layer only renders for federations that
have any.

## API

New endpoint `GET /federations/:id/transactions/histogram/stacked` returns BOTH modes in
one response (so the mode toggle is instant, no refetch). Cache-Control 30s, same as the
existing histogram endpoint.

```jsonc
{
  "user":     { "2024-05-31": { "ln_send": {num_transactions, amount_transferred}, ... }, ... },
  "fedimint": { "2024-05-31": { "peg_in":  {num_transactions, amount_transferred}, ... }, ... }
}
```

`amount_transferred` is an `Amount` (msats), matching the existing histogram endpoint.
The existing `/transactions/histogram` endpoint stays for backward compat and the totals
card.

## Frontend

- `TransactionChart` gains a stacked mode: N series (one per display layer present),
  `stack: 'total'`, area style, per-layer color; tooltip lists each layer + a total row.
  Moving-average / log-scale / outlier controls only apply to the (existing) single-line
  view; in stacked mode they are hidden (a moving average over a stack is not meaningful).
- `FederationDetail` gains a `grain: 'user' | 'fedimint'` toggle beside the existing
  volume/count toggle, fetches the stacked endpoint, maps kinds → layers.
- A small pure util `activityLayers.ts` (+ tests) does the kind→layer mapping and builds
  the per-layer daily series, unit-tested independently of the chart.

## Testing

- Rust: a `federation_tx_kind_daily` test asserting a mixed set of txs (peg-in, ecash,
  lightning) lands in the right kind buckets with correct counts/volume.
- Frontend: `activityLayers` mapping + series-assembly unit tests.
