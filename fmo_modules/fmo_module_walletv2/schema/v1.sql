-- Exact on-chain balance tracking for walletv2.
--
-- walletv2 keeps the whole federation's on-chain funds in a single
-- consolidated UTXO. Every wallet transition (deposit or withdrawal) spends
-- the current UTXO and creates the new consolidated UTXO at vout 0. The txid
-- of every such transition is announced in consensus via
-- `WalletConsensusItem::Signatures(txid, ..)` (one item per signing peer, so
-- several rows can share a txid). The UTXO *value* is not in consensus, so it
-- is resolved out-of-band from a block explorer (esplora) by a background task
-- that fills in `utxo_value_msat` = `esplora(txid).output[0].value * 1000`.
--
-- One row per Signatures consensus item, ordered by (session_index,
-- item_index). The most recent RESOLVED row is the federation's current
-- on-chain balance.
CREATE TABLE wallet_utxos
(
    federation_id   BYTEA     NOT NULL REFERENCES public.federations (federation_id),
    session_index   INTEGER   NOT NULL,
    item_index      INTEGER   NOT NULL,
    -- On-chain txid of the transition, internal byte order (as produced by
    -- `bitcoin::Txid::to_byte_array`); reconstruct with `Txid::from_slice`.
    txid            BYTEA     NOT NULL,
    -- Value of the new consolidated UTXO (output at vout 0) in millisats.
    -- NULL until the background resolver looks it up on an explorer.
    utxo_value_msat BIGINT,
    -- Address of the new consolidated UTXO (output at vout 0). NULL until
    -- resolved, or if the script has no standard address encoding.
    address         TEXT,
    resolved_at     TIMESTAMP,
    PRIMARY KEY (federation_id, session_index, item_index)
);

-- Resolver scans for unresolved rows.
CREATE INDEX walletv2_wallet_utxos_unresolved
    ON wallet_utxos (federation_id)
    WHERE utxo_value_msat IS NULL;

-- Resolving one txid updates every row sharing it (peers announce the same
-- txid separately); and the balance lookup selects by txid too.
CREATE INDEX walletv2_wallet_utxos_txid ON wallet_utxos (federation_id, txid);

-- Balance lookup picks the latest resolved row.
CREATE INDEX walletv2_wallet_utxos_latest
    ON wallet_utxos (federation_id, session_index DESC, item_index DESC)
    WHERE utxo_value_msat IS NOT NULL;
