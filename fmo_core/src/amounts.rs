use deadpool_postgres::GenericClient;

/// Fills `amount_msat` for transaction items whose consensus encoding carries
/// no value, by exploiting that fedimint transactions balance: if exactly one
/// item of a transaction has an unknown amount and every other item is known,
/// the missing value is the difference between the two sides.
///
/// Only transactions from sessions every installed module has already
/// processed are considered, so an item that is merely *not yet* processed is
/// never mistaken for one that is permanently unknowable. Inferred values are
/// marked with `"inferred": true` in the details JSON — they are estimates
/// (exact when module fees are zero), not consensus facts. The whole step is
/// idempotent and replay-safe: module dispatch overwrites these columns on
/// replay and the next run re-infers.
pub async fn infer_missing_amounts(conn: &impl GenericClient) -> anyhow::Result<(u64, u64)> {
    let inputs = conn
        .execute(
            "UPDATE public.transaction_inputs upd
             SET amount_msat = calc.inferred_amount,
                 details = COALESCE(upd.details, '{}'::jsonb) || '{\"inferred\": true}'::jsonb
             FROM (
                 SELECT i.federation_id, i.txid, i.in_index,
                        (SELECT COALESCE(SUM(o.amount_msat), 0) FROM public.transaction_outputs o
                          WHERE o.federation_id = i.federation_id AND o.txid = i.txid)
                      - (SELECT COALESCE(SUM(i2.amount_msat), 0) FROM public.transaction_inputs i2
                          WHERE i2.federation_id = i.federation_id AND i2.txid = i.txid
                            AND i2.in_index <> i.in_index) AS inferred_amount
                 FROM public.transaction_inputs i
                 JOIN public.transactions t
                   ON t.federation_id = i.federation_id AND t.txid = i.txid
                 WHERE i.amount_msat IS NULL
                   AND t.session_index < (SELECT MIN(mp.next_session_index)
                                            FROM public.module_progress mp
                                           WHERE mp.federation_id = i.federation_id)
                   AND NOT EXISTS (SELECT 1 FROM public.transaction_inputs i3
                                    WHERE i3.federation_id = i.federation_id AND i3.txid = i.txid
                                      AND i3.in_index <> i.in_index AND i3.amount_msat IS NULL)
                   AND NOT EXISTS (SELECT 1 FROM public.transaction_outputs o2
                                    WHERE o2.federation_id = i.federation_id AND o2.txid = i.txid
                                      AND o2.amount_msat IS NULL)
             ) calc
             WHERE upd.federation_id = calc.federation_id
               AND upd.txid = calc.txid
               AND upd.in_index = calc.in_index
               AND calc.inferred_amount >= 0",
            &[],
        )
        .await?;

    let outputs = conn
        .execute(
            "UPDATE public.transaction_outputs upd
             SET amount_msat = calc.inferred_amount,
                 details = COALESCE(upd.details, '{}'::jsonb) || '{\"inferred\": true}'::jsonb
             FROM (
                 SELECT o.federation_id, o.txid, o.out_index,
                        (SELECT COALESCE(SUM(i.amount_msat), 0) FROM public.transaction_inputs i
                          WHERE i.federation_id = o.federation_id AND i.txid = o.txid)
                      - (SELECT COALESCE(SUM(o2.amount_msat), 0) FROM public.transaction_outputs o2
                          WHERE o2.federation_id = o.federation_id AND o2.txid = o.txid
                            AND o2.out_index <> o.out_index) AS inferred_amount
                 FROM public.transaction_outputs o
                 JOIN public.transactions t
                   ON t.federation_id = o.federation_id AND t.txid = o.txid
                 WHERE o.amount_msat IS NULL
                   AND t.session_index < (SELECT MIN(mp.next_session_index)
                                            FROM public.module_progress mp
                                           WHERE mp.federation_id = o.federation_id)
                   AND NOT EXISTS (SELECT 1 FROM public.transaction_outputs o3
                                    WHERE o3.federation_id = o.federation_id AND o3.txid = o.txid
                                      AND o3.out_index <> o.out_index AND o3.amount_msat IS NULL)
                   AND NOT EXISTS (SELECT 1 FROM public.transaction_inputs i2
                                    WHERE i2.federation_id = o.federation_id AND i2.txid = o.txid
                                      AND i2.amount_msat IS NULL)
             ) calc
             WHERE upd.federation_id = calc.federation_id
               AND upd.txid = calc.txid
               AND upd.out_index = calc.out_index
               AND calc.inferred_amount >= 0",
            &[],
        )
        .await?;

    Ok((inputs, outputs))
}
