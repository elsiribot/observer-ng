//! SQL fragments shared by more than one API query, so they can't drift out
//! of sync with each other.

/// Resolves `user_tx_key` + `user_tx_kind` + `direction` + `role` for a
/// `transactions` row aliased `t` in the enclosing query, via a single
/// LATERAL join -- avoiding row multiplication if a txid ever maps to more
/// than one `user_tx_key` (`LIMIT 1`) and avoiding separate correlated
/// subqueries. Used by `sessions::federation_session_items` and by
/// `consensus::{TRANSACTION_ONLY_QUERY, ALL_QUERY}`.
// language=postgresql
pub(super) const USER_TX_LATERAL: &str = "
    LEFT JOIN LATERAL (
        SELECT encode(utt.user_tx_key,'hex') AS user_tx_key, ut.kind AS user_tx_kind, ut.direction, utt.role AS role
        FROM user_transaction_txs utt
        JOIN user_transactions ut
          ON ut.federation_id = utt.federation_id AND ut.user_tx_key = utt.user_tx_key
        WHERE utt.federation_id = t.federation_id AND utt.txid = t.txid
        LIMIT 1
    ) uxt ON true
";
