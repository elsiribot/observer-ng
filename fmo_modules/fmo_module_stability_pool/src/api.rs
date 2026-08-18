//! HTTP API for the stability-pool gold layer, mounted by core at
//! `/federations/:federation_id/modules/multi_sig_stability_pool/…`.
//!
//! Handlers get a raw pooled connection (default `search_path`), so every table
//! is fully schema-qualified (`fmo_multi_sig_stability_pool.*`). All fiat
//! values are the federation's stable-currency base unit; timestamps are unix
//! seconds (the naive-UTC columns are read `AT TIME ZONE 'UTC'`).

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fmo_api_types::{
    SpAccount, SpAccountTx, SpAccountTxPage, SpAccountsPage, SpCycle, SpSeriesPoint, SpSummary,
    SpTransferEdge, SpTxAccount,
};
use fmo_core::api::ModuleApiState;
use fmo_core::query::{query, query_one, query_opt};
use postgres_from_row::FromRow;
use serde::Deserialize;

const SCHEMA: &str = "fmo_multi_sig_stability_pool";

/// SQL that derives an account's type from its bech32m HRP prefix, so even
/// deposit-only accounts (whose full `Account` was never observed) are typed.
const ACC_TYPE_SQL: &str = "CASE
    WHEN account_id LIKE 'sps1%' THEN 'seeker'
    WHEN account_id LIKE 'spp1%' THEN 'provider'
    WHEN account_id LIKE 'spd1%' THEN 'btc_depositor'
    ELSE 'unknown' END";

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

pub fn router() -> Router<ModuleApiState> {
    Router::new()
        .route("/summary", get(get_summary))
        .route("/accounts", get(get_accounts))
        .route("/account/:account_id", get(get_account))
        .route(
            "/account/:account_id/transactions",
            get(get_account_transactions),
        )
        .route("/account/:account_id/transfers", get(get_account_transfers))
        .route("/cycles", get(get_cycles))
        .route("/series", get(get_series))
        .route("/tx/:txid/accounts", get(get_tx_accounts))
}

// --- /summary --------------------------------------------------------------

async fn get_summary(
    Path(federation_id): Path<FederationId>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<SpSummary>> {
    #[derive(FromRow)]
    struct Row {
        account_count: i64,
        seeker_count: i64,
        provider_count: i64,
        btc_depositor_count: i64,
        multisig_count: i64,
        net_msat: i64,
        net_fiat: i64,
        latest_cycle_index: Option<i64>,
        latest_price_fiat: Option<i64>,
        cycle_count: i64,
    }

    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();
    let sql = format!(
        "SELECT
           (SELECT COUNT(*) FROM {SCHEMA}.account_totals WHERE federation_id=$1) AS account_count,
           (SELECT COUNT(*) FROM {SCHEMA}.account_totals WHERE federation_id=$1 AND account_id LIKE 'sps1%') AS seeker_count,
           (SELECT COUNT(*) FROM {SCHEMA}.account_totals WHERE federation_id=$1 AND account_id LIKE 'spp1%') AS provider_count,
           (SELECT COUNT(*) FROM {SCHEMA}.account_totals WHERE federation_id=$1 AND account_id LIKE 'spd1%') AS btc_depositor_count,
           (SELECT COUNT(*) FROM {SCHEMA}.account_totals WHERE federation_id=$1 AND is_multisig) AS multisig_count,
           COALESCE((SELECT cumulative_msat FROM {SCHEMA}.pool_flows WHERE federation_id=$1 ORDER BY cycle_index DESC LIMIT 1), 0) AS net_msat,
           COALESCE((SELECT cumulative_fiat FROM {SCHEMA}.pool_flows WHERE federation_id=$1 ORDER BY cycle_index DESC LIMIT 1), 0) AS net_fiat,
           (SELECT MAX(cycle_index) FROM {SCHEMA}.cycles WHERE federation_id=$1) AS latest_cycle_index,
           (SELECT start_price_fiat FROM {SCHEMA}.cycles WHERE federation_id=$1 ORDER BY cycle_index DESC LIMIT 1) AS latest_price_fiat,
           (SELECT COUNT(*) FROM {SCHEMA}.cycles WHERE federation_id=$1) AS cycle_count"
    );
    let r = query_one::<Row>(&conn, &sql, &[&fed]).await?;
    Ok(Json(SpSummary {
        account_count: r.account_count,
        seeker_count: r.seeker_count,
        provider_count: r.provider_count,
        btc_depositor_count: r.btc_depositor_count,
        multisig_count: r.multisig_count,
        net_msat: r.net_msat,
        net_fiat: r.net_fiat,
        latest_cycle_index: r.latest_cycle_index,
        latest_price_fiat: r.latest_price_fiat,
        cycle_count: r.cycle_count,
    }))
}

// --- /accounts -------------------------------------------------------------

#[derive(Deserialize)]
struct AccountsParams {
    order: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(FromRow)]
struct AccountRow {
    account_id: String,
    acc_type: String,
    is_multisig: bool,
    threshold: Option<i64>,
    n_keys: Option<i64>,
    msat_deposited: i64,
    msat_withdrawn: i64,
    msat_net: i64,
    fiat_deposited: i64,
    fiat_withdrawn: i64,
    fiat_net: i64,
    transfers_in_fiat: i64,
    transfers_out_fiat: i64,
    tx_count: i64,
    first_seen: Option<i64>,
    last_seen: Option<i64>,
    first_session: Option<i64>,
    last_session: Option<i64>,
}

impl From<AccountRow> for SpAccount {
    fn from(r: AccountRow) -> Self {
        SpAccount {
            account_id: r.account_id,
            acc_type: r.acc_type,
            is_multisig: r.is_multisig,
            threshold: r.threshold,
            n_keys: r.n_keys,
            msat_deposited: r.msat_deposited,
            msat_withdrawn: r.msat_withdrawn,
            msat_net: r.msat_net,
            fiat_deposited: r.fiat_deposited,
            fiat_withdrawn: r.fiat_withdrawn,
            fiat_net: r.fiat_net,
            transfers_in_fiat: r.transfers_in_fiat,
            transfers_out_fiat: r.transfers_out_fiat,
            tx_count: r.tx_count,
            first_seen: r.first_seen,
            last_seen: r.last_seen,
            first_session: r.first_session,
            last_session: r.last_session,
        }
    }
}

/// Columns exposed by the `account_totals` matview, in the order every account
/// query selects them. The two timestamp columns are read as unix seconds.
fn account_select() -> String {
    format!(
        "account_id, {ACC_TYPE_SQL} AS acc_type, is_multisig, threshold, n_keys,
         msat_deposited, msat_withdrawn, msat_net,
         fiat_deposited, fiat_withdrawn, fiat_net,
         transfers_in_fiat, transfers_out_fiat, tx_count,
         EXTRACT(EPOCH FROM first_seen AT TIME ZONE 'UTC')::bigint AS first_seen,
         EXTRACT(EPOCH FROM last_seen  AT TIME ZONE 'UTC')::bigint AS last_seen,
         first_session, last_session"
    )
}

async fn get_accounts(
    Path(federation_id): Path<FederationId>,
    Query(params): Query<AccountsParams>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<SpAccountsPage>> {
    // Whitelisted sort column (never interpolate user text into SQL).
    let order_col = match params.order.as_deref() {
        Some("net") => "fiat_net",
        Some("activity") => "tx_count",
        _ => "last_session",
    };
    let limit = clamp_limit(params.limit);
    let offset = params.offset.unwrap_or(0).max(0);

    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();

    let total: i64 = query_one::<CountRow>(
        &conn,
        &format!("SELECT COUNT(*) AS n FROM {SCHEMA}.account_totals WHERE federation_id=$1"),
        &[&fed],
    )
    .await?
    .n;

    let sql = format!(
        "SELECT {select} FROM {SCHEMA}.account_totals
         WHERE federation_id=$1
         ORDER BY {order_col} DESC NULLS LAST, account_id
         LIMIT $2 OFFSET $3",
        select = account_select(),
    );
    let items = query::<AccountRow>(&conn, &sql, &[&fed, &limit, &offset])
        .await?
        .into_iter()
        .map(SpAccount::from)
        .collect();
    Ok(Json(SpAccountsPage { items, total }))
}

#[derive(FromRow)]
struct CountRow {
    n: i64,
}

// --- /account/:id ----------------------------------------------------------

async fn get_account(
    Path((federation_id, account_id)): Path<(FederationId, String)>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<SpAccount>> {
    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();
    let sql = format!(
        "SELECT {select} FROM {SCHEMA}.account_totals
         WHERE federation_id=$1 AND account_id=$2",
        select = account_select(),
    );
    let row = query_opt::<AccountRow>(&conn, &sql, &[&fed, &account_id])
        .await?
        .ok_or_else(|| anyhow::anyhow!("account not found"))?;
    Ok(Json(row.into()))
}

// --- /account/:id/transactions ---------------------------------------------

#[derive(Deserialize)]
struct AccountTxParams {
    before_session: Option<i64>,
    before_tx_key: Option<String>,
    limit: Option<i64>,
}

#[derive(FromRow)]
struct AccountTxRow {
    tx_key: String,
    kind: String,
    direction: String,
    amount_msat: Option<i64>,
    fiat_amount: Option<i64>,
    fiat_is_target: bool,
    cycle_index: Option<i64>,
    cycle_price_fiat: Option<i64>,
    session_index: i64,
    timestamp: Option<i64>,
    primary_txid: String,
    secondary_txid: Option<String>,
    counterparty: Option<String>,
}

async fn get_account_transactions(
    Path((federation_id, account_id)): Path<(FederationId, String)>,
    Query(params): Query<AccountTxParams>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<SpAccountTxPage>> {
    let limit = clamp_limit(params.limit);
    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();

    // Keyset by (session_index, tx_key) DESC. counterparty is resolved from the
    // transfers table only for transfer rows.
    let sql = format!(
        "SELECT a.tx_key, a.kind, a.direction, a.amount_msat, a.fiat_amount, a.fiat_is_target,
                a.cycle_index, a.cycle_price_fiat, a.session_index::bigint AS session_index,
                EXTRACT(EPOCH FROM a.timestamp AT TIME ZONE 'UTC')::bigint AS timestamp,
                encode(a.primary_txid, 'hex') AS primary_txid,
                encode(a.secondary_txid, 'hex') AS secondary_txid,
                tr.counterparty
         FROM {SCHEMA}.account_tx a
         LEFT JOIN LATERAL (
             SELECT CASE WHEN a.kind='transfer_out' THEN t.to_account_id
                         ELSE t.from_account_id END AS counterparty
             FROM {SCHEMA}.transfers t
             WHERE t.federation_id=a.federation_id AND t.txid=a.primary_txid
               AND ((a.kind='transfer_out' AND t.from_account_id=a.account_id)
                 OR (a.kind='transfer_in'  AND t.to_account_id=a.account_id))
             LIMIT 1
         ) tr ON a.kind IN ('transfer_in','transfer_out')
         WHERE a.federation_id=$1 AND a.account_id=$2
           AND ($3::bigint IS NULL OR (a.session_index, a.tx_key) < ($3::bigint, $4::text))
         ORDER BY a.session_index DESC, a.tx_key DESC
         LIMIT $5"
    );
    let rows = query::<AccountTxRow>(
        &conn,
        &sql,
        &[
            &fed,
            &account_id,
            &params.before_session,
            &params.before_tx_key,
            &limit,
        ],
    )
    .await?;

    let next = (rows.len() as i64 == limit)
        .then(|| rows.last().map(|r| (r.session_index, r.tx_key.clone())))
        .flatten();
    let items = rows
        .into_iter()
        .map(|r| SpAccountTx {
            tx_key: r.tx_key,
            kind: r.kind,
            direction: r.direction,
            amount_msat: r.amount_msat,
            fiat_amount: r.fiat_amount,
            fiat_is_target: r.fiat_is_target,
            cycle_index: r.cycle_index,
            cycle_price_fiat: r.cycle_price_fiat,
            session_index: r.session_index,
            timestamp: r.timestamp,
            primary_txid: r.primary_txid,
            secondary_txid: r.secondary_txid,
            counterparty: r.counterparty,
        })
        .collect();
    Ok(Json(SpAccountTxPage { items, next }))
}

// --- /account/:id/transfers ------------------------------------------------

async fn get_account_transfers(
    Path((federation_id, account_id)): Path<(FederationId, String)>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<SpTransferEdge>>> {
    #[derive(FromRow)]
    struct EdgeRow {
        counterparty: String,
        direction: String,
        total_fiat: i64,
        n: i64,
        first_session: Option<i64>,
        last_session: Option<i64>,
    }

    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();
    let sql = format!(
        "SELECT to_account_id AS counterparty, 'out' AS direction, total_fiat, n,
                first_session::bigint AS first_session, last_session::bigint AS last_session
         FROM {SCHEMA}.transfer_edges WHERE federation_id=$1 AND from_account_id=$2
         UNION ALL
         SELECT from_account_id, 'in', total_fiat, n,
                first_session::bigint, last_session::bigint
         FROM {SCHEMA}.transfer_edges WHERE federation_id=$1 AND to_account_id=$2
         ORDER BY total_fiat DESC"
    );
    let items = query::<EdgeRow>(&conn, &sql, &[&fed, &account_id])
        .await?
        .into_iter()
        .map(|r| SpTransferEdge {
            counterparty: r.counterparty,
            direction: r.direction,
            total_fiat: r.total_fiat,
            n: r.n,
            first_session: r.first_session,
            last_session: r.last_session,
        })
        .collect();
    Ok(Json(items))
}

// --- /cycles ---------------------------------------------------------------

#[derive(Deserialize)]
struct CyclesParams {
    before: Option<i64>,
    limit: Option<i64>,
}

async fn get_cycles(
    Path(federation_id): Path<FederationId>,
    Query(params): Query<CyclesParams>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<SpCycle>>> {
    #[derive(FromRow)]
    struct CycleRow {
        cycle_index: i64,
        start_price_fiat: i64,
        start_time: Option<i64>,
        num_votes: i64,
    }

    let limit = clamp_limit(params.limit);
    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();
    let sql = format!(
        "SELECT cycle_index, start_price_fiat,
                EXTRACT(EPOCH FROM start_time AT TIME ZONE 'UTC')::bigint AS start_time,
                num_votes
         FROM {SCHEMA}.cycles
         WHERE federation_id=$1 AND ($2::bigint IS NULL OR cycle_index < $2::bigint)
         ORDER BY cycle_index DESC
         LIMIT $3"
    );
    let items = query::<CycleRow>(&conn, &sql, &[&fed, &params.before, &limit])
        .await?
        .into_iter()
        .map(|r| SpCycle {
            cycle_index: r.cycle_index,
            start_price_fiat: r.start_price_fiat,
            start_time: r.start_time,
            num_votes: r.num_votes,
        })
        .collect();
    Ok(Json(items))
}

// --- /series (per-cycle price + cumulative net flow, ascending) ------------

async fn get_series(
    Path(federation_id): Path<FederationId>,
    Query(params): Query<CyclesParams>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<SpSeriesPoint>>> {
    #[derive(FromRow)]
    struct SeriesRow {
        cycle_index: i64,
        start_time: Option<i64>,
        price_fiat: i64,
        cumulative_msat: Option<i64>,
        cumulative_fiat: Option<i64>,
    }

    // A chart wants the whole (small) series; allow a larger cap than the
    // row-listing endpoints.
    let limit = params.limit.unwrap_or(5000).clamp(1, 20000);
    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();
    let sql = format!(
        "SELECT c.cycle_index,
                EXTRACT(EPOCH FROM c.start_time AT TIME ZONE 'UTC')::bigint AS start_time,
                c.start_price_fiat AS price_fiat,
                pf.cumulative_msat, pf.cumulative_fiat
         FROM {SCHEMA}.cycles c
         LEFT JOIN {SCHEMA}.pool_flows pf USING (federation_id, cycle_index)
         WHERE c.federation_id=$1
         ORDER BY c.cycle_index ASC
         LIMIT $2"
    );
    let items = query::<SeriesRow>(&conn, &sql, &[&fed, &limit])
        .await?
        .into_iter()
        .map(|r| SpSeriesPoint {
            cycle_index: r.cycle_index,
            start_time: r.start_time,
            price_fiat: r.price_fiat,
            cumulative_msat: r.cumulative_msat,
            cumulative_fiat: r.cumulative_fiat,
        })
        .collect();
    Ok(Json(items))
}

// --- /tx/:txid/accounts ----------------------------------------------------

async fn get_tx_accounts(
    Path((federation_id, txid)): Path<(FederationId, String)>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<SpTxAccount>>> {
    #[derive(FromRow)]
    struct TxAccountRow {
        side: String,
        index: i32,
        account_id: String,
        kind: String,
        counterparty: Option<String>,
    }

    let conn = state.pool.get().await?;
    let fed = federation_id.consensus_encode_to_vec();
    let sql = format!(
        "SELECT 'output' AS side, out_index AS index, account_id, action AS kind,
                NULL::text AS counterparty
         FROM {SCHEMA}.deposits WHERE federation_id=$1 AND txid=decode($2,'hex')
         UNION ALL
         SELECT 'output', out_index, from_account_id, 'transfer', to_account_id
         FROM {SCHEMA}.transfers WHERE federation_id=$1 AND txid=decode($2,'hex')
         UNION ALL
         SELECT 'input', in_index, account_id, kind, NULL
         FROM {SCHEMA}.withdrawals WHERE federation_id=$1 AND txid=decode($2,'hex')
         ORDER BY side, index"
    );
    let items = query::<TxAccountRow>(&conn, &sql, &[&fed, &txid])
        .await?
        .into_iter()
        .map(|r| SpTxAccount {
            side: r.side,
            index: r.index,
            account_id: r.account_id,
            kind: r.kind,
            counterparty: r.counterparty,
        })
        .collect();
    Ok(Json(items))
}
