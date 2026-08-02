use deadpool_postgres::Runtime;
use fedimint_core::config::ClientConfig;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Decodable;
use fedimint_core::session_outcome::SessionOutcome;
use tokio_postgres::NoTls;
use tracing::info;

use crate::ingest::ingest_session;
use crate::registry::ModuleRegistry;

/// Imports federations, raw sessions and block times from a pre-v0.2
/// (schema v8) Fedimint Observer database into a fresh new-schema database.
///
/// Only bronze-layer data is copied; all derived data is rebuilt by the
/// structural ingest (here) and by module replay (once the server runs,
/// since every module cursor starts at 0). Works for federations that are
/// no longer reachable since no network access is needed.
pub async fn import(
    old_db: &str,
    new_db: &str,
    registry: &ModuleRegistry,
) -> anyhow::Result<()> {
    let (old, old_connection) = tokio_postgres::connect(old_db, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = old_connection.await {
            tracing::error!("old DB connection error: {e}");
        }
    });

    let pool = {
        let pool_config = deadpool_postgres::Config {
            url: Some(new_db.to_owned()),
            ..Default::default()
        };
        pool_config.create_pool(Some(Runtime::Tokio1), NoTls)
    }?;

    crate::db::migrations::setup_core_schema(&pool).await?;

    // 1. federations (raw config bytes are decodable across versions since
    //    the encoding is consensus-critical)
    let federation_rows = old
        .query("SELECT federation_id, config FROM federations", &[])
        .await?;
    info!("Importing {} federations", federation_rows.len());
    let conn = pool.get().await?;
    let mut federations = Vec::new();
    for row in &federation_rows {
        let federation_id_bytes: Vec<u8> = row.get(0);
        let config_bytes: Vec<u8> = row.get(1);
        let federation_id =
            FederationId::consensus_decode_whole(&federation_id_bytes, &Default::default())?;
        let config = ClientConfig::consensus_decode_whole(
            &config_bytes,
            &ModuleRegistry::fallback_decoders(),
        )?;
        conn.execute(
            "INSERT INTO federations VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[&federation_id_bytes, &config_bytes],
        )
        .await?;
        federations.push((federation_id, config));
    }
    drop(conn);

    // 2. block times (bulk copy saves re-fetching from esplora)
    let block_time_rows = old
        .query("SELECT block_height, timestamp FROM block_times", &[])
        .await?;
    info!("Importing {} block times", block_time_rows.len());
    let mut conn = pool.get().await?;
    let dbtx = conn.transaction().await?;
    for chunk in block_time_rows.chunks(10_000) {
        for row in chunk {
            let height: i32 = row.get(0);
            let timestamp: chrono::NaiveDateTime = row.get(1);
            dbtx.execute(
                "INSERT INTO block_times VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&height, &timestamp],
            )
            .await?;
        }
    }
    dbtx.commit().await?;
    drop(conn);

    // 3. raw sessions, re-ingested through the structural pipeline. This is
    //    also the encoding round-trip check: any blob the current fedimint
    //    version can't decode aborts the import with a precise error.
    for (federation_id, config) in &federations {
        let old_session_count: i64 = old
            .query_one(
                "SELECT COUNT(*) FROM sessions WHERE federation_id = $1",
                &[&federation_id_bytes(federation_id)],
            )
            .await?
            .get(0);
        info!("Importing {old_session_count} sessions for federation {federation_id}");

        let decoders = ModuleRegistry::fallback_decoders();
        let mut imported: i64 = 0;
        const BATCH: i64 = 500;
        loop {
            // The old `sessions` table stores raw bytes in the `session`
            // column (new schema calls it `data`).
            let rows = old
                .query(
                    "SELECT session_index, session FROM sessions
                     WHERE federation_id = $1 AND session_index >= $2
                     ORDER BY session_index LIMIT $3",
                    &[
                        &federation_id_bytes(federation_id),
                        &(imported as i32),
                        &BATCH,
                    ],
                )
                .await?;
            if rows.is_empty() {
                break;
            }

            let mut conn = pool.get().await?;
            let dbtx = conn.transaction().await?;
            for row in &rows {
                let session_index: i32 = row.get(0);
                let data: Vec<u8> = row.get(1);
                let session = SessionOutcome::consensus_decode_whole(&data, &decoders)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to decode session {session_index} of federation \
                             {federation_id}: {e}"
                        )
                    })?;
                ingest_session(&dbtx, config, *federation_id, session_index as u64, &session)
                    .await?;
            }
            dbtx.commit().await?;

            imported += rows.len() as i64;
            if imported % 5_000 < BATCH {
                let percentage = (imported as f64) / (old_session_count.max(1) as f64) * 100.0;
                info!("  {imported}/{old_session_count} sessions ({percentage:.1}%)");
            }
        }

        // 4. verify per federation
        let new_session_count: i64 = pool
            .get()
            .await?
            .query_one(
                "SELECT COUNT(*) FROM sessions WHERE federation_id = $1",
                &[&federation_id_bytes(federation_id)],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            old_session_count == new_session_count,
            "Session count mismatch for federation {federation_id}: old {old_session_count}, new {new_session_count}"
        );
        info!("Federation {federation_id}: {new_session_count} sessions imported and verified");
    }

    let module_kinds = registry
        .iter()
        .map(|(kind, _)| kind.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    info!(
        "Import complete. On the next 'serve' run the installed modules ({module_kinds}) \
         will replay all sessions to rebuild their normalized data."
    );

    Ok(())
}

fn federation_id_bytes(federation_id: &FederationId) -> Vec<u8> {
    use fedimint_core::encoding::Encodable;
    federation_id.consensus_encode_to_vec()
}
