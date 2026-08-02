pub mod block_times;
pub mod guardians;
pub mod meta;
pub mod nostr;

use deadpool_postgres::Pool;

use crate::query::query_value;

/// Shared infrastructure handed to modules: mempool/esplora access and
/// core lookup helpers.
#[derive(Debug, Clone)]
pub struct CoreServices {
    mempool_url: String,
    pool: Pool,
}

impl CoreServices {
    pub fn new(mempool_url: String, pool: Pool) -> Self {
        Self { mempool_url, pool }
    }

    pub fn mempool_url(&self) -> &str {
        &self.mempool_url
    }

    pub fn esplora(&self) -> anyhow::Result<esplora_client::AsyncClient> {
        Ok(esplora_client::Builder::new(&self.mempool_url).build_async()?)
    }

    /// Timestamp of the given block height, if already synced into `block_times`.
    pub async fn block_time(&self, height: u32) -> anyhow::Result<Option<chrono::NaiveDateTime>> {
        query_value::<Option<chrono::NaiveDateTime>>(
            &self.pool.get().await?,
            "SELECT MAX(timestamp) FROM block_times WHERE block_height = $1",
            &[&(height as i32)],
        )
        .await
    }
}
