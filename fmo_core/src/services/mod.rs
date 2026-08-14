pub mod block_times;
pub mod guardians;
pub mod meta;
pub mod nostr;

use deadpool_postgres::Pool;
use fedimint_core::config::JsonClientConfig;
use tracing::{debug, warn};

use crate::query::query_value;
use crate::services::meta::{
    merge_metas, parse_meta_lenient, ConsensusMetaCache, MetaFields, MetaOverrideCache,
};

/// Shared infrastructure handed to modules: mempool/esplora access and
/// core lookup helpers.
#[derive(Debug, Clone)]
pub struct CoreServices {
    mempool_url: String,
    pool: Pool,
    /// Shared with [`crate::observer::FederationObserver`] so consensus meta is
    /// fetched once per federation across the API handlers and module tasks.
    consensus_meta_cache: ConsensusMetaCache,
    /// Shared with [`crate::api::AppState`] so override meta files are fetched
    /// once per URL across the API handlers and module tasks.
    meta_override_cache: MetaOverrideCache,
}

impl CoreServices {
    pub fn new(
        mempool_url: String,
        pool: Pool,
        consensus_meta_cache: ConsensusMetaCache,
        meta_override_cache: MetaOverrideCache,
    ) -> Self {
        Self {
            mempool_url,
            pool,
            consensus_meta_cache,
            meta_override_cache,
        }
    }

    pub fn mempool_url(&self) -> &str {
        &self.mempool_url
    }

    pub fn consensus_meta_cache(&self) -> &ConsensusMetaCache {
        &self.consensus_meta_cache
    }

    pub fn meta_override_cache(&self) -> &MetaOverrideCache {
        &self.meta_override_cache
    }

    /// Merges consensus meta, override-file meta and config (consensus-global)
    /// meta with lenient parsing, highest priority first. This is the single
    /// source of truth for merged federation meta; the `/config/:invite/meta`
    /// and `/federations/:id/meta` routes call it too.
    ///
    /// Never returns `Err` in practice — an override-file fetch failure falls
    /// back to config meta — but keeps a `Result` so callers can treat merged
    /// meta as best-effort.
    pub async fn merged_meta(&self, config: &JsonClientConfig) -> anyhow::Result<MetaFields> {
        let maybe_consensus_meta = self.consensus_meta_cache.fetch_meta_cached(config).await;

        let meta_fields_config = parse_meta_lenient(
            config
                .global
                .meta
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned().into())),
        );

        let maybe_meta_override = if let Some(override_url) = meta_fields_config
            .get("meta_override_url")
            .or_else(|| meta_fields_config.get("meta_external_url")) // Fedi legacy field
            .and_then(|url| url.as_str().map(ToOwned::to_owned))
        {
            debug!("fetching {override_url}");
            match self
                .meta_override_cache
                .fetch_meta_cached(&override_url, config.global.calculate_federation_id())
                .await
            {
                Ok(meta) => Some(meta),
                Err(e) => {
                    warn!("Failed to fetch meta fields from {override_url}: {e:?}");
                    return Ok(meta_fields_config);
                }
            }
        } else {
            None
        };

        Ok(merge_metas(&[
            maybe_consensus_meta.unwrap_or_default(),
            maybe_meta_override.unwrap_or_default(),
            meta_fields_config,
        ]))
    }

    pub fn esplora(&self) -> anyhow::Result<esplora_client::AsyncClient> {
        Ok(esplora_client::Builder::new(&self.mempool_url).build_async()?)
    }

    /// Timestamp of the given block height, if already synced into
    /// `block_times`.
    ///
    /// Takes a connection from the pool: safe from background tasks, but must
    /// NOT be called from `process_input`/`process_output`/`process_ci` —
    /// those hold a pooled transaction already and a second acquisition can
    /// deadlock the pool once enough federations process concurrently. Use
    /// `ProcessCtx::block_time` there instead.
    pub async fn block_time(&self, height: u32) -> anyhow::Result<Option<chrono::NaiveDateTime>> {
        query_value::<Option<chrono::NaiveDateTime>>(
            &self.pool.get().await?,
            "SELECT MAX(timestamp) FROM block_times WHERE block_height = $1",
            &[&(height as i32)],
        )
        .await
    }
}
