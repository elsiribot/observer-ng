use std::time::{Duration, SystemTime};

use chrono::DateTime;
use futures::StreamExt;
use tracing::{info, warn};

use crate::observer::FederationObserver;
use crate::query::query_value;

impl FederationObserver {
    /// Seeds the block_times table from the bundled snapshot if empty.
    pub(crate) async fn seed_block_times(&self) -> anyhow::Result<()> {
        if query_value::<i64>(
            &self.connection().await?,
            "SELECT COUNT(*)::bigint FROM block_times",
            &[],
        )
        .await?
            == 0
        {
            self.connection()
                .await?
                .batch_execute(include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/schema/block_times.sql"
                )))
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn fetch_block_times(self) {
        const SLEEP_SECS: u64 = 60;
        loop {
            if let Err(e) = self.fetch_block_times_inner().await {
                warn!("Error while fetching block times: {e:?}");
            }
            info!("Block sync finished, waiting {SLEEP_SECS} seconds");
            tokio::time::sleep(Duration::from_secs(SLEEP_SECS)).await;
        }
    }

    async fn fetch_block_times_inner(&self) -> anyhow::Result<()> {
        let esplora_client = self.services().esplora()?;

        // TODO: find a better way to pre-seed the DB so we don't have to bother
        // blockstream.info Block 820k was mined Dec 2023, afaik there are no
        // compatible federations older than that
        let next_block_height = self.last_fetched_block_height().await?.unwrap_or(820_000) + 1;
        let current_block_height = esplora_client.get_height().await?;

        info!("Fetching block times for block {next_block_height} to {current_block_height}");

        let mut block_stream = futures::stream::iter(next_block_height..=current_block_height)
            .map(move |block_height| {
                let esplora_client_inner = esplora_client.clone();
                async move {
                    let block_hash = esplora_client_inner.get_block_hash(block_height).await?;
                    let block = esplora_client_inner.get_header_by_hash(&block_hash).await?;

                    Result::<_, anyhow::Error>::Ok((block_height, block))
                }
            })
            .buffered(4);

        let mut timer = SystemTime::now();
        let mut last_log_height = next_block_height;
        while let Some((block_height, block)) = block_stream.next().await.transpose()? {
            self.connection()
                .await?
                .execute(
                    "INSERT INTO block_times VALUES ($1, $2)",
                    &[
                        &(block_height as i32),
                        &DateTime::from_timestamp(block.time as i64, 0)
                            .expect("Invalid timestamp")
                            .naive_utc(),
                    ],
                )
                .await?;

            // TODO: write abstraction
            let elapsed = timer.elapsed().unwrap_or_default();
            if elapsed >= Duration::from_secs(5) {
                let blocks_synced = block_height - last_log_height;
                let rate = (blocks_synced as f64) / elapsed.as_secs_f64();
                info!("Synced up to block {block_height}, processed {blocks_synced} blocks at a rate of {rate:.2} blocks/s");
                timer = SystemTime::now();
                last_log_height = block_height;
            }
        }

        Ok(())
    }

    async fn last_fetched_block_height(&self) -> anyhow::Result<Option<u32>> {
        let max_height = query_value::<Option<i32>>(
            &self.connection().await?,
            "SELECT MAX(block_height) AS max_height FROM block_times",
            &[],
        )
        .await?;

        Ok(max_height.map(|max_height| max_height as u32))
    }

    pub async fn get_block_height(&self) -> anyhow::Result<u32> {
        Ok(query_value::<i32>(
            &self.connection().await?,
            "SELECT MAX(block_height) FROM block_times",
            &[],
        )
        .await? as u32)
    }
}
