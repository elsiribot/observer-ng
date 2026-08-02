use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Decodable;
use postgres_from_row::FromRow;
use tokio_postgres::{Error, Row};

/// A federation as stored in the core `federations` table.
#[derive(Debug, Clone)]
pub struct Federation {
    pub federation_id: FederationId,
    pub config: ClientConfig,
}

impl FromRow for Federation {
    fn from_row(row: &Row) -> Self {
        Self::try_from_row(row).expect("Decoding row failed")
    }

    fn try_from_row(row: &Row) -> Result<Self, Error> {
        let federation_id_bytes: Vec<u8> = row.try_get("federation_id")?;
        let federation_id =
            FederationId::consensus_decode_whole(&federation_id_bytes, &Default::default())
                .expect("Invalid data in DB");

        let config_bytes: Vec<u8> = row.try_get("config")?;
        let config = ClientConfig::consensus_decode_whole(&config_bytes, &Default::default())
            .expect("Invalid data in DB");

        Ok(Federation {
            federation_id,
            config,
        })
    }
}
