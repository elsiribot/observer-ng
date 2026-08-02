use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use fedimint_core::config::{FederationId, JsonClientConfig};
use fedimint_core::core::ModuleKind;
use fedimint_core::invite_code::InviteCode;
use reqwest::Method;
use tower_http::cors::{Any, CorsLayer};
use tracing::debug;
use tracing::log::warn;

use crate::api::AppState;
use crate::error::Result;
use crate::registry::ModuleRegistry;
use crate::services::meta::{config_to_json, merge_metas, parse_meta_lenient, MetaFields};

pub fn get_config_routes() -> Router<AppState> {
    let router = Router::new()
        .route("/:invite", get(fetch_federation_config))
        .route("/:invite/meta", get(fetch_federation_meta))
        .route("/:invite/id", get(fetch_federation_id))
        .route("/:invite/module_kinds", get(fetch_federation_module_kinds));

    let cors_enabled = dotenv::var("ALLOW_CONFIG_CORS").is_ok_and(|v| v == "true");

    if cors_enabled {
        router.layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET]),
        )
    } else {
        router
    }
}

async fn fetch_federation_config(
    Path(invite): Path<InviteCode>,
    State(state): State<AppState>,
) -> Result<Json<JsonClientConfig>> {
    Ok(state
        .federation_config_cache
        .fetch_config_cached(&invite, state.observer.registry())
        .await?
        .into())
}

async fn fetch_federation_id(Path(invite): Path<InviteCode>) -> Result<Json<FederationId>> {
    Ok(invite.federation_id().into())
}

async fn fetch_federation_module_kinds(
    Path(invite): Path<InviteCode>,
    State(state): State<AppState>,
) -> Result<Json<BTreeSet<ModuleKind>>> {
    let config = state
        .federation_config_cache
        .fetch_config_cached(&invite, state.observer.registry())
        .await?;
    let module_kinds = config
        .modules
        .into_values()
        .map(|module_config| module_config.kind().to_owned())
        .collect::<BTreeSet<_>>();

    Ok(module_kinds.into())
}

async fn fetch_federation_meta(
    Path(invite): Path<InviteCode>,
    State(state): State<AppState>,
) -> Result<Json<MetaFields>> {
    let config = state
        .federation_config_cache
        .fetch_config_cached(&invite, state.observer.registry())
        .await?;

    federation_meta(&config, &state).await
}

/// Unifies config meta, consensus meta and override meta with lenient parsing.
pub(super) async fn federation_meta(
    cfg: &JsonClientConfig,
    state: &AppState,
) -> Result<Json<MetaFields>> {
    let maybe_consensus_meta = state
        .observer
        .consensus_meta_cache()
        .fetch_meta_cached(cfg)
        .await;

    let meta_fields_config = parse_meta_lenient(
        cfg.global
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
        let meta_override = match state
            .meta_override_cache
            .fetch_meta_cached(&override_url, cfg.global.calculate_federation_id())
            .await
        {
            Ok(meta) => meta,
            Err(e) => {
                warn!("Failed to fetch meta fields from {override_url}: {e:?}");
                return Ok(meta_fields_config.into());
            }
        };
        Some(meta_override)
    } else {
        None
    };

    Ok(Json(merge_metas(&[
        maybe_consensus_meta.unwrap_or_default(),
        maybe_meta_override.unwrap_or_default(),
        meta_fields_config,
    ])))
}

#[derive(Default, Debug, Clone)]
pub struct FederationConfigCache {
    federations: Arc<tokio::sync::RwLock<HashMap<FederationId, JsonClientConfig>>>,
}

impl FederationConfigCache {
    pub async fn fetch_config_cached(
        &self,
        invite: &InviteCode,
        registry: &ModuleRegistry,
    ) -> anyhow::Result<JsonClientConfig> {
        let federation_id = invite.federation_id();

        if let Some(config) = self.federations.read().await.get(&federation_id).cloned() {
            return Ok(config);
        }

        let config = fetch_config_inner(invite, registry).await?;
        let mut cache = self.federations.write().await;
        if let Some(replaced) = cache.insert(federation_id, config.clone()) {
            if replaced != config {
                warn!("Config for federation {federation_id} changed");
            }
        }

        Ok(config)
    }
}

async fn fetch_config_inner(
    invite: &InviteCode,
    registry: &ModuleRegistry,
) -> anyhow::Result<JsonClientConfig> {
    let connectors = fedimint_connectors::ConnectorRegistry::build_from_client_env()?
        .bind()
        .await?;
    let (raw_config, _api) =
        fedimint_api_client::download_from_invite_code(&connectors, invite).await?;
    config_to_json(raw_config, registry)
}
