use std::collections::BTreeMap;
use std::sync::Arc;

use fedimint_core::config::ClientConfig;
use fedimint_core::core::{ModuleInstanceId, ModuleKind};
use fedimint_core::module::registry::ModuleDecoderRegistry;

use crate::module::ObserverModule;

/// The set of observer modules compiled into this FMO instance, keyed by
/// module kind.
#[derive(Clone)]
pub struct ModuleRegistry {
    modules: BTreeMap<ModuleKind, Arc<dyn ObserverModule>>,
}

impl ModuleRegistry {
    /// # Panics
    /// Panics if two modules declare the same kind.
    pub fn new(modules: Vec<Arc<dyn ObserverModule>>) -> Self {
        let mut map = BTreeMap::new();
        for module in modules {
            let kind = module.kind();
            if map.insert(kind.clone(), module).is_some() {
                panic!("Duplicate observer module for kind {kind}");
            }
        }
        Self { modules: map }
    }

    pub fn get(&self, kind: &ModuleKind) -> Option<&Arc<dyn ObserverModule>> {
        self.modules.get(kind)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ModuleKind, &Arc<dyn ObserverModule>)> {
        self.modules.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    /// Decoder registry for a federation: real decoders for installed module
    /// kinds, raw fallback for everything else.
    pub fn decoders(&self, config: &ClientConfig) -> ModuleDecoderRegistry {
        ModuleDecoderRegistry::new(config.modules.iter().filter_map(
            |(module_instance_id, module_config)| {
                let module = self.get(&module_config.kind)?;
                Some((
                    *module_instance_id,
                    module_config.kind.clone(),
                    module.decoder(),
                ))
            },
        ))
        .with_fallback()
    }

    /// Fallback-only registry: decodes session structure, keeps all module
    /// items as raw bytes. Used by the structural ingest layer.
    pub fn fallback_decoders() -> ModuleDecoderRegistry {
        ModuleDecoderRegistry::new(std::iter::empty::<(ModuleInstanceId, ModuleKind, _)>())
            .with_fallback()
    }
}

/// Maps a module instance id to its module kind using the federation config.
pub fn instance_to_kind(config: &ClientConfig, module_instance_id: ModuleInstanceId) -> String {
    config
        .modules
        .get(&module_instance_id)
        .map(|module_config| module_config.kind.to_string())
        .unwrap_or_else(|| "not-in-config".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};

    use super::ModuleRegistry;
    use crate::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};

    struct NoopModule(&'static str);

    #[async_trait::async_trait]
    impl ObserverModule for NoopModule {
        fn kind(&self) -> ModuleKind {
            ModuleKind::from_static_str(self.0)
        }

        fn decoder(&self) -> Decoder {
            Decoder::builder().build()
        }

        fn version(&self) -> u32 {
            1
        }

        fn migrations(&self) -> &'static [Migration] {
            &[]
        }

        async fn process_input(
            &self,
            _ctx: &mut ProcessCtx<'_>,
            _input: &DynInput,
            _meta: &ItemMeta,
        ) -> anyhow::Result<ProcessedItem> {
            Ok(ProcessedItem::default())
        }

        async fn process_output(
            &self,
            _ctx: &mut ProcessCtx<'_>,
            _output: &DynOutput,
            _meta: &ItemMeta,
        ) -> anyhow::Result<ProcessedItem> {
            Ok(ProcessedItem::default())
        }

        async fn process_ci(
            &self,
            _ctx: &mut ProcessCtx<'_>,
            _ci: &DynModuleConsensusItem,
            _meta: &CiMeta,
        ) -> anyhow::Result<Option<serde_json::Value>> {
            Ok(None)
        }
    }

    #[test]
    fn registry_lookup_works() {
        let registry = ModuleRegistry::new(vec![
            Arc::new(NoopModule("mint")),
            Arc::new(NoopModule("wallet")),
        ]);
        assert!(registry.get(&ModuleKind::from_static_str("mint")).is_some());
        assert!(registry.get(&ModuleKind::from_static_str("ln")).is_none());
        assert_eq!(registry.iter().count(), 2);
    }

    #[test]
    #[should_panic(expected = "Duplicate observer module")]
    fn registry_panics_on_duplicate_kind() {
        ModuleRegistry::new(vec![
            Arc::new(NoopModule("mint")),
            Arc::new(NoopModule("mint")),
        ]);
    }
}
