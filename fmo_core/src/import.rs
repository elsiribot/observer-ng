use crate::registry::ModuleRegistry;

/// Imports federations, raw sessions and block times from a pre-v0.2
/// (schema v8) database. Implemented in the next commit.
pub async fn import(
    _old_db: &str,
    _new_db: &str,
    _registry: &ModuleRegistry,
) -> anyhow::Result<()> {
    anyhow::bail!("import is not implemented yet")
}
