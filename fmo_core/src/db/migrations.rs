use deadpool_postgres::Pool;

/// A single SQL migration. Migrations are identified by their position in the
/// migration list; append-only.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub sql: &'static str,
}

/// Postgres schema name owned by an observer module of the given kind.
pub fn schema_name(kind: &str) -> String {
    format!(
        "fmo_{}",
        kind.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
    )
}

const CORE_MIGRATIONS: &[Migration] = &[Migration {
    sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/core/v0.sql")),
}];

/// Applies pending core schema migrations, tracked in `core_schema_version`.
pub async fn setup_core_schema(pool: &Pool) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;

    let current: i32 = match conn
        .query_one("SELECT to_regclass('public.core_schema_version')::text", &[])
        .await?
        .get::<_, Option<String>>(0)
    {
        Some(_) => {
            conn.query_one("SELECT COALESCE(MAX(version), -1) FROM core_schema_version", &[])
                .await?
                .get(0)
        }
        None => -1,
    };

    for (idx, migration) in CORE_MIGRATIONS.iter().enumerate() {
        if (idx as i32) > current {
            let tx = conn.transaction().await?;
            tx.batch_execute(migration.sql).await?;
            tx.execute(
                "INSERT INTO core_schema_version VALUES ($1) ON CONFLICT DO NOTHING",
                &[&(idx as i32)],
            )
            .await?;
            tx.commit().await?;
        }
    }

    Ok(())
}

/// Sets up the Postgres schema owned by the module of the given kind and runs
/// its pending migrations, tracked in `fmo_<kind>.schema_version`.
///
/// If the stored module version differs from `version` the whole module schema
/// is dropped, its processing cursors are reset and all migrations are re-run;
/// the dispatch engine then replays the module from raw session data.
pub async fn setup_module_schema(
    pool: &Pool,
    kind: &str,
    version: u32,
    migrations: &[Migration],
) -> anyhow::Result<()> {
    let schema = schema_name(kind);
    let mut conn = pool.get().await?;

    let stored: Option<i32> = conn
        .query_opt(
            "SELECT module_version FROM module_versions WHERE module_kind = $1",
            &[&kind],
        )
        .await?
        .map(|row| row.get(0));

    if stored.is_some_and(|stored_version| stored_version != version as i32) {
        tracing::info!(
            "Module {kind} version changed ({stored:?} -> {version}), \
             dropping schema {schema} for replay"
        );
        let tx = conn.transaction().await?;
        tx.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await?;
        tx.execute(
            "DELETE FROM module_progress WHERE module_kind = $1",
            &[&kind],
        )
        .await?;
        tx.execute(
            "DELETE FROM module_versions WHERE module_kind = $1",
            &[&kind],
        )
        .await?;
        tx.commit().await?;
    }

    let tx = conn.transaction().await?;
    tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await?;
    tx.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {schema}.schema_version (version INTEGER PRIMARY KEY)"
    ))
    .await?;
    let current: i32 = tx
        .query_one(
            &format!("SELECT COALESCE(MAX(version), -1) FROM {schema}.schema_version"),
            &[],
        )
        .await?
        .get(0);
    tx.batch_execute(&format!("SET LOCAL search_path TO {schema}, public"))
        .await?;
    for (idx, migration) in migrations.iter().enumerate() {
        if (idx as i32) > current {
            tx.batch_execute(migration.sql).await?;
            tx.execute(
                &format!("INSERT INTO {schema}.schema_version VALUES ($1) ON CONFLICT DO NOTHING"),
                &[&(idx as i32)],
            )
            .await?;
        }
    }
    tx.execute(
        "INSERT INTO public.module_versions VALUES ($1, $2)
         ON CONFLICT (module_kind) DO UPDATE SET module_version = EXCLUDED.module_version",
        &[&kind, &(version as i32)],
    )
    .await?;
    tx.commit().await?;

    Ok(())
}
