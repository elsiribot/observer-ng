use deadpool_postgres::GenericClient;
use postgres_from_row::FromRow;

pub async fn execute(
    conn: &impl GenericClient,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> anyhow::Result<u64> {
    let num_rows = conn.execute(sql, params).await?;
    Ok(num_rows)
}

pub async fn query_one<T>(
    conn: &impl GenericClient,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> anyhow::Result<T>
where
    T: FromRow,
{
    let result = conn.query_one(sql, params).await?;
    Ok(T::try_from_row(&result)?)
}

pub async fn query_value<T>(
    conn: &impl GenericClient,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> anyhow::Result<T>
where
    for<'a> T: tokio_postgres::types::FromSql<'a>,
{
    let result = conn.query_one(sql, params).await?;
    Ok(result.try_get(0)?)
}

pub async fn query_opt<T>(
    conn: &impl GenericClient,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> anyhow::Result<Option<T>>
where
    T: FromRow,
{
    let result = conn.query_opt(sql, params).await?;
    Ok(result.map(|row| T::try_from_row(&row)).transpose()?)
}

pub async fn query<T>(
    conn: &impl GenericClient,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> anyhow::Result<Vec<T>>
where
    T: FromRow,
{
    let result = conn.query(sql, params).await?;
    Ok(result
        .iter()
        .map(T::try_from_row)
        .collect::<Result<_, _>>()?)
}
