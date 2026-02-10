use anyhow::Result;
use sqlx::PgPool;

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(include_str!("../migrations/001_create_issue_events.sql"))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_issue_event(
    pool: &PgPool,
    issue_id: i64,
    matrix_event_id: &str,
    matrix_room_id: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO issue_events (issue_id, matrix_event_id, matrix_room_id) VALUES ($1, $2, $3)",
    )
    .bind(issue_id)
    .bind(matrix_event_id)
    .bind(matrix_room_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub struct IssueEvent {
    pub issue_id: i64,
    pub matrix_event_id: String,
    pub matrix_room_id: String,
    pub reaction_event_id: Option<String>,
}

pub async fn get_issue_event(pool: &PgPool, issue_id: i64) -> Result<Option<IssueEvent>> {
    let row = sqlx::query_as::<_, (i64, String, String, Option<String>)>(
        "SELECT issue_id, matrix_event_id, matrix_room_id, reaction_event_id FROM issue_events WHERE issue_id = $1",
    )
    .bind(issue_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(issue_id, matrix_event_id, matrix_room_id, reaction_event_id)| IssueEvent {
            issue_id,
            matrix_event_id,
            matrix_room_id,
            reaction_event_id,
        },
    ))
}

pub async fn set_reaction_event_id(
    pool: &PgPool,
    issue_id: i64,
    reaction_event_id: &str,
) -> Result<()> {
    sqlx::query("UPDATE issue_events SET reaction_event_id = $1 WHERE issue_id = $2")
        .bind(reaction_event_id)
        .bind(issue_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn clear_reaction_event_id(pool: &PgPool, issue_id: i64) -> Result<()> {
    sqlx::query("UPDATE issue_events SET reaction_event_id = NULL WHERE issue_id = $1")
        .bind(issue_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_issue_event_by_matrix_event_id(
    pool: &PgPool,
    matrix_event_id: &str,
) -> Result<Option<IssueEvent>> {
    let row = sqlx::query_as::<_, (i64, String, String, Option<String>)>(
        "SELECT issue_id, matrix_event_id, matrix_room_id, reaction_event_id FROM issue_events WHERE matrix_event_id = $1",
    )
    .bind(matrix_event_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(issue_id, matrix_event_id, matrix_room_id, reaction_event_id)| IssueEvent {
            issue_id,
            matrix_event_id,
            matrix_room_id,
            reaction_event_id,
        },
    ))
}
