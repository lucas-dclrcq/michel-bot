use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use tracing::{error, info, warn};

use crate::AppState;
use crate::db;
use crate::matrix;
use crate::seerr::SeerrWebhookPayload;

pub async fn handle_seerr_webhook(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SeerrWebhookPayload>,
) -> StatusCode {
    info!(
        notification_type = %payload.notification_type,
        subject = %payload.subject,
        "Received Seerr webhook"
    );

    let result = match payload.notification_type.as_str() {
        "ISSUE_CREATED" => handle_issue_created(&state, &payload).await,
        "ISSUE_RESOLVED" => handle_issue_resolved(&state, &payload).await,
        "ISSUE_COMMENT" => handle_issue_comment(&state, &payload).await,
        "ISSUE_REOPENED" => handle_issue_reopened(&state, &payload).await,
        other => {
            warn!("Unknown notification type: {other}");
            return StatusCode::OK;
        }
    };

    match result {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            error!("Error handling webhook: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn handle_issue_created(
    state: &AppState,
    payload: &SeerrWebhookPayload,
) -> anyhow::Result<()> {
    let issue_id: i64 = payload
        .issue_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Missing issue_id"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid issue_id"))?;

    let reported_by = payload.reported_by.as_deref().unwrap_or("unknown");
    let message = payload.message.as_deref().unwrap_or("");

    let plain_body = format!(
        "🔴 New Seerr issue\nSubject: {}\nDescription: {}\nReported by: {}",
        payload.subject, message, reported_by
    );
    let html_body = format!(
        "<h4>🔴 New Seerr issue</h4>\
         <b>Subject:</b> {}<br/>\
         <b>Description:</b> {}<br/>\
         <b>Reported by:</b> {}",
        payload.subject, message, reported_by
    );

    let event_id = matrix::send_html_message(&state.room, &plain_body, &html_body).await?;
    let room_id = state.room.room_id().to_string();

    db::insert_issue_event(&state.db, issue_id, event_id.as_str(), &room_id).await?;
    info!(issue_id, %event_id, "Issue created message sent");

    Ok(())
}

async fn handle_issue_resolved(
    state: &AppState,
    payload: &SeerrWebhookPayload,
) -> anyhow::Result<()> {
    let issue_id: i64 = payload
        .issue_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Missing issue_id"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid issue_id"))?;

    let issue_event = db::get_issue_event(&state.db, issue_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event found for issue {issue_id}"))?;

    let root_event_id = issue_event.matrix_event_id.as_str().try_into()?;

    let comment = payload.comment.as_deref().unwrap_or("");
    let commented_by = payload.commented_by.as_deref().unwrap_or("unknown");

    let plain_body = format!("✅ Issue resolved\nComment: {comment}\nBy: {commented_by}");
    let html_body = format!(
        "<b>✅ Issue resolved</b><br/>\
         <b>Comment:</b> {comment}<br/>\
         <b>By:</b> {commented_by}"
    );

    matrix::send_thread_reply(&state.room, &root_event_id, &plain_body, &html_body).await?;

    let reaction_event_id = matrix::send_reaction(&state.room, &root_event_id, "✅").await?;
    db::set_reaction_event_id(&state.db, issue_id, reaction_event_id.as_str()).await?;

    info!(issue_id, "Issue resolved message sent");
    Ok(())
}

async fn handle_issue_comment(
    state: &AppState,
    payload: &SeerrWebhookPayload,
) -> anyhow::Result<()> {
    let issue_id: i64 = payload
        .issue_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Missing issue_id"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid issue_id"))?;

    let issue_event = db::get_issue_event(&state.db, issue_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event found for issue {issue_id}"))?;

    let root_event_id = issue_event.matrix_event_id.as_str().try_into()?;

    let comment = payload.comment.as_deref().unwrap_or("");
    let commented_by = payload.commented_by.as_deref().unwrap_or("unknown");

    let plain_body = format!("💬 {commented_by} : {comment}");
    let html_body = format!("<b>💬 {commented_by} :</b> {comment}");

    matrix::send_thread_reply(&state.room, &root_event_id, &plain_body, &html_body).await?;

    info!(issue_id, "Issue comment sent");
    Ok(())
}

async fn handle_issue_reopened(
    state: &AppState,
    payload: &SeerrWebhookPayload,
) -> anyhow::Result<()> {
    let issue_id: i64 = payload
        .issue_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Missing issue_id"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid issue_id"))?;

    let issue_event = db::get_issue_event(&state.db, issue_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No event found for issue {issue_id}"))?;

    let root_event_id = issue_event.matrix_event_id.as_str().try_into()?;

    let reported_by = payload.reported_by.as_deref().unwrap_or("unknown");

    let plain_body = format!("🔄 Issue reopened\nBy: {reported_by}");
    let html_body = format!(
        "<b>🔄 Issue reopened</b><br/>\
         <b>By:</b> {reported_by}"
    );

    matrix::send_thread_reply(&state.room, &root_event_id, &plain_body, &html_body).await?;

    if let Some(reaction_event_id_str) = &issue_event.reaction_event_id {
        let reaction_event_id = reaction_event_id_str.as_str().try_into()?;
        matrix::redact_event(&state.room, &reaction_event_id, Some("Issue reopened")).await?;
        db::clear_reaction_event_id(&state.db, issue_id).await?;
    }

    info!(issue_id, "Issue reopened message sent");
    Ok(())
}
