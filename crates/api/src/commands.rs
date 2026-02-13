use std::sync::Arc;

use matrix_sdk::Room;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::OwnedUserId;
use matrix_sdk::ruma::events::room::message::{OriginalSyncRoomMessageEvent, Relation};
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::commands::parser::parse_command;
use michel_seerr::SeerrClient;

mod parser;

pub struct CommandContext {
    pub db: PgPool,
    pub seerr_client: SeerrClient,
    pub admin_users: Vec<OwnedUserId>,
}

#[derive(Debug, PartialEq)]
enum Command {
    Resolve { comment: Option<String> },
}

pub async fn on_room_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    ctx: Ctx<Arc<CommandContext>>,
) {
    if let Err(e) = handle_message(event, &room, &ctx).await {
        error!("Error handling command: {e:#}");
    }
}

async fn handle_message(
    event: OriginalSyncRoomMessageEvent,
    room: &Room,
    ctx: &CommandContext,
) -> anyhow::Result<()> {
    if !ctx.admin_users.iter().any(|u| u == &event.sender) {
        return Ok(());
    }

    let body = event.content.body();
    let command = match parse_command(body) {
        Some(cmd) => cmd,
        None => return Ok(()),
    };

    match command {
        Command::Resolve { comment } => {
            let thread_root_event_id = match &event.content.relates_to {
                Some(Relation::Thread(thread)) => &thread.event_id,
                _ => {
                    warn!("!issues resolve must be sent as a thread reply");
                    return Ok(());
                }
            };

            let issue_event = michel_db::get_issue_event_by_matrix_event_id(
                &ctx.db,
                thread_root_event_id.as_str(),
            )
            .await?;

            let issue_event = match issue_event {
                Some(ev) => ev,
                None => {
                    warn!(
                        event_id = %thread_root_event_id,
                        "No issue found for thread root event"
                    );
                    return Ok(());
                }
            };

            let issue_id = issue_event.issue_id;

            if let Some(ref comment_text) = comment {
                ctx.seerr_client.add_comment(issue_id, comment_text).await?;
                info!(issue_id, comment = %comment_text, "Added comment to issue");
            }

            ctx.seerr_client.resolve_issue(issue_id).await?;
            info!(issue_id, "Resolved issue via command");

            let plain = format!("Issue {issue_id} resolved");
            let html = format!("<b>Issue {issue_id} resolved</b>");
            michel_matrix::send_thread_reply(room, thread_root_event_id, &plain, &html).await?;
        }
    }

    Ok(())
}
