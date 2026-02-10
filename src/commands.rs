use std::sync::Arc;

use matrix_sdk::Room;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::ruma::OwnedUserId;
use matrix_sdk::ruma::events::room::message::{OriginalSyncRoomMessageEvent, Relation};
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::db;
use crate::matrix;
use crate::seerr_client::SeerrClient;

pub struct CommandContext {
    pub db: PgPool,
    pub seerr_client: SeerrClient,
    pub admin_users: Vec<OwnedUserId>,
}

#[derive(Debug, PartialEq)]
enum Command {
    Resolve { comment: Option<String> },
}

fn parse_command(body: &str) -> Option<Command> {
    let body = body.trim();
    let rest = body.strip_prefix("!issues")?;
    let rest = rest.trim_start();

    if let Some(rest) = rest.strip_prefix("resolve") {
        let rest = rest.trim();
        if rest.is_empty() {
            return Some(Command::Resolve { comment: None });
        }

        if let Some(inner) = rest.strip_prefix('"') {
            let comment = inner.strip_suffix('"').unwrap_or(inner);
            if comment.is_empty() {
                return Some(Command::Resolve { comment: None });
            }
            return Some(Command::Resolve {
                comment: Some(comment.to_string()),
            });
        }

        return Some(Command::Resolve {
            comment: Some(rest.to_string()),
        });
    }

    None
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

            let issue_event =
                db::get_issue_event_by_matrix_event_id(&ctx.db, thread_root_event_id.as_str())
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
            matrix::send_thread_reply(room, thread_root_event_id, &plain, &html).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resolve_with_quoted_comment() {
        assert_eq!(
            parse_command(r#"!issues resolve "Subtitles fixed""#),
            Some(Command::Resolve {
                comment: Some("Subtitles fixed".to_string()),
            })
        );
    }

    #[test]
    fn parse_resolve_with_unquoted_comment() {
        assert_eq!(
            parse_command("!issues resolve fixed it"),
            Some(Command::Resolve {
                comment: Some("fixed it".to_string()),
            })
        );
    }

    #[test]
    fn parse_resolve_no_comment() {
        assert_eq!(
            parse_command("!issues resolve"),
            Some(Command::Resolve { comment: None })
        );
    }

    #[test]
    fn parse_resolve_empty_quoted_comment() {
        assert_eq!(
            parse_command(r#"!issues resolve """#),
            Some(Command::Resolve { comment: None })
        );
    }

    #[test]
    fn parse_unrelated_message() {
        assert_eq!(parse_command("hello world"), None);
    }

    #[test]
    fn parse_unknown_subcommand() {
        assert_eq!(parse_command("!issues unknown"), None);
    }

    #[test]
    fn parse_with_leading_whitespace() {
        assert_eq!(
            parse_command("  !issues resolve  "),
            Some(Command::Resolve { comment: None })
        );
    }
}
