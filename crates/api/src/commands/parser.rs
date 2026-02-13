use crate::commands::Command;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "commands/commands.pest"]
struct CommandParser;

/// Parses a chat message body into a [`Command`] using the
/// [Pest grammar](https://docs.rs/pest/latest/pest/) defined in `commands.pest`.
///
/// To add a new command (e.g. `!issues status <id>`):
///
/// 1. **Grammar** (`commands.pest`):
///    - Add a new rule: `status = { "status" ~ id }` with any sub-rules it needs.
///    - Add it to the `subcommand` choice: `subcommand = { resolve | status }`.
///
/// 2. **Enum**: Add a variant to [`Command`]: `Status { id: i64 }`.
///
/// 3. **Parser** (this function): Add a `Rule::status` arm to the match below,
///    extract the inner pairs, and return the new variant.
///
/// 4. **Handler** (`command::handle_message`): Add a match arm for the new variant.
///
/// 5. **Tests**: Add tests covering the new command's happy path and edge cases.
pub fn parse_command(body: &str) -> Option<Command> {
    let body = body.trim();
    let pairs = CommandParser::parse(Rule::command, body).ok()?;

    let command_pair = pairs.into_iter().next()?;
    let subcommand_pair = command_pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::subcommand)?;

    let inner = subcommand_pair.into_inner().next()?;
    match inner.as_rule() {
        Rule::resolve => {
            let comment = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::comment)
                .and_then(|c| {
                    let inner = c.into_inner().next()?;
                    match inner.as_rule() {
                        Rule::quoted_comment => {
                            let text = inner
                                .into_inner()
                                .find(|p| p.as_rule() == Rule::quoted_inner)?
                                .as_str();
                            if text.is_empty() {
                                None
                            } else {
                                Some(text.to_string())
                            }
                        }
                        Rule::unquoted_comment => {
                            let text = inner.as_str().trim();
                            if text.is_empty() {
                                None
                            } else {
                                Some(text.to_string())
                            }
                        }
                        _ => None,
                    }
                });
            Some(Command::Resolve { comment })
        }
        _ => None,
    }
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

    #[test]
    fn parse_empty_input() {
        assert_eq!(parse_command(""), None);
    }

    #[test]
    fn parse_only_prefix() {
        assert_eq!(parse_command("!issues"), None);
    }

    #[test]
    fn parse_resolve_with_extra_spaces_before_comment() {
        assert_eq!(
            parse_command("!issues   resolve   hello"),
            Some(Command::Resolve {
                comment: Some("hello".to_string()),
            })
        );
    }

    #[test]
    fn parse_resolve_quoted_comment_with_spaces() {
        assert_eq!(
            parse_command(r#"!issues resolve "  spaced  ""#),
            Some(Command::Resolve {
                comment: Some("  spaced  ".to_string()),
            })
        );
    }

    #[test]
    fn parse_resolve_multiword_unquoted_comment() {
        assert_eq!(
            parse_command("!issues resolve this is a long comment"),
            Some(Command::Resolve {
                comment: Some("this is a long comment".to_string()),
            })
        );
    }

    #[test]
    fn parse_wrong_prefix() {
        assert_eq!(parse_command("!other resolve"), None);
    }

    #[test]
    fn parse_resolve_with_tab_whitespace() {
        assert_eq!(
            parse_command("!issues\tresolve"),
            Some(Command::Resolve { comment: None })
        );
    }
}
