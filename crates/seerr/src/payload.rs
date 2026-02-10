use serde::Deserialize;
use std::fmt;

#[derive(Debug, Deserialize)]
pub struct SeerrWebhookPayload {
    pub notification_type: SeerrNotificationType,
    pub subject: String,
    pub message: Option<String>,
    pub image: Option<String>,
    pub issue_id: Option<String>,
    pub reported_by: Option<String>,
    pub comment: Option<String>,
    pub commented_by: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SeerrNotificationType {
    IssueCreated,
    IssueResolved,
    IssueComment,
    IssueReopened,
    #[serde(other)]
    Unknown,
}

impl fmt::Display for SeerrNotificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
