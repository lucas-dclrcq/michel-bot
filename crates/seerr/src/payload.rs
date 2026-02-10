use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SeerrWebhookPayload {
    pub notification_type: String,
    pub subject: String,
    pub message: Option<String>,
    pub image: Option<String>,
    pub issue_id: Option<String>,
    pub reported_by: Option<String>,
    pub comment: Option<String>,
    pub commented_by: Option<String>,
}
