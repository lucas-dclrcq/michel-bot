use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;

pub struct SeerrClient {
    base_url: String,
    api_key: String,
    client: Client,
}

impl SeerrClient {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            client: Client::new(),
        }
    }

    pub async fn add_comment(&self, issue_id: i64, message: &str) -> Result<()> {
        self.client
            .post(format!(
                "{}/api/v1/issue/{}/comment",
                self.base_url, issue_id
            ))
            .header("X-Api-Key", &self.api_key)
            .json(&json!({ "message": message }))
            .send()
            .await
            .context("Failed to send comment to Seerr")?
            .error_for_status()
            .context("Seerr returned error for comment")?;
        Ok(())
    }

    pub async fn resolve_issue(&self, issue_id: i64) -> Result<()> {
        self.client
            .post(format!(
                "{}/api/v1/issue/{}/resolved",
                self.base_url, issue_id
            ))
            .header("X-Api-Key", &self.api_key)
            .send()
            .await
            .context("Failed to resolve issue in Seerr")?
            .error_for_status()
            .context("Seerr returned error for resolve")?;
        Ok(())
    }
}
