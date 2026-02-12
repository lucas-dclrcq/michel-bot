use anyhow::{Context, Result, bail};

pub struct Config {
    pub matrix_homeserver_url: String,
    pub matrix_user_id: String,
    pub matrix_password: Option<String>,
    pub matrix_access_token: Option<String>,
    pub matrix_device_id: Option<String>,
    pub matrix_room_alias: String,
    pub database_url: String,
    pub webhook_listen_addr: String,
    pub seerr_api_url: String,
    pub seerr_api_key: String,
    pub matrix_admin_users: Vec<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let matrix_password = std::env::var("MATRIX_PASSWORD").ok();
        let matrix_access_token = std::env::var("MATRIX_ACCESS_TOKEN").ok();
        let matrix_device_id = std::env::var("MATRIX_DEVICE_ID").ok();

        if matrix_password.is_none()
            && (matrix_access_token.is_none() || matrix_device_id.is_none())
        {
            bail!(
                "Either MATRIX_PASSWORD or both MATRIX_ACCESS_TOKEN and MATRIX_DEVICE_ID must be set"
            );
        }

        Ok(Self {
            matrix_homeserver_url: std::env::var("MATRIX_HOMESERVER_URL")
                .context("MATRIX_HOMESERVER_URL must be set")?,
            matrix_user_id: std::env::var("MATRIX_USER_ID")
                .context("MATRIX_USER_ID must be set")?,
            matrix_password,
            matrix_access_token,
            matrix_device_id,
            matrix_room_alias: std::env::var("MATRIX_ROOM_ALIAS")
                .context("MATRIX_ROOM_ALIAS must be set")?,
            database_url: std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?,
            webhook_listen_addr: std::env::var("WEBHOOK_LISTEN_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
            seerr_api_url: std::env::var("SEERR_API_URL").context("SEERR_API_URL must be set")?,
            seerr_api_key: std::env::var("SEERR_API_KEY").context("SEERR_API_KEY must be set")?,
            matrix_admin_users: std::env::var("MATRIX_ADMIN_USERS")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        })
    }
}
