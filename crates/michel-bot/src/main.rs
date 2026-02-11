use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::routing::post;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::ruma::OwnedUserId;
use sqlx::PgPool;
use tracing::info;

use michel_api::{AppState, CommandContext, Config, handle_seerr_webhook, on_room_message};
use michel_seerr::SeerrClient;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_env()?;

    let pool = PgPool::connect(&config.database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;
    michel_db::run_migrations(&pool).await?;
    info!("Database connected and migrations applied");

    let client = michel_matrix::create_and_login(
        &config.matrix_homeserver_url,
        &config.matrix_user_id,
        &config.matrix_password,
        &pool,
    )
    .await?;

    let (room, _room_id) = michel_matrix::join_room(&client, &config.matrix_room_alias).await?;

    let seerr_client = SeerrClient::new(&config.seerr_api_url, &config.seerr_api_key);

    let admin_users: Vec<OwnedUserId> = config
        .matrix_admin_users
        .iter()
        .filter_map(|u| OwnedUserId::try_from(u.as_str()).ok())
        .collect();

    let cmd_ctx = Arc::new(CommandContext {
        db: pool.clone(),
        seerr_client,
        admin_users,
    });

    client.add_event_handler_context(cmd_ctx);
    client.add_event_handler(on_room_message);

    let state = Arc::new(AppState { room, db: pool });

    let app = Router::new()
        .route("/webhook/seerr", post(handle_seerr_webhook))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.webhook_listen_addr)
        .await
        .context("Failed to bind listener")?;
    info!("Webhook server listening on {}", config.webhook_listen_addr);

    let sync_client = client.clone();
    tokio::select! {
        result = axum::serve(listener, app) => {
            result.context("Server error")?;
        }
        _ = sync_client.sync(SyncSettings::default()) => {
            info!("Matrix sync ended");
        }
    }

    Ok(())
}
