mod commands;
mod config;
mod webhook;

pub use commands::{CommandContext, on_room_message};
pub use config::Config;
pub use webhook::handle_seerr_webhook;

use matrix_sdk::Room;
use sqlx::PgPool;

pub struct AppState {
    pub room: Room,
    pub db: PgPool,
}
