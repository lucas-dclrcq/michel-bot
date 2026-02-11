use anyhow::{Context, Result};
use matrix_sdk::config::StoreConfig;
use matrix_sdk::ruma::events::reaction::ReactionEventContent;
use matrix_sdk::ruma::events::relation::Annotation;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::{OwnedEventId, OwnedRoomId, OwnedRoomOrAliasId};
use matrix_sdk::{Client, Room};
use michel_crypto_store::PgCryptoStore;
use sqlx::PgPool;
use tracing::info;

pub async fn create_and_login(
    homeserver_url: &str,
    user_id: &str,
    password: &str,
    pool: &PgPool,
) -> Result<Client> {
    let pg_store = PgCryptoStore::new(pool.clone());
    pg_store
        .run_migrations()
        .await
        .context("Failed to run crypto store migrations")?;

    let store_config = StoreConfig::new("michel-bot".to_owned()).crypto_store(pg_store);

    let client = Client::builder()
        .homeserver_url(homeserver_url)
        .store_config(store_config)
        .build()
        .await
        .context("Failed to create Matrix client")?;

    client
        .matrix_auth()
        .login_username(user_id, password)
        .initial_device_display_name("michel-bot")
        .send()
        .await
        .context("Failed to login to Matrix")?;

    info!("Logged in to Matrix as {user_id}");
    Ok(client)
}

pub async fn join_room(client: &Client, room_alias: &str) -> Result<(Room, OwnedRoomId)> {
    let alias: OwnedRoomOrAliasId = room_alias.try_into().context("Invalid room alias")?;
    let room = client
        .join_room_by_id_or_alias(&alias, &[])
        .await
        .context("Failed to join room")?;
    let room_id = room.room_id().to_owned();
    info!("Joined room {room_alias} ({room_id})");
    Ok((room, room_id))
}

pub async fn send_html_message(
    room: &Room,
    plain_body: &str,
    html_body: &str,
) -> Result<OwnedEventId> {
    let content = RoomMessageEventContent::text_html(plain_body, html_body);
    let response = room.send(content).await.context("Failed to send message")?;
    Ok(response.event_id)
}

pub async fn send_thread_reply(
    room: &Room,
    thread_root_event_id: &OwnedEventId,
    plain_body: &str,
    html_body: &str,
) -> Result<OwnedEventId> {
    let mut content = RoomMessageEventContent::text_html(plain_body, html_body);
    content.relates_to = Some(matrix_sdk::ruma::events::room::message::Relation::Thread(
        matrix_sdk::ruma::events::relation::Thread::plain(
            thread_root_event_id.clone(),
            thread_root_event_id.clone(),
        ),
    ));
    let response = room
        .send(content)
        .await
        .context("Failed to send thread reply")?;
    Ok(response.event_id)
}

pub async fn send_reaction(
    room: &Room,
    event_id: &OwnedEventId,
    emoji: &str,
) -> Result<OwnedEventId> {
    let annotation = Annotation::new(event_id.clone(), emoji.to_string());
    let content = ReactionEventContent::new(annotation);
    let response = room
        .send(content)
        .await
        .context("Failed to send reaction")?;
    Ok(response.event_id)
}

pub async fn redact_event(
    room: &Room,
    event_id: &OwnedEventId,
    reason: Option<&str>,
) -> Result<()> {
    room.redact(event_id, reason, None)
        .await
        .context("Failed to redact event")?;
    Ok(())
}
