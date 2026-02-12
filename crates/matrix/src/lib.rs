use anyhow::{Context, Result};
use matrix_sdk::authentication::matrix::MatrixSession;
use matrix_sdk::config::StoreConfig;
use matrix_sdk::ruma::events::reaction::ReactionEventContent;
use matrix_sdk::ruma::events::relation::Annotation;
use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
use matrix_sdk::ruma::{OwnedEventId, OwnedRoomId, OwnedRoomOrAliasId};
use matrix_sdk::store::RoomLoadSettings;
use matrix_sdk::{Client, Room, SessionMeta, SessionTokens};
use michel_crypto_store::{
    PgCryptoStore, clear_crypto_store, load_matrix_session, save_matrix_session,
};
use sqlx::PgPool;
use tracing::info;

pub enum MatrixCredentials<'a> {
    Password {
        user_id: &'a str,
        password: &'a str,
    },
    AccessToken {
        user_id: &'a str,
        access_token: &'a str,
        device_id: &'a str,
    },
}

pub async fn create_and_login(
    homeserver_url: &str,
    credentials: MatrixCredentials<'_>,
    pool: &PgPool,
) -> Result<Client> {
    let pg_store = PgCryptoStore::new(pool.clone());
    pg_store
        .run_migrations()
        .await
        .context("Failed to run crypto store migrations")?;

    let saved_session = load_matrix_session(pool)
        .await
        .context("Failed to load saved matrix session")?;

    // If there is no saved session, any existing crypto state is stale (e.g.
    // leftover from a previous access-token auth for a different device).
    // Wipe it so the SDK starts with a clean account and doesn't try to
    // re-upload one-time keys the server already has.
    if saved_session.is_none() {
        clear_crypto_store(pool)
            .await
            .context("Failed to clear stale crypto store")?;
    }

    let store_config = StoreConfig::new("michel-bot".to_owned()).crypto_store(pg_store);

    let client = Client::builder()
        .homeserver_url(homeserver_url)
        .store_config(store_config)
        .build()
        .await
        .context("Failed to create Matrix client")?;

    // If we have a saved session, restore it regardless of what credentials
    // were configured. This keeps the device_id consistent with the crypto
    // store so we never re-upload one-time keys.
    if let Some((saved_user_id, saved_device_id, saved_access_token)) = saved_session {
        let session = MatrixSession {
            meta: SessionMeta {
                user_id: saved_user_id
                    .as_str()
                    .try_into()
                    .context("Invalid saved user ID")?,
                device_id: saved_device_id.as_str().into(),
            },
            tokens: SessionTokens {
                access_token: saved_access_token,
                refresh_token: None,
            },
        };
        client
            .matrix_auth()
            .restore_session(session, RoomLoadSettings::default())
            .await
            .context("Failed to restore saved Matrix session")?;
        info!("Restored saved Matrix session for {saved_user_id}");
    } else {
        match credentials {
            MatrixCredentials::Password { user_id, password } => {
                let response = client
                    .matrix_auth()
                    .login_username(user_id, password)
                    .initial_device_display_name("michel-bot")
                    .send()
                    .await
                    .context("Failed to login to Matrix")?;
                info!("Logged in to Matrix as {user_id}");

                save_matrix_session(
                    pool,
                    response.user_id.as_str(),
                    response.device_id.as_str(),
                    &response.access_token,
                )
                .await
                .context("Failed to save matrix session")?;
            }
            MatrixCredentials::AccessToken {
                user_id,
                access_token,
                device_id,
            } => {
                let session = MatrixSession {
                    meta: SessionMeta {
                        user_id: user_id.try_into().context("Invalid user ID")?,
                        device_id: device_id.into(),
                    },
                    tokens: SessionTokens {
                        access_token: access_token.to_owned(),
                        refresh_token: None,
                    },
                };
                client
                    .matrix_auth()
                    .restore_session(session, RoomLoadSettings::default())
                    .await
                    .context("Failed to restore Matrix session")?;
                info!("Restored Matrix session for {user_id}");

                save_matrix_session(pool, user_id, device_id, access_token)
                    .await
                    .context("Failed to save matrix session")?;
            }
        }
    }

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
