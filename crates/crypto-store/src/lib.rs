use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use matrix_sdk_common::cross_process_lock::CrossProcessLockGeneration;
use matrix_sdk_common::locks::RwLock as StdRwLock;
use matrix_sdk_crypto::{
    Account, DeviceData, GossipRequest, GossippedSecret, SecretInfo, Session, UserIdentityData,
    olm::{
        Curve25519PublicKey, InboundGroupSession, OlmMessageHash, OutboundGroupSession,
        PickledAccount, PickledCrossSigningIdentity, PickledInboundGroupSession, PickledSession,
        PrivateCrossSigningIdentity, SenderDataType, StaticAccountData,
    },
    store::{
        CryptoStore, CryptoStoreError,
        types::{
            BackupKeys, Changes, DehydratedDeviceKey, PendingChanges, RoomKeyCounts,
            RoomKeyWithheldEntry, RoomSettings, StoredRoomKeyBundleData, TrackedUser,
        },
    },
};
use ruma::{
    DeviceId, OwnedDeviceId, RoomId, TransactionId, UserId, events::secret::request::SecretName,
};
use sqlx::PgPool;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum PgCryptoStoreError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl From<PgCryptoStoreError> for CryptoStoreError {
    fn from(e: PgCryptoStoreError) -> Self {
        CryptoStoreError::backend(e)
    }
}

fn encode_key_info(info: &SecretInfo) -> String {
    match info {
        SecretInfo::KeyRequest(info) => {
            format!(
                "{}{}{}",
                info.room_id(),
                info.algorithm(),
                info.session_id()
            )
        }
        SecretInfo::SecretRequest(i) => i.as_ref().to_owned(),
    }
}

const MIGRATIONS: &str = r#"
CREATE TABLE IF NOT EXISTS crypto_account (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_identity (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_sessions (
    session_id TEXT PRIMARY KEY,
    sender_key TEXT NOT NULL,
    data BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_sessions_sender_key ON crypto_sessions(sender_key);

CREATE TABLE IF NOT EXISTS crypto_inbound_group_sessions (
    room_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    backed_up_to TEXT,
    sender_key TEXT,
    sender_data_type INT,
    PRIMARY KEY (room_id, session_id)
);

CREATE TABLE IF NOT EXISTS crypto_outbound_group_sessions (
    room_id TEXT PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_devices (
    user_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (user_id, device_id)
);

CREATE TABLE IF NOT EXISTS crypto_user_identities (
    user_id TEXT PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_tracked_users (
    user_id TEXT PRIMARY KEY,
    dirty BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_olm_hashes (
    sender_key TEXT NOT NULL,
    hash TEXT NOT NULL,
    PRIMARY KEY (sender_key, hash)
);

CREATE TABLE IF NOT EXISTS crypto_gossip_requests (
    request_id TEXT PRIMARY KEY,
    info_key TEXT NOT NULL,
    data BYTEA NOT NULL,
    sent_out BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_gossip_requests_info_key ON crypto_gossip_requests(info_key);

CREATE TABLE IF NOT EXISTS crypto_secrets_inbox (
    id BIGSERIAL PRIMARY KEY,
    secret_name TEXT NOT NULL,
    data BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_secrets_inbox_name ON crypto_secrets_inbox(secret_name);

CREATE TABLE IF NOT EXISTS crypto_withheld_info (
    room_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (room_id, session_id)
);

CREATE TABLE IF NOT EXISTS crypto_room_settings (
    room_id TEXT PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_room_key_bundles (
    room_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS crypto_custom_values (
    key TEXT PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_lease_locks (
    key TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expiration_ts BIGINT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS crypto_backup_keys (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    backup_version TEXT,
    backup_decryption_key BYTEA
);

CREATE TABLE IF NOT EXISTS crypto_next_batch_token (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    token TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_dehydrated_device_key (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    data BYTEA NOT NULL
);
"#;

#[derive(Debug)]
pub struct PgCryptoStore {
    pool: PgPool,
    static_account: Arc<StdRwLock<Option<StaticAccountData>>>,
}

impl PgCryptoStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            static_account: Arc::new(StdRwLock::new(None)),
        }
    }

    pub async fn run_migrations(&self) -> std::result::Result<(), sqlx::Error> {
        sqlx::raw_sql(MIGRATIONS).execute(&self.pool).await?;
        Ok(())
    }

    fn get_static_account(&self) -> Option<StaticAccountData> {
        self.static_account.read().clone()
    }
}

type Result<T> = std::result::Result<T, PgCryptoStoreError>;

#[async_trait]
impl CryptoStore for PgCryptoStore {
    type Error = PgCryptoStoreError;

    async fn load_account(&self) -> Result<Option<Account>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_account WHERE id = 1")
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => {
                let pickle: PickledAccount = serde_json::from_slice(&data)?;
                let account = Account::from_pickle(pickle).expect("Failed to unpickle account");
                *self.static_account.write() = Some(account.static_data().clone());
                Ok(Some(account))
            }
            None => Ok(None),
        }
    }

    async fn load_identity(&self) -> Result<Option<PrivateCrossSigningIdentity>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_identity WHERE id = 1")
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => {
                let pickle: PickledCrossSigningIdentity = serde_json::from_slice(&data)?;
                Ok(Some(
                    PrivateCrossSigningIdentity::from_pickle(pickle)
                        .expect("Failed to unpickle cross-signing identity"),
                ))
            }
            None => Ok(None),
        }
    }

    async fn next_batch_token(&self) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT token FROM crypto_next_batch_token WHERE id = 1")
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.map(|(t,)| t))
    }

    async fn save_pending_changes(&self, changes: PendingChanges) -> Result<()> {
        if let Some(account) = changes.account {
            *self.static_account.write() = Some(account.static_data().clone());
            let pickle = account.pickle();
            let data = serde_json::to_vec(&pickle)?;
            sqlx::query(
                "INSERT INTO crypto_account (id, data) VALUES (1, $1) \
                 ON CONFLICT (id) DO UPDATE SET data = $1",
            )
            .bind(&data)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn save_changes(&self, changes: Changes) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Sessions
        for session in changes.sessions {
            let session_id = session.session_id().to_owned();
            let pickle = session.pickle().await;
            let sender_key = pickle.sender_key.to_base64();
            let data = serde_json::to_vec(&pickle)?;
            sqlx::query(
                "INSERT INTO crypto_sessions (session_id, sender_key, data) VALUES ($1, $2, $3) \
                 ON CONFLICT (session_id) DO UPDATE SET sender_key = $2, data = $3",
            )
            .bind(&session_id)
            .bind(&sender_key)
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Inbound group sessions
        self.save_inbound_group_sessions_in_tx(&mut tx, changes.inbound_group_sessions, None)
            .await?;

        // Outbound group sessions
        for session in changes.outbound_group_sessions {
            let room_id = session.room_id().to_string();
            let pickle = session.pickle().await;
            let data = serde_json::to_vec(&pickle)?;
            sqlx::query(
                "INSERT INTO crypto_outbound_group_sessions (room_id, data) VALUES ($1, $2) \
                 ON CONFLICT (room_id) DO UPDATE SET data = $2",
            )
            .bind(&room_id)
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Private identity
        if let Some(identity) = changes.private_identity {
            let pickle = identity.pickle().await;
            let data = serde_json::to_vec(&pickle)?;
            sqlx::query(
                "INSERT INTO crypto_identity (id, data) VALUES (1, $1) \
                 ON CONFLICT (id) DO UPDATE SET data = $1",
            )
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Devices: new + changed
        for device in changes
            .devices
            .new
            .into_iter()
            .chain(changes.devices.changed)
        {
            let user_id = device.user_id().to_string();
            let device_id = device.device_id().to_string();
            let data = serde_json::to_vec(&device)?;
            sqlx::query(
                "INSERT INTO crypto_devices (user_id, device_id, data) VALUES ($1, $2, $3) \
                 ON CONFLICT (user_id, device_id) DO UPDATE SET data = $3",
            )
            .bind(&user_id)
            .bind(&device_id)
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Devices: deleted
        for device in changes.devices.deleted {
            sqlx::query("DELETE FROM crypto_devices WHERE user_id = $1 AND device_id = $2")
                .bind(device.user_id().as_str())
                .bind(device.device_id().as_str())
                .execute(&mut *tx)
                .await?;
        }

        // Identities: new + changed
        for identity in changes
            .identities
            .new
            .into_iter()
            .chain(changes.identities.changed)
        {
            let user_id = identity.user_id().to_string();
            let data = serde_json::to_vec(&identity)?;
            sqlx::query(
                "INSERT INTO crypto_user_identities (user_id, data) VALUES ($1, $2) \
                 ON CONFLICT (user_id) DO UPDATE SET data = $2",
            )
            .bind(&user_id)
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Message hashes
        for hash in changes.message_hashes {
            sqlx::query(
                "INSERT INTO crypto_olm_hashes (sender_key, hash) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(&hash.sender_key)
            .bind(&hash.hash)
            .execute(&mut *tx)
            .await?;
        }

        // Key requests
        for key_request in changes.key_requests {
            let request_id = key_request.request_id.to_string();
            let info_key = encode_key_info(&key_request.info);
            let sent_out = key_request.sent_out;
            let data = serde_json::to_vec(&key_request)?;
            sqlx::query(
                "INSERT INTO crypto_gossip_requests (request_id, info_key, data, sent_out) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (request_id) DO UPDATE SET info_key = $2, data = $3, sent_out = $4",
            )
            .bind(&request_id)
            .bind(&info_key)
            .bind(&data)
            .bind(sent_out)
            .execute(&mut *tx)
            .await?;
        }

        // Backup decryption key
        if let Some(key) = changes.backup_decryption_key {
            let key_data = serde_json::to_vec(&key)?;
            sqlx::query(
                "INSERT INTO crypto_backup_keys (id, backup_decryption_key) VALUES (1, $1) \
                 ON CONFLICT (id) DO UPDATE SET backup_decryption_key = $1",
            )
            .bind(&key_data)
            .execute(&mut *tx)
            .await?;
        }

        // Backup version
        if let Some(version) = changes.backup_version {
            sqlx::query(
                "INSERT INTO crypto_backup_keys (id, backup_version) VALUES (1, $1) \
                 ON CONFLICT (id) DO UPDATE SET backup_version = $1",
            )
            .bind(&version)
            .execute(&mut *tx)
            .await?;
        }

        // Dehydrated device pickle key
        if let Some(pickle_key) = changes.dehydrated_device_pickle_key {
            let data = serde_json::to_vec(&pickle_key)?;
            sqlx::query(
                "INSERT INTO crypto_dehydrated_device_key (id, data) VALUES (1, $1) \
                 ON CONFLICT (id) DO UPDATE SET data = $1",
            )
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Secrets inbox
        for secret in changes.secrets {
            let secret_name = secret.secret_name.to_string();
            let data = serde_json::to_vec(&secret)?;
            sqlx::query("INSERT INTO crypto_secrets_inbox (secret_name, data) VALUES ($1, $2)")
                .bind(&secret_name)
                .bind(&data)
                .execute(&mut *tx)
                .await?;
        }

        // Withheld session info
        for (room_id, data) in changes.withheld_session_info {
            for (session_id, entry) in data {
                let entry_data = serde_json::to_vec(&entry)?;
                sqlx::query(
                    "INSERT INTO crypto_withheld_info (room_id, session_id, data) VALUES ($1, $2, $3) \
                     ON CONFLICT (room_id, session_id) DO UPDATE SET data = $3",
                )
                .bind(room_id.as_str())
                .bind(&session_id)
                .bind(&entry_data)
                .execute(&mut *tx)
                .await?;
            }
        }

        // Next batch token
        if let Some(token) = changes.next_batch_token {
            sqlx::query(
                "INSERT INTO crypto_next_batch_token (id, token) VALUES (1, $1) \
                 ON CONFLICT (id) DO UPDATE SET token = $1",
            )
            .bind(&token)
            .execute(&mut *tx)
            .await?;
        }

        // Room settings
        for (room_id, settings) in changes.room_settings {
            let data = serde_json::to_vec(&settings)?;
            sqlx::query(
                "INSERT INTO crypto_room_settings (room_id, data) VALUES ($1, $2) \
                 ON CONFLICT (room_id) DO UPDATE SET data = $2",
            )
            .bind(room_id.as_str())
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        // Room key bundles
        for bundle in changes.received_room_key_bundles {
            let room_id = bundle.bundle_data.room_id.to_string();
            let user_id = bundle.sender_user.to_string();
            let data = serde_json::to_vec(&bundle)?;
            sqlx::query(
                "INSERT INTO crypto_room_key_bundles (room_id, user_id, data) VALUES ($1, $2, $3) \
                 ON CONFLICT (room_id, user_id) DO UPDATE SET data = $3",
            )
            .bind(&room_id)
            .bind(&user_id)
            .bind(&data)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn save_inbound_group_sessions(
        &self,
        sessions: Vec<InboundGroupSession>,
        backed_up_to_version: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.save_inbound_group_sessions_in_tx(&mut tx, sessions, backed_up_to_version)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_sessions(&self, sender_key: &str) -> Result<Option<Vec<Session>>> {
        let device_keys = self.get_own_device().await?.as_device_keys().clone();

        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_sessions WHERE sender_key = $1")
                .bind(sender_key)
                .fetch_all(&self.pool)
                .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut sessions = Vec::new();
        for (data,) in rows {
            let pickle: PickledSession = serde_json::from_slice(&data)?;
            let session = Session::from_pickle(device_keys.clone(), pickle)
                .expect("Failed to unpickle session");
            sessions.push(session);
        }
        Ok(Some(sessions))
    }

    async fn get_inbound_group_session(
        &self,
        room_id: &RoomId,
        session_id: &str,
    ) -> Result<Option<InboundGroupSession>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crypto_inbound_group_sessions WHERE room_id = $1 AND session_id = $2",
        )
        .bind(room_id.as_str())
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((data,)) => {
                let pickle: PickledInboundGroupSession = serde_json::from_slice(&data)?;
                Ok(Some(
                    InboundGroupSession::from_pickle(pickle)
                        .expect("Failed to unpickle inbound group session"),
                ))
            }
            None => Ok(None),
        }
    }

    async fn get_withheld_info(
        &self,
        room_id: &RoomId,
        session_id: &str,
    ) -> Result<Option<RoomKeyWithheldEntry>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crypto_withheld_info WHERE room_id = $1 AND session_id = $2",
        )
        .bind(room_id.as_str())
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn get_withheld_sessions_by_room_id(
        &self,
        room_id: &RoomId,
    ) -> Result<Vec<RoomKeyWithheldEntry>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_withheld_info WHERE room_id = $1")
                .bind(room_id.as_str())
                .fetch_all(&self.pool)
                .await?;

        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_slice(&data)?))
            .collect()
    }

    async fn get_inbound_group_sessions(&self) -> Result<Vec<InboundGroupSession>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_inbound_group_sessions")
                .fetch_all(&self.pool)
                .await?;

        rows.into_iter()
            .map(|(data,)| {
                let pickle: PickledInboundGroupSession = serde_json::from_slice(&data)?;
                Ok(InboundGroupSession::from_pickle(pickle)
                    .expect("Failed to unpickle inbound group session"))
            })
            .collect()
    }

    async fn inbound_group_session_counts(
        &self,
        backup_version: Option<&str>,
    ) -> Result<RoomKeyCounts> {
        let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM crypto_inbound_group_sessions")
            .fetch_one(&self.pool)
            .await?;

        let backed_up = if let Some(version) = backup_version {
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM crypto_inbound_group_sessions WHERE backed_up_to = $1",
            )
            .bind(version)
            .fetch_one(&self.pool)
            .await?;
            row.0 as usize
        } else {
            0
        };

        Ok(RoomKeyCounts {
            total: total.0 as usize,
            backed_up,
        })
    }

    async fn get_inbound_group_sessions_by_room_id(
        &self,
        room_id: &RoomId,
    ) -> Result<Vec<InboundGroupSession>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_inbound_group_sessions WHERE room_id = $1")
                .bind(room_id.as_str())
                .fetch_all(&self.pool)
                .await?;

        rows.into_iter()
            .map(|(data,)| {
                let pickle: PickledInboundGroupSession = serde_json::from_slice(&data)?;
                Ok(InboundGroupSession::from_pickle(pickle)
                    .expect("Failed to unpickle inbound group session"))
            })
            .collect()
    }

    async fn get_inbound_group_sessions_for_device_batch(
        &self,
        curve_key: Curve25519PublicKey,
        sender_data_type: SenderDataType,
        after_session_id: Option<String>,
        limit: usize,
    ) -> Result<Vec<InboundGroupSession>> {
        let sender_key = curve_key.to_base64();
        let sdt = sender_data_type as i32;

        let rows: Vec<(Vec<u8>,)> = if let Some(ref after) = after_session_id {
            sqlx::query_as(
                "SELECT data FROM crypto_inbound_group_sessions \
                 WHERE sender_key = $1 AND sender_data_type = $2 AND session_id > $3 \
                 ORDER BY session_id ASC LIMIT $4",
            )
            .bind(&sender_key)
            .bind(sdt)
            .bind(after)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                "SELECT data FROM crypto_inbound_group_sessions \
                 WHERE sender_key = $1 AND sender_data_type = $2 \
                 ORDER BY session_id ASC LIMIT $3",
            )
            .bind(&sender_key)
            .bind(sdt)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(|(data,)| {
                let pickle: PickledInboundGroupSession = serde_json::from_slice(&data)?;
                Ok(InboundGroupSession::from_pickle(pickle)
                    .expect("Failed to unpickle inbound group session"))
            })
            .collect()
    }

    async fn inbound_group_sessions_for_backup(
        &self,
        backup_version: &str,
        limit: usize,
    ) -> Result<Vec<InboundGroupSession>> {
        let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crypto_inbound_group_sessions \
             WHERE backed_up_to IS DISTINCT FROM $1 \
             LIMIT $2",
        )
        .bind(backup_version)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|(data,)| {
                let pickle: PickledInboundGroupSession = serde_json::from_slice(&data)?;
                Ok(InboundGroupSession::from_pickle(pickle)
                    .expect("Failed to unpickle inbound group session"))
            })
            .collect()
    }

    async fn mark_inbound_group_sessions_as_backed_up(
        &self,
        backup_version: &str,
        room_and_session_ids: &[(&RoomId, &str)],
    ) -> Result<()> {
        for &(room_id, session_id) in room_and_session_ids {
            let row: Option<(Vec<u8>,)> = sqlx::query_as(
                "SELECT data FROM crypto_inbound_group_sessions WHERE room_id = $1 AND session_id = $2",
            )
            .bind(room_id.as_str())
            .bind(session_id)
            .fetch_optional(&self.pool)
            .await?;

            if let Some((data,)) = row {
                let pickle: PickledInboundGroupSession = serde_json::from_slice(&data)?;
                let session = InboundGroupSession::from_pickle(pickle)
                    .expect("Failed to unpickle inbound group session");
                session.mark_as_backed_up();
                let updated_pickle = session.pickle().await;
                let updated_data = serde_json::to_vec(&updated_pickle)?;

                sqlx::query(
                    "UPDATE crypto_inbound_group_sessions \
                     SET data = $1, backed_up_to = $2 \
                     WHERE room_id = $3 AND session_id = $4",
                )
                .bind(&updated_data)
                .bind(backup_version)
                .bind(room_id.as_str())
                .bind(session_id)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn reset_backup_state(&self) -> Result<()> {
        // No-op: we track backed_up_to per-session with the version string,
        // same pattern as the memory store.
        Ok(())
    }

    async fn load_backup_keys(&self) -> Result<BackupKeys> {
        let row: Option<(Option<String>, Option<Vec<u8>>)> = sqlx::query_as(
            "SELECT backup_version, backup_decryption_key FROM crypto_backup_keys WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((version, key_data)) => {
                let decryption_key = key_data.map(|d| serde_json::from_slice(&d)).transpose()?;
                Ok(BackupKeys {
                    backup_version: version,
                    decryption_key,
                })
            }
            None => Ok(BackupKeys::default()),
        }
    }

    async fn load_dehydrated_device_pickle_key(&self) -> Result<Option<DehydratedDeviceKey>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_dehydrated_device_key WHERE id = 1")
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn delete_dehydrated_device_pickle_key(&self) -> Result<()> {
        sqlx::query("DELETE FROM crypto_dehydrated_device_key WHERE id = 1")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_outbound_group_session(
        &self,
        room_id: &RoomId,
    ) -> Result<Option<OutboundGroupSession>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_outbound_group_sessions WHERE room_id = $1")
                .bind(room_id.as_str())
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => {
                let pickle = serde_json::from_slice(&data)?;
                let account = self
                    .get_static_account()
                    .expect("Account must be loaded before getting outbound group session");
                Ok(Some(
                    OutboundGroupSession::from_pickle(
                        account.device_id,
                        account.identity_keys,
                        pickle,
                    )
                    .expect("Failed to unpickle outbound group session"),
                ))
            }
            None => Ok(None),
        }
    }

    async fn load_tracked_users(&self) -> Result<Vec<TrackedUser>> {
        let rows: Vec<(String, bool)> =
            sqlx::query_as("SELECT user_id, dirty FROM crypto_tracked_users")
                .fetch_all(&self.pool)
                .await?;

        Ok(rows
            .into_iter()
            .map(|(user_id, dirty)| TrackedUser {
                user_id: UserId::parse(user_id).expect("Invalid user_id in tracked users"),
                dirty,
            })
            .collect())
    }

    async fn save_tracked_users(&self, users: &[(&UserId, bool)]) -> Result<()> {
        for &(user_id, dirty) in users {
            sqlx::query(
                "INSERT INTO crypto_tracked_users (user_id, dirty) VALUES ($1, $2) \
                 ON CONFLICT (user_id) DO UPDATE SET dirty = $2",
            )
            .bind(user_id.as_str())
            .bind(dirty)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<DeviceData>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_devices WHERE user_id = $1 AND device_id = $2")
                .bind(user_id.as_str())
                .bind(device_id.as_str())
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn get_user_devices(
        &self,
        user_id: &UserId,
    ) -> Result<HashMap<OwnedDeviceId, DeviceData>> {
        let rows: Vec<(String, Vec<u8>)> =
            sqlx::query_as("SELECT device_id, data FROM crypto_devices WHERE user_id = $1")
                .bind(user_id.as_str())
                .fetch_all(&self.pool)
                .await?;

        let mut devices = HashMap::new();
        for (device_id, data) in rows {
            let device: DeviceData = serde_json::from_slice(&data)?;
            let owned_device_id: OwnedDeviceId = device_id.into();
            devices.insert(owned_device_id, device);
        }
        Ok(devices)
    }

    async fn get_own_device(&self) -> Result<DeviceData> {
        let account = self
            .get_static_account()
            .expect("Account must be loaded before calling get_own_device");

        Ok(self
            .get_device(&account.user_id, &account.device_id)
            .await?
            .expect("Own device must exist in the store"))
    }

    async fn get_user_identity(&self, user_id: &UserId) -> Result<Option<UserIdentityData>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_user_identities WHERE user_id = $1")
                .bind(user_id.as_str())
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn is_message_known(&self, message_hash: &OlmMessageHash) -> Result<bool> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT 1 FROM crypto_olm_hashes WHERE sender_key = $1 AND hash = $2")
                .bind(&message_hash.sender_key)
                .bind(&message_hash.hash)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.is_some())
    }

    async fn get_outgoing_secret_requests(
        &self,
        request_id: &TransactionId,
    ) -> Result<Option<GossipRequest>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_gossip_requests WHERE request_id = $1")
                .bind(request_id.as_str())
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn get_secret_request_by_info(
        &self,
        secret_info: &SecretInfo,
    ) -> Result<Option<GossipRequest>> {
        let info_key = encode_key_info(secret_info);

        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_gossip_requests WHERE info_key = $1")
                .bind(&info_key)
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn get_unsent_secret_requests(&self) -> Result<Vec<GossipRequest>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_gossip_requests WHERE sent_out = false")
                .fetch_all(&self.pool)
                .await?;

        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_slice(&data)?))
            .collect()
    }

    async fn delete_outgoing_secret_requests(&self, request_id: &TransactionId) -> Result<()> {
        sqlx::query("DELETE FROM crypto_gossip_requests WHERE request_id = $1")
            .bind(request_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_secrets_from_inbox(
        &self,
        secret_name: &SecretName,
    ) -> Result<Vec<GossippedSecret>> {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_secrets_inbox WHERE secret_name = $1")
                .bind(secret_name.as_str())
                .fetch_all(&self.pool)
                .await?;

        rows.into_iter()
            .map(|(data,)| Ok(serde_json::from_slice(&data)?))
            .collect()
    }

    async fn delete_secrets_from_inbox(&self, secret_name: &SecretName) -> Result<()> {
        sqlx::query("DELETE FROM crypto_secrets_inbox WHERE secret_name = $1")
            .bind(secret_name.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_room_settings(&self, room_id: &RoomId) -> Result<Option<RoomSettings>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_room_settings WHERE room_id = $1")
                .bind(room_id.as_str())
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn get_received_room_key_bundle_data(
        &self,
        room_id: &RoomId,
        user_id: &UserId,
    ) -> Result<Option<StoredRoomKeyBundleData>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT data FROM crypto_room_key_bundles WHERE room_id = $1 AND user_id = $2",
        )
        .bind(room_id.as_str())
        .bind(user_id.as_str())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((data,)) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    async fn get_custom_value(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM crypto_custom_values WHERE key = $1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;

        Ok(row.map(|(data,)| data))
    }

    async fn set_custom_value(&self, key: &str, value: Vec<u8>) -> Result<()> {
        sqlx::query(
            "INSERT INTO crypto_custom_values (key, data) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET data = $2",
        )
        .bind(key)
        .bind(&value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn remove_custom_value(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM crypto_custom_values WHERE key = $1")
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn try_take_leased_lock(
        &self,
        lease_duration_ms: u32,
        key: &str,
        holder: &str,
    ) -> Result<Option<CrossProcessLockGeneration>> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        let expiration_ms = now_ms + lease_duration_ms as i64;

        // Try to acquire or extend the lease using a single atomic query.
        // Returns the generation if we got the lock, empty result if not.
        let row: Option<(i64,)> = sqlx::query_as(
            "INSERT INTO crypto_lease_locks (key, holder, expiration_ts, generation) \
             VALUES ($1, $2, $3, 1) \
             ON CONFLICT (key) DO UPDATE \
             SET holder = $2, expiration_ts = $3, \
                 generation = CASE \
                     WHEN crypto_lease_locks.holder = $2 THEN crypto_lease_locks.generation \
                     ELSE crypto_lease_locks.generation + 1 \
                 END \
             WHERE crypto_lease_locks.holder = $2 OR crypto_lease_locks.expiration_ts < $4 \
             RETURNING generation",
        )
        .bind(key)
        .bind(holder)
        .bind(expiration_ms)
        .bind(now_ms)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(generation,)| generation as u64))
    }

    async fn get_size(&self) -> Result<Option<usize>> {
        Ok(None)
    }
}

impl PgCryptoStore {
    async fn save_inbound_group_sessions_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sessions: Vec<InboundGroupSession>,
        backed_up_to_version: Option<&str>,
    ) -> Result<()> {
        for session in sessions {
            let room_id = session.room_id().to_string();
            let session_id = session.session_id().to_owned();

            let backed_up = session.backed_up();
            if backed_up != backed_up_to_version.is_some() {
                warn!(
                    backed_up,
                    backed_up_to_version,
                    "Session backed-up flag does not correspond to backup version setting",
                );
            }

            let sender_key = session.sender_key().to_base64();
            let sender_data_type = session.sender_data_type() as i32;

            let pickle = session.pickle().await;
            let data = serde_json::to_vec(&pickle)?;

            sqlx::query(
                "INSERT INTO crypto_inbound_group_sessions \
                 (room_id, session_id, data, backed_up_to, sender_key, sender_data_type) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (room_id, session_id) DO UPDATE SET \
                 data = $3, backed_up_to = $4, sender_key = $5, sender_data_type = $6",
            )
            .bind(&room_id)
            .bind(&session_id)
            .bind(&data)
            .bind(backed_up_to_version)
            .bind(&sender_key)
            .bind(sender_data_type)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}
