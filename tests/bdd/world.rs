use std::collections::HashMap;
use std::sync::Arc;

use cucumber::World;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::{ContainerAsync, ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;
use wiremock::MockServer;

use std::sync::atomic::{AtomicU32, Ordering};

pub const SYNAPSE_PORT: u16 = 8008;
pub const SYNAPSE_VERSION: &str = "v1.146.0";
pub const SHARED_SECRET: &str = "test-secret-key";
pub const BOT_PASSWORD: &str = "bot_password";
pub const OBSERVER_USERNAME: &str = "observer";
pub const OBSERVER_PASSWORD: &str = "observer_password";
pub const ADMIN_USERNAME: &str = "issueadmin";
pub const ADMIN_PASSWORD: &str = "issueadmin_password";

static BOT_COUNTER: AtomicU32 = AtomicU32::new(0);

pub fn next_bot_username() -> String {
    let n = BOT_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("bot{n}")
}

pub struct SharedInfra {
    synapse_container: ContainerAsync<GenericImage>,
    postgres_container: ContainerAsync<Postgres>,
    pub synapse_port: u16,
    pub postgres_port: u16,
    pub admin_access_token: String,
    pub observer_access_token: String,
    pub issue_admin_access_token: String,
}

static SHARED_INFRA: OnceCell<SharedInfra> = OnceCell::const_new();

pub async fn get_shared_infra() -> &'static SharedInfra {
    SHARED_INFRA
        .get_or_init(|| async {
            let t0 = std::time::Instant::now();
            eprintln!("[test-infra] Starting Synapse container...");
            let (synapse_container, synapse_port) = start_synapse().await;
            eprintln!(
                "[test-infra] Synapse ready on port {synapse_port} ({:.1}s)",
                t0.elapsed().as_secs_f64()
            );

            let t1 = std::time::Instant::now();
            eprintln!("[test-infra] Starting Postgres container...");
            let (postgres_container, postgres_port) = start_postgres().await;
            eprintln!(
                "[test-infra] Postgres ready on port {postgres_port} ({:.1}s)",
                t1.elapsed().as_secs_f64()
            );

            let http = reqwest::Client::new();

            eprintln!("[test-infra] Registering admin user...");
            let admin_access_token = register_user_via_shared_secret(
                &http,
                synapse_port,
                "admin",
                "admin_password",
                true,
            )
            .await;

            eprintln!("[test-infra] Registering observer user...");
            let observer_access_token = register_user_via_shared_secret(
                &http,
                synapse_port,
                OBSERVER_USERNAME,
                OBSERVER_PASSWORD,
                false,
            )
            .await;

            eprintln!("[test-infra] Registering issue admin user...");
            let issue_admin_access_token = register_user_via_shared_secret(
                &http,
                synapse_port,
                ADMIN_USERNAME,
                ADMIN_PASSWORD,
                false,
            )
            .await;

            eprintln!("[test-infra] Running database migrations...");
            let database_url =
                format!("postgres://testuser:testpass@localhost:{postgres_port}/michel_bot_test");
            let pool = sqlx::PgPool::connect(&database_url)
                .await
                .expect("Failed to connect to Postgres for migrations");
            michel_bot::db::run_migrations(&pool)
                .await
                .expect("Failed to run migrations");
            pool.close().await;

            eprintln!(
                "[test-infra] All infrastructure ready ({:.1}s total)",
                t0.elapsed().as_secs_f64()
            );

            SharedInfra {
                synapse_container,
                postgres_container,
                synapse_port,
                postgres_port,
                admin_access_token,
                observer_access_token,
                issue_admin_access_token,
            }
        })
        .await
}

pub async fn stop_shared_infra() {
    eprintln!("[test-infra] Stopping containers...");
    if let Some(infra) = SHARED_INFRA.get() {
        let _ = infra.synapse_container.stop().await;
        let _ = infra.postgres_container.stop().await;
    }
    eprintln!("[test-infra] Containers stopped");
}

#[derive(World, Debug, Default)]
#[world(init = Self::default)]
pub struct TestWorld {
    pub synapse_port: u16,
    pub postgres_port: u16,
    pub bot_handle: Option<tokio::task::JoinHandle<()>>,
    pub bot_shutdown: Option<tokio::sync::watch::Sender<bool>>,
    pub bot_username: String,
    pub webhook_port: u16,
    pub observer_access_token: String,
    pub admin_access_token: String,
    pub room_id: String,
    pub room_alias: String,
    pub last_root_event_id: String,
    pub last_thread_event_id: String,
    pub seerr_mock: Option<Arc<MockServer>>,
    pub issue_admin_access_token: String,
}

impl Drop for TestWorld {
    fn drop(&mut self) {
        // Signal bot to shut down
        if let Some(tx) = self.bot_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.bot_handle.take() {
            handle.abort();
        }
    }
}

pub async fn start_synapse() -> (ContainerAsync<GenericImage>, u16) {
    let homeserver_yaml = format!(
        //language=yaml
        r#"server_name: "localhost"
pid_file: /data/homeserver.pid
listeners:
  - port: {SYNAPSE_PORT}
    tls: false
    type: http
    bind_addresses: ['::']
    x_forwarded: false
    resources:
      - names: [client, federation]
        compress: false
database:
  name: sqlite3
  args:
    database: "/data/homeserver.db"
log_config: "/data/localhost.log.config"
media_store_path: "/data/media_store"
registration_shared_secret: "{SHARED_SECRET}"
enable_registration: true
enable_registration_without_verification: true
report_stats: false
macaroon_secret_key: "test-macaroon-secret-key"
form_secret: "test-form-secret"
signing_key_path: "/data/localhost.signing.key"
suppress_key_server_warning: true
"#
    );

    let log_config = r#"version: 1
formatters:
  precise:
    format: '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(request)s - %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    formatter: precise
    stream: ext://sys.stderr
loggers:
  synapse.storage.SQL:
    level: WARN
root:
  level: INFO
  handlers: [console]
"#;

    let container = GenericImage::new("ghcr.io/element-hq/synapse", SYNAPSE_VERSION)
        .with_exposed_port(ContainerPort::Tcp(SYNAPSE_PORT))
        .with_wait_for(WaitFor::healthcheck())
        .with_copy_to("/data/homeserver.yaml", homeserver_yaml.into_bytes())
        .with_copy_to("/data/localhost.log.config", log_config.as_bytes().to_vec())
        .with_env_var("SYNAPSE_CONFIG_PATH", "/data/homeserver.yaml")
        .with_env_var("UID", "0")
        .with_env_var("GID", "0")
        .with_startup_timeout(std::time::Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start Synapse container");

    let port = container
        .get_host_port_ipv4(SYNAPSE_PORT)
        .await
        .expect("Failed to get Synapse port");

    (container, port)
}

pub async fn start_postgres() -> (ContainerAsync<Postgres>, u16) {
    let container = Postgres::default()
        .with_db_name("michel_bot_test")
        .with_user("testuser")
        .with_password("testpass")
        .start()
        .await
        .expect("Failed to start Postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get Postgres port");

    (container, port)
}

pub async fn register_user_via_shared_secret(
    http: &reqwest::Client,
    synapse_port: u16,
    username: &str,
    password: &str,
    admin: bool,
) -> String {
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    type HmacSha1 = Hmac<Sha1>;

    // Step 1: Get nonce
    let nonce_resp: serde_json::Value = http
        .get(format!(
            "http://localhost:{synapse_port}/_synapse/admin/v1/register"
        ))
        .send()
        .await
        .expect("Failed to get nonce")
        .json()
        .await
        .expect("Failed to parse nonce response");

    let nonce = nonce_resp["nonce"].as_str().expect("Missing nonce");

    // Step 2: Compute HMAC
    let admin_str = if admin { "admin" } else { "notadmin" };
    let mut mac =
        HmacSha1::new_from_slice(SHARED_SECRET.as_bytes()).expect("HMAC can take any key size");
    mac.update(nonce.as_bytes());
    mac.update(b"\x00");
    mac.update(username.as_bytes());
    mac.update(b"\x00");
    mac.update(password.as_bytes());
    mac.update(b"\x00");
    mac.update(admin_str.as_bytes());
    let mac_hex = hex::encode(mac.finalize().into_bytes());

    // Step 3: Register
    let register_resp: serde_json::Value = http
        .post(format!(
            "http://localhost:{synapse_port}/_synapse/admin/v1/register"
        ))
        .json(&serde_json::json!({
            "nonce": nonce,
            "username": username,
            "password": password,
            "admin": admin,
            "mac": mac_hex,
        }))
        .send()
        .await
        .expect("Failed to register user")
        .json()
        .await
        .expect("Failed to parse register response");

    register_resp["access_token"]
        .as_str()
        .expect("Missing access_token")
        .to_string()
}

pub async fn create_room(
    http: &reqwest::Client,
    synapse_port: u16,
    access_token: &str,
    alias_local: &str,
    invitees: &[&str],
) -> String {
    let resp: serde_json::Value = http
        .post(format!(
            "http://localhost:{synapse_port}/_matrix/client/v3/createRoom"
        ))
        .bearer_auth(access_token)
        .json(&serde_json::json!({
            "preset": "public_chat",
            "room_alias_name": alias_local,
            "name": alias_local,
            "invite": invitees,
        }))
        .send()
        .await
        .expect("Failed to create room")
        .json()
        .await
        .expect("Failed to parse createRoom response");

    resp["room_id"]
        .as_str()
        .expect("Missing room_id")
        .to_string()
}

pub fn table_to_map(step: &cucumber::gherkin::Step) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(table) = step.table.as_ref() {
        for row in &table.rows {
            if row.len() >= 2 {
                map.insert(row[0].trim().to_string(), row[1].trim().to_string());
            }
        }
    }
    map
}

pub async fn sync_and_find_messages(
    http: &reqwest::Client,
    synapse_port: u16,
    access_token: &str,
    room_id: &str,
) -> Vec<serde_json::Value> {
    let resp: serde_json::Value = http
        .get(format!(
            "http://localhost:{synapse_port}/_matrix/client/v3/rooms/{room_id}/messages"
        ))
        .bearer_auth(access_token)
        .query(&[("dir", "b"), ("limit", "50")])
        .send()
        .await
        .expect("Failed to get messages")
        .json()
        .await
        .expect("Failed to parse messages");

    resp["chunk"].as_array().cloned().unwrap_or_default()
}

pub async fn get_relations(
    http: &reqwest::Client,
    synapse_port: u16,
    access_token: &str,
    room_id: &str,
    event_id: &str,
    rel_type: &str,
) -> Vec<serde_json::Value> {
    let resp: serde_json::Value = http
        .get(format!(
            "http://localhost:{synapse_port}/_matrix/client/v1/rooms/{room_id}/relations/{event_id}/{rel_type}"
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .expect("Failed to get relations")
        .json()
        .await
        .expect("Failed to parse relations response");

    resp["chunk"].as_array().cloned().unwrap_or_default()
}
