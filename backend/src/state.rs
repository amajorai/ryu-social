//! The axum state every handler is built over: the store, an HTTP client, the
//! provider registry, the process config, and the app-event emitter.
//!
//! One state struct rather than per-module states, because the later-owned modules
//! (publish, scheduler, inbox, analytics) each need three of these five and a
//! narrower state per module would just mean converting between them at every call.
//! Every field is cheap to clone (`Arc` inside), so `State<AppState>` extraction
//! costs nothing per request.

use std::sync::Arc;
use std::time::Duration;

use crate::providers::ProviderRegistry;
use crate::store::SocialStore;

/// The hard ceiling on ONE outbound provider call, end to end.
///
/// This is the number the scheduler's lease math already assumed
/// (`scheduler::PER_ATTEMPT_ALLOWANCE_MS` is defined from it) — it lives here
/// because this is where the assumption is actually ENFORCED. Before, nothing bound
/// a provider call at all: a broker that accepts the TCP connection and never
/// answers left `provider.publish(…).await` pending forever, and because the batch
/// runner awaits `join_next()`, one such call wedged the entire tick loop for every
/// workspace — silently, since `/health` only touches the store and kept answering
/// 200.
///
/// It is also the bound on the process-death window `publish`'s module docs concede
/// between a provider returning Ok and the history commit: unbounded before, at most
/// this now.
pub const PROVIDER_CALL_TIMEOUT_MS: u64 = 30_000;

/// Ceiling on the TCP+TLS handshake specifically, well under the whole-call bound.
/// A host that is not answering at all should fail fast rather than burn the full
/// allowance, because a connect that hangs will not start succeeding at second 29.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The one HTTP client shape this process uses for outbound provider traffic.
///
/// A free function rather than an inline `Client::new()` at each site, because there
/// are two construction points ([`AppState::new`] and
/// [`ProviderRegistry::new`](crate::providers::ProviderRegistry::new)) and a bound
/// that holds at only one of them is not a bound. Falls back to the default client
/// if the builder ever fails, so a timeout config problem degrades to today's
/// behaviour instead of refusing to boot.
pub fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(PROVIDER_CALL_TIMEOUT_MS))
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// This app's manifest `id`. Core authorizes every app-event emit against it — the
/// caller must *be* the plugin the event is namespaced to — so it must stay
/// byte-identical to the `id` in `apps-store/social/manifest.json`.
pub const PLUGIN_ID: &str = "@ryu/social";

/// The events this app declares in its manifest's `contributes.hook_events`.
///
/// Held as constants next to the id so the `<plugin id>#<name>` rule Core enforces
/// at load is checkable at a glance rather than spread across the handlers that
/// raise them.
pub const EVENT_POST_SCHEDULED: &str = "@ryu/social#post.scheduled";
pub const EVENT_POST_PUBLISHED: &str = "@ryu/social#post.published";
pub const EVENT_POST_FAILED: &str = "@ryu/social#post.failed";
pub const EVENT_INBOX_RECEIVED: &str = "@ryu/social#inbox.received";

/// Process-level configuration, resolved once at boot from the environment.
///
/// Distinct from [`crate::models::SocialSettings`], which is per-workspace and
/// user-editable. The split matters: a user must not be able to change the port or
/// the shared secret from the settings tab.
#[derive(Debug, Clone)]
pub struct Config {
    /// The loopback port this process listens on.
    pub port: u16,
    /// How many posts one sweep may claim. Bounds the blast radius of a large
    /// backlog: without it, a node that was offline for a week would try to publish
    /// its entire queue in one tick.
    pub sweep_batch_size: usize,
    /// Whether the tick loop runs at all. `RYU_SOCIAL_SCHEDULER=0` disables it,
    /// which is what a test harness or a second read-only replica wants.
    pub scheduler_enabled: bool,
}

impl Config {
    /// Read from the environment, with the defaults a normal Core-spawned run uses.
    pub fn from_env(port: u16) -> Self {
        Self {
            port,
            sweep_batch_size: std::env::var("RYU_SOCIAL_SWEEP_BATCH")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(50),
            scheduler_enabled: std::env::var("RYU_SOCIAL_SCHEDULER")
                .map(|v| !matches!(v.trim(), "0" | "false" | "off"))
                .unwrap_or(true),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub store: SocialStore,
    /// One shared client for every outbound provider call. Shared deliberately:
    /// `reqwest::Client` owns a connection pool, and building one per request would
    /// re-do TLS on every publish. Built by [`build_http_client`], so it carries the
    /// request and connect timeouts — never `Client::new()`, which has neither.
    pub http: reqwest::Client,
    pub config: Arc<Config>,
    pub providers: ProviderRegistry,
    /// Raises this app's declared hook events so plugin hooks and event-triggered
    /// workflows can react to a post publishing without either side knowing the
    /// other exists.
    ///
    /// Safe to hold unconditionally: `from_env` never fails, and every emit no-ops
    /// when `RYU_CORE_PORT`/`RYU_EXT_TOKEN` are absent — which is the state under
    /// this crate's own tests and any standalone run, so no test needs a live Core.
    pub events: ryu_app_events::EventEmitter,
}

impl AppState {
    pub fn new(store: SocialStore, config: Config) -> Self {
        let http = build_http_client();
        Self {
            store,
            // One pool for the whole process: the registry gets the SAME client, so
            // the timeouts and the connection reuse are both single-sourced.
            providers: ProviderRegistry::with_client(http.clone()),
            http,
            config: Arc::new(config),
            events: ryu_app_events::EventEmitter::from_env(PLUGIN_ID),
        }
    }
}
