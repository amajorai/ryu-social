//! Resolution and the capability cache: which implementation serves a platform, and
//! what it says it can do.
//!
//! ## Where credentials come from — env first, then the node settings blob
//!
//! Two sources, in that order, resolved by
//! [`ProviderCredentials::from_node_settings`] at boot:
//!
//! 1. The PROCESS ENVIRONMENT, which Core controls when it spawns this sidecar. An
//!    operator-set variable always wins, so a deployment can pin a key the UI cannot
//!    override.
//! 2. [`crate::settings::NodeSettings`] — the persisted, user-editable blob behind
//!    the settings tab, for the normal case where there is no operator and the user
//!    pastes their own key.
//!
//! Never from a const; that would ship a key in the binary.
//!
//! **Why the settings blob is safe here and `SocialSettings` would not be.** These
//! are two different structs and only one of them is safe to read secrets from.
//! `SocialSettings` is the per-workspace blob serialized WHOLESALE to the caller on
//! every `GET /settings` — a `composio_api_key` field there would hand the key to any
//! client that opened the settings tab. `NodeSettings` is node-scoped and never
//! leaves this process in its raw form: the only shape that crosses the wire is
//! `NodeSettings::redacted()`, which reports `<set>`/`<unset>` and the (public)
//! Bluesky handle, and that is the shape `GET /settings` actually returns. So reading
//! secrets from `NodeSettings` costs nothing in exposure, while refusing to read them
//! is not free — it is what made the settings tab's credential fields inert.
//!
//! **A credential change requires a sidecar restart.** The registry builds its
//! providers once at boot and hands out `Arc` clones, so editing a key in the
//! settings tab does not re-resolve a live provider. Core restarts the sidecar on
//! demand; hot-reloading would mean putting `Inner` behind a lock on every publish
//! path to serve a case that happens approximately once per install.
//!
//! ```text
//!   RYU_SOCIAL_COMPOSIO_API_KEY      (or COMPOSIO_API_KEY)
//!   RYU_SOCIAL_COMPOSIO_BASE_URL     override, for a proxy or a test double
//!   RYU_SOCIAL_BLUESKY_HANDLE        e.g. me.bsky.social
//!   RYU_SOCIAL_BLUESKY_APP_PASSWORD  an APP password, never the account password
//!   RYU_SOCIAL_BLUESKY_PDS           override
//!   RYU_SOCIAL_BLUESKY_APPVIEW       override
//!   RYU_SOCIAL_FAKE_PROVIDER=1       route everything to the in-memory fake (dev)
//!   RYU_SOCIAL_ACCOUNT_PROVIDERS     per-account pins: "acc_a=bluesky,acc_b=fake"
//! ```
//!
//! ## Resolution order
//!
//! 1. A per-ACCOUNT pin, when one is configured. This is the escape hatch for a
//!    workspace with two accounts on one platform reached different ways.
//! 2. A DIRECT adapter for the platform, when its credentials exist — Bluesky today.
//!    Direct wins even when Composio is also configured, because a first-party API is
//!    strictly better informed than a broker wrapping it.
//! 3. The active general provider: Composio, when a key exists.
//! 4. The fake, but ONLY when explicitly enabled.
//! 5. Otherwise [`UnconfiguredProvider`].
//!
//! Step 4 is opt-in rather than the default on purpose. A fake that publishes
//! successfully by default would render as a working integration — green toasts, a
//! populated history, remote URLs that 404 — and the user would find out at exactly
//! the wrong moment. An unconfigured install fails loudly instead.
//!
//! ## The capability cache
//!
//! Keyed on `(provider id, platform)` so switching fake→composio cannot serve a stale
//! answer, and **in memory only, never persisted**: it is derived, cheaply
//! re-fetchable data, and persisting it would put a cache under this app's data
//! retention contract for no benefit. Unlike the design this is ported from, entries
//! carry a TTL — Composio's matrix is inferred from a live tool catalog that changes
//! when the user authorizes new scopes, and a process-lifetime cache would pin a
//! newly-connected account's capabilities at whatever they were before it connected.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::bluesky::{BlueskyCredentials, BlueskyProvider};
use super::composio::{ApiKey, ComposioProvider};
use super::fake::FakeProvider;
use super::types::{PlatformProvider, ProviderAccount, ProviderId, UnconfiguredProvider};
use crate::models::{now_ms, Platform, PlatformCapabilities};

/// How long a cached capability answer is trusted.
const CAPABILITY_TTL_MS: i64 = 5 * 60 * 1_000;

/// Everything the registry needs to build its providers, resolved once at boot.
#[derive(Debug, Default, Clone)]
pub struct ProviderCredentials {
    pub composio_api_key: Option<String>,
    pub composio_base_url: Option<String>,
    pub bluesky_handle: Option<String>,
    pub bluesky_app_password: Option<String>,
    pub bluesky_pds: Option<String>,
    pub bluesky_appview: Option<String>,
    /// Route every platform to the in-memory fake. Dev/demo only.
    pub use_fake: bool,
    /// `social_accounts.id` → the provider that account must use.
    pub account_pins: HashMap<String, ProviderId>,
}

/// Read a non-empty, trimmed env var.
fn env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

impl ProviderCredentials {
    pub fn from_env() -> Self {
        Self {
            composio_api_key: env("RYU_SOCIAL_COMPOSIO_API_KEY")
                .or_else(|| env("COMPOSIO_API_KEY")),
            composio_base_url: env("RYU_SOCIAL_COMPOSIO_BASE_URL"),
            bluesky_handle: env("RYU_SOCIAL_BLUESKY_HANDLE"),
            bluesky_app_password: env("RYU_SOCIAL_BLUESKY_APP_PASSWORD"),
            bluesky_pds: env("RYU_SOCIAL_BLUESKY_PDS"),
            bluesky_appview: env("RYU_SOCIAL_BLUESKY_APPVIEW"),
            use_fake: env("RYU_SOCIAL_FAKE_PROVIDER")
                .map(|v| matches!(v.as_str(), "1" | "true" | "on" | "yes"))
                .unwrap_or(false),
            account_pins: env("RYU_SOCIAL_ACCOUNT_PROVIDERS")
                .map(|raw| parse_pins(&raw))
                .unwrap_or_default(),
        }
    }

    /// The boot path Core actually takes: environment first, then the persisted node
    /// settings for the two credentials the settings tab can edit.
    ///
    /// `NodeSettings`' own accessors already resolve env-before-blob (they are built
    /// on `settings::resolve`, which reads the same `RYU_SOCIAL_*` names this module
    /// documents), so this is a strict SUPERSET of [`Self::from_env`] for those
    /// fields rather than a different precedence — with one exception that has to be
    /// re-added by hand: `from_env` also honours a bare `COMPOSIO_API_KEY`, which the
    /// settings accessor does not know about. Dropping that fallback would silently
    /// unconfigure any install that set the generic Composio variable, so it is
    /// applied last, after both env-specific and stored values miss.
    ///
    /// Everything else — the base-URL/PDS/AppView overrides, the fake switch and the
    /// per-account pins — has no settings equivalent and stays env-only. Those are
    /// operator/dev controls, not user preferences.
    pub fn from_node_settings(node: &crate::settings::NodeSettings) -> Self {
        let bluesky = node.bluesky_credentials();
        // `bluesky_credentials()` is all-or-nothing by design — it returns `None` for
        // a half pair rather than a partial one. That is the right value, but it
        // would also swallow the half-pair WARNING `from_credentials` raises, since
        // the `(Some, None)` arm can no longer be reached down this path. A user who
        // filled in only the handle would get silence and then a publish failure that
        // names neither cause, so the diagnostic is re-raised here instead.
        if bluesky.is_none() {
            let handle_set =
                node.bluesky_handle.is_some() || env("RYU_SOCIAL_BLUESKY_HANDLE").is_some();
            let password_set = node.bluesky_app_password.is_some()
                || env("RYU_SOCIAL_BLUESKY_APP_PASSWORD").is_some();
            if handle_set != password_set {
                tracing::warn!(
                    handle_set,
                    password_set,
                    "ryu-social: bluesky needs BOTH a handle and an app password; ignoring the half that is set"
                );
            }
        }
        Self {
            composio_api_key: node.composio_api_key().or_else(|| env("COMPOSIO_API_KEY")),
            bluesky_handle: bluesky.as_ref().map(|(handle, _)| handle.clone()),
            bluesky_app_password: bluesky.as_ref().map(|(_, password)| password.clone()),
            ..Self::from_env()
        }
    }
}

/// `"acc_a=bluesky,acc_b=fake"` → a pin map. Unparseable entries are skipped with a
/// warning rather than failing boot: a typo in one pin must not take the sidecar down.
fn parse_pins(raw: &str) -> HashMap<String, ProviderId> {
    let mut pins = HashMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        match entry.split_once('=') {
            Some((account, provider)) => match ProviderId::parse(provider) {
                Some(id) => {
                    pins.insert(account.trim().to_string(), id);
                }
                None => tracing::warn!(
                    entry,
                    "ryu-social: ignoring unknown provider in RYU_SOCIAL_ACCOUNT_PROVIDERS"
                ),
            },
            None => tracing::warn!(
                entry,
                "ryu-social: ignoring malformed RYU_SOCIAL_ACCOUNT_PROVIDERS entry"
            ),
        }
    }
    pins
}

#[derive(Debug, Clone, Copy)]
struct CachedCapabilities {
    value: PlatformCapabilities,
    fetched_at: i64,
}

struct Inner {
    composio: Option<Arc<ComposioProvider>>,
    bluesky: Option<Arc<BlueskyProvider>>,
    fake: Option<Arc<FakeProvider>>,
    fallback: Arc<UnconfiguredProvider>,
    /// When set, EVERY platform resolves here. Used by the pipeline's tests and by
    /// nothing else.
    forced: Option<Arc<dyn PlatformProvider>>,
    pins: HashMap<String, ProviderId>,
    capabilities: RwLock<HashMap<(ProviderId, Platform), CachedCapabilities>>,
}

/// Resolves a platform (or an account) to the provider that should serve it, and
/// memoizes the capability answers.
///
/// Cheap to clone (`Arc` inside) so it can live on [`crate::state::AppState`].
#[derive(Clone)]
pub struct ProviderRegistry {
    inner: Arc<Inner>,
}

impl ProviderRegistry {
    /// The boot path: credentials from the environment, a fresh HTTP client.
    ///
    /// The client comes from [`crate::state::build_http_client`], not
    /// `reqwest::Client::new()` — a default client has NO request or connect
    /// timeout, so a provider that accepts the connection and never answers would
    /// hang the caller forever. `AppState::new` shares its own client with
    /// [`Self::with_client`]; this path is for the standalone constructions (tests,
    /// `Default`) that would otherwise silently opt out of the bound.
    pub fn new() -> Self {
        Self::from_credentials(
            crate::state::build_http_client(),
            ProviderCredentials::from_env(),
        )
    }

    /// Same, sharing an existing client — one connection pool for the whole process.
    pub fn with_client(http: reqwest::Client) -> Self {
        Self::from_credentials(http, ProviderCredentials::from_env())
    }

    pub fn from_credentials(http: reqwest::Client, credentials: ProviderCredentials) -> Self {
        // Construction is eager but side-effect free: no provider dials out in its
        // constructor, so building the whole set costs an allocation.
        let composio = credentials.composio_api_key.as_ref().map(|key| {
            Arc::new(ComposioProvider::new(
                http.clone(),
                ApiKey::new(key.clone()),
                credentials.composio_base_url.clone(),
            ))
        });
        let bluesky = match (
            credentials.bluesky_handle.as_ref(),
            credentials.bluesky_app_password.as_ref(),
        ) {
            (Some(handle), Some(password)) => Some(Arc::new(BlueskyProvider::new(
                http,
                BlueskyCredentials::new(handle.clone(), password.clone()),
                credentials.bluesky_pds.clone(),
                credentials.bluesky_appview.clone(),
            ))),
            // A handle with no password (or the reverse) is a misconfiguration, not a
            // partial capability — warn and fall through rather than half-connecting.
            (Some(_), None) | (None, Some(_)) => {
                tracing::warn!(
                    "ryu-social: bluesky needs BOTH RYU_SOCIAL_BLUESKY_HANDLE and RYU_SOCIAL_BLUESKY_APP_PASSWORD; ignoring the one that is set"
                );
                None
            }
            (None, None) => None,
        };
        if credentials.use_fake {
            tracing::warn!(
                "ryu-social: RYU_SOCIAL_FAKE_PROVIDER is on — publishes are simulated in memory and nothing reaches a real platform"
            );
        }

        Self {
            inner: Arc::new(Inner {
                composio,
                bluesky,
                fake: credentials.use_fake.then(|| Arc::new(FakeProvider::new())),
                fallback: Arc::new(UnconfiguredProvider),
                forced: None,
                pins: credentials.account_pins,
                capabilities: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Route every platform to one provider. The seam the publish pipeline's tests
    /// drive; keep a clone of the concrete `Arc` to assert on what it was handed.
    pub fn with_provider<P: PlatformProvider + 'static>(provider: Arc<P>) -> Self {
        Self {
            inner: Arc::new(Inner {
                composio: None,
                bluesky: None,
                fake: None,
                fallback: Arc::new(UnconfiguredProvider),
                forced: Some(provider),
                pins: HashMap::new(),
                capabilities: RwLock::new(HashMap::new()),
            }),
        }
    }

    fn by_id(&self, id: ProviderId) -> Option<Arc<dyn PlatformProvider>> {
        match id {
            ProviderId::Composio => self
                .inner
                .composio
                .clone()
                .map(|p| p as Arc<dyn PlatformProvider>),
            ProviderId::Bluesky => self
                .inner
                .bluesky
                .clone()
                .map(|p| p as Arc<dyn PlatformProvider>),
            ProviderId::Fake => self
                .inner
                .fake
                .clone()
                .map(|p| p as Arc<dyn PlatformProvider>),
            // No Threads adapter yet; a pin to it resolves to nothing and falls back.
            ProviderId::Threads => None,
            ProviderId::Unconfigured => Some(self.inner.fallback.clone()),
        }
    }

    /// The provider that should serve `platform`. Infallible by design — there is
    /// always at least the fallback, so no call site needs a "no provider" branch.
    pub fn provider_for(&self, platform: Platform) -> Arc<dyn PlatformProvider> {
        if let Some(forced) = &self.inner.forced {
            return forced.clone();
        }
        // 1. Direct adapters, by platform.
        if platform == Platform::Bluesky {
            if let Some(bluesky) = &self.inner.bluesky {
                return bluesky.clone();
            }
        }
        // 2. The general broker.
        if let Some(composio) = &self.inner.composio {
            return composio.clone();
        }
        // 3. The fake, only when explicitly enabled.
        if let Some(fake) = &self.inner.fake {
            return fake.clone();
        }
        // 4. An honest all-false.
        self.inner.fallback.clone()
    }

    /// The provider for one ACCOUNT, honouring a per-account pin.
    ///
    /// A pin naming a provider that is not configured falls through to the normal
    /// platform resolution rather than failing: the pin is an operator preference,
    /// not a lock that should be able to take publishing offline.
    pub fn provider_for_account(&self, account: &ProviderAccount) -> Arc<dyn PlatformProvider> {
        if let Some(forced) = &self.inner.forced {
            return forced.clone();
        }
        if let Some(id) = self.inner.pins.get(&account.id) {
            if let Some(provider) = self.by_id(*id) {
                return provider;
            }
            tracing::warn!(
                account = %account.id,
                pinned = %id,
                "ryu-social: pinned provider is not configured; falling back to platform resolution"
            );
        }
        self.provider_for(account.platform)
    }

    /// The capability matrix for one platform, through whichever provider serves it.
    pub async fn capabilities_for(&self, platform: Platform) -> PlatformCapabilities {
        self.capabilities_via(self.provider_for(platform), platform)
            .await
    }

    /// Same, for a specific account (so a pinned account reports ITS provider's
    /// matrix rather than the platform default's).
    pub async fn capabilities_for_account(
        &self,
        account: &ProviderAccount,
    ) -> PlatformCapabilities {
        self.capabilities_via(self.provider_for_account(account), account.platform)
            .await
    }

    async fn capabilities_via(
        &self,
        provider: Arc<dyn PlatformProvider>,
        platform: Platform,
    ) -> PlatformCapabilities {
        let key = (provider.id(), platform);
        let now = now_ms();
        {
            // Scoped tightly: the guard must be dropped before the provider call, or
            // the whole future stops being `Send` and no axum handler can hold it.
            let cache = self.inner.capabilities.read().await;
            if let Some(hit) = cache.get(&key) {
                if now - hit.fetched_at < CAPABILITY_TTL_MS {
                    return hit.value;
                }
            }
        }
        let value = provider.capabilities(platform).await;
        {
            let mut cache = self.inner.capabilities.write().await;
            cache.insert(
                key,
                CachedCapabilities {
                    value,
                    fetched_at: now,
                },
            );
        }
        value
    }

    /// Drop every cached matrix. Called when credentials change; also what a test
    /// uses to prove the cache is a cache and not a one-shot.
    pub async fn invalidate_capabilities(&self) {
        self.inner.capabilities.write().await.clear();
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(id: &str, platform: Platform) -> ProviderAccount {
        ProviderAccount {
            id: id.to_string(),
            platform,
            label: None,
            external_id: None,
        }
    }

    fn credentials() -> ProviderCredentials {
        ProviderCredentials::default()
    }

    #[test]
    fn nothing_configured_resolves_to_the_honest_fallback_not_the_fake() {
        let registry = ProviderRegistry::from_credentials(reqwest::Client::new(), credentials());
        for platform in Platform::ALL {
            assert_eq!(
                registry.provider_for(platform).id(),
                ProviderId::Unconfigured,
                "{platform}"
            );
        }
    }

    #[test]
    fn a_direct_adapter_beats_the_broker_for_its_own_platform_only() {
        let registry = ProviderRegistry::from_credentials(
            reqwest::Client::new(),
            ProviderCredentials {
                composio_api_key: Some("k".into()),
                bluesky_handle: Some("me.bsky.social".into()),
                bluesky_app_password: Some("pw".into()),
                ..credentials()
            },
        );
        assert_eq!(
            registry.provider_for(Platform::Bluesky).id(),
            ProviderId::Bluesky
        );
        assert_eq!(
            registry.provider_for(Platform::X).id(),
            ProviderId::Composio
        );
    }

    #[test]
    fn half_configured_bluesky_is_ignored_rather_than_half_connected() {
        let registry = ProviderRegistry::from_credentials(
            reqwest::Client::new(),
            ProviderCredentials {
                bluesky_handle: Some("me.bsky.social".into()),
                ..credentials()
            },
        );
        assert_eq!(
            registry.provider_for(Platform::Bluesky).id(),
            ProviderId::Unconfigured
        );
    }

    #[test]
    fn an_account_pin_overrides_platform_resolution_and_degrades_when_unconfigured() {
        let registry = ProviderRegistry::from_credentials(
            reqwest::Client::new(),
            ProviderCredentials {
                composio_api_key: Some("k".into()),
                use_fake: true,
                account_pins: [
                    ("acc_fake".to_string(), ProviderId::Fake),
                    // Pinned to an adapter with no credentials: must fall through.
                    ("acc_bsky".to_string(), ProviderId::Bluesky),
                ]
                .into_iter()
                .collect(),
                ..credentials()
            },
        );
        assert_eq!(
            registry
                .provider_for_account(&account("acc_fake", Platform::X))
                .id(),
            ProviderId::Fake
        );
        assert_eq!(
            registry
                .provider_for_account(&account("acc_bsky", Platform::X))
                .id(),
            ProviderId::Composio
        );
        assert_eq!(
            registry
                .provider_for_account(&account("acc_other", Platform::X))
                .id(),
            ProviderId::Composio
        );
    }

    #[test]
    fn pins_parse_leniently() {
        let pins = parse_pins("acc_a=bluesky, acc_b = fake ,,acc_c=nonsense,broken");
        assert_eq!(pins.get("acc_a"), Some(&ProviderId::Bluesky));
        assert_eq!(pins.get("acc_b"), Some(&ProviderId::Fake));
        assert_eq!(pins.len(), 2);
    }

    #[tokio::test]
    async fn capabilities_are_cached_per_provider_and_invalidatable() {
        let fake = Arc::new(FakeProvider::new());
        let registry = ProviderRegistry::with_provider(fake);
        let first = registry.capabilities_for(Platform::X).await;
        let second = registry.capabilities_for(Platform::X).await;
        assert_eq!(first, second);
        assert!(first.publish);
        // schedule is false for EVERY provider — it is never delegated.
        assert!(!first.schedule);
        registry.invalidate_capabilities().await;
        assert_eq!(registry.capabilities_for(Platform::X).await, first);
    }

    #[tokio::test]
    async fn the_unconfigured_fallback_reports_an_empty_matrix() {
        let registry = ProviderRegistry::from_credentials(reqwest::Client::new(), credentials());
        assert_eq!(
            registry.capabilities_for(Platform::X).await,
            PlatformCapabilities::empty()
        );
    }
}
