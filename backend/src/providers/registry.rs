//! Resolution and the capability cache: which implementation serves a platform, and
//! what it says it can do.
//!
//! ## Provider ownership
//!
//! Provider credentials are owned by Ryu's Gateway/provider vault. The registry
//! receives a provider-neutral bridge and only keeps account-specific credentials
//! (for example a Bluesky app password) in this satellite.
//!
//! Account credentials are node-scoped and redacted before `GET /settings`; provider
//! credentials are not part of either settings blob and are resolved by Gateway.
//!
//! **A credential change requires a sidecar restart.** The registry builds its
//! providers once at boot and hands out `Arc` clones, so editing a key in the
//! settings tab does not re-resolve a live provider. Core restarts the sidecar on
//! demand; hot-reloading would mean putting `Inner` behind a lock on every publish
//! path to serve a case that happens approximately once per install.
//!
//! ```text
//!   RYU_SOCIAL_TREG_BASE_URL         public catalog override, for a self-hosted Treg
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
//! 3. The active managed provider: Treg or Composio, when Gateway reports it ready.
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
use super::composio::ComposioProvider;
use super::fake::FakeProvider;
use super::treg::TregProvider;
use super::types::{
    PlatformProvider, ProviderAccount, ProviderId, ProviderOperation, PublishRequest,
    UnconfiguredProvider,
};
use crate::models::{now_ms, Platform, PlatformCapabilities};
use ryu_app_events::ProviderRouter;

/// How long a cached capability answer is trusted.
const CAPABILITY_TTL_MS: i64 = 5 * 60 * 1_000;

/// Everything the registry needs to build its providers, resolved once at boot.
#[derive(Debug, Default, Clone)]
pub struct ProviderCredentials {
    pub treg_base_url: Option<String>,
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
            treg_base_url: env("RYU_SOCIAL_TREG_BASE_URL")
                .or_else(|| env("RYU_TREG_URL"))
                .or_else(|| env("TREG_URL")),
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

    /// The boot path Core actually takes: account settings plus the managed provider
    /// bridge. Provider API keys are not read from the node settings blob.
    ///
    /// `NodeSettings`' accessors supply only account credentials here; provider
    /// credentials remain in Gateway.
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
    treg: Option<Arc<TregProvider>>,
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
        let http = crate::state::build_http_client();
        Self::from_credentials(http.clone(), ProviderCredentials::from_env())
    }

    /// Same, sharing an existing client — one connection pool for the whole process.
    pub fn with_client(http: reqwest::Client) -> Self {
        Self::from_credentials(http, ProviderCredentials::from_env())
    }

    pub fn from_credentials(http: reqwest::Client, credentials: ProviderCredentials) -> Self {
        let router = ProviderRouter::with_client(crate::state::PLUGIN_ID, http.clone());
        Self::from_credentials_with_router(http, credentials, router)
    }

    pub fn from_credentials_with_router(
        http: reqwest::Client,
        credentials: ProviderCredentials,
        router: ProviderRouter,
    ) -> Self {
        // Construction is eager but side-effect free: no provider dials out in its
        // constructor, so building the whole set costs an allocation.
        let composio = router
            .is_hosted()
            .then(|| Arc::new(ComposioProvider::managed(router.clone())));
        let treg = router.is_hosted().then(|| {
            Arc::new(TregProvider::new(
                http.clone(),
                credentials.treg_base_url.clone(),
                router.clone(),
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
                treg,
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
                treg: None,
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
            ProviderId::Treg => self
                .inner
                .treg
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
        // 2. The Treg broker, which may expose a capability Composio does not.
        if let Some(treg) = &self.inner.treg {
            return treg.clone();
        }
        // 3. The general Composio broker.
        if let Some(composio) = &self.inner.composio {
            return composio.clone();
        }
        // 4. The fake, only when explicitly enabled.
        if let Some(fake) = &self.inner.fake {
            return fake.clone();
        }
        // 5. An honest all-false.
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

    /// Resolve the first configured provider that advertises the requested
    /// operation. Treg is therefore selected for X text threads when Composio
    /// has no matching capability, while a media request can continue to fall
    /// through to a provider that supports it.
    pub async fn provider_for_operation(
        &self,
        account: &ProviderAccount,
        operation: ProviderOperation,
        request: Option<&PublishRequest>,
    ) -> Arc<dyn PlatformProvider> {
        let candidates = self.candidates_for_account(account);
        for candidate in candidates {
            if operation == ProviderOperation::Publish
                && request.is_some_and(|request| {
                    !request.media.is_empty() && candidate.id() == ProviderId::Treg
                })
            {
                continue;
            }
            let capabilities = self
                .capabilities_via(candidate.clone(), account.platform)
                .await;
            if operation.is_supported(capabilities) {
                return candidate;
            }
        }
        self.inner.fallback.clone()
    }

    fn candidates_for_account(&self, account: &ProviderAccount) -> Vec<Arc<dyn PlatformProvider>> {
        if let Some(forced) = &self.inner.forced {
            return vec![forced.clone()];
        }
        let mut candidates = Vec::new();
        if let Some(id) = self.inner.pins.get(&account.id) {
            if let Some(provider) = self.by_id(*id) {
                candidates.push(provider);
            }
        }
        if account.platform == Platform::Bluesky {
            if let Some(provider) = self.inner.bluesky.clone() {
                candidates.push(provider);
            }
        }
        if let Some(provider) = self.inner.treg.clone() {
            candidates.push(provider);
        }
        if let Some(provider) = self.inner.composio.clone() {
            candidates.push(provider);
        }
        if let Some(provider) = self.inner.fake.clone() {
            candidates.push(provider);
        }
        let mut seen = std::collections::HashSet::new();
        candidates
            .into_iter()
            .filter(|provider| seen.insert(provider.id()))
            .collect()
    }

    /// The capability matrix for one platform, through whichever provider serves it.
    pub async fn capabilities_for(&self, platform: Platform) -> PlatformCapabilities {
        self.merged_capabilities(&account_for_platform(platform))
            .await
    }

    /// Same, for a specific account (so a pinned account reports ITS provider's
    /// matrix rather than the platform default's).
    pub async fn capabilities_for_account(
        &self,
        account: &ProviderAccount,
    ) -> PlatformCapabilities {
        self.merged_capabilities(account).await
    }

    async fn merged_capabilities(&self, account: &ProviderAccount) -> PlatformCapabilities {
        let mut merged = PlatformCapabilities::empty();
        for provider in self.candidates_for_account(account) {
            let capabilities = self.capabilities_via(provider, account.platform).await;
            merged.publish |= capabilities.publish;
            merged.read_comments |= capabilities.read_comments;
            merged.read_dms |= capabilities.read_dms;
            merged.send_dm |= capabilities.send_dm;
            merged.read_engagement |= capabilities.read_engagement;
        }
        merged
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

fn account_for_platform(platform: Platform) -> ProviderAccount {
    ProviderAccount {
        id: format!("__platform__:{platform}"),
        platform,
        label: None,
        external_id: None,
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

    fn managed_registry(credentials: ProviderCredentials) -> ProviderRegistry {
        ProviderRegistry::from_credentials_with_router(
            reqwest::Client::new(),
            credentials,
            ProviderRouter::for_test(
                crate::state::PLUGIN_ID,
                reqwest::Client::new(),
                "http://127.0.0.1:1/providers/call",
                "http://127.0.0.1:1/providers/status",
            ),
        )
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
        let registry = managed_registry(ProviderCredentials {
            bluesky_handle: Some("me.bsky.social".into()),
            bluesky_app_password: Some("pw".into()),
            ..credentials()
        });
        assert_eq!(
            registry.provider_for(Platform::Bluesky).id(),
            ProviderId::Bluesky
        );
        assert_eq!(registry.provider_for(Platform::X).id(), ProviderId::Treg);
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
        let registry = managed_registry(ProviderCredentials {
            use_fake: true,
            account_pins: [
                ("acc_fake".to_string(), ProviderId::Fake),
                // Pinned to an adapter with no credentials: must fall through.
                ("acc_bsky".to_string(), ProviderId::Bluesky),
            ]
            .into_iter()
            .collect(),
            ..credentials()
        });
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
            ProviderId::Treg
        );
        assert_eq!(
            registry
                .provider_for_account(&account("acc_other", Platform::X))
                .id(),
            ProviderId::Treg
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
