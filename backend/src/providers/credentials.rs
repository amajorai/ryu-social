//! The credentials and operator overrides the provider registry is built from,
//! resolved once at boot (see [`super::registry`] for how they are used).
//!
//! ```text
//!   RYU_SOCIAL_TREG_BASE_URL         public catalog override, for a self-hosted Treg
//!   RYU_SOCIAL_BLUESKY_HANDLE        e.g. me.bsky.social
//!   RYU_SOCIAL_BLUESKY_APP_PASSWORD  an APP password, never the account password
//!   RYU_SOCIAL_BLUESKY_PDS           override
//!   RYU_SOCIAL_FAKE_PROVIDER=1       route everything to the in-memory fake (dev)
//!   RYU_SOCIAL_ACCOUNT_PROVIDERS     per-account pins: "acc_a=bluesky,acc_b=fake"
//! ```
//!
//! Bluesky reads always go to the public AppView, `https://public.api.bsky.app`.

use std::collections::HashMap;

use super::types::ProviderId;

/// Everything the registry needs to build its providers, resolved once at boot.
#[derive(Debug, Default, Clone)]
pub struct ProviderCredentials {
    pub treg_base_url: Option<String>,
    pub bluesky_handle: Option<String>,
    pub bluesky_app_password: Option<String>,
    pub bluesky_pds: Option<String>,
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
    /// Everything else — the Treg base URL and PDS overrides, the fake switch and the
    /// per-account pins — has no settings equivalent and stays env-only. Those are
    /// operator/dev controls, not user preferences. The Bluesky AppView is fixed in
    /// code ([`super::bluesky`]'s `DEFAULT_APPVIEW`).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_parse_leniently() {
        let pins = parse_pins("acc_a=bluesky, acc_b = fake ,,acc_c=nonsense,broken");
        assert_eq!(pins.get("acc_a"), Some(&ProviderId::Bluesky));
        assert_eq!(pins.get("acc_b"), Some(&ProviderId::Fake));
        assert_eq!(pins.len(), 2);
    }
}
