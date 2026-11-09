//! The platform-provider abstraction: the ONE seam between "what Outpost wants to
//! do" and "how a given platform is actually reached".
//!
//! ```text
//!   types.rs      the contract — ProviderAccount / PublishRequest / PlatformProvider
//!   registry.rs   resolution (platform → provider, account → provider) + capability cache
//!   fake.rs       deterministic, in-memory, scriptable; the pipeline's test seam
//!   composio.rs   the managed broker for everything without a first-party adapter
//!   bluesky.rs    AT Protocol, native — and the only genuinely idempotent publish
//! ```
//!
//! ## Async dispatch
//!
//! `PlatformProvider` is an `#[async_trait]` object-safe trait behind
//! `Arc<dyn PlatformProvider>`. `async_trait` was already a declared dependency of
//! this crate for exactly this, so this adds nothing to the dependency set — and a
//! trait object beats an enum here because the registry hands the same handle to the
//! publish runner, the inbox refresher and the analytics reader, none of which should
//! have to match on which implementation they got.
//!
//! ## Two contracts every implementation must honour
//!
//! **Construction is side-effect free.** No network in a constructor — the registry
//! builds every provider eagerly at boot, and a constructor that dialled out would
//! make listing the capability matrix a multi-second operation.
//!
//! **`publish` never returns `Err` for an EXPECTED failure.** A rejected post, a rate
//! limit, an expired token — all of those are [`PublishResult::Err`], a value the
//! retry loop can inspect. `anyhow::Error` is reserved for "this provider is broken",
//! and collapsing the two would make every 4xx look like a bug.
//!
//! ## `schedule` is ALWAYS false
//!
//! Every implementation's [`crate::models::PlatformCapabilities`] reports
//! `schedule: false`, and that is not an oversight to fix later: scheduling is owned
//! by this app's own tick loop and is **never delegated to a provider**. The field
//! exists only so callers have one place to ask "is this action available" instead of
//! most actions being a capability lookup and one being a special case. A provider
//! that returned `true` would be claiming ownership of a queue it cannot see.
//!
//! ## Credentials
//!
//! Read from the process environment by [`registry`], never from a const and never
//! from `SocialSettings` (that blob is serialized to the client on every
//! `GET /settings`). See the [`registry`] module docs for the full variable list.

/// Core refused a managed provider call because of the account access of the
/// agent the work runs for. Kept as its own type so a caller that tolerates
/// ordinary upstream failures (an inbox poll skipping a missing tool) still
/// reports this one.
#[derive(Debug)]
pub(crate) struct AgentAccessRefused(String);

impl std::fmt::Display for AgentAccessRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Ryu blocked this for the agent that scheduled it: {}",
            self.0
        )
    }
}

impl std::error::Error for AgentAccessRefused {}

/// Whether `error` is Core refusing the call for the agent's account access.
pub(crate) fn is_agent_access_refusal(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AgentAccessRefused>().is_some()
}

/// Turn a managed provider call failure into the text a person reads on the
/// failed post. When Core refuses a call because of the scheduling agent's
/// account access, its own message says what to change, so it is used as is.
pub(crate) fn managed_call_error(error: ryu_app_events::ProviderRouterError) -> anyhow::Error {
    if let ryu_app_events::ProviderRouterError::Rejected { status: 403, body } = &error {
        let refusal = serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .filter(|body| body["reason"] == "agent_account_access")
            .and_then(|body| body["error"].as_str().map(str::to_owned));
        if let Some(message) = refusal {
            return AgentAccessRefused(message).into();
        }
    }
    anyhow::anyhow!(error.to_string())
}

mod bluesky;
mod composio;
mod credentials;
mod fake;
mod registry;
mod staged_upload;
mod treg;
mod types;

// This is the module's whole public surface, re-exported so every consumer says
// `crate::providers::X` regardless of which file X lives in. Several names have no
// caller in the binary today — the concrete providers are reached through
// `ProviderRegistry`, and `ProviderCreatorPost` is the analytics backfill's shape —
// so the unused-import lint is silenced here rather than by trimming the surface
// back and forth as consumers land.
#[allow(unused_imports)]
pub use bluesky::{BlueskyCredentials, BlueskyProvider};
#[allow(unused_imports)]
pub use composio::ComposioProvider;
#[allow(unused_imports)]
pub use credentials::ProviderCredentials;
#[allow(unused_imports)]
pub use fake::{FakeCall, FakeProvider};
#[allow(unused_imports)]
pub use registry::ProviderRegistry;
#[allow(unused_imports)]
pub use treg::TregProvider;
#[allow(unused_imports)]
pub use types::{
    stable_key, PlatformProvider, ProviderAccount, ProviderCreatorPost, ProviderId,
    ProviderInboxItem, ProviderOperation, PublishDeadline, PublishMedia, PublishRequest,
    PublishResult, PublishSegment, RemotePostRef, UnconfiguredProvider,
};
