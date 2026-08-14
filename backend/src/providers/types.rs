//! The provider contract: the types every platform integration speaks, and the
//! trait it implements.
//!
//! These types were originally declared inline in [`super`]; they live here now so
//! the four implementations can `use super::types::*` without a circular read of the
//! module that also declares them. `mod.rs` re-exports everything, so
//! `crate::providers::ProviderAccount` still resolves — no call site moved.
//!
//! **This file is additive-only from here on.** `api.rs` builds a [`ProviderAccount`]
//! with a four-field struct literal and the inbox/analytics modules are written
//! against the trait as it stands; a new required field or a changed signature breaks
//! them at a distance. New capability goes in as a defaulted trait method.

use async_trait::async_trait;

use crate::models::{EngagementCounts, InboxKind, Platform, PlatformCapabilities};

/// Which implementation is behind a provider handle. Used as the capability-cache
/// key, so switching from the fake to a real broker cannot serve stale answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderId {
    Fake,
    Composio,
    Bluesky,
    Threads,
    /// Nothing is configured for this platform. A distinct id rather than reusing
    /// `Fake`: the capability cache is keyed on this, and an all-false matrix from
    /// "unconfigured" must not be served later as the fake's answer (or the reverse).
    Unconfigured,
}

impl ProviderId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::Composio => "composio",
            Self::Bluesky => "bluesky",
            Self::Threads => "threads",
            Self::Unconfigured => "unconfigured",
        }
    }

    /// Parse a pin written by an operator (see `RYU_SOCIAL_ACCOUNT_PROVIDERS`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fake" => Some(Self::Fake),
            "composio" => Some(Self::Composio),
            "bluesky" => Some(Self::Bluesky),
            "threads" => Some(Self::Threads),
            "unconfigured" | "none" => Some(Self::Unconfigured),
            _ => None,
        }
    }
}

impl std::fmt::Display for ProviderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The account a provider call acts on, flattened so a provider never needs to read
/// the database.
#[derive(Debug, Clone)]
pub struct ProviderAccount {
    pub id: String,
    pub platform: Platform,
    /// May be `None` when the account row was deleted but its targets survive —
    /// providers must tolerate this rather than assuming a label exists.
    pub label: Option<String>,
    pub external_id: Option<String>,
}

/// One attachment as handed to a provider. `url` is either an `http(s)` URL or a
/// LOCAL absolute path; a provider that cannot read local files must reject those
/// explicitly rather than passing an unfetchable path to a remote broker.
#[derive(Debug, Clone)]
pub struct PublishMedia {
    pub url: String,
    pub mime_type: String,
    pub alt_text: Option<String>,
}

impl PublishMedia {
    /// Whether `url` is a remote reference a hosted broker could fetch itself.
    pub fn is_remote(&self) -> bool {
        let lower = self.url.to_ascii_lowercase();
        lower.starts_with("http://") || lower.starts_with("https://")
    }
}

#[derive(Debug, Clone)]
pub struct PublishSegment {
    pub text: String,
    pub media: Vec<PublishMedia>,
}

/// Everything needed for one publish call.
#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub account: ProviderAccount,
    /// Mirrors `segments[0].text`, so a provider that does not understand threads
    /// still has something correct to send.
    pub text: String,
    pub media: Vec<PublishMedia>,
    /// `Some` ONLY when there is more than one segment. A single-segment post sends
    /// no segment list at all, which is what makes the degrade for thread-unaware
    /// providers automatic rather than something each one has to implement.
    pub segments: Option<Vec<PublishSegment>>,
    /// Stable across every attempt of one run AND across a re-run, so a provider that
    /// supports caller-chosen record keys can make the publish genuinely idempotent
    /// instead of double-posting on a timeout-then-retry.
    ///
    /// Derived in [`crate::publish`] from `post_id + social_account_id` rather than
    /// from the target row's own id: the derivation survives the target row being
    /// recreated (a re-schedule of the same post to the same account), which a UUID
    /// column does not.
    pub idempotency_key: Option<String>,
}

impl PublishRequest {
    /// A stable, charset-safe key for segment `index` of this request.
    ///
    /// Threading platforms create one remote record per segment, so a single key for
    /// the whole request cannot address them. This derives a per-record key by
    /// hashing `"{idempotency_key}:{index}"` — hashed rather than concatenated
    /// because the raw key contains `:` and our ids are longer than some platforms'
    /// record-key limits.
    ///
    /// Returns `None` when there is no key at all, which is the honest signal that
    /// this publish cannot be made idempotent.
    pub fn segment_key(&self, index: usize) -> Option<String> {
        let key = self.idempotency_key.as_ref()?;
        Some(stable_key(&format!("{key}:{index}")))
    }

    /// The segments to actually publish: the explicit list when there is one, else a
    /// single synthesized segment from `text`/`media`. Every provider needs this
    /// exact fallback, so it lives here instead of in three implementations.
    pub fn effective_segments(&self) -> Vec<PublishSegment> {
        match &self.segments {
            Some(segments) if !segments.is_empty() => segments.clone(),
            _ => vec![PublishSegment {
                text: self.text.clone(),
                media: self.media.clone(),
            }],
        }
    }
}

/// A short, deterministic, alphanumeric key derived from an arbitrary string.
///
/// FNV-1a 64 rendered base36. Not a cryptographic hash and not trying to be: this is
/// a *naming* function whose only requirements are determinism across processes (so a
/// retry in a restarted sidecar derives the same name) and a charset every platform's
/// record-key validator accepts. Collisions are irrelevant — two different keys
/// colliding would mean two different posts to the same account being named the same,
/// and the inputs are `post_id:account_id:index` with a UUID inside.
pub fn stable_key(input: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    let mut n = hash;
    while n > 0 {
        out.push(alphabet[(n % 36) as usize]);
        n /= 36;
    }
    if out.is_empty() {
        out.push(b'0');
    }
    out.reverse();
    format!("ryu{}", String::from_utf8_lossy(&out))
}

/// The outcome of a publish. Deliberately NOT a `Result`: see the module docs.
#[derive(Debug, Clone)]
pub enum PublishResult {
    Ok {
        remote_id: String,
        remote_url: Option<String>,
    },
    Err {
        error: String,
    },
}

impl PublishResult {
    pub fn err(msg: impl Into<String>) -> Self {
        Self::Err { error: msg.into() }
    }

    pub const fn is_ok(&self) -> bool {
        matches!(self, Self::Ok { .. })
    }

    /// The error text, when this is a failure.
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Err { error } => Some(error.as_str()),
            Self::Ok { .. } => None,
        }
    }
}

/// A published post, addressed for a metrics read.
#[derive(Debug, Clone)]
pub struct RemotePostRef {
    pub platform: Platform,
    pub remote_id: String,
    pub remote_url: Option<String>,
}

/// An inbound engagement item as a provider reports it, before it is given a local
/// id and persisted.
#[derive(Debug, Clone)]
pub struct ProviderInboxItem {
    pub external_id: String,
    pub platform: Platform,
    pub kind: InboxKind,
    pub author: String,
    pub text: String,
    pub permalink: Option<String>,
    pub received_at: i64,
}

/// A post by some creator (usually the account owner), with its metrics — the shape
/// the analytics backfill reads.
#[derive(Debug, Clone)]
pub struct ProviderCreatorPost {
    pub external_id: String,
    pub platform: Platform,
    pub text: String,
    pub permalink: Option<String>,
    pub engagement: EngagementCounts,
    pub published_at: Option<i64>,
}

/// What every platform integration must provide.
#[async_trait]
pub trait PlatformProvider: Send + Sync {
    fn id(&self) -> ProviderId;

    /// Begin/verify the account link. Errors on bad credentials.
    async fn connect(&self, account: &ProviderAccount) -> anyhow::Result<Option<String>>;

    /// Drop the link. Should be idempotent — disconnecting twice is not an error.
    async fn disconnect(&self, account: &ProviderAccount) -> anyhow::Result<()>;

    async fn publish(&self, request: &PublishRequest) -> PublishResult;

    async fn read_engagement(&self, post: &RemotePostRef) -> anyhow::Result<EngagementCounts>;

    /// What this provider can do for one platform. Must NOT error — an unreachable
    /// provider returns [`PlatformCapabilities::empty`], so one bad platform cannot
    /// blank the whole matrix.
    async fn capabilities(&self, platform: Platform) -> PlatformCapabilities;

    // ── Optional surface ──
    //
    // Defaulted rather than required, and "not implemented" is defined as "returns
    // nothing" rather than an error: a provider with no inbox should render an
    // EMPTY inbox, not an error banner.

    async fn read_inbox(
        &self,
        _account: &ProviderAccount,
    ) -> anyhow::Result<Vec<ProviderInboxItem>> {
        Ok(Vec::new())
    }

    async fn reply_to_inbox_item(
        &self,
        _item: &ProviderInboxItem,
        _text: &str,
    ) -> PublishResult {
        PublishResult::err("replying is not supported by this provider")
    }

    /// Top posts for a handle on one platform. Defaults to "nothing", which the
    /// analytics layer reads as "no backfill available" rather than as an error.
    async fn read_creator_top_posts(
        &self,
        _platform: Platform,
        _handle: &str,
    ) -> anyhow::Result<Vec<ProviderCreatorPost>> {
        Ok(Vec::new())
    }
}

/// The provider a platform resolves to when nothing is configured for it.
///
/// Present so the router, the publish pipeline and the capability endpoint all
/// return an HONEST answer: all-false capabilities and a publish that fails with a
/// readable reason, rather than a panic or a silent success that would look like a
/// working integration. This is why [`super::FakeProvider`] is NOT the default —
/// see the note on `RYU_SOCIAL_FAKE_PROVIDER` in [`super::registry`].
pub struct UnconfiguredProvider;

#[async_trait]
impl PlatformProvider for UnconfiguredProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Unconfigured
    }

    async fn connect(&self, account: &ProviderAccount) -> anyhow::Result<Option<String>> {
        anyhow::bail!(
            "no provider is configured for {} — connect a Composio key or platform credentials first",
            account.platform
        )
    }

    async fn disconnect(&self, _account: &ProviderAccount) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish(&self, request: &PublishRequest) -> PublishResult {
        PublishResult::err(format!(
            "no provider is configured for {}",
            request.account.platform
        ))
    }

    async fn read_engagement(&self, post: &RemotePostRef) -> anyhow::Result<EngagementCounts> {
        anyhow::bail!("no provider is configured for {}", post.platform)
    }

    async fn capabilities(&self, _platform: Platform) -> PlatformCapabilities {
        PlatformCapabilities::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_key_is_deterministic_and_charset_safe() {
        let a = stable_key("sp_1:acc_1:0");
        assert_eq!(a, stable_key("sp_1:acc_1:0"));
        assert_ne!(a, stable_key("sp_1:acc_1:1"));
        assert!(a.starts_with("ryu"));
        assert!(a.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        // AT-Protocol record keys cap at 512 chars; ours is nowhere near it, and
        // must never be "." or ".." (both are rejected outright).
        assert!(a.len() < 32);
    }

    #[test]
    fn segment_keys_differ_per_segment_but_repeat_for_the_same_one() {
        let request = PublishRequest {
            account: ProviderAccount {
                id: "acc_1".into(),
                platform: Platform::Bluesky,
                label: None,
                external_id: None,
            },
            text: "hi".into(),
            media: vec![],
            segments: None,
            idempotency_key: Some("sp_1:acc_1".into()),
        };
        assert_eq!(request.segment_key(0), request.segment_key(0));
        assert_ne!(request.segment_key(0), request.segment_key(1));

        let unkeyed = PublishRequest {
            idempotency_key: None,
            ..request.clone()
        };
        assert_eq!(unkeyed.segment_key(0), None);

        // With no explicit segment list, the fallback is one segment mirroring the
        // top level — which is what makes a thread-unaware caller correct by default.
        let effective = request.effective_segments();
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].text, "hi");
    }
}
