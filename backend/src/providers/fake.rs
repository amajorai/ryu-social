//! The deterministic in-memory provider.
//!
//! This is the test seam for the whole publish spine: retry, backoff, partial
//! failure and idempotency are all pipeline behaviour, and without a provider that
//! can be scripted to fail they could only be exercised against a live network — i.e.
//! never, in CI.
//!
//! Three properties are load-bearing and must survive any edit here:
//!
//! 1. **No network and no clock beyond `now_ms`.** A test that sleeps or dials out is
//!    a test that gets deleted the first time it flakes.
//! 2. **It honours `idempotency_key`.** A repeat publish under a key it has already
//!    seen returns the ORIGINAL remote id and records no second post. That is what
//!    makes "a retry must not double-post" assertable.
//! 3. **It records what it was handed.** [`FakeProvider::calls`] returns every
//!    publish call with its key and segment count, so a test can assert the pipeline
//!    forwarded the whole thread rather than just checking that it did not error.
//!
//! It is NOT the default provider — see [`super::registry`] for why an unconfigured
//! install gets [`super::UnconfiguredProvider`] instead.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;

use super::types::{
    PlatformProvider, ProviderAccount, ProviderCreatorPost, ProviderId, ProviderInboxItem,
    PublishRequest, PublishResult, RemotePostRef,
};
use crate::models::{now_ms, EngagementCounts, Platform, PlatformCapabilities};

/// One recorded publish call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeCall {
    pub account_id: String,
    pub platform: Platform,
    pub idempotency_key: Option<String>,
    pub text: String,
    pub segment_texts: Vec<String>,
    pub media_count: usize,
    /// Whether this call was answered from the idempotency map rather than by
    /// creating a new post.
    pub deduped: bool,
}

#[derive(Debug, Clone)]
struct FakePost {
    remote_id: String,
    platform: Platform,
}

#[derive(Default)]
struct FakeState {
    /// idempotency key → the post created under it.
    by_key: BTreeMap<String, FakePost>,
    posts: Vec<FakePost>,
    calls: Vec<FakeCall>,
    /// Remaining scripted failures, decremented on each publish attempt.
    failures_left: u32,
}

/// A scriptable, deterministic provider.
pub struct FakeProvider {
    state: Mutex<FakeState>,
    /// Platforms whose publish always fails, for the partial-failure test.
    fail_platforms: HashSet<Platform>,
    /// Platforms this fake refuses to publish for at the CAPABILITY level.
    unpublishable: HashSet<Platform>,
}

impl Default for FakeProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeProvider {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FakeState::default()),
            fail_platforms: HashSet::new(),
            unpublishable: HashSet::new(),
        }
    }

    /// Every publish to `platform` fails with a transport-shaped error.
    pub fn failing_on(mut self, platform: Platform) -> Self {
        self.fail_platforms.insert(platform);
        self
    }

    /// `platform` reports `publish: false`, so a publish is rejected before any post
    /// is recorded — the "capability says no" path.
    pub fn without_publish_for(mut self, platform: Platform) -> Self {
        self.unpublishable.insert(platform);
        self
    }

    /// The first `n` publish attempts fail; everything after succeeds. Drives the
    /// retry loop without a sleep long enough to notice.
    pub fn failing_first(self, n: u32) -> Self {
        self.state.lock().expect("fake state").failures_left = n;
        self
    }

    /// Every publish call this provider has seen, in order.
    pub fn calls(&self) -> Vec<FakeCall> {
        self.state.lock().expect("fake state").calls.clone()
    }

    /// How many posts actually exist remotely. The assertion that proves a retry did
    /// not double-post.
    pub fn published_count(&self) -> usize {
        self.state.lock().expect("fake state").posts.len()
    }

    fn capabilities_for(&self, platform: Platform) -> PlatformCapabilities {
        PlatformCapabilities {
            publish: !self.unpublishable.contains(&platform),
            read_comments: true,
            // Only the two platforms with a real DM surface, matching what the
            // upstream fake modelled — a fake that claimed every capability would
            // make the capability gate untestable.
            read_dms: matches!(platform, Platform::X | Platform::Instagram),
            send_dm: matches!(platform, Platform::X | Platform::Instagram),
            read_engagement: true,
            // ALWAYS false, for every provider: scheduling is ours and is never
            // delegated. See `PlatformCapabilities`.
            schedule: false,
        }
    }
}

/// Deterministic pseudo-metrics: a polynomial rolling hash of `"{platform}:{id}"`.
/// Same input, same numbers, forever — so a snapshot test of an analytics projection
/// is stable.
fn engagement_hash(platform: Platform, remote_id: &str) -> u64 {
    let input = format!("{platform}:{remote_id}");
    let mut hash: u64 = 0;
    for byte in input.as_bytes() {
        hash = (hash.wrapping_mul(31).wrapping_add(u64::from(*byte))) % 2_147_483_647;
    }
    hash
}

#[async_trait]
impl PlatformProvider for FakeProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Fake
    }

    async fn connect(&self, account: &ProviderAccount) -> anyhow::Result<Option<String>> {
        Ok(Some(format!("fake_account_{}", account.id)))
    }

    async fn disconnect(&self, _account: &ProviderAccount) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish(&self, request: &PublishRequest) -> PublishResult {
        let platform = request.account.platform;
        let segments = request.effective_segments();
        let segment_texts: Vec<String> = segments.iter().map(|s| s.text.clone()).collect();
        let media_count: usize = segments.iter().map(|s| s.media.len()).sum();

        // The capability gate comes first: a platform this provider cannot publish to
        // must not record a call as if it tried.
        if !self.capabilities_for(platform).publish {
            return PublishResult::err(format!("Publishing is not supported for {platform}"));
        }

        let mut state = self.state.lock().expect("fake state");

        // Idempotency BEFORE the scripted failure, deliberately: a key that already
        // produced a post is already live remotely, and a real provider honouring the
        // key would return the existing id rather than re-running its failure path.
        if let Some(key) = &request.idempotency_key {
            if let Some(existing) = state.by_key.get(key).cloned() {
                state.calls.push(FakeCall {
                    account_id: request.account.id.clone(),
                    platform,
                    idempotency_key: Some(key.clone()),
                    text: request.text.clone(),
                    segment_texts,
                    media_count,
                    deduped: true,
                });
                return PublishResult::Ok {
                    remote_url: Some(format!(
                        "https://fake.local/{platform}/{}",
                        existing.remote_id
                    )),
                    remote_id: existing.remote_id,
                };
            }
        }

        let scripted_failure = if state.failures_left > 0 {
            state.failures_left -= 1;
            true
        } else {
            false
        };

        state.calls.push(FakeCall {
            account_id: request.account.id.clone(),
            platform,
            idempotency_key: request.idempotency_key.clone(),
            text: request.text.clone(),
            segment_texts,
            media_count,
            deduped: false,
        });

        if scripted_failure || self.fail_platforms.contains(&platform) {
            return PublishResult::err(format!("fake: publish to {platform} failed"));
        }

        let remote_id = match &request.idempotency_key {
            Some(key) => format!("fake_{key}"),
            None => format!("fake_{platform}_{}", state.posts.len() + 1),
        };
        let post = FakePost {
            remote_id: remote_id.clone(),
            platform,
        };
        state.posts.push(post.clone());
        if let Some(key) = &request.idempotency_key {
            state.by_key.insert(key.clone(), post);
        }
        PublishResult::Ok {
            remote_url: Some(format!("https://fake.local/{platform}/{remote_id}")),
            remote_id,
        }
    }

    async fn read_engagement(&self, post: &RemotePostRef) -> anyhow::Result<EngagementCounts> {
        let hash = engagement_hash(post.platform, &post.remote_id);
        Ok(EngagementCounts {
            likes: Some(hash % 1_000),
            comments: Some(hash % 137),
            shares: Some(hash % 53),
            views: Some((hash % 1_000) * 10),
            fetched_at: now_ms(),
        })
    }

    async fn capabilities(&self, platform: Platform) -> PlatformCapabilities {
        self.capabilities_for(platform)
    }

    async fn read_inbox(
        &self,
        _account: &ProviderAccount,
    ) -> anyhow::Result<Vec<ProviderInboxItem>> {
        Ok(Vec::new())
    }

    async fn read_creator_top_posts(
        &self,
        _platform: Platform,
        _handle: &str,
    ) -> anyhow::Result<Vec<ProviderCreatorPost>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(key: Option<&str>) -> PublishRequest {
        PublishRequest {
            account: ProviderAccount {
                id: "acc_1".into(),
                platform: Platform::X,
                label: Some("@me".into()),
                external_id: None,
            },
            text: "hello".into(),
            media: vec![],
            segments: None,
            idempotency_key: key.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn a_repeat_under_the_same_key_returns_the_first_post_and_creates_no_second() {
        let fake = FakeProvider::new();
        let first = fake.publish(&request(Some("sp_1:acc_1"))).await;
        let second = fake.publish(&request(Some("sp_1:acc_1"))).await;
        let (PublishResult::Ok { remote_id: a, .. }, PublishResult::Ok { remote_id: b, .. }) =
            (&first, &second)
        else {
            panic!("both publishes should succeed: {first:?} {second:?}");
        };
        assert_eq!(a, b);
        assert_eq!(fake.published_count(), 1);
        assert!(fake.calls()[1].deduped);
    }

    #[tokio::test]
    async fn scripted_failures_are_consumed_one_per_attempt() {
        let fake = FakeProvider::new().failing_first(2);
        assert!(!fake.publish(&request(None)).await.is_ok());
        assert!(!fake.publish(&request(None)).await.is_ok());
        assert!(fake.publish(&request(None)).await.is_ok());
        assert_eq!(fake.published_count(), 1);
    }

    #[tokio::test]
    async fn a_platform_without_the_publish_capability_is_rejected_before_any_post() {
        let fake = FakeProvider::new().without_publish_for(Platform::X);
        let result = fake.publish(&request(None)).await;
        assert_eq!(result.error(), Some("Publishing is not supported for x"));
        assert_eq!(fake.published_count(), 0);
        assert!(fake.calls().is_empty());
    }

    #[tokio::test]
    async fn engagement_is_deterministic() {
        let fake = FakeProvider::new();
        let post = RemotePostRef {
            platform: Platform::X,
            remote_id: "fake_x_1".into(),
            remote_url: None,
        };
        let a = fake.read_engagement(&post).await.unwrap();
        let b = fake.read_engagement(&post).await.unwrap();
        assert_eq!(a.likes, b.likes);
        assert_eq!(a.comments, b.comments);
    }

    #[tokio::test]
    async fn schedule_is_never_a_provider_capability() {
        let fake = FakeProvider::new();
        for platform in Platform::ALL {
            assert!(!fake.capabilities(platform).await.schedule, "{platform}");
        }
    }
}
