//! The publish pipeline: turning a due post into N provider calls and one durable
//! outcome per target.
//!
//! ## The ordering, and why each step is where it is
//!
//! 1. `claim_post_for_publishing` — a guarded CAS. If it returns false another
//!    worker owns this post, and [`run_post`] returns `Err` without touching a thing.
//! 2. `list_targets`.
//! 3. For each target, **SEQUENTIALLY** — not concurrently. Backoff sleeps from
//!    several targets running in parallel would stack into a thundering herd against
//!    one platform, which is exactly the behaviour that gets an API key rate-limited.
//!    a. Skip terminal targets (a `published` leg from an earlier run is never
//!       re-attempted — that is what makes `POST /posts/:id/retry` safe).
//!    b. `claim_target` (stamps the lease). False ⇒ skip; someone else has it.
//!    c. **Already-live check** — see the idempotency section below.
//!    d. Resolve content. If there is none, write ONE `failed` history row with a
//!       clear reason, settle the target with `attempts: 0`, and never contact a
//!       provider. The `attempts: 0` is load-bearing observability: it is how a
//!       reader tells "we never tried" from "we tried three times".
//!    e. Validate against the platform's limits. A post that is over the character
//!       limit fails THIS TARGET, not the post — the same content may be perfectly
//!       legal on the other three accounts.
//!    f. Otherwise loop up to `max_attempts`: call the provider, and on failure sleep
//!       [`backoff_delay_ms`] before the next attempt.
//!    g. Write exactly ONE history row for the whole run — after retries are
//!       exhausted, not one per attempt — then settle the target.
//! 4. Settle the post from the **persisted** target statuses.
//!
//! Step 4 reads the targets back rather than aggregating this run's outcomes, and the
//! difference is not cosmetic. A run that skipped every target (all already terminal,
//! or all held by another worker) has an empty outcome list, and
//! `aggregate_status(&[])` is `Failed` — so aggregating over outcomes would stamp
//! `failed` on a post whose legs are live. It also under-counts on a retry, where the
//! already-published legs are not part of this run at all. And when some target is
//! still `pending`/`publishing`, the post is deliberately left in `publishing` for the
//! lease reaper to resolve, rather than settled on incomplete information.
//!
//! `run_post` returns `Err` for exactly two things: a lost claim and a vanished row.
//! A post that fails to publish is a settled post, not a 500.
//!
//! ## Idempotency — the hazard this module exists to contain
//!
//! The key is derived as `"{post_id}:{social_account_id}"` (see
//! [`idempotency_key_for`]) and is reused verbatim by every attempt of a run and by
//! any later re-run. Deriving it rather than using the target row's own UUID means it
//! survives the target row being recreated.
//!
//! A key only helps if the provider forwards it. [`crate::providers::BlueskyProvider`]
//! does, as a caller-chosen record key; Composio is not documented to. So the durable
//! guard is local: before attempting, this module reads `post_history` for the target
//! and treats a `published` row carrying a `remote_id` as proof the post is already
//! live, settling the target without contacting anyone. That check is what makes
//! [`crate::store::SocialStore::reap_expired_claims`] safe to run — a reaper that
//! returns an interrupted target to the queue without it would *cause* double-posts
//! rather than recover from them.
//!
//! **The residual window:** history is written once per run, after the attempt loop.
//! Between "the provider returned Ok" and "the history row committed" there is a
//! process-death window the local check cannot cover. Only a provider-honoured key
//! closes that one, which is why the Bluesky adapter derives a deterministic `rkey`
//! and reads the record back on a collision instead of trusting an error string.

use crate::error::{ApiError, ApiResult};
use crate::models::{
    limits_for, mime_for_extension, now_ms, validate_segments_for_platform, HistoryStatus,
    MediaRef, Platform, PostSegment, PostStatus, PostTarget, ScheduledPost, SegmentStyle,
    SocialSettings, TargetStatus,
};
use crate::providers::{
    PlatformProvider, ProviderAccount, ProviderOperation, PublishMedia, PublishRequest,
    PublishResult, PublishSegment,
};
use crate::state::{AppState, EVENT_POST_FAILED, EVENT_POST_PUBLISHED};

/// The upper bound on `max_attempts`, whatever the settings say. A user who types
/// 500 into the retry box would otherwise pin one target against a failing platform
/// for hours while the rest of the queue waits behind it (targets are sequential).
const MAX_ATTEMPTS_CEILING: u32 = 10;

mod attempt;
mod refusals;
mod runner;

#[cfg(test)]
pub(crate) use attempt::to_publish_media;
use attempt::*;
pub(crate) use refusals::{cannot_cancel, cannot_reschedule};
use refusals::{cannot_publish_now, cannot_retry};
pub use runner::run_post;
use runner::*;

/// What happened to one fan-out leg.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TargetOutcome {
    pub target_id: String,
    pub platform: Platform,
    pub status: TargetStatus,
    pub remote_id: Option<String>,
    pub remote_url: Option<String>,
    pub error: Option<String>,
    /// Provider calls made during **this run**. `0` means the run failed locally
    /// (no body, or content the platform will not accept) and no provider was
    /// contacted.
    ///
    /// Deliberately a DIFFERENT number from the persisted `post_targets.attempts`
    /// column, which counts calls over the target's whole lifetime: a target reaped
    /// mid-flight at `attempts: 2` that then succeeds on its first call of the next
    /// run reports `attempts: 1` here and stores `3` there. Both are correct for what
    /// they answer — "how hard did this run work" versus "how much has this leg cost
    /// in total" — so a surface must not read one as the other.
    pub attempts: u32,
}

/// What happened to a whole post.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PostOutcome {
    pub post_id: String,
    pub status: PostStatus,
    pub targets: Vec<TargetOutcome>,
}

/// Exponential backoff with no jitter and no cap: `base * 2^(attempt - 1)`, so at
/// the default 1000 ms base a run sleeps 1 s after attempt 1 and 2 s after attempt 2.
///
/// Two known weaknesses inherited from the design, recorded so they are a decision
/// rather than an oversight: there is no jitter (several targets that fail at the
/// same instant retry in lockstep), and only coarse error-class discrimination: a
/// provider can mark a failure not retryable ([`PublishResult::final_err`], used
/// when a post may already be live), but an ordinary 400 that will never succeed
/// still costs the full 3 s of sleeping. Both are worth fixing; neither changes
/// the shape of the loop.
pub fn backoff_delay_ms(attempt: u32, base_ms: u64) -> u64 {
    if attempt == 0 {
        return 0;
    }
    base_ms.saturating_mul(1u64 << (attempt - 1).min(16))
}

/// Derive a post's terminal status from its legs.
///
/// A post with ZERO outcomes is `Failed`, not `Published`: "nothing was attempted"
/// must never read as success.
pub fn aggregate_status(outcomes: &[TargetOutcome]) -> PostStatus {
    aggregate_target_statuses(&outcomes.iter().map(|o| o.status).collect::<Vec<_>>())
}

/// The same rule over raw statuses — the form used to settle from PERSISTED targets,
/// which is the authoritative one. See the module docs for why the persisted form is
/// what settles the post.
pub fn aggregate_target_statuses(statuses: &[TargetStatus]) -> PostStatus {
    if statuses.is_empty() {
        return PostStatus::Failed;
    }
    let published = statuses
        .iter()
        .filter(|s| **s == TargetStatus::Published)
        .count();
    if published == statuses.len() {
        PostStatus::Published
    } else if published == 0 {
        PostStatus::Failed
    } else {
        PostStatus::Partial
    }
}

/// The idempotency key for one fan-out leg.
///
/// Derived from `(post, account)` rather than taken from `post_targets.id`: a target
/// row can be deleted and recreated (re-scheduling the same post to the same
/// account), and a UUID column would then hand the provider a key it has never seen
/// for a post that may already be live.
pub fn idempotency_key_for(post_id: &str, social_account_id: &str) -> String {
    format!("{post_id}:{social_account_id}")
}

/// "Publish now": move an already-scheduled post to the front of the queue.
///
/// This is a real, durable transition — the post becomes `due` immediately and the
/// next runner pass owns it. It deliberately does NOT publish inline: doing so would
/// hold an HTTP request open for the full retry-and-backoff window, and a client
/// timeout would then leave a publish running with nobody listening.
///
/// Guarded to `scheduled`, so it cannot race a sweep that already claimed the row.
///
/// `agent_grant` is the grant of the agent asking (`None` for a person); it
/// replaces the post's recorded agent in the same transition.
pub async fn queue_now(
    state: &AppState,
    post_id: &str,
    agent_grant: Option<&str>,
) -> ApiResult<ScheduledPost> {
    let Some(post) = state.store.get_scheduled_post(post_id).await? else {
        return Err(ApiError::not_found("post"));
    };
    if !state
        .store
        .mark_post_due_now(post_id, now_ms(), agent_grant)
        .await?
    {
        return Err(cannot_publish_now(post.status));
    }
    state
        .store
        .get_scheduled_post(post_id)
        .await?
        .ok_or_else(|| ApiError::not_found("post"))
}

/// Re-queue a settled-but-incomplete post. Failed targets go back to `pending` with
/// their attempt counter reset; **published targets are untouched**, so a retry can
/// never double-post the legs that already worked.
///
/// `agent_grant` replaces the post's recorded agent, as in [`queue_now`].
pub async fn queue_retry(
    state: &AppState,
    post_id: &str,
    agent_grant: Option<&str>,
) -> ApiResult<ScheduledPost> {
    let Some(post) = state.store.get_scheduled_post(post_id).await? else {
        return Err(ApiError::not_found("post"));
    };
    if !state
        .store
        .retry_post(post_id, now_ms(), agent_grant)
        .await?
    {
        return Err(cannot_retry(post.status));
    }
    state
        .store
        .get_scheduled_post(post_id)
        .await?
        .ok_or_else(|| ApiError::not_found("post"))
}

#[cfg(test)]
mod tests;
