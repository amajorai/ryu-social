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
    PlatformProvider, ProviderAccount, PublishMedia, PublishRequest, PublishResult, PublishSegment,
};
use crate::state::{
    AppState, EVENT_POST_FAILED, EVENT_POST_PUBLISHED, PROVIDER_CALL_TIMEOUT_MS,
};

/// The upper bound on `max_attempts`, whatever the settings say. A user who types
/// 500 into the retry box would otherwise pin one target against a failing platform
/// for hours while the rest of the queue waits behind it (targets are sequential).
const MAX_ATTEMPTS_CEILING: u32 = 10;

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
/// same instant retry in lockstep), and no error-class discrimination (a 400 that
/// will never succeed still costs the full 3 s of sleeping). Both are worth fixing;
/// neither changes the shape of the loop.
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
pub async fn queue_now(state: &AppState, post_id: &str) -> ApiResult<ScheduledPost> {
    let Some(post) = state.store.get_scheduled_post(post_id).await? else {
        return Err(ApiError::not_found("post"));
    };
    if !state.store.mark_post_due_now(post_id, now_ms()).await? {
        return Err(ApiError::conflict(format!(
            "post is {} and can no longer be published now",
            post.status.as_str()
        )));
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
pub async fn queue_retry(state: &AppState, post_id: &str) -> ApiResult<ScheduledPost> {
    let Some(post) = state.store.get_scheduled_post(post_id).await? else {
        return Err(ApiError::not_found("post"));
    };
    if !state.store.retry_post(post_id, now_ms()).await? {
        return Err(ApiError::conflict(format!(
            "only a partial or failed post can be retried; this one is {}",
            post.status.as_str()
        )));
    }
    state
        .store
        .get_scheduled_post(post_id)
        .await?
        .ok_or_else(|| ApiError::not_found("post"))
}

// ── The runner ─────────────────────────────────────────────────────────────────

/// Content resolved for one target, ready to become a provider call.
struct ResolvedContent {
    account: ProviderAccount,
    segments: Vec<PostSegment>,
}

/// Run one claimed post through its targets. See the module docs for the ordering.
///
/// Returns `Err` ONLY for a lost claim (another worker owns the post) or a row that
/// vanished mid-flight. Every publish failure is an `Ok` whose `status` says what
/// happened.
pub async fn run_post(state: &AppState, post: &ScheduledPost) -> ApiResult<PostOutcome> {
    // The claim IS the concurrency control. In-process flags would be unsound: this
    // sidecar has concurrent handlers plus the tick task against one database.
    if !state.store.claim_post_for_publishing(&post.id).await? {
        return Err(ApiError::conflict(format!(
            "post {} is not due (another worker may already own it)",
            post.id
        )));
    }

    let settings = state.store.get_settings(&post.workspace_id).await?;
    let max_attempts = settings.max_attempts.clamp(1, MAX_ATTEMPTS_CEILING);
    let targets = state.store.list_targets(&post.id).await?;

    // The draft is loaded ONCE for the whole fan-out rather than per target: every
    // leg without a variant override resolves from the same body, and re-reading it
    // per target would let an edit land mid-run and publish two different posts.
    let draft_body = match &post.draft_id {
        Some(draft_id) => state
            .store
            .get_draft(draft_id)
            .await?
            .map(|draft| draft.body),
        None => None,
    };

    let mut outcomes = Vec::with_capacity(targets.len());
    for target in &targets {
        if target.status.is_terminal() {
            // A leg that already published (or was cancelled) is never re-attempted.
            continue;
        }
        if !state.store.claim_target(&target.id, now_ms()).await? {
            tracing::debug!(target = %target.id, "ryu-social: target already claimed; skipping");
            continue;
        }
        let outcome = run_target(
            state,
            post,
            target,
            draft_body.as_ref(),
            max_attempts,
            settings.base_backoff_ms,
        )
        .await?;
        outcomes.push(outcome);
    }

    let status = settle_post(state, post, &outcomes).await?;
    Ok(PostOutcome {
        post_id: post.id.clone(),
        status,
        targets: outcomes,
    })
}

/// Settle the post from its PERSISTED targets, emit the app event, and return the
/// status the post is now in.
async fn settle_post(
    state: &AppState,
    post: &ScheduledPost,
    outcomes: &[TargetOutcome],
) -> ApiResult<PostStatus> {
    let persisted = state.store.list_targets(&post.id).await?;
    if !persisted.is_empty() && !persisted.iter().all(|t| t.status.is_terminal()) {
        // Someone else still owes work on this post. Leaving it in `publishing` is
        // correct: the lease reaper is what resolves an owner that died, and settling
        // here would publish a verdict over a leg that is still in flight.
        tracing::debug!(post = %post.id, "ryu-social: post left publishing; some targets are still in flight");
        return Ok(PostStatus::Publishing);
    }

    let status = aggregate_target_statuses(
        &persisted.iter().map(|t| t.status).collect::<Vec<_>>(),
    );
    // Guarded on `publishing`, so a settle arriving after a reaper already recycled
    // the row cannot resurrect a stale verdict.
    if !state.store.settle_post(&post.id, status).await? {
        tracing::warn!(post = %post.id, "ryu-social: post was no longer publishing at settle time");
    }

    let payload = serde_json::json!({
        "post_id": post.id,
        "workspace_id": post.workspace_id,
        "status": status,
        "targets": outcomes,
    });
    // Best effort and a no-op when this process is not Core-hosted, so no test needs
    // a live Core and a down Core never blocks a publish.
    match status {
        PostStatus::Published => state.events.emit(EVENT_POST_PUBLISHED, payload).await,
        PostStatus::Partial | PostStatus::Failed => {
            state.events.emit(EVENT_POST_FAILED, payload).await;
        }
        _ => {}
    }
    Ok(status)
}

/// Run one target: resolve, validate, attempt, record.
async fn run_target(
    state: &AppState,
    post: &ScheduledPost,
    target: &PostTarget,
    draft_body: Option<&crate::models::DraftBody>,
    max_attempts: u32,
    base_backoff_ms: u64,
) -> ApiResult<TargetOutcome> {
    // ── Already live? ──
    //
    // A `published` history row carrying a remote id is durable proof this leg
    // reached the platform. Re-attempting it after a reaper returned the target to
    // the queue is exactly how a double-post happens.
    let history = state.store.list_history_for_target(&target.id).await?;
    if let Some(existing) = history
        .iter()
        .find(|h| h.status == HistoryStatus::Published && h.remote_id.is_some())
    {
        tracing::info!(
            target = %target.id,
            "ryu-social: target already has a published record; settling without contacting the provider"
        );
        state
            .store
            .settle_target(&target.id, TargetStatus::Published, target.attempts, None)
            .await?;
        return Ok(TargetOutcome {
            target_id: target.id.clone(),
            platform: target.platform,
            status: TargetStatus::Published,
            remote_id: existing.remote_id.clone(),
            remote_url: existing.remote_url.clone(),
            error: None,
            attempts: 0,
        });
    }

    // ── Content ──
    let Some(content) = resolve_content(state, target, draft_body).await? else {
        return fail_locally(
            state,
            target,
            "No post body to publish (no variant override and no draft)",
        )
        .await;
    };

    // ── Limits ──
    //
    // Validated per target, not per post: 280 characters is a rejection on X and
    // perfectly legal on LinkedIn, so a shared body must be able to fail one leg
    // while the others publish.
    //
    // This check is UNCONDITIONAL, and deliberately does not consult
    // `SocialSettings.enforce_platform_limits`. That setting governs the COMPOSER —
    // whether `POST /posts/validate` blocks a schedule or merely warns — because the
    // limit figures are public estimates rather than a contract and can be wrong.
    // Warn-only at compose does not mean warn-only at publish: by the time we are
    // here the platform is going to reject the post anyway, and failing locally costs
    // no API call, no rate-limit budget, and no half-published thread. Do not "fix"
    // this by wiring the setting in.
    if let Some(reason) =
        validate_segments_for_platform(target.platform.as_str(), &content.segments)
    {
        return fail_locally(
            state,
            target,
            format!("{}: {reason}", target.platform.label()),
        )
        .await;
    }

    let request = build_request(post, target, &content);
    let provider = state.providers.provider_for_account(&content.account);
    attempt_publish(state, target, provider, &request, max_attempts, base_backoff_ms).await
}

/// One provider call, under a hard deadline.
///
/// The deadline is enforced HERE rather than left to each provider's HTTP client,
/// because this is the single place every publish passes through and the only place
/// the guarantee is worth anything. `state::build_http_client` sets the same bound on
/// the shared `reqwest::Client`, but a provider that grows its own client, or an
/// adapter that awaits something other than HTTP, would quietly opt out — and the
/// consequence is not a slow publish. `run_batch` awaits `join_next()`, so ONE
/// unbounded call stops the tick loop for every workspace, forever, while `/health`
/// keeps reporting 200 because it only reads the store.
///
/// `PER_ATTEMPT_ALLOWANCE_MS` — the number the whole lease calculation rests on —
/// is defined from the same constant, so the reaper's arithmetic and the call's
/// actual ceiling can no longer disagree.
///
/// An elapsed deadline maps to a normal `PublishResult::Err`, so it retries and
/// settles exactly like any other provider failure rather than needing a third arm.
async fn publish_once(
    provider: &dyn PlatformProvider,
    request: &PublishRequest,
) -> PublishResult {
    let bound = std::time::Duration::from_millis(PROVIDER_CALL_TIMEOUT_MS);
    match tokio::time::timeout(bound, provider.publish(request)).await {
        Ok(result) => result,
        Err(_) => PublishResult::err(format!(
            "the {} provider did not answer within {}s",
            request.account.platform,
            PROVIDER_CALL_TIMEOUT_MS / 1000
        )),
    }
}

/// The attempt loop. One history row for the whole run, written after the loop.
async fn attempt_publish(
    state: &AppState,
    target: &PostTarget,
    provider: std::sync::Arc<dyn PlatformProvider>,
    request: &PublishRequest,
    max_attempts: u32,
    base_backoff_ms: u64,
) -> ApiResult<TargetOutcome> {
    // Only surfaces if `max_attempts` were somehow zero, which the clamp prevents —
    // kept so the failure path can never report an empty reason.
    let mut last_error = "Unknown publish error".to_string();

    for attempt in 1..=max_attempts {
        match publish_once(provider.as_ref(), request).await {
            PublishResult::Ok {
                remote_id,
                remote_url,
            } => {
                state
                    .store
                    .insert_history(
                        &target.id,
                        HistoryStatus::Published,
                        Some(&remote_id),
                        remote_url.as_deref(),
                        None,
                    )
                    .await?;
                state
                    .store
                    .settle_target(
                        &target.id,
                        TargetStatus::Published,
                        target.attempts + attempt,
                        None,
                    )
                    .await?;
                return Ok(TargetOutcome {
                    target_id: target.id.clone(),
                    platform: target.platform,
                    status: TargetStatus::Published,
                    remote_id: Some(remote_id),
                    remote_url,
                    error: None,
                    attempts: attempt,
                });
            }
            PublishResult::Err { error } => {
                last_error = error;
                tracing::warn!(
                    target = %target.id,
                    attempt,
                    max_attempts,
                    error = %last_error,
                    "ryu-social: publish attempt failed"
                );
                if attempt < max_attempts {
                    let delay = backoff_delay_ms(attempt, base_backoff_ms);
                    if delay > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    }
                }
            }
        }
    }

    state
        .store
        .insert_history(
            &target.id,
            HistoryStatus::Failed,
            None,
            None,
            Some(&last_error),
        )
        .await?;
    state
        .store
        .settle_target(
            &target.id,
            TargetStatus::Failed,
            target.attempts + max_attempts,
            None,
        )
        .await?;
    Ok(TargetOutcome {
        target_id: target.id.clone(),
        platform: target.platform,
        status: TargetStatus::Failed,
        remote_id: None,
        remote_url: None,
        error: Some(last_error),
        attempts: max_attempts,
    })
}

/// Fail a target without ever contacting a provider: no body, or content the platform
/// will not accept. `attempts: 0` is the signal that distinguishes this from a run
/// that tried and was rejected remotely.
async fn fail_locally(
    state: &AppState,
    target: &PostTarget,
    reason: impl Into<String>,
) -> ApiResult<TargetOutcome> {
    let reason = reason.into();
    state
        .store
        .insert_history(&target.id, HistoryStatus::Failed, None, None, Some(&reason))
        .await?;
    state
        .store
        .settle_target(&target.id, TargetStatus::Failed, target.attempts, None)
        .await?;
    Ok(TargetOutcome {
        target_id: target.id.clone(),
        platform: target.platform,
        status: TargetStatus::Failed,
        remote_id: None,
        remote_url: None,
        error: Some(reason),
        attempts: 0,
    })
}

/// Resolve what this target should publish.
///
/// The per-target `variant_body` is a FULL draft body, so an override replaces the
/// draft losslessly — it keeps that target's media and its thread structure, unlike a
/// plain-text override which silently drops both.
///
/// A missing account row is tolerated (it can be hard-deleted while its targets
/// survive): the platform comes from the denormalized column on the target, and the
/// label is simply absent.
async fn resolve_content(
    state: &AppState,
    target: &PostTarget,
    draft_body: Option<&crate::models::DraftBody>,
) -> ApiResult<Option<ResolvedContent>> {
    let account_row = state.store.get_account(&target.social_account_id).await?;
    let account = ProviderAccount {
        id: target.social_account_id.clone(),
        platform: target.platform,
        label: account_row.as_ref().map(|a| a.account_label.clone()),
        external_id: account_row.as_ref().and_then(|a| a.external_id.clone()),
    };

    let body = match (&target.variant_body, draft_body) {
        (Some(variant), _) => variant.clone(),
        (None, Some(draft)) => draft.clone(),
        (None, None) => return Ok(None),
    };
    if body.is_empty() {
        return Ok(None);
    }

    let mut segments = body.segments;
    // On a single-post platform the extra segments are a documented DEGRADE, not an
    // error — compose validates only the first one for exactly this reason, so the
    // publish must drop the rest rather than send a thread the platform cannot make.
    if limits_for(target.platform).segment_style == SegmentStyle::None {
        segments.truncate(1);
    }
    if segments.is_empty() {
        return Ok(None);
    }
    Ok(Some(ResolvedContent { account, segments }))
}

/// Build the provider call, including the mirror invariant `segments[0]` → `text`.
fn build_request(
    post: &ScheduledPost,
    target: &PostTarget,
    content: &ResolvedContent,
) -> PublishRequest {
    let segments: Vec<PublishSegment> = content
        .segments
        .iter()
        .map(|segment| PublishSegment {
            text: segment.text.clone(),
            media: segment.media.iter().map(to_publish_media).collect(),
        })
        .collect();
    let first = segments.first();
    PublishRequest {
        account: content.account.clone(),
        text: first.map(|s| s.text.clone()).unwrap_or_default(),
        media: first.map(|s| s.media.clone()).unwrap_or_default(),
        // `Some` only past one segment, so a thread-unaware provider sees plain
        // `text`/`media` and degrades correctly without implementing anything.
        segments: (segments.len() > 1).then(|| segments.clone()),
        idempotency_key: Some(idempotency_key_for(&post.id, &target.social_account_id)),
    }
}

fn to_publish_media(media: &MediaRef) -> PublishMedia {
    // A file picked by path may carry no mime type; derive it from the extension so
    // the provider (and the limit check) see the same answer.
    let mime = if media.mime_type.is_empty() {
        let from_name = mime_for_extension(&media.name);
        if from_name.is_empty() {
            mime_for_extension(&media.path)
        } else {
            from_name
        }
        .to_string()
    } else {
        media.mime_type.clone()
    };
    PublishMedia {
        url: media.path.clone(),
        mime_type: mime,
        // Deliberately NOT the file name. "IMG_4821.png" announced by a screen reader
        // is worse than silence, and passing it as alt text would make every post look
        // like it had been described when none of them had.
        alt_text: None,
    }
}

/// The settings a run uses, for callers that want to reason about the retry budget
/// without re-deriving the clamp.
pub fn effective_max_attempts(settings: &SocialSettings) -> u32 {
    settings.max_attempts.clamp(1, MAX_ATTEMPTS_CEILING)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DraftBody, MediaRef, SocialSettings, DEFAULT_WORKSPACE_ID};
    use crate::providers::{FakeProvider, ProviderRegistry};
    use crate::state::Config;
    use crate::store::{NewTarget, SocialStore};
    use std::sync::Arc;

    #[test]
    fn backoff_doubles_per_attempt_and_is_zero_before_the_first() {
        assert_eq!(backoff_delay_ms(0, 1_000), 0);
        assert_eq!(backoff_delay_ms(1, 1_000), 1_000);
        assert_eq!(backoff_delay_ms(2, 1_000), 2_000);
        assert_eq!(backoff_delay_ms(3, 1_000), 4_000);
        // A pathological attempt count saturates rather than overflowing the shift.
        assert!(backoff_delay_ms(u32::MAX, 1_000) > 0);
    }

    fn outcome(status: TargetStatus) -> TargetOutcome {
        TargetOutcome {
            target_id: "tgt_1".into(),
            platform: Platform::X,
            status,
            remote_id: None,
            remote_url: None,
            error: None,
            attempts: 1,
        }
    }

    #[test]
    fn aggregate_status_treats_no_targets_as_failure() {
        assert_eq!(aggregate_status(&[]), PostStatus::Failed);
        assert_eq!(
            aggregate_status(&[outcome(TargetStatus::Published)]),
            PostStatus::Published
        );
        assert_eq!(
            aggregate_status(&[outcome(TargetStatus::Failed)]),
            PostStatus::Failed
        );
        assert_eq!(
            aggregate_status(&[
                outcome(TargetStatus::Published),
                outcome(TargetStatus::Failed)
            ]),
            PostStatus::Partial
        );
        // A cancelled leg is not a published one, so a mixed post is partial.
        assert_eq!(
            aggregate_target_statuses(&[TargetStatus::Published, TargetStatus::Cancelled]),
            PostStatus::Partial
        );
    }

    #[test]
    fn the_idempotency_key_is_derived_and_stable() {
        let a = idempotency_key_for("sp_1", "acc_1");
        assert_eq!(a, idempotency_key_for("sp_1", "acc_1"));
        assert_ne!(a, idempotency_key_for("sp_1", "acc_2"));
        assert_ne!(a, idempotency_key_for("sp_2", "acc_1"));
    }

    #[test]
    fn the_attempt_budget_is_clamped_in_both_directions() {
        let mut settings = SocialSettings::default();
        settings.max_attempts = 0;
        assert_eq!(effective_max_attempts(&settings), 1);
        settings.max_attempts = 5_000;
        assert_eq!(effective_max_attempts(&settings), MAX_ATTEMPTS_CEILING);
    }

    // ── Pipeline tests, all against the deterministic fake ──

    /// A state whose every platform resolves to `fake`, with zero backoff so the
    /// retry loop runs at full speed.
    async fn state_with(fake: Arc<FakeProvider>) -> AppState {
        let store = SocialStore::open_in_memory().expect("in-memory store");
        store
            .put_settings(
                DEFAULT_WORKSPACE_ID,
                &SocialSettings {
                    max_attempts: 3,
                    base_backoff_ms: 0,
                    ..SocialSettings::default()
                },
            )
            .await
            .unwrap();
        let mut state = AppState::new(
            store,
            Config {
                port: 0,
                sweep_batch_size: 10,
                scheduler_enabled: false,
            },
        );
        state.providers = ProviderRegistry::with_provider(fake);
        state
    }

    /// Schedule a post with one target per platform and move it to `due`.
    async fn due_post(
        state: &AppState,
        body: &DraftBody,
        platforms: &[Platform],
    ) -> ScheduledPost {
        let mut targets = Vec::new();
        for (i, platform) in platforms.iter().enumerate() {
            let account = state
                .store
                .create_account(
                    DEFAULT_WORKSPACE_ID,
                    *platform,
                    &format!("@acct{i}"),
                    Some("ext_1"),
                )
                .await
                .unwrap();
            targets.push(NewTarget {
                social_account_id: account.id,
                platform: *platform,
                variant_body: None,
            });
        }
        let draft = state
            .store
            .create_draft(DEFAULT_WORKSPACE_ID, body)
            .await
            .unwrap();
        let post = state
            .store
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                Some(&draft.id),
                now_ms(),
                &targets,
            )
            .await
            .unwrap();
        assert!(state
            .store
            .mark_post_due_now(&post.id, now_ms())
            .await
            .unwrap());
        state
            .store
            .get_scheduled_post(&post.id)
            .await
            .unwrap()
            .unwrap()
    }

    fn body(text: &str) -> DraftBody {
        let mut body = DraftBody::empty();
        body.segments[0].text = text.to_string();
        body.normalize();
        body
    }

    #[tokio::test]
    async fn a_post_over_the_platform_limit_fails_that_target_without_calling_the_provider() {
        let fake = Arc::new(FakeProvider::new());
        let state = state_with(fake.clone()).await;
        // 300 characters: legal on LinkedIn, over X's 280-character limit.
        let post = due_post(&state, &body(&"a".repeat(300)), &[Platform::X, Platform::Linkedin]).await;

        let outcome = run_post(&state, &post).await.unwrap();

        assert_eq!(outcome.status, PostStatus::Partial);
        let x = outcome
            .targets
            .iter()
            .find(|t| t.platform == Platform::X)
            .unwrap();
        assert_eq!(x.status, TargetStatus::Failed);
        // Never contacted a provider — that is what `attempts: 0` means.
        assert_eq!(x.attempts, 0);
        assert!(x.error.as_ref().unwrap().contains("character limit"));
        let linkedin = outcome
            .targets
            .iter()
            .find(|t| t.platform == Platform::Linkedin)
            .unwrap();
        assert_eq!(linkedin.status, TargetStatus::Published);
        // Exactly one provider call: the legal leg. The rejected one never dialled.
        assert_eq!(fake.calls().len(), 1);
        assert_eq!(fake.calls()[0].platform, Platform::Linkedin);
        // Both legs recorded a durable history row.
        let history = state
            .store
            .list_history(DEFAULT_WORKSPACE_ID, 50)
            .await
            .unwrap();
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn one_failing_leg_makes_the_post_partial_and_leaves_the_others_alone() {
        let fake = Arc::new(FakeProvider::new().failing_on(Platform::Reddit));
        let state = state_with(fake.clone()).await;
        let post = due_post(
            &state,
            &body("hello"),
            &[Platform::X, Platform::Reddit, Platform::Linkedin],
        )
        .await;

        let outcome = run_post(&state, &post).await.unwrap();

        assert_eq!(outcome.status, PostStatus::Partial);
        let reddit = outcome
            .targets
            .iter()
            .find(|t| t.platform == Platform::Reddit)
            .unwrap();
        assert_eq!(reddit.status, TargetStatus::Failed);
        // The full retry budget was spent on the failing leg.
        assert_eq!(reddit.attempts, 3);
        assert_eq!(
            outcome
                .targets
                .iter()
                .filter(|t| t.status == TargetStatus::Published)
                .count(),
            2
        );
        // Two real posts exist; the failing leg created none.
        assert_eq!(fake.published_count(), 2);

        let settled = state
            .store
            .get_scheduled_post(&post.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.status, PostStatus::Partial);
    }

    #[tokio::test]
    async fn every_attempt_of_a_run_reuses_the_same_idempotency_key() {
        // Two failures then a success: three provider calls for one leg.
        let fake = Arc::new(FakeProvider::new().failing_first(2));
        let state = state_with(fake.clone()).await;
        let post = due_post(&state, &body("hello"), &[Platform::X]).await;

        let outcome = run_post(&state, &post).await.unwrap();

        assert_eq!(outcome.status, PostStatus::Published);
        assert_eq!(outcome.targets[0].attempts, 3);
        let calls = fake.calls();
        assert_eq!(calls.len(), 3);
        let keys: Vec<Option<String>> =
            calls.iter().map(|c| c.idempotency_key.clone()).collect();
        // Not merely "a key was present": every attempt carried the SAME one, which
        // is what lets a provider that honours it collapse the retry.
        assert!(keys[0].is_some());
        assert!(keys.iter().all(|k| *k == keys[0]));
        let expected =
            idempotency_key_for(&post.id, &post.targets[0].social_account_id);
        assert_eq!(keys[0].as_deref(), Some(expected.as_str()));
        assert_eq!(fake.published_count(), 1);
    }

    #[tokio::test]
    async fn a_target_with_a_published_history_row_is_never_republished() {
        // The reaper case: an interrupted run left durable proof the post is live,
        // and the target came back to the queue as `pending`.
        let fake = Arc::new(FakeProvider::new());
        let state = state_with(fake.clone()).await;
        let post = due_post(&state, &body("hello"), &[Platform::X]).await;
        let target = &post.targets[0];
        state
            .store
            .insert_history(
                &target.id,
                HistoryStatus::Published,
                Some("remote_already_live"),
                Some("https://example.test/1"),
                None,
            )
            .await
            .unwrap();

        let outcome = run_post(&state, &post).await.unwrap();

        assert_eq!(outcome.status, PostStatus::Published);
        assert_eq!(outcome.targets[0].attempts, 0);
        assert_eq!(
            outcome.targets[0].remote_id.as_deref(),
            Some("remote_already_live")
        );
        // The whole point: no provider was contacted, so nothing was posted twice.
        assert!(fake.calls().is_empty());
        assert_eq!(fake.published_count(), 0);
    }

    #[tokio::test]
    async fn a_retry_republishes_only_the_failed_leg() {
        let fake = Arc::new(FakeProvider::new().failing_on(Platform::Reddit));
        let state = state_with(fake.clone()).await;
        let post = due_post(&state, &body("hello"), &[Platform::X, Platform::Reddit]).await;
        let first = run_post(&state, &post).await.unwrap();
        assert_eq!(first.status, PostStatus::Partial);
        assert_eq!(fake.published_count(), 1);

        // The user retries. The published leg must not be touched.
        let requeued = queue_retry(&state, &post.id).await.unwrap();
        assert_eq!(requeued.status, PostStatus::Due);
        let second = run_post(&state, &requeued).await.unwrap();

        // Only the failed leg ran again.
        assert_eq!(second.targets.len(), 1);
        assert_eq!(second.targets[0].platform, Platform::Reddit);
        assert_eq!(second.status, PostStatus::Partial);
        // Still exactly one real post: the X leg was never re-sent.
        assert_eq!(fake.published_count(), 1);
    }

    #[tokio::test]
    async fn all_legs_published_settles_the_post_as_published() {
        let fake = Arc::new(FakeProvider::new());
        let state = state_with(fake.clone()).await;
        let post = due_post(&state, &body("hello"), &[Platform::X, Platform::Linkedin]).await;

        let outcome = run_post(&state, &post).await.unwrap();

        assert_eq!(outcome.status, PostStatus::Published);
        assert!(outcome
            .targets
            .iter()
            .all(|t| t.status == TargetStatus::Published));
        assert_eq!(
            state
                .store
                .get_scheduled_post(&post.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Published
        );
    }

    #[tokio::test]
    async fn a_post_with_no_body_fails_locally_without_a_provider_call() {
        let fake = Arc::new(FakeProvider::new());
        let state = state_with(fake.clone()).await;
        let post = due_post(&state, &DraftBody::empty(), &[Platform::X]).await;

        let outcome = run_post(&state, &post).await.unwrap();

        assert_eq!(outcome.status, PostStatus::Failed);
        assert_eq!(outcome.targets[0].attempts, 0);
        assert!(outcome.targets[0]
            .error
            .as_ref()
            .unwrap()
            .contains("No post body"));
        assert!(fake.calls().is_empty());
    }

    #[tokio::test]
    async fn a_lost_claim_is_an_error_and_publishes_nothing() {
        let fake = Arc::new(FakeProvider::new());
        let state = state_with(fake.clone()).await;
        let post = due_post(&state, &body("hello"), &[Platform::X]).await;
        // Another worker got there first.
        assert!(state
            .store
            .claim_post_for_publishing(&post.id)
            .await
            .unwrap());

        let result = run_post(&state, &post).await;

        assert!(matches!(result, Err(ApiError::Conflict(_))));
        assert!(fake.calls().is_empty());
    }

    #[tokio::test]
    async fn a_multi_segment_post_forwards_every_segment_to_a_thread_platform() {
        let fake = Arc::new(FakeProvider::new());
        let state = state_with(fake.clone()).await;
        let mut draft = DraftBody::empty();
        draft.segments = vec![
            PostSegment {
                text: "one".into(),
                media: vec![],
            },
            PostSegment {
                text: "two".into(),
                media: vec![],
            },
        ];
        draft.normalize();
        // X threads; Reddit is a single-post platform and must see only segment 0.
        let post = due_post(&state, &draft, &[Platform::X, Platform::Reddit]).await;

        run_post(&state, &post).await.unwrap();

        let calls = fake.calls();
        let x = calls.iter().find(|c| c.platform == Platform::X).unwrap();
        assert_eq!(x.segment_texts, vec!["one".to_string(), "two".to_string()]);
        let reddit = calls
            .iter()
            .find(|c| c.platform == Platform::Reddit)
            .unwrap();
        assert_eq!(reddit.segment_texts, vec!["one".to_string()]);
    }

    #[test]
    fn media_mime_types_are_derived_from_the_file_name_when_absent() {
        let derived = to_publish_media(&MediaRef {
            path: "/tmp/cat.png".into(),
            mime_type: String::new(),
            name: "cat.png".into(),
        });
        assert_eq!(derived.mime_type, "image/png");
        // Falls back to the path when there is no name at all.
        let from_path = to_publish_media(&MediaRef {
            path: "/tmp/clip.mp4".into(),
            mime_type: String::new(),
            name: String::new(),
        });
        assert_eq!(from_path.mime_type, "video/mp4");
        // Alt text is never fabricated from the file name.
        assert_eq!(derived.alt_text, None);
    }
}
