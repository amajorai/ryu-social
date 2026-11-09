//! The runner: one claimed post through its targets, and one target through its
//! provider call. See the parent module docs for the ordering.

use super::*;

// ── The runner ─────────────────────────────────────────────────────────────────

/// Content resolved for one target, ready to become a provider call.
pub(super) struct ResolvedContent {
    pub(super) account: ProviderAccount,
    pub(super) segments: Vec<PostSegment>,
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
    // The agent behind the post, read AFTER the draft: an agent's draft edit
    // records its grant before changing the text, so this order can never pair
    // an agent's new text with an older (wider) actor. Core applies that agent's
    // current account access to every provider call this run makes.
    let agent_grant = state.store.post_agent_grant(&post.id).await?;

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
            agent_grant.as_deref(),
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
pub(super) async fn settle_post(
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

    let status = aggregate_target_statuses(&persisted.iter().map(|t| t.status).collect::<Vec<_>>());
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
    // a live Core and a down Core never blocks a publish. A completed publish raises
    // a user notification (the Inbox row shows the Outpost icon) so the outcome
    // surfaces even when nobody is watching the app.
    match status {
        PostStatus::Published => {
            state
                .events
                .emit_with_notify(
                    EVENT_POST_PUBLISHED,
                    payload,
                    Some(crate::fanout_messages::post_published()),
                )
                .await
        }
        PostStatus::Partial | PostStatus::Failed => {
            state
                .events
                .emit_with_notify(
                    EVENT_POST_FAILED,
                    payload,
                    Some(crate::fanout_messages::post_failed()),
                )
                .await;
        }
        _ => {}
    }
    Ok(status)
}

/// Run one target: resolve, validate, attempt, record.
pub(super) async fn run_target(
    state: &AppState,
    post: &ScheduledPost,
    target: &PostTarget,
    draft_body: Option<&crate::models::DraftBody>,
    agent_grant: Option<&str>,
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

    for segment in &content.segments {
        for media in &segment.media {
            let lower_path = media.path.to_ascii_lowercase();
            if lower_path.starts_with("http://") || lower_path.starts_with("https://") {
                continue;
            }
            if let Err(error) = crate::state::approved_local_media_path(&media.path) {
                return fail_locally(
                    state,
                    target,
                    format!("local media is not allowed: {error}"),
                )
                .await;
            }
        }
    }

    let request = build_request(post, target, &content, agent_grant);
    let provider = state
        .providers
        .provider_for_operation(&content.account, ProviderOperation::Publish, Some(&request))
        .await;
    attempt_publish(
        state,
        target,
        provider,
        &request,
        max_attempts,
        base_backoff_ms,
    )
    .await
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
/// An elapsed deadline maps to a `PublishResult::Err`. For an ordinary call it
/// retries and settles like any other provider failure. A provider that raises the
/// deadline for a long upload ([`PlatformProvider::publish_deadline`]) can mark an
/// elapsed deadline as not retryable: the upload may already have reached the
/// platform when it was cut off, and a blind retry could publish it twice.
pub(super) async fn publish_once(
    provider: &dyn PlatformProvider,
    request: &PublishRequest,
) -> PublishResult {
    let deadline = provider.publish_deadline(request);
    match tokio::time::timeout(deadline.limit, provider.publish(request)).await {
        Ok(result) => result,
        Err(_) => {
            let message = format!(
                "the {} provider did not answer within {}s",
                request.account.platform,
                deadline.limit.as_secs()
            );
            if deadline.retry_after_timeout {
                PublishResult::err(message)
            } else {
                PublishResult::final_err(format!(
                    "{message}; the post may have been published anyway, so it was not retried. Check {} before publishing it again",
                    request.account.platform.label()
                ))
            }
        }
    }
}

/// How often a running publish refreshes its target's lease.
///
/// Well under any lease the scheduler computes (its floor is minutes), so a
/// healthy publish that outlives the computed worst case, such as a multi-minute
/// video upload, is never reaped and re-run underneath itself. A process that
/// dies stops refreshing, and the reaper recovers the target as before.
pub(super) const LEASE_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(30);

/// [`publish_once`], refreshing the target's lease while the call runs.
pub(super) async fn publish_once_leased(
    state: &AppState,
    target_id: &str,
    provider: &dyn PlatformProvider,
    request: &PublishRequest,
) -> PublishResult {
    let call = publish_once(provider, request);
    tokio::pin!(call);
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + LEASE_HEARTBEAT,
        LEASE_HEARTBEAT,
    );
    loop {
        tokio::select! {
            result = &mut call => return result,
            _ = heartbeat.tick() => {
                if let Err(error) = state.store.renew_claim(target_id, now_ms()).await {
                    tracing::warn!(target = %target_id, error = %error, "ryu-social: could not refresh the publish lease");
                }
            }
        }
    }
}
