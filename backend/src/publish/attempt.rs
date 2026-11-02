//! The attempt loop, local failures, and turning resolved content into a provider
//! request.

use super::*;

/// The attempt loop. One history row for the whole run, written after the loop.
pub(super) async fn attempt_publish(
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

    let mut attempts_spent = max_attempts;
    for attempt in 1..=max_attempts {
        match publish_once_leased(state, &target.id, provider.as_ref(), request).await {
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
            PublishResult::Err { error, retryable } => {
                last_error = error;
                tracing::warn!(
                    target = %target.id,
                    attempt,
                    max_attempts,
                    retryable,
                    error = %last_error,
                    "ryu-social: publish attempt failed"
                );
                if !retryable {
                    // The post may be live, or a retry would only repeat a
                    // failed full-size transfer. Settle now, with the reason.
                    attempts_spent = attempt;
                    break;
                }
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
            target.attempts + attempts_spent,
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
        attempts: attempts_spent,
    })
}

/// Fail a target without ever contacting a provider: no body, or content the platform
/// will not accept. `attempts: 0` is the signal that distinguishes this from a run
/// that tried and was rejected remotely.
pub(super) async fn fail_locally(
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
pub(super) async fn resolve_content(
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
pub(super) fn build_request(
    post: &ScheduledPost,
    target: &PostTarget,
    content: &ResolvedContent,
    agent_grant: Option<&str>,
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
        agent_grant: agent_grant.map(str::to_owned),
    }
}

pub(crate) fn to_publish_media(media: &MediaRef) -> PublishMedia {
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
