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
    let store = SocialStore::open_in_memory_sealed().expect("in-memory store");
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
async fn due_post(state: &AppState, body: &DraftBody, platforms: &[Platform]) -> ScheduledPost {
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
        .create_scheduled_post(DEFAULT_WORKSPACE_ID, Some(&draft.id), now_ms(), &targets)
        .await
        .unwrap();
    assert!(state
        .store
        .mark_post_due_now(&post.id, now_ms(), None)
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

/// The runner hands the provider the grant of the agent that scheduled the
/// post, so Core can apply that agent's account access when it goes out.
#[tokio::test]
async fn the_runner_hands_the_scheduling_agents_grant_to_the_provider() {
    let fake = Arc::new(FakeProvider::new());
    let state = state_with(fake.clone()).await;
    let account = state
        .store
        .create_account(
            DEFAULT_WORKSPACE_ID,
            Platform::Linkedin,
            "@me",
            Some("ext_1"),
        )
        .await
        .unwrap();
    let draft = state
        .store
        .create_draft(DEFAULT_WORKSPACE_ID, &body("hello"))
        .await
        .unwrap();
    let post = state
        .store
        .create_scheduled_post_for_agent(
            DEFAULT_WORKSPACE_ID,
            Some(&draft.id),
            now_ms(),
            &[NewTarget {
                social_account_id: account.id,
                platform: Platform::Linkedin,
                variant_body: None,
            }],
            Some("grant.agent"),
        )
        .await
        .unwrap();
    // "Publish now" by the same agent keeps it the post's actor.
    assert!(state
        .store
        .mark_post_due_now(&post.id, now_ms(), Some("grant.agent"))
        .await
        .unwrap());
    let post = state
        .store
        .get_scheduled_post(&post.id)
        .await
        .unwrap()
        .unwrap();

    run_post(&state, &post).await.unwrap();

    let calls = fake.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].agent_grant.as_deref(), Some("grant.agent"));
}

#[tokio::test]
async fn a_post_over_the_platform_limit_fails_that_target_without_calling_the_provider() {
    let fake = Arc::new(FakeProvider::new());
    let state = state_with(fake.clone()).await;
    // 300 characters: legal on LinkedIn, over X's 280-character limit.
    let post = due_post(
        &state,
        &body(&"a".repeat(300)),
        &[Platform::X, Platform::Linkedin],
    )
    .await;

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
    let keys: Vec<Option<String>> = calls.iter().map(|c| c.idempotency_key.clone()).collect();
    // Not merely "a key was present": every attempt carried the SAME one, which
    // is what lets a provider that honours it collapse the retry.
    assert!(keys[0].is_some());
    assert!(keys.iter().all(|k| *k == keys[0]));
    let expected = idempotency_key_for(&post.id, &post.targets[0].social_account_id);
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
    let requeued = queue_retry(&state, &post.id, None).await.unwrap();
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
