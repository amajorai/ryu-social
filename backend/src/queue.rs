//! The read model behind `GET /queue`: what the runner is going to do, in the order
//! it will actually do it.
//!
//! ## Why this is a module and not a `SELECT`
//!
//! [`crate::store::SocialStore::list_queue`] already returns the rows —
//! target + its post's schedule + status — ordered by
//! `COALESCE(next_attempt_at, scheduled_for)`. What it cannot answer is the question
//! a user actually opens the queue to ask: *"this one says it will retry in four
//! minutes — why did it fail?"* That answer lives in `post_history`, keyed by target,
//! and joining it into the list query would mean either a correlated subquery per row
//! or a `GROUP BY` whose "latest row" semantics SQLite makes easy to get subtly wrong.
//!
//! So the enrichment happens here, in one pass, with two rules that keep it cheap:
//!
//! - **Accounts are fetched once** for the workspace and joined in memory. The queue
//!   is bounded (default 50 rows) and a workspace has a handful of accounts; one
//!   query beats N.
//! - **History is fetched ONLY for targets that have actually run** (`attempts > 0`).
//!   A freshly scheduled queue — the common case — makes zero history queries,
//!   because a target that has never been attempted has nothing to explain.
//!
//! ## Ordering is the store's, not this module's
//!
//! Rows come back in `next_attempt_at` order and are NOT re-sorted here. That column
//! is the runner's own predicate, so any local re-ranking (by platform, by post, by
//! creation) would show a queue that disagrees with what happens next — which is the
//! one thing a queue view must never do.

use std::collections::HashMap;

use serde::Serialize;

use crate::error::ApiResult;
use crate::models::{now_ms, HistoryStatus, TargetStatus};
use crate::state::AppState;
use crate::store::QueueEntry;

/// One queued target, with everything needed to render a row without a second fetch.
#[derive(Debug, Clone, Serialize)]
pub struct QueueItem {
    /// The store's projection: the target, its post's `scheduled_for` / status /
    /// `draft_id`, and the resolved `next_attempt_at`. Flattened, so the wire shape
    /// stays a superset of what `list_queue` alone returned — a consumer written
    /// against the raw entry keeps working.
    #[serde(flatten)]
    pub entry: QueueEntry,
    /// The account's `@handle`. `None` when the account row was hard-deleted out from
    /// under a target that is still queued — which the schema deliberately allows, so
    /// the UI needs a shape for it rather than a broken join.
    pub account_label: Option<String>,
    /// The platform's display name, resolved once here so every surface renders
    /// "TikTok" and "LinkedIn" identically.
    pub platform_label: String,
    /// Why the last run failed, if one did. `None` for a target that has never been
    /// attempted OR whose last run succeeded.
    pub last_error: Option<String>,
    /// When that last run was recorded.
    pub last_attempt_at: Option<i64>,
    /// Provider calls left before this target is failed for good, under the
    /// workspace's current `max_attempts`. Saturating: lowering `max_attempts` below
    /// what a target has already spent shows `0`, not a negative.
    pub attempts_remaining: u32,
    /// Milliseconds until this target runs. Negative when it is overdue — which is
    /// information, not an error: it means the runner has not caught up yet, and
    /// clamping it to zero would hide a stalled scheduler.
    pub runs_in_ms: i64,
    /// Whether a runner currently holds this target.
    pub in_flight: bool,
}

/// The whole queue view.
#[derive(Debug, Clone, Serialize)]
pub struct QueueView {
    pub items: Vec<QueueItem>,
    /// When the runner next acts, across the whole queue. `None` on an empty queue.
    pub next_run_at: Option<i64>,
    /// How many rows a runner currently holds. Surfaced separately because "3 queued"
    /// and "3 queued, 2 publishing right now" are very different states to look at.
    pub in_flight: usize,
    /// The clock this view's `runs_in_ms` values were computed against, so a client
    /// can re-derive a countdown without assuming its own clock agrees.
    pub generated_at: i64,
}

/// Build the queue view for one workspace.
pub async fn build(state: &AppState, workspace_id: &str, limit: usize) -> ApiResult<QueueView> {
    let now = now_ms();
    let entries = state.store.list_queue(workspace_id, limit).await?;
    if entries.is_empty() {
        return Ok(QueueView {
            items: Vec::new(),
            next_run_at: None,
            in_flight: 0,
            generated_at: now,
        });
    }

    let max_attempts = state.store.get_settings(workspace_id).await?.max_attempts;

    // One query for every label in the view.
    let labels: HashMap<String, String> = state
        .store
        .list_accounts(workspace_id)
        .await?
        .into_iter()
        .map(|a| (a.id, a.account_label))
        .collect();

    let mut items = Vec::with_capacity(entries.len());
    for entry in entries {
        // Only a target that has actually run can have an explanation. This is the
        // whole reason the common case costs zero extra queries.
        let last = if entry.target.attempts > 0 {
            last_failure(state, &entry.target.id).await?
        } else {
            None
        };
        items.push(QueueItem {
            account_label: labels.get(&entry.target.social_account_id).cloned(),
            platform_label: entry.target.platform.label().to_string(),
            last_error: last.as_ref().and_then(|(error, _)| error.clone()),
            last_attempt_at: last.and_then(|(_, at)| at),
            attempts_remaining: max_attempts.saturating_sub(entry.target.attempts),
            runs_in_ms: entry.next_attempt_at - now,
            in_flight: entry.target.status == TargetStatus::Publishing,
            entry,
        });
    }

    Ok(QueueView {
        // The store already ordered by when each row runs, so the first row IS the
        // next one — no min() scan, and no risk of the two disagreeing.
        next_run_at: items.first().map(|i| i.entry.next_attempt_at),
        in_flight: items.iter().filter(|i| i.in_flight).count(),
        generated_at: now,
        items,
    })
}

/// The most recent recorded run for a target, as `(error, published_at)`.
///
/// `list_history_for_target` orders `published_at DESC`, so the first row is the
/// latest. The error is returned only when that latest row FAILED: a target that
/// failed twice and then succeeded has a failure in its history, but surfacing it
/// next to a successful run would read as a current problem.
async fn last_failure(
    state: &AppState,
    target_id: &str,
) -> ApiResult<Option<(Option<String>, Option<i64>)>> {
    let history = state.store.list_history_for_target(target_id).await?;
    Ok(history.first().map(|entry| {
        let error = if entry.status == HistoryStatus::Failed {
            entry.error.clone()
        } else {
            None
        };
        (error, entry.published_at)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        HistoryStatus, Platform, PostStatus, SocialSettings, TargetStatus, DEFAULT_WORKSPACE_ID,
    };
    use crate::state::Config;
    use crate::store::{NewTarget, SocialStore};

    async fn state() -> (AppState, SocialStore) {
        let store = SocialStore::open_in_memory().expect("in-memory store");
        (AppState::new(store.clone(), Config::from_env(0)), store)
    }

    #[tokio::test]
    async fn an_empty_queue_is_an_empty_view_not_an_error() {
        let (state, _) = state().await;
        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert!(view.items.is_empty());
        assert_eq!(view.next_run_at, None);
        assert_eq!(view.in_flight, 0);
    }

    #[tokio::test]
    async fn the_view_orders_by_when_each_target_actually_runs() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@handle", None)
            .await
            .unwrap();
        // Scheduled later…
        let later = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                50_000,
                &[NewTarget {
                    social_account_id: account.id.clone(),
                    platform: Platform::Bluesky,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        // …but scheduled sooner.
        let sooner = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                10_000,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::Bluesky,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert_eq!(view.items.len(), 2);
        assert_eq!(view.items[0].entry.target.scheduled_post_id, sooner.id);
        assert_eq!(view.items[1].entry.target.scheduled_post_id, later.id);
        assert_eq!(view.next_run_at, Some(10_000));
        // Labels are resolved, not left to the client.
        assert_eq!(view.items[0].account_label.as_deref(), Some("@handle"));
        assert_eq!(view.items[0].platform_label, "Bluesky");
        assert_eq!(view.items[0].entry.post_status, PostStatus::Scheduled);
        // Never attempted ⇒ nothing to explain, and the full retry budget left.
        assert_eq!(view.items[0].last_error, None);
        assert_eq!(
            view.items[0].attempts_remaining,
            SocialSettings::default().max_attempts
        );
    }

    #[tokio::test]
    async fn a_retrying_target_carries_its_last_error_and_remaining_budget() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        let target = post.targets[0].id.clone();

        // `insert_history` stamps `published_at` itself, so the assertion below
        // brackets it rather than pinning a literal.
        let before = now_ms();
        s.insert_history(
            &target,
            HistoryStatus::Failed,
            None,
            None,
            Some("rate limited"),
        )
        .await
        .unwrap();
        s.settle_target(&target, TargetStatus::Pending, 1, Some(9_000))
            .await
            .unwrap();

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        let item = view
            .items
            .iter()
            .find(|i| i.entry.target.id == target)
            .unwrap();
        assert_eq!(item.last_error.as_deref(), Some("rate limited"));
        assert!(item.last_attempt_at.is_some_and(|at| at >= before));
        assert_eq!(item.entry.next_attempt_at, 9_000);
        assert_eq!(item.attempts_remaining, 2);
        assert!(!item.in_flight);
    }

    /// A target that failed, retried, and then SUCCEEDED still has a failure in its
    /// history. Showing it would read as a live problem.
    #[tokio::test]
    async fn a_recovered_target_does_not_keep_reporting_its_old_failure() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        let target = post.targets[0].id.clone();

        let before = now_ms();
        s.insert_history(
            &target,
            HistoryStatus::Failed,
            None,
            None,
            Some("transient"),
        )
        .await
        .unwrap();
        // Both rows can land in the SAME millisecond, which is precisely the tie
        // `list_history_for_target`'s `rowid DESC` tiebreak exists to resolve. If
        // that tiebreak is ever dropped this test fails intermittently — which is
        // the point of asserting it here rather than sleeping to dodge it.
        s.insert_history(
            &target,
            HistoryStatus::Published,
            Some("remote-1"),
            Some("https://example.test/1"),
            None,
        )
        .await
        .unwrap();
        // Still queued (another leg of the same post is what keeps it listed), but
        // its own latest run was a success.
        s.settle_target(&target, TargetStatus::Pending, 2, Some(9_000))
            .await
            .unwrap();

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        let item = view
            .items
            .iter()
            .find(|i| i.entry.target.id == target)
            .unwrap();
        assert_eq!(item.last_error, None);
        assert!(item.last_attempt_at.is_some_and(|at| at >= before));
    }

    #[tokio::test]
    async fn an_in_flight_target_is_counted_and_flagged() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post.id).await.unwrap();
        s.claim_target(&post.targets[0].id, 1_000).await.unwrap();

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert_eq!(view.in_flight, 1);
        assert!(view.items[0].in_flight);
    }

    /// The account can be hard-deleted while a target is still IN FLIGHT — the
    /// schema allows the dangling reference on purpose. The view must have a shape
    /// for that rather than dropping the row.
    ///
    /// The target is claimed first because `delete_account` cancels the account's
    /// still-`pending` legs (a removed account must stop receiving queued posts),
    /// and a cancelled leg correctly leaves the queue. A `publishing` leg has already
    /// contacted a provider and is left alone, so it is the case that exercises
    /// rendering without an account row.
    #[tokio::test]
    async fn a_target_whose_account_was_deleted_still_renders() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Linkedin, "@gone", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[NewTarget {
                    social_account_id: account.id.clone(),
                    platform: Platform::Linkedin,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post.id).await.unwrap();
        s.claim_target(&post.targets[0].id, 1_000).await.unwrap();
        assert!(s.delete_account(&account.id).await.unwrap());

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert_eq!(view.items.len(), 1);
        assert_eq!(view.items[0].account_label, None);
        // The denormalized platform on the target is what keeps the row renderable.
        assert_eq!(view.items[0].platform_label, "LinkedIn");
    }

    /// The wire contract: the enrichment is ADDITIVE. `QueueItem` flattens
    /// `QueueEntry`, which itself flattens `PostTarget`, so a consumer written
    /// against the raw `list_queue` row keeps working. A nested `#[serde(flatten)]`
    /// that silently nested instead of flattening would break every such consumer
    /// without breaking the build, which is why this is asserted rather than assumed.
    #[tokio::test]
    async fn the_serialized_row_is_a_superset_of_the_raw_store_entry() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        s.create_scheduled_post(
            DEFAULT_WORKSPACE_ID,
            None,
            10_000,
            &[NewTarget {
                social_account_id: account.id,
                platform: Platform::X,
                variant_body: None,
            }],
        )
        .await
        .unwrap();

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        let json = serde_json::to_value(&view.items[0]).unwrap();
        let row = json.as_object().expect("a queue row is a flat object");
        for key in [
            // From `PostTarget`, two flatten levels down.
            "id",
            "scheduled_post_id",
            "social_account_id",
            "platform",
            "status",
            "attempts",
            // From `QueueEntry`.
            "scheduled_for",
            "post_status",
            "next_attempt_at",
            // This module's enrichment.
            "account_label",
            "platform_label",
            "last_error",
            "attempts_remaining",
            "runs_in_ms",
            "in_flight",
        ] {
            assert!(row.contains_key(key), "missing `{key}` in {row:?}");
        }
    }

    #[tokio::test]
    async fn an_overdue_target_reports_a_negative_countdown_rather_than_zero() {
        let (state, s) = state().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        // Scheduled at the epoch: comprehensively overdue.
        s.create_scheduled_post(
            DEFAULT_WORKSPACE_ID,
            None,
            1_000,
            &[NewTarget {
                social_account_id: account.id,
                platform: Platform::X,
                variant_body: None,
            }],
        )
        .await
        .unwrap();

        let view = build(&state, DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert!(
            view.items[0].runs_in_ms < 0,
            "an overdue row must stay visibly overdue, not clamp to now"
        );
    }
}
