//! Inbound engagement: pulling comments, replies, mentions and DMs off each
//! connected account, and sending a reply back where the capability matrix allows.
//!
//! Reading and mutating stored items lives in [`crate::store`] and is wired straight
//! into the routes. This module owns the two operations that leave the process —
//! [`refresh`] and [`reply`] — plus the background poller that calls the first on a
//! timer.
//!
//! ## `refresh`: capability first, then one independent call per read
//!
//! The order matters. A provider that reports `read_comments: false` **and**
//! `read_dms: false` is skipped without a single request: guessing at an unsupported
//! endpoint turns one unconfigured platform into a 404 storm against a third-party
//! key, and rate limits are charged per request, not per useful request. Each
//! supported read is then made independently and its failure captured into
//! [`RefreshSummary::errors`], so one dead tool cannot lose the items the others
//! returned.
//!
//! Every item goes through [`crate::store::SocialStore::ingest_inbox_item`], an
//! `INSERT OR IGNORE` against the `(workspace, account, external_id)` unique index.
//! That dedupe is what makes refresh safe to call on a timer: re-reading the same 50
//! comments inserts nothing and — crucially — does not reset their local `read` /
//! `replied` state. `fetched` and `new` are reported separately so the UI can say
//! "up to date" rather than a misleading "0 items".
//!
//! ### The `inbox.received` event is best-effort, and deliberately AT-MOST-ONCE
//!
//! One event is emitted per genuinely new row, AFTER the insert is durable. That
//! ordering is chosen, not incidental: emitting first would announce items a crash
//! could still lose, and re-emitting on the next poll is impossible because
//! `ingest_inbox_item` will never report that row as new again. So a crash in the
//! window between the insert and the emit permanently drops the notification while
//! keeping the item.
//!
//! That is the correct trade — a hook that files comments into a triage workflow would
//! rather miss one than process the same comment on every poll for the rest of time —
//! and the durable record is the ROW, not the event. **Do not "fix" this by emitting
//! before the insert**, and do not add a retry that re-emits on a later pass: both turn
//! at-most-once into at-least-once for a fan-out that has no idempotency key.
//!
//! ## `reply`: the send is a user action, and it is not history
//!
//! Two things this deliberately does NOT do.
//!
//! **It does not invent the text.** Any AI-suggested draft belongs in an editable box
//! in the UI; this function only transmits what a human approved.
//!
//! **It does not write a `post_history` row.** That would look like the right place to
//! record a reply, and it is a trap: `list_history` reaches a workspace by
//! `post_history → post_targets → scheduled_posts`, an INNER JOIN. A reply has no
//! post target, so a history row for one would need a dangling `post_target_id` and
//! could never be returned by `/history` — an invisible row that makes the table lie
//! about how many publishes happened. The reply's durable record is the item's own
//! `replied` flag plus the platform's copy of the reply.
//!
//! And the ordering is send-then-mark, never the reverse: marking `replied` on a
//! provider call that failed is worse than not replying at all, because the item
//! vanishes from the unreplied filter with nothing on the platform to show for it.

use std::time::Duration;

use crate::error::{ApiError, ApiResult};
use crate::models::{new_id, now_ms, InboxItem, InboxKind, PlatformCapabilities, SocialAccount, ID_INBOX};
use crate::providers::{ProviderAccount, ProviderInboxItem, PublishResult};
use crate::state::{AppState, EVENT_INBOX_RECEIVED};
use crate::store::SocialStore;

/// What one refresh pass did. `fetched` counts items the providers returned;
/// `new` counts the ones that were not already stored. Reporting both is what lets
/// the UI say "up to date" instead of a misleading "0 items".
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RefreshSummary {
    pub accounts_polled: usize,
    /// Connected accounts whose provider reports no inbox capability at all. Counted
    /// rather than silently dropped: "0 polled, 3 skipped" is a configuration
    /// problem the user can act on, while a bare "0 items" is not.
    pub accounts_skipped: usize,
    pub fetched: usize,
    pub new: usize,
    /// Per-account failures, so a partial refresh reports what it could not reach
    /// rather than silently under-returning.
    pub errors: Vec<String>,
}

/// Does this capability matrix admit reading an inbox at all?
///
/// `read_comments` covers comments, replies and mentions — the matrix has one bit for
/// the public surface, mirroring how the broker derives it (any tool slug containing
/// `comment` or `repl`). `read_dms` is the private surface.
pub const fn can_read_inbox(caps: PlatformCapabilities) -> bool {
    caps.read_comments || caps.read_dms
}

/// Which capability bit gates replying to an item of this kind.
///
/// A DM reply is a send on the private surface (`send_dm`); everything else is a
/// public reply, gated by the same bit that admits reading the public surface. There
/// is no separate "reply to comment" bit in the matrix, and inventing one here would
/// mean this module disagreeing with `GET /accounts/:id/capabilities` about what is
/// possible.
pub const fn can_reply(caps: PlatformCapabilities, kind: InboxKind) -> bool {
    match kind {
        InboxKind::Dm => caps.send_dm,
        _ => caps.read_comments,
    }
}

/// Flatten a stored account into the shape a provider call takes.
fn provider_account(account: &SocialAccount) -> ProviderAccount {
    ProviderAccount {
        id: account.id.clone(),
        platform: account.platform,
        label: Some(account.account_label.clone()),
        external_id: account.external_id.clone(),
    }
}

/// Turn a stored item back into the provider shape, for the reply call. The provider
/// needs the remote id it is replying to, not our local row id.
fn provider_item(item: &InboxItem) -> ProviderInboxItem {
    ProviderInboxItem {
        external_id: item.external_id.clone(),
        platform: item.platform,
        kind: item.kind,
        author: item.author.clone(),
        text: item.text.clone(),
        permalink: item.permalink.clone(),
        received_at: item.received_at,
    }
}

/// Persist what one provider read returned, returning ONLY the rows that were new.
///
/// Split out from [`refresh`] deliberately: this half is the whole dedupe contract
/// and it touches no network, so it can be tested against a real in-memory store
/// without a provider to fake. The `Vec` it returns is what the caller emits events
/// for — emitting per *fetched* item would re-announce the same comment on every
/// poll.
///
/// Two normalizations, both to keep a sloppy provider from poisoning the table:
/// - an item with a blank `external_id` is DROPPED, because it has no dedupe key and
///   would re-insert on every single poll;
/// - `platform` comes from the ACCOUNT, not from the item. The account is the
///   authority on which platform it is; a provider that reported something else would
///   create rows the account filter can never reach.
pub async fn ingest_items(
    store: &SocialStore,
    workspace_id: &str,
    account: &SocialAccount,
    items: &[ProviderInboxItem],
) -> anyhow::Result<Vec<InboxItem>> {
    let mut fresh = Vec::new();
    for incoming in items {
        let external_id = incoming.external_id.trim();
        if external_id.is_empty() {
            tracing::debug!(
                account = %account.id,
                "ryu-social: dropping an inbox item with no external id"
            );
            continue;
        }
        let author = incoming.author.trim();
        let item = InboxItem {
            id: new_id(ID_INBOX),
            workspace_id: workspace_id.to_string(),
            social_account_id: account.id.clone(),
            platform: account.platform,
            kind: incoming.kind,
            // An unattributed comment still has to render as something; "unknown"
            // beats an empty author chip.
            author: if author.is_empty() {
                "unknown".to_string()
            } else {
                author.to_string()
            },
            text: incoming.text.clone(),
            permalink: incoming
                .permalink
                .as_deref()
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string),
            external_id: external_id.to_string(),
            // A missing remote timestamp becomes "now" rather than the epoch: sorting
            // by received_at is what keeps the inbox in conversation order, and a 1970
            // stamp would bury the item forever.
            received_at: if incoming.received_at > 0 {
                incoming.received_at
            } else {
                now_ms()
            },
            replied: false,
            read: false,
        };
        if store.ingest_inbox_item(&item).await? {
            fresh.push(item);
        }
    }
    Ok(fresh)
}

/// Poll the platforms for new inbound engagement.
///
/// `account_id` narrows the pass to one account; `None` polls every CONNECTED account
/// in the workspace. A disconnected account is never polled — its credentials are by
/// definition not usable, and calling anyway is how a revoked token turns into a
/// stream of 401s.
pub async fn refresh(
    state: &AppState,
    workspace_id: &str,
    account_id: Option<&str>,
) -> ApiResult<RefreshSummary> {
    let all = state.store.list_accounts(workspace_id).await?;
    let accounts: Vec<SocialAccount> = match account_id {
        Some(id) => {
            let account = all
                .into_iter()
                .find(|a| a.id == id)
                .ok_or_else(|| ApiError::not_found("account"))?;
            // An explicit request for a disconnected account is a 409, not a silent
            // empty result: the caller asked for something specific and deserves to
            // know why it did not happen.
            if !account.connected {
                return Err(ApiError::conflict(format!(
                    "account {} is not connected",
                    account.account_label
                )));
            }
            vec![account]
        }
        None => all.into_iter().filter(|a| a.connected).collect(),
    };

    let mut summary = RefreshSummary::default();
    let mut fresh_items: Vec<InboxItem> = Vec::new();

    for account in &accounts {
        let caps = state.providers.capabilities_for(account.platform).await;
        if !can_read_inbox(caps) {
            summary.accounts_skipped += 1;
            continue;
        }
        summary.accounts_polled += 1;

        let provider = state.providers.provider_for(account.platform);
        // Each account's read is independently fallible — one unreachable platform
        // must not lose the items the others returned.
        let items = match provider.read_inbox(&provider_account(account)).await {
            Ok(items) => items,
            Err(e) => {
                summary
                    .errors
                    .push(format!("{}: {e}", account.account_label));
                continue;
            }
        };
        summary.fetched += items.len();

        match ingest_items(&state.store, workspace_id, account, &items).await {
            Ok(mut new_items) => {
                summary.new += new_items.len();
                fresh_items.append(&mut new_items);
            }
            Err(e) => summary
                .errors
                .push(format!("{}: storing items failed: {e}", account.account_label)),
        }
    }

    // Emitted per NEW item, after everything is durable. A hook that files a comment
    // into a triage workflow wants one event per comment, and emitting before the
    // insert would announce items a crash could still lose.
    for item in &fresh_items {
        state
            .events
            .emit(
                EVENT_INBOX_RECEIVED,
                serde_json::json!({
                    "id": item.id,
                    "workspace_id": item.workspace_id,
                    "social_account_id": item.social_account_id,
                    "platform": item.platform,
                    "kind": item.kind,
                    "author": item.author,
                    "text": item.text,
                    "permalink": item.permalink,
                    "received_at": item.received_at,
                }),
            )
            .await;
    }

    Ok(summary)
}

/// Send a reply to one inbound item, then mark it replied.
pub async fn reply(state: &AppState, item_id: &str, text: &str) -> ApiResult<InboxItem> {
    let item = state
        .store
        .get_inbox_item(item_id)
        .await?
        .ok_or_else(|| ApiError::not_found("inbox item"))?;

    let caps = state.providers.capabilities_for(item.platform).await;
    if !can_reply(caps, item.kind) {
        return Err(ApiError::conflict(format!(
            "replying to a {} on {} is not supported by the configured provider",
            item.kind.as_str(),
            item.platform.label()
        )));
    }

    let provider = state.providers.provider_for(item.platform);
    match provider.reply_to_inbox_item(&provider_item(&item), text).await {
        PublishResult::Ok { .. } => {}
        // 502, not 500: the fault is the platform's, and the distinction is what
        // tells a user "try again" from "this is broken".
        PublishResult::Err { error } => return Err(ApiError::upstream(error)),
    }

    // Only after the send succeeded. The store couples `read = 1` on this path, so a
    // replied item cannot be left unread — a state no user could produce by hand.
    state.store.mark_inbox_replied(&item.id, true).await?;
    state
        .store
        .get_inbox_item(&item.id)
        .await?
        .ok_or_else(|| ApiError::not_found("inbox item"))
}

// ── Background poller ──────────────────────────────────────────────────────────

/// Start the inbox poll loop. Returns the handle so `main` can abort it on shutdown.
///
/// Three things it deliberately does:
///
/// - **Re-reads the node settings every cycle**, so changing the interval or turning
///   polling off in the settings tab takes effect on the next pass rather than on the
///   next process restart. That is also why it sleeps rather than using a
///   `tokio::time::interval`, whose period is fixed at construction.
/// - **Skips a workspace entirely when nothing supports an inbox read.** With no
///   provider configured, every account's matrix is all-false, so the whole pass is
///   one capability lookup per account and zero network calls.
/// - **Respects quiet hours.** The poll itself is invisible, but a new-item event
///   fans out to hooks and workflows that may well notify someone.
pub fn spawn_poller(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // Read settings FIRST so a disabled poller still costs one cheap read per
            // period instead of spinning.
            let settings = match crate::settings::load(&state.store).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "ryu-social: inbox poller could not read settings");
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    continue;
                }
            };
            let period = settings.inbox_poll_period();

            if settings.inbox_polling_enabled && !settings.in_quiet_hours(now_ms()) {
                poll_once(&state).await;
            }
            tokio::time::sleep(period).await;
        }
    })
}

/// One poll pass over every workspace. Never propagates: a failing pass must not kill
/// the loop, because the next one may well succeed and a dead poller is silent.
async fn poll_once(state: &AppState) {
    let workspaces = match state.store.list_workspaces().await {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(error = %e, "ryu-social: inbox poll could not list workspaces");
            return;
        }
    };
    for workspace in workspaces {
        match refresh(state, &workspace.id, None).await {
            Ok(summary) => {
                if summary.new > 0 || !summary.errors.is_empty() {
                    tracing::info!(
                        workspace = %workspace.id,
                        polled = summary.accounts_polled,
                        skipped = summary.accounts_skipped,
                        fetched = summary.fetched,
                        new = summary.new,
                        errors = summary.errors.len(),
                        "ryu-social: inbox poll"
                    );
                }
            }
            Err(e) => tracing::warn!(
                workspace = %workspace.id,
                error = %e,
                "ryu-social: inbox poll failed"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Platform, DEFAULT_WORKSPACE_ID};
    use crate::store::InboxFilter;

    fn incoming(external_id: &str, kind: InboxKind, text: &str) -> ProviderInboxItem {
        ProviderInboxItem {
            external_id: external_id.to_string(),
            platform: Platform::X,
            kind,
            author: "@someone".to_string(),
            text: text.to_string(),
            permalink: Some("https://x.com/1".to_string()),
            received_at: 1_000,
        }
    }

    async fn fixture() -> (SocialStore, SocialAccount) {
        let store = SocialStore::open_in_memory().unwrap();
        let account = store
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", Some("ext_1"))
            .await
            .unwrap();
        (store, account)
    }

    #[tokio::test]
    async fn re_polling_the_same_items_inserts_nothing_and_keeps_local_state() {
        let (store, account) = fixture().await;
        let batch = vec![
            incoming("c1", InboxKind::Comment, "nice"),
            incoming("c2", InboxKind::Mention, "hey @me"),
        ];

        let first = ingest_items(&store, DEFAULT_WORKSPACE_ID, &account, &batch)
            .await
            .unwrap();
        assert_eq!(first.len(), 2);

        // The user reads and replies to one of them.
        let stored = store
            .list_inbox(DEFAULT_WORKSPACE_ID, &InboxFilter::default(), 50)
            .await
            .unwrap();
        let target = stored.iter().find(|i| i.external_id == "c1").unwrap();
        store.mark_inbox_replied(&target.id, true).await.unwrap();

        // A re-poll returns the same two items plus one genuinely new one.
        let mut second_batch = batch.clone();
        second_batch.push(incoming("c3", InboxKind::Reply, "thanks!"));
        let second = ingest_items(&store, DEFAULT_WORKSPACE_ID, &account, &second_batch)
            .await
            .unwrap();
        assert_eq!(second.len(), 1, "only the unseen item is new");
        assert_eq!(second[0].external_id, "c3");

        let after = store
            .list_inbox(DEFAULT_WORKSPACE_ID, &InboxFilter::default(), 50)
            .await
            .unwrap();
        assert_eq!(after.len(), 3, "no duplicates");
        let replied = after.iter().find(|i| i.external_id == "c1").unwrap();
        assert!(replied.replied, "a re-poll must not reset replied");
        assert!(replied.read, "…nor the read flag it implies");
    }

    #[tokio::test]
    async fn ingest_normalizes_a_sloppy_provider_payload() {
        let (store, account) = fixture().await;
        let items = vec![
            // No external id → dropped; it has no dedupe key.
            ProviderInboxItem {
                external_id: "  ".into(),
                ..incoming("x", InboxKind::Comment, "orphan")
            },
            // Blank author and a missing timestamp.
            ProviderInboxItem {
                external_id: "c9".into(),
                author: "   ".into(),
                received_at: 0,
                permalink: Some("  ".into()),
                // The provider claims the wrong platform; the ACCOUNT wins.
                platform: Platform::Linkedin,
                ..incoming("c9", InboxKind::Dm, "hello")
            },
        ];
        let new_items = ingest_items(&store, DEFAULT_WORKSPACE_ID, &account, &items)
            .await
            .unwrap();
        assert_eq!(new_items.len(), 1);
        let item = &new_items[0];
        assert_eq!(item.author, "unknown");
        assert!(item.received_at > 0);
        assert_eq!(item.permalink, None);
        assert_eq!(item.platform, Platform::X);
    }

    #[tokio::test]
    async fn two_accounts_may_carry_the_same_external_id() {
        // The dedupe key is (workspace, account, external_id) — two platforms
        // numbering their comments from 1 must not collide.
        let (store, first) = fixture().await;
        let second = store
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@me.bsky", None)
            .await
            .unwrap();
        let batch = vec![incoming("1", InboxKind::Comment, "same id, other account")];

        assert_eq!(
            ingest_items(&store, DEFAULT_WORKSPACE_ID, &first, &batch)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            ingest_items(&store, DEFAULT_WORKSPACE_ID, &second, &batch)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .list_inbox(DEFAULT_WORKSPACE_ID, &InboxFilter::default(), 50)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn capability_gates_match_the_matrix() {
        let none = PlatformCapabilities::empty();
        assert!(!can_read_inbox(none));
        assert!(!can_reply(none, InboxKind::Comment));
        assert!(!can_reply(none, InboxKind::Dm));

        let public_only = PlatformCapabilities {
            read_comments: true,
            ..PlatformCapabilities::empty()
        };
        assert!(can_read_inbox(public_only));
        assert!(can_reply(public_only, InboxKind::Comment));
        assert!(can_reply(public_only, InboxKind::Mention));
        // Reading the public surface says nothing about sending a DM.
        assert!(!can_reply(public_only, InboxKind::Dm));

        let dms_only = PlatformCapabilities {
            read_dms: true,
            send_dm: true,
            ..PlatformCapabilities::empty()
        };
        assert!(can_read_inbox(dms_only));
        assert!(can_reply(dms_only, InboxKind::Dm));
        assert!(!can_reply(dms_only, InboxKind::Comment));
    }

    #[tokio::test]
    async fn refresh_skips_accounts_no_provider_can_read() {
        // With no provider configured every matrix is all-false, so a refresh makes
        // ZERO network calls and reports the skip honestly rather than "0 items".
        let store = SocialStore::open_in_memory().unwrap();
        store
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let state = AppState::new(store, crate::state::Config::from_env(0));

        let summary = refresh(&state, DEFAULT_WORKSPACE_ID, None).await.unwrap();
        assert_eq!(summary.accounts_polled, 0);
        assert_eq!(summary.accounts_skipped, 1);
        assert_eq!(summary.fetched, 0);
        assert!(summary.errors.is_empty());
    }

    #[tokio::test]
    async fn refresh_never_polls_a_disconnected_account() {
        let store = SocialStore::open_in_memory().unwrap();
        let account = store
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        store
            .set_account_connection(&account.id, false, None)
            .await
            .unwrap();
        let state = AppState::new(store, crate::state::Config::from_env(0));

        let summary = refresh(&state, DEFAULT_WORKSPACE_ID, None).await.unwrap();
        assert_eq!(summary.accounts_polled + summary.accounts_skipped, 0);

        // Asking for it BY ID is a conflict, not a silent empty pass.
        let err = refresh(&state, DEFAULT_WORKSPACE_ID, Some(&account.id))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Conflict(_)), "{err}");
    }
}
