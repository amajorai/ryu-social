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
        connected_account_id: None,
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

    let summary = refresh(&state, DEFAULT_WORKSPACE_ID, None, None)
        .await
        .unwrap();
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

    let summary = refresh(&state, DEFAULT_WORKSPACE_ID, None, None)
        .await
        .unwrap();
    assert_eq!(summary.accounts_polled + summary.accounts_skipped, 0);

    // Asking for it BY ID is a conflict, not a silent empty pass.
    let err = refresh(&state, DEFAULT_WORKSPACE_ID, Some(&account.id), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Conflict(_)), "{err}");
}
