//! Handler-level tests for account connect/capabilities, the inbox refresh and
//! reply round trip, template patch/use, workspace rename, and media-import abort.
//!
//! Kept out of `api.rs`'s own test module only for size; the handlers are called
//! directly with real extractors over an in-memory store, as the tests there are.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde_json::json;

use super::*;
use crate::models::{limits_for, InboxKind, Platform, PlatformCapabilities, TemplateBody};
use crate::providers::{
    FakeProvider, PlatformProvider, ProviderAccount, ProviderId, ProviderInboxItem,
    ProviderRegistry, PublishRequest, PublishResult, RemotePostRef,
};
use crate::state::Config;
use crate::store::SocialStore;

fn state() -> AppState {
    AppState::new(
        SocialStore::open_in_memory_sealed().expect("in-memory store"),
        Config::from_env(0),
    )
}

fn state_with<P: PlatformProvider + 'static>(provider: Arc<P>) -> AppState {
    let mut state = state();
    state.providers = ProviderRegistry::with_provider(provider);
    state
}

fn grant_headers(grant: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ryu_app_events::HDR_AGENT_GRANT, grant.parse().unwrap());
    headers
}

// ── Accounts: connect + capabilities ──────────────────────────────────────────

#[tokio::test]
async fn connect_records_the_handshake_external_id_and_marks_connected() {
    let state = state_with(Arc::new(FakeProvider::new()));
    let account = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
        .await
        .unwrap();
    state
        .store
        .set_account_connection(&account.id, false, None)
        .await
        .unwrap();

    let connected = connect_account(
        State(state.clone()),
        Path(account.id.clone()),
        HeaderMap::new(),
    )
    .await
    .unwrap()
    .0;
    let expected_external = format!("fake_account_{}", account.id);
    assert!(connected.connected);
    assert_eq!(
        connected.external_id.as_deref(),
        Some(expected_external.as_str())
    );

    let stored = state.store.get_account(&account.id).await.unwrap().unwrap();
    assert!(stored.connected);
    assert_eq!(
        stored.external_id.as_deref(),
        Some(expected_external.as_str())
    );
}

#[tokio::test]
async fn a_failed_handshake_is_upstream_and_leaves_the_account_untouched() {
    // No provider configured: the fallback refuses to connect.
    let state = state();
    let account = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::Linkedin, "@co", None)
        .await
        .unwrap();
    state
        .store
        .set_account_connection(&account.id, false, None)
        .await
        .unwrap();

    let refused = connect_account(
        State(state.clone()),
        Path(account.id.clone()),
        HeaderMap::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(refused, ApiError::Upstream(_)), "{refused}");
    let stored = state.store.get_account(&account.id).await.unwrap().unwrap();
    assert!(!stored.connected);
    assert!(stored.external_id.is_none());

    let missing = connect_account(State(state), Path("acc_missing".into()), HeaderMap::new())
        .await
        .unwrap_err();
    assert!(matches!(missing, ApiError::NotFound(_)), "{missing}");
}

#[tokio::test]
async fn capabilities_follow_the_accounts_platform_and_provider() {
    let state = state_with(Arc::new(
        FakeProvider::new().without_publish_for(Platform::Bluesky),
    ));
    let x = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
        .await
        .unwrap();
    let bluesky = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@me.bsky", None)
        .await
        .unwrap();

    let x_caps = account_capabilities(State(state.clone()), Path(x.id.clone()))
        .await
        .unwrap()
        .0;
    assert_eq!(x_caps["account_id"], json!(x.id));
    assert_eq!(x_caps["platform"], json!("x"));
    assert_eq!(x_caps["capabilities"]["publish"], json!(true));
    assert_eq!(x_caps["capabilities"]["send_dm"], json!(true));
    assert_eq!(
        x_caps["limits"],
        serde_json::to_value(limits_for(Platform::X)).unwrap()
    );

    let bsky_caps = account_capabilities(State(state.clone()), Path(bluesky.id))
        .await
        .unwrap()
        .0;
    assert_eq!(bsky_caps["capabilities"]["publish"], json!(false));
    assert_eq!(bsky_caps["capabilities"]["send_dm"], json!(false));
    assert_eq!(
        bsky_caps["limits"],
        serde_json::to_value(limits_for(Platform::Bluesky)).unwrap()
    );
    assert_ne!(bsky_caps["limits"], x_caps["limits"]);

    let missing = account_capabilities(State(state), Path("acc_missing".into()))
        .await
        .unwrap_err();
    assert!(matches!(missing, ApiError::NotFound(_)), "{missing}");
}

// ── Inbox: refresh + reply ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
struct SentReply {
    external_id: String,
    connected_account_id: Option<String>,
    text: String,
    grant: Option<String>,
}

/// An inbox-capable provider: public comments read and reply, no DMs.
#[derive(Default)]
struct ScriptedInbox {
    items: Vec<ProviderInboxItem>,
    fail_reply: bool,
    read_grants: Mutex<Vec<Option<String>>>,
    replies: Mutex<Vec<SentReply>>,
}

#[async_trait]
impl PlatformProvider for ScriptedInbox {
    fn id(&self) -> ProviderId {
        ProviderId::Fake
    }

    async fn connect(
        &self,
        _account: &ProviderAccount,
        _agent_grant: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    async fn disconnect(&self, _account: &ProviderAccount) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish(&self, _request: &PublishRequest) -> PublishResult {
        PublishResult::err("not used")
    }

    async fn read_engagement(
        &self,
        _post: &RemotePostRef,
        _agent_grant: Option<&str>,
    ) -> anyhow::Result<crate::models::EngagementCounts> {
        anyhow::bail!("not used")
    }

    async fn capabilities(&self, _platform: Platform) -> PlatformCapabilities {
        PlatformCapabilities {
            read_comments: true,
            ..PlatformCapabilities::empty()
        }
    }

    async fn read_inbox(
        &self,
        _account: &ProviderAccount,
        agent_grant: Option<&str>,
    ) -> anyhow::Result<Vec<ProviderInboxItem>> {
        self.read_grants
            .lock()
            .unwrap()
            .push(agent_grant.map(str::to_owned));
        Ok(self.items.clone())
    }

    async fn reply_to_inbox_item(
        &self,
        item: &ProviderInboxItem,
        text: &str,
        agent_grant: Option<&str>,
    ) -> PublishResult {
        self.replies.lock().unwrap().push(SentReply {
            external_id: item.external_id.clone(),
            connected_account_id: item.connected_account_id.clone(),
            text: text.to_owned(),
            grant: agent_grant.map(str::to_owned),
        });
        if self.fail_reply {
            PublishResult::err("platform rejected the reply")
        } else {
            PublishResult::Ok {
                remote_id: "reply_1".into(),
                remote_url: None,
            }
        }
    }
}

fn incoming(external_id: &str, kind: InboxKind) -> ProviderInboxItem {
    ProviderInboxItem {
        external_id: external_id.into(),
        platform: Platform::X,
        kind,
        author: "@fan".into(),
        text: format!("item {external_id}"),
        permalink: None,
        received_at: 1_000,
        connected_account_id: None,
    }
}

/// A state over `provider`, one connected X account, and its inbox refreshed.
async fn refreshed(provider: Arc<ScriptedInbox>) -> (AppState, Vec<crate::models::InboxItem>) {
    let state = state_with(provider);
    state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", Some("ext_me"))
        .await
        .unwrap();
    let summary = refresh_inbox(
        State(state.clone()),
        grant_headers("grant.reader"),
        Json(RefreshInboxBody {
            workspace_id: None,
            account_id: None,
        }),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(summary.accounts_polled, 1);
    let items = state
        .store
        .list_inbox(DEFAULT_WORKSPACE_ID, &Default::default(), 50)
        .await
        .unwrap();
    (state, items)
}

#[tokio::test]
async fn refresh_stores_new_items_once_and_passes_the_agent_grant() {
    let provider = Arc::new(ScriptedInbox {
        items: vec![
            incoming("c1", InboxKind::Comment),
            incoming("c2", InboxKind::Mention),
        ],
        ..ScriptedInbox::default()
    });
    let (state, items) = refreshed(provider.clone()).await;
    assert_eq!(items.len(), 2);
    assert_eq!(
        provider.read_grants.lock().unwrap().as_slice(),
        &[Some("grant.reader".to_owned())]
    );

    let again = refresh_inbox(
        State(state.clone()),
        HeaderMap::new(),
        Json(RefreshInboxBody {
            workspace_id: None,
            account_id: None,
        }),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(again.fetched, 2);
    assert_eq!(again.new, 0);

    let unknown = refresh_inbox(
        State(state),
        HeaderMap::new(),
        Json(RefreshInboxBody {
            workspace_id: None,
            account_id: Some("acc_missing".into()),
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(unknown, ApiError::NotFound(_)), "{unknown}");
}

#[tokio::test]
async fn reply_sends_through_the_provider_then_marks_the_item_replied() {
    let provider = Arc::new(ScriptedInbox {
        items: vec![incoming("c1", InboxKind::Comment)],
        ..ScriptedInbox::default()
    });
    let (state, items) = refreshed(provider.clone()).await;
    let item = &items[0];
    assert!(!item.replied && !item.read);

    let replied = reply_inbox(
        State(state.clone()),
        Path(item.id.clone()),
        grant_headers("grant.replier"),
        Json(ReplyBody {
            text: "Thanks for the note!".into(),
        }),
    )
    .await
    .unwrap()
    .0;
    assert!(replied.replied);
    assert!(replied.read, "a replied item is never left unread");
    assert_eq!(
        provider.replies.lock().unwrap().as_slice(),
        &[SentReply {
            external_id: "c1".into(),
            connected_account_id: Some("ext_me".into()),
            text: "Thanks for the note!".into(),
            grant: Some("grant.replier".into()),
        }]
    );
    let stored = state.store.get_inbox_item(&item.id).await.unwrap().unwrap();
    assert!(stored.replied);
}

#[tokio::test]
async fn reply_failures_are_mapped_and_never_mark_the_item_replied() {
    let provider = Arc::new(ScriptedInbox {
        items: vec![
            incoming("c1", InboxKind::Comment),
            incoming("d1", InboxKind::Dm),
        ],
        fail_reply: true,
        ..ScriptedInbox::default()
    });
    let (state, items) = refreshed(provider.clone()).await;
    let comment = items.iter().find(|i| i.external_id == "c1").unwrap();
    let dm = items.iter().find(|i| i.external_id == "d1").unwrap();
    let reply = |id: String, text: &str| {
        reply_inbox(
            State(state.clone()),
            Path(id),
            HeaderMap::new(),
            Json(ReplyBody { text: text.into() }),
        )
    };

    // Blank text: refused before the provider is called.
    let blank = reply(comment.id.clone(), "   ").await.unwrap_err();
    assert!(matches!(blank, ApiError::BadRequest(_)), "{blank}");
    // A DM reply needs `send_dm`, which this provider lacks.
    let dm_refused = reply(dm.id.clone(), "hi").await.unwrap_err();
    assert!(matches!(dm_refused, ApiError::Conflict(_)), "{dm_refused}");
    assert!(provider.replies.lock().unwrap().is_empty());

    // The platform rejects the send: 502, and the item stays unreplied.
    let upstream = reply(comment.id.clone(), "hi").await.unwrap_err();
    assert!(matches!(upstream, ApiError::Upstream(_)), "{upstream}");
    assert_eq!(provider.replies.lock().unwrap().len(), 1);
    let stored = state
        .store
        .get_inbox_item(&comment.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!stored.replied);

    let missing = reply("inb_missing".into(), "hi").await.unwrap_err();
    assert!(matches!(missing, ApiError::NotFound(_)), "{missing}");
}

// ── Templates, workspace rename, media-import abort ───────────────────────────

fn template_body(text: &str) -> TemplateBody {
    serde_json::from_value(json!({ "text": text, "platform_defaults": { "x": "short" } })).unwrap()
}

#[tokio::test]
async fn template_patch_changes_only_the_given_fields_and_rejects_a_blank_name() {
    let state = state();
    let template = state
        .store
        .create_template(
            DEFAULT_WORKSPACE_ID,
            "Launch",
            &template_body("We shipped!"),
        )
        .await
        .unwrap();
    let patch = |value: serde_json::Value| {
        patch_template(
            State(state.clone()),
            Path(template.id.clone()),
            Json(serde_json::from_value::<PatchTemplateBody>(value).unwrap()),
        )
    };

    let renamed = patch(json!({ "name": "  Launch v2  " })).await.unwrap().0;
    assert_eq!(renamed.name, "Launch v2");
    assert_eq!(renamed.body.text, "We shipped!");
    assert_eq!(
        renamed.body.platform_defaults.get("x").map(String::as_str),
        Some("short")
    );

    let rewritten = patch(json!({ "body": { "text": "New copy" } }))
        .await
        .unwrap()
        .0;
    assert_eq!(rewritten.name, "Launch v2");
    assert_eq!(rewritten.body.text, "New copy");
    assert!(rewritten.body.platform_defaults.is_empty());

    let blank = patch(json!({ "name": "   " })).await.unwrap_err();
    assert!(matches!(blank, ApiError::BadRequest(_)), "{blank}");
    let stored = state
        .store
        .get_template(&template.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.name, "Launch v2");

    let missing = patch_template(
        State(state.clone()),
        Path("tpl_missing".into()),
        Json(serde_json::from_value::<PatchTemplateBody>(json!({ "name": "x" })).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, ApiError::NotFound(_)), "{missing}");
}

#[tokio::test]
async fn using_a_template_creates_a_draft_in_its_workspace_and_leaves_it_unchanged() {
    let state = state();
    let brand = state.store.create_workspace("Brand B").await.unwrap();
    let template = state
        .store
        .create_template(&brand.id, "Promo", &template_body("Sale ends Friday"))
        .await
        .unwrap();

    let draft = use_template(State(state.clone()), Path(template.id.clone()))
        .await
        .unwrap()
        .0;
    assert_eq!(draft.workspace_id, brand.id);
    assert_eq!(draft.body.text, "Sale ends Friday");
    assert_eq!(draft.body.segments[0].text, "Sale ends Friday");
    let stored = state.store.get_draft(&draft.id).await.unwrap().unwrap();
    assert_eq!(stored.body.text, "Sale ends Friday");
    assert!(state
        .store
        .list_drafts(DEFAULT_WORKSPACE_ID)
        .await
        .unwrap()
        .is_empty());

    // Using it twice makes two drafts; the template itself is untouched.
    let second = use_template(State(state.clone()), Path(template.id.clone()))
        .await
        .unwrap()
        .0;
    assert_ne!(second.id, draft.id);
    let unchanged = state
        .store
        .get_template(&template.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.name, "Promo");
    assert_eq!(unchanged.body, template.body);

    let missing = use_template(State(state), Path("tpl_missing".into()))
        .await
        .unwrap_err();
    assert!(matches!(missing, ApiError::NotFound(_)), "{missing}");
}

#[tokio::test]
async fn workspace_rename_persists_and_validates() {
    let state = state();
    let workspace = state.store.create_workspace("Old name").await.unwrap();
    let rename = |id: String, name: &str| {
        patch_workspace(
            State(state.clone()),
            Path(id),
            Json(NameBody { name: name.into() }),
        )
    };

    let _ok = rename(workspace.id.clone(), "  New name ").await.unwrap();
    let stored = state
        .store
        .get_workspace(&workspace.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.name, "New name");

    let blank = rename(workspace.id.clone(), "  ").await.unwrap_err();
    assert!(matches!(blank, ApiError::BadRequest(_)), "{blank}");
    let missing = rename("ws_missing".into(), "Name").await.unwrap_err();
    assert!(matches!(missing, ApiError::NotFound(_)), "{missing}");
    assert_eq!(
        state
            .store
            .get_workspace(&workspace.id)
            .await
            .unwrap()
            .unwrap()
            .name,
        "New name"
    );
}

fn partial_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(root.join("imports").join(".partial"))
        .map(|dir| dir.map(|entry| entry.unwrap().path()).collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn aborting_an_import_deletes_its_partial_file_and_only_in_its_workspace() {
    use base64::Engine;
    let root = std::env::temp_dir().join(format!("ryu-social-abort-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let state = state().with_media_root(root.clone());

    let started = start_media_import(
        State(state.clone()),
        Json(crate::media_import::StartImportBody {
            workspace_id: None,
            name: "clip.mp4".into(),
            mime_type: "video/mp4".into(),
            size: 8,
        }),
    )
    .await
    .unwrap()
    .0;
    let id = started["id"].as_str().unwrap().to_owned();
    let _received = append_media_import(
        State(state.clone()),
        Path(id.clone()),
        Json(crate::media_import::ImportChunkBody {
            workspace_id: None,
            offset: 0,
            data: base64::engine::general_purpose::STANDARD.encode(b"half"),
        }),
    )
    .await
    .unwrap();
    assert_eq!(partial_files(&root).len(), 1);

    let abort = |workspace: Option<&str>| {
        abort_media_import(
            State(state.clone()),
            Path(id.clone()),
            Query(ScopeQuery {
                workspace_id: workspace.map(str::to_owned),
                limit: None,
            }),
        )
    };

    // Another workspace cannot cancel this session or see that it exists.
    let foreign = abort(Some("ws_other")).await.unwrap_err();
    assert!(matches!(foreign, ApiError::NotFound(_)), "{foreign}");
    assert_eq!(partial_files(&root).len(), 1);

    let _ok = abort(None).await.unwrap();
    assert!(
        partial_files(&root).is_empty(),
        "the partial upload is deleted"
    );

    // The session is gone: neither a second abort nor a late chunk finds it.
    let again = abort(None).await.unwrap_err();
    assert!(matches!(again, ApiError::NotFound(_)), "{again}");
    let late = append_media_import(
        State(state.clone()),
        Path(id.clone()),
        Json(crate::media_import::ImportChunkBody {
            workspace_id: None,
            offset: 4,
            data: base64::engine::general_purpose::STANDARD.encode(b"rest"),
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(late, ApiError::NotFound(_)), "{late}");
    std::fs::remove_dir_all(root).unwrap();
}
