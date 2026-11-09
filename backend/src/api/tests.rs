use super::*;
use crate::state::Config;
use crate::store::SocialStore;

fn state() -> AppState {
    AppState::new(
        SocialStore::open_in_memory_sealed().expect("in-memory store"),
        Config::from_env(0),
    )
}

/// Building the router is what validates every path pattern. Two routes that
/// conflict panic HERE, at `Router::new().route(...)`, not at `cargo check` —
/// which is exactly why this test exists rather than relying on the build.
#[test]
fn the_router_builds_with_every_route_registered() {
    let _router = routes(state());
}

#[test]
fn scope_query_defaults_to_the_seeded_workspace_and_clamps_limits() {
    let empty = ScopeQuery {
        workspace_id: None,
        limit: None,
    };
    assert_eq!(empty.workspace(), DEFAULT_WORKSPACE_ID);
    assert_eq!(empty.limit(), DEFAULT_LIMIT);

    let blank = ScopeQuery {
        workspace_id: Some("   ".into()),
        limit: Some(0),
    };
    // A blank param is the same as an absent one — a UI that always sends the
    // key must not end up scoped to a workspace called "".
    assert_eq!(blank.workspace(), DEFAULT_WORKSPACE_ID);
    assert_eq!(blank.limit(), 1);

    let huge = ScopeQuery {
        workspace_id: Some("ws_1".into()),
        limit: Some(100_000),
    };
    assert_eq!(huge.workspace(), "ws_1");
    assert_eq!(huge.limit(), MAX_LIMIT);
}

/// The gate must authorize the workspace the handler will actually use: reads
/// scope from the percent-decoded query, writes from the body, and a request
/// naming two different workspaces is refused instead of split between them.
#[test]
fn the_gate_scopes_each_request_the_way_its_handler_does() {
    let scope = |method: Method, uri: &str, body: Option<Value>| {
        scope_workspace(&method, &uri.parse::<Uri>().unwrap(), body.as_ref())
    };

    // A write takes its scope from the body, which is what `create_*` reads.
    assert_eq!(
        scope(
            Method::POST,
            "/posts",
            Some(json!({ "workspace_id": "ws_b" }))
        )
        .unwrap(),
        "ws_b"
    );
    // A query-only scope on a write does not reach the handler, so it must not
    // be what the gate authorizes either.
    assert_eq!(
        scope(Method::POST, "/posts?workspace_id=ws_a", Some(json!({}))).unwrap(),
        DEFAULT_WORKSPACE_ID
    );
    assert!(scope(
        Method::POST,
        "/posts?workspace_id=ws_a",
        Some(json!({ "workspace_id": "ws_b" }))
    )
    .is_err());

    // A read decodes the key exactly as `Query<ScopeQuery>` does, and a body on
    // a GET never picks the scope.
    assert_eq!(
        scope(Method::GET, "/posts?workspace%5Fid=ws_b", None).unwrap(),
        "ws_b"
    );
    assert!(scope(
        Method::GET,
        "/posts?workspace%5Fid=ws_b",
        Some(json!({ "workspace_id": "ws_a" }))
    )
    .is_err());
    assert_eq!(
        scope(
            Method::GET,
            "/posts",
            Some(json!({ "workspace_id": "ws_a" }))
        )
        .unwrap(),
        DEFAULT_WORKSPACE_ID
    );
    assert!(scope(
        Method::GET,
        "/posts?workspace_id=ws_a&workspace%5Fid=ws_b",
        None
    )
    .is_err());
    assert_eq!(
        scope(Method::GET, "/posts?workspace_id=%20", None).unwrap(),
        DEFAULT_WORKSPACE_ID
    );
}

#[test]
fn every_declared_post_status_string_parses_back() {
    // Guards the `?status=` filter: a value in KNOWN_POST_STATUSES that does not
    // round-trip would be silently coerced to `failed` by `from_db`.
    for s in KNOWN_POST_STATUSES {
        assert_eq!(PostStatus::from_db(s).as_str(), *s);
    }
}

fn grant_headers(grant: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ryu_app_events::HDR_AGENT_GRANT, grant.parse().unwrap());
    headers
}

/// An agent's post keeps the Core-signed grant Core stamped on its request,
/// so the publish runner can hand it back later; the latest actor owns the
/// publish, so a person's action clears it, and an agent's draft edit marks
/// the posts that publish that draft.
#[tokio::test]
async fn posts_remember_the_agent_that_last_acted_on_them() {
    let state = state();
    let account = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@me", None)
        .await
        .unwrap();
    let new_post = |headers: HeaderMap| {
        create_post(
            State(state.clone()),
            headers,
            Json(CreatePostBody {
                workspace_id: None,
                draft_id: None,
                body: Some(DraftBody::empty()),
                scheduled_for: Some(i64::MAX / 2),
                account_ids: vec![account.id.clone()],
                variants: BTreeMap::new(),
            }),
        )
    };

    let by_agent = new_post(grant_headers("grant.agent")).await.unwrap().0;
    let by_person = new_post(HeaderMap::new()).await.unwrap().0;
    let grant = |id: String| {
        let store = state.store.clone();
        async move { store.post_agent_grant(&id).await.unwrap() }
    };
    assert_eq!(
        grant(by_agent.id.clone()).await.as_deref(),
        Some("grant.agent")
    );
    assert_eq!(grant(by_person.id.clone()).await, None);

    // A person moving the agent's post takes it over.
    let _moved = reschedule(state.clone(), by_agent.id.clone(), i64::MAX / 3, None)
        .await
        .unwrap();
    assert_eq!(grant(by_agent.id.clone()).await, None);

    // An agent publishing the person's post now owns that publish.
    let _queued = publish_now(
        State(state.clone()),
        Path(by_person.id.clone()),
        grant_headers("grant.other"),
    )
    .await
    .unwrap();
    assert_eq!(
        grant(by_person.id.clone()).await.as_deref(),
        Some("grant.other")
    );

    // An agent editing the text of a person's scheduled post marks it too.
    let draft_id = by_agent
        .draft_id
        .clone()
        .expect("inline body becomes a draft");
    let _edited = patch_draft(
        State(state.clone()),
        Path(draft_id),
        grant_headers("grant.editor"),
        Json(DraftBodyPayload {
            workspace_id: None,
            body: DraftBody::empty(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(grant(by_agent.id).await.as_deref(), Some("grant.editor"));
}

/// A fan-out naming the same account twice must produce ONE leg.
///
/// Two legs is not a cosmetic duplicate: the runner's durable already-published
/// guard is keyed on the target row's id, so the second leg cannot see that the
/// first already went out, and both calls carry the same idempotency key — which
/// the Composio broker is not documented to honour. One request, the post live
/// twice on the same account.
#[tokio::test]
async fn create_post_collapses_a_repeated_account_into_one_target() {
    let state = state();
    let account = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@me", None)
        .await
        .unwrap();

    let post = create_post(
        State(state.clone()),
        HeaderMap::new(),
        Json(CreatePostBody {
            workspace_id: None,
            draft_id: None,
            body: Some(DraftBody::empty()),
            scheduled_for: Some(9_000),
            account_ids: vec![account.id.clone(), account.id.clone(), account.id],
            variants: BTreeMap::new(),
        }),
    )
    .await
    .expect("a repeated account is a well-formed request, not an error")
    .0;

    assert_eq!(post.targets.len(), 1);
}

/// Order is user-visible (it is the response's target order, and `variants` is
/// keyed by account id), so the dedupe must keep FIRST occurrence rather than
/// whatever a set iterates in.
#[tokio::test]
async fn create_post_dedupe_keeps_first_occurrence_order() {
    let state = state();
    let first = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@zzz", None)
        .await
        .unwrap();
    let second = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@aaa", None)
        .await
        .unwrap();

    let post = create_post(
        State(state.clone()),
        HeaderMap::new(),
        Json(CreatePostBody {
            workspace_id: None,
            draft_id: None,
            body: Some(DraftBody::empty()),
            scheduled_for: Some(9_000),
            account_ids: vec![
                second.id.clone(),
                first.id.clone(),
                second.id.clone(),
                first.id.clone(),
            ],
            variants: BTreeMap::new(),
        }),
    )
    .await
    .unwrap()
    .0;

    let got: Vec<&str> = post
        .targets
        .iter()
        .map(|t| t.social_account_id.as_str())
        .collect();
    assert_eq!(got, vec![second.id.as_str(), first.id.as_str()]);
}

/// One over-limit body, validated twice against the same workspace with only
/// `enforce_platform_limits` flipped. The toggle is a real Settings control, so a
/// regression here is a switch that silently does nothing — the exact failure the
/// setting's doc block warns about.
///
/// `reason` must survive the flip: warn-only means the composer still SHOWS the
/// violation (as advice), it just does not block on it. A fix that cleared
/// `reason` alongside `ok` would pass a naive assertion and blind the composer.
#[tokio::test]
async fn validate_blocks_or_merely_warns_per_the_workspace_setting() {
    let state = state();
    let over_limit = vec![PostSegment {
        text: "x".repeat(400),
        media: Vec::new(),
    }];
    let body = || ValidateBody {
        platforms: vec!["x".to_owned()],
        segments: over_limit.clone(),
        workspace_id: None,
    };

    let mut settings = state
        .store
        .get_settings(DEFAULT_WORKSPACE_ID)
        .await
        .unwrap();
    assert!(
        settings.enforce_platform_limits,
        "a fresh workspace must enforce by default"
    );

    let strict = validate_post(State(state.clone()), Json(body()))
        .await
        .unwrap()
        .0;
    assert_eq!(strict["ok"], json!(false), "enforcing must block");
    assert_eq!(strict["enforced"], json!(true));
    assert!(strict["results"][0]["reason"].is_string());
    assert_eq!(strict["results"][0]["ok"], json!(false));

    settings.enforce_platform_limits = false;
    state
        .store
        .put_settings(DEFAULT_WORKSPACE_ID, &settings)
        .await
        .unwrap();

    let warn = validate_post(State(state.clone()), Json(body()))
        .await
        .unwrap()
        .0;
    assert_eq!(warn["ok"], json!(true), "warn-only must not block");
    assert_eq!(warn["enforced"], json!(false));
    assert_eq!(warn["results"][0]["ok"], json!(true));
    assert!(
        warn["results"][0]["reason"].is_string(),
        "the violation must still be reported so the composer can warn"
    );
}

mod composio;
mod media;
mod routes;
