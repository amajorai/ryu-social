//! The HTTP surface. Every path here is RELATIVE to the mount — `main` nests this
//! router at `/api/social`, and Core's ext-proxy rewrites the external
//! `/api/ext/@ryu/social/*` onto the same prefix, so the two entry points serve
//! byte-identical paths.
//!
//! Conventions the companion UI depends on, so they are not negotiable per-route:
//!
//! - **Every response is JSON.** Even a delete returns `{"ok": true}` rather than
//!   204, because the manifest's view DSL reads a JSON body and a bodiless response
//!   makes an action indistinguishable from a network failure.
//! - **List routes return `{"<plural>": [...]}`**, never a bare array. The
//!   `sidebar_sections` / `views` source DSL names the row array by key, and a bare
//!   array leaves it guessing.
//! - **Single-entity routes return the entity at the top level**, so a detail view
//!   binds to `{{item.field}}` without an unwrapping step.
//! - **`workspace_id` is a query parameter that defaults**, never a required path
//!   segment. Almost every install has exactly one workspace; forcing it into the
//!   path would make every URL in the UI carry a constant.
//!
//! Handlers here are thin: anything that is pure persistence is implemented inline
//! against [`crate::store`], and anything that leaves the process is one call into
//! the module that owns it (`publish`, `inbox`, `analytics`, `providers`). That
//! split is why this file can be complete while those modules are still stubs.

use std::collections::{BTreeMap, BTreeSet};

use axum::{
    body::{to_bytes, Body},
    extract::{Path, Query, State},
    http::{HeaderMap, Method, Request, StatusCode, Uri},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{ApiError, ApiResult};
use crate::models::*;
use crate::state::AppState;
use crate::store::{InboxFilter, NewTarget};

/// Cap on any single list read.
///
/// A hard ceiling rather than an unbounded query: these lists back a UI, and a
/// workspace with a year of history would otherwise serialize tens of megabytes
/// into a sandboxed iframe on first paint.
const MAX_LIMIT: usize = 500;
const DEFAULT_LIMIT: usize = 200;

const MAX_AUTHORIZATION_BODY_BYTES: usize = 10 * 1024 * 1024;

#[derive(Debug, Clone, Default)]
struct CallerIdentity {
    user_id: Option<String>,
    org_id: Option<String>,
}

fn caller_identity(headers: &HeaderMap) -> CallerIdentity {
    let value = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    CallerIdentity {
        user_id: value("x-ryu-caller-user-id"),
        org_id: value("x-ryu-caller-org-id"),
    }
}

/// The Core-signed grant naming the agent behind this request, if an agent
/// made it. Core strips any caller copy and stamps it only after verifying the
/// agent, so its absence means a person (or an app action) made the request.
/// Opaque here: it is stored with the work and handed back to Core on provider
/// calls, where Core applies that agent's account access.
fn agent_grant(headers: &HeaderMap) -> Option<String> {
    headers
        .get(ryu_app_events::HDR_AGENT_GRANT)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// The workspace a collection-level route's handler scopes itself to.
///
/// This must read the SAME source, decoded the SAME way, as the handler that runs
/// next — otherwise the gate authorizes one workspace and the handler acts on
/// another. Read handlers take `workspace_id` from `Query<..>` (serde_urlencoded,
/// which percent-decodes keys and values); write handlers take it from the JSON
/// body only. Either way a blank value means the default workspace. A request that
/// names two different workspaces (query vs body, or a repeated query key) is
/// ambiguous and refused rather than guessed at.
fn scope_workspace(method: &Method, uri: &Uri, body: Option<&Value>) -> Result<String, ApiError> {
    let Query(pairs) = Query::<Vec<(String, String)>>::try_from_uri(uri)
        .map_err(|_| ApiError::bad_request("malformed query string"))?;
    let mut query_values = pairs
        .into_iter()
        .filter(|(name, _)| name == "workspace_id")
        .map(|(_, value)| value);
    let query_workspace = query_values.next();
    if query_values.next().is_some() {
        return Err(ApiError::bad_request("`workspace_id` given more than once"));
    }
    let query_workspace = query_workspace.filter(|workspace| !workspace.trim().is_empty());
    let body_workspace = body
        .and_then(|body| body.get("workspace_id"))
        .and_then(Value::as_str)
        .filter(|workspace| !workspace.trim().is_empty())
        .map(str::to_owned);
    if let (Some(query), Some(body)) = (&query_workspace, &body_workspace) {
        if query != body {
            return Err(ApiError::bad_request(
                "query and body name different `workspace_id`s",
            ));
        }
    }
    let workspace = if method == Method::GET || method == Method::HEAD {
        query_workspace
    } else {
        body_workspace
    };
    Ok(workspace.unwrap_or_else(|| DEFAULT_WORKSPACE_ID.to_owned()))
}

async fn entity_workspace(state: &AppState, segments: &[&str]) -> anyhow::Result<Option<String>> {
    let Some(id) = segments.get(1).copied() else {
        return Ok(None);
    };
    match segments[0] {
        "accounts" => state.store.workspace_for_account(id).await,
        "drafts" => state.store.workspace_for_draft(id).await,
        "posts" => state.store.workspace_for_post(id).await,
        "history" => state.store.workspace_for_history(id).await,
        "inbox" => state.store.workspace_for_inbox(id).await,
        "templates" => state.store.workspace_for_template(id).await,
        "media" => state.store.workspace_for_media(id).await,
        _ => Ok(None),
    }
}

async fn request_workspace(
    state: &AppState,
    method: &Method,
    uri: &Uri,
    body: Option<&Value>,
) -> anyhow::Result<Result<Option<String>, ApiError>> {
    let segments: Vec<&str> = uri
        .path()
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let Some(resource) = segments.first().copied() else {
        return Ok(Ok(None));
    };
    if resource == "workspaces" {
        return Ok(Ok(segments.get(1).map(|id| (*id).to_owned())));
    }
    if !matches!(
        resource,
        "accounts"
            | "drafts"
            | "posts"
            | "calendar"
            | "queue"
            | "best-times"
            | "history"
            | "inbox"
            | "templates"
            | "media"
            | "activity"
            | "settings"
    ) {
        return Ok(Ok(None));
    }
    if segments.len() > 1 && !(resource == "posts" && segments[1] == "validate") {
        if let Some(workspace) = entity_workspace(state, &segments).await? {
            return Ok(Ok(Some(workspace)));
        }
    }
    Ok(scope_workspace(method, uri, body).map(Some))
}

async fn authorize_request(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let (parts, body) = request.into_parts();
    let body_bytes = match to_bytes(body, MAX_AUTHORIZATION_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let body_json = if body_bytes.is_empty() {
        None
    } else {
        serde_json::from_slice::<Value>(&body_bytes).ok()
    };
    let identity = caller_identity(&parts.headers);
    let workspace =
        match request_workspace(&state, &parts.method, &parts.uri, body_json.as_ref()).await {
            Ok(Ok(workspace)) => workspace,
            Ok(Err(rejection)) => return rejection.into_response(),
            Err(error) => {
                tracing::warn!(error = %error, "social workspace authorization lookup failed");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
    if let Some(workspace) = workspace {
        match state
            .store
            .workspace_accessible(
                &workspace,
                identity.user_id.as_deref(),
                identity.org_id.as_deref(),
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => return ApiError::not_found("workspace").into_response(),
            Err(error) => {
                tracing::warn!(error = %error, "social workspace authorization failed");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }
    next.run(Request::from_parts(parts, Body::from(body_bytes)))
        .await
}

/// Build the router. Paths are relative to `/api/social`; `/health` is deliberately
/// NOT here — it must sit outside the auth gate, so `main` owns it.
pub fn routes(state: AppState) -> Router<()> {
    Router::new()
        // ── Workspaces ──
        .route("/workspaces", get(list_workspaces).post(create_workspace))
        .route(
            "/workspaces/:id",
            get(get_workspace)
                .patch(patch_workspace)
                .delete(delete_workspace),
        )
        // ── Accounts ──
        .route("/accounts", get(list_accounts).post(create_account))
        .route("/accounts/:id", get(get_account).delete(delete_account))
        .route("/accounts/:id/connect", post(connect_account))
        .route("/accounts/:id/capabilities", get(account_capabilities))
        // ── Drafts ──
        .route("/drafts", get(list_drafts).post(create_draft))
        .route(
            "/drafts/:id",
            get(get_draft).patch(patch_draft).delete(delete_draft),
        )
        // ── Scheduled posts ──
        //
        // `/posts/validate` is declared BEFORE `/posts/:id` for readability only —
        // the router gives a static segment priority over a parameter regardless of
        // insertion order, so "validate" can never be captured as an id.
        .route("/posts", get(list_posts).post(create_post))
        .route("/posts/validate", post(validate_post))
        .route(
            "/posts/:id",
            get(get_post).patch(patch_post).delete(delete_post),
        )
        .route("/posts/:id/schedule", post(schedule_post))
        .route("/posts/:id/cancel", post(cancel_post))
        .route("/posts/:id/publish-now", post(publish_now))
        .route("/posts/:id/retry", post(retry_post))
        // ── Projections ──
        .route("/calendar", get(calendar))
        .route("/queue", get(queue))
        // The timing recommender. A dedicated route rather than a field on `/queue`:
        // it costs an `activity_items` read, and the queue view's whole design point
        // is that a freshly-scheduled queue makes zero extra queries. Callers that
        // want "when should I post this" ask for it; callers rendering the queue do
        // not pay for it.
        .route("/best-times", get(best_times))
        // ── History + engagement ──
        .route("/history", get(list_history))
        .route("/history/:id", get(get_history))
        .route("/history/:id/refresh-engagement", post(refresh_engagement))
        // ── Inbox ──
        .route("/inbox", get(list_inbox))
        .route("/inbox/refresh", post(refresh_inbox))
        .route("/inbox/:id/reply", post(reply_inbox))
        .route("/inbox/:id/read", post(read_inbox))
        // ── Templates ──
        .route("/templates", get(list_templates).post(create_template))
        .route(
            "/templates/:id",
            get(get_template)
                .patch(patch_template)
                .delete(delete_template),
        )
        // The Store tab's "Use" action. A sub-path of `/templates/:id` and therefore
        // a SEPARATE `http.routes[]` entry in the manifest — Core's ext-proxy matcher
        // requires an exact segment-count match, so declaring the parent does not
        // admit this child.
        .route("/templates/:id/use", post(use_template))
        // ── Media library ──
        //
        // Not in the original route list, but compose cannot attach anything without
        // it: `media_assets` is how a picked file becomes re-selectable across
        // drafts. Paths are references, never copies — deleting a row never touches
        // the user's file.
        .route("/media", get(list_media).post(create_media))
        .route("/media/:id", axum::routing::delete(delete_media))
        // Upload sessions that copy a file INTO the media root, for apps that
        // reach Outpost only through `social.request` (see `media_import`).
        .route("/media/import", post(start_media_import))
        .route("/media/import/:id/chunks", post(append_media_import))
        .route("/media/import/:id/finish", post(finish_media_import))
        .route(
            "/media/import/:id",
            axum::routing::delete(abort_media_import),
        )
        // ── Read models ──
        .route("/activity", get(list_activity))
        .route("/settings", get(get_settings).patch(patch_settings))
        .route("/node-settings", axum::routing::patch(patch_node_settings))
        .route("/platforms", get(list_platforms))
        .layer(from_fn_with_state(state.clone(), authorize_request))
        .with_state(state)
}

// ── OpenAPI ────────────────────────────────────────────────────────────────────

/// The machine-readable description of this app's HTTP surface, served at
/// `/openapi.json` and imported by Core into agent tools.
///
/// **Paths here are ABSOLUTE and use `{id}`, while the router above is relative and
/// uses `:id`.** That is not a drift to fix: Core's importer reads the spec's own
/// path (which therefore has to carry the `/api/social` mount, because the importer
/// strips the mount back off before matching), and its route matcher writes the
/// placeholder in the `{brace}` form. The router keeps axum's `:colon` form because
/// that is what axum parses. Change either side to match the other and every derived
/// tool is silently dropped — the importer logs a debug line and nothing else.
///
/// **The list is hand-written**, so a new route does NOT become a tool until it is
/// added here. There is no `utoipa-axum` in this workspace to derive it from the
/// router.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        // Reads first, in the order an agent tends to want them.
        list_posts,
        get_post,
        list_drafts,
        get_draft,
        calendar,
        queue,
        best_times,
        list_history,
        get_history,
        list_inbox,
        list_activity,
        list_templates,
        get_template,
        list_accounts,
        get_account,
        account_capabilities,
        list_workspaces,
        get_workspace,
        list_media,
        list_platforms,
        get_settings,
        // Compose + publish.
        create_post,
        validate_post,
        patch_post,
        schedule_post,
        cancel_post,
        publish_now,
        retry_post,
        delete_post,
        create_draft,
        patch_draft,
        delete_draft,
        reply_inbox,
        read_inbox,
        refresh_inbox,
        refresh_engagement,
        use_template,
        // Library + setup.
        create_workspace,
        patch_workspace,
        delete_workspace,
        create_account,
        connect_account,
        delete_account,
        create_template,
        patch_template,
        delete_template,
        create_media,
        delete_media,
        start_media_import,
        append_media_import,
        finish_media_import,
        abort_media_import,
    ),
    components(schemas(
        NameBody,
        CreateAccountBody,
        DraftBodyPayload,
        DraftBody,
        PostSegment,
        MediaRef,
        CreatePostBody,
        RescheduleBody,
        ValidateBody,
        RefreshInboxBody,
        ReplyBody,
        ReadBody,
        CreateTemplateBody,
        PatchTemplateBody,
        TemplateBody,
        CreateMediaBody,
        crate::media_import::StartImportBody,
        crate::media_import::ImportChunkBody,
        crate::media_import::FinishImportBody,
    ))
)]
struct SocialApiDoc;

/// The served document. `main` mounts this INSIDE the shared-secret gate at the
/// server root, which is the first URL Core's fetcher tries.
pub fn openapi() -> utoipa::openapi::OpenApi {
    <SocialApiDoc as utoipa::OpenApi>::openapi()
}

// ── Shared query shapes ────────────────────────────────────────────────────────

/// The workspace scope every list route accepts.
#[derive(Debug, Deserialize)]
pub struct ScopeQuery {
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl ScopeQuery {
    fn workspace(&self) -> &str {
        self.workspace_id
            .as_deref()
            .filter(|w| !w.trim().is_empty())
            .unwrap_or(DEFAULT_WORKSPACE_ID)
    }

    fn limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }
}

/// Turn a store `bool` "did anything change" into a 404, so a caller can tell a
/// missing row from a successful no-op.
fn require_hit(changed: bool, what: &str) -> ApiResult<()> {
    if changed {
        Ok(())
    } else {
        Err(ApiError::not_found(what))
    }
}

// ── Workspaces ─────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/workspaces",
    tag = "Social",
    summary = "list the social workspaces (brands/clients) on this node.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_workspaces(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let identity = caller_identity(&headers);
    let workspaces = state
        .store
        .list_workspaces_for_caller(identity.user_id.as_deref(), identity.org_id.as_deref())
        .await?;
    Ok(Json(json!({ "workspaces": workspaces })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct NameBody {
    /// The workspace's display name. Required and non-blank.
    name: String,
}

#[utoipa::path(
    post,
    path = "/api/social/workspaces",
    tag = "Social",
    summary = "create a social workspace.",
    request_body = NameBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn create_workspace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<NameBody>,
) -> ApiResult<Json<Workspace>> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("a workspace needs a name"));
    }
    let identity = caller_identity(&headers);
    Ok(Json(
        state
            .store
            .create_workspace_for_caller(
                &body.name,
                identity.user_id.as_deref(),
                identity.org_id.as_deref(),
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/social/workspaces/{id}",
    tag = "Social",
    summary = "read one social workspace.",
    params(("id" = String, Path, description = "workspace id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Workspace>> {
    state
        .store
        .get_workspace(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("workspace"))
}

#[utoipa::path(
    patch,
    path = "/api/social/workspaces/{id}",
    tag = "Social",
    summary = "rename a social workspace.",
    params(("id" = String, Path, description = "workspace id")),
    request_body = NameBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn patch_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<NameBody>,
) -> ApiResult<Json<Value>> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("a workspace needs a name"));
    }
    require_hit(
        state.store.rename_workspace(&id, &body.name).await?,
        "workspace",
    )?;
    Ok(Json(json!({ "ok": true })))
}

#[utoipa::path(
    delete,
    path = "/api/social/workspaces/{id}",
    tag = "Social",
    summary = "delete a social workspace and everything filed under it.",
    params(("id" = String, Path, description = "workspace id; the seeded default cannot be deleted")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_workspace(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    // The seeded workspace is structural, not user data. Checked HERE rather than
    // by mapping every store error to 409 — that would report a genuine SQL failure
    // as "not deletable" and send whoever debugs it down the wrong path.
    if id == DEFAULT_WORKSPACE_ID {
        return Err(ApiError::conflict(
            "the default workspace cannot be deleted",
        ));
    }
    require_hit(state.store.delete_workspace(&id).await?, "workspace")?;
    Ok(Json(json!({ "ok": true })))
}

// ── Accounts ───────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/accounts",
    tag = "Social",
    summary = "list the connected social accounts a post can be published to.",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_accounts(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let accounts = state.store.list_accounts(scope.workspace()).await?;
    Ok(Json(json!({ "accounts": accounts })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreateAccountBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    workspace_id: Option<String>,
    /// One of: x, instagram, tiktok, youtube, linkedin, reddit, facebook, bluesky,
    /// threads. An unknown value is rejected.
    platform: String,
    /// How the account is shown in pickers, e.g. `@acme`.
    account_label: String,
    /// The platform's own id for the account, when it is already known. Normally left
    /// out — the connect handshake fills it in.
    #[serde(default)]
    external_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/social/accounts",
    tag = "Social",
    summary = "register a social account in a workspace (does not connect it yet).",
    request_body = CreateAccountBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn create_account(
    State(state): State<AppState>,
    Json(body): Json<CreateAccountBody>,
) -> ApiResult<Json<SocialAccount>> {
    // Strict parse here, unlike the tolerant lookups elsewhere: this is the one
    // place a NEW platform string enters the database, and accepting an unknown one
    // would silently create an account nothing can ever publish to.
    let platform = Platform::parse(&body.platform)
        .ok_or_else(|| ApiError::bad_request(format!("unknown platform \"{}\"", body.platform)))?;
    if body.account_label.trim().is_empty() {
        return Err(ApiError::bad_request("an account needs a label"));
    }
    let workspace = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    Ok(Json(
        state
            .store
            .create_account(
                workspace,
                platform,
                &body.account_label,
                body.external_id.as_deref(),
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/social/accounts/{id}",
    tag = "Social",
    summary = "read one social account.",
    params(("id" = String, Path, description = "account id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_account(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<SocialAccount>> {
    state
        .store
        .get_account(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("account"))
}

#[utoipa::path(
    delete,
    path = "/api/social/accounts/{id}",
    tag = "Social",
    summary = "remove a social account from its workspace.",
    params(("id" = String, Path, description = "account id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_account(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    // Only the local row goes: no provider is called, so there is no agent
    // grant to hand back.
    require_hit(state.store.delete_account(&id).await?, "account")?;
    Ok(Json(json!({ "ok": true })))
}

/// Run the provider's connect handshake and record the outcome.
///
/// Whether this opens an OAuth flow, validates an app password, or is a no-op is
/// entirely the provider's business — this handler only persists the answer.
#[utoipa::path(
    post,
    path = "/api/social/accounts/{id}/connect",
    tag = "Social",
    summary = "run the platform's connect handshake for an account and record the result.",
    params(("id" = String, Path, description = "account id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn connect_account(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<SocialAccount>> {
    let account = state
        .store
        .get_account(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("account"))?;
    let provider = state
        .providers
        .provider_for_operation(
            &crate::providers::ProviderAccount {
                id: account.id.clone(),
                platform: account.platform,
                label: Some(account.account_label.clone()),
                external_id: account.external_id.clone(),
            },
            crate::providers::ProviderOperation::Connect,
            None,
        )
        .await;
    let external_id = provider
        .connect(
            &crate::providers::ProviderAccount {
                id: account.id.clone(),
                platform: account.platform,
                label: Some(account.account_label.clone()),
                external_id: account.external_id.clone(),
            },
            agent_grant(&headers).as_deref(),
        )
        .await
        // A failed handshake is an upstream/config problem, not an internal fault —
        // 502 keeps it out of the "this app is broken" bucket.
        .map_err(|e| ApiError::upstream(e.to_string()))?;
    state
        .store
        .set_account_connection(&id, true, external_id.as_deref())
        .await?;
    state
        .store
        .get_account(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("account"))
}

#[utoipa::path(
    get,
    path = "/api/social/accounts/{id}/capabilities",
    tag = "Social",
    summary = "what an account's platform supports (threads, media, limits) before composing for it.",
    params(("id" = String, Path, description = "account id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn account_capabilities(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let account = state
        .store
        .get_account(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("account"))?;
    let capabilities = state.providers.capabilities_for(account.platform).await;
    Ok(Json(json!({
        "account_id": account.id,
        "platform": account.platform,
        "capabilities": capabilities,
        "limits": limits_for(account.platform),
    })))
}

// ── Drafts ─────────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/drafts",
    tag = "Social",
    summary = "list unscheduled post drafts in a workspace.",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_drafts(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let drafts = state.store.list_drafts(scope.workspace()).await?;
    Ok(Json(json!({ "drafts": drafts })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct DraftBodyPayload {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    workspace_id: Option<String>,
    /// The post content.
    #[serde(default)]
    body: DraftBody,
}

#[utoipa::path(
    post,
    path = "/api/social/drafts",
    tag = "Social",
    summary = "save post content as a draft, without scheduling it.",
    request_body = DraftBodyPayload,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn create_draft(
    State(state): State<AppState>,
    Json(payload): Json<DraftBodyPayload>,
) -> ApiResult<Json<Draft>> {
    let workspace = payload
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    Ok(Json(
        state.store.create_draft(workspace, &payload.body).await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/social/drafts/{id}",
    tag = "Social",
    summary = "read one draft's full content.",
    params(("id" = String, Path, description = "draft id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_draft(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Draft>> {
    state
        .store
        .get_draft(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("draft"))
}

#[utoipa::path(
    patch,
    path = "/api/social/drafts/{id}",
    tag = "Social",
    summary = "rewrite a draft's content.",
    params(("id" = String, Path, description = "draft id")),
    request_body = DraftBodyPayload,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn patch_draft(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<DraftBodyPayload>,
) -> ApiResult<Json<Draft>> {
    // Text an agent wrote goes out under that agent's account access, even on a
    // post a person scheduled; a person's edit leaves the post's actor alone.
    // Recorded BEFORE the text changes, so a run that starts in between can only
    // publish the old text under the agent's (narrower) access, never the new
    // text without it.
    if let Some(grant) = agent_grant(&headers) {
        state.store.set_draft_posts_agent_grant(&id, &grant).await?;
    }
    require_hit(state.store.update_draft(&id, &payload.body).await?, "draft")?;
    state
        .store
        .get_draft(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("draft"))
}

#[utoipa::path(
    delete,
    path = "/api/social/drafts/{id}",
    tag = "Social",
    summary = "delete a draft.",
    params(("id" = String, Path, description = "draft id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_draft(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_hit(state.store.delete_draft(&id).await?, "draft")?;
    Ok(Json(json!({ "ok": true })))
}

// ── Scheduled posts ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct PostListQuery {
    #[serde(default)]
    workspace_id: Option<String>,
    /// Comma-separated statuses, e.g. `?status=scheduled,due`. Unknown values are
    /// dropped rather than rejected: a filter chip from a newer UI must not 400 an
    /// older sidecar.
    #[serde(default)]
    status: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/social/posts",
    tag = "Social",
    summary = "list scheduled posts, optionally filtered by status.",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("status" = Option<String>, Query, description = "comma-separated: scheduled,due,publishing,published,partial,failed,cancelled"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_posts(
    State(state): State<AppState>,
    Query(q): Query<PostListQuery>,
) -> ApiResult<Json<Value>> {
    let workspace = q
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    let statuses: Vec<PostStatus> = q
        .status
        .as_deref()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                // `from_db` is total, so an unknown chip would silently become
                // `failed`. Filter against the known set first so it is dropped.
                .filter(|s| KNOWN_POST_STATUSES.contains(s))
                .map(PostStatus::from_db)
                .collect()
        })
        .unwrap_or_default();
    let posts = state
        .store
        .list_scheduled_posts(workspace, &statuses)
        .await?;
    Ok(Json(json!({ "posts": posts })))
}

const KNOWN_POST_STATUSES: &[&str] = &[
    "scheduled",
    "due",
    "publishing",
    "published",
    "partial",
    "failed",
    "cancelled",
];

/// Compose + schedule in one call: give either an existing `draft_id` or an inline
/// `body`, plus the accounts to send to.
//
// `//` from here down — this is a documented request body, so a `///` would ship to
// a model. One endpoint rather than "create draft, then schedule" because the
// composer's Publish button is one user action and a half-completed pair would leave
// an orphan draft. An inline body creates a draft first, so the content stays
// addressable and editable afterwards.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreatePostBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    workspace_id: Option<String>,
    /// Schedule an existing draft. Give this OR `body`, not both.
    #[serde(default)]
    draft_id: Option<String>,
    /// Inline content. Used only when `draft_id` is absent.
    // `schema(inline)` because a bare `Option<DraftBody>` renders as a nullable
    // wrapper around a `$ref`, which buries the real fields (text/segments/media) a
    // caller has to fill in — see the same treatment on `PatchTemplateBody::body`.
    #[schema(inline)]
    #[serde(default)]
    body: Option<DraftBody>,
    /// Epoch millis. Absent means NOW — "post now" is not a separate concept, it is
    /// a schedule whose time has already arrived.
    #[serde(default)]
    scheduled_for: Option<i64>,
    /// The accounts to fan out to. At least one is required.
    account_ids: Vec<String>,
    /// Optional per-account content overrides, keyed by account id. A full draft
    /// body, so an override keeps that target's media and thread structure.
    #[serde(default)]
    variants: BTreeMap<String, DraftBody>,
}

#[utoipa::path(
    post,
    path = "/api/social/posts",
    tag = "Social",
    summary = "schedule a post to one or more accounts (omit scheduled_for to post now).",
    request_body = CreatePostBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn create_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreatePostBody>,
) -> ApiResult<Json<ScheduledPost>> {
    let workspace = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID)
        .to_string();
    if body.account_ids.is_empty() {
        return Err(ApiError::bad_request(
            "a scheduled post needs at least one account",
        ));
    }

    // Collapse repeats BEFORE resolving. A fan-out naming the same account twice is
    // one target, not two: the runner's already-published guard is keyed on the
    // TARGET row's id, so a sibling row for the same account is invisible to it and
    // publishes the post to that account a second time. The unique index added in
    // `V2_DDL` is the floor under this; deduping here is what turns the collision
    // into a clean 200 instead of a 500 from a constraint violation.
    //
    // First-occurrence order is preserved (a `Vec` for order, a `BTreeSet` only to
    // remember what has been seen): the target order shows up in the response, and
    // `variants` is keyed by account id, so reordering would be user-visible.
    let mut seen = BTreeSet::new();
    let mut account_ids = Vec::with_capacity(body.account_ids.len());
    for account_id in &body.account_ids {
        if seen.insert(account_id.as_str()) {
            account_ids.push(account_id.as_str());
        }
    }

    // Resolve every account BEFORE writing anything: a fan-out that references a
    // deleted account should fail the whole request, not create a post with a leg
    // that can never publish.
    let mut targets = Vec::with_capacity(account_ids.len());
    for account_id in account_ids {
        let account = state
            .store
            .get_account(account_id)
            .await?
            .ok_or_else(|| ApiError::bad_request(format!("unknown account {account_id}")))?;
        if account.workspace_id != workspace {
            return Err(ApiError::bad_request(format!(
                "account {account_id} belongs to another workspace"
            )));
        }
        targets.push(NewTarget {
            social_account_id: account.id,
            platform: account.platform,
            variant_body: body.variants.get(account_id).cloned(),
        });
    }

    let draft_id = match (&body.draft_id, &body.body) {
        (Some(id), _) => {
            // Validate it exists and is in this workspace, so a typo does not
            // produce a post whose content resolution silently fails at publish.
            let draft = state
                .store
                .get_draft(id)
                .await?
                .ok_or_else(|| ApiError::bad_request(format!("unknown draft {id}")))?;
            if draft.workspace_id != workspace {
                return Err(ApiError::bad_request("draft belongs to another workspace"));
            }
            Some(draft.id)
        }
        (None, Some(inline)) => Some(state.store.create_draft(&workspace, inline).await?.id),
        (None, None) => {
            return Err(ApiError::bad_request(
                "a scheduled post needs either a draft_id or an inline body",
            ))
        }
    };

    let scheduled_for = body.scheduled_for.unwrap_or_else(now_ms);
    let post = state
        .store
        .create_scheduled_post_for_agent(
            &workspace,
            draft_id.as_deref(),
            scheduled_for,
            &targets,
            agent_grant(&headers).as_deref(),
        )
        .await
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    // Best-effort, and deliberately after the commit: a hook subscriber must never
    // be able to prevent a post from being scheduled, and an emit that failed is not
    // a reason to fail the request.
    state
        .events
        .emit(
            crate::state::EVENT_POST_SCHEDULED,
            serde_json::to_value(&post).unwrap_or_else(|_| json!({ "id": post.id })),
        )
        .await;

    Ok(Json(post))
}

#[utoipa::path(
    get,
    path = "/api/social/posts/{id}",
    tag = "Social",
    summary = "read one scheduled post with its per-account targets and statuses.",
    params(("id" = String, Path, description = "scheduled-post id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ScheduledPost>> {
    state
        .store
        .get_scheduled_post(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("post"))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct RescheduleBody {
    /// The new send time, epoch millis UTC.
    scheduled_for: i64,
}

/// Move a post's time. Guarded to `scheduled` in the store, so a post the sweep
/// already claimed reports 409 rather than racing the runner.
#[utoipa::path(
    patch,
    path = "/api/social/posts/{id}",
    tag = "Social",
    summary = "move a scheduled post to a different time.",
    params(("id" = String, Path, description = "scheduled-post id; must still be `scheduled`")),
    request_body = RescheduleBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn patch_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<RescheduleBody>,
) -> ApiResult<Json<ScheduledPost>> {
    reschedule(state, id, body.scheduled_for, agent_grant(&headers)).await
}

/// The explicit verb for the same transition. Both exist because the UI uses PATCH
/// from a detail form and POST from a drag on the calendar; they must not diverge,
/// so they share one implementation.
#[utoipa::path(
    post,
    path = "/api/social/posts/{id}/schedule",
    tag = "Social",
    summary = "set a scheduled post's send time (same transition as PATCH /posts/{id}).",
    params(("id" = String, Path, description = "scheduled-post id; must still be `scheduled`")),
    request_body = RescheduleBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn schedule_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<RescheduleBody>,
) -> ApiResult<Json<ScheduledPost>> {
    reschedule(state, id, body.scheduled_for, agent_grant(&headers)).await
}

async fn reschedule(
    state: AppState,
    id: String,
    scheduled_for: i64,
    grant: Option<String>,
) -> ApiResult<Json<ScheduledPost>> {
    let post = state
        .store
        .get_scheduled_post(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("post"))?;
    if !state
        .store
        .reschedule_post(&id, scheduled_for, grant.as_deref())
        .await?
    {
        return Err(ApiError::conflict(format!(
            "a {} post can no longer be rescheduled",
            post.status.as_str()
        )));
    }
    state
        .store
        .get_scheduled_post(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("post"))
}

#[utoipa::path(
    post,
    path = "/api/social/posts/{id}/cancel",
    tag = "Social",
    summary = "cancel a scheduled post before it goes out.",
    params(("id" = String, Path, description = "scheduled-post id; 409 once publishing has begun")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn cancel_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ScheduledPost>> {
    let post = state
        .store
        .get_scheduled_post(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("post"))?;
    if !state.store.cancel_post(&id).await? {
        // Publishing has already begun: cancelling locally would make our state lie
        // about what is live on the platform.
        return Err(ApiError::conflict(format!(
            "a {} post can no longer be cancelled",
            post.status.as_str()
        )));
    }
    state
        .store
        .get_scheduled_post(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("post"))
}

#[utoipa::path(
    post,
    path = "/api/social/posts/{id}/publish-now",
    tag = "Social",
    summary = "publish a scheduled post immediately instead of waiting for its time.",
    params(("id" = String, Path, description = "scheduled-post id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn publish_now(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<ScheduledPost>> {
    Ok(Json(
        crate::publish::queue_now(&state, &id, agent_grant(&headers).as_deref()).await?,
    ))
}

#[utoipa::path(
    post,
    path = "/api/social/posts/{id}/retry",
    tag = "Social",
    summary = "re-run the failed legs of a post that did not fully publish.",
    params(("id" = String, Path, description = "scheduled-post id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn retry_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<ScheduledPost>> {
    Ok(Json(
        crate::publish::queue_retry(&state, &id, agent_grant(&headers).as_deref()).await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/social/posts/{id}",
    tag = "Social",
    summary = "delete a scheduled post (does not retract anything already live).",
    params(("id" = String, Path, description = "scheduled-post id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_hit(state.store.delete_scheduled_post(&id).await?, "post")?;
    Ok(Json(json!({ "ok": true })))
}

// ── Compose validation ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct ValidateBody {
    /// Platform keys to check against, e.g. `["x", "bluesky"]`. An unknown key is
    /// checked against generous default limits rather than rejected.
    platforms: Vec<String>,
    /// The post content to measure, one entry per thread segment.
    #[serde(default)]
    segments: Vec<PostSegment>,
    /// Scope; omit for the default workspace.
    // The workspace is what decides whether an over-limit result BLOCKS or merely
    // warns (`enforce_platform_limits`). A settings read that fails falls back to
    // enforcing — the stricter side, so a storage hiccup can never silently unblock
    // a bad post.
    #[serde(default)]
    workspace_id: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct ValidationResult {
    platform: String,
    label: String,
    /// Whether the composer may proceed. Equals `reason.is_none()` while enforcement
    /// is on; always true while it is off, where `reason` degrades to advice.
    ok: bool,
    /// The FIRST limit violation, or null. Populated regardless of enforcement so the
    /// composer can warn even when it is not blocking. First-only because the composer
    /// shows one inline message per platform chip and a user fixes one thing at a time.
    reason: Option<String>,
    limits: PlatformLimits,
    /// Character count of segment 0, so the composer can render its counter from the
    /// same numbers the check used instead of re-deriving them.
    chars: usize,
}

/// Per-platform limit check for a compose payload.
///
/// The limit figures are public estimates, not a contract, so the workspace's
/// `enforce_platform_limits` decides whether a violation blocks the schedule or is
/// shown as a warning the user may overrule. That choice lives HERE and only here:
/// [`crate::publish`] re-checks unconditionally at publish time, because by then the
/// platform is going to reject the post anyway and failing locally costs no API call
/// and no half-published thread.
#[utoipa::path(
    post,
    path = "/api/social/posts/validate",
    tag = "Social",
    summary = "check draft text against each platform's length and media limits before scheduling.",
    request_body = ValidateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn validate_post(
    State(state): State<AppState>,
    Json(body): Json<ValidateBody>,
) -> ApiResult<Json<Value>> {
    if body.platforms.is_empty() {
        return Err(ApiError::bad_request("no platforms to validate against"));
    }
    let workspace_id = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    let enforced = state
        .store
        .get_settings(workspace_id)
        .await
        .map_or(true, |s| s.enforce_platform_limits);
    let chars = body.segments.first().map_or(0, |s| s.text.chars().count());
    let results: Vec<ValidationResult> = body
        .platforms
        .iter()
        .map(|platform| {
            let reason = validate_segments_for_platform(platform, &body.segments);
            ValidationResult {
                platform: platform.clone(),
                label: label_for_str(platform),
                ok: !enforced || reason.is_none(),
                reason,
                limits: limits_for_str(platform),
                chars,
            }
        })
        .collect();
    let ok = results.iter().all(|r| r.ok);
    Ok(Json(
        json!({ "ok": ok, "enforced": enforced, "results": results }),
    ))
}

// ── Calendar ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RangeQuery {
    #[serde(default)]
    workspace_id: Option<String>,
    /// Epoch millis, inclusive.
    from: i64,
    /// Epoch millis, EXCLUSIVE — so consecutive buckets tile the timeline with no
    /// post appearing in two of them.
    to: i64,
}

/// What kind of thing sits on the calendar at a given moment.
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "lowercase")]
enum CalendarKind {
    /// A post the scheduler still owes work on.
    Scheduled,
    /// Something that already went out, from the engagement snapshot table.
    Published,
}

#[derive(Debug, serde::Serialize)]
struct CalendarEntry {
    id: String,
    kind: CalendarKind,
    /// The moment this sits at, epoch millis.
    at: i64,
    status: Option<PostStatus>,
    platforms: Vec<Platform>,
    /// A short preview so a calendar cell can render without a second fetch.
    title: String,
    post_id: Option<String>,
    permalink: Option<String>,
}

/// How much draft text a calendar cell gets.
const CALENDAR_TITLE_CHARS: usize = 120;

fn preview(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= CALENDAR_TITLE_CHARS {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(CALENDAR_TITLE_CHARS).collect();
    format!("{head}…")
}

/// Scheduled posts and published activity projected onto one time range.
///
/// Both kinds in one list because the calendar renders them in the same grid, and
/// two endpoints would force the UI to merge and re-sort them itself.
#[utoipa::path(
    get,
    path = "/api/social/calendar",
    tag = "Social",
    summary = "everything scheduled or already published inside one time range.",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("from" = i64, Query, description = "range start, epoch millis UTC, inclusive"),
        ("to" = i64, Query, description = "range end, epoch millis UTC, exclusive; must be after `from`"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn calendar(
    State(state): State<AppState>,
    Query(q): Query<RangeQuery>,
) -> ApiResult<Json<Value>> {
    if q.to <= q.from {
        return Err(ApiError::bad_request("`to` must be after `from`"));
    }
    let workspace = q
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);

    let posts = state
        .store
        .list_posts_in_range(workspace, q.from, q.to)
        .await?;

    // One list read to title every post, rather than a `get_draft` per post: a month
    // view holds tens of posts and this keeps the endpoint at a fixed query count.
    let drafts: BTreeMap<String, String> = state
        .store
        .list_drafts(workspace)
        .await?
        .into_iter()
        .map(|d| (d.id, d.body.text))
        .collect();

    let mut entries: Vec<CalendarEntry> = posts
        .into_iter()
        .map(|post| CalendarEntry {
            at: post.scheduled_for,
            status: Some(post.status),
            platforms: post.targets.iter().map(|t| t.platform).collect(),
            title: post
                .draft_id
                .as_ref()
                .and_then(|id| drafts.get(id))
                .map(|t| preview(t))
                .unwrap_or_default(),
            post_id: Some(post.id.clone()),
            permalink: None,
            id: post.id,
            kind: CalendarKind::Scheduled,
        })
        .collect();

    // Published activity is filtered in memory: the snapshot table is bounded by the
    // list limit and a range predicate on a nullable `published_at` would need its
    // own index for no real benefit at this size.
    entries.extend(
        state
            .store
            .list_activity(workspace, MAX_LIMIT)
            .await?
            .into_iter()
            .filter(|a| a.published_at.is_some_and(|at| at >= q.from && at < q.to))
            .map(|a| CalendarEntry {
                at: a.published_at.unwrap_or_default(),
                status: None,
                platforms: vec![a.platform],
                title: a.text.as_deref().map(preview).unwrap_or_default(),
                post_id: None,
                permalink: a.permalink,
                id: a.id,
                kind: CalendarKind::Published,
            }),
    );

    entries.sort_by_key(|e| e.at);
    Ok(Json(json!({ "entries": entries })))
}

// ── Queue ──────────────────────────────────────────────────────────────────────

/// What the runner will do next, ordered by when it will do it.
///
/// Every row carries a non-null `next_attempt_at` (a target that has never run
/// inherits its post's scheduled time), so the UI can render a countdown without a
/// null branch.
/// Rows are the store's `list_queue` projection ENRICHED by [`crate::queue`] with the
/// account label, the last run's error, and the remaining retry budget — the things a
/// user opens this view to find out. The `queue` key keeps the raw entry's shape as a
/// superset, so the extra fields are additive.
#[utoipa::path(
    get,
    path = "/api/social/queue",
    tag = "Social",
    summary = "what the publisher will send next, with the last error and remaining retries.",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("limit" = Option<i64>, Query, description = "rows to return, 1-500 (default 200)"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn queue(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let view = crate::queue::build(&state, scope.workspace(), scope.limit()).await?;
    Ok(Json(json!({
        "queue": view.items,
        "next_run_at": view.next_run_at,
        "in_flight": view.in_flight,
        "generated_at": view.generated_at,
    })))
}

/// The workspace scope plus an optional platform filter.
#[derive(Debug, Deserialize)]
struct BestTimesQuery {
    #[serde(default)]
    workspace_id: Option<String>,
    /// Rank slots using only this platform's history. Absent means "all platforms",
    /// which is the right default for a compose box that has not picked accounts yet.
    #[serde(default)]
    platform: Option<String>,
}

/// When to post: ranked `(weekday, hour)` slots derived from this workspace's own
/// engagement history.
///
/// Never errors on thin data — [`crate::scheduler::best_times`] falls back to a
/// documented default table and says so in its `basis`, because "we don't know yet"
/// has to render as a suggestion rather than an empty state.
#[utoipa::path(
    get,
    path = "/api/social/best-times",
    tag = "Social",
    summary = "recommended posting slots ranked from this workspace's own engagement history.",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("platform" = Option<String>, Query, description = "rank using only this platform's history, e.g. `bluesky`; omit for all"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn best_times(
    State(state): State<AppState>,
    Query(q): Query<BestTimesQuery>,
) -> ApiResult<Json<Value>> {
    let workspace = q
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    // An unrecognized platform is rejected rather than silently widened to "all":
    // a typo'd filter that returns whole-workspace rankings looks like a working
    // filter and would be read as advice about a platform it never consulted.
    let platform = match q
        .platform
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        Some(raw) => Some(
            Platform::parse(raw)
                .ok_or_else(|| ApiError::bad_request(format!("unknown platform `{raw}`")))?,
        ),
        None => None,
    };
    let recommendation = crate::scheduler::best_times(&state, workspace, platform).await?;
    Ok(Json(json!({ "best_times": recommendation })))
}

// ── History ────────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/history",
    tag = "Social",
    summary = "posts that already went out, newest first, with their permalinks.",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("limit" = Option<i64>, Query, description = "rows to return, 1-500 (default 200)"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_history(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let history = state
        .store
        .list_history(scope.workspace(), scope.limit())
        .await?;
    Ok(Json(json!({ "history": history })))
}

#[utoipa::path(
    get,
    path = "/api/social/history/{id}",
    tag = "Social",
    summary = "one published post with its engagement snapshot.",
    params(("id" = String, Path, description = "history entry id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<PostHistoryEntry>> {
    state
        .store
        .get_history(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("history entry"))
}

#[utoipa::path(
    post,
    path = "/api/social/history/{id}/refresh-engagement",
    tag = "Social",
    summary = "re-read likes/replies/reposts for one published post from its platform.",
    params(("id" = String, Path, description = "history entry id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn refresh_engagement(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<ActivityItem>> {
    Ok(Json(
        crate::analytics::refresh_engagement(&state, &id, agent_grant(&headers).as_deref()).await?,
    ))
}

// ── Inbox ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct InboxQuery {
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    unread: Option<bool>,
    #[serde(default)]
    unreplied: Option<bool>,
}

#[utoipa::path(
    get,
    path = "/api/social/inbox",
    tag = "Social",
    summary = "incoming comments, replies, mentions and DMs across the connected accounts.",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("limit" = Option<i64>, Query, description = "rows to return, 1-500 (default 200)"),
        ("account_id" = Option<String>, Query, description = "only items on this account"),
        ("kind" = Option<String>, Query, description = "one of: comment, reply, mention, dm"),
        ("unread" = Option<bool>, Query, description = "true to return only unread items"),
        ("unreplied" = Option<bool>, Query, description = "true to return only items with no reply yet"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_inbox(
    State(state): State<AppState>,
    Query(q): Query<InboxQuery>,
) -> ApiResult<Json<Value>> {
    let workspace = q
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    let filter = InboxFilter {
        account_id: q.account_id.filter(|a| !a.trim().is_empty()),
        // An unrecognized kind filter is dropped rather than 400'd: `from_db` is
        // total and would silently coerce it to `comment`, which is worse than
        // showing everything.
        kind: q
            .kind
            .as_deref()
            .filter(|k| matches!(*k, "comment" | "reply" | "mention" | "dm"))
            .map(InboxKind::from_db),
        unread_only: q.unread.unwrap_or(false),
        unreplied_only: q.unreplied.unwrap_or(false),
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let inbox = state.store.list_inbox(workspace, &filter, limit).await?;
    Ok(Json(json!({ "inbox": inbox })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct RefreshInboxBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    workspace_id: Option<String>,
    /// Refresh one account instead of every connected account in the workspace.
    #[serde(default)]
    account_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/social/inbox/refresh",
    tag = "Social",
    summary = "pull new inbox items from the platforms right now.",
    request_body = RefreshInboxBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn refresh_inbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<RefreshInboxBody>,
) -> ApiResult<Json<crate::inbox::RefreshSummary>> {
    let workspace = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    Ok(Json(
        crate::inbox::refresh(
            &state,
            workspace,
            body.account_id.as_deref(),
            agent_grant(&headers).as_deref(),
        )
        .await?,
    ))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct ReplyBody {
    /// The reply to publish. Required and non-blank.
    text: String,
}

#[utoipa::path(
    post,
    path = "/api/social/inbox/{id}/reply",
    tag = "Social",
    summary = "publish a reply to an inbox item.",
    params(("id" = String, Path, description = "inbox item id")),
    request_body = ReplyBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn reply_inbox(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ReplyBody>,
) -> ApiResult<Json<InboxItem>> {
    if body.text.trim().is_empty() {
        return Err(ApiError::bad_request("a reply needs text"));
    }
    Ok(Json(
        crate::inbox::reply(&state, &id, &body.text, agent_grant(&headers).as_deref()).await?,
    ))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct ReadBody {
    /// Absent means "mark read". Pass `false` to mark it unread again.
    #[serde(default)]
    read: Option<bool>,
}

// `request_body = ReadBody` — the PLAIN type, even though the extractor is
// `Option<Json<ReadBody>>` and an absent body is legal here.
//
// `Option<ReadBody>` would render as `{"oneOf":[{"type":"null"},{"$ref":…}]}`, and a
// `oneOf` node has no `properties` for the importer to lower, so the derived tool
// comes out with zero arguments — discoverable and uncallable. utoipa 5 derives
// `required` solely from `is_option()` and offers no `required = false` knob, so the
// plain type is the only shape that keeps `read` visible. The cost is a body
// documented as required that this handler in fact tolerates omitting; sending
// `{"read": true}` is always valid, so the lie is harmless. `ryu-quests`'
// `use_item` made the same trade for the same reason.
#[utoipa::path(
    post,
    path = "/api/social/inbox/{id}/read",
    tag = "Social",
    summary = "mark an inbox item read (or unread).",
    params(("id" = String, Path, description = "inbox item id")),
    request_body = ReadBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn read_inbox(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<ReadBody>>,
) -> ApiResult<Json<InboxItem>> {
    // The body is optional: a "mark read" click sends no payload, and requiring an
    // empty JSON object would make the simplest call the fiddliest.
    let read = body.and_then(|Json(b)| b.read).unwrap_or(true);
    require_hit(state.store.mark_inbox_read(&id, read).await?, "inbox item")?;
    state
        .store
        .get_inbox_item(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("inbox item"))
}

// ── Templates ──────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/templates",
    tag = "Social",
    summary = "list reusable post templates (starter templates are seeded on first read).",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_templates(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    // The starter templates are seeded lazily, HERE, rather than at boot: a workspace
    // created after startup would otherwise show an empty template list until the next
    // restart. The seed is guarded by a per-workspace marker, so this is one indexed
    // primary-key lookup on every call after the first — and, crucially, a built-in the
    // user deleted stays deleted instead of reappearing on the next page load.
    crate::templates::ensure_seeded(&state.store, scope.workspace()).await;
    let templates = state.store.list_templates(scope.workspace()).await?;
    Ok(Json(json!({ "templates": templates })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreateTemplateBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    workspace_id: Option<String>,
    /// The template's display name. Required and non-blank.
    name: String,
    /// The template's starting text and any per-platform variants.
    #[serde(default)]
    body: TemplateBody,
}

#[utoipa::path(
    post,
    path = "/api/social/templates",
    tag = "Social",
    summary = "save a reusable post template.",
    request_body = CreateTemplateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn create_template(
    State(state): State<AppState>,
    Json(body): Json<CreateTemplateBody>,
) -> ApiResult<Json<Template>> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("a template needs a name"));
    }
    let workspace = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    Ok(Json(
        state
            .store
            .create_template(workspace, &body.name, &body.body)
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/social/templates/{id}",
    tag = "Social",
    summary = "read one post template.",
    params(("id" = String, Path, description = "template id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_template(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Template>> {
    state
        .store
        .get_template(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("template"))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct PatchTemplateBody {
    /// New display name. Omit to leave it unchanged.
    #[serde(default)]
    name: Option<String>,
    /// New content. Omit to leave it unchanged.
    // Inlined for the same reason as `CreatePostBody::body`: the nullable wrapper
    // would hide `text` / `platform_defaults` behind a `$ref`.
    #[schema(inline)]
    #[serde(default)]
    body: Option<TemplateBody>,
}

#[utoipa::path(
    patch,
    path = "/api/social/templates/{id}",
    tag = "Social",
    summary = "rename a post template or replace its content.",
    params(("id" = String, Path, description = "template id")),
    request_body = PatchTemplateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn patch_template(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(patch): Json<PatchTemplateBody>,
) -> ApiResult<Json<Template>> {
    // Same rule as `create_template`: a rename may not blank the name.
    if patch
        .name
        .as_deref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(ApiError::bad_request("a template needs a name"));
    }
    require_hit(
        state
            .store
            .update_template(&id, patch.name.as_deref(), patch.body.as_ref())
            .await?,
        "template",
    )?;
    state
        .store
        .get_template(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("template"))
}

#[utoipa::path(
    delete,
    path = "/api/social/templates/{id}",
    tag = "Social",
    summary = "delete a post template.",
    params(("id" = String, Path, description = "template id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_template(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_hit(state.store.delete_template(&id).await?, "template")?;
    Ok(Json(json!({ "ok": true })))
}

/// `POST /templates/:id/use` — start a new draft from a template.
///
/// The action behind the Store tab's "Use" button (`contributes.store_tabs[].spec
/// .install` in the manifest), which makes it the one route here whose caller is
/// Core's declarative view renderer rather than the companion frame. The renderer
/// sends no request body, so everything this needs comes from the path.
///
/// It is a CREATE, never a mutation of the template. "Using" a template must leave
/// it reusable — the Store row is not an install toggle that flips to "installed",
/// it is a button that hands back something editable and leaves the catalogue as it
/// was.
///
/// The draft lands in the TEMPLATE's own workspace rather than a caller-supplied
/// one: reading a scope off a query string here would let a stray `?workspace_id=`
/// file the draft into a workspace where its template does not exist.
#[utoipa::path(
    post,
    path = "/api/social/templates/{id}/use",
    tag = "Social",
    summary = "start a new draft from a template (the template itself is unchanged).",
    params(("id" = String, Path, description = "template id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn use_template(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Draft>> {
    let template = state
        .store
        .get_template(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("template"))?;
    // Only `text` carries over. `platform_defaults` is a per-platform OVERRIDE and a
    // fresh draft has no target accounts yet, so there is nothing to key the
    // overrides onto — compose resolves them once the user picks accounts.
    //
    // Written as ONE segment rather than as `text`, because `DraftBody::normalize`
    // (run by `create_draft`) mirrors `segments[0]` INTO `text`, not the other way
    // round: setting `text` alone against the default empty segment would normalize
    // straight back to an empty draft.
    let body = DraftBody {
        segments: vec![PostSegment {
            text: template.body.text.clone(),
            media: Vec::new(),
        }],
        ..DraftBody::empty()
    };
    Ok(Json(
        state
            .store
            .create_draft(&template.workspace_id, &body)
            .await?,
    ))
}

// ── Media library ──────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/media",
    tag = "Social",
    summary = "list the workspace's media library (references to local files).",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_media(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let media = state.store.list_media(scope.workspace()).await?;
    Ok(Json(json!({ "media": media })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreateMediaBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    workspace_id: Option<String>,
    /// A LOCAL absolute path. Never copied — the library holds a reference.
    path: String,
    /// Display name. Defaults to the file name in `path`.
    #[serde(default)]
    name: Option<String>,
    /// e.g. `image/png`. Inferred from the extension when omitted.
    #[serde(default)]
    mime_type: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/social/media",
    tag = "Social",
    summary = "add a local file to the workspace's media library so posts can attach it.",
    request_body = CreateMediaBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn create_media(
    State(state): State<AppState>,
    Json(body): Json<CreateMediaBody>,
) -> ApiResult<Json<MediaAsset>> {
    if body.path.trim().is_empty() {
        return Err(ApiError::bad_request("media needs a path"));
    }
    let approved_path = crate::state::approved_local_media_path(&body.path)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let approved_path = approved_path.to_string_lossy().into_owned();
    let workspace = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    let name = body
        .name
        .clone()
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| {
            approved_path
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or(&approved_path)
                .to_string()
        });
    // Infer from the extension when the caller did not say. A wrong mime is worse
    // than none: it decides whether the file passes a platform's allowed-prefix
    // check, so an empty inference stays empty rather than guessing "image".
    let mime = body
        .mime_type
        .clone()
        .filter(|m| !m.trim().is_empty())
        .or_else(|| {
            let inferred = mime_for_extension(&name);
            (!inferred.is_empty()).then(|| inferred.to_string())
        });
    Ok(Json(
        state
            .store
            .upsert_media(workspace, &approved_path, &name, mime.as_deref())
            .await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/social/media/{id}",
    tag = "Social",
    summary = "drop a media asset from the library. A file you picked stays on disk; a file imported into Outpost's media root (`POST /media/import`) is deleted once no library row or draft refers to it.",
    params(("id" = String, Path, description = "media asset id")),
    responses((status = 200, description = "`{ok, file_removed}`", body = serde_json::Value))
)]
async fn delete_media(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let Some((path, referenced)) = state.store.delete_media_row(&id).await? else {
        return Err(ApiError::not_found("media asset"));
    };
    let file_removed = crate::media_import::remove_unreferenced_import(
        &state.media_root,
        std::path::Path::new(&path),
        referenced,
    )
    .await
    .map_err(|e| {
        ApiError::conflict(format!(
            "the library row was removed, but its imported file could not be deleted: {e}"
        ))
    })?;
    Ok(Json(json!({ "ok": true, "file_removed": file_removed })))
}

// ── Media import (upload sessions into the media root) ────────────────────────

fn import_workspace(raw: Option<&str>) -> &str {
    raw.filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID)
}

fn import_error(error: crate::media_import::ImportError) -> ApiError {
    use crate::media_import::ImportError;
    match error {
        ImportError::BadRequest(why) => ApiError::bad_request(why),
        ImportError::NotFound => ApiError::not_found("import session"),
        ImportError::Conflict(why) => ApiError::conflict(why),
        busy @ ImportError::Busy => ApiError::conflict(busy.to_string()),
    }
}

#[utoipa::path(
    post,
    path = "/api/social/media/import",
    tag = "Social",
    summary = "start an upload session that copies a video or image into Outpost's media root.",
    request_body = crate::media_import::StartImportBody,
    responses((status = 200, description = "`{id, chunk_size, max_bytes}`", body = serde_json::Value))
)]
async fn start_media_import(
    State(state): State<AppState>,
    Json(body): Json<crate::media_import::StartImportBody>,
) -> ApiResult<Json<Value>> {
    let workspace = import_workspace(body.workspace_id.as_deref());
    let id = state
        .imports
        .start(&state.media_root, workspace, &body)
        .await
        .map_err(import_error)?;
    Ok(Json(json!({
        "id": id,
        "chunk_size": crate::media_import::IMPORT_CHUNK_BYTES,
        "max_bytes": crate::media_import::MAX_IMPORT_BYTES,
    })))
}

#[utoipa::path(
    post,
    path = "/api/social/media/import/{id}/chunks",
    tag = "Social",
    summary = "append the next base64 chunk to a media-import session.",
    params(("id" = String, Path, description = "import session id")),
    request_body = crate::media_import::ImportChunkBody,
    responses((status = 200, description = "`{received}` bytes so far", body = serde_json::Value))
)]
async fn append_media_import(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<crate::media_import::ImportChunkBody>,
) -> ApiResult<Json<Value>> {
    let workspace = import_workspace(body.workspace_id.as_deref());
    let received = state
        .imports
        .append(&id, workspace, &body)
        .await
        .map_err(import_error)?;
    Ok(Json(json!({ "received": received })))
}

#[utoipa::path(
    post,
    path = "/api/social/media/import/{id}/finish",
    tag = "Social",
    summary = "finish a media import: verify it, file it under the media root and add it to the media library.",
    params(("id" = String, Path, description = "import session id")),
    request_body = crate::media_import::FinishImportBody,
    responses((status = 200, description = "`{media, metadata_path}`", body = serde_json::Value))
)]
async fn finish_media_import(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<crate::media_import::FinishImportBody>,
) -> ApiResult<Json<Value>> {
    let workspace = import_workspace(body.workspace_id.as_deref());
    let done = state
        .imports
        .finish(&state.media_root, &id, workspace, &body)
        .await
        .map_err(import_error)?;
    let path = done.path.to_string_lossy().into_owned();
    let media = state
        .store
        .upsert_media(&done.workspace_id, &path, &done.name, Some(&done.mime_type))
        .await?;
    Ok(Json(json!({
        "media": media,
        "metadata_path": done.metadata_path.map(|p| p.to_string_lossy().into_owned()),
    })))
}

#[utoipa::path(
    delete,
    path = "/api/social/media/import/{id}",
    tag = "Social",
    summary = "cancel a media-import session and delete its partial file.",
    params(
        ("id" = String, Path, description = "import session id"),
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn abort_media_import(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let existed = state.imports.abort(&id, scope.workspace()).await;
    require_hit(existed, "import session")?;
    Ok(Json(json!({ "ok": true })))
}

// ── Activity ───────────────────────────────────────────────────────────────────

/// Published activity plus the projections the analytics surface renders.
///
/// The rollups ride along on THIS route rather than getting one of their own, and
/// that is a contract decision, not laziness: the manifest declares one `http.routes`
/// entry per path and a prefix does not admit its subpaths, so a route this app
/// serves but the manifest does not declare is unreachable through Core's ext-proxy.
/// Every surface that wants the rollups already wants the rows.
///
/// The aggregate is computed over the CAPPED window (`limit`, default 200, max 500),
/// not the whole table — so a workspace with years of history gets rollups over its
/// most recent posts. `totals.posts` says how many rows fed them.
#[utoipa::path(
    get,
    path = "/api/social/activity",
    tag = "Social",
    summary = "published-post rows plus engagement rollups (best day/hour, totals per platform).",
    params(
        ("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace"),
        ("limit" = Option<i64>, Query, description = "rows the rollups are computed over, 1-500 (default 200)"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_activity(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let activity = state
        .store
        .list_activity(scope.workspace(), scope.limit())
        .await?;
    // The local-day/hour bucketing needs a fixed UTC offset; the node settings carry
    // it. A read failure degrades to UTC rather than failing the list — the rows are
    // the point and the offset only moves labels.
    let offset = crate::settings::load(&state.store)
        .await
        .map(|s| s.utc_offset_minutes)
        .unwrap_or(0);
    let rollups = crate::analytics::rollups(&activity, offset);
    Ok(Json(json!({ "activity": activity, "rollups": rollups })))
}

// ── Settings ───────────────────────────────────────────────────────────────────

/// The settings tab's whole payload: the per-workspace blob under `settings`, and the
/// node-scoped one under `node`.
///
/// **`node` is REDACTED and always will be.** It carries `*_set: bool` presence flags
/// and `*_source` ("env" / "stored" / "unset") — never a credential's value. A GET
/// that echoed an account credential back would put it in every proxy log and every
/// screenshot of this screen. Provider credentials are Gateway-owned and are not
/// part of this payload. Plaintext is accepted on PATCH only for account credentials.
#[utoipa::path(
    get,
    path = "/api/social/settings",
    tag = "Social",
    summary = "read the scheduler and publishing settings (credentials are redacted to presence flags).",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_settings(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let settings = state.store.get_settings(scope.workspace()).await?;
    let node = crate::settings::load(&state.store).await?.redacted();
    Ok(Json(json!({ "settings": settings, "node": node })))
}

/// Partial update. Every field is optional; absent means "leave unchanged", which is
/// why this is not just the settings struct.
#[derive(Debug, Deserialize)]
struct PatchSettingsBody {
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    scheduler_enabled: Option<bool>,
    #[serde(default)]
    poll_interval_secs: Option<u64>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    base_backoff_ms: Option<u64>,
    #[serde(default)]
    claim_lease_secs: Option<u64>,
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    enforce_platform_limits: Option<bool>,
    /// Captured only so a node-scoped half sent to this workspace route is REFUSED
    /// rather than silently dropped. Node-wide policy and account credentials are
    /// install-wide, so they are changed through `PATCH /node-settings`, which Core
    /// gates on the separate `social.manage-node` permission. Workspace access plus
    /// `social.publish` (this route's gate) is not authority over the whole node.
    #[serde(default)]
    node: Option<serde::de::IgnoredAny>,
}

// DELIBERATELY NOT in `SocialApiDoc`, and `PatchSettingsBody`/`NodeSettingsPatch`
// deliberately carry no `ToSchema`.
//
// This is the one route whose request body accepts PLAINTEXT account credentials
// (`node.bluesky_app_password`,
// `node.threads_access_token`).
// Documenting it would lower it into an agent tool whose advertised arguments are
// "paste a provider API key here" — an invitation for a model to invent or relay a
// secret, and a schema that puts those field names in every model's context. The
// redacted `GET /settings` is documented instead, so an agent can still READ the
// configuration; changing credentials stays a human action in the settings tab.
async fn patch_settings(
    State(state): State<AppState>,
    Json(patch): Json<PatchSettingsBody>,
) -> ApiResult<Json<Value>> {
    if patch.node.is_some() {
        return Err(ApiError::bad_request(
            "node settings are changed through PATCH /node-settings, which requires node management permission",
        ));
    }
    let workspace = patch
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    let mut settings = state.store.get_settings(workspace).await?;
    if let Some(v) = patch.scheduler_enabled {
        settings.scheduler_enabled = v;
    }
    if let Some(v) = patch.poll_interval_secs {
        // Floored, not rejected: a 1-second poll would hammer the database for no
        // benefit, and silently clamping is friendlier than a 400 on a slider.
        settings.poll_interval_secs = v.max(10);
    }
    if let Some(v) = patch.max_attempts {
        settings.max_attempts = v.clamp(1, 10);
    }
    if let Some(v) = patch.base_backoff_ms {
        settings.base_backoff_ms = v.clamp(100, 60_000);
    }
    if let Some(v) = patch.claim_lease_secs {
        // The lease MUST outlast the worst-case publish or a healthy run gets
        // double-claimed — which, given that most providers do not honour an
        // idempotency key, means a double-post.
        settings.claim_lease_secs = v.max(60);
    }
    if let Some(v) = patch.timezone {
        if !v.trim().is_empty() {
            settings.timezone = v;
        }
    }
    if let Some(v) = patch.enforce_platform_limits {
        settings.enforce_platform_limits = v;
    }
    let saved = state.store.put_settings(workspace, &settings).await?;

    // The node view is read back unconditionally, so the response is always the
    // complete current state and a client does not need a follow-up GET.
    let node = crate::settings::load(&state.store).await?.redacted();
    Ok(Json(json!({ "settings": saved, "node": node })))
}

// Like `PATCH /settings`, DELIBERATELY NOT in `SocialApiDoc`: the body accepts
// PLAINTEXT account credentials, and changing them stays a human action in the
// settings tab rather than an agent tool.
//
// The node blob is shared by every workspace on this install (inbox polling,
// engagement refresh, quiet hours, analytics offset, direct-account credentials).
// The manifest gates this route on `social.manage-node`, separate from
// `social.publish`, and the workspace gate in `authorize_request` deliberately does
// not apply: no workspace's access decision is authority over the whole node.
async fn patch_node_settings(
    State(state): State<AppState>,
    Json(patch): Json<crate::settings::NodeSettingsPatch>,
) -> ApiResult<Json<Value>> {
    let node = crate::settings::patch(&state.store, patch).await?;
    Ok(Json(json!({ "node": node })))
}

// ── Platforms ──────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
struct PlatformInfo {
    platform: Platform,
    label: String,
    limits: PlatformLimits,
    capabilities: PlatformCapabilities,
}

/// The limits + capability matrix the composer renders from.
///
/// One endpoint for both because they are always read together: a chip needs to know
/// both "can I publish here" and "what are the limits", and splitting them would
/// double the round-trips on first paint.
#[utoipa::path(
    get,
    path = "/api/social/platforms",
    tag = "Social",
    summary = "every supported platform with its character/media limits and capabilities.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn list_platforms(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let mut platforms = Vec::with_capacity(Platform::ALL.len());
    for platform in Platform::ALL {
        platforms.push(PlatformInfo {
            platform,
            label: platform.label().to_string(),
            limits: limits_for(platform),
            capabilities: state.providers.capabilities_for(platform).await,
        });
    }
    Ok(Json(json!({ "platforms": platforms })))
}

#[cfg(test)]
mod tests {
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

    /// Tests that point the process-wide `RYU_SOCIAL_MEDIA_ROOT` at their own
    /// root run one at a time.
    static MEDIA_ROOT_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Import `render` through the chunked session handlers, as Video Studio's
    /// Send to Outpost does, and return the library row's `path`.
    async fn import_render(state: &AppState, render: &[u8]) -> String {
        use base64::Engine;
        let started = start_media_import(
            State(state.clone()),
            Json(crate::media_import::StartImportBody {
                workspace_id: None,
                name: "Launch.mp4".into(),
                mime_type: "video/mp4".into(),
                size: render.len() as u64,
            }),
        )
        .await
        .unwrap()
        .0;
        let id = started["id"].as_str().unwrap().to_owned();
        let chunk_size = started["chunk_size"].as_u64().unwrap() as usize;
        for (index, chunk) in render.chunks(chunk_size).enumerate() {
            let _received = append_media_import(
                State(state.clone()),
                Path(id.clone()),
                Json(crate::media_import::ImportChunkBody {
                    workspace_id: None,
                    offset: (index * chunk_size) as u64,
                    data: base64::engine::general_purpose::STANDARD.encode(chunk),
                }),
            )
            .await
            .unwrap();
        }
        let finished = finish_media_import(
            State(state.clone()),
            Path(id),
            Json(crate::media_import::FinishImportBody {
                workspace_id: None,
                sha256: None,
                metadata_text: Some("Title: Launch\n".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(finished["media"]["kind"], json!("video"));
        assert!(finished["metadata_path"]
            .as_str()
            .unwrap()
            .ends_with("Launch-metadata.txt"));
        finished["media"]["path"].as_str().unwrap().to_owned()
    }

    fn temp_media_root(label: &str) -> std::path::PathBuf {
        let root =
            std::env::temp_dir().join(format!("ryu-social-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    /// One scheduled post of `path` to a new `platform` account, made due now.
    async fn due_post_of(
        state: &AppState,
        platform: Platform,
        path: &str,
    ) -> crate::models::ScheduledPost {
        let account = state
            .store
            .create_account(DEFAULT_WORKSPACE_ID, platform, "@studio", Some("ext_1"))
            .await
            .unwrap();
        let mut body = DraftBody::empty();
        body.segments[0].text = "Launch day".into();
        body.segments[0].media = vec![MediaRef {
            path: path.to_owned(),
            mime_type: "video/mp4".into(),
            name: "Launch.mp4".into(),
        }];
        body.account_ids = vec![account.id.clone()];
        body.normalize();
        let draft = create_draft(
            State(state.clone()),
            Json(DraftBodyPayload {
                workspace_id: None,
                body,
            }),
        )
        .await
        .unwrap()
        .0;
        let post = create_post(
            State(state.clone()),
            HeaderMap::new(),
            Json(CreatePostBody {
                workspace_id: None,
                draft_id: Some(draft.id),
                body: None,
                scheduled_for: Some(1),
                account_ids: vec![account.id],
                variants: BTreeMap::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        let now = chrono::Utc::now().timestamp_millis();
        assert!(state
            .store
            .mark_post_due_now(&post.id, now, None)
            .await
            .unwrap());
        state
            .store
            .get_scheduled_post(&post.id)
            .await
            .unwrap()
            .unwrap()
    }

    /// The Video Studio handoff end to end against the deterministic fake
    /// provider: a render is imported in chunks into the media root, attached to
    /// one draft per target platform, and published. The fake provider accepts
    /// local files; the real Composio provider stages them first (see
    /// `an_imported_render_is_staged_and_published_through_composio`).
    #[tokio::test]
    async fn an_imported_render_publishes_through_the_fake_provider() {
        use std::sync::Arc;
        let root = temp_media_root("handoff");
        // Publish re-checks local media against the process-wide root.
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let fake = Arc::new(crate::providers::FakeProvider::new());
        let mut state = state().with_media_root(root.clone());
        state.providers = crate::providers::ProviderRegistry::with_provider(fake.clone());

        let render: Vec<u8> = (0..(crate::media_import::IMPORT_CHUNK_BYTES + 10))
            .map(|i| (i % 251) as u8)
            .collect();
        let path = import_render(&state, &render).await;
        assert_eq!(std::fs::read(&path).unwrap(), render);
        assert_eq!(
            list_media(
                State(state.clone()),
                Query(ScopeQuery {
                    workspace_id: None,
                    limit: None
                })
            )
            .await
            .unwrap()
            .0["media"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        for platform in [Platform::Youtube, Platform::Tiktok] {
            let post = due_post_of(&state, platform, &path).await;
            let outcome = crate::publish::run_post(&state, &post).await.unwrap();
            assert_eq!(
                outcome.status,
                PostStatus::Published,
                "{platform:?}: {outcome:?}"
            );
        }
        let calls = fake.calls();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.media_count == 1));
        std::fs::remove_dir_all(root).unwrap();
    }

    /// What the fake Gateway router and fake storage endpoint saw.
    #[derive(Clone, Default)]
    struct BrokerLog {
        calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        /// The agent grant header of each call, in the same order as `calls`.
        grants: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
        uploads: std::sync::Arc<std::sync::Mutex<Vec<(Vec<u8>, Option<String>, Option<String>)>>>,
        storage: std::sync::Arc<std::sync::Mutex<String>>,
        refuse_upload: bool,
        /// Answer the upload tool's `execute` with this HTTP status, as Core
        /// relays a Gateway timeout or upstream failure.
        execute_status: Option<u16>,
        /// Answer the upload tool with `data` as a JSON-encoded string.
        string_data: bool,
        /// `Content-MD5` headers the fake storage saw.
        content_md5: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    /// A fake of Core's `providers.call` bridge (and the Gateway behind it) and
    /// of the presigned storage endpoint, on one loopback server.
    async fn fake_broker(log: BrokerLog) -> (String, tokio::task::JoinHandle<()>) {
        use axum::routing::{post, put};
        async fn call(
            State(log): State<BrokerLog>,
            headers: axum::http::HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> (axum::http::StatusCode, Json<serde_json::Value>) {
            log.grants.lock().unwrap().push(
                headers
                    .get(ryu_app_events::HDR_AGENT_GRANT)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            );
            log.calls.lock().unwrap().push(body.clone());
            if body["operation"] == json!("execute") {
                if let Some(status) = log.execute_status {
                    return (
                        axum::http::StatusCode::from_u16(status).unwrap(),
                        Json(json!({ "error": "upstream timed out" })),
                    );
                }
                if log.string_data {
                    let id = json!({ "id": "yt_string_1" }).to_string();
                    return (
                        axum::http::StatusCode::OK,
                        Json(json!({ "ok": true, "status": 200, "body": { "data": id } })),
                    );
                }
            }
            let reply = match (body["operation"].as_str(), body["toolId"].as_str()) {
                (Some("capabilities"), Some(toolkit)) => json!({
                    "items": [{ "slug": format!("{}_UPLOAD_VIDEO", toolkit.to_ascii_uppercase()) }]
                }),
                (Some("file_upload"), _) => json!({
                    "key": "uploads/fixture/render.mp4",
                    "uploadUrl": format!("{}/put?sig=fixture", log.storage.lock().unwrap()),
                    "storageBackend": "s3",
                }),
                (Some("execute"), Some("YOUTUBE_UPLOAD_VIDEO")) => {
                    json!({ "response_data": { "id": "yt_video_1" } })
                }
                (Some("execute"), Some("TIKTOK_UPLOAD_VIDEO")) => {
                    json!({ "data": { "publish_id": "v_pub_1" } })
                }
                _ => json!({}),
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({ "ok": true, "status": 200, "body": reply })),
            )
        }
        async fn upload(
            State(log): State<BrokerLog>,
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> axum::http::StatusCode {
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
            };
            log.uploads.lock().unwrap().push((
                body.to_vec(),
                header("content-type"),
                header("content-length"),
            ));
            log.content_md5.lock().unwrap().push(header("content-md5"));
            // Like S3 and Azure: a body that does not match its declared
            // digest is refused.
            if let Some(declared) = header("content-md5") {
                use base64::Engine;
                use md5::Digest;
                let actual =
                    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(&body));
                if actual != declared {
                    return axum::http::StatusCode::BAD_REQUEST;
                }
            }
            if log.refuse_upload {
                axum::http::StatusCode::FORBIDDEN
            } else {
                axum::http::StatusCode::OK
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        *log.storage.lock().unwrap() = base.clone();
        let router = axum::Router::new()
            .route("/providers/call", post(call))
            .route("/put", put(upload))
            .with_state(log);
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (base, task)
    }

    fn composio_via(base: &str) -> crate::providers::ComposioProvider {
        crate::providers::ComposioProvider::managed(ryu_app_events::ProviderRouter::for_test(
            crate::state::PLUGIN_ID,
            reqwest::Client::new(),
            format!("{base}/providers/call"),
            format!("{base}/providers/status"),
        ))
        .with_loopback_uploads()
    }

    fn render_request(platform: Platform, path: &str) -> crate::providers::PublishRequest {
        crate::providers::PublishRequest {
            account: crate::providers::ProviderAccount {
                id: "acc_1".into(),
                platform,
                label: None,
                external_id: Some("ca_1".into()),
            },
            text: "Launch day\n\nThe whole story.\n\n0:00 Intro".into(),
            media: vec![crate::publish::to_publish_media(&MediaRef {
                path: path.to_owned(),
                mime_type: "video/mp4".into(),
                name: "Launch.mp4".into(),
            })],
            segments: None,
            idempotency_key: Some("sp_1:acc_1".into()),
            agent_grant: None,
        }
    }

    /// A Send to Outpost render published through the real Composio provider:
    /// the file is staged through the Gateway router's `file_upload` operation,
    /// streamed byte for byte to the presigned URL, and handed to the platform's
    /// upload tool as `{ name, mimetype, s3key }`. The router and storage are
    /// fakes; no live YouTube or TikTok publish is exercised here.
    #[tokio::test]
    async fn an_imported_render_is_staged_and_published_through_composio() {
        use crate::providers::PlatformProvider;
        let root = temp_media_root("composio-handoff");
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let state = state().with_media_root(root.clone());
        let render: Vec<u8> = (0..(crate::media_import::IMPORT_CHUNK_BYTES + 77))
            .map(|i| (i % 241) as u8)
            .collect();
        let path = import_render(&state, &render).await;
        let log = BrokerLog::default();
        let (base, server) = fake_broker(log.clone()).await;
        let composio = composio_via(&base);

        let youtube = composio
            .publish(&render_request(Platform::Youtube, &path))
            .await;
        assert_eq!(
            youtube,
            crate::providers::PublishResult::Ok {
                remote_id: "yt_video_1".into(),
                remote_url: Some("https://www.youtube.com/watch?v=yt_video_1".into()),
            }
        );
        let tiktok = composio
            .publish(&render_request(Platform::Tiktok, &path))
            .await;
        assert_eq!(
            tiktok,
            crate::providers::PublishResult::Ok {
                remote_id: "v_pub_1".into(),
                remote_url: None,
            }
        );

        let uploads = log.uploads.lock().unwrap().clone();
        assert_eq!(uploads.len(), 2);
        for (bytes, content_type, content_length) in &uploads {
            assert_eq!(bytes, &render);
            assert_eq!(content_type.as_deref(), Some("video/mp4"));
            assert_eq!(
                content_length.as_deref(),
                Some(render.len().to_string().as_str())
            );
        }
        let calls = log.calls.lock().unwrap().clone();
        let operations: Vec<(&str, &str)> = calls
            .iter()
            .map(|call| {
                (
                    call["operation"].as_str().unwrap(),
                    call["toolId"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            operations,
            [
                ("file_upload", "YOUTUBE_UPLOAD_VIDEO"),
                ("execute", "YOUTUBE_UPLOAD_VIDEO"),
                ("file_upload", "TIKTOK_UPLOAD_VIDEO"),
                ("execute", "TIKTOK_UPLOAD_VIDEO"),
            ]
        );
        let md5 = calls[0]["body"]["md5"].as_str().unwrap();
        assert_eq!(md5.len(), 32);
        assert_eq!(calls[0]["body"]["size"], json!(render.len()));
        // Every PUT declared the hashed digest, so storage verifies the bytes.
        let digests = log.content_md5.lock().unwrap().clone();
        assert_eq!(digests.len(), 2);
        assert!(digests.iter().all(Option::is_some));
        // The upload tool is given a long upstream timeout; staging is not.
        assert!(calls[0].get("timeoutMs").is_none());
        assert!(calls[1]["timeoutMs"].as_u64().unwrap() >= 600_000);
        assert_eq!(calls[0]["body"]["toolkitSlug"], json!("youtube"));
        assert!(calls[0]["body"]["filename"]
            .as_str()
            .unwrap()
            .ends_with(".mp4"));
        let staged = json!({
            "name": calls[0]["body"]["filename"],
            "mimetype": "video/mp4",
            "s3key": "uploads/fixture/render.mp4",
        });
        let youtube_args = &calls[1]["body"];
        assert_eq!(calls[1]["accountId"], json!("ca_1"));
        assert_eq!(youtube_args["videoFilePath"], staged);
        assert_eq!(youtube_args["title"], json!("Launch day"));
        assert_eq!(
            youtube_args["description"],
            json!("The whole story.\n\n0:00 Intro")
        );
        assert_eq!(youtube_args["privacyStatus"], json!("public"));
        let tiktok_args = &calls[3]["body"];
        assert_eq!(tiktok_args["file_to_upload"], staged);
        assert_eq!(tiktok_args["publish"], json!(true));
        assert_eq!(tiktok_args["privacy_level"], json!("PUBLIC_TO_EVERYONE"));
        // The broker credential never appears in anything this process sent.
        assert!(calls.iter().all(|call| call.get("apiKey").is_none()));
        server.abort();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A scheduling agent's grant covers the whole video publish: the staging
    /// step names the connected account and carries the grant, like the upload
    /// tool call that follows, so Core checks the agent's account access for
    /// both and never sees a grant-less call for the agent's own post.
    #[tokio::test]
    async fn video_staging_names_the_account_and_carries_the_agents_grant() {
        use crate::providers::PlatformProvider;
        let root = temp_media_root("composio-grant");
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let state = state().with_media_root(root.clone());
        let path = import_render(&state, b"render bytes").await;
        let log = BrokerLog::default();
        let (base, server) = fake_broker(log.clone()).await;
        let composio = composio_via(&base);

        let mut request = render_request(Platform::Youtube, &path);
        request.agent_grant = Some("grant.sig".into());
        let outcome = composio.publish(&request).await;
        assert!(
            matches!(outcome, crate::providers::PublishResult::Ok { .. }),
            "{outcome:?}"
        );

        let calls = log.calls.lock().unwrap().clone();
        let grants = log.grants.lock().unwrap().clone();
        let steps: Vec<(&str, Option<&str>, Option<&str>)> = calls
            .iter()
            .zip(&grants)
            .filter(|(call, _)| {
                matches!(
                    call["operation"].as_str(),
                    Some("file_upload") | Some("execute")
                )
            })
            .map(|(call, grant)| {
                (
                    call["operation"].as_str().unwrap(),
                    call["accountId"].as_str(),
                    grant.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            steps,
            [
                ("file_upload", Some("ca_1"), Some("grant.sig")),
                ("execute", Some("ca_1"), Some("grant.sig")),
            ]
        );
        server.abort();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A storage refusal fails the publish before the upload tool runs, so a
    /// half-staged file is never published; platforms without an upload tool
    /// still refuse local media before any network call.
    #[tokio::test]
    async fn a_refused_upload_or_an_unstageable_platform_fails_visibly() {
        use crate::providers::PlatformProvider;
        let root = temp_media_root("composio-refused");
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let state = state().with_media_root(root.clone());
        let path = import_render(&state, b"render bytes").await;
        let log = BrokerLog {
            refuse_upload: true,
            ..BrokerLog::default()
        };
        let (base, server) = fake_broker(log.clone()).await;
        let composio = composio_via(&base);

        let error = composio
            .publish(&render_request(Platform::Youtube, &path))
            .await
            .error()
            .unwrap_or_default()
            .to_owned();
        assert!(error.contains("HTTP 403"), "{error}");
        assert!(log
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|call| call["operation"] != json!("execute")));

        log.calls.lock().unwrap().clear();
        for platform in [Platform::Instagram, Platform::Linkedin, Platform::X] {
            let error = composio
                .publish(&render_request(platform, &path))
                .await
                .error()
                .unwrap_or_default()
                .to_owned();
            assert!(
                error.contains("Composio cannot publish the local file") && error.contains(&path),
                "{platform:?}: {error}"
            );
        }
        assert!(log.calls.lock().unwrap().is_empty());

        // A file outside the media root is never staged.
        let outside =
            std::env::temp_dir().join(format!("ryu-social-outside-{}.mp4", uuid::Uuid::new_v4()));
        std::fs::write(&outside, b"x").unwrap();
        let error = composio
            .publish(&render_request(Platform::Tiktok, outside.to_str().unwrap()))
            .await
            .error()
            .unwrap_or_default()
            .to_owned();
        assert!(error.contains("media root"), "{error}");
        assert!(log.calls.lock().unwrap().is_empty());
        std::fs::remove_file(outside).unwrap();
        server.abort();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The duplicate-post hazard: Composio runs the upload tool upstream even
    /// when Gateway's answer is a timeout. A scheduled publish whose upload call
    /// fails after dispatch is settled as failed after ONE call, with a reason
    /// that says to check the platform, instead of being retried into a second
    /// public video.
    #[tokio::test]
    async fn a_failed_upload_call_is_never_retried_into_a_duplicate() {
        use std::sync::Arc;
        let root = temp_media_root("composio-no-dup");
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let mut state = state().with_media_root(root.clone());
        let path = import_render(&state, b"render bytes for a large file").await;

        for (execute_status, string_data) in [(Some(504), false), (Some(502), false), (None, true)]
        {
            let log = BrokerLog {
                execute_status,
                string_data,
                ..BrokerLog::default()
            };
            let (base, server) = fake_broker(log.clone()).await;
            state.providers =
                crate::providers::ProviderRegistry::with_provider(Arc::new(composio_via(&base)));
            let post = due_post_of(&state, Platform::Youtube, &path).await;
            let outcome = crate::publish::run_post(&state, &post).await.unwrap();
            let executes = log
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call["operation"] == json!("execute"))
                .count();
            assert_eq!(
                executes, 1,
                "{execute_status:?}: the upload tool ran once; {outcome:?}"
            );
            if string_data {
                // `data` as a JSON string still yields the video id.
                assert_eq!(outcome.status, PostStatus::Published, "{outcome:?}");
                assert_eq!(outcome.targets[0].remote_id.as_deref(), Some("yt_string_1"));
            } else {
                assert_eq!(outcome.status, PostStatus::Failed, "{outcome:?}");
                let target = &outcome.targets[0];
                assert_eq!(target.attempts, 1);
                let error = target.error.as_deref().unwrap_or_default();
                assert!(
                    error.contains("outcome is unknown") && error.contains("Check YouTube"),
                    "{error}"
                );
            }
            server.abort();
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A Gateway refusal before dispatch (here a 429 rate limit) published
    /// nothing, so it stays an ordinary, retryable failure.
    #[tokio::test]
    async fn a_refusal_before_dispatch_stays_retryable() {
        use crate::providers::PlatformProvider;
        let root = temp_media_root("composio-refused-early");
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let state = state().with_media_root(root.clone());
        let path = import_render(&state, b"render").await;
        let log = BrokerLog {
            execute_status: Some(429),
            ..BrokerLog::default()
        };
        let (base, server) = fake_broker(log.clone()).await;
        let result = composio_via(&base)
            .publish(&render_request(Platform::Tiktok, &path))
            .await;
        assert!(result.is_retryable(), "{result:?}");
        assert!(result.error().unwrap().contains("refused"));
        server.abort();
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Deleting an imported file's library row deletes the file once nothing
    /// refers to it; a draft that still attaches it keeps it on disk, and a
    /// hand-picked file is never deleted.
    #[tokio::test]
    async fn deleting_imported_media_frees_its_file_once_unreferenced() {
        let root = temp_media_root("import-delete");
        let _env = MEDIA_ROOT_ENV.lock().await;
        std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
        let state = state().with_media_root(root.clone());
        let media_id = |path: &str| {
            let state = state.clone();
            let path = path.to_owned();
            async move {
                state
                    .store
                    .list_media(DEFAULT_WORKSPACE_ID)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|media| media.path == path)
                    .unwrap()
                    .id
            }
        };

        // Attached to a draft: the row goes, the file stays.
        let kept = import_render(&state, b"kept").await;
        let _post = due_post_of(&state, Platform::Youtube, &kept).await;
        let removed = delete_media(State(state.clone()), Path(media_id(&kept).await))
            .await
            .unwrap()
            .0;
        assert_eq!(removed["file_removed"], json!(false));
        assert!(std::path::Path::new(&kept).exists());

        // Unreferenced: the file and its metadata text are deleted.
        let freed = import_render(&state, b"freed").await;
        let removed = delete_media(State(state.clone()), Path(media_id(&freed).await))
            .await
            .unwrap()
            .0;
        assert_eq!(removed["file_removed"], json!(true));
        assert!(!std::path::Path::new(&freed).parent().unwrap().exists());

        // A file the user picked is only ever a reference.
        let picked = root.join("picked.mp4");
        std::fs::write(&picked, b"mine").unwrap();
        let picked_path = picked.to_string_lossy().into_owned();
        state
            .store
            .upsert_media(
                DEFAULT_WORKSPACE_ID,
                &picked_path,
                "picked.mp4",
                Some("video/mp4"),
            )
            .await
            .unwrap();
        let removed = delete_media(State(state.clone()), Path(media_id(&picked_path).await))
            .await
            .unwrap()
            .0;
        assert_eq!(removed["file_removed"], json!(false));
        assert!(picked.exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn openapi_doc_covers_the_served_routes() {
        let doc = openapi();
        assert!(!doc.paths.paths.is_empty());
    }

    /// Every documented path must be ABSOLUTE, under this app's mount.
    ///
    /// The failure this catches is silent and total: Core matches a derived operation
    /// by stripping the mount off the spec's own path, so a relative `/posts` slipping
    /// into an annotation makes that operation match nothing and vanish. Nothing logs
    /// above debug, the app still serves the route, and the tool simply never exists —
    /// which is why the emptiness check above is not enough on its own.
    #[test]
    fn every_documented_path_is_absolute_under_the_mount() {
        for path in openapi().paths.paths.keys() {
            // The mount root itself is allowed even though nothing serves it today,
            // so this asserts "under the mount" and not the narrower "has a subpath".
            assert!(
                path == "/api/social" || path.starts_with("/api/social/"),
                "`{path}` is not under the /api/social mount"
            );
        }
    }

    /// The doc must not describe a route the manifest does not declare, and must not
    /// use axum's `:id` form.
    ///
    /// Core intersects derived operations against `sidecars[].http.routes[]` and DROPS
    /// the rest, so an operation outside that set is wasted work at best. The
    /// placeholder half is the sharper edge: the matcher writes `{id}`, the router
    /// writes `:id`, and mixing them up silently drops the operation.
    #[test]
    fn documented_paths_use_brace_placeholders_and_match_the_router() {
        for path in openapi().paths.paths.keys() {
            assert!(
                !path.contains(':'),
                "`{path}` uses axum's `:param` form; the spec must use `{{param}}`"
            );
        }
    }

    /// Agents draft, schedule and publish through the `ryu_ext` tools Core lowers
    /// from this document. Core keeps an operation only when a manifest route
    /// declares its path (`:param` matches one segment, a trailing `*rest` the
    /// rest) for its method (a route with no method takes any), and drops the rest
    /// with a debug line. A dropped compose operation would leave agents unable to
    /// post at all, and nothing else would fail, so this pins both halves: every
    /// documented operation is declared, and the operations agents publish with
    /// are documented. Those calls reach the app through Core's ext-proxy, which
    /// stamps the calling agent's grant, so Outpost's provider calls carry it.
    #[test]
    fn agent_publishing_operations_survive_the_declared_route_intersection() {
        let manifest: Value =
            serde_json::from_str(include_str!("../../manifest.json")).expect("manifest parses");
        let http = &manifest["sidecars"][0]["http"];
        let mount = http["mount"]
            .as_str()
            .expect("the sidecar declares a mount");
        let routes = http["routes"]
            .as_array()
            .expect("the sidecar declares routes");
        let matches = |pattern: &str, actual: &str| {
            let pattern: Vec<&str> = pattern.trim_start_matches('/').split('/').collect();
            let actual: Vec<&str> = actual.trim_start_matches('/').split('/').collect();
            for (index, segment) in pattern.iter().enumerate() {
                if segment.starts_with('*') {
                    return true;
                }
                let Some(value) = actual.get(index) else {
                    return false;
                };
                if segment.starts_with(':') {
                    if value.is_empty() {
                        return false;
                    }
                } else if segment != value {
                    return false;
                }
            }
            pattern.len() == actual.len()
        };

        let doc = serde_json::to_value(openapi()).expect("the document serializes");
        let paths = doc["paths"].as_object().expect("the document has paths");
        let mut operations = Vec::new();
        for (path, item) in paths {
            for method in ["get", "post", "put", "patch", "delete"] {
                if let Some(operation) = item.get(method) {
                    let id = operation["operationId"].as_str().unwrap_or_default();
                    operations.push((id.to_owned(), method, path.clone()));
                }
            }
        }

        for (id, method, path) in &operations {
            let sub_path = path
                .strip_prefix(mount)
                .unwrap_or_else(|| panic!("`{path}` is not under `{mount}`"));
            let declared = routes.iter().any(|route| {
                matches(route["path"].as_str().unwrap_or_default(), sub_path)
                    && route["method"]
                        .as_str()
                        .is_none_or(|declared| declared.eq_ignore_ascii_case(method))
            });
            assert!(
                declared,
                "`{id}` ({method} {path}) is documented but the manifest declares no route \
                 for it, so Core drops it and agents never get the tool"
            );
        }

        for (id, method, path) in [
            ("create_draft", "post", "/api/social/drafts"),
            ("patch_draft", "patch", "/api/social/drafts/{id}"),
            ("validate_post", "post", "/api/social/posts/validate"),
            ("create_post", "post", "/api/social/posts"),
            ("schedule_post", "post", "/api/social/posts/{id}/schedule"),
            ("publish_now", "post", "/api/social/posts/{id}/publish-now"),
            ("retry_post", "post", "/api/social/posts/{id}/retry"),
            ("cancel_post", "post", "/api/social/posts/{id}/cancel"),
        ] {
            assert!(
                operations
                    .iter()
                    .any(|(seen, seen_method, seen_path)| seen == id
                        && *seen_method == method
                        && seen_path == path),
                "agents need `{id}` ({method} {path}) as a derived tool"
            );
        }
    }

    /// The audit exploit: a caller holding only `social.publish` plus access to one
    /// workspace sent a nested `node` object to the workspace route and rewrote the
    /// install-wide inbox policy and stored account credentials. The workspace route
    /// must now refuse the node half and leave the node blob untouched.
    #[tokio::test]
    async fn workspace_settings_route_refuses_node_wide_changes() {
        let state = state();
        let before = crate::settings::load(&state.store).await.unwrap();
        assert!(before.inbox_polling_enabled);
        assert!(before.bluesky_app_password.is_none());

        let body: PatchSettingsBody = serde_json::from_value(json!({
            "workspace_id": DEFAULT_WORKSPACE_ID,
            "scheduler_enabled": false,
            "node": {
                "inbox_polling_enabled": false,
                "bluesky_handle": "@attacker",
                "bluesky_app_password": "attacker-app-password"
            }
        }))
        .unwrap();
        let refused = patch_settings(State(state.clone()), Json(body)).await;
        assert!(matches!(refused, Err(ApiError::BadRequest(_))));

        let after = crate::settings::load(&state.store).await.unwrap();
        assert!(after.inbox_polling_enabled);
        assert!(after.bluesky_handle.is_none());
        assert!(after.bluesky_app_password.is_none());
        // Refused as a whole: the workspace half of the same body is not applied.
        let workspace = state
            .store
            .get_settings(DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        assert!(workspace.scheduler_enabled);
    }

    #[tokio::test]
    async fn workspace_settings_route_still_patches_workspace_fields() {
        let state = state();
        let body: PatchSettingsBody = serde_json::from_value(json!({
            "scheduler_enabled": false,
            "node": null
        }))
        .unwrap();
        let saved = patch_settings(State(state.clone()), Json(body))
            .await
            .unwrap()
            .0;
        assert_eq!(saved["settings"]["scheduler_enabled"], json!(false));
        assert_eq!(saved["node"]["inbox_polling_enabled"], json!(true));
    }

    #[tokio::test]
    async fn node_settings_route_applies_node_changes_and_redacts_credentials() {
        let state = state();
        let patch: crate::settings::NodeSettingsPatch = serde_json::from_value(json!({
            "inbox_polling_enabled": false,
            "bluesky_app_password": "owner-app-password"
        }))
        .unwrap();
        let response = patch_node_settings(State(state.clone()), Json(patch))
            .await
            .unwrap()
            .0;
        assert_eq!(response["node"]["inbox_polling_enabled"], json!(false));
        assert_eq!(response["node"]["bluesky_app_password_set"], json!(true));
        assert!(!response.to_string().contains("owner-app-password"));
        let stored = crate::settings::load(&state.store).await.unwrap();
        assert!(!stored.inbox_polling_enabled);
    }

    /// Core enforces the manifest's per-route permission before the sidecar sees a
    /// request, so the route split only holds if the manifest keeps the node route on
    /// its own level and `social.publish` never implies it.
    #[test]
    fn manifest_gates_node_settings_on_a_separate_permission() {
        let manifest: Value =
            serde_json::from_str(include_str!("../../manifest.json")).expect("manifest JSON");
        let routes = manifest["sidecars"][0]["http"]["routes"]
            .as_array()
            .expect("routes");
        let permission_for = |path: &str, method: &str| {
            routes
                .iter()
                .find(|route| route["path"] == path && route["method"] == method)
                .and_then(|route| route["permission"].as_str())
                .map(str::to_owned)
        };
        assert_eq!(
            permission_for("/settings", "PATCH").as_deref(),
            Some("social.publish")
        );
        assert_eq!(
            permission_for("/node-settings", "PATCH").as_deref(),
            Some("social.manage-node")
        );

        let levels = manifest["permission_levels"].as_array().expect("levels");
        let implies = |id: &str| -> Vec<String> {
            levels
                .iter()
                .find(|level| level["id"] == id)
                .and_then(|level| level["implies"].as_array())
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut reachable = vec!["social.publish".to_owned()];
        let mut index = 0;
        while index < reachable.len() {
            for next in implies(&reachable[index]) {
                if !reachable.contains(&next) {
                    reachable.push(next);
                }
            }
            index += 1;
        }
        assert!(!reachable.iter().any(|level| level == "social.manage-node"));
        assert!(levels
            .iter()
            .any(|level| level["id"] == "social.manage-node"));
    }

    #[test]
    fn preview_truncates_on_characters_not_bytes() {
        assert_eq!(preview("  hi  "), "hi");
        let long = "🎉".repeat(200);
        let out = preview(&long);
        assert_eq!(out.chars().count(), CALENDAR_TITLE_CHARS + 1);
    }
}

#[cfg(test)]
#[path = "api_flow_tests.rs"]
mod flow_tests;
