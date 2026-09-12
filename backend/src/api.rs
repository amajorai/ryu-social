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
    extract::{Path, Query, State},
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
        // ── Read models ──
        .route("/activity", get(list_activity))
        .route("/settings", get(get_settings).patch(patch_settings))
        .route("/platforms", get(list_platforms))
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
async fn list_workspaces(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let workspaces = state.store.list_workspaces().await?;
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
    Json(body): Json<NameBody>,
) -> ApiResult<Json<Workspace>> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("a workspace needs a name"));
    }
    Ok(Json(state.store.create_workspace(&body.name).await?))
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
        .connect(&crate::providers::ProviderAccount {
            id: account.id.clone(),
            platform: account.platform,
            label: Some(account.account_label.clone()),
            external_id: account.external_id.clone(),
        })
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
    Json(payload): Json<DraftBodyPayload>,
) -> ApiResult<Json<Draft>> {
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
        .create_scheduled_post(&workspace, draft_id.as_deref(), scheduled_for, &targets)
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
    Json(body): Json<RescheduleBody>,
) -> ApiResult<Json<ScheduledPost>> {
    reschedule(state, id, body.scheduled_for).await
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
    Json(body): Json<RescheduleBody>,
) -> ApiResult<Json<ScheduledPost>> {
    reschedule(state, id, body.scheduled_for).await
}

async fn reschedule(
    state: AppState,
    id: String,
    scheduled_for: i64,
) -> ApiResult<Json<ScheduledPost>> {
    let post = state
        .store
        .get_scheduled_post(&id)
        .await?
        .ok_or_else(|| ApiError::not_found("post"))?;
    if !state.store.reschedule_post(&id, scheduled_for).await? {
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
) -> ApiResult<Json<ScheduledPost>> {
    Ok(Json(crate::publish::queue_now(&state, &id).await?))
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
) -> ApiResult<Json<ScheduledPost>> {
    Ok(Json(crate::publish::queue_retry(&state, &id).await?))
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
) -> ApiResult<Json<ActivityItem>> {
    Ok(Json(
        crate::analytics::refresh_engagement(&state, &id).await?,
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
    Json(body): Json<RefreshInboxBody>,
) -> ApiResult<Json<crate::inbox::RefreshSummary>> {
    let workspace = body
        .workspace_id
        .as_deref()
        .filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID);
    Ok(Json(
        crate::inbox::refresh(&state, workspace, body.account_id.as_deref()).await?,
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
    Json(body): Json<ReplyBody>,
) -> ApiResult<Json<InboxItem>> {
    if body.text.trim().is_empty() {
        return Err(ApiError::bad_request("a reply needs text"));
    }
    Ok(Json(crate::inbox::reply(&state, &id, &body.text).await?))
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
            body.path
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or(&body.path)
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
            .upsert_media(workspace, &body.path, &name, mime.as_deref())
            .await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/social/media/{id}",
    tag = "Social",
    summary = "drop a media asset from the library (the file on disk is untouched).",
    params(("id" = String, Path, description = "media asset id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_media(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_hit(state.store.delete_media(&id).await?, "media asset")?;
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
    /// The node-scoped half, absent when a caller is only editing workspace settings.
    ///
    /// A nested object rather than flattened siblings, because this is the ONE place
    /// account credentials enter the process and having them under a named key makes
    /// that boundary greppable. Provider credentials are managed by Gateway.
    #[serde(default)]
    node: Option<crate::settings::NodeSettingsPatch>,
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

    // The node half is applied second and read back unconditionally, so the response
    // is always the complete current state — a client that patched only the workspace
    // half still gets the node view and does not need a follow-up GET.
    if let Some(node_patch) = patch.node {
        let node = crate::settings::patch(&state.store, node_patch).await?;
        return Ok(Json(json!({ "settings": saved, "node": node })));
    }
    let node = crate::settings::load(&state.store).await?.redacted();
    Ok(Json(json!({ "settings": saved, "node": node })))
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
            SocialStore::open_in_memory().expect("in-memory store"),
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

    #[test]
    fn every_declared_post_status_string_parses_back() {
        // Guards the `?status=` filter: a value in KNOWN_POST_STATUSES that does not
        // round-trip would be silently coerced to `failed` by `from_db`.
        for s in KNOWN_POST_STATUSES {
            assert_eq!(PostStatus::from_db(s).as_str(), *s);
        }
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

    #[test]
    fn preview_truncates_on_characters_not_bytes() {
        assert_eq!(preview("  hi  "), "hi");
        let long = "🎉".repeat(200);
        let out = preview(&long);
        assert_eq!(out.chars().count(), CALENDAR_TITLE_CHARS + 1);
    }
}
