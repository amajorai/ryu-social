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

mod auth;
mod calendar;
mod drafts;
mod inbox_routes;
mod library;
mod posts;
mod setup;
mod workspaces;

use auth::*;
use calendar::*;
use drafts::*;
use inbox_routes::*;
use library::*;
use posts::*;
use setup::*;
use workspaces::*;

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

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "../api_flow_tests.rs"]
mod flow_tests;
