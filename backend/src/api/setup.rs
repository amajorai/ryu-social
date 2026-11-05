//! Media import sessions, activity, settings and platforms.

use super::*;

// ── Media import (upload sessions into the media root) ────────────────────────

pub(super) fn import_workspace(raw: Option<&str>) -> &str {
    raw.filter(|w| !w.trim().is_empty())
        .unwrap_or(DEFAULT_WORKSPACE_ID)
}

pub(super) fn import_error(error: crate::media_import::ImportError) -> ApiError {
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
pub(super) async fn start_media_import(
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
pub(super) async fn append_media_import(
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
pub(super) async fn finish_media_import(
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
pub(super) async fn abort_media_import(
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
pub(super) async fn list_activity(
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
pub(super) async fn get_settings(
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
pub(super) struct PatchSettingsBody {
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    #[serde(default)]
    pub(super) scheduler_enabled: Option<bool>,
    #[serde(default)]
    pub(super) poll_interval_secs: Option<u64>,
    #[serde(default)]
    pub(super) max_attempts: Option<u32>,
    #[serde(default)]
    pub(super) base_backoff_ms: Option<u64>,
    #[serde(default)]
    pub(super) claim_lease_secs: Option<u64>,
    #[serde(default)]
    pub(super) timezone: Option<String>,
    #[serde(default)]
    pub(super) enforce_platform_limits: Option<bool>,
    /// Captured only so a node-scoped half sent to this workspace route is REFUSED
    /// rather than silently dropped. Node-wide policy and account credentials are
    /// install-wide, so they are changed through `PATCH /node-settings`, which Core
    /// gates on the separate `social.manage-node` permission. Workspace access plus
    /// `social.publish` (this route's gate) is not authority over the whole node.
    #[serde(default)]
    pub(super) node: Option<serde::de::IgnoredAny>,
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
pub(super) async fn patch_settings(
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
pub(super) async fn patch_node_settings(
    State(state): State<AppState>,
    Json(patch): Json<crate::settings::NodeSettingsPatch>,
) -> ApiResult<Json<Value>> {
    let node = crate::settings::patch(&state.store, patch).await?;
    Ok(Json(json!({ "node": node })))
}

// ── Platforms ──────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
pub(super) struct PlatformInfo {
    pub(super) platform: Platform,
    pub(super) label: String,
    pub(super) limits: PlatformLimits,
    pub(super) capabilities: PlatformCapabilities,
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
pub(super) async fn list_platforms(State(state): State<AppState>) -> ApiResult<Json<Value>> {
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
