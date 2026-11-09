//! Drafts and compose validation.

use super::*;

// ── Drafts ─────────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/drafts",
    tag = "Social",
    summary = "list unscheduled post drafts in a workspace.",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn list_drafts(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let drafts = state.store.list_drafts(scope.workspace()).await?;
    Ok(Json(json!({ "drafts": drafts })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(super) struct DraftBodyPayload {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// The post content.
    #[serde(default)]
    pub(super) body: DraftBody,
}

#[utoipa::path(
    post,
    path = "/api/social/drafts",
    tag = "Social",
    summary = "save post content as a draft, without scheduling it.",
    request_body = DraftBodyPayload,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn create_draft(
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
pub(super) async fn get_draft(
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
pub(super) async fn patch_draft(
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
pub(super) async fn delete_draft(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_hit(state.store.delete_draft(&id).await?, "draft")?;
    Ok(Json(json!({ "ok": true })))
}

// ── Compose validation ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(super) struct ValidateBody {
    /// Platform keys to check against, e.g. `["x", "bluesky"]`. An unknown key is
    /// checked against generous default limits rather than rejected.
    pub(super) platforms: Vec<String>,
    /// The post content to measure, one entry per thread segment.
    #[serde(default)]
    pub(super) segments: Vec<PostSegment>,
    /// Scope; omit for the default workspace.
    // The workspace is what decides whether an over-limit result BLOCKS or merely
    // warns (`enforce_platform_limits`). A settings read that fails falls back to
    // enforcing — the stricter side, so a storage hiccup can never silently unblock
    // a bad post.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct ValidationResult {
    pub(super) platform: String,
    pub(super) label: String,
    /// Whether the composer may proceed. Equals `reason.is_none()` while enforcement
    /// is on; always true while it is off, where `reason` degrades to advice.
    pub(super) ok: bool,
    /// The FIRST limit violation, or null. Populated regardless of enforcement so the
    /// composer can warn even when it is not blocking. First-only because the composer
    /// shows one inline message per platform chip and a user fixes one thing at a time.
    pub(super) reason: Option<String>,
    pub(super) limits: PlatformLimits,
    /// Character count of segment 0, so the composer can render its counter from the
    /// same numbers the check used instead of re-deriving them.
    pub(super) chars: usize,
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
pub(super) async fn validate_post(
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
