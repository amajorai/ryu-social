//! Templates and the media library.

use super::*;

// ── Templates ──────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/templates",
    tag = "Social",
    summary = "list reusable post templates (starter templates are seeded on first read).",
    params(("workspace_id" = Option<String>, Query, description = "scope; omit for the default workspace")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn list_templates(
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
pub(super) struct CreateTemplateBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// The template's display name. Required and non-blank.
    pub(super) name: String,
    /// The template's starting text and any per-platform variants.
    #[serde(default)]
    pub(super) body: TemplateBody,
}

#[utoipa::path(
    post,
    path = "/api/social/templates",
    tag = "Social",
    summary = "save a reusable post template.",
    request_body = CreateTemplateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn create_template(
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
pub(super) async fn get_template(
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
pub(super) struct PatchTemplateBody {
    /// New display name. Omit to leave it unchanged.
    #[serde(default)]
    pub(super) name: Option<String>,
    /// New content. Omit to leave it unchanged.
    // Inlined for the same reason as `CreatePostBody::body`: the nullable wrapper
    // would hide `text` / `platform_defaults` behind a `$ref`.
    #[schema(inline)]
    #[serde(default)]
    pub(super) body: Option<TemplateBody>,
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
pub(super) async fn patch_template(
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
pub(super) async fn delete_template(
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
pub(super) async fn use_template(
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
pub(super) async fn list_media(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let media = state.store.list_media(scope.workspace()).await?;
    Ok(Json(json!({ "media": media })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(super) struct CreateMediaBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// A LOCAL absolute path. Never copied — the library holds a reference.
    pub(super) path: String,
    /// Display name. Defaults to the file name in `path`.
    #[serde(default)]
    pub(super) name: Option<String>,
    /// e.g. `image/png`. Inferred from the extension when omitted.
    #[serde(default)]
    pub(super) mime_type: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/social/media",
    tag = "Social",
    summary = "add a local file to the workspace's media library so posts can attach it.",
    request_body = CreateMediaBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn create_media(
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
pub(super) async fn delete_media(
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
