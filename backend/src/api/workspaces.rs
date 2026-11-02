//! Workspaces and connected accounts.

use super::*;

// ── Workspaces ─────────────────────────────────────────────────────────────────

#[utoipa::path(
    get,
    path = "/api/social/workspaces",
    tag = "Social",
    summary = "list the social workspaces (brands/clients) on this node.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn list_workspaces(
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
pub(super) struct NameBody {
    /// The workspace's display name. Required and non-blank.
    pub(super) name: String,
}

#[utoipa::path(
    post,
    path = "/api/social/workspaces",
    tag = "Social",
    summary = "create a social workspace.",
    request_body = NameBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn create_workspace(
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
pub(super) async fn get_workspace(
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
pub(super) async fn patch_workspace(
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
pub(super) async fn delete_workspace(
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
pub(super) async fn list_accounts(
    State(state): State<AppState>,
    Query(scope): Query<ScopeQuery>,
) -> ApiResult<Json<Value>> {
    let accounts = state.store.list_accounts(scope.workspace()).await?;
    Ok(Json(json!({ "accounts": accounts })))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(super) struct CreateAccountBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// One of: x, instagram, tiktok, youtube, linkedin, reddit, facebook, bluesky,
    /// threads. An unknown value is rejected.
    pub(super) platform: String,
    /// How the account is shown in pickers, e.g. `@acme`.
    pub(super) account_label: String,
    /// The platform's own id for the account, when it is already known. Normally left
    /// out — the connect handshake fills it in.
    #[serde(default)]
    pub(super) external_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/social/accounts",
    tag = "Social",
    summary = "register a social account in a workspace (does not connect it yet).",
    request_body = CreateAccountBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn create_account(
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
pub(super) async fn get_account(
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
pub(super) async fn delete_account(
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
pub(super) async fn connect_account(
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
pub(super) async fn account_capabilities(
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
