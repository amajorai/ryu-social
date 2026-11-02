//! Caller identity, agent grants and the workspace authorization gate.

use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct CallerIdentity {
    pub(super) user_id: Option<String>,
    pub(super) org_id: Option<String>,
}

pub(super) fn caller_identity(headers: &HeaderMap) -> CallerIdentity {
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
pub(super) fn agent_grant(headers: &HeaderMap) -> Option<String> {
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
pub(super) fn scope_workspace(
    method: &Method,
    uri: &Uri,
    body: Option<&Value>,
) -> Result<String, ApiError> {
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

pub(super) async fn entity_workspace(
    state: &AppState,
    segments: &[&str],
) -> anyhow::Result<Option<String>> {
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

pub(super) async fn request_workspace(
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

pub(super) async fn authorize_request(
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
