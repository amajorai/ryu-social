//! The unified inbox.

use super::*;

// ── Inbox ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub(super) struct InboxQuery {
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) account_id: Option<String>,
    #[serde(default)]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) unread: Option<bool>,
    #[serde(default)]
    pub(super) unreplied: Option<bool>,
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
pub(super) async fn list_inbox(
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
pub(super) struct RefreshInboxBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// Refresh one account instead of every connected account in the workspace.
    #[serde(default)]
    pub(super) account_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/social/inbox/refresh",
    tag = "Social",
    summary = "pull new inbox items from the platforms right now.",
    request_body = RefreshInboxBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn refresh_inbox(
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
pub(super) struct ReplyBody {
    /// The reply to publish. Required and non-blank.
    pub(super) text: String,
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
pub(super) async fn reply_inbox(
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
pub(super) struct ReadBody {
    /// Absent means "mark read". Pass `false` to mark it unread again.
    #[serde(default)]
    pub(super) read: Option<bool>,
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
pub(super) async fn read_inbox(
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
