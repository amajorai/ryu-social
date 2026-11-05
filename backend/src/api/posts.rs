//! Scheduled posts: create, reschedule, cancel, publish now and retry.

use super::*;

// ── Scheduled posts ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub(super) struct PostListQuery {
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// Comma-separated statuses, e.g. `?status=scheduled,due`. Unknown values are
    /// dropped rather than rejected: a filter chip from a newer UI must not 400 an
    /// older sidecar.
    #[serde(default)]
    pub(super) status: Option<String>,
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
pub(super) async fn list_posts(
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

pub(super) const KNOWN_POST_STATUSES: &[&str] = &[
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
pub(super) struct CreatePostBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// Schedule an existing draft. Give this OR `body`, not both.
    #[serde(default)]
    pub(super) draft_id: Option<String>,
    /// Inline content. Used only when `draft_id` is absent.
    // `schema(inline)` because a bare `Option<DraftBody>` renders as a nullable
    // wrapper around a `$ref`, which buries the real fields (text/segments/media) a
    // caller has to fill in — see the same treatment on `PatchTemplateBody::body`.
    #[schema(inline)]
    #[serde(default)]
    pub(super) body: Option<DraftBody>,
    /// Epoch millis. Absent means NOW — "post now" is not a separate concept, it is
    /// a schedule whose time has already arrived.
    #[serde(default)]
    pub(super) scheduled_for: Option<i64>,
    /// The accounts to fan out to. At least one is required.
    pub(super) account_ids: Vec<String>,
    /// Optional per-account content overrides, keyed by account id. A full draft
    /// body, so an override keeps that target's media and thread structure.
    #[serde(default)]
    pub(super) variants: BTreeMap<String, DraftBody>,
}

#[utoipa::path(
    post,
    path = "/api/social/posts",
    tag = "Social",
    summary = "schedule a post to one or more accounts (omit scheduled_for to post now).",
    request_body = CreatePostBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub(super) async fn create_post(
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
pub(super) async fn get_post(
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
pub(super) struct RescheduleBody {
    /// The new send time, epoch millis UTC.
    pub(super) scheduled_for: i64,
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
pub(super) async fn patch_post(
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
pub(super) async fn schedule_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<RescheduleBody>,
) -> ApiResult<Json<ScheduledPost>> {
    reschedule(state, id, body.scheduled_for, agent_grant(&headers)).await
}

pub(super) async fn reschedule(
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
        return Err(crate::publish::cannot_reschedule(post.status));
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
pub(super) async fn cancel_post(
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
        return Err(crate::publish::cannot_cancel(post.status));
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
pub(super) async fn publish_now(
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
pub(super) async fn retry_post(
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
pub(super) async fn delete_post(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    require_hit(state.store.delete_scheduled_post(&id).await?, "post")?;
    Ok(Json(json!({ "ok": true })))
}
