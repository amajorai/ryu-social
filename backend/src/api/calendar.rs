//! Calendar, queue and history projections.

use super::*;

// ── Calendar ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub(super) struct RangeQuery {
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// Epoch millis, inclusive.
    pub(super) from: i64,
    /// Epoch millis, EXCLUSIVE — so consecutive buckets tile the timeline with no
    /// post appearing in two of them.
    pub(super) to: i64,
}

/// What kind of thing sits on the calendar at a given moment.
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum CalendarKind {
    /// A post the scheduler still owes work on.
    Scheduled,
    /// Something that already went out, from the engagement snapshot table.
    Published,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct CalendarEntry {
    pub(super) id: String,
    pub(super) kind: CalendarKind,
    /// The moment this sits at, epoch millis.
    pub(super) at: i64,
    pub(super) status: Option<PostStatus>,
    pub(super) platforms: Vec<Platform>,
    /// A short preview so a calendar cell can render without a second fetch.
    pub(super) title: String,
    pub(super) post_id: Option<String>,
    pub(super) permalink: Option<String>,
}

/// How much draft text a calendar cell gets.
pub(super) const CALENDAR_TITLE_CHARS: usize = 120;

pub(super) fn preview(text: &str) -> String {
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
pub(super) async fn calendar(
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
pub(super) async fn queue(
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
pub(super) struct BestTimesQuery {
    #[serde(default)]
    pub(super) workspace_id: Option<String>,
    /// Rank slots using only this platform's history. Absent means "all platforms",
    /// which is the right default for a compose box that has not picked accounts yet.
    #[serde(default)]
    pub(super) platform: Option<String>,
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
pub(super) async fn best_times(
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
pub(super) async fn list_history(
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
pub(super) async fn get_history(
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
pub(super) async fn refresh_engagement(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<ActivityItem>> {
    Ok(Json(
        crate::analytics::refresh_engagement(&state, &id, agent_grant(&headers).as_deref()).await?,
    ))
}
