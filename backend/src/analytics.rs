//! Engagement reads and the projections built on them.
//!
//! ## The honesty constraint this module works under
//!
//! `activity_items` is a LATEST-SNAPSHOT table, not a time series: one row per remote
//! post, counts overwritten on each refresh. There is no history of how a post's
//! engagement grew, and no follower count anywhere in the schema.
//!
//! That bounds what analytics may claim, and it is why nothing below is named
//! "engagement over time". [`AnalyticsRollups::published_by_day`] buckets posts by
//! the day they were PUBLISHED and reports each post's CURRENT totals — it is a
//! histogram of output with today's numbers attached, not a curve of how those
//! numbers moved. A "best day to post" read off it means *"when the content that
//! performed was published"*; it cannot distinguish a post that did well from a post
//! published when the audience happened to be large. Every projection therefore
//! carries a `basis` the UI must render, so a thin-sample answer never implies a
//! learned model.
//!
//! ## Refresh: batched, paced, and never on the scheduler's thread
//!
//! [`refresh_engagement`] is the single-post path behind
//! `POST /history/:id/refresh-engagement`. [`refresh_workspace_engagement`] is the
//! batched one, and it is deliberately not wired into the publish tick: an engagement
//! sweep is dozens of third-party calls, and a tick that waited for them would delay
//! every scheduled post behind an analytics refresh. It runs on its own slow task
//! ([`spawn_refresher`]) with a pause between calls, a per-pass cap, and a minimum
//! age below which a post is not re-read at all.

use std::time::Duration;

use serde::Serialize;

use crate::error::{ApiError, ApiResult};
use crate::models::{
    new_id, now_ms, ActivityItem, EngagementCounts, HistoryStatus, Platform, PostHistoryEntry,
    ID_ACTIVITY,
};
use crate::providers::RemotePostRef;
use crate::state::AppState;

/// Pause between provider calls in a batched pass. Not a rate limiter — it is a
/// courtesy pace that keeps a 25-post sweep from arriving as a 25-request burst,
/// which is what trips a per-second limit even when the per-hour budget is fine.
const BATCH_PACE: Duration = Duration::from_millis(250);

/// A snapshot younger than this is not re-read. Engagement moves over hours and days;
/// re-reading a post someone refreshed ten minutes ago spends a third-party call to
/// learn nothing.
const MIN_REFRESH_AGE_MS: i64 = 30 * 60 * 1_000;

/// The single ranking scalar: likes + comments + shares.
///
/// **Views are deliberately excluded.** They are an impression count, not
/// engagement — a post shown to a large audience that nobody interacted with would
/// otherwise outrank a post that started a conversation, which inverts the thing
/// the ranking is for. Platforms also disagree wildly on what counts as a view,
/// making the number incomparable across the very platforms this ranks together.
pub fn engagement_score(counts: &EngagementCounts) -> u64 {
    counts.likes.unwrap_or(0) + counts.comments.unwrap_or(0) + counts.shares.unwrap_or(0)
}

/// The same scalar for a stored snapshot. Routed through [`engagement_score`] rather
/// than re-adding the three fields, so there is exactly ONE definition of what
/// engagement means and a change to it cannot apply to half the surfaces.
pub fn item_engagement(item: &ActivityItem) -> u64 {
    engagement_score(&EngagementCounts {
        likes: Some(item.likes),
        comments: Some(item.comments),
        shares: Some(item.shares),
        views: Some(item.views),
        fetched_at: item.engagement_fetched_at.unwrap_or(0),
    })
}

// ── Refresh ────────────────────────────────────────────────────────────────────

/// Re-read a published post's metrics from its platform and refresh the snapshot.
///
/// Resolves `history → target → post` to recover the workspace, the account and the
/// platform (a history row carries none of them), asks the provider for counts, and
/// upserts. The upsert's COALESCE split is load-bearing and lives in
/// [`crate::store::SocialStore::upsert_activity`]: **counts overwrite
/// unconditionally, metadata only fills in**, so a metrics-only read cannot blank a
/// permalink or a body it does not carry.
pub async fn refresh_engagement(state: &AppState, history_id: &str) -> ApiResult<ActivityItem> {
    let entry = state
        .store
        .get_history(history_id)
        .await?
        .ok_or_else(|| ApiError::not_found("history entry"))?;
    refresh_one(state, &entry).await
}

async fn refresh_one(state: &AppState, entry: &PostHistoryEntry) -> ApiResult<ActivityItem> {
    // A failed publish has no remote post to address. Caught here rather than handed
    // to a provider, which would turn "this never published" into an upstream 4xx.
    let remote_id = entry
        .remote_id
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .ok_or_else(|| {
            ApiError::conflict("this publish never reached the platform, so it has no metrics")
        })?;

    let target = state
        .store
        .get_target(&entry.post_target_id)
        .await?
        .ok_or_else(|| ApiError::not_found("post target"))?;
    let post = state
        .store
        .get_scheduled_post(&target.scheduled_post_id)
        .await?
        .ok_or_else(|| ApiError::not_found("scheduled post"))?;

    let caps = state.providers.capabilities_for(target.platform).await;
    if !caps.read_engagement {
        return Err(ApiError::conflict(format!(
            "reading engagement on {} is not supported by the configured provider",
            target.platform.label()
        )));
    }

    let counts = state
        .providers
        .provider_for(target.platform)
        .read_engagement(&RemotePostRef {
            platform: target.platform,
            remote_id: remote_id.to_string(),
            remote_url: entry.remote_url.clone(),
        })
        .await
        .map_err(|e| ApiError::upstream(e.to_string()))?;

    // Body text, best effort: the per-target override wins over the draft, matching
    // what was actually published. `None` is fine — COALESCE keeps whatever text the
    // row already had rather than blanking it.
    let text = resolve_body_text(state, &target, &post).await;

    let item = ActivityItem {
        // Only used when this is the FIRST snapshot for this remote post; on conflict
        // the existing row keeps its id.
        id: new_id(ID_ACTIVITY),
        workspace_id: post.workspace_id.clone(),
        social_account_id: target.social_account_id.clone(),
        platform: target.platform,
        post_remote_id: remote_id.to_string(),
        permalink: entry.remote_url.clone(),
        text,
        likes: counts.likes.unwrap_or(0),
        comments: counts.comments.unwrap_or(0),
        shares: counts.shares.unwrap_or(0),
        views: counts.views.unwrap_or(0),
        engagement_fetched_at: Some(counts.fetched_at),
        published_at: entry.published_at,
    };
    state.store.upsert_activity(&item).await?;

    // Re-read rather than returning what we sent: the stored row is the merge of this
    // read and everything already known, and the caller should see the merge.
    state
        .store
        .get_activity_by_remote(&post.workspace_id, &target.social_account_id, remote_id)
        .await?
        .ok_or_else(|| ApiError::not_found("activity item"))
}

/// The text that was published on this target, if it can still be recovered.
async fn resolve_body_text(
    state: &AppState,
    target: &crate::models::PostTarget,
    post: &crate::models::ScheduledPost,
) -> Option<String> {
    if let Some(variant) = &target.variant_body {
        let text = variant.text.trim();
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    let draft_id = post.draft_id.as_deref()?;
    let draft = state.store.get_draft(draft_id).await.ok().flatten()?;
    let text = draft.body.text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// What one batched pass did.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EngagementRefreshSummary {
    /// Published history entries considered.
    pub considered: usize,
    /// Provider calls actually made.
    pub refreshed: usize,
    /// Skipped because the stored snapshot was younger than [`MIN_REFRESH_AGE_MS`],
    /// or because the same remote post appeared twice in the window.
    pub skipped: usize,
    pub errors: Vec<String>,
}

/// Refresh the engagement snapshots of a workspace's recently published posts.
///
/// Bounded three ways, because every iteration is a billable third-party call: at most
/// `limit` refreshes per pass, nothing re-read inside [`MIN_REFRESH_AGE_MS`], and a
/// [`BATCH_PACE`] pause between calls. A per-post failure is captured and the pass
/// continues — one dead post must not cost the rest of the sweep.
pub async fn refresh_workspace_engagement(
    state: &AppState,
    workspace_id: &str,
    limit: usize,
) -> ApiResult<EngagementRefreshSummary> {
    // A generous read window, then filtered in memory: `list_history` is already
    // workspace-joined and capped, and a dedicated "published only" query would be a
    // second index for one caller.
    let history = state.store.list_history(workspace_id, 500).await?;
    let now = now_ms();

    let mut summary = EngagementRefreshSummary::default();
    let mut seen: Vec<String> = Vec::new();

    for entry in history {
        if summary.refreshed >= limit {
            break;
        }
        if entry.status != HistoryStatus::Published {
            continue;
        }
        let Some(remote_id) = entry
            .remote_id
            .as_deref()
            .map(str::trim)
            .filter(|r| !r.is_empty())
        else {
            continue;
        };
        summary.considered += 1;

        // One remote post can have several history rows (a retry writes another).
        // Refreshing each would spend N calls for one post's counts.
        if seen.iter().any(|r| r == remote_id) {
            summary.skipped += 1;
            continue;
        }
        seen.push(remote_id.to_string());

        if is_fresh_enough(state, workspace_id, &entry, remote_id, now).await {
            summary.skipped += 1;
            continue;
        }

        match refresh_one(state, &entry).await {
            Ok(_) => summary.refreshed += 1,
            Err(e) => summary.errors.push(format!("{remote_id}: {e}")),
        }
        tokio::time::sleep(BATCH_PACE).await;
    }
    Ok(summary)
}

/// Has this post been read recently enough to skip?
///
/// Resolving the target just to find the account id costs one indexed lookup and
/// saves a network call, which is the trade worth making.
async fn is_fresh_enough(
    state: &AppState,
    workspace_id: &str,
    entry: &PostHistoryEntry,
    remote_id: &str,
    now: i64,
) -> bool {
    let Ok(Some(target)) = state.store.get_target(&entry.post_target_id).await else {
        return false;
    };
    let Ok(Some(existing)) = state
        .store
        .get_activity_by_remote(workspace_id, &target.social_account_id, remote_id)
        .await
    else {
        return false;
    };
    existing
        .engagement_fetched_at
        .is_some_and(|at| now.saturating_sub(at) < MIN_REFRESH_AGE_MS)
}

/// Start the slow engagement-refresh loop. Returns the handle so `main` can abort it.
///
/// Separate from the scheduler tick on purpose: this task sleeps for hours at a time
/// and makes dozens of network calls when it wakes, and folding it into the 30-second
/// publish tick would put every scheduled post behind an analytics sweep.
pub fn spawn_refresher(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let settings = match crate::settings::load(&state.store).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "ryu-social: engagement refresher could not read settings");
                    tokio::time::sleep(Duration::from_secs(3_600)).await;
                    continue;
                }
            };
            let period = settings.engagement_refresh_period();

            // Quiet hours cover outbound work generally; an engagement read is
            // outbound and can wait until morning.
            if !settings.in_quiet_hours(now_ms()) {
                let workspaces = state.store.list_workspaces().await.unwrap_or_default();
                for workspace in workspaces {
                    match refresh_workspace_engagement(
                        &state,
                        &workspace.id,
                        settings.engagement_refresh_batch,
                    )
                    .await
                    {
                        Ok(summary) if summary.refreshed > 0 || !summary.errors.is_empty() => {
                            tracing::info!(
                                workspace = %workspace.id,
                                refreshed = summary.refreshed,
                                skipped = summary.skipped,
                                errors = summary.errors.len(),
                                "ryu-social: engagement refresh"
                            );
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(
                            workspace = %workspace.id,
                            error = %e,
                            "ryu-social: engagement refresh failed"
                        ),
                    }
                }
            }
            tokio::time::sleep(period).await;
        }
    })
}

// ── Rollups ────────────────────────────────────────────────────────────────────
//
// Everything below is PURE: `&[ActivityItem]` in, plain data out, no clock read and
// no store access. That is what makes them unit-testable without a database and what
// lets the `/activity` handler compute them inline instead of maintaining a second
// materialized table that could disagree with the first.

/// How much a projection can be trusted. Rendered by the UI, not decoration: a
/// cadence computed from two posts is arithmetic, not a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Basis {
    /// Enough dated posts to describe a habit.
    Observed,
    /// Below [`MIN_CADENCE_SAMPLE`] — the numbers are shown, the pattern is not
    /// claimed.
    Insufficient,
}

/// Dated posts needed before a cadence is described as observed rather than
/// arithmetic.
pub const MIN_CADENCE_SAMPLE: usize = 4;

/// How many posts the "best performing" list carries.
pub const TOP_POSTS: usize = 10;

/// Summed counts over a set of posts.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct EngagementTotals {
    pub posts: usize,
    pub likes: u64,
    pub comments: u64,
    pub shares: u64,
    pub views: u64,
    /// likes + comments + shares. Views excluded — see [`engagement_score`].
    pub engagement: u64,
    /// Engagement per post. `0.0` for an empty set rather than NaN, so the field is
    /// always renderable.
    pub avg_engagement: f64,
}

impl EngagementTotals {
    fn add(&mut self, item: &ActivityItem) {
        self.posts += 1;
        self.likes += item.likes;
        self.comments += item.comments;
        self.shares += item.shares;
        self.views += item.views;
        self.engagement += item_engagement(item);
    }

    fn finish(mut self) -> Self {
        self.avg_engagement = if self.posts == 0 {
            0.0
        } else {
            self.engagement as f64 / self.posts as f64
        };
        self
    }
}

/// One platform's totals plus its share of the workspace's engagement.
#[derive(Debug, Clone, Serialize)]
pub struct PlatformRollup {
    pub platform: Platform,
    pub label: String,
    #[serde(flatten)]
    pub totals: EngagementTotals,
    /// 0.0–1.0 of total engagement. `0.0` when nothing has engagement at all, which
    /// is honest: a share of nothing is not 100%.
    pub share_of_engagement: f64,
}

/// One post in the best-performing list.
#[derive(Debug, Clone, Serialize)]
pub struct TopPost {
    #[serde(flatten)]
    pub item: ActivityItem,
    pub engagement: u64,
}

/// One local day's output.
///
/// **This is not an engagement time series.** `posts` is how many posts were
/// PUBLISHED that day and the counts are those posts' CURRENT totals as of the last
/// refresh — the schema stores no history of how they grew.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DayBucket {
    /// Local `YYYY-MM-DD`.
    pub day: String,
    /// UTC millis of that local day's midnight, so a chart can plot without
    /// re-parsing the label.
    pub day_start_ms: i64,
    pub posts: usize,
    pub likes: u64,
    pub comments: u64,
    pub shares: u64,
    pub views: u64,
    pub engagement: u64,
}

/// When a workspace actually posts.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CadenceSummary {
    /// Posts with a known `published_at`. Undated snapshots are excluded from every
    /// number here rather than defaulted to the epoch, which would invent a Thursday
    /// in 1970.
    pub dated_posts: usize,
    pub first_published_at: Option<i64>,
    pub last_published_at: Option<i64>,
    /// Inclusive span in local days between the first and last post.
    pub days_covered: u32,
    /// Posts per 7 days over that span. `0.0` when the span is unknown.
    pub posts_per_week: f64,
    /// Counts by local day of week, MONDAY FIRST (index 0 = Monday).
    pub by_day_of_week: [u32; 7],
    /// Counts by local hour, 0–23.
    pub by_hour: [u32; 24],
    /// The modal day/hour, or `None` when nothing is dated. Ties resolve to the
    /// earliest index — arbitrary, but deterministic, which matters more.
    pub busiest_day_of_week: Option<u8>,
    pub busiest_hour: Option<u32>,
    pub basis: Basis,
}

/// Everything `/activity` reports alongside the raw rows.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsRollups {
    pub totals: EngagementTotals,
    /// Platforms with at least one post, best-engaging first.
    pub by_platform: Vec<PlatformRollup>,
    /// Best-performing posts, engagement descending, capped at [`TOP_POSTS`].
    pub top_posts: Vec<TopPost>,
    /// Output per local day, oldest first. See [`DayBucket`] for what this is NOT.
    pub published_by_day: Vec<DayBucket>,
    pub cadence: CadenceSummary,
    /// The fixed UTC offset the local bucketing used, echoed so a client can label
    /// the axis without guessing.
    pub utc_offset_minutes: i32,
}

/// Local calendar parts of an instant under a fixed UTC offset.
fn local_parts(at_ms: i64, utc_offset_minutes: i32) -> Option<chrono::NaiveDateTime> {
    chrono::DateTime::from_timestamp_millis(at_ms)
        .map(|dt| (dt + chrono::TimeDelta::minutes(i64::from(utc_offset_minutes))).naive_utc())
}

/// Compute every projection in one pass over the snapshot rows.
///
/// `utc_offset_minutes` is a fixed offset, not an IANA zone: this crate has no zone
/// database, and treating `Europe/Berlin` as UTC would be a wrong answer presented as
/// a right one. The offset used is echoed back in the result.
pub fn rollups(items: &[ActivityItem], utc_offset_minutes: i32) -> AnalyticsRollups {
    use chrono::Datelike;
    use chrono::Timelike;

    let mut totals = EngagementTotals::default();
    // Keyed by platform, in `Platform::ALL` order so the UI's chip order is stable
    // across refreshes rather than reordering as engagement moves.
    let mut per_platform: Vec<(Platform, EngagementTotals)> = Vec::new();
    let mut per_day: Vec<DayBucket> = Vec::new();

    let mut cadence = CadenceSummary {
        dated_posts: 0,
        first_published_at: None,
        last_published_at: None,
        days_covered: 0,
        posts_per_week: 0.0,
        by_day_of_week: [0; 7],
        by_hour: [0; 24],
        busiest_day_of_week: None,
        busiest_hour: None,
        basis: Basis::Insufficient,
    };

    for item in items {
        totals.add(item);

        match per_platform.iter_mut().find(|(p, _)| *p == item.platform) {
            Some((_, t)) => t.add(item),
            None => {
                let mut t = EngagementTotals::default();
                t.add(item);
                per_platform.push((item.platform, t));
            }
        }

        let Some(published_at) = item.published_at else {
            continue;
        };
        let Some(local) = local_parts(published_at, utc_offset_minutes) else {
            continue;
        };

        cadence.dated_posts += 1;
        cadence.first_published_at = Some(
            cadence
                .first_published_at
                .map_or(published_at, |first| first.min(published_at)),
        );
        cadence.last_published_at = Some(
            cadence
                .last_published_at
                .map_or(published_at, |last| last.max(published_at)),
        );
        cadence.by_day_of_week[local.weekday().num_days_from_monday() as usize] += 1;
        cadence.by_hour[local.hour() as usize] += 1;

        let day = local.date().format("%Y-%m-%d").to_string();
        let day_start_ms = local
            .date()
            .and_hms_opt(0, 0, 0)
            .map(|midnight| {
                midnight.and_utc().timestamp_millis()
                    - i64::from(utc_offset_minutes) * 60_000
            })
            .unwrap_or(published_at);
        let bucket = match per_day.iter_mut().find(|b| b.day == day) {
            Some(b) => b,
            None => {
                per_day.push(DayBucket {
                    day,
                    day_start_ms,
                    posts: 0,
                    likes: 0,
                    comments: 0,
                    shares: 0,
                    views: 0,
                    engagement: 0,
                });
                per_day.last_mut().expect("just pushed")
            }
        };
        bucket.posts += 1;
        bucket.likes += item.likes;
        bucket.comments += item.comments;
        bucket.shares += item.shares;
        bucket.views += item.views;
        bucket.engagement += item_engagement(item);
    }

    let totals = totals.finish();

    // Span and cadence.
    if let (Some(first), Some(last)) = (cadence.first_published_at, cadence.last_published_at) {
        let first_day = local_parts(first, utc_offset_minutes).map(|d| d.date());
        let last_day = local_parts(last, utc_offset_minutes).map(|d| d.date());
        if let (Some(a), Some(b)) = (first_day, last_day) {
            // Inclusive: one post spans one day, not zero, so `posts_per_week` for a
            // single post is 7.0 rather than a division by zero.
            cadence.days_covered = ((b - a).num_days().max(0) + 1) as u32;
            cadence.posts_per_week =
                cadence.dated_posts as f64 * 7.0 / f64::from(cadence.days_covered.max(1));
        }
    }
    cadence.busiest_day_of_week = modal_index(&cadence.by_day_of_week).map(|i| i as u8);
    cadence.busiest_hour = modal_index(&cadence.by_hour).map(|i| i as u32);
    cadence.basis = if cadence.dated_posts >= MIN_CADENCE_SAMPLE {
        Basis::Observed
    } else {
        Basis::Insufficient
    };

    // Platforms, best-engaging first; ties broken by declaration order so the list is
    // deterministic when nothing has engagement yet.
    per_platform.sort_by(|(pa, a), (pb, b)| {
        b.engagement
            .cmp(&a.engagement)
            .then_with(|| pa.cmp(pb))
    });
    let by_platform = per_platform
        .into_iter()
        .map(|(platform, t)| {
            let t = t.finish();
            PlatformRollup {
                platform,
                label: platform.label().to_string(),
                share_of_engagement: if totals.engagement == 0 {
                    0.0
                } else {
                    t.engagement as f64 / totals.engagement as f64
                },
                totals: t,
            }
        })
        .collect();

    let mut top_posts: Vec<TopPost> = items
        .iter()
        .map(|item| TopPost {
            engagement: item_engagement(item),
            item: item.clone(),
        })
        .collect();
    // Engagement desc, then newest first, then id — fully deterministic, which
    // matters because this list is rendered next to a "why is this ranked here"
    // question.
    top_posts.sort_by(|a, b| {
        b.engagement
            .cmp(&a.engagement)
            .then_with(|| b.item.published_at.cmp(&a.item.published_at))
            .then_with(|| a.item.id.cmp(&b.item.id))
    });
    top_posts.truncate(TOP_POSTS);

    per_day.sort_by(|a, b| a.day_start_ms.cmp(&b.day_start_ms));

    AnalyticsRollups {
        totals,
        by_platform,
        top_posts,
        published_by_day: per_day,
        cadence,
        utc_offset_minutes,
    }
}

/// Index of the largest value, or `None` when every bucket is zero. Ties go to the
/// lowest index.
fn modal_index(counts: &[u32]) -> Option<usize> {
    let mut best: Option<(usize, u32)> = None;
    for (i, &n) in counts.iter().enumerate() {
        let beats_current = match best {
            None => true,
            Some((_, current)) => n > current,
        };
        if n > 0 && beats_current {
            best = Some((i, n));
        }
    }
    best.map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::DEFAULT_WORKSPACE_ID;

    #[test]
    fn engagement_score_ignores_views_and_treats_unknown_as_zero() {
        let counts = EngagementCounts {
            likes: Some(10),
            comments: Some(5),
            shares: Some(2),
            views: Some(1_000_000),
            fetched_at: 0,
        };
        assert_eq!(engagement_score(&counts), 17);
        // "Not reported" must contribute nothing rather than being invented.
        assert_eq!(
            engagement_score(&EngagementCounts {
                likes: Some(3),
                ..Default::default()
            }),
            3
        );
    }

    /// 2024-01-01T00:00:00Z was a MONDAY — every timestamp below is an offset from it,
    /// so the weekday assertions are checkable by hand.
    const MONDAY_UTC: i64 = 1_704_067_200_000;
    const HOUR: i64 = 3_600 * 1_000;
    const DAY: i64 = 24 * HOUR;

    fn activity(
        id: &str,
        platform: Platform,
        likes: u64,
        comments: u64,
        shares: u64,
        views: u64,
        published_at: Option<i64>,
    ) -> ActivityItem {
        ActivityItem {
            id: id.to_string(),
            workspace_id: DEFAULT_WORKSPACE_ID.to_string(),
            social_account_id: "acc_1".to_string(),
            platform,
            post_remote_id: format!("remote_{id}"),
            permalink: None,
            text: None,
            likes,
            comments,
            shares,
            views,
            engagement_fetched_at: Some(0),
            published_at,
        }
    }

    #[test]
    fn totals_and_platform_shares_add_up() {
        let items = vec![
            activity("a", Platform::X, 10, 5, 2, 1_000, Some(MONDAY_UTC)),
            activity("b", Platform::X, 1, 1, 1, 10, Some(MONDAY_UTC + HOUR)),
            activity("c", Platform::Bluesky, 4, 0, 0, 0, Some(MONDAY_UTC + DAY)),
        ];
        let r = rollups(&items, 0);

        assert_eq!(r.totals.posts, 3);
        assert_eq!(r.totals.likes, 15);
        assert_eq!(r.totals.views, 1_010);
        assert_eq!(r.totals.engagement, 17 + 3 + 4);
        assert!((r.totals.avg_engagement - 8.0).abs() < f64::EPSILON);

        // Best-engaging platform first.
        assert_eq!(r.by_platform.len(), 2);
        assert_eq!(r.by_platform[0].platform, Platform::X);
        assert_eq!(r.by_platform[0].totals.engagement, 20);
        assert_eq!(r.by_platform[1].platform, Platform::Bluesky);
        // Shares sum to 1.
        let share_sum: f64 = r.by_platform.iter().map(|p| p.share_of_engagement).sum();
        assert!((share_sum - 1.0).abs() < 1e-9, "{share_sum}");
    }

    #[test]
    fn an_empty_workspace_produces_zeroes_not_nan_and_claims_nothing() {
        let r = rollups(&[], 0);
        assert_eq!(r.totals.posts, 0);
        assert_eq!(r.totals.avg_engagement, 0.0);
        assert!(r.by_platform.is_empty());
        assert!(r.top_posts.is_empty());
        assert!(r.published_by_day.is_empty());
        assert_eq!(r.cadence.basis, Basis::Insufficient);
        assert_eq!(r.cadence.busiest_hour, None);
        assert_eq!(r.cadence.posts_per_week, 0.0);
        // A share of nothing is 0, not 100%.
        let single = vec![activity("a", Platform::X, 0, 0, 0, 500, Some(MONDAY_UTC))];
        assert_eq!(rollups(&single, 0).by_platform[0].share_of_engagement, 0.0);
    }

    #[test]
    fn top_posts_rank_by_engagement_and_ignore_views() {
        let items = vec![
            // Huge reach, no engagement — must NOT outrank the conversation.
            activity("viral-dud", Platform::X, 0, 0, 0, 9_000_000, Some(MONDAY_UTC)),
            activity("talker", Platform::X, 1, 30, 0, 12, Some(MONDAY_UTC)),
            activity("liked", Platform::Linkedin, 20, 0, 0, 40, Some(MONDAY_UTC)),
        ];
        let r = rollups(&items, 0);
        assert_eq!(r.top_posts[0].item.id, "talker");
        assert_eq!(r.top_posts[0].engagement, 31);
        assert_eq!(r.top_posts[1].item.id, "liked");
        assert_eq!(r.top_posts[2].item.id, "viral-dud");
        assert_eq!(r.top_posts[2].engagement, 0);
    }

    #[test]
    fn top_posts_are_capped_and_deterministic() {
        let items: Vec<ActivityItem> = (0..25)
            .map(|i| {
                activity(
                    &format!("p{i:02}"),
                    Platform::X,
                    // Every post ties on engagement, so only the tiebreak decides.
                    5,
                    0,
                    0,
                    0,
                    Some(MONDAY_UTC),
                )
            })
            .collect();
        let first = rollups(&items, 0);
        let mut shuffled = items.clone();
        shuffled.reverse();
        let second = rollups(&shuffled, 0);
        assert_eq!(first.top_posts.len(), TOP_POSTS);
        let ids_a: Vec<&str> = first.top_posts.iter().map(|t| t.item.id.as_str()).collect();
        let ids_b: Vec<&str> = second.top_posts.iter().map(|t| t.item.id.as_str()).collect();
        assert_eq!(ids_a, ids_b, "input order must not change the ranking");
    }

    #[test]
    fn cadence_buckets_by_local_day_and_hour() {
        // Monday 00:00Z, Monday 09:00Z, Wednesday 09:00Z, Wednesday 23:00Z.
        let items = vec![
            activity("a", Platform::X, 1, 0, 0, 0, Some(MONDAY_UTC)),
            activity("b", Platform::X, 1, 0, 0, 0, Some(MONDAY_UTC + 9 * HOUR)),
            activity("c", Platform::X, 1, 0, 0, 0, Some(MONDAY_UTC + 2 * DAY + 9 * HOUR)),
            activity("d", Platform::X, 1, 0, 0, 0, Some(MONDAY_UTC + 2 * DAY + 23 * HOUR)),
        ];
        let r = rollups(&items, 0);
        assert_eq!(r.cadence.dated_posts, 4);
        assert_eq!(r.cadence.by_day_of_week[0], 2, "Monday");
        assert_eq!(r.cadence.by_day_of_week[2], 2, "Wednesday");
        assert_eq!(r.cadence.by_hour[9], 2);
        assert_eq!(r.cadence.busiest_hour, Some(9));
        // Ties go to the lowest index — deterministic, and documented as arbitrary.
        assert_eq!(r.cadence.busiest_day_of_week, Some(0));
        // Mon..Wed inclusive is 3 days.
        assert_eq!(r.cadence.days_covered, 3);
        assert!((r.cadence.posts_per_week - (4.0 * 7.0 / 3.0)).abs() < 1e-9);
        assert_eq!(r.cadence.basis, Basis::Observed);

        assert_eq!(r.published_by_day.len(), 2);
        assert_eq!(r.published_by_day[0].day, "2024-01-01");
        assert_eq!(r.published_by_day[0].posts, 2);
        assert_eq!(r.published_by_day[1].day, "2024-01-03");
    }

    #[test]
    fn the_utc_offset_moves_a_post_into_the_previous_local_day() {
        // Monday 00:30Z is Sunday 16:30 in UTC-8.
        let items = vec![activity(
            "a",
            Platform::X,
            1,
            0,
            0,
            0,
            Some(MONDAY_UTC + HOUR / 2),
        )];
        let utc = rollups(&items, 0);
        assert_eq!(utc.published_by_day[0].day, "2024-01-01");
        assert_eq!(utc.cadence.busiest_day_of_week, Some(0), "Monday");

        let pacific = rollups(&items, -480);
        assert_eq!(pacific.published_by_day[0].day, "2023-12-31");
        assert_eq!(pacific.cadence.busiest_day_of_week, Some(6), "Sunday");
        assert_eq!(pacific.cadence.busiest_hour, Some(16));
        assert_eq!(pacific.utc_offset_minutes, -480);
    }

    #[test]
    fn undated_posts_count_in_totals_but_never_invent_a_date() {
        let items = vec![
            activity("dated", Platform::X, 2, 0, 0, 0, Some(MONDAY_UTC)),
            activity("undated", Platform::X, 3, 0, 0, 0, None),
        ];
        let r = rollups(&items, 0);
        assert_eq!(r.totals.posts, 2);
        assert_eq!(r.totals.engagement, 5);
        // The undated one is absent from every time-based projection — defaulting it
        // to the epoch would put a post in 1970 and skew the span to 54 years.
        assert_eq!(r.cadence.dated_posts, 1);
        assert_eq!(r.published_by_day.len(), 1);
        assert_eq!(r.published_by_day[0].posts, 1);
        assert_eq!(r.cadence.days_covered, 1);
        // One dated post is still below the sample floor.
        assert_eq!(r.cadence.basis, Basis::Insufficient);
    }

    #[tokio::test]
    async fn refreshing_a_failed_publish_never_calls_a_provider() {
        let store = crate::store::SocialStore::open_in_memory().unwrap();
        let account = store
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = store
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[crate::store::NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        let entry = store
            .insert_history(
                &post.targets[0].id,
                HistoryStatus::Failed,
                None,
                None,
                Some("rate limited"),
            )
            .await
            .unwrap();
        let state = AppState::new(store, crate::state::Config::from_env(0));

        let err = refresh_engagement(&state, &entry.id).await.unwrap_err();
        assert!(matches!(err, ApiError::Conflict(_)), "{err}");
    }
}
