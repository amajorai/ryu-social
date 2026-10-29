//! The scheduler tick: the background task that notices a post's time has come,
//! hands it to the publish runner, recovers work a dead process abandoned, and
//! answers "when should I post next?".
//!
//! Three things live here, and they are together because they share one clock and
//! one settings read:
//!
//! 1. **The tick loop** — claim, drain, run, bounded across posts.
//! 2. **Crash recovery** — a boot pass plus a per-tick lease reaper.
//! 3. **The timing recommender** — a pure projection over engagement history that
//!    answers "best time to post", with a documented default table when there is not
//!    enough history to say anything honest.
//!
//! ## Why every guard is in SQL and none of them is a flag
//!
//! The design this is ported from ran in one webview and guarded re-entrancy with a
//! module-level `isSweeping` boolean and an in-memory `inFlight` set. Neither
//! survives the move: this sidecar has concurrent request handlers plus this tick
//! task against one database, so an in-process flag guards only the actor holding
//! it. Every guard here is therefore a compare-and-swap in SQL — the `scheduled →
//! due` flip IS the claim, the `due → publishing` CAS is the runner's claim, and a
//! concurrent sweep simply gets an empty batch instead of a duplicate one.
//!
//! ## Time
//!
//! Every stored instant is **UTC epoch millis**, including `scheduled_for`. The
//! workspace's `settings.timezone` is an IANA zone used for exactly two things:
//! rendering, and deciding which *local* hour a past post went out in for the
//! recommender below. It never participates in deciding whether a post is due —
//! that comparison is `scheduled_for <= now`, in UTC, with no zone anywhere near it.
//! Getting this backwards is the classic scheduler bug: a DST shift silently moves
//! every queued post by an hour.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde::Serialize;

use crate::error::ApiResult;
use crate::models::{
    now_ms, ActivityItem, EngagementCounts, Platform, PostStatus, ScheduledPost, SocialSettings,
    TargetStatus, DEFAULT_WORKSPACE_ID,
};
use crate::state::AppState;

// ── Tick policy ────────────────────────────────────────────────────────────────

/// Sweep cadence when no workspace says otherwise.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Clamps on the user-settable cadence. The floor keeps a mistyped `1` from turning
/// the tick into a busy loop against one mutex-guarded connection; the ceiling keeps
/// a mistyped `86400` from making the app look broken for a day.
const MIN_POLL_INTERVAL: Duration = Duration::from_secs(5);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// Floor on the publish lease, used when a workspace has never written settings.
const DEFAULT_CLAIM_LEASE: Duration = Duration::from_secs(300);

/// How long ONE provider call may take, worst case, when sizing the lease.
///
/// Not an independent guess: it IS the deadline `publish::publish_once` enforces on
/// every provider call. It used to be a bare `30_000` that nothing enforced, which
/// made the entire lease calculation below rest on an assumption a single hung
/// socket could violate without bound. Defining it from the enforced constant means
/// the two can never drift apart.
const PER_ATTEMPT_ALLOWANCE_MS: u64 = crate::state::PROVIDER_CALL_TIMEOUT_MS;

/// Slack added on top of the computed worst-case publish before the reaper is
/// allowed to consider a lease dead.
const LEASE_MARGIN_MS: u64 = 60_000;

/// How many posts may be in flight at once. Targets *within* a post stay strictly
/// sequential (see [`crate::publish`] — stacked backoff sleeps against one platform
/// are how an API key gets rate-limited); this only overlaps independent posts.
///
/// Small on purpose: the store is one `Arc<Mutex<Connection>>`, so the database work
/// serializes regardless and the only thing this actually parallelizes is the
/// network wait. Four is enough to hide latency without opening a fan of concurrent
/// writes to the same platform.
const MAX_CONCURRENT_POSTS: usize = 4;

/// How many `publishing` rows the boot recovery pass will examine. A bound rather
/// than "all", because this runs before the listener is accepting and a pathological
/// database should not delay the port bind indefinitely.
const RECOVERY_SCAN_LIMIT: usize = 500;

/// The merged, process-wide tick policy derived from every workspace's settings.
///
/// Per-workspace settings meeting a per-process loop needs a merge rule, and the two
/// fields pull in opposite directions, so they merge in opposite directions:
///
/// - **`poll_interval` is the MINIMUM** across enabled workspaces. A workspace that
///   asked for a 5-second cadence gets it; the others just sweep more often than
///   they asked, which is harmless.
/// - **`lease` is the MAXIMUM** across ALL workspaces. Under-leasing is the
///   dangerous direction: the reaper would recycle a healthy in-flight publish and
///   (on any provider that ignores the idempotency key) double-post. Over-leasing
///   only delays crash recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickPolicy {
    pub poll_interval: Duration,
    pub lease: Duration,
    /// Workspaces whose `scheduler_enabled` is on. Posts belonging to any other
    /// workspace are drained but not run — see [`tick`].
    pub enabled_workspaces: HashSet<String>,
}

impl Default for TickPolicy {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            lease: DEFAULT_CLAIM_LEASE,
            enabled_workspaces: HashSet::from([DEFAULT_WORKSPACE_ID.to_string()]),
        }
    }
}

impl TickPolicy {
    pub fn is_enabled(&self, workspace_id: &str) -> bool {
        self.enabled_workspaces.contains(workspace_id)
    }

    pub fn any_enabled(&self) -> bool {
        !self.enabled_workspaces.is_empty()
    }
}

/// The longest a single target's publish run can legitimately take: every attempt's
/// provider call plus every backoff sleep between them.
///
/// `max_attempts` calls, but only `max_attempts - 1` sleeps — there is no backoff
/// after the final attempt, and counting one would inflate the lease for free.
pub fn worst_case_publish_ms(max_attempts: u32, base_backoff_ms: u64) -> u64 {
    let sleeps = (1..max_attempts)
        .map(|attempt| crate::publish::backoff_delay_ms(attempt, base_backoff_ms))
        .fold(0u64, u64::saturating_add);
    let calls = u64::from(max_attempts).saturating_mul(PER_ATTEMPT_ALLOWANCE_MS);
    sleeps.saturating_add(calls)
}

/// The lease this workspace's retry policy actually requires.
///
/// **The coupling that makes this a function rather than a settings read:**
/// `claim_lease_secs` and `max_attempts`/`base_backoff_ms` are three independent
/// knobs on one settings form, and nothing stops a user setting
/// `max_attempts: 10, base_backoff_ms: 10_000` (worst case ≈ 85 minutes) while
/// leaving the lease at its 5-minute default. The reaper would then reclaim a
/// perfectly healthy run at minute five, a second runner would claim it, and both
/// would publish. So the configured lease is a FLOOR, not the answer.
pub fn effective_lease_ms(settings: &SocialSettings) -> u64 {
    let configured = settings.claim_lease_secs.saturating_mul(1_000);
    let required =
        worst_case_publish_ms(settings.max_attempts, settings.base_backoff_ms) + LEASE_MARGIN_MS;
    configured.max(required)
}

/// Merge every workspace's settings into the one policy the loop runs on.
///
/// Pure, so the merge rule is testable without a database.
pub fn merge_policy(per_workspace: &[(String, SocialSettings)]) -> TickPolicy {
    if per_workspace.is_empty() {
        return TickPolicy {
            enabled_workspaces: HashSet::new(),
            ..TickPolicy::default()
        };
    }

    let enabled_workspaces: HashSet<String> = per_workspace
        .iter()
        .filter(|(_, s)| s.scheduler_enabled)
        .map(|(id, _)| id.clone())
        .collect();

    // Minimum across ENABLED workspaces only: a disabled workspace's cadence is not
    // a request for anything, and letting it drag the whole process to a 5-second
    // tick would be a knob doing the opposite of what it says.
    let poll_secs = per_workspace
        .iter()
        .filter(|(id, _)| enabled_workspaces.contains(id))
        .map(|(_, s)| s.poll_interval_secs)
        .min();
    let poll_interval = poll_secs.map_or(DEFAULT_POLL_INTERVAL, |secs| {
        Duration::from_secs(secs).clamp(MIN_POLL_INTERVAL, MAX_POLL_INTERVAL)
    });

    // Maximum across ALL workspaces, disabled included: disabling the scheduler stops
    // NEW claims, it does not abort a publish already in flight, and that in-flight
    // run still needs its lease honoured.
    let lease_ms = per_workspace
        .iter()
        .map(|(_, s)| effective_lease_ms(s))
        .max()
        .unwrap_or_else(|| DEFAULT_CLAIM_LEASE.as_millis() as u64);

    TickPolicy {
        poll_interval,
        lease: Duration::from_millis(lease_ms),
        enabled_workspaces,
    }
}

/// Read every workspace's settings and merge them. Falls back to the default policy
/// (rather than stalling the loop) when the read fails — a settings query that errors
/// is not a reason to stop publishing.
pub async fn read_policy(state: &AppState) -> TickPolicy {
    let workspaces = match state.store.list_workspaces().await {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "ryu-social: could not read workspaces; using the default tick policy");
            return TickPolicy::default();
        }
    };
    let mut per_workspace = Vec::with_capacity(workspaces.len());
    for ws in workspaces {
        match state.store.get_settings(&ws.id).await {
            Ok(settings) => per_workspace.push((ws.id, settings)),
            // `get_settings` already degrades an unparseable blob to the default, so
            // reaching here means the query itself failed. Assume defaults for that
            // one workspace rather than dropping it from the policy entirely, which
            // would silently stop publishing for it.
            Err(e) => {
                tracing::warn!(workspace = %ws.id, error = %e, "ryu-social: settings read failed; assuming defaults");
                per_workspace.push((ws.id, SocialSettings::default()));
            }
        }
    }
    merge_policy(&per_workspace)
}

// ── The loop ───────────────────────────────────────────────────────────────────

/// Start the tick loop. Returns the handle so the caller can abort it on shutdown.
///
/// Spawned rather than awaited: `main` must keep serving HTTP.
pub fn spawn(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !state.config.scheduler_enabled {
            tracing::info!(
                "ryu-social: scheduler disabled (RYU_SOCIAL_SCHEDULER); posts will not publish automatically"
            );
            return;
        }

        // Boot recovery FIRST, before any claim: a post this process is about to
        // find in `due` may have an orphaned sibling target from the previous run,
        // and recovering after claiming would race our own runner.
        match recover_orphaned_work(&state).await {
            Ok(report) if report.is_empty() => {}
            Ok(report) => tracing::warn!(
                reaped_targets = report.reaped_targets,
                requeued_posts = report.requeued_posts,
                settled_posts = report.settled_posts,
                "ryu-social: recovered work orphaned by a previous process"
            ),
            Err(e) => tracing::error!(error = %e, "ryu-social: boot recovery failed"),
        }

        loop {
            // Re-read every iteration rather than once at spawn: the cadence and the
            // lease are user-editable at runtime, and a settings change that only
            // takes effect on restart is a settings change that looks broken.
            let policy = read_policy(&state).await;
            tick(&state, &policy).await;
            // A fixed DELAY between sweeps, not a fixed rate. `tokio::time::interval`
            // would need rebuilding whenever the cadence changed, and its catch-up
            // behaviour after a slow tick is the wrong shape here: a sweep that
            // overran because it was publishing should not be immediately followed by
            // the sweeps it "missed".
            tokio::time::sleep(policy.poll_interval).await;
        }
    })
}

/// One sweep. Never panics and never propagates: a failing tick must not kill the
/// loop, because the next one may well succeed and a dead scheduler is silent.
pub async fn tick(state: &AppState, policy: &TickPolicy) {
    // ONE clock read for the whole tick. Two reads would let the reaper cutoff and
    // the due predicate disagree by however long the reap took, which is exactly the
    // kind of drift that makes an intermittent double-claim.
    let now = now_ms();

    // 1. Recover leases that expired BEFORE claiming anything new, so a reaped target
    //    is eligible in this same pass rather than waiting another interval.
    let cutoff = now - policy.lease.as_millis() as i64;
    match state.store.reap_expired_claims(cutoff, now).await {
        // WARN, not debug: a reap always means a previous run died mid-publish.
        Ok(ids) if !ids.is_empty() => {
            tracing::warn!(count = ids.len(), targets = ?ids, "ryu-social: reclaimed publish targets whose lease expired");
        }
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "ryu-social: reaper pass failed"),
    }

    // 2. Claim everything that has come due.
    //
    //    THE CLAIM QUERY, explained — it is `SocialStore::claim_due_posts`:
    //
    //      UPDATE scheduled_posts SET status = 'due'
    //       WHERE id IN (SELECT id FROM scheduled_posts
    //                     WHERE status = 'scheduled' AND scheduled_for <= ?now
    //                     ORDER BY scheduled_for ASC LIMIT ?batch)
    //       RETURNING <post columns>;
    //
    //    One statement, not a SELECT followed by an UPDATE. That is the whole point:
    //    the `status = 'scheduled'` predicate and the write to `'due'` are evaluated
    //    under one implicit transaction, so two ticks racing each other cannot both
    //    match the same row — whichever commits first removes it from the other's
    //    predicate, and the loser gets an empty batch rather than a duplicate one.
    //    The `RETURNING` clause is what makes the flip a *claim* rather than a
    //    fire-and-forget update: the rows handed back ARE the work this caller now
    //    exclusively owns. The `id IN (SELECT … LIMIT ?)` wrapper bounds one sweep's
    //    batch (SQLite does not accept LIMIT directly on UPDATE), which caps the blast
    //    radius of a node that was offline for a week.
    //
    //    Note what is NOT in the predicate: `post_targets.next_attempt_at`. Backoff is
    //    per-TARGET state and the post-level claim is deliberately coarser — a post
    //    with one backing-off target still has other targets that may be ready. The
    //    per-attempt gate lives in the runner, where the target is in hand.
    if policy.any_enabled() {
        match state
            .store
            .claim_due_posts(now, state.config.sweep_batch_size)
            .await
        {
            Ok(posts) if !posts.is_empty() => {
                tracing::info!(count = posts.len(), "ryu-social: posts became due")
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "ryu-social: sweep failed"),
        }
    }

    // 3. Drain. `list_due_posts` is a SUPERSET of what step 2 just claimed: it also
    //    picks up rows an EARLIER process (or an earlier tick, or `POST
    //    /posts/:id/publish-now`, or `POST /posts/:id/retry`) flipped to `due` and
    //    never ran. Draining the single `due` set instead of also carrying step 2's
    //    return value is what keeps a post from being handed to the runner twice in
    //    one tick — the runner's own CAS would reject the duplicate, but only after
    //    paying for it.
    let due = match state
        .store
        .list_due_posts(state.config.sweep_batch_size)
        .await
    {
        Ok(posts) => posts,
        Err(e) => {
            tracing::error!(error = %e, "ryu-social: due drain failed");
            return;
        }
    };
    if due.is_empty() {
        return;
    }

    // A post whose workspace has the scheduler switched off is left where it is. It
    // is HELD, not lost: re-enabling the workspace makes the very next drain pick it
    // up with no repair step.
    //
    // Be precise about WHERE it is held, because it differs by case and the
    // difference is visible to the user:
    //
    // - **Every workspace disabled** — step 2 is skipped entirely, so its posts never
    //   leave `scheduled`. This is what `SocialSettings::scheduler_enabled`'s doc
    //   describes.
    // - **Some enabled, some not** — `claim_due_posts` is a single global statement
    //   with no workspace predicate, so a disabled workspace's due posts DO flip to
    //   `due` along with everyone else's. They are then filtered out here and never
    //   run. Making the claim genuinely workspace-scoped would need the store to
    //   learn the set, which is not worth a schema-adjacent change for a switch that
    //   is rarely off: `due` still honestly means "its time came", and the safety
    //   property — a held post is never PUBLISHED — holds either way.
    //
    // The cost of holding in `due` is that held posts occupy the head of a
    // limit-bounded drain, so a workspace that stays disabled with a large backlog
    // can crowd out an enabled one. Acceptable, and visible because of the log below.
    let (runnable, held): (Vec<_>, Vec<_>) = due
        .into_iter()
        .partition(|post| policy.is_enabled(&post.workspace_id));
    if !held.is_empty() {
        tracing::debug!(
            count = held.len(),
            "ryu-social: due posts held — their workspace has the scheduler switched off"
        );
    }
    if runnable.is_empty() {
        return;
    }

    run_batch(state, runnable, MAX_CONCURRENT_POSTS).await;
}

/// Hand a batch of due posts to the runner with bounded concurrency.
///
/// Bounded by refilling from an iterator as each task completes, rather than
/// spawning everything and letting a semaphore sort it out: this way the batch's
/// memory is one post per in-flight task, and an abort of the tick drops the rest of
/// the queue without having already committed it to tasks.
async fn run_batch(state: &AppState, posts: Vec<ScheduledPost>, max_concurrent: usize) {
    let mut pending = posts.into_iter();
    let mut set: tokio::task::JoinSet<(String, Result<PostStatus, String>)> =
        tokio::task::JoinSet::new();
    let cap = max_concurrent.max(1);

    loop {
        while set.len() < cap {
            let Some(post) = pending.next() else { break };
            let state = state.clone();
            set.spawn(async move {
                let id = post.id.clone();
                let result = crate::publish::run_post(&state, &post)
                    .await
                    .map(|outcome| outcome.status)
                    .map_err(|e| e.to_string());
                (id, result)
            });
        }
        let Some(joined) = set.join_next().await else {
            break;
        };
        match joined {
            Ok((id, Ok(status))) => {
                tracing::info!(post = %id, status = status.as_str(), "ryu-social: post run settled")
            }
            // An error here is the runner declining, not a publish failing — a post
            // that fails to publish is a SETTLED post with a terminal status, not an
            // `Err`. So this is either the not-yet-implemented runner or a genuine
            // internal fault, and in both cases the row stays `due` for the next tick.
            Ok((id, Err(e))) => {
                tracing::warn!(post = %id, error = %e, "ryu-social: post is due but not published")
            }
            // A panicking runner must not take the tick down with it.
            Err(e) => tracing::error!(error = %e, "ryu-social: publish task did not complete"),
        }
    }
}

// ── Crash recovery ─────────────────────────────────────────────────────────────

/// What one boot recovery pass repaired.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RecoveryReport {
    pub reaped_targets: usize,
    pub requeued_posts: usize,
    pub settled_posts: usize,
}

impl RecoveryReport {
    pub fn is_empty(&self) -> bool {
        self.reaped_targets == 0 && self.requeued_posts == 0 && self.settled_posts == 0
    }
}

/// The boot pass: return everything a dead process abandoned to a state the runner
/// can act on.
///
/// ## Why this is boot-ONLY, and why that is the safety property
///
/// The per-tick reaper is conservative by necessity — it can only touch leases older
/// than the lease TTL, because a live runner in this same process may hold a fresh
/// one. At boot that ambiguity does not exist: this process has just started, so it
/// owns no leases, and this sidecar is the sole writer of its `social.db` (Core
/// spawns exactly one, and a dev profile gets a different `RYU_DIR` and therefore a
/// different file). Every claim found here is therefore dead by construction, which
/// is what makes the aggressive `cutoff = now` sweep below correct rather than a new
/// double-publish race.
///
/// Running the same logic every tick would NOT be correct, and that is the trap this
/// separation exists to avoid.
///
/// ## The idempotency coupling, and where it is satisfied
///
/// [`crate::store`]'s own docs are emphatic that a reaper without a durable record of
/// what already reached the platform does not fix double-publishing, it CAUSES it —
/// and this pass is the most aggressive reaper in the app, reclaiming every stamped
/// lease rather than only expired ones. The other half of that pair lives in
/// [`crate::publish`]: `run_target` opens by reading `post_history` for the target and,
/// on finding a `published` row that carries a `remote_id`, settles the target as
/// published WITHOUT contacting the provider. So a target this pass returns to
/// `pending` after its publish had already landed remotely is settled from that
/// record, not re-sent. **If that check is ever removed, this pass becomes a
/// double-post generator** — they are one mechanism in two files.
///
/// ## The three orphan shapes
///
/// 1. **Target stuck in `publishing`** — a lease was stamped and the process died.
///    Handled by [`crate::store::SocialStore::reap_expired_claims`] with a `now`
///    cutoff; its own follow-up returns the parent post to `due`.
/// 2. **Post `publishing`, every target still `pending`** — died in the window
///    between claiming the post and claiming its first target. No `claimed_at` was
///    ever written, so shape 1 cannot see it. Requeued to `due`.
/// 3. **Post `publishing`, every target terminal** — died after settling the last
///    target but before settling the post. There is no work left; requeueing it would
///    hand the runner a post with nothing to claim and (with zero outcomes) settle a
///    fully published post as `failed`. So this one is settled directly from what its
///    targets already say.
pub async fn recover_orphaned_work(state: &AppState) -> anyhow::Result<RecoveryReport> {
    let now = now_ms();
    let mut report = RecoveryReport::default();

    // Shape 1. `claimed_at < ?cutoff`, so the cutoff must be strictly greater than
    // any stamp written up to this instant.
    let reaped = state
        .store
        .reap_expired_claims(now.saturating_add(1), now)
        .await?;
    report.reaped_targets = reaped.len();

    // Shapes 2 and 3. Read AFTER the reap, so anything shape 1 already repaired is
    // no longer in `publishing` and is not reconsidered here.
    let stranded = state
        .store
        .list_posts_with_status(PostStatus::Publishing, RECOVERY_SCAN_LIMIT)
        .await?;
    for post in stranded {
        let has_pending = post
            .targets
            .iter()
            .any(|t| t.status == TargetStatus::Pending);
        if has_pending {
            if state.store.requeue_publishing_post(&post.id, now).await? {
                report.requeued_posts += 1;
            }
            continue;
        }
        // No pending work left. Derive the verdict its own targets already imply,
        // using the same rule the runner would have: all published ⇒ published, none
        // ⇒ failed, mixed ⇒ partial. A post with no targets at all is `failed`, which
        // matches "nothing was attempted must never read as success".
        let settled = settle_status_from_targets(&post);
        if state.store.settle_post(&post.id, settled).await? {
            report.settled_posts += 1;
        }
    }

    Ok(report)
}

/// The terminal status a post's already-settled targets imply.
///
/// Delegates to [`crate::publish::aggregate_target_statuses`] rather than restating
/// the rule. This function used to carry its own copy of the four-case match, on the
/// grounds that it "mirrors `publish::aggregate_status`" — but `aggregate_status` is
/// the *outcome*-shaped wrapper, and the persisted-row form it wraps,
/// `aggregate_target_statuses(&[TargetStatus])`, is exactly this call's input. So the
/// copy was not a deliberate mirror of a differently-shaped function, it was the same
/// function written twice. Two copies of "when is a post Partial" is precisely the
/// kind of drift that ends with the queue and the history view disagreeing about
/// whether a post succeeded.
fn settle_status_from_targets(post: &ScheduledPost) -> PostStatus {
    crate::publish::aggregate_target_statuses(
        &post.targets.iter().map(|t| t.status).collect::<Vec<_>>(),
    )
}

// ── The timing recommender ─────────────────────────────────────────────────────

/// Minimum posts on a platform before anything but the default table is claimed.
pub const MIN_PLATFORM_SAMPLE: usize = 4;
/// Minimum posts in one `(weekday, hour)` bucket before that bucket is trusted.
pub const MIN_FINE_BUCKET_SAMPLE: usize = 2;
/// How many slots a recommendation returns.
pub const MAX_SLOTS: usize = 3;

/// How many activity rows the store-backed adapter samples. Bounded because the
/// recommender is a nicety and must never become the app's most expensive query.
const TIMING_SAMPLE_LIMIT: usize = 500;

/// The built-in slots, used whenever there is not enough history to say anything
/// honest. Tuesday 11:00, Wednesday 18:00, Thursday 13:00 — published general-advice
/// figures, and labelled as defaults on the wire so no surface can present them as a
/// learned result.
const DEFAULT_SLOTS: [(u8, u8); MAX_SLOTS] = [(2, 11), (3, 18), (4, 13)];

/// One past post, reduced to the only two things the recommender uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimingSample {
    /// UTC epoch millis.
    pub published_at: i64,
    /// [`crate::analytics::engagement_score`] — likes + comments + shares.
    pub engagement: u64,
}

/// What a recommendation is actually derived from. Serialized so the UI can label it
/// rather than implying a model that does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingBasis {
    /// Not enough history; these are the built-in slots.
    Defaults,
    /// Ranked `(weekday, hour)` buckets, each with at least
    /// [`MIN_FINE_BUCKET_SAMPLE`] posts behind it.
    WeekdayHour,
    /// Not enough posts in any single weekday bucket, so hours were pooled across
    /// days. Weaker: it can say "evenings", not "Tuesday evenings".
    HourOfDay,
}

/// One recommended posting slot, already projected to a concrete future instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TimeSlot {
    /// ISO weekday, 1 = Monday … 7 = Sunday. `None` on an [`TimingBasis::HourOfDay`]
    /// slot, which genuinely has no day behind it.
    pub day_of_week: Option<u8>,
    /// Local hour in the workspace's zone, 0..=23.
    pub hour: u8,
    /// How much better this slot did than the workspace's own average, as a percent.
    /// `None` when it cannot be computed honestly: a zero baseline, or a bucket with
    /// a single post behind it (one post's "lift" is noise, and the badge should
    /// simply not render).
    pub lift_pct: Option<i64>,
    /// Posts behind this slot.
    pub sample_size: usize,
    /// The next time this slot comes around, as UTC epoch millis. DST-correct — see
    /// [`next_occurrence`].
    pub next_occurrence: i64,
}

/// A whole recommendation, with its basis attached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TimingRecommendation {
    /// The IANA zone the hours are expressed in.
    pub timezone: String,
    pub basis: TimingBasis,
    /// Posts considered. Zero on the defaults path.
    pub sample_size: usize,
    pub slots: Vec<TimeSlot>,
}

/// Resolve an IANA zone name, falling back to UTC.
///
/// Never fails: the zone is a display preference typed into a settings box, and a
/// typo must degrade to UTC rather than 500 the calendar.
pub fn zone_for(name: &str) -> Tz {
    name.trim().parse::<Tz>().unwrap_or(chrono_tz::UTC)
}

/// **The recommender. Pure**: same samples, same zone, same `now` ⇒ same answer, with
/// no clock read and no I/O.
///
/// ## What this can and cannot honestly claim
///
/// `activity_items` is a latest-snapshot table — one row per remote post, counts
/// overwritten on each refresh. There is no time series of how a post's engagement
/// grew and no follower count anywhere in the schema. So a "best slot" here means
/// *"when the content that performed was published"*. It cannot separate a good post
/// from a well-timed one, and it is not a learned engagement curve. That is why
/// [`TimingBasis`] is on the wire: every surface must be able to say which of the
/// three things it is showing.
///
/// ## The three-tier degrade
///
/// 1. Fewer than [`MIN_PLATFORM_SAMPLE`] dated samples ⇒ [`DEFAULT_SLOTS`], basis
///    `defaults`.
/// 2. `(weekday, hour)` buckets with at least [`MIN_FINE_BUCKET_SAMPLE`] posts,
///    ranked by mean engagement.
/// 3. If no fine bucket qualifies, hours pooled across weekdays.
pub fn recommend_slots(samples: &[TimingSample], tz: Tz, now: i64) -> TimingRecommendation {
    let dated: Vec<&TimingSample> = samples.iter().filter(|s| s.published_at > 0).collect();

    if dated.len() < MIN_PLATFORM_SAMPLE {
        return TimingRecommendation {
            timezone: tz.name().to_string(),
            basis: TimingBasis::Defaults,
            sample_size: dated.len(),
            slots: DEFAULT_SLOTS
                .iter()
                .map(|&(day, hour)| TimeSlot {
                    day_of_week: Some(day),
                    hour,
                    lift_pct: None,
                    sample_size: 0,
                    next_occurrence: next_occurrence(tz, now, Some(day), hour),
                })
                .collect(),
        };
    }

    // The baseline every lift is measured against: the mean engagement of everything
    // considered. Integer division is fine — lift is reported to the percent.
    let total: u64 = dated
        .iter()
        .map(|s| s.engagement)
        .fold(0, u64::saturating_add);
    let baseline = total as f64 / dated.len() as f64;

    let mut fine: HashMap<(u8, u8), Vec<u64>> = HashMap::new();
    let mut coarse: HashMap<u8, Vec<u64>> = HashMap::new();
    for sample in &dated {
        let Some((day, hour)) = local_weekday_hour(tz, sample.published_at) else {
            continue;
        };
        fine.entry((day, hour)).or_default().push(sample.engagement);
        coarse.entry(hour).or_default().push(sample.engagement);
    }

    let fine_slots: Vec<((u8, u8), Vec<u64>)> = fine
        .into_iter()
        .filter(|(_, values)| values.len() >= MIN_FINE_BUCKET_SAMPLE)
        .collect();

    if !fine_slots.is_empty() {
        let mut ranked: Vec<(u8, u8, f64, usize)> = fine_slots
            .into_iter()
            .map(|((day, hour), values)| {
                let mean = mean_of(&values);
                (day, hour, mean, values.len())
            })
            .collect();
        // Descending by mean, then ascending by (day, hour) so ties are STABLE across
        // runs — a recommendation that reshuffles on every refresh reads as broken.
        ranked.sort_by(|a, b| {
            b.2.partial_cmp(&a.2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
                .then(a.1.cmp(&b.1))
        });
        return TimingRecommendation {
            timezone: tz.name().to_string(),
            basis: TimingBasis::WeekdayHour,
            sample_size: dated.len(),
            slots: ranked
                .into_iter()
                .take(MAX_SLOTS)
                .map(|(day, hour, mean, count)| TimeSlot {
                    day_of_week: Some(day),
                    hour,
                    lift_pct: lift_pct(mean, baseline, count),
                    sample_size: count,
                    next_occurrence: next_occurrence(tz, now, Some(day), hour),
                })
                .collect(),
        };
    }

    // Tier 3: nothing repeated on the same weekday. Pool by hour.
    let mut ranked: Vec<(u8, f64, usize)> = coarse
        .into_iter()
        .map(|(hour, values)| {
            let mean = mean_of(&values);
            (hour, mean, values.len())
        })
        .collect();
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });

    TimingRecommendation {
        timezone: tz.name().to_string(),
        basis: TimingBasis::HourOfDay,
        sample_size: dated.len(),
        slots: ranked
            .into_iter()
            .take(MAX_SLOTS)
            .map(|(hour, mean, count)| TimeSlot {
                day_of_week: None,
                hour,
                lift_pct: lift_pct(mean, baseline, count),
                sample_size: count,
                next_occurrence: next_occurrence(tz, now, None, hour),
            })
            .collect(),
    }
}

fn mean_of(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().copied().fold(0u64, u64::saturating_add) as f64 / values.len() as f64
}

/// `(mean - baseline) / baseline`, as a rounded percent.
///
/// `None` in the two cases where the number would be a lie: a zero baseline (nothing
/// in this workspace has any engagement, so "300% better" is 3 likes vs 1) and a
/// single-post bucket (one post's deviation from the mean is noise, not lift).
fn lift_pct(mean: f64, baseline: f64, sample_size: usize) -> Option<i64> {
    if baseline <= 0.0 || sample_size < MIN_FINE_BUCKET_SAMPLE {
        return None;
    }
    Some((((mean - baseline) / baseline) * 100.0).round() as i64)
}

/// The local ISO weekday (1 = Monday) and hour a UTC instant falls on in `tz`.
fn local_weekday_hour(tz: Tz, at: i64) -> Option<(u8, u8)> {
    let utc = Utc.timestamp_millis_opt(at).single()?;
    let local = utc.with_timezone(&tz);
    Some((
        local.weekday().number_from_monday() as u8,
        local.hour() as u8,
    ))
}

/// Project a slot forward to the next UTC instant at which it occurs.
///
/// ## The DST handling, which is the entire reason this is not arithmetic
///
/// Adding `7 * 24 * 3600 * 1000` to a timestamp is wrong across a DST boundary: the
/// wall-clock hour drifts by one. So this walks forward in LOCAL CALENDAR DAYS and
/// re-applies the wall-clock hour to each candidate date, which is the only way the
/// answer stays "18:00 local" rather than "18:00 local, until March".
///
/// Two boundary cases the walk has to survive, both of which `from_local_datetime`
/// reports rather than guessing at:
///
/// - **Spring forward** — the requested hour does not exist on that date (02:00 in a
///   zone that jumps 02:00 → 03:00). The slot is nudged to the next hour, which is
///   the first moment that wall clock actually reaches.
/// - **Fall back** — the hour occurs twice. The EARLIER instant is taken, so the slot
///   fires at the first opportunity rather than an hour late.
///
/// A 15-day walk covers any weekday plus a full week of margin. If it somehow finds
/// nothing (a zone with pathological rules), it returns `now` rather than a sentinel:
/// a slot the UI renders as "now" is visibly wrong, whereas a `0` or an `i64::MAX`
/// renders as 1970 or a crash.
pub fn next_occurrence(tz: Tz, now: i64, day_of_week: Option<u8>, hour: u8) -> i64 {
    let Some(local_now) = Utc
        .timestamp_millis_opt(now)
        .single()
        .map(|utc| utc.with_timezone(&tz))
    else {
        return now;
    };
    let mut date = local_now.date_naive();
    for _ in 0..15 {
        let matches_day =
            day_of_week.is_none_or(|d| date.weekday().number_from_monday() as u8 == d);
        if matches_day {
            if let Some(at) = local_instant(tz, date, hour) {
                if at > now {
                    return at;
                }
            }
        }
        let Some(next) = date.succ_opt() else { break };
        date = next;
    }
    now
}

/// One local wall-clock instant as UTC millis, or `None` when that wall clock does
/// not exist on that date even after the spring-forward nudge.
fn local_instant(tz: Tz, date: NaiveDate, hour: u8) -> Option<i64> {
    let naive = date.and_hms_opt(u32::from(hour), 0, 0)?;
    // `.earliest()` collapses both mapped cases at once: a normal hour yields the one
    // instant, an ambiguous (fall-back) hour yields the earlier of the two.
    if let Some(dt) = tz.from_local_datetime(&naive).earliest() {
        return Some(dt.timestamp_millis());
    }
    // Spring-forward gap: nudge to the next wall-clock hour.
    let nudged = date.and_hms_opt(u32::from(hour) + 1, 0, 0)?;
    tz.from_local_datetime(&nudged)
        .earliest()
        .map(|dt| dt.timestamp_millis())
}

/// Reduce an activity row to a [`TimingSample`].
///
/// Routes through [`crate::analytics::engagement_score`] rather than adding the three
/// counts here, so the ranking scalar has exactly one definition — including its
/// deliberate exclusion of `views`, which is an impression count and would let a post
/// nobody interacted with outrank one that started a conversation.
pub fn sample_from_activity(item: &ActivityItem) -> Option<TimingSample> {
    let published_at = item.published_at?;
    Some(TimingSample {
        published_at,
        engagement: crate::analytics::engagement_score(&EngagementCounts {
            likes: Some(item.likes),
            comments: Some(item.comments),
            shares: Some(item.shares),
            views: Some(item.views),
            fetched_at: item.engagement_fetched_at.unwrap_or(0),
        }),
    })
}

/// The store-backed adapter: read a workspace's engagement history and recommend.
///
/// ## Why the samples come from `activity_items` and not `post_history`
///
/// `post_history` records WHEN a publish happened and whether it succeeded; it holds
/// no engagement at all. Ranking slots off it would rank by how often you posted,
/// which recommends your current habit back to you. `activity_items` is the only
/// table with both a publish time and a performance number, so it is the input — and
/// when it is too thin, the honest answer is the default table, which is exactly what
/// [`recommend_slots`] returns.
pub async fn best_times(
    state: &AppState,
    workspace_id: &str,
    platform: Option<Platform>,
) -> ApiResult<TimingRecommendation> {
    let settings = state.store.get_settings(workspace_id).await?;
    let activity = state
        .store
        .list_activity(workspace_id, TIMING_SAMPLE_LIMIT)
        .await?;
    let samples: Vec<TimingSample> = activity
        .iter()
        .filter(|item| platform.is_none_or(|p| item.platform == p))
        .filter_map(sample_from_activity)
        .collect();
    Ok(recommend_slots(
        &samples,
        zone_for(&settings.timezone),
        now_ms(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Platform, DEFAULT_WORKSPACE_ID};
    use crate::store::{NewTarget, SocialStore};

    async fn store() -> SocialStore {
        SocialStore::open_in_memory().expect("in-memory store")
    }

    /// Schedule a post with one X target at `at`, returning `(post_id, target_id)`.
    async fn scheduled_post(s: &SocialStore, at: i64) -> (String, String) {
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                at,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        let target = post.targets[0].id.clone();
        (post.id, target)
    }

    // ── The claim ──────────────────────────────────────────────────────────────

    /// The property the whole scheduler rests on: the `scheduled → due` flip IS the
    /// claim, so two sweeps racing the same row cannot both get it.
    ///
    /// Scope note, so this is not read as more than it is: both claims here run
    /// against one `SocialStore`, whose connection is behind an async mutex, so they
    /// serialize. What this proves is that the SQL PREDICATE is self-excluding —
    /// the second claim finds nothing because the first already moved the row out of
    /// `status = 'scheduled'`, not because a lock hid it. That is the property that
    /// also holds across processes, where the mutex does not exist.
    #[tokio::test]
    async fn two_concurrent_sweeps_cannot_claim_the_same_post() {
        let s = store().await;
        let (post_id, _) = scheduled_post(&s, 1_000).await;

        let a = s.clone();
        let b = s.clone();
        let (first, second) = tokio::join!(
            tokio::spawn(async move { a.claim_due_posts(5_000, 10).await.unwrap() }),
            tokio::spawn(async move { b.claim_due_posts(5_000, 10).await.unwrap() }),
        );
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(
            first.len() + second.len(),
            1,
            "exactly one sweep may claim the post, got {} + {}",
            first.len(),
            second.len()
        );
        assert_eq!(
            s.get_scheduled_post(&post_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Due
        );

        // And a third sweep, run after the fact, still finds nothing: the flip is
        // durable, not a lock that releases.
        assert!(s.claim_due_posts(5_000, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_post_whose_time_has_not_come_is_not_claimed() {
        let s = store().await;
        let (post_id, _) = scheduled_post(&s, 10_000).await;
        assert!(s.claim_due_posts(9_999, 10).await.unwrap().is_empty());
        assert_eq!(
            s.get_scheduled_post(&post_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Scheduled
        );
    }

    /// The runner's claim is the second half of the handoff: only one worker may take
    /// a `due` post to `publishing`.
    #[tokio::test]
    async fn only_one_runner_can_claim_a_due_post() {
        let s = store().await;
        let (post_id, _) = scheduled_post(&s, 0).await;
        s.claim_due_posts(1_000, 10).await.unwrap();
        assert!(s.claim_post_for_publishing(&post_id).await.unwrap());
        assert!(!s.claim_post_for_publishing(&post_id).await.unwrap());
    }

    // ── Lease + backoff sizing ─────────────────────────────────────────────────

    #[tokio::test]
    async fn an_expired_lease_returns_its_target_and_post_to_the_queue() {
        let s = store().await;
        let (post_id, target_id) = scheduled_post(&s, 0).await;
        s.claim_due_posts(1_000, 10).await.unwrap();
        assert!(s.claim_post_for_publishing(&post_id).await.unwrap());
        // Stamp the lease at t=1_000 — `claim_target` is the only writer of
        // `claimed_at`, so an old `now` is how a stale lease is simulated.
        assert!(s.claim_target(&target_id, 1_000).await.unwrap());

        // A cutoff BEFORE the stamp leaves it alone: a live run must survive the
        // reaper, which is the whole reason the lease has a TTL at all.
        assert!(s.reap_expired_claims(900, 5_000).await.unwrap().is_empty());
        assert_eq!(
            s.get_scheduled_post(&post_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Publishing
        );

        // A cutoff after it reclaims both the target and its parent post.
        let reaped = s.reap_expired_claims(2_000, 5_000).await.unwrap();
        assert_eq!(reaped, vec![target_id.clone()]);
        let post = s.get_scheduled_post(&post_id).await.unwrap().unwrap();
        assert_eq!(post.status, PostStatus::Due);
        let target = post.targets.iter().find(|t| t.id == target_id).unwrap();
        assert_eq!(target.status, TargetStatus::Pending);
        assert_eq!(target.claimed_at, None);
        assert_eq!(target.next_attempt_at, Some(5_000));
    }

    /// The coupling that a plain settings read would miss: a retry policy whose
    /// worst case exceeds the configured lease must WIDEN the lease, or the reaper
    /// recycles a healthy publish and (on a provider that ignores the idempotency
    /// key) double-posts.
    #[test]
    fn the_lease_is_widened_to_cover_the_configured_retry_policy() {
        let defaults = SocialSettings::default();
        // 3 attempts, 1s base ⇒ sleeps 1s + 2s = 3s, calls 3 × 30s = 90s, +60s
        // margin = 153s. The configured 300s floor already covers that, so it wins.
        assert_eq!(
            effective_lease_ms(&defaults),
            defaults.claim_lease_secs * 1_000
        );

        let greedy = SocialSettings {
            max_attempts: 10,
            base_backoff_ms: 10_000,
            claim_lease_secs: 300,
            ..SocialSettings::default()
        };
        let required = worst_case_publish_ms(10, 10_000) + LEASE_MARGIN_MS;
        assert!(
            required > 300 * 1_000,
            "this policy must genuinely exceed the configured lease for the test to bite"
        );
        assert_eq!(effective_lease_ms(&greedy), required);
    }

    #[test]
    fn worst_case_counts_one_fewer_sleep_than_attempts() {
        // 3 attempts ⇒ 2 sleeps (1s, 2s). No backoff after the final attempt.
        assert_eq!(
            worst_case_publish_ms(3, 1_000),
            1_000 + 2_000 + 3 * PER_ATTEMPT_ALLOWANCE_MS
        );
        // A single attempt sleeps not at all.
        assert_eq!(worst_case_publish_ms(1, 1_000), PER_ATTEMPT_ALLOWANCE_MS);
        assert_eq!(worst_case_publish_ms(0, 1_000), 0);
    }

    #[test]
    fn policy_takes_the_shortest_cadence_and_the_longest_lease() {
        let fast = SocialSettings {
            poll_interval_secs: 10,
            ..SocialSettings::default()
        };
        let slow_but_patient = SocialSettings {
            poll_interval_secs: 600,
            claim_lease_secs: 3_600,
            ..SocialSettings::default()
        };
        let policy = merge_policy(&[("a".into(), fast), ("b".into(), slow_but_patient)]);
        assert_eq!(policy.poll_interval, Duration::from_secs(10));
        assert_eq!(policy.lease, Duration::from_secs(3_600));
        assert!(policy.is_enabled("a") && policy.is_enabled("b"));
    }

    #[test]
    fn a_disabled_workspace_neither_drags_the_cadence_nor_loses_its_lease() {
        let off = SocialSettings {
            scheduler_enabled: false,
            poll_interval_secs: 5,
            claim_lease_secs: 7_200,
            ..SocialSettings::default()
        };
        let on = SocialSettings {
            poll_interval_secs: 120,
            ..SocialSettings::default()
        };
        let policy = merge_policy(&[("off".into(), off), ("on".into(), on)]);
        // The disabled workspace's 5s cadence is not a request for anything.
        assert_eq!(policy.poll_interval, Duration::from_secs(120));
        // But its in-flight publish still gets its lease honoured.
        assert_eq!(policy.lease, Duration::from_secs(7_200));
        assert!(!policy.is_enabled("off"));
        assert!(policy.any_enabled());

        // Every workspace off ⇒ nothing is claimed at all.
        let all_off = merge_policy(&[(
            "off".into(),
            SocialSettings {
                scheduler_enabled: false,
                ..SocialSettings::default()
            },
        )]);
        assert!(!all_off.any_enabled());
    }

    #[test]
    fn a_pathological_cadence_is_clamped_rather_than_honoured() {
        let busy = merge_policy(&[(
            "a".into(),
            SocialSettings {
                poll_interval_secs: 0,
                ..SocialSettings::default()
            },
        )]);
        assert_eq!(busy.poll_interval, MIN_POLL_INTERVAL);
        let glacial = merge_policy(&[(
            "a".into(),
            SocialSettings {
                poll_interval_secs: 86_400,
                ..SocialSettings::default()
            },
        )]);
        assert_eq!(glacial.poll_interval, MAX_POLL_INTERVAL);
    }

    /// The backoff progression the runner writes into `next_attempt_at`, asserted
    /// through the store rather than over the pure function (which `publish`'s own
    /// tests already cover): what matters here is that a retrying target stays
    /// visible to the queue with a FUTURE run time.
    #[tokio::test]
    async fn a_backing_off_target_carries_its_next_attempt_forward() {
        let s = store().await;
        let (post_id, target_id) = scheduled_post(&s, 0).await;
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post_id).await.unwrap();

        let base = SocialSettings::default().base_backoff_ms;
        let mut at = 1_000i64;
        for attempt in 1..=2u32 {
            let delay = crate::publish::backoff_delay_ms(attempt, base) as i64;
            at += delay;
            s.settle_target(&target_id, TargetStatus::Pending, attempt, Some(at))
                .await
                .unwrap();
            let queued = s.list_queue(DEFAULT_WORKSPACE_ID, 10).await.unwrap();
            let row = queued.iter().find(|q| q.target.id == target_id).unwrap();
            assert_eq!(row.target.attempts, attempt);
            assert_eq!(row.next_attempt_at, at);
        }
        // 1s then 2s: doubling, and strictly increasing.
        assert_eq!(at, 1_000 + 1_000 + 2_000);
    }

    // ── Boot recovery ──────────────────────────────────────────────────────────

    /// Orphan shape 2: the process died between claiming the post and claiming its
    /// first target, so no lease was ever stamped and the per-tick reaper is blind
    /// to it. Without the boot pass this row is stuck forever.
    #[tokio::test]
    async fn boot_recovery_requeues_a_post_that_never_reached_its_first_target() {
        let s = store().await;
        let (post_id, target_id) = scheduled_post(&s, 0).await;
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post_id).await.unwrap();
        // No `claim_target` — this is the gap.
        assert!(s
            .reap_expired_claims(i64::MAX, 5_000)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            s.get_scheduled_post(&post_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Publishing
        );

        assert!(s.requeue_publishing_post(&post_id, 5_000).await.unwrap());
        let post = s.get_scheduled_post(&post_id).await.unwrap().unwrap();
        assert_eq!(post.status, PostStatus::Due);
        let target = post.targets.iter().find(|t| t.id == target_id).unwrap();
        assert_eq!(target.status, TargetStatus::Pending);
        assert_eq!(target.next_attempt_at, Some(5_000));
    }

    /// The guard that keeps recovery from BECOMING the bug: targets publish
    /// sequentially, so a healthy two-target post is normally
    /// `t1 = publishing, t2 = pending`. Requeueing that underneath its live runner is
    /// a double-publish.
    #[tokio::test]
    async fn boot_recovery_leaves_a_post_with_a_live_target_alone() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let other = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@me2", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[
                    NewTarget {
                        social_account_id: account.id,
                        platform: Platform::X,
                        variant_body: None,
                    },
                    NewTarget {
                        social_account_id: other.id,
                        platform: Platform::Bluesky,
                        variant_body: None,
                    },
                ],
            )
            .await
            .unwrap();
        let live = post.targets[0].id.clone();
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post.id).await.unwrap();
        s.claim_target(&live, 1_000).await.unwrap();

        assert!(
            !s.requeue_publishing_post(&post.id, 5_000).await.unwrap(),
            "a post with a target still publishing must not be recycled"
        );
        assert_eq!(
            s.get_scheduled_post(&post.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Publishing
        );
    }

    /// Orphan shape 3: every target settled, the post did not. There is no work left,
    /// so requeueing it would hand the runner nothing to claim and settle a fully
    /// published post as `failed`.
    #[tokio::test]
    async fn boot_recovery_settles_a_post_whose_targets_all_finished() {
        let s = store().await;
        let (post_id, target_id) = scheduled_post(&s, 0).await;
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post_id).await.unwrap();
        s.settle_target(&target_id, TargetStatus::Published, 1, None)
            .await
            .unwrap();
        // Died here, before `settle_post`.

        let post = s
            .list_posts_with_status(PostStatus::Publishing, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(settle_status_from_targets(&post), PostStatus::Published);
        assert!(s
            .settle_post(&post_id, PostStatus::Published)
            .await
            .unwrap());
        assert_eq!(
            s.get_scheduled_post(&post_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Published
        );
    }

    #[tokio::test]
    async fn the_whole_boot_pass_repairs_every_orphan_shape_in_one_go() {
        let s = store().await;
        let state = AppState::new(s.clone(), crate::state::Config::from_env(0));

        // Shape 1: a stamped lease.
        let (leased_post, leased_target) = scheduled_post(&s, 0).await;
        // Shape 2: claimed, never reached a target.
        let (stranded_post, _) = scheduled_post(&s, 0).await;
        // Shape 3: targets done, post not settled.
        let (finished_post, finished_target) = scheduled_post(&s, 0).await;

        s.claim_due_posts(1_000, 10).await.unwrap();
        for id in [&leased_post, &stranded_post, &finished_post] {
            s.claim_post_for_publishing(id).await.unwrap();
        }
        s.claim_target(&leased_target, 1_000).await.unwrap();
        s.settle_target(&finished_target, TargetStatus::Failed, 3, None)
            .await
            .unwrap();

        let report = recover_orphaned_work(&state).await.unwrap();
        assert_eq!(report.reaped_targets, 1);
        assert_eq!(report.requeued_posts, 1);
        assert_eq!(report.settled_posts, 1);
        assert!(!report.is_empty());

        let status = |id: String| {
            let s = s.clone();
            async move { s.get_scheduled_post(&id).await.unwrap().unwrap().status }
        };
        assert_eq!(status(leased_post).await, PostStatus::Due);
        assert_eq!(status(stranded_post).await, PostStatus::Due);
        // Its one target failed, so the post is failed — not silently published.
        assert_eq!(status(finished_post).await, PostStatus::Failed);

        // Idempotent: a second pass finds nothing to do.
        assert!(recover_orphaned_work(&state).await.unwrap().is_empty());
    }

    // ── The tick ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_tick_claims_due_work_and_leaves_it_recoverable_when_the_runner_declines() {
        let s = store().await;
        let state = AppState::new(s.clone(), crate::state::Config::from_env(0));
        let (post_id, _) = scheduled_post(&s, 0).await;

        tick(&state, &TickPolicy::default()).await;

        // `publish::run_post` is another module's seam. Whether it is a stub or a
        // real runner, the invariant this asserts is the same: the post left
        // `scheduled`, and it is still findable — a declined run is a recoverable
        // intermediate state, never a lost row.
        let status = s
            .get_scheduled_post(&post_id)
            .await
            .unwrap()
            .unwrap()
            .status;
        assert_ne!(status, PostStatus::Scheduled);
        assert!(!s
            .list_scheduled_posts(DEFAULT_WORKSPACE_ID, &[])
            .await
            .unwrap()
            .is_empty());
    }

    /// Every workspace off ⇒ the sweep is skipped outright and nothing moves. This is
    /// the case `SocialSettings::scheduler_enabled`'s doc describes.
    #[tokio::test]
    async fn a_tick_with_every_workspace_disabled_leaves_posts_scheduled() {
        let s = store().await;
        let state = AppState::new(s.clone(), crate::state::Config::from_env(0));
        let (post_id, _) = scheduled_post(&s, 0).await;
        s.put_settings(
            DEFAULT_WORKSPACE_ID,
            &SocialSettings {
                scheduler_enabled: false,
                ..SocialSettings::default()
            },
        )
        .await
        .unwrap();

        let policy = read_policy(&state).await;
        assert!(!policy.any_enabled());
        tick(&state, &policy).await;

        // Untouched, so re-enabling resumes it with no repair.
        assert_eq!(
            s.get_scheduled_post(&post_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PostStatus::Scheduled
        );
    }

    /// The MIXED case, which behaves differently and must not be assumed from the
    /// test above: `claim_due_posts` has no workspace predicate, so a disabled
    /// workspace's post is flipped to `due` alongside everyone else's and is then
    /// held at the run step. Pinned here so the divergence is a decision on the
    /// record rather than a surprise.
    #[tokio::test]
    async fn a_disabled_workspace_is_held_at_the_run_step_not_the_claim() {
        let s = store().await;
        let state = AppState::new(s.clone(), crate::state::Config::from_env(0));

        let off = s.create_workspace("Paused").await.unwrap();
        s.put_settings(
            &off.id,
            &SocialSettings {
                scheduler_enabled: false,
                ..SocialSettings::default()
            },
        )
        .await
        .unwrap();
        let account = s
            .create_account(&off.id, Platform::X, "@paused", None)
            .await
            .unwrap();
        let held = s
            .create_scheduled_post(
                &off.id,
                None,
                0,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();

        // The default workspace stays enabled and has its own due post.
        let (live, _) = scheduled_post(&s, 0).await;

        let policy = read_policy(&state).await;
        assert!(policy.any_enabled());
        assert!(policy.is_enabled(DEFAULT_WORKSPACE_ID));
        assert!(!policy.is_enabled(&off.id));

        tick(&state, &policy).await;

        // Held, not published — the safety property. It sits in `due`, which is where
        // re-enabling the workspace picks it back up.
        let held_after = s.get_scheduled_post(&held.id).await.unwrap().unwrap();
        assert_eq!(held_after.status, PostStatus::Due);
        assert!(
            held_after
                .targets
                .iter()
                .all(|t| t.status == TargetStatus::Pending),
            "a held post must not have had any target claimed"
        );

        // And the enabled workspace's post was genuinely handed to the runner.
        assert_ne!(
            s.get_scheduled_post(&live).await.unwrap().unwrap().status,
            PostStatus::Scheduled
        );
    }

    // ── The timing recommender ─────────────────────────────────────────────────

    /// 2026-08-10 is a Monday. Every fixture below is built from it so the weekday
    /// assertions are readable.
    const MON_2026_08_10_UTC: i64 = 1_786_320_000_000;

    fn sample(offset_days: i64, hour: i64, engagement: u64) -> TimingSample {
        TimingSample {
            published_at: MON_2026_08_10_UTC + offset_days * 86_400_000 + hour * 3_600_000,
            engagement,
        }
    }

    #[test]
    fn the_fixture_monday_really_is_a_monday() {
        // Guards every weekday assertion below against a mistyped epoch constant.
        assert_eq!(
            local_weekday_hour(chrono_tz::UTC, MON_2026_08_10_UTC),
            Some((1, 0))
        );
    }

    #[test]
    fn too_little_history_returns_the_documented_defaults_and_says_so() {
        let thin = vec![sample(0, 9, 100), sample(1, 9, 100), sample(2, 9, 100)];
        let rec = recommend_slots(&thin, chrono_tz::UTC, MON_2026_08_10_UTC);
        assert_eq!(rec.basis, TimingBasis::Defaults);
        assert_eq!(rec.sample_size, 3);
        assert_eq!(rec.slots.len(), MAX_SLOTS);
        assert_eq!(
            rec.slots
                .iter()
                .map(|s| (s.day_of_week.unwrap(), s.hour))
                .collect::<Vec<_>>(),
            DEFAULT_SLOTS.to_vec()
        );
        // No lift is invented for a default slot, and every one is projected forward.
        assert!(rec.slots.iter().all(|s| s.lift_pct.is_none()));
        assert!(rec
            .slots
            .iter()
            .all(|s| s.next_occurrence > MON_2026_08_10_UTC));

        // An empty history takes the same path rather than erroring.
        let empty = recommend_slots(&[], chrono_tz::UTC, MON_2026_08_10_UTC);
        assert_eq!(empty.basis, TimingBasis::Defaults);
        assert_eq!(empty.sample_size, 0);

        // So does a history with no usable timestamps.
        let undated = recommend_slots(
            &[TimingSample {
                published_at: 0,
                engagement: 999,
            }],
            chrono_tz::UTC,
            MON_2026_08_10_UTC,
        );
        assert_eq!(undated.basis, TimingBasis::Defaults);
    }

    #[test]
    fn repeated_weekday_hours_are_ranked_by_engagement_with_an_honest_lift() {
        // Tuesday 18:00 twice, big numbers; Monday 09:00 twice, small ones.
        let samples = vec![
            sample(1, 18, 100),
            sample(8, 18, 140),
            sample(0, 9, 10),
            sample(7, 9, 10),
        ];
        let rec = recommend_slots(&samples, chrono_tz::UTC, MON_2026_08_10_UTC);
        assert_eq!(rec.basis, TimingBasis::WeekdayHour);
        assert_eq!(rec.sample_size, 4);
        // Tuesday = ISO weekday 2.
        assert_eq!(rec.slots[0].day_of_week, Some(2));
        assert_eq!(rec.slots[0].hour, 18);
        assert_eq!(rec.slots[0].sample_size, 2);
        // baseline = (100+140+10+10)/4 = 65; Tuesday mean = 120 ⇒ +85%.
        assert_eq!(rec.slots[0].lift_pct, Some(85));
        // The weaker bucket is still returned, with a negative lift rather than none.
        assert_eq!(rec.slots[1].day_of_week, Some(1));
        assert!(rec.slots[1].lift_pct.unwrap() < 0);
    }

    #[test]
    fn hours_pool_across_weekdays_when_no_single_weekday_repeats() {
        // Four posts, four different weekdays — no fine bucket reaches 2 — but three
        // of them land at 20:00.
        let samples = vec![
            sample(0, 20, 50),
            sample(1, 20, 70),
            sample(2, 20, 60),
            sample(3, 8, 5),
        ];
        let rec = recommend_slots(&samples, chrono_tz::UTC, MON_2026_08_10_UTC);
        assert_eq!(rec.basis, TimingBasis::HourOfDay);
        assert_eq!(rec.slots[0].hour, 20);
        // An hour-of-day slot genuinely has no weekday behind it and must not claim one.
        assert!(rec.slots[0].day_of_week.is_none());
        assert_eq!(rec.slots[0].sample_size, 3);
        // The single-post 08:00 bucket reports no lift — one post's deviation is noise.
        let lonely = rec.slots.iter().find(|s| s.hour == 8).unwrap();
        assert_eq!(lonely.lift_pct, None);
    }

    #[test]
    fn a_zero_engagement_history_reports_no_lift_rather_than_a_meaningless_percent() {
        let samples = vec![
            sample(1, 18, 0),
            sample(8, 18, 0),
            sample(0, 9, 0),
            sample(7, 9, 0),
        ];
        let rec = recommend_slots(&samples, chrono_tz::UTC, MON_2026_08_10_UTC);
        assert_eq!(rec.basis, TimingBasis::WeekdayHour);
        assert!(rec.slots.iter().all(|s| s.lift_pct.is_none()));
    }

    #[test]
    fn slots_are_bucketed_in_the_workspace_zone_not_utc() {
        let tz: Tz = "Asia/Tokyo".parse().unwrap();
        // 2026-08-10 22:00 UTC is Tuesday 07:00 in Tokyo (UTC+9) — a different day
        // AND a different hour, so a UTC implementation cannot pass this.
        assert_eq!(
            local_weekday_hour(tz, MON_2026_08_10_UTC + 22 * 3_600_000),
            Some((2, 7))
        );
        let rec = recommend_slots(
            &[
                sample(0, 22, 10),
                sample(7, 22, 10),
                sample(1, 22, 1),
                sample(8, 22, 1),
            ],
            tz,
            MON_2026_08_10_UTC,
        );
        assert_eq!(rec.timezone, "Asia/Tokyo");
        assert_eq!(rec.slots[0].day_of_week, Some(2));
        assert_eq!(rec.slots[0].hour, 7);
    }

    #[test]
    fn an_unknown_timezone_degrades_to_utc_instead_of_failing() {
        assert_eq!(zone_for("Middle/Earth"), chrono_tz::UTC);
        assert_eq!(zone_for("  Europe/Berlin  "), chrono_tz::Europe::Berlin);
        assert_eq!(zone_for(""), chrono_tz::UTC);
    }

    #[test]
    fn next_occurrence_lands_on_the_right_local_weekday_and_hour() {
        let tz = chrono_tz::UTC;
        // From Monday 00:00, the next Tuesday 11:00 is 35 hours away.
        let at = next_occurrence(tz, MON_2026_08_10_UTC, Some(2), 11);
        assert_eq!(at, MON_2026_08_10_UTC + 35 * 3_600_000);
        assert_eq!(local_weekday_hour(tz, at), Some((2, 11)));

        // Same weekday, hour already past ⇒ next week, not today.
        let noon_monday = MON_2026_08_10_UTC + 12 * 3_600_000;
        let next_monday_nine = next_occurrence(tz, noon_monday, Some(1), 9);
        assert!(next_monday_nine > noon_monday);
        assert_eq!(local_weekday_hour(tz, next_monday_nine), Some((1, 9)));
        assert_eq!(
            next_monday_nine,
            MON_2026_08_10_UTC + 7 * 86_400_000 + 9 * 3_600_000
        );

        // An hour-only slot takes the next occurrence of that hour on any day.
        let any_day = next_occurrence(tz, noon_monday, None, 20);
        assert_eq!(any_day, MON_2026_08_10_UTC + 20 * 3_600_000);
    }

    /// The reason this is a calendar walk and not `+ 7 * 86_400_000`: across a DST
    /// boundary, fixed arithmetic drifts the wall-clock hour by one.
    #[test]
    fn next_occurrence_holds_the_wall_clock_hour_across_a_dst_boundary() {
        let tz: Tz = "America/New_York".parse().unwrap();
        // 2026-10-29 is a Thursday, still on EDT (UTC-4). US DST ends 2026-11-01.
        // 20:00 is deliberately PAST the 18:00 slot, so the walk has to cross the
        // boundary instead of landing later the same day.
        let before = tz
            .with_ymd_and_hms(2026, 10, 29, 20, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        // The next Thursday 18:00 local is 2026-11-05, by which point the zone is on
        // EST (UTC-5). Naive arithmetic would land at 17:00 local.
        let at = next_occurrence(tz, before, Some(4), 18);
        assert_eq!(local_weekday_hour(tz, at), Some((4, 18)));
        // 20:00 Thu → 18:00 the next Thu is 6d22h on the wall clock, but 6d23h of
        // real elapsed time. That extra hour IS the DST shift, and it is exactly what
        // fixed-millisecond arithmetic would have swallowed.
        let elapsed_hours = (at - before) / 3_600_000;
        assert_eq!(elapsed_hours, 6 * 24 + 22 + 1);
    }

    /// Spring forward: 02:00 does not exist on the transition date. The slot must
    /// nudge rather than silently returning a wrong instant or nothing at all.
    #[test]
    fn a_slot_inside_a_spring_forward_gap_is_nudged_to_the_next_real_hour() {
        let tz: Tz = "America/New_York".parse().unwrap();
        // 2026-03-08 02:00 local does not exist (02:00 → 03:00).
        let gap_day = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        assert!(tz
            .from_local_datetime(&gap_day.and_hms_opt(2, 0, 0).unwrap())
            .earliest()
            .is_none());
        let at = local_instant(tz, gap_day, 2).expect("the gap must nudge, not vanish");
        assert_eq!(local_weekday_hour(tz, at), Some((7, 3)));
    }

    #[tokio::test]
    async fn the_store_backed_recommender_degrades_to_defaults_on_an_empty_workspace() {
        let s = store().await;
        let state = AppState::new(s, crate::state::Config::from_env(0));
        let rec = best_times(&state, DEFAULT_WORKSPACE_ID, None)
            .await
            .unwrap();
        assert_eq!(rec.basis, TimingBasis::Defaults);
        assert_eq!(rec.sample_size, 0);
        // The default settings zone, round-tripped through the parser.
        assert_eq!(rec.timezone, "UTC");
    }

    #[test]
    fn an_activity_row_with_no_publish_time_is_not_a_timing_sample() {
        let mut item = ActivityItem {
            id: "act_1".into(),
            workspace_id: DEFAULT_WORKSPACE_ID.into(),
            social_account_id: "acc_1".into(),
            platform: Platform::X,
            post_remote_id: "remote-1".into(),
            permalink: None,
            text: None,
            likes: 4,
            comments: 3,
            shares: 2,
            views: 10_000,
            engagement_fetched_at: Some(1),
            published_at: None,
        };
        assert!(sample_from_activity(&item).is_none());
        item.published_at = Some(MON_2026_08_10_UTC);
        // 4 + 3 + 2; views are deliberately excluded from the ranking scalar.
        assert_eq!(sample_from_activity(&item).unwrap().engagement, 9);
    }
}
