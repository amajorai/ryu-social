//! Node-scoped settings: the knobs and credentials that belong to the INSTALL
//! rather than to one workspace.
//!
//! ## Why this is a second settings blob, not more fields on [`SocialSettings`]
//!
//! [`crate::models::SocialSettings`] is per-WORKSPACE: a user with a client
//! workspace and a personal one legitimately wants a different timezone and a
//! different "enforce platform limits" in each. Credentials are the opposite — one
//! Composio key and one Bluesky app password serve the whole node, and duplicating
//! them per workspace would mean re-entering them for every workspace and having no
//! answer for which copy the provider registry should believe.
//!
//! So the split is by OWNERSHIP, not by convenience:
//!
//! | lives in [`SocialSettings`] (per workspace) | lives in [`NodeSettings`] (per node) |
//! |---|---|
//! | `scheduler_enabled`, `poll_interval_secs` | provider credentials |
//! | `max_attempts`, `base_backoff_ms` | `inbox_poll_interval_secs` |
//! | `claim_lease_secs` | `engagement_refresh_interval_secs` |
//! | `timezone`, `enforce_platform_limits` | `default_platforms`, `quiet_hours` |
//!
//! **The tick interval and the retry policy are deliberately NOT duplicated here.**
//! `poll_interval_secs` / `max_attempts` / `base_backoff_ms` / `claim_lease_secs`
//! already exist on `SocialSettings` and the scheduler and publish modules read them
//! from there. A second copy in this blob would be two sources of truth for one
//! knob, and the loser would be silently ignored — which is exactly the failure a
//! settings screen must never have.
//!
//! ## Storage: a reserved key in the existing `settings` table
//!
//! The `settings` table is keyed by `workspace_id`; this blob is stored under the
//! reserved key [`NODE_SETTINGS_KEY`], which no real workspace id can collide with
//! (ids are `default` or `ws_<uuid>`). That deliberately avoids a `SCHEMA_VERSION`
//! bump for a single row — a bump is a shared resource, and two modules racing to
//! claim `v2` is a merge conflict in a migration ladder.
//!
//! ## Secrets — the rules this module enforces
//!
//! 1. **A secret is never returned.** [`NodeSettings::redacted`] is the ONLY shape
//!    that reaches an HTTP response, and it carries `*_set: bool` presence flags, not
//!    values. A GET that echoed the key back would put it in every proxy log, every
//!    browser devtools panel, and every screenshot of the settings tab.
//! 2. **A secret is accepted only on PATCH**, as plaintext, and an empty string
//!    clears it. "Absent means unchanged" is what lets the UI submit the whole form
//!    without having to round-trip a value it was never given.
//! 3. **A secret is never logged**, at any level. There is no `Debug` derive that
//!    prints one — see the manual [`std::fmt::Debug`] impl below, which is load-bearing
//!    rather than decorative: a `#[derive(Debug)]` here would leak the key the first
//!    time anything traced this struct.
//! 4. **Environment beats storage.** Every credential reader checks its `RYU_SOCIAL_*`
//!    env var first, so an operator can inject credentials at spawn and never write
//!    them to disk at all.
//!
//! ### Known limitation, recorded rather than hidden
//!
//! A credential set through PATCH is stored **in the SQLite file in plaintext**. The
//! design this is ported from kept these in the OS keychain via a Tauri secure-storage
//! seam, which a standalone sidecar does not have. Obfuscating the bytes here would be
//! worse than plaintext, because it would imply a protection that is not there. The
//! real fix is a host-provided secret seam; until then the documented posture is:
//! prefer the env vars, and treat `social.db` as sensitive.

use serde::{Deserialize, Serialize};

use crate::models::{Platform, DEFAULT_WORKSPACE_ID};
use crate::store::SocialStore;

/// The reserved `settings.workspace_id` this blob lives under.
///
/// Deliberately not a valid workspace id. Note `delete_workspace`'s cascade deletes
/// `settings WHERE workspace_id = ?1` with a real id, so removing a workspace can
/// never take the node credentials with it.
pub const NODE_SETTINGS_KEY: &str = "__node__";

// ── Credential environment overrides ───────────────────────────────────────────

const ENV_COMPOSIO_API_KEY: &str = "RYU_SOCIAL_COMPOSIO_API_KEY";
const ENV_BLUESKY_HANDLE: &str = "RYU_SOCIAL_BLUESKY_HANDLE";
const ENV_BLUESKY_APP_PASSWORD: &str = "RYU_SOCIAL_BLUESKY_APP_PASSWORD";
const ENV_THREADS_ACCESS_TOKEN: &str = "RYU_SOCIAL_THREADS_ACCESS_TOKEN";
const ENV_THREADS_USER_ID: &str = "RYU_SOCIAL_THREADS_USER_ID";

/// Read a credential from the environment, treating blank as absent.
fn from_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Env first, stored second. Returns which source won alongside the value so the
/// settings UI can say "set by the environment" instead of offering an edit box that
/// would appear to do nothing.
fn resolve(env_key: &str, stored: Option<&String>) -> Option<(String, CredentialSource)> {
    if let Some(v) = from_env(env_key) {
        return Some((v, CredentialSource::Env));
    }
    stored
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| (s.to_string(), CredentialSource::Stored))
}

/// Where a credential came from. Surfaced (not the value) so the UI can disable an
/// input the environment is overriding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CredentialSource {
    /// Injected at spawn; never written to `social.db`.
    Env,
    /// Set through `PATCH /settings` and persisted.
    Stored,
    Unset,
}

// ── Quiet hours ────────────────────────────────────────────────────────────────

/// A daily window during which this node does no outbound work of its own.
///
/// Hours are LOCAL, resolved with [`NodeSettings::utc_offset_minutes`] rather than an
/// IANA zone: this crate depends on `chrono` without `chrono-tz`, so there is no zone
/// database to resolve `Europe/Berlin` against. A fixed offset is honest about that;
/// silently treating an IANA string as UTC would not be. `SocialSettings::timezone`
/// remains the display label.
///
/// The window WRAPS: `start_hour: 22, end_hour: 7` means 22:00–07:00, which is the
/// only shape anyone actually configures. `start == end` means the window is empty,
/// not "all day" — a settings mistake must never silently pause the whole node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuietHours {
    pub enabled: bool,
    /// 0–23, local.
    pub start_hour: u32,
    /// 0–23, local, exclusive.
    pub end_hour: u32,
}

impl Default for QuietHours {
    fn default() -> Self {
        Self {
            enabled: false,
            start_hour: 22,
            end_hour: 7,
        }
    }
}

impl QuietHours {
    /// Is `hour` (local, 0–23) inside the window?
    pub fn contains_hour(self, hour: u32) -> bool {
        if !self.enabled || self.start_hour == self.end_hour {
            return false;
        }
        let (start, end) = (self.start_hour % 24, self.end_hour % 24);
        if start < end {
            (start..end).contains(&hour)
        } else {
            // Wrapping window: 22..24 ∪ 0..7.
            hour >= start || hour < end
        }
    }
}

/// The local hour-of-day at `at_ms`, given a fixed UTC offset in minutes.
///
/// Total by construction: a timestamp chrono cannot represent yields hour 0 rather
/// than an error, because "we could not compute the hour" must not become "the whole
/// poller stopped".
pub fn local_hour(at_ms: i64, utc_offset_minutes: i32) -> u32 {
    use chrono::Timelike;
    chrono::DateTime::from_timestamp_millis(at_ms)
        .map(|dt| (dt + chrono::TimeDelta::minutes(i64::from(utc_offset_minutes))).hour())
        .unwrap_or(0)
}

// ── The blob ───────────────────────────────────────────────────────────────────

/// Node-scoped settings. Every field carries `#[serde(default)]` so a blob written by
/// an older build — or by a build that had not yet grown a field — still decodes
/// instead of resetting the user's credentials to nothing.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSettings {
    // ── Credentials (secret; never serialized to a response) ──
    #[serde(default)]
    pub composio_api_key: Option<String>,
    /// The Bluesky `@handle`. A public identifier, NOT a secret — it is returned
    /// verbatim in the redacted view so the settings tab can show which account is
    /// wired up.
    #[serde(default)]
    pub bluesky_handle: Option<String>,
    /// An APP PASSWORD, never the account password.
    #[serde(default)]
    pub bluesky_app_password: Option<String>,
    #[serde(default)]
    pub threads_access_token: Option<String>,
    /// The Threads user id. A public identifier, like the Bluesky handle.
    #[serde(default)]
    pub threads_user_id: Option<String>,

    // ── Non-secret node policy ──
    /// The workspace a request with no `?workspace_id=` resolves to.
    #[serde(default = "default_workspace")]
    pub default_workspace_id: String,
    /// Master switch for the background inbox poll.
    #[serde(default = "default_true")]
    pub inbox_polling_enabled: bool,
    /// How often the background poller asks each provider for new inbound
    /// engagement. Ten minutes by default: an inbox is not a chat, and a tighter
    /// loop spends a third-party rate-limit budget on nothing.
    #[serde(default = "default_inbox_interval")]
    pub inbox_poll_interval_secs: u64,
    /// How often published posts' engagement snapshots are re-read. Deliberately
    /// much slower than the inbox: counts move over days, and every refresh is a
    /// billable third-party call.
    #[serde(default = "default_engagement_interval")]
    pub engagement_refresh_interval_secs: u64,
    /// How many published posts one batched engagement pass may refresh. Bounds the
    /// blast radius against a rate limit on a workspace with a long history.
    #[serde(default = "default_engagement_batch")]
    pub engagement_refresh_batch: usize,
    /// The platforms a fresh compose pre-selects. Empty means "no preselection",
    /// which is the honest default before any account is connected.
    #[serde(default)]
    pub default_platforms: Vec<Platform>,
    /// Fixed offset from UTC, in minutes (e.g. `-480` for PST, `60` for CET). Used
    /// for quiet hours and for bucketing analytics by local day/hour.
    #[serde(default)]
    pub utc_offset_minutes: i32,
    #[serde(default)]
    pub quiet_hours: QuietHours,
}

fn default_workspace() -> String {
    DEFAULT_WORKSPACE_ID.to_string()
}
const fn default_true() -> bool {
    true
}
const fn default_inbox_interval() -> u64 {
    600
}
const fn default_engagement_interval() -> u64 {
    6 * 3_600
}
const fn default_engagement_batch() -> usize {
    25
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            composio_api_key: None,
            bluesky_handle: None,
            bluesky_app_password: None,
            threads_access_token: None,
            threads_user_id: None,
            default_workspace_id: default_workspace(),
            inbox_polling_enabled: default_true(),
            inbox_poll_interval_secs: default_inbox_interval(),
            engagement_refresh_interval_secs: default_engagement_interval(),
            engagement_refresh_batch: default_engagement_batch(),
            default_platforms: Vec::new(),
            utc_offset_minutes: 0,
            quiet_hours: QuietHours::default(),
        }
    }
}

/// Hand-written so a stray `tracing::debug!(?settings)` cannot print a credential.
///
/// This is the reason there is no `#[derive(Debug)]` above. Rule 3 in the module docs
/// is only enforceable if the type physically cannot render its secrets.
impl std::fmt::Debug for NodeSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSettings")
            .field("composio_api_key", &redacted_marker(&self.composio_api_key))
            .field("bluesky_handle", &self.bluesky_handle)
            .field(
                "bluesky_app_password",
                &redacted_marker(&self.bluesky_app_password),
            )
            .field(
                "threads_access_token",
                &redacted_marker(&self.threads_access_token),
            )
            .field("threads_user_id", &self.threads_user_id)
            .field("default_workspace_id", &self.default_workspace_id)
            .field("inbox_polling_enabled", &self.inbox_polling_enabled)
            .field("inbox_poll_interval_secs", &self.inbox_poll_interval_secs)
            .field(
                "engagement_refresh_interval_secs",
                &self.engagement_refresh_interval_secs,
            )
            .field("engagement_refresh_batch", &self.engagement_refresh_batch)
            .field("default_platforms", &self.default_platforms)
            .field("utc_offset_minutes", &self.utc_offset_minutes)
            .field("quiet_hours", &self.quiet_hours)
            .finish()
    }
}

fn redacted_marker(value: &Option<String>) -> &'static str {
    if value.as_deref().is_some_and(|v| !v.trim().is_empty()) {
        "<set>"
    } else {
        "<unset>"
    }
}

impl NodeSettings {
    /// The Composio broker key, env first.
    pub fn composio_api_key(&self) -> Option<String> {
        resolve(ENV_COMPOSIO_API_KEY, self.composio_api_key.as_ref()).map(|(v, _)| v)
    }

    /// Bluesky credentials, env first. `None` unless BOTH halves are present — a
    /// handle with no app password cannot mint a session, and returning a half pair
    /// would make the registry pick the direct adapter and then fail every publish.
    pub fn bluesky_credentials(&self) -> Option<(String, String)> {
        let handle = resolve(ENV_BLUESKY_HANDLE, self.bluesky_handle.as_ref())?.0;
        let password = resolve(ENV_BLUESKY_APP_PASSWORD, self.bluesky_app_password.as_ref())?.0;
        // A leading `@` is what a user types and what the AT-Protocol rejects.
        Some((handle.trim_start_matches('@').to_string(), password))
    }

    /// Threads credentials, env first. Both halves required, same reasoning.
    pub fn threads_credentials(&self) -> Option<(String, String)> {
        let token = resolve(ENV_THREADS_ACCESS_TOKEN, self.threads_access_token.as_ref())?.0;
        let user_id = resolve(ENV_THREADS_USER_ID, self.threads_user_id.as_ref())?.0;
        Some((token, user_id))
    }

    /// Is `at_ms` inside the configured quiet window?
    pub fn in_quiet_hours(&self, at_ms: i64) -> bool {
        self.quiet_hours
            .contains_hour(local_hour(at_ms, self.utc_offset_minutes))
    }

    /// The inbox poll period, floored. A 5-second inbox poll would burn a
    /// third-party rate limit for no user-visible benefit, so the floor is silent
    /// rather than a validation error on a slider.
    pub fn inbox_poll_period(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.inbox_poll_interval_secs.max(60))
    }

    pub fn engagement_refresh_period(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.engagement_refresh_interval_secs.max(300))
    }

    /// The ONLY shape that may leave this process.
    pub fn redacted(&self) -> NodeSettingsView {
        NodeSettingsView {
            composio_api_key_set: self.composio_api_key().is_some(),
            composio_api_key_source: source_of(ENV_COMPOSIO_API_KEY, &self.composio_api_key),
            bluesky_handle: resolve(ENV_BLUESKY_HANDLE, self.bluesky_handle.as_ref())
                .map(|(v, _)| v),
            bluesky_app_password_set: resolve(
                ENV_BLUESKY_APP_PASSWORD,
                self.bluesky_app_password.as_ref(),
            )
            .is_some(),
            bluesky_app_password_source: source_of(
                ENV_BLUESKY_APP_PASSWORD,
                &self.bluesky_app_password,
            ),
            threads_access_token_set: resolve(
                ENV_THREADS_ACCESS_TOKEN,
                self.threads_access_token.as_ref(),
            )
            .is_some(),
            threads_access_token_source: source_of(
                ENV_THREADS_ACCESS_TOKEN,
                &self.threads_access_token,
            ),
            threads_user_id: resolve(ENV_THREADS_USER_ID, self.threads_user_id.as_ref())
                .map(|(v, _)| v),
            default_workspace_id: self.default_workspace_id.clone(),
            inbox_polling_enabled: self.inbox_polling_enabled,
            inbox_poll_interval_secs: self.inbox_poll_interval_secs,
            engagement_refresh_interval_secs: self.engagement_refresh_interval_secs,
            engagement_refresh_batch: self.engagement_refresh_batch,
            default_platforms: self.default_platforms.clone(),
            utc_offset_minutes: self.utc_offset_minutes,
            quiet_hours: self.quiet_hours,
        }
    }

    /// Apply a partial update in place. Clamps are silent, matching the workspace
    /// settings patch: a slider that snaps is friendlier than a 400.
    pub fn apply(&mut self, patch: NodeSettingsPatch) {
        set_secret(&mut self.composio_api_key, patch.composio_api_key);
        set_secret(&mut self.bluesky_handle, patch.bluesky_handle);
        set_secret(&mut self.bluesky_app_password, patch.bluesky_app_password);
        set_secret(&mut self.threads_access_token, patch.threads_access_token);
        set_secret(&mut self.threads_user_id, patch.threads_user_id);

        if let Some(v) = patch.default_workspace_id {
            if !v.trim().is_empty() {
                self.default_workspace_id = v.trim().to_string();
            }
        }
        if let Some(v) = patch.inbox_polling_enabled {
            self.inbox_polling_enabled = v;
        }
        if let Some(v) = patch.inbox_poll_interval_secs {
            self.inbox_poll_interval_secs = v.clamp(60, 24 * 3_600);
        }
        if let Some(v) = patch.engagement_refresh_interval_secs {
            self.engagement_refresh_interval_secs = v.clamp(300, 7 * 24 * 3_600);
        }
        if let Some(v) = patch.engagement_refresh_batch {
            self.engagement_refresh_batch = v.clamp(1, 200);
        }
        if let Some(v) = patch.default_platforms {
            // Deduped, order preserved: the compose chips render in this order and a
            // duplicate would render a duplicate chip.
            let mut seen = Vec::new();
            for p in v {
                if !seen.contains(&p) {
                    seen.push(p);
                }
            }
            self.default_platforms = seen;
        }
        if let Some(v) = patch.utc_offset_minutes {
            // ±14h is the real-world range (Kiribati is +14).
            self.utc_offset_minutes = v.clamp(-14 * 60, 14 * 60);
        }
        if let Some(v) = patch.quiet_hours_enabled {
            self.quiet_hours.enabled = v;
        }
        if let Some(v) = patch.quiet_hours_start {
            self.quiet_hours.start_hour = v.min(23);
        }
        if let Some(v) = patch.quiet_hours_end {
            self.quiet_hours.end_hour = v.min(23);
        }
    }
}

/// Which source a credential would resolve from, for the redacted view.
fn source_of(env_key: &str, stored: &Option<String>) -> CredentialSource {
    match resolve(env_key, stored.as_ref()) {
        Some((_, source)) => source,
        None => CredentialSource::Unset,
    }
}

/// PATCH semantics for one secret: absent = unchanged, `""` = clear, else set
/// (trimmed — a pasted key routinely carries a trailing newline, and an API key with
/// a stray `\n` fails auth in a way nothing surfaces).
fn set_secret(slot: &mut Option<String>, patch: Option<String>) {
    let Some(value) = patch else { return };
    let trimmed = value.trim();
    *slot = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    };
}

/// The redacted projection. **This is the only node-settings shape that is ever
/// serialized into a response** — note there is no field holding a secret's value.
#[derive(Debug, Clone, Serialize)]
pub struct NodeSettingsView {
    pub composio_api_key_set: bool,
    pub composio_api_key_source: CredentialSource,
    pub bluesky_handle: Option<String>,
    pub bluesky_app_password_set: bool,
    pub bluesky_app_password_source: CredentialSource,
    pub threads_access_token_set: bool,
    pub threads_access_token_source: CredentialSource,
    pub threads_user_id: Option<String>,
    pub default_workspace_id: String,
    pub inbox_polling_enabled: bool,
    pub inbox_poll_interval_secs: u64,
    pub engagement_refresh_interval_secs: u64,
    pub engagement_refresh_batch: usize,
    pub default_platforms: Vec<Platform>,
    pub utc_offset_minutes: i32,
    pub quiet_hours: QuietHours,
}

/// A partial update. Deserialize-only, and deliberately NOT `Serialize`: a type that
/// can hold a plaintext secret must not be renderable into a response by accident.
/// Its `Debug` is hand-written for the same reason as [`NodeSettings`]' — the request
/// body is exactly what a "log the payload on 400" reflex would print.
#[derive(Default, Deserialize)]
pub struct NodeSettingsPatch {
    #[serde(default)]
    pub composio_api_key: Option<String>,
    #[serde(default)]
    pub bluesky_handle: Option<String>,
    #[serde(default)]
    pub bluesky_app_password: Option<String>,
    #[serde(default)]
    pub threads_access_token: Option<String>,
    #[serde(default)]
    pub threads_user_id: Option<String>,
    #[serde(default)]
    pub default_workspace_id: Option<String>,
    #[serde(default)]
    pub inbox_polling_enabled: Option<bool>,
    #[serde(default)]
    pub inbox_poll_interval_secs: Option<u64>,
    #[serde(default)]
    pub engagement_refresh_interval_secs: Option<u64>,
    #[serde(default)]
    pub engagement_refresh_batch: Option<usize>,
    #[serde(default)]
    pub default_platforms: Option<Vec<Platform>>,
    #[serde(default)]
    pub utc_offset_minutes: Option<i32>,
    #[serde(default)]
    pub quiet_hours_enabled: Option<bool>,
    #[serde(default)]
    pub quiet_hours_start: Option<u32>,
    #[serde(default)]
    pub quiet_hours_end: Option<u32>,
}

impl std::fmt::Debug for NodeSettingsPatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSettingsPatch")
            .field("composio_api_key", &patch_marker(&self.composio_api_key))
            .field("bluesky_handle", &self.bluesky_handle)
            .field(
                "bluesky_app_password",
                &patch_marker(&self.bluesky_app_password),
            )
            .field(
                "threads_access_token",
                &patch_marker(&self.threads_access_token),
            )
            .field("threads_user_id", &self.threads_user_id)
            .field("default_workspace_id", &self.default_workspace_id)
            .field("inbox_polling_enabled", &self.inbox_polling_enabled)
            .field("inbox_poll_interval_secs", &self.inbox_poll_interval_secs)
            .field(
                "engagement_refresh_interval_secs",
                &self.engagement_refresh_interval_secs,
            )
            .field("engagement_refresh_batch", &self.engagement_refresh_batch)
            .field("default_platforms", &self.default_platforms)
            .field("utc_offset_minutes", &self.utc_offset_minutes)
            .field("quiet_hours_enabled", &self.quiet_hours_enabled)
            .field("quiet_hours_start", &self.quiet_hours_start)
            .field("quiet_hours_end", &self.quiet_hours_end)
            .finish()
    }
}

/// Three-state marker for a patch field: absent, cleared, or a value we will not show.
fn patch_marker(value: &Option<String>) -> &'static str {
    match value.as_deref().map(str::trim) {
        None => "<absent>",
        Some("") => "<clear>",
        Some(_) => "<set>",
    }
}

// ── Persistence ────────────────────────────────────────────────────────────────

/// Read the node blob. Never fails on a corrupt/absent blob — it falls back to
/// [`NodeSettings::default`], for the same reason `get_settings` does: settings that
/// cannot be parsed must not take the process down with them.
pub async fn load(store: &SocialStore) -> anyhow::Result<NodeSettings> {
    let raw = store.get_settings_blob(NODE_SETTINGS_KEY).await?;
    Ok(raw
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default())
}

/// Write the node blob back.
pub async fn save(store: &SocialStore, settings: &NodeSettings) -> anyhow::Result<()> {
    let json = serde_json::to_string(settings)?;
    store.put_settings_blob(NODE_SETTINGS_KEY, &json).await
}

/// Read → patch → write, returning the REDACTED result. The only mutation entry
/// point, so there is exactly one place a secret can be written.
///
/// An all-absent patch is a legal no-op that still rewrites the row (and bumps
/// `settings.updated_at`). Checked and left as-is: nothing reads that column — it is
/// written by both settings writers and read by neither — so short-circuiting would
/// add a branch to save nothing. If a change-detection consumer ever starts reading
/// it, this is the line to revisit.
pub async fn patch(
    store: &SocialStore,
    patch: NodeSettingsPatch,
) -> anyhow::Result<NodeSettingsView> {
    let mut settings = load(store).await?;
    settings.apply(patch);
    save(store, &settings).await?;
    Ok(settings.redacted())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn node_settings_round_trip_and_default_on_an_unwritten_blob() {
        let store = SocialStore::open_in_memory().unwrap();
        // Nothing written yet → defaults, not an error.
        let fresh = load(&store).await.unwrap();
        assert_eq!(fresh, NodeSettings::default());
        assert_eq!(fresh.inbox_poll_interval_secs, 600);

        let view = patch(
            &store,
            NodeSettingsPatch {
                composio_api_key: Some("  sk-live-123  ".into()),
                inbox_poll_interval_secs: Some(5),
                default_platforms: Some(vec![Platform::X, Platform::X, Platform::Bluesky]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Clamped, deduped, and the key is stored trimmed.
        assert_eq!(view.inbox_poll_interval_secs, 60);
        assert_eq!(view.default_platforms, vec![Platform::X, Platform::Bluesky]);
        assert!(view.composio_api_key_set);
        assert_eq!(view.composio_api_key_source, CredentialSource::Stored);
        let stored = load(&store).await.unwrap();
        assert_eq!(stored.composio_api_key.as_deref(), Some("sk-live-123"));
    }

    #[tokio::test]
    async fn a_partial_patch_leaves_every_other_field_alone() {
        let store = SocialStore::open_in_memory().unwrap();
        patch(
            &store,
            NodeSettingsPatch {
                composio_api_key: Some("key".into()),
                bluesky_handle: Some("@me.bsky.social".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // A later patch that mentions neither must not clear them.
        let view = patch(
            &store,
            NodeSettingsPatch {
                utc_offset_minutes: Some(-480),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(view.composio_api_key_set);
        assert_eq!(view.bluesky_handle.as_deref(), Some("@me.bsky.social"));
        assert_eq!(view.utc_offset_minutes, -480);

        // An explicit empty string CLEARS.
        let cleared = patch(
            &store,
            NodeSettingsPatch {
                composio_api_key: Some(String::new()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(!cleared.composio_api_key_set);
        assert_eq!(cleared.composio_api_key_source, CredentialSource::Unset);
    }

    #[test]
    fn the_redacted_view_and_the_debug_impl_carry_no_secret() {
        let settings = NodeSettings {
            composio_api_key: Some("sk-super-secret".into()),
            bluesky_app_password: Some("abcd-efgh-ijkl-mnop".into()),
            bluesky_handle: Some("me.bsky.social".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&settings.redacted()).unwrap();
        assert!(!json.contains("sk-super-secret"), "{json}");
        assert!(!json.contains("abcd-efgh"), "{json}");
        // The public handle IS returned — it is an identifier, not a credential.
        assert!(json.contains("me.bsky.social"));
        assert!(json.contains("\"composio_api_key_set\":true"));

        let debugged = format!("{settings:?}");
        assert!(!debugged.contains("sk-super-secret"), "{debugged}");
        assert!(!debugged.contains("abcd-efgh"), "{debugged}");
    }

    #[test]
    fn bluesky_credentials_need_both_halves() {
        let mut settings = NodeSettings {
            bluesky_handle: Some("@me.bsky.social".into()),
            ..Default::default()
        };
        assert!(settings.bluesky_credentials().is_none());
        settings.bluesky_app_password = Some("pw".into());
        let (handle, password) = settings.bluesky_credentials().unwrap();
        // The `@` a user types is stripped — the PDS rejects it.
        assert_eq!(handle, "me.bsky.social");
        assert_eq!(password, "pw");
    }

    #[test]
    fn quiet_hours_wrap_across_midnight_and_an_empty_window_never_matches() {
        let night = QuietHours {
            enabled: true,
            start_hour: 22,
            end_hour: 7,
        };
        assert!(night.contains_hour(23));
        assert!(night.contains_hour(0));
        assert!(night.contains_hour(6));
        assert!(!night.contains_hour(7));
        assert!(!night.contains_hour(12));

        let daytime = QuietHours {
            enabled: true,
            start_hour: 9,
            end_hour: 17,
        };
        assert!(daytime.contains_hour(9));
        assert!(!daytime.contains_hour(17));
        assert!(!daytime.contains_hour(3));

        // Disabled, and start == end, are both "never" — a misconfiguration must not
        // silently pause the node for 24 hours.
        assert!(!QuietHours {
            enabled: false,
            ..night
        }
        .contains_hour(23));
        assert!(!QuietHours {
            enabled: true,
            start_hour: 5,
            end_hour: 5,
        }
        .contains_hour(5));
    }

    #[test]
    fn local_hour_applies_the_offset() {
        // 1970-01-01T12:00:00Z
        let noon_utc = 12 * 3_600 * 1_000;
        assert_eq!(local_hour(noon_utc, 0), 12);
        assert_eq!(local_hour(noon_utc, -480), 4);
        assert_eq!(local_hour(noon_utc, 60), 13);
    }
}
