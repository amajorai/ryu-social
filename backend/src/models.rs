//! Domain types for the Outpost scheduling spine — the wire contract the sidecar
//! serves, the store persists, and the companion UI renders from.
//!
//! Conventions, applied uniformly and deliberately:
//!
//! - **Ids are TEXT with a typed prefix** (`ws_`, `acc_`, `dft_`, `sp_`, `tgt_`,
//!   `hist_`, `tpl_`, `med_`, `inb_`, `act_`) wrapping a UUIDv4. The prefix is not
//!   decoration: `post_history.post_target_id` and `post_targets.social_account_id`
//!   are cross-table references with no FK constraint behind them (see
//!   [`crate::store`] for why), so a mis-wired id is otherwise invisible until it
//!   silently matches nothing.
//! - **Timestamps are `i64` epoch MILLIS**, never RFC-3339 strings. The sibling
//!   apps (`ryu-teams`, `ryu-meetings`) store `TEXT` timestamps; this app does not,
//!   because its two hot predicates are range scans (`scheduled_for <= now`,
//!   `published_at BETWEEN from AND to`) and lexicographic string comparison is the
//!   wrong tool for a calendar.
//! - **Booleans are `bool` on the wire, `INTEGER` 0/1 in SQLite** (SQLite has no
//!   bool type).
//! - **Every field name is snake_case on the wire.** The upstream Outpost stores
//!   its `drafts.body` / `templates.body` JSON in camelCase (`schemaVersion`,
//!   `accountIds`, `mimeType`); this is a fresh database with nothing to migrate,
//!   so those blobs are snake_case here too and match every other shape the sidecar
//!   serves. The *tolerant decode* behaviour is kept verbatim — see [`DraftBody`].
//!
//! Enum columns carry no SQL `CHECK` constraint. The Rust enum plus its `FromStr`
//! IS the guard: a value that fails to parse degrades to a documented default
//! rather than failing a whole list query, which is what keeps one corrupt row from
//! blanking the calendar.

use serde::{Deserialize, Serialize};

// ── Time + id helpers ──────────────────────────────────────────────────────────

/// Now, as epoch millis. Every `created_at` / `updated_at` / `scheduled_for` in
/// this app is produced here so there is exactly one clock read to stub in tests.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A fresh prefixed id. See the module docs for why the prefix exists.
pub fn new_id(prefix: &str) -> String {
    format!("{prefix}{}", uuid::Uuid::new_v4().simple())
}

pub const ID_WORKSPACE: &str = "ws_";
pub const ID_ACCOUNT: &str = "acc_";
pub const ID_DRAFT: &str = "dft_";
pub const ID_POST: &str = "sp_";
pub const ID_TARGET: &str = "tgt_";
pub const ID_HISTORY: &str = "hist_";
pub const ID_TEMPLATE: &str = "tpl_";
pub const ID_MEDIA: &str = "med_";
pub const ID_INBOX: &str = "inb_";
pub const ID_ACTIVITY: &str = "act_";

/// The workspace every install starts with. Seeded `INSERT OR IGNORE` at migration
/// time so a first-run client always has somewhere to write, and so
/// `?workspace_id=` can be defaulted rather than required on every route.
pub const DEFAULT_WORKSPACE_ID: &str = "default";
pub const DEFAULT_WORKSPACE_NAME: &str = "Default";

// ── Platform ───────────────────────────────────────────────────────────────────

/// The nine platforms the spine knows about.
///
/// Declaration order is load-bearing: it is the order the UI renders account
/// pickers and the per-platform limit table in, so reordering these variants
/// reorders the product.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    X,
    Instagram,
    Tiktok,
    Youtube,
    Linkedin,
    Reddit,
    Facebook,
    Bluesky,
    Threads,
}

impl Platform {
    pub const ALL: [Platform; 9] = [
        Platform::X,
        Platform::Instagram,
        Platform::Tiktok,
        Platform::Youtube,
        Platform::Linkedin,
        Platform::Reddit,
        Platform::Facebook,
        Platform::Bluesky,
        Platform::Threads,
    ];

    /// The wire/SQL value. Must stay byte-identical to the serde `rename_all`
    /// output above — it is the same string in both places.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::X => "x",
            Self::Instagram => "instagram",
            Self::Tiktok => "tiktok",
            Self::Youtube => "youtube",
            Self::Linkedin => "linkedin",
            Self::Reddit => "reddit",
            Self::Facebook => "facebook",
            Self::Bluesky => "bluesky",
            Self::Threads => "threads",
        }
    }

    /// The human label the UI shows. Diverges from [`Self::as_str`] for exactly two
    /// platforms (`x` → "X", `tiktok` → "TikTok"), which is why it is a table and
    /// not a capitalize().
    pub const fn label(self) -> &'static str {
        match self {
            Self::X => "X",
            Self::Instagram => "Instagram",
            Self::Tiktok => "TikTok",
            Self::Youtube => "YouTube",
            Self::Linkedin => "LinkedIn",
            Self::Reddit => "Reddit",
            Self::Facebook => "Facebook",
            Self::Bluesky => "Bluesky",
            Self::Threads => "Threads",
        }
    }

    /// The Composio toolkit slug for this platform. Note `x` → `twitter`: the
    /// broker never renamed its toolkit, so the mapping is not the identity.
    pub const fn composio_toolkit(self) -> &'static str {
        match self {
            Self::X => "twitter",
            other => other.as_str(),
        }
    }

    /// Strict parse. Returns `None` for anything unrecognized — callers that must
    /// not fail (limit lookups, list decoding) use the tolerant helpers below
    /// instead of unwrapping this.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.as_str() == s)
    }
}

impl std::str::FromStr for Platform {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s).ok_or_else(|| format!("unknown platform \"{s}\""))
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Per-platform limits ────────────────────────────────────────────────────────

/// How a multi-segment post degrades on a given platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentStyle {
    /// Each segment is a reply chained to the previous one (X, Bluesky, Threads).
    Thread,
    /// Segments are slides inside ONE post (Instagram, LinkedIn).
    Carousel,
    /// Single post: extra segments are dropped. This is a documented DEGRADE, not
    /// a validation error — see [`validate_segments_for_platform`].
    None,
}

impl SegmentStyle {
    /// The noun the over-limit message uses ("Allows at most 25 posts" vs
    /// "…10 slides"). `None` never reaches a segment-count check, so its value here
    /// is only a safe filler.
    pub const fn noun(self) -> &'static str {
        match self {
            Self::Thread => "posts",
            Self::Carousel => "slides",
            Self::None => "posts",
        }
    }

    /// The per-segment prefix on a validation failure ("Post 2: …" / "Slide 2: …").
    pub const fn item_noun(self) -> &'static str {
        match self {
            Self::Carousel => "Slide",
            _ => "Post",
        }
    }
}

/// The conservative, client-side guard-rail figures for one platform.
///
/// These are PUBLIC published limits, not a contractual guarantee from any
/// platform — they exist so the composer can warn before a publish fails, and the
/// provider is still the authority. Keeping them `const` (hence
/// `&'static [&'static str]` for the mime prefixes) means the table is compiled in
/// with no allocation and no startup parse.
/// Serialize-only: `&'static [&'static str]` cannot be deserialized, and nothing
/// should be reading a limits table off the wire anyway — this table IS the
/// authority, so accepting a caller-supplied one would be a way to talk the
/// composer out of its own guard rails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PlatformLimits {
    pub max_chars: usize,
    pub allowed_mime_prefixes: &'static [&'static str],
    pub max_media: usize,
    pub segment_style: SegmentStyle,
    pub max_segments: usize,
}

const IMAGE_AND_VIDEO: &[&str] = &["image/", "video/"];
const VIDEO_ONLY: &[&str] = &["video/"];
/// Bluesky is the one image-only platform: the AT-Protocol embed this app writes is
/// `app.bsky.embed.images`, and video would be silently dropped at upload.
const IMAGE_ONLY: &[&str] = &["image/"];

/// What an unrecognized platform key gets. The lookup NEVER fails — a caller may
/// hand us a platform string from a stored row written by a newer version, and
/// blanking the composer would be worse than guard-railing it generously.
pub const DEFAULT_PLATFORM_LIMITS: PlatformLimits = PlatformLimits {
    max_chars: 5_000,
    allowed_mime_prefixes: IMAGE_AND_VIDEO,
    max_media: 10,
    segment_style: SegmentStyle::None,
    max_segments: 1,
};

/// The real per-platform numbers. Order matches [`Platform::ALL`].
pub const PLATFORM_LIMITS: [(Platform, PlatformLimits); 9] = [
    (
        Platform::X,
        PlatformLimits {
            max_chars: 280,
            allowed_mime_prefixes: IMAGE_AND_VIDEO,
            max_media: 4,
            segment_style: SegmentStyle::Thread,
            max_segments: 25,
        },
    ),
    (
        Platform::Instagram,
        PlatformLimits {
            max_chars: 2_200,
            allowed_mime_prefixes: IMAGE_AND_VIDEO,
            max_media: 10,
            segment_style: SegmentStyle::Carousel,
            max_segments: 10,
        },
    ),
    (
        Platform::Tiktok,
        PlatformLimits {
            max_chars: 2_200,
            allowed_mime_prefixes: VIDEO_ONLY,
            max_media: 1,
            segment_style: SegmentStyle::None,
            max_segments: 1,
        },
    ),
    (
        Platform::Youtube,
        PlatformLimits {
            max_chars: 5_000,
            allowed_mime_prefixes: VIDEO_ONLY,
            max_media: 1,
            segment_style: SegmentStyle::None,
            max_segments: 1,
        },
    ),
    (
        Platform::Linkedin,
        PlatformLimits {
            max_chars: 3_000,
            allowed_mime_prefixes: IMAGE_AND_VIDEO,
            max_media: 9,
            segment_style: SegmentStyle::Carousel,
            max_segments: 20,
        },
    ),
    (
        Platform::Reddit,
        PlatformLimits {
            max_chars: 40_000,
            allowed_mime_prefixes: IMAGE_AND_VIDEO,
            max_media: 1,
            segment_style: SegmentStyle::None,
            max_segments: 1,
        },
    ),
    (
        Platform::Facebook,
        PlatformLimits {
            max_chars: 63_206,
            allowed_mime_prefixes: IMAGE_AND_VIDEO,
            max_media: 10,
            segment_style: SegmentStyle::None,
            max_segments: 1,
        },
    ),
    (
        Platform::Bluesky,
        PlatformLimits {
            max_chars: 300,
            allowed_mime_prefixes: IMAGE_ONLY,
            max_media: 4,
            segment_style: SegmentStyle::Thread,
            max_segments: 25,
        },
    ),
    (
        Platform::Threads,
        PlatformLimits {
            max_chars: 500,
            allowed_mime_prefixes: IMAGE_AND_VIDEO,
            max_media: 10,
            segment_style: SegmentStyle::Thread,
            max_segments: 25,
        },
    ),
];

/// Limits for a known platform.
pub fn limits_for(platform: Platform) -> PlatformLimits {
    PLATFORM_LIMITS
        .iter()
        .find(|(key, _)| *key == platform)
        .map_or(DEFAULT_PLATFORM_LIMITS, |(_, limits)| *limits)
}

/// Limits for a caller-supplied platform STRING. Never fails — `POST /posts/validate`
/// takes whatever the composer sends, and an unknown key must guard-rail against the
/// generous default rather than 400 the whole compose payload.
pub fn limits_for_str(platform: &str) -> PlatformLimits {
    Platform::parse(platform).map_or(DEFAULT_PLATFORM_LIMITS, limits_for)
}

/// Label for a caller-supplied platform string, falling back to the raw key so an
/// unknown platform still renders as *something* instead of an empty chip.
pub fn label_for_str(platform: &str) -> String {
    Platform::parse(platform).map_or_else(|| platform.to_string(), |p| p.label().to_string())
}

/// Map a file extension to a mime type, for media picked by path with no type
/// attached. An unknown extension (or no dot) yields `""`, which then fails the
/// allowed-prefix check with the honest "unknown type" message.
pub fn mime_for_extension(name: &str) -> &'static str {
    let ext = name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("avif") => "image/avif",
        Some("heic") => "image/heic",
        Some("mp4") => "video/mp4",
        Some("mov") => "video/quicktime",
        Some("webm") => "video/webm",
        Some("m4v") => "video/x-m4v",
        _ => "",
    }
}

// ── Capabilities ───────────────────────────────────────────────────────────────

/// What a provider can actually do for one platform.
///
/// `schedule` is ALWAYS false for every provider and is not a bug: Outpost owns
/// scheduling locally and never delegates it to a broker. The field exists so
/// callers have exactly one place to ask "is this action available", instead of
/// some actions being a capability lookup and one being a special case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlatformCapabilities {
    pub publish: bool,
    pub read_comments: bool,
    pub read_dms: bool,
    pub send_dm: bool,
    pub read_engagement: bool,
    pub schedule: bool,
}

impl PlatformCapabilities {
    /// All-false. The deliberate degrade for "provider errored" and for "this
    /// provider does not serve this platform" — never an `Err`, so one unreachable
    /// platform cannot blank the whole matrix.
    pub const fn empty() -> Self {
        Self {
            publish: false,
            read_comments: false,
            read_dms: false,
            send_dm: false,
            read_engagement: false,
            schedule: false,
        }
    }
}

// ── Media ──────────────────────────────────────────────────────────────────────

/// A file attached to a post. The path is a LOCAL ABSOLUTE PATH; the bytes are never
/// copied, so the file must stay where it is until the post has gone out.
//
// `//` for the rest, because this is a request-body schema and the note below is for
// whoever writes a provider, not for whoever fills in the field: a hosted broker
// cannot fetch a local path, so any provider not running on this machine needs an
// upload leg.
// `ToSchema` here, and on the three types below, because each is reachable from a
// documented REQUEST body in `api`. utoipa needs the whole transitive graph derived
// or the doc does not compile; nothing outside that graph gets a derive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct MediaRef {
    /// Local absolute path to the file.
    pub path: String,
    /// e.g. `image/png`. Empty when unknown.
    #[serde(default)]
    pub mime_type: String,
    /// Display name, usually the file name.
    #[serde(default)]
    pub name: String,
}

/// A row in the workspace media library. Distinct from [`MediaRef`] (which is an
/// attachment inside a draft body): this is the addressable library entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaAsset {
    pub id: String,
    pub workspace_id: String,
    pub kind: MediaKind,
    pub path: String,
    pub name: String,
    pub mime_type: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MediaKind {
    Image,
    Video,
}

impl MediaKind {
    /// `video/*` is video; EVERYTHING else — including a missing mime type — is
    /// image. Asymmetric on purpose: an unknown attachment renders as a thumbnail
    /// rather than as a broken video player.
    pub fn from_mime(mime: Option<&str>) -> Self {
        match mime {
            Some(m) if m.starts_with("video/") => Self::Video,
            _ => Self::Image,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
        }
    }
}

// ── Workspace ──────────────────────────────────────────────────────────────────

/// The tenant boundary. Every domain table carries `workspace_id`; the two child
/// tables (`post_targets`, `post_history`) reach it through their parent instead of
/// duplicating it, so there is no way for a target to disagree with its post about
/// which workspace it belongs to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub created_at: i64,
}

// ── Social account ─────────────────────────────────────────────────────────────

/// A connected (or connectable) account on one platform.
///
/// Multiple accounts per platform per workspace are allowed — there is deliberately
/// no UNIQUE index — because posting the same content from a brand account and a
/// founder account is the normal case, not an error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocialAccount {
    pub id: String,
    pub workspace_id: String,
    pub platform: Platform,
    /// The human `@handle` shown in pickers.
    pub account_label: String,
    /// The remote platform's own id for this account, once known. `None` until the
    /// connect flow completes.
    pub external_id: Option<String>,
    pub connected: bool,
    pub created_at: i64,
}

// ── Draft ──────────────────────────────────────────────────────────────────────

/// One segment of a post: a unit of text plus its own attachments. On a `thread`
/// platform this becomes a chained reply; on a `carousel` platform, a slide.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PostSegment {
    /// This segment's text.
    #[serde(default)]
    pub text: String,
    /// Files attached to this segment.
    #[serde(default)]
    pub media: Vec<MediaRef>,
}

/// A post's content: its text, its attachments, and its thread structure.
//
// Everything below this line is `//` rather than `///` ON PURPOSE. This struct is a
// documented request-body schema, so a `///` here is shipped to a model as the
// argument's description — and none of the following is something a caller deciding
// what to send needs to read.
//
// **Segments are stored as JSON, not as a `post_segments` table.** A table would
// need snapshot-at-schedule-time semantics (does editing a draft change an already
// scheduled post's thread?) that nothing in the spine defines; keeping the thread
// structure inside the draft body means the resolution happens once, at publish
// time, from a single source.
//
// **The mirror invariant:** `text` and `media` always equal `segments[0]`'s. They
// exist so a consumer that does not care about threads (a preview card, a provider
// that ignores segments) can read one field instead of reaching into an array.
// `normalize` re-establishes the invariant on every decode AND every encode, so it
// cannot drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DraftBody {
    /// Leave this out; it defaults to the current version.
    // A body with NO version is a v1 body and is migrated on read; see `decode`.
    #[serde(default = "DraftBody::current_schema_version")]
    pub schema_version: u32,
    /// The post's main text. Mirrors `segments[0].text`, so setting one is enough.
    #[serde(default)]
    pub text: String,
    /// Files attached to the first segment.
    #[serde(default)]
    pub media: Vec<MediaRef>,
    /// The accounts this draft is aimed at. Advisory — the authoritative fan-out is
    /// the target list given when the post is scheduled.
    #[serde(default)]
    pub account_ids: Vec<String>,
    /// The thread: one entry per chained reply (or carousel slide). One segment is
    /// an ordinary single post.
    #[serde(default)]
    pub segments: Vec<PostSegment>,
}

impl Default for DraftBody {
    fn default() -> Self {
        Self::empty()
    }
}

impl DraftBody {
    pub const SCHEMA_VERSION: u32 = 2;

    const fn current_schema_version() -> u32 {
        Self::SCHEMA_VERSION
    }

    /// A body with exactly one empty segment. This is what an unparseable blob
    /// decodes to — never an error.
    pub fn empty() -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            text: String::new(),
            media: Vec::new(),
            account_ids: Vec::new(),
            segments: vec![PostSegment::default()],
        }
    }

    /// Decode a stored blob. **Infallible by contract.**
    ///
    /// A draft is user content that has already been persisted; refusing to read it
    /// back makes the row permanently unopenable and loses the user's work. Every
    /// failure mode therefore degrades:
    ///
    /// - unparseable JSON, or valid JSON that is not an object → [`Self::empty`]
    /// - `schema_version` absent → treat as v1 and synthesize `segments` from the
    ///   top-level `text`/`media`
    /// - `segments` present but empty → same synthesis
    pub fn decode(raw: &str) -> Self {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
            return Self::empty();
        };
        if !value.is_object() {
            return Self::empty();
        }
        // The `#[serde(default)]` on every field means a v1 body (no
        // `schema_version`, no `segments`) deserializes cleanly with an empty
        // segment list — which `normalize` then fills from `text`/`media`. That is
        // exactly the v1 → v2 migration, so it needs no separate branch.
        let mut body: Self = serde_json::from_value(value).unwrap_or_else(|_| Self::empty());
        body.normalize();
        body
    }

    /// Encode for storage, re-establishing the mirror invariant first so what is
    /// written back is always self-consistent.
    pub fn encode(&self) -> String {
        let mut body = self.clone();
        body.normalize();
        serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string())
    }

    /// Force `segments.len() >= 1` and mirror `segments[0]` into `text`/`media`.
    pub fn normalize(&mut self) {
        self.schema_version = Self::SCHEMA_VERSION;
        if self.segments.is_empty() {
            self.segments = vec![PostSegment {
                text: std::mem::take(&mut self.text),
                media: std::mem::take(&mut self.media),
            }];
        }
        let first = &self.segments[0];
        self.text = first.text.clone();
        self.media = first.media.clone();
    }

    /// True when there is nothing worth publishing. Checked at publish time: a body
    /// with neither text nor media resolves to "no content" and the target fails
    /// without ever contacting a provider.
    pub fn is_empty(&self) -> bool {
        self.segments
            .iter()
            .all(|s| s.text.trim().is_empty() && s.media.is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Draft {
    pub id: String,
    pub workspace_id: String,
    pub body: DraftBody,
    pub created_at: i64,
    pub updated_at: i64,
}

// ── Scheduled post + targets ───────────────────────────────────────────────────

/// The lifecycle of a scheduled post.
///
/// `due` is the handoff state between the scheduler sweep and the publish runner,
/// and the `scheduled → due` flip IS the idempotency guard: once flipped, the
/// sweep's own predicate can no longer select the row, so a post becomes due
/// exactly once no matter how many sweeps overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PostStatus {
    /// Queued; eligible for the sweep.
    Scheduled,
    /// Time elapsed and the sweep claimed it. Waiting for the runner.
    Due,
    /// The runner claimed it and is working through its targets.
    Publishing,
    /// EVERY target published.
    Published,
    /// Some targets published, some failed.
    Partial,
    /// Zero targets published — including the degenerate "post has no targets" case.
    Failed,
    /// The user cancelled before publishing started.
    Cancelled,
}

impl PostStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Due => "due",
            Self::Publishing => "publishing",
            Self::Published => "published",
            Self::Partial => "partial",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Tolerant parse for a value read back out of SQLite. An unrecognized string
    /// degrades to `Failed` rather than poisoning a list query: a row we cannot
    /// interpret is definitionally not going to publish, and showing it as failed is
    /// both true and visible.
    pub fn from_db(s: &str) -> Self {
        match s {
            "scheduled" => Self::Scheduled,
            "due" => Self::Due,
            "publishing" => Self::Publishing,
            "published" => Self::Published,
            "partial" => Self::Partial,
            "cancelled" => Self::Cancelled,
            _ => Self::Failed,
        }
    }

    /// Nothing transitions out of these.
    ///
    /// Note `Partial` is terminal and has NO automatic retry path — the failed
    /// targets are not re-attempted by the runner. `POST /posts/:id/retry` is the
    /// deliberate, user-initiated way out, and it is the reason that route exists.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Published | Self::Partial | Self::Failed | Self::Cancelled
        )
    }

    /// Whether the runner still owes this post work.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Scheduled | Self::Due | Self::Publishing)
    }

    /// The legal transition table.
    ///
    /// This is advisory documentation, NOT the enforcement point. Enforcement is a
    /// guarded compare-and-swap in SQL (`… WHERE id = ?1 AND status = ?2`), because
    /// this sidecar has concurrent request handlers plus a tick task against one
    /// database and a read-then-blind-UPDATE has no claim semantics. Use this to
    /// reason; use the store's CAS helpers to act.
    pub fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            // The sweep claims a due post.
            (Self::Scheduled, Self::Due) => true,
            // Reschedule is a guarded self-loop: only while still `scheduled`, so a
            // post the sweep already claimed cannot be silently moved out from
            // under the runner.
            (Self::Scheduled, Self::Scheduled) => true,
            // The runner claims it.
            (Self::Due, Self::Publishing) => true,
            // The crash reaper returns an expired lease to the queue. Without this
            // edge, a process death mid-publish orphans the row forever.
            (Self::Publishing, Self::Due) => true,
            // Settlement.
            (Self::Publishing, Self::Published | Self::Partial | Self::Failed) => true,
            // A user-initiated retry of a settled-but-incomplete post.
            (Self::Partial | Self::Failed, Self::Due) => true,
            // Cancellation, only before any provider was contacted.
            (Self::Scheduled | Self::Due, Self::Cancelled) => true,
            _ => false,
        }
    }
}

/// The lifecycle of ONE fan-out leg (one post → one account).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetStatus {
    Pending,
    Publishing,
    Published,
    Failed,
    Cancelled,
}

impl TargetStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Publishing => "publishing",
            Self::Published => "published",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Tolerant parse; see [`PostStatus::from_db`] for the rationale.
    pub fn from_db(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "publishing" => Self::Publishing,
            "published" => Self::Published,
            "cancelled" => Self::Cancelled,
            _ => Self::Failed,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Published | Self::Failed | Self::Cancelled)
    }
}

/// A post scheduled for a moment in time. "Post now" is not a separate concept —
/// it is a schedule whose `scheduled_for` is now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledPost {
    pub id: String,
    pub workspace_id: String,
    /// `None` for an ad-hoc post composed inline with no saved draft behind it.
    pub draft_id: Option<String>,
    pub scheduled_for: i64,
    pub status: PostStatus,
    pub created_at: i64,
    /// The fan-out legs. Populated on the single-post read and on the list read,
    /// because every surface that shows a post shows which accounts it goes to —
    /// making it optional would just guarantee an N+1 on every list.
    #[serde(default)]
    pub targets: Vec<PostTarget>,
}

/// One leg of the fan-out: this post, on this account.
///
/// This row is what carries retry state, so the retry policy is durable across a
/// process restart instead of living in the runner's stack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostTarget {
    pub id: String,
    pub scheduled_post_id: String,
    pub social_account_id: String,
    /// A denormalized copy of the account's platform. Deliberate: an account row can
    /// be hard-deleted while its targets survive in history, and the platform is
    /// needed to render and to route the retry.
    pub platform: Platform,
    /// A per-target override of the draft body.
    ///
    /// Stored as a full [`DraftBody`] JSON blob, NOT plain text. Upstream Outpost
    /// stores plain text here, which means a per-target tweak ("shorten this for
    /// LinkedIn") silently drops that target's media AND its thread structure. A
    /// full body keeps the override lossless.
    pub variant_body: Option<DraftBody>,
    pub status: TargetStatus,
    /// Provider calls made so far, across the whole lifetime of this target. `0`
    /// with a `failed` status means the failure was local (no body to publish) and
    /// no provider was ever contacted — a genuinely useful distinction when
    /// debugging a failed post.
    pub attempts: u32,
    /// When the runner may next try. Written when a backoff is scheduled; this is
    /// what `GET /queue` projects.
    pub next_attempt_at: Option<i64>,
    /// The lease stamp. Set when a runner claims this target, cleared on settle. A
    /// reaper returns targets whose lease expired — the ONLY exit from `publishing`
    /// after a process death.
    pub claimed_at: Option<i64>,
}

// ── Publish history ────────────────────────────────────────────────────────────

/// The terminal record of one publish RUN for one target — written once, after
/// retries are exhausted, not once per attempt. `attempts` on the target carries
/// the per-attempt count; this table carries the outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostHistoryEntry {
    pub id: String,
    pub post_target_id: String,
    pub status: HistoryStatus,
    pub remote_url: Option<String>,
    pub remote_id: Option<String>,
    pub error: Option<String>,
    /// Set even on the failed path — a failed publish still happened at a time, and
    /// a history list with null timestamps sorts arbitrarily.
    pub published_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HistoryStatus {
    Published,
    Failed,
}

impl HistoryStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }

    pub fn from_db(s: &str) -> Self {
        if s == "published" {
            Self::Published
        } else {
            Self::Failed
        }
    }
}

// ── Engagement + activity ──────────────────────────────────────────────────────

/// Metrics read back from a platform. Every count is optional because platforms
/// genuinely differ in what they expose — AT-Protocol has no view count at all —
/// and a `0` for "not reported" would be a lie the analytics layer then averages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngagementCounts {
    pub likes: Option<u64>,
    pub comments: Option<u64>,
    pub shares: Option<u64>,
    pub views: Option<u64>,
    /// Required: a metric with no read time cannot be aged out or compared.
    pub fetched_at: i64,
}

/// A published post plus its LATEST engagement snapshot.
///
/// This is a latest-snapshot table, not a time series: there is one row per remote
/// post and its counts are overwritten on each refresh. That is a real constraint on
/// what analytics can honestly claim — "best posting time" here means "when the
/// content that performed was published", never a learned engagement curve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityItem {
    pub id: String,
    pub workspace_id: String,
    pub social_account_id: String,
    pub platform: Platform,
    /// The remote platform's post id. Part of the dedupe key.
    pub post_remote_id: String,
    pub permalink: Option<String>,
    pub text: Option<String>,
    pub likes: u64,
    pub comments: u64,
    pub shares: u64,
    pub views: u64,
    pub engagement_fetched_at: Option<i64>,
    pub published_at: Option<i64>,
}

// ── Inbox ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InboxKind {
    Comment,
    Reply,
    Mention,
    Dm,
}

impl InboxKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Comment => "comment",
            Self::Reply => "reply",
            Self::Mention => "mention",
            Self::Dm => "dm",
        }
    }

    /// Tolerant parse; unknown kinds read back as `comment`, the least-privileged
    /// interpretation (a public reply box, not a DM composer).
    pub fn from_db(s: &str) -> Self {
        match s {
            "reply" => Self::Reply,
            "mention" => Self::Mention,
            "dm" => Self::Dm,
            _ => Self::Comment,
        }
    }
}

/// One piece of inbound engagement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboxItem {
    pub id: String,
    pub workspace_id: String,
    pub social_account_id: String,
    pub platform: Platform,
    pub kind: InboxKind,
    pub author: String,
    pub text: String,
    pub permalink: Option<String>,
    /// The remote item's own id — the dedupe key, so re-polling a platform does not
    /// duplicate the inbox.
    pub external_id: String,
    /// The REMOTE creation time, not when we fetched it. Sorting by fetch time would
    /// scramble the conversation on the first backfill.
    pub received_at: i64,
    pub replied: bool,
    /// Read state. Not in the upstream schema; added because an inbox without one
    /// has no way to shrink.
    pub read: bool,
}

// ── Templates ──────────────────────────────────────────────────────────────────

/// The JSON blob stored in `templates.body`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TemplateBody {
    /// Leave this out; it defaults to the current version.
    #[serde(default = "TemplateBody::current_schema_version")]
    pub schema_version: u32,
    /// The template's starting text.
    #[serde(default)]
    pub text: String,
    /// Starting text for specific platforms, keyed by platform (`x`, `bluesky`, …).
    // Omitted entirely when empty rather than serialized as `{}`, so a template that
    // has never had a per-platform default is byte-distinguishable from one whose
    // defaults were cleared.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub platform_defaults: std::collections::BTreeMap<String, String>,
}

impl TemplateBody {
    pub const SCHEMA_VERSION: u32 = 1;

    const fn current_schema_version() -> u32 {
        Self::SCHEMA_VERSION
    }

    /// Decode a stored blob. **Infallible**, with one deliberate legacy tolerance: a
    /// body that is not parseable JSON, or that parses to a non-object (a bare
    /// string, number, or array), is taken as the template's TEXT verbatim. Templates
    /// predate the JSON body, and reading an old plain-text template back as an
    /// empty template would look like data loss.
    pub fn decode(raw: &str) -> Self {
        match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(v) if v.is_object() => {
                let mut body: Self = serde_json::from_value(v).unwrap_or_default();
                body.normalize();
                body
            }
            _ => Self {
                schema_version: Self::SCHEMA_VERSION,
                text: raw.to_string(),
                platform_defaults: Default::default(),
            },
        }
    }

    pub fn encode(&self) -> String {
        let mut body = self.clone();
        body.normalize();
        serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string())
    }

    /// Drop blank per-platform defaults. An entry whose value is whitespace is
    /// indistinguishable from absence to the user, so persisting it would make the
    /// "has a LinkedIn default" badge lie.
    pub fn normalize(&mut self) {
        self.schema_version = Self::SCHEMA_VERSION;
        self.platform_defaults.retain(|_, v| !v.trim().is_empty());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Template {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub body: TemplateBody,
    pub created_at: i64,
}

// ── Settings ───────────────────────────────────────────────────────────────────

/// Per-workspace app settings, stored as one JSON blob.
///
/// A blob rather than columns because these are read as a unit by the settings tab
/// and never queried on — and because a later agent adding a knob should not need a
/// schema migration to do it.
///
/// ## Every knob here is live — keep it that way
///
/// A settings control that silently does nothing is worse than one that is absent, so
/// each field names its reader. If you add a knob, wire it in the same change:
///
/// - `scheduler_enabled` → [`crate::scheduler`] filters the sweep to workspaces that
///   have it on. The sweep still runs and claims nothing, so turning it back on does
///   not stampede a backlog.
/// - `poll_interval_secs` → [`crate::scheduler`]. Per-WORKSPACE while the tick is
///   per-process, so the loop takes the SHORTEST across workspaces rather than reading
///   one directly.
/// - `claim_lease_secs` → [`crate::scheduler`], and it is the same number the
///   [`crate::store`] reaper cuts off at. The two disagreeing is how a healthy publish
///   gets double-claimed, so they read one value.
/// - `max_attempts` / `base_backoff_ms` → [`crate::publish`]'s retry loop and
///   [`crate::publish::backoff_delay_ms`]; `max_attempts` also feeds
///   [`crate::queue`]'s `attempts_remaining`.
/// - `enforce_platform_limits` → `POST /posts/validate` ONLY, where it decides whether
///   an over-limit result blocks the schedule or is shown as an overrulable warning.
///   [`crate::publish`] re-checks unconditionally at publish time and deliberately
///   ignores this — see the comment there before "fixing" that asymmetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocialSettings {
    /// Master switch for the tick loop. When false the sweep still runs but claims
    /// nothing, so turning it back on does not stampede a backlog through an
    /// un-warmed provider.
    pub scheduler_enabled: bool,
    /// How often the sweep runs.
    pub poll_interval_secs: u64,
    /// Provider calls per target before it is failed. 3 = one initial + two retries.
    pub max_attempts: u32,
    /// First backoff, doubled per attempt: 1s then 2s at the default.
    pub base_backoff_ms: u64,
    /// How long a runner's claim on a target is honoured before the reaper takes it
    /// back. Must exceed the worst-case publish (`max_attempts` calls plus their
    /// backoff sleeps) or a slow-but-healthy publish gets double-claimed.
    pub claim_lease_secs: u64,
    /// IANA zone the calendar and "best time" projections render in. The store is
    /// always UTC millis; this only affects presentation.
    pub timezone: String,
    /// Whether the composer blocks a schedule that fails a per-platform limit, or
    /// merely warns. Warn-only exists because these limits are public figures, not a
    /// contract, and can be wrong.
    pub enforce_platform_limits: bool,
}

impl Default for SocialSettings {
    fn default() -> Self {
        Self {
            scheduler_enabled: true,
            poll_interval_secs: 30,
            max_attempts: 3,
            base_backoff_ms: 1_000,
            claim_lease_secs: 300,
            timezone: "UTC".to_string(),
            enforce_platform_limits: true,
        }
    }
}

// ── Compose validation ─────────────────────────────────────────────────────────

/// Check one segment's worth of content against a platform's limits.
///
/// Returns the FIRST blocking reason, or `None` when it passes. First-only (rather
/// than a list) because the composer shows one inline message per platform chip and
/// a user fixes one thing at a time.
///
/// Note the deliberate asymmetry inherited from the spec: emptiness is checked on
/// TRIMMED text, while the character limit is checked on UNTRIMMED text. That
/// matches what the platforms actually count — trailing whitespace is transmitted.
pub fn validate_for_platform(platform: &str, text: &str, media: &[MediaRef]) -> Option<String> {
    let limits = limits_for_str(platform);

    if text.trim().is_empty() && media.is_empty() {
        return Some("Post is empty".to_string());
    }

    // `chars().count()`, not `len()`: platform limits are character counts, and
    // byte length would reject a perfectly legal post full of emoji or CJK.
    let chars = text.chars().count();
    if chars > limits.max_chars {
        let over = chars - limits.max_chars;
        return Some(format!(
            "Over the {} character limit by {over}",
            limits.max_chars
        ));
    }

    if media.len() > limits.max_media {
        return Some(format!("Allows at most {} attachment(s)", limits.max_media));
    }

    for item in media {
        let mime = if item.mime_type.is_empty() {
            mime_for_extension(&item.name)
        } else {
            item.mime_type.as_str()
        };
        let allowed = limits
            .allowed_mime_prefixes
            .iter()
            .any(|prefix| mime.starts_with(prefix));
        if !allowed {
            let shown = if mime.is_empty() {
                "unknown type"
            } else {
                mime
            };
            let name = if item.name.is_empty() {
                item.path.as_str()
            } else {
                item.name.as_str()
            };
            return Some(format!("Does not accept \"{name}\" ({shown})"));
        }
    }

    None
}

/// Check a whole multi-segment post against a platform's limits.
///
/// On a `none`-style platform ONLY segment 0 is validated: the extra segments are a
/// documented degrade (they are dropped at publish), not a user error, so failing
/// the compose would block a legitimate cross-post.
pub fn validate_segments_for_platform(platform: &str, segments: &[PostSegment]) -> Option<String> {
    let limits = limits_for_str(platform);

    let Some(first) = segments.first() else {
        return Some("Post is empty".to_string());
    };

    if limits.segment_style == SegmentStyle::None {
        return validate_for_platform(platform, &first.text, &first.media);
    }

    if segments.len() > limits.max_segments {
        return Some(format!(
            "Allows at most {} {}",
            limits.max_segments,
            limits.segment_style.noun()
        ));
    }

    for (i, segment) in segments.iter().enumerate() {
        if let Some(reason) = validate_for_platform(platform, &segment.text, &segment.media) {
            return Some(format!(
                "{} {}: {reason}",
                limits.segment_style.item_noun(),
                i + 1
            ));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_round_trips_through_its_wire_value() {
        for p in Platform::ALL {
            assert_eq!(Platform::parse(p.as_str()), Some(p));
            // The serde value and `as_str` must agree — they are the same string in
            // SQLite and on the wire.
            let json = serde_json::to_string(&p).unwrap();
            assert_eq!(json, format!("\"{}\"", p.as_str()));
        }
        assert_eq!(Platform::parse("mastodon"), None);
    }

    #[test]
    fn limits_table_covers_every_platform_and_falls_back_for_unknowns() {
        assert_eq!(PLATFORM_LIMITS.len(), Platform::ALL.len());
        for p in Platform::ALL {
            // Every platform must have a real row, not the default.
            assert!(PLATFORM_LIMITS.iter().any(|(k, _)| *k == p), "{p} missing");
        }
        assert_eq!(limits_for(Platform::X).max_chars, 280);
        assert_eq!(
            limits_for(Platform::Bluesky).allowed_mime_prefixes,
            IMAGE_ONLY
        );
        assert_eq!(limits_for_str("nope"), DEFAULT_PLATFORM_LIMITS);
        assert_eq!(label_for_str("nope"), "nope");
        assert_eq!(label_for_str("x"), "X");
    }

    #[test]
    fn draft_body_decode_never_fails_and_holds_the_mirror_invariant() {
        // Garbage → one empty segment.
        assert_eq!(DraftBody::decode("not json"), DraftBody::empty());
        assert_eq!(DraftBody::decode("[1,2,3]"), DraftBody::empty());
        assert_eq!(DraftBody::decode("\"a string\""), DraftBody::empty());

        // A v1 body (no schema_version, no segments) is migrated.
        let v1 = DraftBody::decode(r#"{"text":"hi","media":[]}"#);
        assert_eq!(v1.schema_version, DraftBody::SCHEMA_VERSION);
        assert_eq!(v1.segments.len(), 1);
        assert_eq!(v1.segments[0].text, "hi");

        // The mirror is re-established from segments[0], overwriting a stale top
        // level rather than trusting it.
        let mirrored = DraftBody::decode(
            r#"{"schema_version":2,"text":"stale","segments":[{"text":"fresh","media":[]}]}"#,
        );
        assert_eq!(mirrored.text, "fresh");
    }

    #[test]
    fn template_body_decode_treats_legacy_plain_text_as_the_body() {
        let legacy = TemplateBody::decode("just some text");
        assert_eq!(legacy.text, "just some text");
        let modern = TemplateBody::decode(r#"{"schema_version":1,"text":"hi"}"#);
        assert_eq!(modern.text, "hi");
        // Blank per-platform defaults are dropped, not persisted.
        let mut body = TemplateBody {
            platform_defaults: [("x".into(), "  ".into()), ("linkedin".into(), "hi".into())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        body.normalize();
        assert_eq!(body.platform_defaults.len(), 1);
    }

    #[test]
    fn post_status_transitions_match_the_documented_machine() {
        assert!(PostStatus::Scheduled.can_transition_to(PostStatus::Due));
        assert!(PostStatus::Due.can_transition_to(PostStatus::Publishing));
        assert!(PostStatus::Publishing.can_transition_to(PostStatus::Partial));
        // The crash-recovery edge: an expired lease returns to the queue.
        assert!(PostStatus::Publishing.can_transition_to(PostStatus::Due));
        // Cancelling after the provider was contacted is NOT legal.
        assert!(!PostStatus::Publishing.can_transition_to(PostStatus::Cancelled));
        // Published is truly terminal.
        assert!(PostStatus::Published.is_terminal());
        assert!(!PostStatus::Published.can_transition_to(PostStatus::Due));
        // Partial is terminal but user-retryable.
        assert!(PostStatus::Partial.is_terminal());
        assert!(PostStatus::Partial.can_transition_to(PostStatus::Due));
    }

    #[test]
    fn validation_reports_the_first_blocking_reason() {
        let long = "a".repeat(300);
        assert_eq!(
            validate_for_platform("x", &long, &[]),
            Some("Over the 280 character limit by 20".to_string())
        );
        assert_eq!(
            validate_for_platform("x", "  ", &[]),
            Some("Post is empty".to_string())
        );
        // Bluesky is image-only.
        let video = MediaRef {
            path: "/tmp/a.mp4".into(),
            mime_type: "video/mp4".into(),
            name: "a.mp4".into(),
        };
        assert!(validate_for_platform("bluesky", "hi", &[video.clone()]).is_some());
        assert!(validate_for_platform("x", "hi", &[video]).is_none());
        // Emoji count as one character each, not four bytes.
        assert!(validate_for_platform("bluesky", &"🎉".repeat(300), &[]).is_none());
    }

    #[test]
    fn segment_validation_degrades_rather_than_erroring_on_single_post_platforms() {
        let segs = vec![
            PostSegment {
                text: "one".into(),
                media: vec![],
            },
            PostSegment {
                text: "two".into(),
                media: vec![],
            },
        ];
        // Reddit is `none`: the extra segment is dropped at publish, not rejected.
        assert_eq!(validate_segments_for_platform("reddit", &segs), None);
        // X threads, so both segments are checked and prefixed.
        let over = vec![
            PostSegment {
                text: "ok".into(),
                media: vec![],
            },
            PostSegment {
                text: "a".repeat(300),
                media: vec![],
            },
        ];
        assert_eq!(
            validate_segments_for_platform("x", &over),
            Some("Post 2: Over the 280 character limit by 20".to_string())
        );
        assert_eq!(
            validate_segments_for_platform("x", &[]),
            Some("Post is empty".to_string())
        );
    }
}
