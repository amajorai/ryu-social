//! SQLite persistence for the whole Outpost spine (`~/.ryu/social.db`).
//!
//! ## Concurrency: why every status write is a compare-and-swap
//!
//! The design this schema is ported from ran entirely inside one webview process,
//! so a module-level `isSweeping` boolean and an in-memory `inFlight` set were
//! sufficient to keep a post from publishing twice. That premise is INVERTED here:
//! this sidecar has concurrent axum handlers plus a tokio tick task, all against one
//! database. Every in-process guard from the original is therefore unsound, and a
//! read-then-blind-`UPDATE` has no claim semantics at all — two workers both "win".
//!
//! So every transition that hands work between actors is written as a guarded CAS
//! with the expected prior state in the `WHERE` clause, and the affected-row count
//! (or the `RETURNING` rows) IS the claim token:
//!
//! ```sql
//! UPDATE scheduled_posts SET status = 'due'
//!  WHERE status = 'scheduled' AND scheduled_for <= ?1
//!  RETURNING …;                       -- claim: only one sweep gets each row
//! UPDATE scheduled_posts SET status = 'publishing'
//!  WHERE id = ?1 AND status = 'due';  -- 0 rows changed ⇒ someone else has it
//! ```
//!
//! `UPDATE … RETURNING` collapses the original's SELECT-then-UPDATE pair into one
//! atomic statement, which is what makes the sweep safe to run from more than one
//! place.
//!
//! ## Crash recovery
//!
//! `publishing` in the original had NO exit on process death: `getDuePosts` only
//! selected `status='due'`, so a crash mid-publish orphaned the row permanently.
//! Here, `post_targets.claimed_at` is a lease stamp and
//! [`SocialStore::reap_expired_claims`] returns anything past its TTL to `due`.
//!
//! **This is coupled to idempotency and must not ship alone.** A reaper without a
//! durable record of what already reached the remote platform does not fix
//! double-publishing, it CAUSES it. `post_history` + `post_targets.attempts` are
//! that record, and the publish runner must consult them before re-attempting.
//!
//! ## No foreign keys — a deliberate choice, not an omission
//!
//! `PRAGMA foreign_keys` is **per-connection and not persisted in the file**, so a
//! schema with real `ON DELETE CASCADE` behaves differently depending on which code
//! path opened the connection — silent orphans on one, cascades on the other. That
//! failure mode is invisible until data is already lost. Instead, deletes run an
//! explicit ordered cascade inside a transaction (see
//! [`SocialStore::delete_workspace`]), which is auditable and connection-independent.
//! `post_targets.social_account_id` is consequently allowed to dangle: an account may
//! be hard-deleted while its published history survives, and the publish path
//! tolerates a missing account row by falling back to the denormalized platform.
//!
//! ## Locking
//!
//! One `Arc<tokio::sync::Mutex<Connection>>` (the async mutex, matching `ryu-teams` /
//! `ryu-mail`) — a single writer with WAL underneath. `busy_timeout` still matters
//! because WAL admits readers from OTHER processes (a `sqlite3` shell, a backup),
//! about which this process's mutex knows nothing.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, Row};
use tokio::sync::Mutex;

use crate::models::*;

/// The schema version this build expects. Bump it and add a `v<N>` arm in
/// [`SocialStore::migrate`] when the shape changes.
///
/// A `PRAGMA user_version` ladder rather than bare `CREATE TABLE IF NOT EXISTS`
/// (which is what the sibling apps use): `IF NOT EXISTS` cannot add a COLUMN to a
/// table that already exists, so the moment a later agent needs one it would have
/// to retrofit the whole versioning scheme onto live user databases. Paying for it
/// now costs one integer.
const SCHEMA_VERSION: i32 = 2;

/// SQLite-backed store for the social spine. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct SocialStore {
    conn: Arc<Mutex<Connection>>,
}

impl SocialStore {
    /// Open (creating if needed) the DB at `path` and migrate it. The path is
    /// injected by the caller (`paths::ryu_dir().join("social.db")`) so this module
    /// has no opinion about where the node's data lives.
    pub fn open(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).context("creating parent dir for social.db")?;
        }
        let conn = Connection::open(&path)
            .with_context(|| format!("opening social db at {}", path.display()))?;
        Self::prepare(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// In-memory store. A plain `pub fn`, not `#[cfg(test)]`, so the later agents'
    /// module tests (publish, scheduler, inbox, analytics) can build a real store
    /// without a temp file — the same convention `ryu-teams` uses.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::prepare(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Pragmas then migrations. Both paths call this so an in-memory store is
    /// byte-for-byte the same schema as a real one — a divergence here would make
    /// every module test a lie.
    fn prepare(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            // WAL: readers never block the single writer, which matters because the
            // tick task writes while the UI polls the calendar.
            // synchronous=NORMAL: safe under WAL (a crash can lose the last commit,
            // not corrupt the file) and avoids an fsync per publish.
            // busy_timeout: this process serializes its own writes behind the mutex,
            // but another process holding the file (a shell, a backup) would
            // otherwise fail instantly instead of waiting.
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;",
        )
        .context("applying social db pragmas")?;
        Self::migrate(conn)
    }

    fn migrate(conn: &Connection) -> Result<()> {
        let current: i32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if current >= SCHEMA_VERSION {
            return Ok(());
        }
        if current < 1 {
            conn.execute_batch(V1_DDL)
                .context("applying social schema v1")?;
        }
        if current < 2 {
            conn.execute_batch(V2_DDL)
                .context("applying social schema v2")?;
        }
        // Each arm is additive and idempotent, and the ladder never re-runs a
        // completed step.
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)
            .context("stamping social schema version")?;
        Ok(())
    }
}

/// The complete v1 schema.
///
/// Collapsed into ONE statement batch rather than replayed as a migration history,
/// because there are no existing databases to migrate — this app has never shipped.
/// Every table is declared in its final shape.
const V1_DDL: &str = "
CREATE TABLE IF NOT EXISTS workspaces (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS social_accounts (
  id            TEXT PRIMARY KEY,
  workspace_id  TEXT NOT NULL,
  platform      TEXT NOT NULL,
  account_label TEXT NOT NULL,
  external_id   TEXT,
  connected     INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_social_accounts_workspace
  ON social_accounts(workspace_id);

CREATE TABLE IF NOT EXISTS drafts (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  body         TEXT NOT NULL,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_drafts_workspace
  ON drafts(workspace_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS scheduled_posts (
  id            TEXT PRIMARY KEY,
  workspace_id  TEXT NOT NULL,
  draft_id      TEXT,
  scheduled_for INTEGER NOT NULL,
  status        TEXT NOT NULL,
  created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scheduled_posts_workspace
  ON scheduled_posts(workspace_id, scheduled_for);
-- The scheduler's hot predicate is `status = 'scheduled' AND scheduled_for <= ?`.
-- The workspace-leading index above cannot serve it, so without this the sweep is
-- a full table scan every 30 seconds, forever.
CREATE INDEX IF NOT EXISTS idx_scheduled_posts_sweep
  ON scheduled_posts(status, scheduled_for);

CREATE TABLE IF NOT EXISTS post_targets (
  id                TEXT PRIMARY KEY,
  scheduled_post_id TEXT NOT NULL,
  social_account_id TEXT NOT NULL,
  platform          TEXT NOT NULL,
  -- A full DraftBody JSON blob, not plain text: a per-target override must not
  -- silently drop that target's media and thread structure.
  variant_body      TEXT,
  status            TEXT NOT NULL,
  attempts          INTEGER NOT NULL DEFAULT 0,
  next_attempt_at   INTEGER,
  -- Lease stamp for the CAS claim + crash reaper. NULL = unclaimed.
  claimed_at        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_post_targets_post
  ON post_targets(scheduled_post_id);
-- Serves `GET /queue` (pending work ordered by when it may next run) and the
-- reaper's `status = 'publishing' AND claimed_at < ?` scan.
CREATE INDEX IF NOT EXISTS idx_post_targets_queue
  ON post_targets(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS post_history (
  id             TEXT PRIMARY KEY,
  post_target_id TEXT NOT NULL,
  status         TEXT NOT NULL,
  remote_url     TEXT,
  remote_id      TEXT,
  error          TEXT,
  published_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_post_history_target
  ON post_history(post_target_id, published_at DESC);

CREATE TABLE IF NOT EXISTS templates (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  name         TEXT NOT NULL,
  body         TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_templates_workspace
  ON templates(workspace_id, created_at DESC);

CREATE TABLE IF NOT EXISTS media_assets (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  kind         TEXT NOT NULL,
  path         TEXT NOT NULL,
  name         TEXT NOT NULL,
  mime_type    TEXT,
  created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_media_assets_workspace
  ON media_assets(workspace_id, created_at DESC);
-- A real UNIQUE index, not a pre-insert SELECT: check-then-insert is racy under
-- concurrent handlers, and `INSERT … ON CONFLICT DO NOTHING` is not.
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_assets_path
  ON media_assets(workspace_id, path);

CREATE TABLE IF NOT EXISTS inbox_items (
  id                TEXT PRIMARY KEY,
  workspace_id      TEXT NOT NULL,
  social_account_id TEXT NOT NULL,
  platform          TEXT NOT NULL,
  kind              TEXT NOT NULL,
  author            TEXT NOT NULL,
  text              TEXT NOT NULL,
  permalink         TEXT,
  external_id       TEXT NOT NULL,
  received_at       INTEGER NOT NULL,
  replied           INTEGER NOT NULL DEFAULT 0,
  read              INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_inbox_items_workspace
  ON inbox_items(workspace_id, received_at DESC);
-- The dedupe key: re-polling a platform must not duplicate the inbox.
CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_items_external
  ON inbox_items(workspace_id, social_account_id, external_id);

CREATE TABLE IF NOT EXISTS activity_items (
  id                    TEXT PRIMARY KEY,
  workspace_id          TEXT NOT NULL,
  social_account_id     TEXT NOT NULL,
  platform              TEXT NOT NULL,
  post_remote_id        TEXT NOT NULL,
  permalink             TEXT,
  text                  TEXT,
  likes                 INTEGER NOT NULL DEFAULT 0,
  comments              INTEGER NOT NULL DEFAULT 0,
  shares                INTEGER NOT NULL DEFAULT 0,
  views                 INTEGER NOT NULL DEFAULT 0,
  engagement_fetched_at INTEGER,
  published_at          INTEGER
);
CREATE INDEX IF NOT EXISTS idx_activity_items_workspace
  ON activity_items(workspace_id, published_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_items_dedupe
  ON activity_items(workspace_id, social_account_id, post_remote_id);

CREATE TABLE IF NOT EXISTS settings (
  workspace_id TEXT PRIMARY KEY,
  json         TEXT NOT NULL,
  updated_at   INTEGER NOT NULL
);

-- Seeded so a first-run client always has somewhere to write and `?workspace_id=`
-- can be defaulted instead of required on every route.
INSERT OR IGNORE INTO workspaces (id, name, created_at)
  VALUES ('default', 'Default', 0);
";

/// v2 — one account may appear at most ONCE in a post's fan-out.
///
/// Without it, `POST /posts {"account_ids":["acc_x","acc_x"]}` writes two
/// `post_targets` rows for the same account. The runner's durable already-published
/// guard is keyed on `post_targets.id`, so it cannot see the sibling row, and the
/// second leg issues a second provider call carrying the same
/// `idempotency_key_for(post_id, account_id)` — which Composio is not documented to
/// honour. The result is the same post published twice to the same account, from one
/// well-formed request, with no race involved.
///
/// A real UNIQUE index rather than a pre-insert SELECT, for the same reason
/// `idx_media_assets_path` is one: it makes the durable guard's key AGREE with the
/// idempotency key's key at the storage layer, where check-then-insert cannot.
/// `create_post` dedupes ahead of it so the API returns a clean 200 rather than a 500
/// from a constraint violation; the index is the floor under that, not a substitute.
///
/// The DELETE runs first because a dev database written before this arm existed may
/// already hold duplicate legs, and `CREATE UNIQUE INDEX` over them would fail — the
/// migration would then abort on every boot, forever. It keeps the lowest-`rowid`
/// leg (the one whose history, if any, was written first) and drops the siblings.
const V2_DDL: &str = "
DELETE FROM post_history WHERE post_target_id IN (
  SELECT id FROM post_targets WHERE rowid NOT IN (
    SELECT MIN(rowid) FROM post_targets
     GROUP BY scheduled_post_id, social_account_id
  )
);
DELETE FROM post_targets WHERE rowid NOT IN (
  SELECT MIN(rowid) FROM post_targets
   GROUP BY scheduled_post_id, social_account_id
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_post_targets_account
  ON post_targets(scheduled_post_id, social_account_id);
";

// ── Row decoders ───────────────────────────────────────────────────────────────
//
// One decoder per table, each taking an explicit column order that its callers'
// SELECTs must match. Every SELECT in this file names its columns explicitly rather
// than `SELECT *`, so adding a column can never silently shift a positional read.

fn row_to_workspace(row: &Row<'_>) -> rusqlite::Result<Workspace> {
    Ok(Workspace {
        id: row.get(0)?,
        name: row.get(1)?,
        created_at: row.get(2)?,
    })
}

fn row_to_account(row: &Row<'_>) -> rusqlite::Result<SocialAccount> {
    let platform: String = row.get(2)?;
    Ok(SocialAccount {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        // An unknown platform string degrades to `X` rather than failing the whole
        // list read — see the tolerance rationale in `models`.
        platform: Platform::parse(&platform).unwrap_or(Platform::X),
        account_label: row.get(3)?,
        external_id: row.get(4)?,
        connected: row.get::<_, i64>(5)? != 0,
        created_at: row.get(6)?,
    })
}

fn row_to_draft(row: &Row<'_>) -> rusqlite::Result<Draft> {
    let body: String = row.get(2)?;
    Ok(Draft {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        body: DraftBody::decode(&body),
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

fn row_to_post(row: &Row<'_>) -> rusqlite::Result<ScheduledPost> {
    let status: String = row.get(4)?;
    Ok(ScheduledPost {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        draft_id: row.get(2)?,
        scheduled_for: row.get(3)?,
        status: PostStatus::from_db(&status),
        created_at: row.get(5)?,
        targets: Vec::new(),
    })
}

fn row_to_target(row: &Row<'_>) -> rusqlite::Result<PostTarget> {
    let platform: String = row.get(3)?;
    let variant: Option<String> = row.get(4)?;
    let status: String = row.get(5)?;
    Ok(PostTarget {
        id: row.get(0)?,
        scheduled_post_id: row.get(1)?,
        social_account_id: row.get(2)?,
        platform: Platform::parse(&platform).unwrap_or(Platform::X),
        // A blank override is the same as no override — the composer writes "" when
        // a user clears the per-target box, and treating that as an empty body would
        // publish nothing.
        variant_body: variant
            .filter(|v| !v.trim().is_empty())
            .map(|v| DraftBody::decode(&v)),
        status: TargetStatus::from_db(&status),
        attempts: row.get::<_, i64>(6)?.max(0) as u32,
        next_attempt_at: row.get(7)?,
        claimed_at: row.get(8)?,
    })
}

fn row_to_history(row: &Row<'_>) -> rusqlite::Result<PostHistoryEntry> {
    let status: String = row.get(2)?;
    Ok(PostHistoryEntry {
        id: row.get(0)?,
        post_target_id: row.get(1)?,
        status: HistoryStatus::from_db(&status),
        remote_url: row.get(3)?,
        remote_id: row.get(4)?,
        error: row.get(5)?,
        published_at: row.get(6)?,
    })
}

fn row_to_template(row: &Row<'_>) -> rusqlite::Result<Template> {
    let body: String = row.get(3)?;
    Ok(Template {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        name: row.get(2)?,
        body: TemplateBody::decode(&body),
        created_at: row.get(4)?,
    })
}

fn row_to_media(row: &Row<'_>) -> rusqlite::Result<MediaAsset> {
    let kind: String = row.get(2)?;
    Ok(MediaAsset {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        kind: if kind == "video" {
            MediaKind::Video
        } else {
            MediaKind::Image
        },
        path: row.get(3)?,
        name: row.get(4)?,
        mime_type: row.get(5)?,
        created_at: row.get(6)?,
    })
}

fn row_to_inbox(row: &Row<'_>) -> rusqlite::Result<InboxItem> {
    let platform: String = row.get(3)?;
    let kind: String = row.get(4)?;
    Ok(InboxItem {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        social_account_id: row.get(2)?,
        platform: Platform::parse(&platform).unwrap_or(Platform::X),
        kind: InboxKind::from_db(&kind),
        author: row.get(5)?,
        text: row.get(6)?,
        permalink: row.get(7)?,
        external_id: row.get(8)?,
        received_at: row.get(9)?,
        replied: row.get::<_, i64>(10)? != 0,
        read: row.get::<_, i64>(11)? != 0,
    })
}

fn row_to_activity(row: &Row<'_>) -> rusqlite::Result<ActivityItem> {
    let platform: String = row.get(3)?;
    Ok(ActivityItem {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        social_account_id: row.get(2)?,
        platform: Platform::parse(&platform).unwrap_or(Platform::X),
        post_remote_id: row.get(4)?,
        permalink: row.get(5)?,
        text: row.get(6)?,
        likes: row.get::<_, i64>(7)?.max(0) as u64,
        comments: row.get::<_, i64>(8)?.max(0) as u64,
        shares: row.get::<_, i64>(9)?.max(0) as u64,
        views: row.get::<_, i64>(10)?.max(0) as u64,
        engagement_fetched_at: row.get(11)?,
        published_at: row.get(12)?,
    })
}

// Column lists, declared once so a decoder and its SELECTs cannot drift apart.
const COLS_WORKSPACE: &str = "id, name, created_at";
const COLS_ACCOUNT: &str =
    "id, workspace_id, platform, account_label, external_id, connected, created_at";
const COLS_DRAFT: &str = "id, workspace_id, body, created_at, updated_at";
const COLS_POST: &str = "id, workspace_id, draft_id, scheduled_for, status, created_at";
const COLS_TARGET: &str = "id, scheduled_post_id, social_account_id, platform, variant_body, \
                           status, attempts, next_attempt_at, claimed_at";
const COLS_HISTORY: &str = "id, post_target_id, status, remote_url, remote_id, error, published_at";
const COLS_TEMPLATE: &str = "id, workspace_id, name, body, created_at";
const COLS_MEDIA: &str = "id, workspace_id, kind, path, name, mime_type, created_at";
const COLS_INBOX: &str = "id, workspace_id, social_account_id, platform, kind, author, text, \
                          permalink, external_id, received_at, replied, read";
const COLS_ACTIVITY: &str = "id, workspace_id, social_account_id, platform, post_remote_id, \
                             permalink, text, likes, comments, shares, views, \
                             engagement_fetched_at, published_at";

// ── Workspaces ─────────────────────────────────────────────────────────────────

impl SocialStore {
    pub async fn list_workspaces(&self) -> Result<Vec<Workspace>> {
        let conn = self.conn.lock().await;
        let sql =
            format!("SELECT {COLS_WORKSPACE} FROM workspaces ORDER BY created_at ASC, name ASC");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], row_to_workspace)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub async fn get_workspace(&self, id: &str) -> Result<Option<Workspace>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_WORKSPACE} FROM workspaces WHERE id = ?1");
        Ok(conn
            .query_row(&sql, params![id], row_to_workspace)
            .optional()?)
    }

    pub async fn create_workspace(&self, name: &str) -> Result<Workspace> {
        let ws = Workspace {
            id: new_id(ID_WORKSPACE),
            name: name.trim().to_string(),
            created_at: now_ms(),
        };
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at) VALUES (?1, ?2, ?3)",
            params![ws.id, ws.name, ws.created_at],
        )?;
        Ok(ws)
    }

    /// Rename. Returns `false` when no row matched, so the caller can 404 instead of
    /// reporting a successful no-op.
    pub async fn rename_workspace(&self, id: &str, name: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE workspaces SET name = ?2 WHERE id = ?1",
            params![id, name.trim()],
        )?;
        Ok(n > 0)
    }

    /// Delete a workspace and everything under it.
    ///
    /// An explicit ordered cascade in ONE transaction (see the module docs for why
    /// not `ON DELETE CASCADE`). Order matters: the two child tables are reached
    /// through their parents by subselect, so they must go FIRST — deleting
    /// `scheduled_posts` before `post_targets` would strand the targets with no way
    /// left to find them.
    pub async fn delete_workspace(&self, id: &str) -> Result<bool> {
        if id == DEFAULT_WORKSPACE_ID {
            anyhow::bail!("the default workspace cannot be deleted");
        }
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM post_history WHERE post_target_id IN (
                 SELECT t.id FROM post_targets t
                 JOIN scheduled_posts p ON p.id = t.scheduled_post_id
                 WHERE p.workspace_id = ?1)",
            params![id],
        )?;
        tx.execute(
            "DELETE FROM post_targets WHERE scheduled_post_id IN (
                 SELECT id FROM scheduled_posts WHERE workspace_id = ?1)",
            params![id],
        )?;
        for table in [
            "scheduled_posts",
            "drafts",
            "social_accounts",
            "templates",
            "media_assets",
            "inbox_items",
            "activity_items",
            "settings",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE workspace_id = ?1"),
                params![id],
            )?;
        }
        let n = tx.execute("DELETE FROM workspaces WHERE id = ?1", params![id])?;
        tx.commit()?;
        Ok(n > 0)
    }
}

// ── Social accounts ────────────────────────────────────────────────────────────

impl SocialStore {
    pub async fn list_accounts(&self, workspace_id: &str) -> Result<Vec<SocialAccount>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_ACCOUNT} FROM social_accounts WHERE workspace_id = ?1
             ORDER BY platform ASC, account_label ASC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![workspace_id], row_to_account)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub async fn get_account(&self, id: &str) -> Result<Option<SocialAccount>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_ACCOUNT} FROM social_accounts WHERE id = ?1");
        Ok(conn
            .query_row(&sql, params![id], row_to_account)
            .optional()?)
    }

    pub async fn create_account(
        &self,
        workspace_id: &str,
        platform: Platform,
        account_label: &str,
        external_id: Option<&str>,
    ) -> Result<SocialAccount> {
        let account = SocialAccount {
            id: new_id(ID_ACCOUNT),
            workspace_id: workspace_id.to_string(),
            platform,
            account_label: account_label.trim().to_string(),
            external_id: external_id.map(str::to_string),
            // Optimistically connected: the row is created BY a connect flow, and
            // the provider handshake either confirms it or `set_connected(false)`
            // walks it back. Starting disconnected would render every freshly added
            // account as broken.
            connected: true,
            created_at: now_ms(),
        };
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO social_accounts
               (id, workspace_id, platform, account_label, external_id, connected, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                account.id,
                account.workspace_id,
                account.platform.as_str(),
                account.account_label,
                account.external_id,
                i64::from(account.connected),
                account.created_at
            ],
        )?;
        Ok(account)
    }

    /// Record the outcome of a connect/disconnect handshake.
    pub async fn set_account_connection(
        &self,
        id: &str,
        connected: bool,
        external_id: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            // COALESCE so a reconnect that does not re-report the external id keeps
            // the one we already know, instead of nulling it.
            "UPDATE social_accounts
                SET connected = ?2, external_id = COALESCE(?3, external_id)
              WHERE id = ?1",
            params![id, i64::from(connected), external_id],
        )?;
        Ok(n > 0)
    }

    /// Hard delete. Deliberately does NOT cascade to `post_targets`: published
    /// history must survive disconnecting an account, and the target's denormalized
    /// `platform` keeps it renderable without the account row.
    ///
    /// It DOES cancel the account's still-`pending` legs, in the same transaction.
    /// Preserving history and continuing to publish are different things: the
    /// publish path deliberately tolerates a missing account row (it rebuilds a
    /// `ProviderAccount` from the target's denormalized `platform`), and the Bluesky
    /// adapter authenticates from node-level settings rather than the account's
    /// `external_id` — so without this, a post already queued against a removed
    /// account still went out to it. `published`/`failed` targets and every
    /// `post_history` row are untouched.
    pub async fn delete_account(&self, id: &str) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute("DELETE FROM social_accounts WHERE id = ?1", params![id])?;
        if n > 0 {
            tx.execute(
                "UPDATE post_targets
                    SET status = ?2, next_attempt_at = NULL, claimed_at = NULL
                  WHERE social_account_id = ?1 AND status = ?3",
                params![
                    id,
                    TargetStatus::Cancelled.as_str(),
                    TargetStatus::Pending.as_str()
                ],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }
}

// ── Drafts ─────────────────────────────────────────────────────────────────────

impl SocialStore {
    pub async fn list_drafts(&self, workspace_id: &str) -> Result<Vec<Draft>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_DRAFT} FROM drafts WHERE workspace_id = ?1 ORDER BY updated_at DESC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![workspace_id], row_to_draft)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub async fn get_draft(&self, id: &str) -> Result<Option<Draft>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_DRAFT} FROM drafts WHERE id = ?1");
        Ok(conn.query_row(&sql, params![id], row_to_draft).optional()?)
    }

    pub async fn create_draft(&self, workspace_id: &str, body: &DraftBody) -> Result<Draft> {
        let now = now_ms();
        let mut body = body.clone();
        body.normalize();
        let draft = Draft {
            id: new_id(ID_DRAFT),
            workspace_id: workspace_id.to_string(),
            body,
            created_at: now,
            updated_at: now,
        };
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO drafts (id, workspace_id, body, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                draft.id,
                draft.workspace_id,
                draft.body.encode(),
                draft.created_at,
                draft.updated_at
            ],
        )?;
        Ok(draft)
    }

    /// Replace a draft's body, preserving `created_at`.
    pub async fn update_draft(&self, id: &str, body: &DraftBody) -> Result<bool> {
        let mut body = body.clone();
        body.normalize();
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE drafts SET body = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, body.encode(), now_ms()],
        )?;
        Ok(n > 0)
    }

    pub async fn delete_draft(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        // Scheduled posts keep their `draft_id` pointing at a deleted draft. That is
        // survivable — content resolution treats a missing draft as "no body" and
        // fails the target with a clear reason — and is strictly better than
        // cascading a delete into a queue the user did not ask to empty.
        let n = conn.execute("DELETE FROM drafts WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }
}

// ── Scheduled posts + targets ──────────────────────────────────────────────────

/// One leg of a requested fan-out, before it becomes a [`PostTarget`] row.
#[derive(Debug, Clone)]
pub struct NewTarget {
    pub social_account_id: String,
    pub platform: Platform,
    pub variant_body: Option<DraftBody>,
}

impl SocialStore {
    /// Create a scheduled post AND its targets in one transaction.
    ///
    /// Atomic on purpose: a post row with no targets is indistinguishable from a
    /// post whose fan-out failed, and the runner settles it as `failed` — so a
    /// partial write would surface to the user as a mysterious failed post.
    ///
    /// Scheduling with zero targets is rejected outright for the same reason.
    pub async fn create_scheduled_post(
        &self,
        workspace_id: &str,
        draft_id: Option<&str>,
        scheduled_for: i64,
        targets: &[NewTarget],
    ) -> Result<ScheduledPost> {
        if targets.is_empty() {
            anyhow::bail!("a scheduled post needs at least one target account");
        }
        let now = now_ms();
        let post_id = new_id(ID_POST);
        let rows: Vec<PostTarget> = targets
            .iter()
            .map(|t| PostTarget {
                id: new_id(ID_TARGET),
                scheduled_post_id: post_id.clone(),
                social_account_id: t.social_account_id.clone(),
                platform: t.platform,
                variant_body: t.variant_body.clone(),
                status: TargetStatus::Pending,
                attempts: 0,
                next_attempt_at: Some(scheduled_for),
                claimed_at: None,
            })
            .collect();

        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO scheduled_posts
               (id, workspace_id, draft_id, scheduled_for, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                post_id,
                workspace_id,
                draft_id,
                scheduled_for,
                PostStatus::Scheduled.as_str(),
                now
            ],
        )?;
        for t in &rows {
            tx.execute(
                "INSERT INTO post_targets
                   (id, scheduled_post_id, social_account_id, platform, variant_body,
                    status, attempts, next_attempt_at, claimed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, NULL)",
                params![
                    t.id,
                    t.scheduled_post_id,
                    t.social_account_id,
                    t.platform.as_str(),
                    t.variant_body.as_ref().map(DraftBody::encode),
                    t.status.as_str(),
                    t.next_attempt_at
                ],
            )?;
        }
        tx.commit()?;

        Ok(ScheduledPost {
            id: post_id,
            workspace_id: workspace_id.to_string(),
            draft_id: draft_id.map(str::to_string),
            scheduled_for,
            status: PostStatus::Scheduled,
            created_at: now,
            targets: rows,
        })
    }

    /// List posts in a workspace, newest-scheduled first, with targets attached.
    ///
    /// `statuses` filters when non-empty. Targets are fetched in ONE extra query and
    /// grouped in memory rather than per-post, so this is 2 queries regardless of
    /// page size.
    pub async fn list_scheduled_posts(
        &self,
        workspace_id: &str,
        statuses: &[PostStatus],
    ) -> Result<Vec<ScheduledPost>> {
        let conn = self.conn.lock().await;
        let mut sql = format!("SELECT {COLS_POST} FROM scheduled_posts WHERE workspace_id = ?1");
        let mut binds: Vec<String> = vec![workspace_id.to_string()];
        if !statuses.is_empty() {
            let placeholders: Vec<String> =
                (0..statuses.len()).map(|i| format!("?{}", i + 2)).collect();
            sql.push_str(&format!(" AND status IN ({})", placeholders.join(", ")));
            binds.extend(statuses.iter().map(|s| s.as_str().to_string()));
        }
        sql.push_str(" ORDER BY scheduled_for DESC");
        let mut stmt = conn.prepare(&sql)?;
        let posts = stmt
            .query_map(params_from_iter(binds.iter()), row_to_post)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        Ok(attach_targets(&conn, posts)?)
    }

    pub async fn get_scheduled_post(&self, id: &str) -> Result<Option<ScheduledPost>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_POST} FROM scheduled_posts WHERE id = ?1");
        let Some(post) = conn.query_row(&sql, params![id], row_to_post).optional()? else {
            return Ok(None);
        };
        Ok(attach_targets(&conn, vec![post])?.pop())
    }

    /// Posts whose scheduled time falls inside `[from, to)`, for the calendar.
    ///
    /// Half-open on purpose: consecutive day/week buckets tile the timeline with no
    /// post appearing in two of them.
    pub async fn list_posts_in_range(
        &self,
        workspace_id: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<ScheduledPost>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_POST} FROM scheduled_posts
              WHERE workspace_id = ?1 AND scheduled_for >= ?2 AND scheduled_for < ?3
              ORDER BY scheduled_for ASC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let posts = stmt
            .query_map(params![workspace_id, from, to], row_to_post)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        Ok(attach_targets(&conn, posts)?)
    }

    /// Move a post's time. **Guarded**: only while still `scheduled`, so a post the
    /// sweep already claimed cannot be moved out from under the runner. A no-op
    /// return (`false`) means exactly that, and the caller should 409, not 404.
    pub async fn reschedule_post(&self, id: &str, scheduled_for: i64) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute(
            "UPDATE scheduled_posts SET scheduled_for = ?2
              WHERE id = ?1 AND status = ?3",
            params![id, scheduled_for, PostStatus::Scheduled.as_str()],
        )?;
        // Keep the targets' next-attempt projection honest, or `GET /queue` would
        // still show the old time — but ONLY when the guard above actually moved the
        // post, and in the same transaction. Ungated, a reschedule the guard rejected
        // (the post is already `publishing`) still rewrote its pending targets'
        // `next_attempt_at`, and `GET /queue` orders and counts down on exactly that
        // column: the caller got a correct 409 while the read model silently moved to
        // a time the runner never honours, because `next_attempt_at` is not in the
        // runner's predicate. Same shape as `cancel_post` and `mark_post_due_now`.
        if n > 0 {
            tx.execute(
                "UPDATE post_targets SET next_attempt_at = ?2
                  WHERE scheduled_post_id = ?1 AND status = ?3",
                params![id, scheduled_for, TargetStatus::Pending.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    /// Cancel a post and its still-pending targets, in one transaction.
    ///
    /// Guarded to `scheduled`/`due`: a post already `publishing` has contacted (or is
    /// contacting) a provider, and cancelling locally would leave the local state
    /// lying about what is live on the platform. Targets already `publishing` are
    /// likewise left alone.
    pub async fn cancel_post(&self, id: &str) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute(
            "UPDATE scheduled_posts SET status = ?2
              WHERE id = ?1 AND status IN (?3, ?4)",
            params![
                id,
                PostStatus::Cancelled.as_str(),
                PostStatus::Scheduled.as_str(),
                PostStatus::Due.as_str()
            ],
        )?;
        if n > 0 {
            tx.execute(
                "UPDATE post_targets SET status = ?2, next_attempt_at = NULL, claimed_at = NULL
                  WHERE scheduled_post_id = ?1 AND status = ?3",
                params![
                    id,
                    TargetStatus::Cancelled.as_str(),
                    TargetStatus::Pending.as_str()
                ],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    pub async fn delete_scheduled_post(&self, id: &str) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM post_history WHERE post_target_id IN
               (SELECT id FROM post_targets WHERE scheduled_post_id = ?1)",
            params![id],
        )?;
        tx.execute(
            "DELETE FROM post_targets WHERE scheduled_post_id = ?1",
            params![id],
        )?;
        let n = tx.execute("DELETE FROM scheduled_posts WHERE id = ?1", params![id])?;
        tx.commit()?;
        Ok(n > 0)
    }

    // ── The claim/settle CAS surface (the scheduler + publish runner's seam) ──

    /// The sweep. Atomically flips every eligible post to `due` and RETURNS the rows
    /// it claimed.
    ///
    /// One statement, not a SELECT then an UPDATE: the flip IS the claim, so a
    /// concurrent sweep gets an empty result rather than a duplicate batch. Callers
    /// should treat the returned vec as work they now exclusively own.
    pub async fn claim_due_posts(&self, now: i64, limit: usize) -> Result<Vec<ScheduledPost>> {
        let conn = self.conn.lock().await;
        // The `id IN (SELECT … LIMIT ?)` wrapper is what bounds one sweep's batch;
        // SQLite does not accept LIMIT directly on UPDATE in the default build.
        let sql = format!(
            "UPDATE scheduled_posts SET status = '{due}'
              WHERE id IN (
                SELECT id FROM scheduled_posts
                 WHERE status = '{scheduled}' AND scheduled_for <= ?1
                 ORDER BY scheduled_for ASC LIMIT ?2)
              RETURNING {COLS_POST}",
            due = PostStatus::Due.as_str(),
            scheduled = PostStatus::Scheduled.as_str(),
        );
        let mut stmt = conn.prepare(&sql)?;
        let posts = stmt
            .query_map(params![now, limit as i64], row_to_post)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        Ok(attach_targets(&conn, posts)?)
    }

    /// The late-subscriber drain: posts already flipped to `due` by an earlier sweep
    /// (including the catch-up sweep at boot) that no runner has claimed yet.
    pub async fn list_due_posts(&self, limit: usize) -> Result<Vec<ScheduledPost>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_POST} FROM scheduled_posts WHERE status = ?1
              ORDER BY scheduled_for ASC LIMIT ?2"
        );
        let mut stmt = conn.prepare(&sql)?;
        let posts = stmt
            .query_map(params![PostStatus::Due.as_str(), limit as i64], row_to_post)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        Ok(attach_targets(&conn, posts)?)
    }

    /// Claim a due post for publishing. `false` means another worker got there
    /// first — the caller must NOT proceed.
    pub async fn claim_post_for_publishing(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE scheduled_posts SET status = ?2 WHERE id = ?1 AND status = ?3",
            params![
                id,
                PostStatus::Publishing.as_str(),
                PostStatus::Due.as_str()
            ],
        )?;
        Ok(n > 0)
    }

    /// Settle a post to its terminal status. Guarded on `publishing` so a settle
    /// arriving after a reaper already recycled the row cannot resurrect a stale
    /// verdict.
    pub async fn settle_post(&self, id: &str, status: PostStatus) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE scheduled_posts SET status = ?2 WHERE id = ?1 AND status = ?3",
            params![id, status.as_str(), PostStatus::Publishing.as_str()],
        )?;
        Ok(n > 0)
    }

    /// Re-queue a settled-but-incomplete post for another run: the post goes back to
    /// `due` and its failed targets back to `pending` with their attempt counter
    /// reset.
    ///
    /// Guarded to `partial`/`failed` — a `published` post has nothing to retry, and a
    /// `publishing` one is already in flight. `published` targets are untouched, so a
    /// retry cannot double-post the legs that worked.
    pub async fn retry_post(&self, id: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute(
            "UPDATE scheduled_posts SET status = ?2 WHERE id = ?1 AND status IN (?3, ?4)",
            params![
                id,
                PostStatus::Due.as_str(),
                PostStatus::Partial.as_str(),
                PostStatus::Failed.as_str()
            ],
        )?;
        if n > 0 {
            tx.execute(
                "UPDATE post_targets
                    SET status = ?2, attempts = 0, next_attempt_at = ?3, claimed_at = NULL
                  WHERE scheduled_post_id = ?1 AND status = ?4",
                params![
                    id,
                    TargetStatus::Pending.as_str(),
                    now,
                    TargetStatus::Failed.as_str()
                ],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    /// Move a post to `due` right now, regardless of its scheduled time. This is
    /// "publish now" for an already-scheduled post; guarded to `scheduled` so it
    /// cannot race a sweep that already claimed the row.
    pub async fn mark_post_due_now(&self, id: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute(
            "UPDATE scheduled_posts SET status = ?2, scheduled_for = ?3
              WHERE id = ?1 AND status = ?4",
            params![
                id,
                PostStatus::Due.as_str(),
                now,
                PostStatus::Scheduled.as_str()
            ],
        )?;
        if n > 0 {
            tx.execute(
                "UPDATE post_targets SET next_attempt_at = ?2
                  WHERE scheduled_post_id = ?1 AND status = ?3",
                params![id, now, TargetStatus::Pending.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    /// A post's targets in the order the caller passed them to
    /// [`Self::create_scheduled_post`] — which is the order the runner fans out in.
    ///
    /// Ordered by `rowid`, NOT by `id`. `id` is a v4 UUID, so `ORDER BY id` is a
    /// random permutation: it would hand the runner (and the UI) a different fan-out
    /// order than the user composed, and a different one on each read of the same
    /// row set. `rowid` is SQLite's insertion counter, so it reproduces the insert
    /// order of the single transaction that wrote these rows. Safe because targets
    /// are only ever inserted once per post — both `DELETE FROM post_targets` sites
    /// drop the parent post with them, so no surviving post can be re-populated and
    /// pick up a reused rowid out of order.
    pub async fn list_targets(&self, post_id: &str) -> Result<Vec<PostTarget>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_TARGET} FROM post_targets WHERE scheduled_post_id = ?1 \
             ORDER BY rowid ASC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![post_id], row_to_target)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Claim one target for a publish attempt, stamping the lease. `false` means
    /// another worker holds it.
    pub async fn claim_target(&self, id: &str, now: i64) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE post_targets SET status = ?2, claimed_at = ?3
              WHERE id = ?1 AND status = ?4",
            params![
                id,
                TargetStatus::Publishing.as_str(),
                now,
                TargetStatus::Pending.as_str()
            ],
        )?;
        Ok(n > 0)
    }

    /// Record the outcome of a target's run: its terminal status, the attempt count
    /// actually spent, and (when a backoff is pending rather than terminal) when it
    /// may next run. Clears the lease.
    pub async fn settle_target(
        &self,
        id: &str,
        status: TargetStatus,
        attempts: u32,
        next_attempt_at: Option<i64>,
    ) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE post_targets
                SET status = ?2, attempts = ?3, next_attempt_at = ?4, claimed_at = NULL
              WHERE id = ?1",
            params![id, status.as_str(), i64::from(attempts), next_attempt_at],
        )?;
        Ok(n > 0)
    }

    /// Return targets whose publish lease expired to the queue.
    ///
    /// The ONLY exit from `publishing` after a process death. `cutoff` is
    /// `now - claim_lease_secs`; it must exceed the worst-case publish (every attempt
    /// plus its backoff sleeps) or a slow-but-healthy run gets double-claimed.
    ///
    /// Returns the ids reaped so the caller can log which work was recovered — a
    /// silent reaper makes a double-post impossible to diagnose after the fact.
    pub async fn reap_expired_claims(&self, cutoff: i64, now: i64) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "UPDATE post_targets
                SET status = '{pending}', next_attempt_at = ?2, claimed_at = NULL
              WHERE status = '{publishing}' AND claimed_at IS NOT NULL AND claimed_at < ?1
              RETURNING id",
            pending = TargetStatus::Pending.as_str(),
            publishing = TargetStatus::Publishing.as_str(),
        );
        let mut stmt = conn.prepare(&sql)?;
        let ids = stmt
            .query_map(params![cutoff, now], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        if !ids.is_empty() {
            // Their parent posts are stuck in `publishing` with no runner; return
            // them to `due` so a later drain picks them back up.
            //
            // BOTH guards below are load-bearing, and getting either wrong
            // re-creates the double-publish this reaper exists to make safe:
            //
            // 1. Scoped to the ids we ACTUALLY reaped. A predicate like "any post
            //    with a pending target" would sweep in unrelated posts every time
            //    any lease anywhere expired.
            // 2. `NOT EXISTS (… still publishing)`. Targets publish SEQUENTIALLY,
            //    so the normal mid-flight state of a three-account post is
            //    `t1=publishing, t2=pending, t3=pending`. Without this clause, a
            //    perfectly healthy post gets flipped back to `due` underneath its
            //    live runner, a second runner claims it, and both publish.
            let placeholders = (2..ids.len() + 2)
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut binds: Vec<String> = vec![PostStatus::Due.as_str().to_string()];
            binds.extend(ids.iter().cloned());
            conn.execute(
                &format!(
                    "UPDATE scheduled_posts SET status = ?1
                      WHERE status = '{publishing}'
                        AND id IN (SELECT scheduled_post_id FROM post_targets
                                    WHERE id IN ({placeholders}))
                        AND NOT EXISTS (SELECT 1 FROM post_targets t
                                         WHERE t.scheduled_post_id = scheduled_posts.id
                                           AND t.status = '{target_publishing}')",
                    publishing = PostStatus::Publishing.as_str(),
                    target_publishing = TargetStatus::Publishing.as_str(),
                ),
                params_from_iter(binds.iter()),
            )?;
        }
        Ok(ids)
    }

    /// The queue projection: every target that still owes work, with the post's
    /// workspace and schedule joined in so the UI can render one flat list.
    pub async fn list_queue(&self, workspace_id: &str, limit: usize) -> Result<Vec<QueueEntry>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {targets}, p.scheduled_for, p.status, p.draft_id
               FROM post_targets t
               JOIN scheduled_posts p ON p.id = t.scheduled_post_id
              WHERE p.workspace_id = ?1 AND t.status IN (?2, ?3)
              -- `t.rowid` breaks the tie, and the tie is the common case, not an
              -- edge one: `create_scheduled_post` stamps the SAME `next_attempt_at`
              -- on every target of a post, so all of one post's rows sort equal
              -- here. Without the tiebreaker the queue renders a post's accounts in
              -- an arbitrary order that changes between reads — and disagrees with
              -- the order the runner actually publishes them in, which is the one
              -- thing this view must never do.
              ORDER BY COALESCE(t.next_attempt_at, p.scheduled_for) ASC, t.rowid ASC
              LIMIT ?4",
            targets = COLS_TARGET
                .split(", ")
                .map(|c| format!("t.{}", c.trim()))
                .collect::<Vec<_>>()
                .join(", "),
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![
                workspace_id,
                TargetStatus::Pending.as_str(),
                TargetStatus::Publishing.as_str(),
                limit as i64
            ],
            |row| {
                let target = row_to_target(row)?;
                let scheduled_for: i64 = row.get(9)?;
                let post_status: String = row.get(10)?;
                Ok(QueueEntry {
                    // Fall back to the post's own time when the target has no
                    // explicit next attempt — that is what "next" means for a
                    // target that has never run.
                    next_attempt_at: target.next_attempt_at.unwrap_or(scheduled_for),
                    scheduled_for,
                    post_status: PostStatus::from_db(&post_status),
                    draft_id: row.get(11)?,
                    target,
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

// ── Boot crash-recovery surface (appended by the scheduler agent) ──────────────
//
// `reap_expired_claims` above keys off `post_targets.claimed_at`, which is only
// ever stamped by `claim_target`. That leaves ONE orphan shape it cannot see: a
// process that dies between `claim_post_for_publishing` (post → `publishing`) and
// the first `claim_target` leaves a post in `publishing` whose targets are all
// still `pending`. No `claimed_at` was ever written, so there is nothing to reap;
// `claim_due_posts` needs `scheduled` and `list_due_posts` needs `due`, so neither
// finds it either. The row is orphaned forever.
//
// The two methods below close that, and are called ONLY from the scheduler's boot
// pass — see `crate::scheduler::recover_orphaned_work` for why boot-only is what
// makes them safe rather than a new double-publish race.

impl SocialStore {
    /// Posts currently in one status, oldest-scheduled first.
    ///
    /// Generalizes [`Self::list_due_posts`] (which is the `Due` case, kept as its own
    /// method because it is the runner's hot path). Used by the boot recovery pass to
    /// enumerate `publishing` rows no live runner can possibly own.
    pub async fn list_posts_with_status(
        &self,
        status: PostStatus,
        limit: usize,
    ) -> Result<Vec<ScheduledPost>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_POST} FROM scheduled_posts WHERE status = ?1
              ORDER BY scheduled_for ASC LIMIT ?2"
        );
        let mut stmt = conn.prepare(&sql)?;
        let posts = stmt
            .query_map(params![status.as_str(), limit as i64], row_to_post)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        Ok(attach_targets(&conn, posts)?)
    }

    /// Return an orphaned `publishing` post to the queue as `due`.
    ///
    /// Two guards, both load-bearing:
    ///
    /// 1. `status = 'publishing'` — the CAS. A post another actor already settled is
    ///    left alone.
    /// 2. `NOT EXISTS (… target still 'publishing')` — the same clause
    ///    [`Self::reap_expired_claims`] uses, and for the same reason: targets publish
    ///    SEQUENTIALLY, so the normal mid-flight state of a three-account post is
    ///    `t1=publishing, t2=pending`. Recycling that post underneath its live runner
    ///    is precisely the double-publish this recovery exists to prevent.
    ///
    /// Returns to `due`, not `scheduled`: `due` is the state the runner drains and the
    /// only transition out of `publishing` the model admits
    /// ([`PostStatus::can_transition_to`]). Sending it back to `scheduled` would also
    /// re-arm the sweep's `scheduled_for <= now` predicate, which for a backlogged
    /// post is a no-op flip that just adds a hop.
    pub async fn requeue_publishing_post(&self, id: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction()?;
        let n = tx.execute(
            &format!(
                "UPDATE scheduled_posts SET status = ?2
                  WHERE id = ?1 AND status = ?3
                    AND NOT EXISTS (SELECT 1 FROM post_targets t
                                     WHERE t.scheduled_post_id = scheduled_posts.id
                                       AND t.status = '{target_publishing}')",
                target_publishing = TargetStatus::Publishing.as_str(),
            ),
            params![
                id,
                PostStatus::Due.as_str(),
                PostStatus::Publishing.as_str()
            ],
        )?;
        if n > 0 {
            // Keep the queue projection honest: a recovered target runs now, not at
            // whatever backoff instant the dead process last wrote.
            tx.execute(
                "UPDATE post_targets SET next_attempt_at = ?2, claimed_at = NULL
                  WHERE scheduled_post_id = ?1 AND status = ?3",
                params![id, now, TargetStatus::Pending.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }
}

/// One row of `GET /queue`: a target that still owes work, flattened with the
/// context needed to render it without a second fetch.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueueEntry {
    #[serde(flatten)]
    pub target: PostTarget,
    pub scheduled_for: i64,
    pub post_status: PostStatus,
    pub draft_id: Option<String>,
    /// When the runner will next act on this target. Never null: a target with no
    /// recorded backoff inherits its post's scheduled time.
    pub next_attempt_at: i64,
}

/// Fetch the targets for a batch of posts in one query and attach them.
///
/// Takes the already-held connection guard rather than re-locking, because it is
/// always called from inside a store method that holds the mutex — re-entering the
/// async mutex there would deadlock.
fn attach_targets(
    conn: &Connection,
    mut posts: Vec<ScheduledPost>,
) -> rusqlite::Result<Vec<ScheduledPost>> {
    if posts.is_empty() {
        return Ok(posts);
    }
    let placeholders = (1..=posts.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        // `rowid`, not `id` — see `list_targets` for why. One ordered scan is enough
        // for the multi-post case: grouping into `by_post` below preserves each
        // post's relative order, so every post comes out in its own insert order.
        "SELECT {COLS_TARGET} FROM post_targets
          WHERE scheduled_post_id IN ({placeholders}) ORDER BY rowid ASC"
    );
    let ids: Vec<&str> = posts.iter().map(|p| p.id.as_str()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let targets = stmt
        .query_map(params_from_iter(ids.iter()), row_to_target)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut by_post: BTreeMap<String, Vec<PostTarget>> = BTreeMap::new();
    for t in targets {
        by_post
            .entry(t.scheduled_post_id.clone())
            .or_default()
            .push(t);
    }
    for post in &mut posts {
        post.targets = by_post.remove(&post.id).unwrap_or_default();
    }
    Ok(posts)
}

// ── Publish history ────────────────────────────────────────────────────────────

impl SocialStore {
    /// Write the terminal record of one publish run. Called ONCE per target per run,
    /// after retries are exhausted — not once per attempt.
    pub async fn insert_history(
        &self,
        post_target_id: &str,
        status: HistoryStatus,
        remote_id: Option<&str>,
        remote_url: Option<&str>,
        error: Option<&str>,
    ) -> Result<PostHistoryEntry> {
        let entry = PostHistoryEntry {
            id: new_id(ID_HISTORY),
            post_target_id: post_target_id.to_string(),
            status,
            remote_url: remote_url.map(str::to_string),
            remote_id: remote_id.map(str::to_string),
            error: error.map(str::to_string),
            // Stamped even on the failed path: a failed publish still happened at a
            // time, and null timestamps sort arbitrarily in the history list.
            published_at: Some(now_ms()),
        };
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO post_history
               (id, post_target_id, status, remote_url, remote_id, error, published_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                entry.id,
                entry.post_target_id,
                entry.status.as_str(),
                entry.remote_url,
                entry.remote_id,
                entry.error,
                entry.published_at
            ],
        )?;
        Ok(entry)
    }

    /// Workspace-wide history, newest first. Joins through `post_targets` →
    /// `scheduled_posts` because history rows carry no `workspace_id` of their own.
    pub async fn list_history(
        &self,
        workspace_id: &str,
        limit: usize,
    ) -> Result<Vec<PostHistoryEntry>> {
        let conn = self.conn.lock().await;
        let cols = COLS_HISTORY
            .split(", ")
            .map(|c| format!("h.{}", c.trim()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {cols} FROM post_history h
               JOIN post_targets t ON t.id = h.post_target_id
               JOIN scheduled_posts p ON p.id = t.scheduled_post_id
              WHERE p.workspace_id = ?1
              ORDER BY h.published_at DESC LIMIT ?2"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![workspace_id, limit as i64], row_to_history)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub async fn get_history(&self, id: &str) -> Result<Option<PostHistoryEntry>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_HISTORY} FROM post_history WHERE id = ?1");
        Ok(conn
            .query_row(&sql, params![id], row_to_history)
            .optional()?)
    }

    pub async fn list_history_for_target(&self, target_id: &str) -> Result<Vec<PostHistoryEntry>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            // `rowid DESC` is a tiebreak, not decoration: `insert_history` stamps
            // `published_at` with `now_ms()`, so two runs of the same target inside
            // one millisecond collide and `published_at DESC` alone leaves their
            // order to SQLite. Callers read `.first()` as "the latest run" (the queue
            // view decides whether to show an error from it), so an arbitrary tie
            // would make a target that failed and then SUCCEEDED report its old error.
            // rowid is insertion order, which is exactly the intended tiebreak.
            "SELECT {COLS_HISTORY} FROM post_history WHERE post_target_id = ?1
              ORDER BY published_at DESC, rowid DESC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![target_id], row_to_history)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

// ── Templates ──────────────────────────────────────────────────────────────────

impl SocialStore {
    pub async fn list_templates(&self, workspace_id: &str) -> Result<Vec<Template>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_TEMPLATE} FROM templates WHERE workspace_id = ?1 ORDER BY name ASC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![workspace_id], row_to_template)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub async fn get_template(&self, id: &str) -> Result<Option<Template>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_TEMPLATE} FROM templates WHERE id = ?1");
        Ok(conn
            .query_row(&sql, params![id], row_to_template)
            .optional()?)
    }

    pub async fn create_template(
        &self,
        workspace_id: &str,
        name: &str,
        body: &TemplateBody,
    ) -> Result<Template> {
        let mut body = body.clone();
        body.normalize();
        let template = Template {
            id: new_id(ID_TEMPLATE),
            workspace_id: workspace_id.to_string(),
            name: name.trim().to_string(),
            body,
            created_at: now_ms(),
        };
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO templates (id, workspace_id, name, body, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                template.id,
                template.workspace_id,
                template.name,
                template.body.encode(),
                template.created_at
            ],
        )?;
        Ok(template)
    }

    /// Patch a template. Absent fields are left unchanged, which is why both
    /// arguments are `Option` rather than a whole record.
    pub async fn update_template(
        &self,
        id: &str,
        name: Option<&str>,
        body: Option<&TemplateBody>,
    ) -> Result<bool> {
        let encoded = body.map(|b| {
            let mut b = b.clone();
            b.normalize();
            b.encode()
        });
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE templates
                SET name = COALESCE(?2, name), body = COALESCE(?3, body)
              WHERE id = ?1",
            params![id, name.map(str::trim), encoded],
        )?;
        Ok(n > 0)
    }

    pub async fn delete_template(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute("DELETE FROM templates WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }
}

// ── Media assets ───────────────────────────────────────────────────────────────

impl SocialStore {
    pub async fn list_media(&self, workspace_id: &str) -> Result<Vec<MediaAsset>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_MEDIA} FROM media_assets WHERE workspace_id = ?1
              ORDER BY created_at DESC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![workspace_id], row_to_media)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Register a local file in the workspace library, returning the existing row
    /// when the same path is registered twice.
    ///
    /// `ON CONFLICT DO NOTHING` + a re-read, rather than the check-then-insert the
    /// upstream uses: two concurrent handlers adding the same file would both pass a
    /// pre-insert SELECT and one would then fail the UNIQUE index.
    pub async fn upsert_media(
        &self,
        workspace_id: &str,
        path: &str,
        name: &str,
        mime_type: Option<&str>,
    ) -> Result<MediaAsset> {
        let kind = MediaKind::from_mime(mime_type);
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO media_assets (id, workspace_id, kind, path, name, mime_type, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(workspace_id, path) DO NOTHING",
            params![
                new_id(ID_MEDIA),
                workspace_id,
                kind.as_str(),
                path,
                name,
                mime_type,
                now_ms()
            ],
        )?;
        let sql =
            format!("SELECT {COLS_MEDIA} FROM media_assets WHERE workspace_id = ?1 AND path = ?2");
        Ok(conn.query_row(&sql, params![workspace_id, path], row_to_media)?)
    }

    pub async fn delete_media(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        // Only the library row goes; the user's file on disk is never ours to
        // delete — `path` is a reference, not a copy.
        let n = conn.execute("DELETE FROM media_assets WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }
}

// ── Inbox ──────────────────────────────────────────────────────────────────────

/// Filters for `GET /inbox`. All optional and ANDed.
#[derive(Debug, Clone, Default)]
pub struct InboxFilter {
    pub account_id: Option<String>,
    pub kind: Option<InboxKind>,
    pub unread_only: bool,
    pub unreplied_only: bool,
}

impl SocialStore {
    pub async fn list_inbox(
        &self,
        workspace_id: &str,
        filter: &InboxFilter,
        limit: usize,
    ) -> Result<Vec<InboxItem>> {
        let conn = self.conn.lock().await;
        let mut sql = format!("SELECT {COLS_INBOX} FROM inbox_items WHERE workspace_id = ?1");
        let mut binds: Vec<String> = vec![workspace_id.to_string()];
        if let Some(account) = &filter.account_id {
            binds.push(account.clone());
            sql.push_str(&format!(" AND social_account_id = ?{}", binds.len()));
        }
        if let Some(kind) = filter.kind {
            binds.push(kind.as_str().to_string());
            sql.push_str(&format!(" AND kind = ?{}", binds.len()));
        }
        if filter.unread_only {
            sql.push_str(" AND read = 0");
        }
        if filter.unreplied_only {
            sql.push_str(" AND replied = 0");
        }
        binds.push(limit.to_string());
        sql.push_str(&format!(
            " ORDER BY received_at DESC LIMIT ?{}",
            binds.len()
        ));
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(binds.iter()), row_to_inbox)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub async fn get_inbox_item(&self, id: &str) -> Result<Option<InboxItem>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_INBOX} FROM inbox_items WHERE id = ?1");
        Ok(conn.query_row(&sql, params![id], row_to_inbox).optional()?)
    }

    /// Ingest one item from a provider poll. Returns `true` when the row was NEW.
    ///
    /// `INSERT OR IGNORE` against the `(workspace_id, social_account_id, external_id)`
    /// UNIQUE index, with `changes()` as the "was this new" signal — so a refresh
    /// that re-reads the same 50 comments inserts nothing and reports 0 new.
    /// Deliberately does NOT update existing rows: local `read`/`replied` state must
    /// survive a re-poll.
    pub async fn ingest_inbox_item(&self, item: &InboxItem) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "INSERT OR IGNORE INTO inbox_items
               (id, workspace_id, social_account_id, platform, kind, author, text,
                permalink, external_id, received_at, replied, read)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, 0)",
            params![
                item.id,
                item.workspace_id,
                item.social_account_id,
                item.platform.as_str(),
                item.kind.as_str(),
                item.author,
                item.text,
                item.permalink,
                item.external_id,
                item.received_at
            ],
        )?;
        Ok(n > 0)
    }

    pub async fn mark_inbox_replied(&self, id: &str, replied: bool) -> Result<bool> {
        let conn = self.conn.lock().await;
        // Replying implies reading it — leaving a replied item unread would be a
        // state no user could produce.
        let n = conn.execute(
            "UPDATE inbox_items SET replied = ?2, read = CASE WHEN ?2 = 1 THEN 1 ELSE read END
              WHERE id = ?1",
            params![id, i64::from(replied)],
        )?;
        Ok(n > 0)
    }

    pub async fn mark_inbox_read(&self, id: &str, read: bool) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "UPDATE inbox_items SET read = ?2 WHERE id = ?1",
            params![id, i64::from(read)],
        )?;
        Ok(n > 0)
    }
}

// ── Activity ───────────────────────────────────────────────────────────────────

impl SocialStore {
    pub async fn list_activity(
        &self,
        workspace_id: &str,
        limit: usize,
    ) -> Result<Vec<ActivityItem>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_ACTIVITY} FROM activity_items WHERE workspace_id = ?1
              ORDER BY published_at DESC, engagement_fetched_at DESC LIMIT ?2"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![workspace_id, limit as i64], row_to_activity)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Upsert a published post's latest engagement snapshot.
    ///
    /// The COALESCE split is the whole point: **counts are overwritten
    /// unconditionally** (they are the fresh reading), while **permalink / text /
    /// published_at are only filled in when the incoming row actually has them**. A
    /// metrics-only refresh — which is most refreshes — carries no text and would
    /// otherwise blank the metadata we already know.
    pub async fn upsert_activity(&self, item: &ActivityItem) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO activity_items
               (id, workspace_id, social_account_id, platform, post_remote_id, permalink,
                text, likes, comments, shares, views, engagement_fetched_at, published_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT(workspace_id, social_account_id, post_remote_id) DO UPDATE SET
               platform              = excluded.platform,
               permalink             = COALESCE(excluded.permalink, activity_items.permalink),
               text                  = COALESCE(excluded.text, activity_items.text),
               likes                 = excluded.likes,
               comments              = excluded.comments,
               shares                = excluded.shares,
               views                 = excluded.views,
               engagement_fetched_at = excluded.engagement_fetched_at,
               published_at          = COALESCE(excluded.published_at, activity_items.published_at)",
            params![
                item.id,
                item.workspace_id,
                item.social_account_id,
                item.platform.as_str(),
                item.post_remote_id,
                item.permalink,
                item.text,
                item.likes as i64,
                item.comments as i64,
                item.shares as i64,
                item.views as i64,
                item.engagement_fetched_at,
                item.published_at
            ],
        )?;
        Ok(())
    }
}

// ── Settings ───────────────────────────────────────────────────────────────────

impl SocialStore {
    /// Read a workspace's settings, falling back to [`SocialSettings::default`] when
    /// none were ever written (the normal case) or when the stored blob no longer
    /// parses (a downgrade). Never fails: settings that cannot be read must not take
    /// the app down with them.
    pub async fn get_settings(&self, workspace_id: &str) -> Result<SocialSettings> {
        let conn = self.conn.lock().await;
        let json: Option<String> = conn
            .query_row(
                "SELECT json FROM settings WHERE workspace_id = ?1",
                params![workspace_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(json
            .and_then(|j| serde_json::from_str(&j).ok())
            .unwrap_or_default())
    }

    pub async fn put_settings(
        &self,
        workspace_id: &str,
        settings: &SocialSettings,
    ) -> Result<SocialSettings> {
        let json = serde_json::to_string(settings)?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO settings (workspace_id, json, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id) DO UPDATE SET json = excluded.json,
                                                     updated_at = excluded.updated_at",
            params![workspace_id, json, now_ms()],
        )?;
        Ok(settings.clone())
    }
}

// ── Appends for the inbox / analytics / templates / settings module ────────────
//
// Kept in their own block at the end rather than spliced into the sections above, so
// the addition is one contiguous, reviewable diff against a file several agents share.
// Every one of these is a narrow read or an idempotent write that an existing helper
// could not express.

impl SocialStore {
    /// Read a RAW `settings` blob by key.
    ///
    /// [`Self::get_settings`] deserializes into [`SocialSettings`] and falls back to
    /// the default on a parse failure — correct for a workspace row, and exactly wrong
    /// for the two non-workspace blobs that also live in this table (the node-scoped
    /// settings under `__node__`, and the per-workspace template seed markers under
    /// `__seed__:<id>`), which have different shapes and would silently decode to a
    /// default `SocialSettings`. Those callers need the bytes.
    pub async fn get_settings_blob(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().await;
        Ok(conn
            .query_row(
                "SELECT json FROM settings WHERE workspace_id = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Write a RAW `settings` blob by key. Upsert, matching [`Self::put_settings`].
    pub async fn put_settings_blob(&self, key: &str, json: &str) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO settings (workspace_id, json, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id) DO UPDATE SET json = excluded.json,
                                                     updated_at = excluded.updated_at",
            params![key, json, now_ms()],
        )?;
        Ok(())
    }

    /// Insert a template at a CALLER-CHOSEN id, ignoring a collision. Returns whether
    /// a row was created.
    ///
    /// [`Self::create_template`] mints a random id, which is right for user-authored
    /// templates and useless for a seed: the whole point of a starter set is that its
    /// ids are deterministic, so seeding twice converges on one row per built-in
    /// instead of duplicating the set on every call.
    pub async fn insert_seed_template(
        &self,
        id: &str,
        workspace_id: &str,
        name: &str,
        body: &TemplateBody,
    ) -> Result<bool> {
        let mut body = body.clone();
        body.normalize();
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "INSERT OR IGNORE INTO templates (id, workspace_id, name, body, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, workspace_id, name.trim(), body.encode(), now_ms()],
        )?;
        Ok(n > 0)
    }

    /// One target by id.
    ///
    /// [`Self::list_targets`] is keyed by POST, but an engagement refresh starts from
    /// a `post_history` row, which knows only its `post_target_id` — so recovering the
    /// platform and account for that history entry has no other route.
    pub async fn get_target(&self, id: &str) -> Result<Option<PostTarget>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {COLS_TARGET} FROM post_targets WHERE id = ?1");
        Ok(conn
            .query_row(&sql, params![id], row_to_target)
            .optional()?)
    }

    /// One activity snapshot by its dedupe key, so a refresh can read back the MERGED
    /// row [`Self::upsert_activity`] produced rather than echoing what it sent — the
    /// stored row carries the COALESCE'd metadata the metrics-only write did not have.
    pub async fn get_activity_by_remote(
        &self,
        workspace_id: &str,
        social_account_id: &str,
        post_remote_id: &str,
    ) -> Result<Option<ActivityItem>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {COLS_ACTIVITY} FROM activity_items
              WHERE workspace_id = ?1 AND social_account_id = ?2 AND post_remote_id = ?3"
        );
        Ok(conn
            .query_row(
                &sql,
                params![workspace_id, social_account_id, post_remote_id],
                row_to_activity,
            )
            .optional()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> SocialStore {
        SocialStore::open_in_memory().expect("in-memory store")
    }

    /// The one test that actually EXECUTES the DDL. `cargo check` cannot: a typo in
    /// `V1_DDL` is a string literal and compiles perfectly, then panics on the first
    /// real open. Run this before anything else in this crate.
    #[tokio::test]
    async fn migrations_apply_on_a_fresh_db_and_seed_the_default_workspace() {
        let store = store().await;
        let workspaces = store.list_workspaces().await.unwrap();
        assert_eq!(workspaces.len(), 1);
        assert_eq!(workspaces[0].id, DEFAULT_WORKSPACE_ID);
        assert_eq!(workspaces[0].name, DEFAULT_WORKSPACE_NAME);
    }

    /// The UPGRADE path, which the fresh-DB test above cannot reach.
    ///
    /// A database written before `idx_post_targets_account` existed may already hold
    /// duplicate legs — that is the whole reason this app double-published. Creating
    /// the unique index over them fails, and because `migrate` runs on every open, a
    /// failure there is not a one-time error: the sidecar would refuse to boot,
    /// forever, on exactly the databases the fix is meant to repair. So `V2_DDL`
    /// dedupes first, and the ordering of those DELETEs is load-bearing — the history
    /// sweep reads `post_targets` and must therefore run BEFORE the targets are
    /// removed, or it would find nothing to clean up and orphan the rows.
    ///
    /// Asserted rather than reasoned about, for the same reason
    /// `migrations_apply_on_a_fresh_db` exists: a wrong DDL string compiles perfectly.
    #[tokio::test]
    async fn migrating_a_v1_db_with_duplicate_legs_dedupes_instead_of_failing() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(V1_DDL).unwrap();
        conn.pragma_update(None, "user_version", 1).unwrap();

        // Two legs of one post against one account — the shape `create_post` used to
        // write, and the shape the index must not choke on.
        conn.execute(
            "INSERT INTO scheduled_posts
               (id, workspace_id, draft_id, scheduled_for, status, created_at)
             VALUES ('sp_1', 'default', NULL, 0, 'scheduled', 0)",
            [],
        )
        .unwrap();
        for target in ["pt_first", "pt_second"] {
            conn.execute(
                "INSERT INTO post_targets
                   (id, scheduled_post_id, social_account_id, platform, variant_body,
                    status, attempts, next_attempt_at, claimed_at)
                 VALUES (?1, 'sp_1', 'acc_x', 'x', NULL, 'pending', 0, 0, NULL)",
                params![target],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO post_history
                   (id, post_target_id, status, remote_url, remote_id, error, published_at)
                 VALUES (?1, ?2, 'published', NULL, ?3, NULL, 0)",
                params![format!("ph_{target}"), target, format!("remote_{target}")],
            )
            .unwrap();
        }
        // A leg on a DIFFERENT account, which must survive untouched.
        conn.execute(
            "INSERT INTO post_targets
               (id, scheduled_post_id, social_account_id, platform, variant_body,
                status, attempts, next_attempt_at, claimed_at)
             VALUES ('pt_other', 'sp_1', 'acc_y', 'bluesky', NULL, 'pending', 0, 0, NULL)",
            [],
        )
        .unwrap();

        SocialStore::migrate(&conn).expect("the v1 -> v2 upgrade must not fail on dupes");

        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // The lowest-rowid leg survives — the one whose history, if any, was written
        // first — and its sibling is gone.
        let surviving: Vec<String> = conn
            .prepare("SELECT id FROM post_targets ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(surviving, vec!["pt_first", "pt_other"]);

        // The dropped leg's history went with it; the kept leg's did not.
        let history: Vec<String> = conn
            .prepare("SELECT post_target_id FROM post_history ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(history, vec!["pt_first"]);

        // And the index is now actually in force.
        assert!(conn
            .execute(
                "INSERT INTO post_targets
                   (id, scheduled_post_id, social_account_id, platform, variant_body,
                    status, attempts, next_attempt_at, claimed_at)
                 VALUES ('pt_third', 'sp_1', 'acc_x', 'x', NULL, 'pending', 0, 0, NULL)",
                [],
            )
            .is_err());

        // Idempotent: a second open of an already-migrated DB is a no-op, not a
        // re-run that would trip over its own index.
        SocialStore::migrate(&conn).expect("re-running the ladder is a no-op");
    }

    #[tokio::test]
    async fn every_list_query_parses_against_the_real_schema() {
        // Exercises every SELECT's column list against the DDL. An index-vs-decoder
        // mismatch shows up here rather than in production.
        let s = store().await;
        let ws = DEFAULT_WORKSPACE_ID;
        assert!(s.list_accounts(ws).await.unwrap().is_empty());
        assert!(s.list_drafts(ws).await.unwrap().is_empty());
        assert!(s.list_scheduled_posts(ws, &[]).await.unwrap().is_empty());
        assert!(s
            .list_scheduled_posts(ws, &[PostStatus::Scheduled])
            .await
            .unwrap()
            .is_empty());
        assert!(s
            .list_posts_in_range(ws, 0, i64::MAX)
            .await
            .unwrap()
            .is_empty());
        assert!(s.list_queue(ws, 50).await.unwrap().is_empty());
        assert!(s.list_history(ws, 50).await.unwrap().is_empty());
        assert!(s.list_templates(ws).await.unwrap().is_empty());
        assert!(s.list_media(ws).await.unwrap().is_empty());
        assert!(s
            .list_inbox(ws, &InboxFilter::default(), 50)
            .await
            .unwrap()
            .is_empty());
        assert!(s.list_activity(ws, 50).await.unwrap().is_empty());
        assert!(s.list_due_posts(10).await.unwrap().is_empty());
        assert_eq!(s.get_settings(ws).await.unwrap(), SocialSettings::default());
    }

    #[tokio::test]
    async fn scheduling_writes_post_and_targets_atomically() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@me", None)
            .await
            .unwrap();
        let draft = s
            .create_draft(DEFAULT_WORKSPACE_ID, &DraftBody::empty())
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                Some(&draft.id),
                1_000,
                &[NewTarget {
                    social_account_id: account.id.clone(),
                    platform: Platform::Bluesky,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        assert_eq!(post.targets.len(), 1);
        let fetched = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(fetched.targets.len(), 1);
        assert_eq!(fetched.status, PostStatus::Scheduled);

        // Zero targets is rejected, not silently allowed to settle as `failed`.
        assert!(s
            .create_scheduled_post(DEFAULT_WORKSPACE_ID, None, 1_000, &[])
            .await
            .is_err());
    }

    /// The storage-layer floor under `api::create_post`'s dedupe.
    ///
    /// Two legs for one account cannot be caught by the runner: its durable
    /// already-published guard reads history by TARGET id, so the sibling row is
    /// invisible to it and publishes the same content to the same account again —
    /// under an idempotency key the broker is not documented to honour. The unique
    /// index is what makes the guard's key and the idempotency key agree.
    #[tokio::test]
    async fn one_account_cannot_hold_two_legs_of_the_same_post() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let target = || NewTarget {
            social_account_id: account.id.clone(),
            platform: Platform::X,
            variant_body: None,
        };
        assert!(s
            .create_scheduled_post(DEFAULT_WORKSPACE_ID, None, 1_000, &[target(), target()])
            .await
            .is_err());

        // The write is one transaction, so the rejected post left nothing behind.
        assert!(s
            .list_scheduled_posts(DEFAULT_WORKSPACE_ID, &[])
            .await
            .unwrap()
            .is_empty());

        // The SAME account in a DIFFERENT post is fine — the index is scoped to the
        // post, not global.
        for _ in 0..2 {
            s.create_scheduled_post(DEFAULT_WORKSPACE_ID, None, 1_000, &[target()])
                .await
                .unwrap();
        }
    }

    /// A rejected reschedule must not move the read model.
    ///
    /// `GET /queue` sorts and counts down on `COALESCE(t.next_attempt_at, …)`, but
    /// `next_attempt_at` is not in the runner's predicate — so a targets update that
    /// ran while the guarded post update matched nothing produced a queue that
    /// counted down to a time nothing would ever honour, off the back of a request
    /// that correctly 409'd.
    #[tokio::test]
    async fn a_rejected_reschedule_leaves_the_targets_untouched() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                1_000,
                &[NewTarget {
                    social_account_id: account.id.clone(),
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();

        // While still `scheduled`, a reschedule moves both.
        assert!(s.reschedule_post(&post.id, 5_000).await.unwrap());
        let moved = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(moved.scheduled_for, 5_000);
        assert_eq!(moved.targets[0].next_attempt_at, Some(5_000));

        // Once it is publishing, the guard rejects — and nothing moves.
        s.claim_due_posts(9_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post.id).await.unwrap();
        assert!(!s.reschedule_post(&post.id, 99_000).await.unwrap());
        let after = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(after.scheduled_for, 5_000);
        assert_eq!(after.targets[0].next_attempt_at, Some(5_000));
    }

    /// Removing an account stops it receiving posts that were already queued.
    ///
    /// The publish path tolerates a missing account row on purpose (it rebuilds the
    /// provider account from the target's denormalized platform), and the Bluesky
    /// adapter authenticates from node settings rather than the account's
    /// `external_id` — so a still-`pending` leg would have published to an account
    /// the user believes they removed. History and settled legs are untouched.
    #[tokio::test]
    async fn deleting_an_account_cancels_its_pending_legs_but_keeps_history() {
        let s = store().await;
        let gone = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@gone", None)
            .await
            .unwrap();
        let kept = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@kept", None)
            .await
            .unwrap();

        let published = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[NewTarget {
                    social_account_id: gone.id.clone(),
                    platform: Platform::Bluesky,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();
        s.insert_history(
            &published.targets[0].id,
            HistoryStatus::Published,
            Some("remote_1"),
            None,
            None,
        )
        .await
        .unwrap();
        s.settle_target(&published.targets[0].id, TargetStatus::Published, 1, None)
            .await
            .unwrap();

        let queued = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[
                    NewTarget {
                        social_account_id: gone.id.clone(),
                        platform: Platform::Bluesky,
                        variant_body: None,
                    },
                    NewTarget {
                        social_account_id: kept.id.clone(),
                        platform: Platform::X,
                        variant_body: None,
                    },
                ],
            )
            .await
            .unwrap();

        assert!(s.delete_account(&gone.id).await.unwrap());

        let after = s.get_scheduled_post(&queued.id).await.unwrap().unwrap();
        assert_eq!(after.targets[0].status, TargetStatus::Cancelled);
        assert_eq!(after.targets[0].next_attempt_at, None);
        // The other account's leg is untouched — one removal is not a cancellation
        // of the whole post.
        assert_eq!(after.targets[1].status, TargetStatus::Pending);

        // The published leg and its history survive, which is the whole reason this
        // delete does not cascade.
        let settled = s.get_scheduled_post(&published.id).await.unwrap().unwrap();
        assert_eq!(settled.targets[0].status, TargetStatus::Published);
        assert_eq!(
            s.list_history_for_target(&published.targets[0].id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn the_due_claim_is_exclusive() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                1_000,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::X,
                    variant_body: None,
                }],
            )
            .await
            .unwrap();

        // First sweep claims it; a second sweep over the same window gets nothing.
        let first = s.claim_due_posts(2_000, 10).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].id, post.id);
        assert!(s.claim_due_posts(2_000, 10).await.unwrap().is_empty());

        // Same for the runner's claim.
        assert!(s.claim_post_for_publishing(&post.id).await.unwrap());
        assert!(!s.claim_post_for_publishing(&post.id).await.unwrap());

        // And settle is guarded on `publishing`.
        assert!(s
            .settle_post(&post.id, PostStatus::Published)
            .await
            .unwrap());
        assert!(!s.settle_post(&post.id, PostStatus::Failed).await.unwrap());
    }

    #[tokio::test]
    async fn cancel_and_reschedule_are_guarded_by_state() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let new_post = |id: String| NewTarget {
            social_account_id: id,
            platform: Platform::X,
            variant_body: None,
        };
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                1_000,
                &[new_post(account.id.clone())],
            )
            .await
            .unwrap();

        assert!(s.reschedule_post(&post.id, 5_000).await.unwrap());
        assert!(s.cancel_post(&post.id).await.unwrap());
        // Already cancelled: both are no-ops rather than corrupting the state.
        assert!(!s.cancel_post(&post.id).await.unwrap());
        assert!(!s.reschedule_post(&post.id, 9_000).await.unwrap());
        let fetched = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, PostStatus::Cancelled);
        assert_eq!(fetched.targets[0].status, TargetStatus::Cancelled);
    }

    /// Targets must read back in the order they were WRITTEN, on every path.
    ///
    /// This is a regression test for `ORDER BY id ASC` over a v4 UUID primary key,
    /// which is a random permutation, not an order. It made the fan-out order the UI
    /// shows disagree with the order the runner publishes in, and made
    /// `the_reaper_leaves_a_post_that_still_has_a_live_target_alone` fail about half
    /// the time — the two-target coin flip.
    ///
    /// Six targets, so a regression cannot pass by luck: random ordering would agree
    /// with insertion order once in 720 runs rather than once in two. All three read
    /// paths are asserted because they were three separate queries, and fixing only
    /// the one a failing test happened to touch would leave the other two shuffling.
    #[tokio::test]
    async fn targets_read_back_in_insertion_order_on_every_path() {
        let s = store().await;
        // Distinct platforms, so the assert names WHICH row moved rather than just
        // reporting that some opaque id is in the wrong slot.
        let platforms = [
            Platform::X,
            Platform::Bluesky,
            Platform::Reddit,
            Platform::Linkedin,
            Platform::Threads,
            Platform::Facebook,
        ];
        let mut targets = Vec::new();
        for platform in platforms {
            let account = s
                .create_account(DEFAULT_WORKSPACE_ID, platform, "@me", None)
                .await
                .unwrap();
            targets.push(NewTarget {
                social_account_id: account.id,
                platform,
                variant_body: None,
            });
        }
        let post = s
            .create_scheduled_post(DEFAULT_WORKSPACE_ID, None, 1_000, &targets)
            .await
            .unwrap();
        // The returned value is built in memory from the caller's slice, so it is the
        // insertion order by construction — and therefore the baseline every read
        // path has to reproduce.
        assert_eq!(
            post.targets.iter().map(|t| t.platform).collect::<Vec<_>>(),
            platforms.to_vec(),
            "the created post must carry the caller's order"
        );

        // 1. `attach_targets`, via the single-post read.
        let fetched = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(
            fetched
                .targets
                .iter()
                .map(|t| t.platform)
                .collect::<Vec<_>>(),
            platforms.to_vec(),
            "get_scheduled_post must not reorder the fan-out"
        );

        // 2. `list_targets`, the standalone query.
        let listed = s.list_targets(&post.id).await.unwrap();
        assert_eq!(
            listed.iter().map(|t| t.platform).collect::<Vec<_>>(),
            platforms.to_vec(),
            "list_targets must not reorder the fan-out"
        );

        // 3. `list_queue`. Every target of a post shares one `next_attempt_at`, so
        //    the primary sort key ties for all six rows and the tiebreaker is the
        //    ONLY thing deciding their order here.
        let queued = s.list_queue(DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert_eq!(
            queued.iter().map(|e| e.target.platform).collect::<Vec<_>>(),
            platforms.to_vec(),
            "the queue view must agree with the order the runner will publish in"
        );
    }

    #[tokio::test]
    async fn expired_leases_are_reaped_back_to_the_queue() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
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
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post.id).await.unwrap();
        let target_id = post.targets[0].id.clone();
        assert!(s.claim_target(&target_id, 1_000).await.unwrap());
        // A second worker cannot take a live claim.
        assert!(!s.claim_target(&target_id, 1_000).await.unwrap());

        // Lease expires: the target returns to `pending` and the post to `due`.
        let reaped = s.reap_expired_claims(5_000, 5_000).await.unwrap();
        assert_eq!(reaped, vec![target_id]);
        let fetched = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, PostStatus::Due);
        assert_eq!(fetched.targets[0].status, TargetStatus::Pending);
    }

    /// The regression the single-target test above cannot reach.
    ///
    /// Targets publish sequentially, so a healthy multi-target post is normally
    /// `t1=publishing, t2=pending`. An unrelated expired lease must NOT drag that
    /// post back to `due` under its live runner — doing so lets a second runner
    /// claim it and publish the same content twice.
    #[tokio::test]
    async fn the_reaper_leaves_a_post_that_still_has_a_live_target_alone() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        // Two DISTINCT accounts: one post may hold at most one leg per account
        // (`idx_post_targets_account`), because a second leg for the same account is
        // invisible to the runner's per-target already-published guard.
        let other = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@you", None)
            .await
            .unwrap();
        let target = |id: &str| NewTarget {
            social_account_id: id.to_string(),
            platform: Platform::X,
            variant_body: None,
        };
        // Post A: two targets, mid-flight and healthy.
        let a = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[target(&account.id), target(&other.id)],
            )
            .await
            .unwrap();
        // Post B: one target, claimed long ago — its lease is what expires.
        let b = s
            .create_scheduled_post(DEFAULT_WORKSPACE_ID, None, 0, &[target(&account.id)])
            .await
            .unwrap();

        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&a.id).await.unwrap();
        s.claim_post_for_publishing(&b.id).await.unwrap();
        // A's first target is claimed RECENTLY; its second is still pending.
        s.claim_target(&a.targets[0].id, 10_000).await.unwrap();
        // B's only target was claimed long ago.
        s.claim_target(&b.targets[0].id, 1_000).await.unwrap();

        let reaped = s.reap_expired_claims(5_000, 5_000).await.unwrap();
        assert_eq!(
            reaped,
            vec![b.targets[0].id.clone()],
            "only B's lease expired"
        );

        let a_after = s.get_scheduled_post(&a.id).await.unwrap().unwrap();
        assert_eq!(
            a_after.status,
            PostStatus::Publishing,
            "a post with a live target must stay claimed by its runner"
        );
        assert_eq!(a_after.targets[0].status, TargetStatus::Publishing);
        assert_eq!(a_after.targets[1].status, TargetStatus::Pending);

        let b_after = s.get_scheduled_post(&b.id).await.unwrap().unwrap();
        assert_eq!(b_after.status, PostStatus::Due);
        assert_eq!(b_after.targets[0].status, TargetStatus::Pending);
    }

    #[tokio::test]
    async fn retry_requeues_failed_targets_but_never_published_ones() {
        let s = store().await;
        let a = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@a", None)
            .await
            .unwrap();
        let b = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Bluesky, "@b", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[
                    NewTarget {
                        social_account_id: a.id,
                        platform: Platform::X,
                        variant_body: None,
                    },
                    NewTarget {
                        social_account_id: b.id,
                        platform: Platform::Bluesky,
                        variant_body: None,
                    },
                ],
            )
            .await
            .unwrap();
        s.claim_due_posts(1_000, 10).await.unwrap();
        s.claim_post_for_publishing(&post.id).await.unwrap();
        s.settle_target(&post.targets[0].id, TargetStatus::Published, 1, None)
            .await
            .unwrap();
        s.settle_target(&post.targets[1].id, TargetStatus::Failed, 3, None)
            .await
            .unwrap();
        s.settle_post(&post.id, PostStatus::Partial).await.unwrap();

        assert!(s.retry_post(&post.id, 9_000).await.unwrap());
        let fetched = s.get_scheduled_post(&post.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, PostStatus::Due);
        assert_eq!(fetched.targets[0].status, TargetStatus::Published);
        assert_eq!(fetched.targets[1].status, TargetStatus::Pending);
        assert_eq!(fetched.targets[1].attempts, 0);
    }

    #[tokio::test]
    async fn inbox_ingest_dedupes_and_preserves_local_state() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        let item = InboxItem {
            id: new_id(ID_INBOX),
            workspace_id: DEFAULT_WORKSPACE_ID.into(),
            social_account_id: account.id.clone(),
            platform: Platform::X,
            kind: InboxKind::Comment,
            author: "someone".into(),
            text: "hi".into(),
            permalink: None,
            external_id: "remote-1".into(),
            received_at: 100,
            replied: false,
            read: false,
        };
        assert!(s.ingest_inbox_item(&item).await.unwrap());
        s.mark_inbox_read(&item.id, true).await.unwrap();

        // Re-poll: same external id, new local id → ignored, read state survives.
        let again = InboxItem {
            id: new_id(ID_INBOX),
            ..item.clone()
        };
        assert!(!s.ingest_inbox_item(&again).await.unwrap());
        let stored = s.get_inbox_item(&item.id).await.unwrap().unwrap();
        assert!(stored.read);
        assert_eq!(
            s.list_inbox(DEFAULT_WORKSPACE_ID, &InboxFilter::default(), 50)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn activity_upsert_refreshes_counts_without_clobbering_metadata() {
        let s = store().await;
        let base = ActivityItem {
            id: new_id(ID_ACTIVITY),
            workspace_id: DEFAULT_WORKSPACE_ID.into(),
            social_account_id: "acc_1".into(),
            platform: Platform::Bluesky,
            post_remote_id: "at://x".into(),
            permalink: Some("https://bsky.app/p/1".into()),
            text: Some("hello".into()),
            likes: 1,
            comments: 0,
            shares: 0,
            views: 0,
            engagement_fetched_at: Some(10),
            published_at: Some(5),
        };
        s.upsert_activity(&base).await.unwrap();
        // A metrics-only refresh: no permalink, no text, no published_at.
        s.upsert_activity(&ActivityItem {
            id: new_id(ID_ACTIVITY),
            permalink: None,
            text: None,
            published_at: None,
            likes: 42,
            engagement_fetched_at: Some(20),
            ..base.clone()
        })
        .await
        .unwrap();
        let items = s.list_activity(DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert_eq!(items.len(), 1, "dedupe key collapsed the two writes");
        assert_eq!(items[0].likes, 42, "counts are overwritten");
        assert_eq!(items[0].text.as_deref(), Some("hello"), "metadata survives");
        assert_eq!(items[0].published_at, Some(5));
    }

    #[tokio::test]
    async fn media_upsert_is_idempotent_on_path() {
        let s = store().await;
        let a = s
            .upsert_media(
                DEFAULT_WORKSPACE_ID,
                "/tmp/a.png",
                "a.png",
                Some("image/png"),
            )
            .await
            .unwrap();
        let b = s
            .upsert_media(
                DEFAULT_WORKSPACE_ID,
                "/tmp/a.png",
                "a.png",
                Some("image/png"),
            )
            .await
            .unwrap();
        assert_eq!(a.id, b.id);
        assert_eq!(a.kind, MediaKind::Image);
        assert_eq!(s.list_media(DEFAULT_WORKSPACE_ID).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn workspace_delete_cascades_and_refuses_the_default() {
        let s = store().await;
        let ws = s.create_workspace("Second").await.unwrap();
        let account = s
            .create_account(&ws.id, Platform::X, "@me", None)
            .await
            .unwrap();
        let post = s
            .create_scheduled_post(
                &ws.id,
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
        s.insert_history(
            &post.targets[0].id,
            HistoryStatus::Published,
            Some("r1"),
            None,
            None,
        )
        .await
        .unwrap();

        assert!(s.delete_workspace(&ws.id).await.unwrap());
        assert!(s.get_scheduled_post(&post.id).await.unwrap().is_none());
        assert!(s.list_targets(&post.id).await.unwrap().is_empty());
        assert!(s
            .list_history_for_target(&post.targets[0].id)
            .await
            .unwrap()
            .is_empty());
        // The seeded workspace is structural, not user data.
        assert!(s.delete_workspace(DEFAULT_WORKSPACE_ID).await.is_err());
    }

    #[tokio::test]
    async fn queue_projects_pending_targets_with_a_never_null_next_attempt() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::X, "@me", None)
            .await
            .unwrap();
        s.create_scheduled_post(
            DEFAULT_WORKSPACE_ID,
            None,
            7_000,
            &[NewTarget {
                social_account_id: account.id,
                platform: Platform::X,
                variant_body: None,
            }],
        )
        .await
        .unwrap();
        let queue = s.list_queue(DEFAULT_WORKSPACE_ID, 50).await.unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].next_attempt_at, 7_000);
        assert_eq!(queue[0].post_status, PostStatus::Scheduled);
    }

    #[tokio::test]
    async fn settings_round_trip_and_default_when_unset() {
        let s = store().await;
        assert_eq!(
            s.get_settings(DEFAULT_WORKSPACE_ID).await.unwrap(),
            SocialSettings::default()
        );
        let mut settings = SocialSettings::default();
        settings.max_attempts = 5;
        settings.timezone = "Europe/Berlin".into();
        s.put_settings(DEFAULT_WORKSPACE_ID, &settings)
            .await
            .unwrap();
        assert_eq!(
            s.get_settings(DEFAULT_WORKSPACE_ID).await.unwrap(),
            settings
        );
    }

    #[tokio::test]
    async fn variant_bodies_round_trip_as_full_draft_bodies() {
        let s = store().await;
        let account = s
            .create_account(DEFAULT_WORKSPACE_ID, Platform::Linkedin, "@me", None)
            .await
            .unwrap();
        let mut variant = DraftBody::empty();
        variant.segments = vec![
            PostSegment {
                text: "slide one".into(),
                media: vec![MediaRef {
                    path: "/tmp/a.png".into(),
                    mime_type: "image/png".into(),
                    name: "a.png".into(),
                }],
            },
            PostSegment {
                text: "slide two".into(),
                media: vec![],
            },
        ];
        let post = s
            .create_scheduled_post(
                DEFAULT_WORKSPACE_ID,
                None,
                0,
                &[NewTarget {
                    social_account_id: account.id,
                    platform: Platform::Linkedin,
                    variant_body: Some(variant),
                }],
            )
            .await
            .unwrap();
        let targets = s.list_targets(&post.id).await.unwrap();
        let body = targets[0].variant_body.as_ref().expect("variant survived");
        // The whole point of storing a body rather than plain text: media and thread
        // structure survive a per-target override.
        assert_eq!(body.segments.len(), 2);
        assert_eq!(body.segments[0].media.len(), 1);
    }
}
