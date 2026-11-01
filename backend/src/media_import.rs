//! Upload sessions that copy a file INTO Outpost's own media root.
//!
//! `POST /media` only references a file that already sits beneath the media root
//! (`RYU_SOCIAL_MEDIA_ROOT`, default `media/` in the plugin data dir). Another
//! app that has the `social:crud` grant — Video Studio handing off a render, for
//! example — has no way to put a file there: it reaches Outpost only through
//! `social.request`, a JSON forwarder with a bounded body. These sessions close
//! that gap:
//!
//! 1. `POST /media/import` declares the name, mime type and exact size, and
//!    returns a session id plus the chunk size.
//! 2. `POST /media/import/:id/chunks` appends one base64 chunk at the next
//!    offset. Out-of-order or oversized chunks are refused, never reordered.
//! 3. `POST /media/import/:id/finish` checks the size (and the SHA-256, when
//!    given), moves the file to `imports/<uuid>/<name>` under the media root,
//!    optionally writes a `<stem>-metadata.txt` next to it, and adds the file to
//!    the workspace media library.
//! 4. `DELETE /media/import/:id` discards a session and its partial file.
//!
//! The file never leaves the media root, so every later attach and publish goes
//! through the same `approved_local_media_path` check as a hand-picked file.
//!
//! Disk use is bounded three ways: each file is at most [`MAX_IMPORT_BYTES`],
//! everything under `imports/` (finished files plus the declared size of open
//! sessions) is at most [`MAX_IMPORTS_TOTAL_BYTES`], and a session idle past
//! [`SESSION_IDLE`] is expired on its next use and by a background reaper, with
//! its partial file deleted. Deleting an imported file's library row deletes the
//! file too once no other row or draft refers to it ([`remove_unreferenced_import`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Raw bytes per chunk. Base64 grows it by a third, so one chunk request stays
/// far below Core's 10 MiB ext-proxy body cap and the host bridge's limits.
pub const IMPORT_CHUNK_BYTES: usize = 1024 * 1024;
/// Largest file one session may import.
pub const MAX_IMPORT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Largest `metadata.txt` written beside an import.
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
/// Total bytes `imports/` may hold: finished imports plus the declared size of
/// every open session.
pub const MAX_IMPORTS_TOTAL_BYTES: u64 = 10 * 1024 * 1024 * 1024;
/// Sessions open at once, across workspaces.
pub const MAX_OPEN_SESSIONS: usize = 8;
/// A session with no chunk for this long is discarded with its partial file.
pub const SESSION_IDLE: Duration = Duration::from_secs(15 * 60);
/// Longest file name kept (characters, after sanitizing).
const MAX_NAME_CHARS: usize = 120;

/// Media types an import may declare. Platforms take these for video and image
/// posts; anything else is refused instead of being filed as an "image".
pub const IMPORT_MIME_TYPES: [&str; 7] = [
    "video/mp4",
    "video/quicktime",
    "video/webm",
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
];

/// One open upload session.
#[derive(Debug)]
pub struct ImportSession {
    pub workspace_id: String,
    pub name: String,
    pub mime_type: String,
    pub size: u64,
    pub received: u64,
    pub partial: PathBuf,
    pub hasher: Sha256,
    pub touched: Instant,
}

/// Open sessions, shared by every handler.
#[derive(Debug, Clone)]
pub struct ImportSessions {
    inner: Arc<Mutex<HashMap<String, ImportSession>>>,
    /// How long a session may go without a chunk before it expires.
    idle: Duration,
    /// Cap on everything under `imports/`.
    quota: u64,
}

impl Default for ImportSessions {
    fn default() -> Self {
        Self::with_limits(SESSION_IDLE, MAX_IMPORTS_TOTAL_BYTES)
    }
}

/// Why a session operation was refused. Mapped to HTTP errors by the API layer.
#[derive(Debug, PartialEq, Eq)]
pub enum ImportError {
    BadRequest(String),
    NotFound,
    Conflict(String),
    Busy,
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(why) | Self::Conflict(why) => f.write_str(why),
            Self::NotFound => f.write_str("import session not found or expired"),
            Self::Busy => write!(
                f,
                "{MAX_OPEN_SESSIONS} imports are already open; finish or cancel one first"
            ),
        }
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct StartImportBody {
    /// Scope; omit for the default workspace.
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// File name, for example `launch.mp4`. Path separators are removed.
    pub name: String,
    /// One of `video/mp4`, `video/quicktime`, `video/webm`, `image/png`,
    /// `image/jpeg`, `image/gif` or `image/webp`.
    pub mime_type: String,
    /// Exact size in bytes; at most 2 GiB.
    pub size: u64,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ImportChunkBody {
    /// Must match the workspace the session was started in.
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// Byte offset of this chunk; must equal the bytes received so far.
    pub offset: u64,
    /// Base64 (standard alphabet) bytes, at most `chunk_size` once decoded.
    pub data: String,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct FinishImportBody {
    /// Must match the workspace the session was started in.
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// Lowercase hex SHA-256 of the whole file; checked when given.
    #[serde(default)]
    pub sha256: Option<String>,
    /// Text written as `<name>-metadata.txt` next to the file (title,
    /// description, chapters). At most 64 KiB.
    #[serde(default)]
    pub metadata_text: Option<String>,
}

/// A finished import: where the file now lives under the media root.
#[derive(Debug)]
pub struct FinishedImport {
    pub workspace_id: String,
    pub path: PathBuf,
    pub name: String,
    pub mime_type: String,
    pub metadata_path: Option<PathBuf>,
}

/// File name safe to place on disk: no separators, no control characters, no
/// leading dots, bounded length, and never empty.
pub fn sanitize_file_name(raw: &str) -> String {
    let cleaned: String = raw
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .map(|c| {
            if matches!(c, ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let trimmed = cleaned.trim().trim_start_matches('.').trim();
    let bounded: String = trimmed.chars().take(MAX_NAME_CHARS).collect();
    if bounded.is_empty() {
        "import".to_owned()
    } else {
        bounded
    }
}

/// Bytes held by finished imports under `root/imports` (every
/// `imports/<uuid>/` directory; the `.partial` directory is counted through the
/// open sessions instead).
async fn finished_import_bytes(root: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut dirs = match tokio::fs::read_dir(root.join("imports")).await {
        Ok(dirs) => dirs,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    while let Some(dir) = dirs.next_entry().await? {
        if dir.file_name() == ".partial" || !dir.file_type().await?.is_dir() {
            continue;
        }
        let mut files = tokio::fs::read_dir(dir.path()).await?;
        while let Some(file) = files.next_entry().await? {
            let meta = file.metadata().await?;
            if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Ok(total)
}

/// The `imports/<uuid>` directory holding `path`, when `path` is a finished
/// import directly inside one under `root`. Anything else (a hand-picked file,
/// a partial, a nested path) is `None`, so it is never deleted.
pub fn import_directory(root: &Path, path: &Path) -> Option<PathBuf> {
    let dir = path.parent()?;
    let imports = dir.parent()?;
    if imports != root.join("imports") {
        return None;
    }
    let name = dir.file_name()?.to_str()?;
    uuid::Uuid::parse_str(name).ok()?;
    path.file_name()?;
    Some(dir.to_path_buf())
}

/// Delete the import holding `path` (the file and its metadata text) when it
/// is a finished import under `root` and `still_referenced` is false. Returns
/// whether anything was removed.
pub async fn remove_unreferenced_import(
    root: &Path,
    path: &Path,
    still_referenced: bool,
) -> std::io::Result<bool> {
    if still_referenced {
        return Ok(false);
    }
    let Some(dir) = import_directory(root, path) else {
        return Ok(false);
    };
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Expire idle sessions every minute for the process lifetime, so an abandoned
/// session's partial file is removed even if no other import ever runs.
pub fn spawn_reaper(sessions: ImportSessions) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            sessions.prune_idle().await;
        }
    })
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

impl ImportSessions {
    /// Sessions that expire after `idle` and share a `quota`-byte `imports/`.
    pub fn with_limits(idle: Duration, quota: u64) -> Self {
        Self {
            inner: Arc::default(),
            idle,
            quota,
        }
    }

    /// Expire idle sessions now, deleting their partial files.
    pub async fn prune_idle(&self) {
        let mut map = self.inner.lock().await;
        self.prune(&mut map).await;
    }

    /// Drop sessions idle past the idle limit, deleting their partial files.
    async fn prune(&self, map: &mut HashMap<String, ImportSession>) {
        let stale: Vec<String> = map
            .iter()
            .filter(|(_, session)| session.touched.elapsed() > self.idle)
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            if let Some(session) = map.remove(&id) {
                let _ = tokio::fs::remove_file(&session.partial).await;
            }
        }
    }

    /// Open a session under `root`. Returns the new session id.
    pub async fn start(
        &self,
        root: &Path,
        workspace_id: &str,
        body: &StartImportBody,
    ) -> Result<String, ImportError> {
        let mime = body.mime_type.trim().to_ascii_lowercase();
        if !IMPORT_MIME_TYPES.contains(&mime.as_str()) {
            return Err(ImportError::BadRequest(format!(
                "`{mime}` cannot be imported; use one of {}",
                IMPORT_MIME_TYPES.join(", ")
            )));
        }
        if body.size == 0 || body.size > MAX_IMPORT_BYTES {
            return Err(ImportError::BadRequest(format!(
                "import size must be between 1 byte and {MAX_IMPORT_BYTES} bytes"
            )));
        }
        let name = sanitize_file_name(&body.name);
        let mut map = self.inner.lock().await;
        self.prune(&mut map).await;
        if map.len() >= MAX_OPEN_SESSIONS {
            return Err(ImportError::Busy);
        }
        let finished = finished_import_bytes(root)
            .await
            .map_err(|e| ImportError::Conflict(format!("media root is unavailable: {e}")))?;
        let reserved: u64 = map.values().map(|session| session.size).sum();
        let used = finished.saturating_add(reserved);
        if used.saturating_add(body.size) > self.quota {
            return Err(ImportError::Conflict(format!(
                "imports would exceed {} bytes ({used} in use); delete imported media from the library first",
                self.quota
            )));
        }
        let partial_dir = root.join("imports").join(".partial");
        tokio::fs::create_dir_all(&partial_dir)
            .await
            .map_err(|e| ImportError::Conflict(format!("media root is unavailable: {e}")))?;
        let id = uuid::Uuid::new_v4().to_string();
        let partial = partial_dir.join(format!("{id}.part"));
        tokio::fs::File::create(&partial)
            .await
            .map_err(|e| ImportError::Conflict(format!("cannot open the import file: {e}")))?;
        map.insert(
            id.clone(),
            ImportSession {
                workspace_id: workspace_id.to_owned(),
                name,
                mime_type: mime,
                size: body.size,
                received: 0,
                partial,
                hasher: Sha256::new(),
                touched: Instant::now(),
            },
        );
        Ok(id)
    }

    /// Append one chunk. Returns the bytes received so far.
    pub async fn append(
        &self,
        id: &str,
        workspace_id: &str,
        body: &ImportChunkBody,
    ) -> Result<u64, ImportError> {
        // Decode before taking the lock: a malformed chunk never touches state.
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(body.data.trim())
            .map_err(|_| ImportError::BadRequest("chunk `data` is not valid base64".into()))?;
        if bytes.is_empty() {
            return Err(ImportError::BadRequest("chunk `data` is empty".into()));
        }
        if bytes.len() > IMPORT_CHUNK_BYTES {
            return Err(ImportError::BadRequest(format!(
                "a chunk carries at most {IMPORT_CHUNK_BYTES} bytes"
            )));
        }
        let mut map = self.inner.lock().await;
        self.prune(&mut map).await;
        let session = map
            .get_mut(id)
            .filter(|session| session.workspace_id == workspace_id)
            .ok_or(ImportError::NotFound)?;
        if body.offset != session.received {
            return Err(ImportError::Conflict(format!(
                "chunk offset {} does not follow the {} bytes received",
                body.offset, session.received
            )));
        }
        let next = session.received + bytes.len() as u64;
        if next > session.size {
            return Err(ImportError::BadRequest(format!(
                "chunk runs past the declared {} bytes",
                session.size
            )));
        }
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&session.partial)
            .await
            .map_err(|e| ImportError::Conflict(format!("cannot write the import file: {e}")))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| ImportError::Conflict(format!("cannot write the import file: {e}")))?;
        file.flush()
            .await
            .map_err(|e| ImportError::Conflict(format!("cannot write the import file: {e}")))?;
        session.hasher.update(&bytes);
        session.received = next;
        session.touched = Instant::now();
        Ok(next)
    }

    /// Complete a session: verify it, move the file into place, and write the
    /// optional metadata text beside it. The session is consumed either way
    /// once its bytes are complete; an incomplete session stays open.
    pub async fn finish(
        &self,
        root: &Path,
        id: &str,
        workspace_id: &str,
        body: &FinishImportBody,
    ) -> Result<FinishedImport, ImportError> {
        if let Some(text) = &body.metadata_text {
            if text.len() > MAX_METADATA_BYTES {
                return Err(ImportError::BadRequest(format!(
                    "`metadata_text` is at most {MAX_METADATA_BYTES} bytes"
                )));
            }
        }
        let mut map = self.inner.lock().await;
        self.prune(&mut map).await;
        let session = map
            .get(id)
            .filter(|session| session.workspace_id == workspace_id)
            .ok_or(ImportError::NotFound)?;
        if session.received != session.size {
            return Err(ImportError::Conflict(format!(
                "received {} of {} bytes; send the remaining chunks first",
                session.received, session.size
            )));
        }
        let Some(session) = map.remove(id) else {
            return Err(ImportError::NotFound);
        };
        drop(map);
        let digest = hex(&session.hasher.finalize());
        if let Some(expected) = &body.sha256 {
            if !expected.trim().eq_ignore_ascii_case(&digest) {
                let _ = tokio::fs::remove_file(&session.partial).await;
                return Err(ImportError::BadRequest(
                    "the file's SHA-256 does not match `sha256`; the import was discarded".into(),
                ));
            }
        }
        let dir = root.join("imports").join(uuid::Uuid::new_v4().to_string());
        let placed = async {
            tokio::fs::create_dir_all(&dir).await?;
            let path = dir.join(&session.name);
            tokio::fs::rename(&session.partial, &path).await?;
            let mut metadata_path = None;
            if let Some(text) = body
                .metadata_text
                .as_deref()
                .filter(|t| !t.trim().is_empty())
            {
                let stem = Path::new(&session.name)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("import");
                let meta = dir.join(format!("{stem}-metadata.txt"));
                tokio::fs::write(&meta, text.as_bytes()).await?;
                metadata_path = Some(meta);
            }
            std::io::Result::Ok((path, metadata_path))
        }
        .await;
        match placed {
            Ok((path, metadata_path)) => Ok(FinishedImport {
                workspace_id: session.workspace_id,
                path,
                name: session.name,
                mime_type: session.mime_type,
                metadata_path,
            }),
            Err(e) => {
                // The rename may already have moved the media into `dir`; remove
                // both, or that file would count against the import quota with
                // no library row pointing at it. `dir` is a fresh UUID path.
                let _ = tokio::fs::remove_file(&session.partial).await;
                let _ = tokio::fs::remove_dir_all(&dir).await;
                Err(ImportError::Conflict(format!(
                    "cannot place the imported file: {e}"
                )))
            }
        }
    }

    /// Discard a session and its partial file. Returns whether it existed.
    pub async fn abort(&self, id: &str, workspace_id: &str) -> bool {
        let mut map = self.inner.lock().await;
        self.prune(&mut map).await;
        let owned = map
            .get(id)
            .is_some_and(|session| session.workspace_id == workspace_id);
        if !owned {
            return false;
        }
        if let Some(session) = map.remove(id) {
            let _ = tokio::fs::remove_file(&session.partial).await;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!("ryu-social-import-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn start_body(size: u64) -> StartImportBody {
        StartImportBody {
            workspace_id: None,
            name: "../../launch.mp4".into(),
            mime_type: "video/mp4".into(),
            size,
        }
    }

    fn chunk(offset: u64, bytes: &[u8]) -> ImportChunkBody {
        ImportChunkBody {
            workspace_id: None,
            offset,
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
        }
    }

    #[test]
    fn names_are_sanitized() {
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("C:\\x\\a?.mp4"), "a_.mp4");
        assert_eq!(sanitize_file_name("...hidden"), "hidden");
        assert_eq!(sanitize_file_name(""), "import");
        assert_eq!(
            sanitize_file_name(&"a".repeat(500)).chars().count(),
            MAX_NAME_CHARS
        );
    }

    #[tokio::test]
    async fn chunks_assemble_in_order_and_finish_under_the_root() {
        let root = temp_root();
        let sessions = ImportSessions::default();
        let id = sessions.start(&root, "ws", &start_body(6)).await.unwrap();
        assert_eq!(sessions.append(&id, "ws", &chunk(0, b"abc")).await, Ok(3));
        // A repeated or skipped offset is refused, never reordered.
        assert!(matches!(
            sessions.append(&id, "ws", &chunk(0, b"abc")).await,
            Err(ImportError::Conflict(_))
        ));
        // Another workspace cannot see the session.
        assert_eq!(
            sessions.append(&id, "other", &chunk(3, b"def")).await,
            Err(ImportError::NotFound)
        );
        // Finishing early keeps the session open.
        let early = FinishImportBody {
            workspace_id: None,
            sha256: None,
            metadata_text: None,
        };
        assert!(matches!(
            sessions.finish(&root, &id, "ws", &early).await,
            Err(ImportError::Conflict(_))
        ));
        assert_eq!(sessions.append(&id, "ws", &chunk(3, b"def")).await, Ok(6));
        let digest = hex(&Sha256::digest(b"abcdef"));
        let done = sessions
            .finish(
                &root,
                &id,
                "ws",
                &FinishImportBody {
                    workspace_id: None,
                    sha256: Some(digest),
                    metadata_text: Some("Title: Launch\n".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read(&done.path).unwrap(), b"abcdef");
        assert_eq!(done.name, "launch.mp4");
        assert!(done.path.starts_with(root.join("imports")));
        let meta = done.metadata_path.unwrap();
        assert_eq!(meta.file_name().unwrap(), "launch-metadata.txt");
        assert_eq!(std::fs::read_to_string(meta).unwrap(), "Title: Launch\n");
        // The session is consumed.
        assert_eq!(
            sessions.finish(&root, &id, "ws", &early).await.unwrap_err(),
            ImportError::NotFound
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A placement that fails part-way leaves nothing behind: the fresh
    /// `imports/<uuid>` directory (which would hold the already-renamed media
    /// when a later step fails) is removed with the partial file, so it cannot
    /// count against the import quota.
    #[tokio::test]
    async fn a_failed_placement_leaves_no_import_directory() {
        let root = temp_root();
        let sessions = ImportSessions::default();
        let id = sessions.start(&root, "ws", &start_body(3)).await.unwrap();
        sessions.append(&id, "ws", &chunk(0, b"abc")).await.unwrap();
        // Lose the partial file, so the rename into the new directory fails.
        let partial_dir = root.join("imports").join(".partial");
        for entry in std::fs::read_dir(&partial_dir).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
        let failed = sessions
            .finish(
                &root,
                &id,
                "ws",
                &FinishImportBody {
                    workspace_id: None,
                    sha256: None,
                    metadata_text: Some("Title: Launch\n".into()),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(failed, ImportError::Conflict(_)), "{failed:?}");
        let leftovers: Vec<_> = std::fs::read_dir(root.join("imports"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name != ".partial")
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        assert_eq!(finished_import_bytes(&root).await.unwrap(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refusals_are_explicit() {
        let root = temp_root();
        let sessions = ImportSessions::default();
        let mut text = start_body(4);
        text.mime_type = "text/html".into();
        assert!(matches!(
            sessions.start(&root, "ws", &text).await,
            Err(ImportError::BadRequest(_))
        ));
        assert!(matches!(
            sessions
                .start(&root, "ws", &start_body(MAX_IMPORT_BYTES + 1))
                .await,
            Err(ImportError::BadRequest(_))
        ));
        let id = sessions.start(&root, "ws", &start_body(2)).await.unwrap();
        assert!(matches!(
            sessions.append(&id, "ws", &chunk(0, b"abc")).await,
            Err(ImportError::BadRequest(_))
        ));
        let bad = ImportChunkBody {
            workspace_id: None,
            offset: 0,
            data: "%%%".into(),
        };
        assert!(matches!(
            sessions.append(&id, "ws", &bad).await,
            Err(ImportError::BadRequest(_))
        ));
        sessions.append(&id, "ws", &chunk(0, b"ab")).await.unwrap();
        let wrong_hash = FinishImportBody {
            workspace_id: None,
            sha256: Some("00".repeat(32)),
            metadata_text: None,
        };
        assert!(matches!(
            sessions.finish(&root, &id, "ws", &wrong_hash).await,
            Err(ImportError::BadRequest(_))
        ));
        // A mismatched hash discards the partial file.
        assert_eq!(
            std::fs::read_dir(root.join("imports").join(".partial"))
                .unwrap()
                .count(),
            0
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn sessions_are_bounded_and_abortable() {
        let root = temp_root();
        let sessions = ImportSessions::default();
        let mut ids = Vec::new();
        for _ in 0..MAX_OPEN_SESSIONS {
            ids.push(sessions.start(&root, "ws", &start_body(1)).await.unwrap());
        }
        assert_eq!(
            sessions.start(&root, "ws", &start_body(1)).await,
            Err(ImportError::Busy)
        );
        assert!(!sessions.abort(&ids[0], "other").await);
        assert!(sessions.abort(&ids[0], "ws").await);
        assert!(!sessions.abort(&ids[0], "ws").await);
        assert!(sessions.start(&root, "ws", &start_body(1)).await.is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }

    fn partial_count(root: &Path) -> usize {
        std::fs::read_dir(root.join("imports").join(".partial"))
            .map(|dir| dir.count())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn idle_sessions_expire_on_every_operation() {
        let root = temp_root();
        let sessions = ImportSessions::with_limits(Duration::from_millis(30), u64::MAX);
        let done = FinishImportBody {
            workspace_id: None,
            sha256: None,
            metadata_text: None,
        };
        // Appending to an idle session is refused and deletes its partial file.
        let id = sessions.start(&root, "ws", &start_body(2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            sessions.append(&id, "ws", &chunk(0, b"ab")).await,
            Err(ImportError::NotFound)
        );
        assert_eq!(partial_count(&root), 0);
        // So is finishing a complete session that then sat idle.
        let id = sessions.start(&root, "ws", &start_body(2)).await.unwrap();
        sessions.append(&id, "ws", &chunk(0, b"ab")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            sessions.finish(&root, &id, "ws", &done).await.unwrap_err(),
            ImportError::NotFound
        );
        assert!(!root
            .join("imports")
            .read_dir()
            .unwrap()
            .any(|entry| { entry.unwrap().file_name() != ".partial" }));
        // The reaper path removes abandoned partials without any other call.
        let id = sessions.start(&root, "ws", &start_body(2)).await.unwrap();
        assert_eq!(partial_count(&root), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        sessions.prune_idle().await;
        assert_eq!(partial_count(&root), 0);
        assert!(!sessions.abort(&id, "ws").await);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn imports_share_one_total_quota() {
        let root = temp_root();
        let sessions = ImportSessions::with_limits(SESSION_IDLE, 10);
        let none = FinishImportBody {
            workspace_id: None,
            sha256: None,
            metadata_text: None,
        };
        let id = sessions.start(&root, "ws", &start_body(6)).await.unwrap();
        // An open session's declared size counts against the quota.
        assert!(matches!(
            sessions.start(&root, "ws", &start_body(5)).await,
            Err(ImportError::Conflict(why)) if why.contains("exceed")
        ));
        sessions
            .append(&id, "ws", &chunk(0, b"abcdef"))
            .await
            .unwrap();
        let finished = sessions.finish(&root, &id, "ws", &none).await.unwrap();
        // So does a finished file, until it is removed.
        assert!(matches!(
            sessions.start(&root, "ws", &start_body(5)).await,
            Err(ImportError::Conflict(_))
        ));
        assert!(sessions.start(&root, "ws", &start_body(4)).await.is_ok());
        assert!(remove_unreferenced_import(&root, &finished.path, false)
            .await
            .unwrap());
        assert!(!finished.path.exists());
        assert!(sessions.start(&root, "ws", &start_body(5)).await.is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn only_unreferenced_finished_imports_are_removed() {
        let root = temp_root();
        let sessions = ImportSessions::default();
        let id = sessions.start(&root, "ws", &start_body(1)).await.unwrap();
        sessions.append(&id, "ws", &chunk(0, b"a")).await.unwrap();
        let done = sessions
            .finish(
                &root,
                &id,
                "ws",
                &FinishImportBody {
                    workspace_id: None,
                    sha256: None,
                    metadata_text: Some("Title\n".into()),
                },
            )
            .await
            .unwrap();
        // Still referenced by a draft or another row: kept.
        assert!(!remove_unreferenced_import(&root, &done.path, true)
            .await
            .unwrap());
        assert!(done.path.exists());
        // A hand-picked file elsewhere under the root is never ours to delete.
        let picked = root.join("picked.mp4");
        std::fs::write(&picked, b"x").unwrap();
        assert!(!remove_unreferenced_import(&root, &picked, false)
            .await
            .unwrap());
        assert!(picked.exists());
        assert_eq!(
            import_directory(&root, &root.join("imports/not-a-uuid/a.mp4")),
            None
        );
        // Unreferenced: the file and its metadata text go.
        assert!(remove_unreferenced_import(&root, &done.path, false)
            .await
            .unwrap());
        assert!(!done.path.parent().unwrap().exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
