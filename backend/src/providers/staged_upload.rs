//! Staged uploads: getting a local media file to a broker that cannot read
//! this machine's disk.
//!
//! A hosted broker (Composio) takes a file parameter as a reference to an object
//! it already holds. The broker stages that object in two steps:
//!
//! 1. A slot request, made through the Gateway-vaulted provider router
//!    (`operation: "file_upload"`). Gateway holds the broker credential, checks
//!    the request, and returns only a storage key plus a presigned, single-object
//!    write URL.
//! 2. A `PUT` of the raw bytes to that URL, made here. The URL is a short-lived
//!    write grant for one object; it is not the broker credential.
//!
//! The bytes stream from disk (never buffered whole), the declared length is
//! fixed before the request starts, and the reader is capped at that length, so
//! a file that grows mid-upload cannot send more than was declared. The PUT
//! carries the hashed digest as `Content-MD5`, so a same-size rewrite between
//! hashing and upload is refused by the storage endpoint instead of stored
//! under the wrong digest. The target is
//! screened and DNS-pinned through `ryu-egress` (HTTPS only, no private or
//! loopback addresses, no redirects).

use std::path::PathBuf;
use std::time::Duration;

use md5::{Digest, Md5};
use tokio::io::{AsyncReadExt, BufReader};

use super::types::PublishMedia;
use crate::state::approved_local_media_path;

/// Largest file staged for a broker. Matches the media-import cap, so any file
/// Video Studio can hand off can also be published.
pub const MAX_STAGED_UPLOAD_BYTES: u64 = crate::media_import::MAX_IMPORT_BYTES;

/// Read size used while hashing and streaming.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// A local file checked and measured for staging.
#[derive(Debug, Clone)]
pub struct LocalUpload {
    pub path: PathBuf,
    /// File name sent to the broker (path stripped).
    pub name: String,
    pub mime_type: String,
    pub size: u64,
    /// Lowercase hex MD5 of the bytes, which Composio uses to dedupe.
    pub md5_hex: String,
    /// The same digest, base64-encoded for the PUT's `Content-MD5` header, so
    /// the storage endpoint rejects bytes that changed after they were hashed.
    pub md5_base64: String,
}

/// A presigned PUT that ran out of time. Kept as its own type so the caller can
/// tell it from a refusal: re-sending a full multi-gigabyte file on a link that
/// just timed out would only time out again.
#[derive(Debug)]
pub struct UploadTimedOut {
    pub after: Duration,
}

impl std::fmt::Display for UploadTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "media upload did not finish within {}s",
            self.after.as_secs()
        )
    }
}

impl std::error::Error for UploadTimedOut {}

/// Check that `media` is an approved local file under the media root, within the
/// size cap, and hash it.
pub async fn inspect_local_media(media: &PublishMedia) -> anyhow::Result<LocalUpload> {
    let path = approved_local_media_path(&media.url)?;
    let metadata = tokio::fs::metadata(&path).await?;
    if !metadata.is_file() {
        anyhow::bail!("\"{}\" is not a file", media.url);
    }
    let size = metadata.len();
    if size == 0 {
        anyhow::bail!("\"{}\" is empty", media.url);
    }
    if size > MAX_STAGED_UPLOAD_BYTES {
        anyhow::bail!(
            "\"{}\" is {size} bytes; uploads are limited to {MAX_STAGED_UPLOAD_BYTES} bytes",
            media.url
        );
    }
    let file = tokio::fs::File::open(&path).await?;
    let mut reader = BufReader::new(file).take(size);
    let mut hasher = Md5::new();
    let mut buffer = vec![0u8; READ_CHUNK_BYTES];
    let mut read_total = 0u64;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        read_total += read as u64;
    }
    if read_total != size {
        anyhow::bail!("\"{}\" changed while it was being read", media.url);
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow::anyhow!("\"{}\" has no usable file name", media.url))?;
    let digest = hasher.finalize();
    Ok(LocalUpload {
        path,
        name,
        mime_type: media.mime_type.clone(),
        size,
        md5_hex: hex_lower(&digest),
        md5_base64: {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(digest)
        },
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Where a presigned PUT may go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadEgress {
    /// Production: HTTPS, public addresses only, DNS-pinned, no redirects.
    Guarded,
    /// Tests: a loopback fake storage server over plain HTTP.
    #[cfg(test)]
    Loopback,
}

/// Seconds allowed per MiB uploaded: four, which is a sustained uplink of
/// about 2 Mbit/s. A 2 GiB render gets a little over two hours.
const UPLOAD_SECS_PER_MIB: u64 = 4;

/// How long one PUT may take: a fixed allowance plus [`UPLOAD_SECS_PER_MIB`]
/// per MiB, so a large render on a slow uplink is not cut off while a stalled
/// small upload still fails in minutes.
pub fn upload_timeout(size: u64) -> Duration {
    Duration::from_secs(120 + size.div_ceil(1024 * 1024) * UPLOAD_SECS_PER_MIB)
}

/// Stream `upload` to the presigned `url`.
///
/// `storage_backend` is the broker's reported backend: Azure Blob Storage needs
/// `x-ms-blob-type: BlockBlob` on the write, S3 needs nothing extra.
pub async fn put_presigned(
    url: &str,
    storage_backend: &str,
    upload: &LocalUpload,
    egress: UploadEgress,
) -> anyhow::Result<()> {
    let timeout = upload_timeout(upload.size);
    let client = match egress {
        UploadEgress::Guarded => {
            let target = ryu_egress::resolve_guarded_url(
                url,
                ryu_egress::GuardedFetchPolicy {
                    allow_http: false,
                    max_body_bytes: MAX_STAGED_UPLOAD_BYTES,
                    max_redirect_hops: 0,
                    timeout,
                },
            )
            .await
            .map_err(|error| anyhow::anyhow!("upload URL refused: {error}"))?;
            reqwest::Client::builder()
                .timeout(timeout)
                .connect_timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .resolve_to_addrs(&target.host, &target.addresses)
                .build()?
        }
        #[cfg(test)]
        UploadEgress::Loopback => reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    };
    let file = tokio::fs::File::open(&upload.path).await?;
    let current = file.metadata().await?.len();
    if current != upload.size {
        anyhow::bail!(
            "\"{}\" changed size after it was checked ({} → {current} bytes)",
            upload.name,
            upload.size
        );
    }
    let stream =
        tokio_util::io::ReaderStream::with_capacity(file.take(upload.size), READ_CHUNK_BYTES);
    let mut request = client
        .put(url)
        .header(reqwest::header::CONTENT_TYPE, upload.mime_type.as_str())
        .header(reqwest::header::CONTENT_LENGTH, upload.size)
        // S3 and Azure Blob both verify `Content-MD5` and refuse a mismatch, so
        // a same-size rewrite between hashing and this PUT cannot be stored
        // under the digest the broker was given.
        .header("content-md5", upload.md5_base64.as_str())
        .body(reqwest::Body::wrap_stream(stream));
    if storage_backend.eq_ignore_ascii_case("azure_blob_storage") {
        request = request.header("x-ms-blob-type", "BlockBlob");
    }
    let response = request.send().await.map_err(|error| {
        if error.is_timeout() {
            anyhow::Error::new(UploadTimedOut { after: timeout })
        } else {
            anyhow::anyhow!("media upload failed: {error}")
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        // Storage error bodies can echo request details; report only the status.
        anyhow::bail!("media upload was refused by the storage endpoint: HTTP {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_is_lowercase_and_padded() {
        assert_eq!(hex_lower(&[0x00, 0x0a, 0xff]), "000aff");
    }

    #[test]
    fn the_timeout_grows_with_the_file() {
        assert_eq!(upload_timeout(0), Duration::from_secs(120));
        assert_eq!(upload_timeout(1), Duration::from_secs(124));
        assert_eq!(
            upload_timeout(2 * 1024 * 1024 * 1024),
            Duration::from_secs(120 + 2048 * 4)
        );
    }

    /// A same-size rewrite between hashing and the PUT is refused by storage
    /// that verifies `Content-MD5` (as S3 and Azure do), instead of being stored
    /// under the digest the broker was given.
    #[tokio::test]
    async fn a_rewrite_after_hashing_is_refused_by_the_declared_digest() {
        use axum::routing::put;
        use base64::Engine;
        async fn verify(
            headers: axum::http::HeaderMap,
            body: axum::body::Bytes,
        ) -> axum::http::StatusCode {
            let declared = headers
                .get("content-md5")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            let actual = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&body));
            if declared == actual {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::BAD_REQUEST
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/put", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, axum::Router::new().route("/put", put(verify)))
                .await
                .unwrap();
        });
        let dir = std::env::temp_dir().join(format!("ryu-social-md5-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("a.mp4");
        tokio::fs::write(&path, b"original").await.unwrap();
        let digest = Md5::digest(b"original");
        let upload = LocalUpload {
            path: path.clone(),
            name: "a.mp4".into(),
            mime_type: "video/mp4".into(),
            size: 8,
            md5_hex: hex_lower(&digest),
            md5_base64: base64::engine::general_purpose::STANDARD.encode(digest),
        };
        put_presigned(&url, "s3", &upload, UploadEgress::Loopback)
            .await
            .unwrap();
        tokio::fs::write(&path, b"replaced").await.unwrap();
        let error = put_presigned(&url, "s3", &upload, UploadEgress::Loopback)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("HTTP 400"), "{error}");
        server.abort();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn a_guarded_upload_refuses_plain_http_and_loopback_targets() {
        let dir = std::env::temp_dir().join(format!("ryu-social-stage-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("a.mp4");
        tokio::fs::write(&path, b"abc").await.unwrap();
        let upload = LocalUpload {
            path,
            name: "a.mp4".into(),
            mime_type: "video/mp4".into(),
            size: 3,
            md5_hex: String::new(),
            md5_base64: String::new(),
        };
        for url in ["http://uploads.example.com/put", "https://127.0.0.1/put"] {
            let error = put_presigned(url, "s3", &upload, UploadEgress::Guarded)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("upload URL refused"), "{url}: {error}");
        }
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
