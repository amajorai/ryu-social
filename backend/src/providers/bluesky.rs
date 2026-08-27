//! Bluesky over the AT Protocol — a first-party adapter, no broker in the path.
//!
//! This is the one provider here that is fully specifiable, so it is also the one
//! that can be genuinely idempotent: `com.atproto.repo.createRecord` accepts a
//! CALLER-CHOSEN record key, which means a retry can address the same record instead
//! of creating a second post. See [`BlueskyProvider::publish`].
//!
//! ## Three things that bite
//!
//! 1. **`refreshSession` authenticates with the REFRESH token**, not the access
//!    token. Using the access token there fails in a way that reads like an expiry
//!    problem and sends you looking in the wrong place.
//! 2. **Writes go to the PDS, reads go to the AppView**, and the AppView read is
//!    UNAUTHENTICATED. Sending a bearer to the public AppView is not an error, it is
//!    just pointless.
//! 3. **`uploadBlob` posts RAW BYTES with the media's own content-type** — not
//!    multipart, not JSON. This is why the crate does not enable reqwest's
//!    `multipart` feature.
//!
//! ## Credentials
//!
//! Handle + **app password** (never the account password), read from the environment
//! by [`super::registry`]. The session tokens live in memory only and are never
//! written to `social.db`.
//!
//! ## What is NOT covered by tests here
//!
//! Everything that needs a live PDS: the create/refresh/expire ladder, `uploadBlob`,
//! and the collision-then-`getRecord` idempotency recovery. The pure parts are unit
//! tested (JWT expiry decode, permalink construction, freshness arithmetic) and the
//! failure paths are proven to degrade to a [`PublishResult::Err`] against a dead
//! port, but a run whose first attempt fails on an expired token and whose retry must
//! refresh has never actually executed against a server. `session()` is called per
//! publish and re-checks freshness at call time, so the shape is right; it is the
//! wire behaviour that is unverified. A mock XRPC server is the way to close this.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use super::types::{
    PlatformProvider, ProviderAccount, ProviderId, PublishMedia, PublishRequest, PublishResult,
    RemotePostRef,
};
use crate::models::{now_ms, EngagementCounts, Platform, PlatformCapabilities};

/// Writes and session management.
const DEFAULT_PDS: &str = "https://bsky.social";
/// Reads. Public and unauthenticated.
const DEFAULT_APPVIEW: &str = "https://public.api.bsky.app";
/// URL construction only — never called.
const WEB_BASE: &str = "https://bsky.app";

const COLLECTION: &str = "app.bsky.feed.post";

/// Refresh this long before the token's own expiry, so a request that is in flight
/// when the clock crosses `exp` does not fail.
const EXPIRY_SKEW_MS: i64 = 60_000;
/// Assumed lifetime when the JWT's `exp` claim cannot be read. Deliberately shorter
/// than the real one: refreshing early is cheap, using a dead token is a failed
/// publish.
const ASSUMED_TTL_MS: i64 = 90 * 60 * 1_000;

/// Handle + app password. `Debug` is redacted — an app password is a credential.
#[derive(Clone)]
pub struct BlueskyCredentials {
    pub handle: String,
    pub app_password: String,
}

impl BlueskyCredentials {
    /// Normalizes the handle the way the PDS expects: trimmed, no leading `@`.
    pub fn new(handle: impl Into<String>, app_password: impl Into<String>) -> Self {
        let handle = handle.into().trim().trim_start_matches('@').to_string();
        Self {
            handle,
            app_password: app_password.into(),
        }
    }
}

impl std::fmt::Debug for BlueskyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlueskyCredentials")
            .field("handle", &self.handle)
            .field("app_password", &"***")
            .finish()
    }
}

#[derive(Clone, Debug)]
struct Session {
    access_jwt: String,
    refresh_jwt: String,
    did: String,
    handle: String,
    expires_at: i64,
}

impl Session {
    fn is_fresh(&self, now: i64) -> bool {
        now < self.expires_at - EXPIRY_SKEW_MS
    }
}

/// A strong reference to a record: the pair every reply must carry.
#[derive(Clone, Debug)]
struct StrongRef {
    uri: String,
    cid: String,
}

pub struct BlueskyProvider {
    http: reqwest::Client,
    credentials: BlueskyCredentials,
    pds: String,
    appview: String,
    /// One shared session behind an async mutex, so concurrent publishes mint ONE
    /// session rather than racing to create several.
    session: Arc<Mutex<Option<Session>>>,
}

impl std::fmt::Debug for BlueskyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlueskyProvider")
            .field("credentials", &self.credentials)
            .field("pds", &self.pds)
            .finish()
    }
}

impl BlueskyProvider {
    /// Side-effect free: stores and validates shape only. No session is minted until
    /// the first call that needs one.
    pub fn new(
        http: reqwest::Client,
        credentials: BlueskyCredentials,
        pds: Option<String>,
        appview: Option<String>,
    ) -> Self {
        Self {
            http,
            credentials,
            pds: pds.unwrap_or_else(|| DEFAULT_PDS.to_string()),
            appview: appview.unwrap_or_else(|| DEFAULT_APPVIEW.to_string()),
            session: Arc::new(Mutex::new(None)),
        }
    }

    /// A live session, refreshed or created as needed.
    async fn session(&self) -> anyhow::Result<Session> {
        let mut guard = self.session.lock().await;
        let now = now_ms();
        if let Some(session) = guard.as_ref() {
            if session.is_fresh(now) {
                return Ok(session.clone());
            }
            // Try the cheap path first; a refresh token that has itself expired just
            // means we fall through to a full login.
            match self.refresh_session(&session.refresh_jwt).await {
                Ok(refreshed) => {
                    *guard = Some(refreshed.clone());
                    return Ok(refreshed);
                }
                Err(e) => {
                    tracing::debug!(error = %e, "ryu-social: bluesky refresh failed; creating a new session");
                }
            }
        }
        let created = self.create_session().await?;
        *guard = Some(created.clone());
        Ok(created)
    }

    async fn create_session(&self) -> anyhow::Result<Session> {
        let value = self
            .xrpc_post(
                &self.pds,
                "com.atproto.server.createSession",
                None,
                json!({
                    "identifier": self.credentials.handle,
                    "password": self.credentials.app_password,
                }),
            )
            .await?;
        Self::session_from(value)
    }

    async fn refresh_session(&self, refresh_jwt: &str) -> anyhow::Result<Session> {
        // The REFRESH token authenticates this call — not the access token.
        let value = self
            .xrpc_post(
                &self.pds,
                "com.atproto.server.refreshSession",
                Some(refresh_jwt),
                json!({}),
            )
            .await?;
        Self::session_from(value)
    }

    fn session_from(value: Value) -> anyhow::Result<Session> {
        let field = |k: &str| {
            value
                .get(k)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("bluesky session response missing \"{k}\""))
        };
        let access_jwt = field("accessJwt")?;
        let expires_at = jwt_expiry_ms(&access_jwt).unwrap_or_else(|| now_ms() + ASSUMED_TTL_MS);
        Ok(Session {
            refresh_jwt: field("refreshJwt")?,
            did: field("did")?,
            handle: field("handle").unwrap_or_default(),
            access_jwt,
            expires_at,
        })
    }

    async fn xrpc_post(
        &self,
        host: &str,
        method: &str,
        bearer: Option<&str>,
        body: Value,
    ) -> anyhow::Result<Value> {
        let mut req = self
            .http
            .post(format!("{host}/xrpc/{method}"))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&body);
        if let Some(token) = bearer {
            req = req.bearer_auth(token);
        }
        let response = req.send().await?;
        Self::read_json(method, response).await
    }

    async fn xrpc_get(&self, host: &str, method: &str, query: &str) -> anyhow::Result<Value> {
        let response = self
            .http
            .get(format!("{host}/xrpc/{method}?{query}"))
            .send()
            .await?;
        Self::read_json(method, response).await
    }

    async fn read_json(method: &str, response: reqwest::Response) -> anyhow::Result<Value> {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("bluesky {method} failed: {} {}", status.as_u16(), text);
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text)?)
    }

    /// Raw bytes for one attachment: fetched when it is a URL, read from disk when it
    /// is a local path. This sidecar is a native process, so the local read is a real
    /// capability here — unlike a hosted broker, which is why
    /// [`super::ComposioProvider`] rejects local paths outright.
    async fn media_bytes(&self, media: &PublishMedia) -> anyhow::Result<Vec<u8>> {
        if media.is_remote() {
            let response = self.http.get(&media.url).send().await?;
            if !response.status().is_success() {
                anyhow::bail!("could not fetch media {}: {}", media.url, response.status());
            }
            return Ok(response.bytes().await?.to_vec());
        }
        let path = std::path::Path::new(&media.url);
        if !path.is_absolute() {
            anyhow::bail!("media path must be absolute, got \"{}\"", media.url);
        }
        Ok(tokio::fs::read(path).await?)
    }

    /// `com.atproto.repo.uploadBlob` — raw bytes, the media's own content type.
    async fn upload_blob(&self, session: &Session, media: &PublishMedia) -> anyhow::Result<Value> {
        let bytes = self.media_bytes(media).await?;
        let response = self
            .http
            .post(format!("{}/xrpc/com.atproto.repo.uploadBlob", self.pds))
            .header(reqwest::header::CONTENT_TYPE, media.mime_type.clone())
            .bearer_auth(&session.access_jwt)
            .body(bytes)
            .send()
            .await?;
        let value = Self::read_json("com.atproto.repo.uploadBlob", response).await?;
        value
            .get("blob")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("bluesky uploadBlob returned no blob reference"))
    }

    /// The images embed for one segment, or `None` when it has no usable image.
    ///
    /// Video is dropped rather than erroring: the embed this adapter writes is
    /// `app.bsky.embed.images`, and the platform limit table already declares Bluesky
    /// image-only, so a video here means someone bypassed compose validation.
    async fn build_embed(
        &self,
        session: &Session,
        media: &[PublishMedia],
    ) -> anyhow::Result<Option<Value>> {
        let images: Vec<&PublishMedia> = media
            .iter()
            .filter(|m| m.mime_type.starts_with("image/"))
            .collect();
        if images.is_empty() {
            return Ok(None);
        }
        let mut entries = Vec::with_capacity(images.len());
        for image in images {
            let blob = self.upload_blob(session, image).await?;
            entries.push(json!({
                // Alt text is whatever the composer supplied. It is deliberately NOT
                // defaulted to the file name: "IMG_4821.png" is not a description,
                // and a screen reader announcing it is worse than announcing nothing.
                "alt": image.alt_text.clone().unwrap_or_default(),
                "image": blob,
            }));
        }
        Ok(Some(json!({
            "$type": "app.bsky.embed.images",
            "images": entries,
        })))
    }

    /// Create one post record under a caller-chosen key.
    async fn create_record(
        &self,
        session: &Session,
        rkey: Option<&str>,
        text: &str,
        embed: Option<Value>,
        reply: Option<(&StrongRef, &StrongRef)>,
    ) -> anyhow::Result<StrongRef> {
        let mut record = json!({
            "$type": COLLECTION,
            "text": text,
            "createdAt": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        });
        if let Some(embed) = embed {
            record["embed"] = embed;
        }
        if let Some((root, parent)) = reply {
            record["reply"] = json!({
                "root": { "uri": root.uri, "cid": root.cid },
                "parent": { "uri": parent.uri, "cid": parent.cid },
            });
        }
        let mut body = json!({
            "repo": session.did,
            "collection": COLLECTION,
            "record": record,
        });
        if let Some(rkey) = rkey {
            body["rkey"] = json!(rkey);
        }
        let value = self
            .xrpc_post(
                &self.pds,
                "com.atproto.repo.createRecord",
                Some(&session.access_jwt),
                body,
            )
            .await?;
        Self::strong_ref_from(&value)
    }

    fn strong_ref_from(value: &Value) -> anyhow::Result<StrongRef> {
        Ok(StrongRef {
            uri: value
                .get("uri")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("bluesky createRecord returned no uri"))?
                .to_string(),
            cid: value
                .get("cid")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        })
    }

    /// Does a record with this key already exist in our repo?
    ///
    /// This is the idempotency recovery path, and it is EVIDENCE rather than a guess:
    /// after a failed `createRecord` under a deterministic key, the failure could mean
    /// "the call never landed" or "the call landed and the retry collided". Asking the
    /// repo settles it without depending on the exact shape of the broker's error
    /// string, which is the sort of thing that changes silently.
    async fn existing_record(&self, session: &Session, rkey: &str) -> Option<StrongRef> {
        let query = format!(
            "repo={}&collection={COLLECTION}&rkey={rkey}",
            urlencode(&session.did)
        );
        let value = self
            .xrpc_get(&self.pds, "com.atproto.repo.getRecord", &query)
            .await
            .ok()?;
        Self::strong_ref_from(&value).ok()
    }

    async fn publish_inner(&self, request: &PublishRequest) -> anyhow::Result<PublishResult> {
        let session = self.session().await?;
        let segments = request.effective_segments();

        let mut root: Option<StrongRef> = None;
        let mut parent: Option<StrongRef> = None;
        let mut already_live = false;

        for (index, segment) in segments.iter().enumerate() {
            let rkey = request.segment_key(index);
            let embed = self.build_embed(&session, &segment.media).await?;
            let reply = match (&root, &parent) {
                (Some(root), Some(parent)) => Some((root, parent)),
                _ => None,
            };
            let created = match self
                .create_record(&session, rkey.as_deref(), &segment.text, embed, reply)
                .await
            {
                Ok(created) => created,
                Err(e) => {
                    // A deterministic rkey turns "retry after a lost response" into a
                    // server-side collision instead of a second post. Confirm which
                    // one this is by reading the record back.
                    let Some(rkey) = rkey.as_deref() else {
                        return Err(e);
                    };
                    match self.existing_record(&session, rkey).await {
                        Some(existing) => {
                            already_live = true;
                            tracing::info!(
                                rkey,
                                "ryu-social: bluesky record already existed under our key; treating the retry as a no-op"
                            );
                            existing
                        }
                        None => return Err(e),
                    }
                }
            };
            if root.is_none() {
                root = Some(created.clone());
            }
            parent = Some(created);
        }

        let Some(root) = root else {
            return Ok(PublishResult::err("nothing to publish"));
        };
        if already_live {
            tracing::debug!(uri = %root.uri, "ryu-social: bluesky publish resolved to an existing thread");
        }
        Ok(PublishResult::Ok {
            // The ROOT uri, so an engagement read always addresses the thread head.
            remote_url: post_url(&session.handle, &root.uri),
            remote_id: root.uri,
        })
    }
}

/// `at://{did}/{collection}/{rkey}` → the record key.
fn rkey_of(uri: &str) -> Option<&str> {
    uri.strip_prefix("at://")?.rsplit('/').next()
}

/// The human-facing permalink. `None` when the uri does not parse — a publish that
/// succeeded with an unparseable uri is still a success, just without a link.
fn post_url(handle: &str, uri: &str) -> Option<String> {
    if handle.is_empty() {
        return None;
    }
    rkey_of(uri).map(|rkey| format!("{WEB_BASE}/profile/{handle}/post/{rkey}"))
}

/// The `exp` claim of a JWT, as epoch millis. `None` when anything at all is off —
/// the caller then assumes a conservative TTL rather than trusting a token forever.
fn jwt_expiry_ms(jwt: &str) -> Option<i64> {
    let payload = jwt.split('.').nth(1)?;
    let bytes = b64url_decode(payload)?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    value.get("exp").and_then(Value::as_i64).map(|s| s * 1_000)
}

/// Base64url decode, no padding required.
///
/// Hand-rolled for the same reason as the percent-encoder: this crate's dependency
/// set is pinned to Core's, and one JWT payload is not worth moving the shared
/// lockfile for.
fn b64url_decode(input: &str) -> Option<Vec<u8>> {
    let mut buffer: u32 = 0;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    for c in input.bytes() {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => continue,
            _ => return None,
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xFF) as u8);
        }
    }
    Some(out)
}

fn urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[async_trait]
impl PlatformProvider for BlueskyProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Bluesky
    }

    async fn connect(&self, _account: &ProviderAccount) -> anyhow::Result<Option<String>> {
        // "Connecting" is exactly "prove the credentials work": mint a session and
        // record the DID as the external id.
        let session = self.session().await?;
        Ok(Some(session.did))
    }

    async fn disconnect(&self, _account: &ProviderAccount) -> anyhow::Result<()> {
        // Drops the in-memory session only. The stored credentials are the operator's
        // to remove; silently deleting them here would be a surprise.
        *self.session.lock().await = None;
        Ok(())
    }

    async fn publish(&self, request: &PublishRequest) -> PublishResult {
        if request.account.platform != Platform::Bluesky {
            return PublishResult::err(format!(
                "the Bluesky adapter cannot publish to {}",
                request.account.platform
            ));
        }
        match self.publish_inner(request).await {
            Ok(result) => result,
            // Every expected failure becomes a value: bad credentials, a rejected
            // record, an unreadable media file. `publish` never errors.
            Err(e) => PublishResult::err(e.to_string()),
        }
    }

    async fn read_engagement(&self, post: &RemotePostRef) -> anyhow::Result<EngagementCounts> {
        // AppView, unauthenticated.
        let value = self
            .xrpc_get(
                &self.appview,
                "app.bsky.feed.getPosts",
                &format!("uris={}", urlencode(&post.remote_id)),
            )
            .await?;
        let first = value
            .get("posts")
            .and_then(Value::as_array)
            .and_then(|posts| posts.first())
            .cloned()
            .unwrap_or(Value::Null);
        Ok(EngagementCounts {
            likes: first.get("likeCount").and_then(Value::as_u64),
            comments: first.get("replyCount").and_then(Value::as_u64),
            shares: first.get("repostCount").and_then(Value::as_u64),
            // AT-Protocol exposes no view count at all. `None`, not `0` — a zero here
            // would be averaged by analytics as if it were a measurement.
            views: None,
            fetched_at: now_ms(),
        })
    }

    async fn capabilities(&self, platform: Platform) -> PlatformCapabilities {
        if platform != Platform::Bluesky {
            return PlatformCapabilities::empty();
        }
        PlatformCapabilities {
            publish: true,
            // Notifications (`app.bsky.notification.listNotifications`) would give a
            // real inbox; not wired yet, so the matrix says so rather than promising
            // a surface that returns nothing.
            read_comments: false,
            read_dms: false,
            send_dm: false,
            read_engagement: true,
            // ALWAYS false: scheduling is ours.
            schedule: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_normalize_the_handle_and_never_print_the_password() {
        let creds = BlueskyCredentials::new("  @me.bsky.social ", "abcd-efgh");
        assert_eq!(creds.handle, "me.bsky.social");
        let printed = format!("{creds:?}");
        assert!(printed.contains("me.bsky.social"));
        assert!(!printed.contains("abcd-efgh"));
    }

    #[test]
    fn jwt_expiry_is_read_from_the_payload_and_degrades_to_none() {
        // {"alg":"HS256"}.{"exp":1700000000}.sig — base64url, unpadded.
        let payload = "eyJleHAiOjE3MDAwMDAwMDB9";
        let jwt = format!("header.{payload}.sig");
        assert_eq!(jwt_expiry_ms(&jwt), Some(1_700_000_000_000));
        assert_eq!(jwt_expiry_ms("not-a-jwt"), None);
        assert_eq!(jwt_expiry_ms("a.!!!.c"), None);
    }

    #[test]
    fn a_session_without_a_readable_expiry_still_expires() {
        let session = Session {
            access_jwt: "a".into(),
            refresh_jwt: "r".into(),
            did: "did:plc:x".into(),
            handle: "me.bsky.social".into(),
            expires_at: now_ms() + ASSUMED_TTL_MS,
        };
        assert!(session.is_fresh(now_ms()));
        assert!(!session.is_fresh(now_ms() + ASSUMED_TTL_MS));
    }

    #[test]
    fn permalinks_are_built_from_the_record_key() {
        let uri = "at://did:plc:abc/app.bsky.feed.post/3kabcd";
        assert_eq!(rkey_of(uri), Some("3kabcd"));
        assert_eq!(
            post_url("me.bsky.social", uri).as_deref(),
            Some("https://bsky.app/profile/me.bsky.social/post/3kabcd")
        );
        // An unparseable uri loses the link, not the publish.
        assert_eq!(post_url("me.bsky.social", "garbage"), None);
        assert_eq!(post_url("", uri), None);
    }

    #[tokio::test]
    async fn the_adapter_refuses_platforms_that_are_not_bluesky() {
        let provider = BlueskyProvider::new(
            reqwest::Client::new(),
            BlueskyCredentials::new("me", "pw"),
            Some("http://127.0.0.1:1".into()),
            Some("http://127.0.0.1:1".into()),
        );
        let request = PublishRequest {
            account: ProviderAccount {
                id: "acc_1".into(),
                platform: Platform::X,
                label: None,
                external_id: None,
            },
            text: "hi".into(),
            media: vec![],
            segments: None,
            idempotency_key: None,
        };
        assert!(provider
            .publish(&request)
            .await
            .error()
            .unwrap()
            .contains("cannot publish to x"));
        assert_eq!(
            provider.capabilities(Platform::X).await,
            PlatformCapabilities::empty()
        );
        assert!(provider.capabilities(Platform::Bluesky).await.publish);
        assert!(!provider.capabilities(Platform::Bluesky).await.schedule);
    }

    #[tokio::test]
    async fn an_unreachable_pds_fails_the_publish_as_a_value() {
        let provider = BlueskyProvider::new(
            reqwest::Client::new(),
            BlueskyCredentials::new("me", "pw"),
            Some("http://127.0.0.1:1".into()),
            Some("http://127.0.0.1:1".into()),
        );
        let request = PublishRequest {
            account: ProviderAccount {
                id: "acc_1".into(),
                platform: Platform::Bluesky,
                label: None,
                external_id: None,
            },
            text: "hi".into(),
            media: vec![],
            segments: None,
            idempotency_key: Some("sp_1:acc_1".into()),
        };
        assert!(!provider.publish(&request).await.is_ok());
    }
}
