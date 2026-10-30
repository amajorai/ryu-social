//! Composio: Ryu's managed provider broker for every platform we have no
//! first-party adapter for.
//!
//! ## What is verified and what is a GUESS
//!
//! Verified against the published API: the base URL, the `x-api-key` header, the
//! `POST /tools/execute` envelope, the `GET /tools?toolkit_slug=…&limit=200` catalog
//! query, and the `/connected_accounts` paths.
//!
//! **NOT verified — every `toolSlug` literal below.** They are synthesized by
//! concatenating the toolkit name with a verb (`TWITTER_CREATE_POST`,
//! `LINKEDIN_LIST_COMMENTS`, …). The real catalog may name them anything. A call
//! against a wrong slug 404s, which this provider surfaces as a
//! [`PublishResult::Err`] carrying the broker's own message rather than pretending to
//! succeed — so a wrong guess is loud, not silent. Before trusting any of these in
//! production, enumerate the live `/tools` catalog for the toolkit and correct
//! [`tool_slug`].
//!
//! ## Secret handling
//!
//! The Composio credential is injected and used only by Ryu's Gateway/provider
//! vault. This satellite sends provider-neutral operation facts through
//! [`ProviderRouter`]; no key is read from `SocialSettings`, the node settings
//! blob, an app environment variable, or a client response.

use async_trait::async_trait;
use serde_json::{json, Value};

use super::staged_upload::{
    inspect_local_media, put_presigned, upload_timeout, UploadEgress, UploadTimedOut,
    MAX_STAGED_UPLOAD_BYTES,
};
use super::types::{
    PlatformProvider, ProviderAccount, ProviderId, ProviderInboxItem, PublishDeadline,
    PublishMedia, PublishRequest, PublishResult, RemotePostRef,
};
use crate::models::{now_ms, EngagementCounts, InboxKind, Platform, PlatformCapabilities};
use ryu_app_events::{ManagedProviderCall, ProviderRouter, ProviderRouterError};

pub struct ComposioProvider {
    router: ProviderRouter,
    /// Where staged-upload PUTs may go. Always [`UploadEgress::Guarded`] outside
    /// tests.
    upload_egress: UploadEgress,
}

impl std::fmt::Debug for ComposioProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposioProvider")
            .field("managed", &true)
            .finish()
    }
}

impl ComposioProvider {
    /// Construction is side-effect free — no network, no validation call. The
    /// registry builds providers eagerly, while Gateway owns the credential and
    /// selects the upstream provider at call time.
    pub fn managed(router: ProviderRouter) -> Self {
        Self {
            router,
            upload_egress: UploadEgress::Guarded,
        }
    }

    /// Send staged-upload PUTs to a loopback fake storage server.
    #[cfg(test)]
    pub fn with_loopback_uploads(mut self) -> Self {
        self.upload_egress = UploadEgress::Loopback;
        self
    }

    /// Stage one local file for `tool`'s file parameter and return the value
    /// that parameter takes: `{ name, mimetype, s3key }`.
    ///
    /// The slot request goes through the Gateway-vaulted router, so the broker
    /// credential never reaches this process; the bytes then stream straight to
    /// the presigned URL Gateway returned. It names the connected account and
    /// carries the scheduling agent's grant, so Core applies that agent's
    /// account access to the staging step as it does to the publish that follows.
    async fn stage_local_file(
        &self,
        tool: &UploadTool,
        media: &PublishMedia,
        connected_account_id: &str,
        agent_grant: Option<&str>,
    ) -> Result<Value, PublishResult> {
        let upload = inspect_local_media(media)
            .await
            .map_err(|error| PublishResult::err(error.to_string()))?;
        let response = self
            .router
            .call_with_agent_grant(
                ManagedProviderCall {
                    provider: "composio".to_owned(),
                    tool_id: tool.slug.to_owned(),
                    operation: Some("file_upload".to_owned()),
                    account_id: Some(connected_account_id.to_owned()),
                    method: "POST".to_owned(),
                    query: Vec::new(),
                    body: Some(json!({
                        "toolkitSlug": tool.toolkit,
                        "filename": upload.name,
                        "mimetype": upload.mime_type,
                        "md5": upload.md5_hex,
                        "size": upload.size,
                    })),
                    idempotency_key: None,
                    request_id: format!("social:composio:file_upload:{}", uuid::Uuid::new_v4()),
                    fallback_cost_micro_usd: None,
                    task_label: Some("Outpost Composio media staging".to_owned()),
                    timeout_ms: None,
                    max_cost_micro_usd: None,
                },
                agent_grant,
            )
            .await
            .map_err(|error| {
                PublishResult::err(format!("could not stage media with Composio: {error}"))
            })?;
        if !response.ok {
            return Err(PublishResult::err(format!(
                "could not stage media with Composio: HTTP {}",
                response.status
            )));
        }
        let field = |name: &str| {
            response
                .body
                .get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
        };
        let (Some(key), Some(upload_url)) = (field("key"), field("uploadUrl")) else {
            return Err(PublishResult::err(
                "Composio staging returned no upload slot",
            ));
        };
        put_presigned(
            upload_url,
            field("storageBackend").unwrap_or("s3"),
            &upload,
            self.upload_egress,
        )
        .await
        .map_err(|error| {
            if error.is::<UploadTimedOut>() {
                // Nothing was published, but re-sending the whole file on the
                // link that just timed out would only repeat the timeout.
                PublishResult::final_err(format!(
                    "{error}; Outpost does not retry a timed-out upload automatically. Retry the post when the connection is faster"
                ))
            } else {
                PublishResult::err(error.to_string())
            }
        })?;
        Ok(json!({
            "name": upload.name,
            "mimetype": upload.mime_type,
            "s3key": key,
        }))
    }

    /// Publish one local video through the platform's upload tool.
    async fn publish_uploaded_video(
        &self,
        request: &PublishRequest,
        tool: &UploadTool,
        connected_account_id: &str,
    ) -> PublishResult {
        let platform = request.account.platform;
        let [media] = request.media.as_slice() else {
            return PublishResult::err(format!(
                "{platform} uploads take exactly one video; this post has {} attachments",
                request.media.len()
            ));
        };
        if !media.mime_type.to_ascii_lowercase().starts_with("video/") {
            return PublishResult::err(format!(
                "{platform} uploads take a video file, not {}",
                media.mime_type
            ));
        }
        let file = match self
            .stage_local_file(
                tool,
                media,
                connected_account_id,
                request.agent_grant.as_deref(),
            )
            .await
        {
            Ok(file) => file,
            Err(failure) => return failure,
        };
        let arguments = upload_arguments(platform, &request.text, file);
        // From here on the upload tool has been dispatched. Composio pulls the
        // staged file and pushes it to the platform inside this one call, so a
        // timeout or unreadable answer does NOT mean nothing was posted. Those
        // outcomes are final: retrying could publish the video twice.
        let size =
            std::fs::metadata(local_media_path(media)).map_or(MAX_STAGED_UPLOAD_BYTES, |m| m.len());
        // The upload publishes to the agent's connected account like any other
        // post, so it carries the scheduling agent's grant for Core to check.
        let response = self
            .router
            .call_with_agent_grant(
                ManagedProviderCall {
                    provider: "composio".to_owned(),
                    tool_id: tool.slug.to_owned(),
                    operation: Some("execute".to_owned()),
                    account_id: Some(connected_account_id.to_owned()),
                    method: "POST".to_owned(),
                    query: Vec::new(),
                    body: Some(arguments),
                    idempotency_key: request.idempotency_key.clone(),
                    request_id: format!("social:composio:{}:{}", tool.slug, uuid::Uuid::new_v4()),
                    fallback_cost_micro_usd: None,
                    task_label: Some("Outpost Composio video upload".to_owned()),
                    timeout_ms: Some(
                        upload_execute_timeout(size)
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX),
                    ),
                    max_cost_micro_usd: None,
                },
                request.agent_grant.as_deref(),
            )
            .await;
        match response {
            Ok(response) if response.ok => upload_result(platform, &response.body),
            Ok(response) => PublishResult::final_err(outcome_unknown(
                platform,
                &format!("the upload call returned HTTP {}", response.status),
            )),
            Err(error) if was_refused_before_dispatch(&error) => {
                PublishResult::err(format!("{platform} upload was refused: {error}"))
            }
            Err(error) => PublishResult::final_err(outcome_unknown(platform, &error.to_string())),
        }
    }

    /// Run one broker tool and unwrap its envelope.
    ///
    /// `{ successful: false }` or a non-empty `error` is a FAILED tool call even
    /// though the HTTP status was 200 — collapsing the two is how a failed publish
    /// gets recorded as published.
    ///
    /// `agent_grant` is the Core-signed grant of the agent the action runs for,
    /// if any; Core applies that agent's account access to the call.
    async fn execute_tool(
        &self,
        slug: &str,
        connected_account_id: Option<&str>,
        arguments: Value,
        agent_grant: Option<&str>,
    ) -> anyhow::Result<Value> {
        let response = self
            .router
            .call_with_agent_grant(
                ManagedProviderCall {
                    provider: "composio".to_owned(),
                    tool_id: slug.to_owned(),
                    operation: Some("execute".to_owned()),
                    account_id: connected_account_id.map(str::to_owned),
                    method: "POST".to_owned(),
                    query: Vec::new(),
                    body: Some(arguments),
                    idempotency_key: None,
                    request_id: format!("social:composio:{slug}:{}", uuid::Uuid::new_v4()),
                    fallback_cost_micro_usd: None,
                    task_label: Some("Outpost Composio action".to_owned()),
                    timeout_ms: None,
                    max_cost_micro_usd: None,
                },
                agent_grant,
            )
            .await
            .map_err(super::managed_call_error)?;
        if !response.ok {
            anyhow::bail!("managed Composio action returned HTTP {}", response.status);
        }
        Ok(response.body)
    }
}

/// `{TOOLKIT}_{VERB}` — the slug shape. See the module docs: the VERB half is a
/// guess for every verb below.
fn tool_slug(platform: Platform, verb: &str) -> String {
    format!(
        "{}_{verb}",
        platform.composio_toolkit().to_ascii_uppercase()
    )
}

/// A Composio tool that takes an uploaded file, and the parameter it takes it in.
///
/// Slugs and parameter names come from Composio's published toolkit reference
/// (`YOUTUBE_UPLOAD_VIDEO.videoFilePath`, `TIKTOK_UPLOAD_VIDEO.file_to_upload`),
/// unlike the synthesized `{TOOLKIT}_{VERB}` slugs above. They have not been run
/// against a live account from this repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UploadTool {
    slug: &'static str,
    toolkit: &'static str,
    file_param: &'static str,
}

fn upload_tool(platform: Platform) -> Option<UploadTool> {
    match platform {
        Platform::Youtube => Some(UploadTool {
            slug: "YOUTUBE_UPLOAD_VIDEO",
            toolkit: "youtube",
            file_param: "videoFilePath",
        }),
        Platform::Tiktok => Some(UploadTool {
            slug: "TIKTOK_UPLOAD_VIDEO",
            toolkit: "tiktok",
            file_param: "file_to_upload",
        }),
        _ => None,
    }
}

/// How long the upload tool may run upstream: ten minutes plus two seconds per
/// MiB for Composio to pull the staged file and push it to the platform,
/// within the provider router's ceiling.
fn upload_execute_timeout(size: u64) -> std::time::Duration {
    let secs = 600 + size.div_ceil(1024 * 1024) * 2;
    std::time::Duration::from_millis(
        secs.saturating_mul(1000)
            .min(ManagedProviderCall::MAX_TIMEOUT_MS),
    )
}

/// Where a local media reference points, for sizing deadlines. Falls back to
/// the raw reference; a path that cannot be read sizes as the largest upload.
fn local_media_path(media: &PublishMedia) -> std::path::PathBuf {
    crate::state::approved_local_media_path(&media.url)
        .unwrap_or_else(|_| std::path::PathBuf::from(&media.url))
}

/// Whether a router error proves the upload tool never ran: the request was
/// invalid, there is no Core host, or Gateway refused it with a 4xx before any
/// upstream call (bad request, credentials, credits, rate limit). Gateway
/// reports upstream failures and timeouts as 5xx, which stay "outcome unknown".
fn was_refused_before_dispatch(error: &ProviderRouterError) -> bool {
    match error {
        ProviderRouterError::NotHosted | ProviderRouterError::Invalid(_) => true,
        ProviderRouterError::Rejected { status, .. } => (400..500).contains(status),
        ProviderRouterError::Transport(_) => false,
    }
}

/// The reason recorded when an upload call may have published.
fn outcome_unknown(platform: Platform, detail: &str) -> String {
    let name = platform.label();
    format!(
        "{name} upload outcome is unknown ({detail}). The video may have been published, so Outpost did not retry it. Check {name} before publishing it again"
    )
}

/// YouTube's title limit, in characters.
const YOUTUBE_TITLE_CHARS: usize = 100;
/// YouTube's description limit, in characters (the API counts bytes; staying
/// under 5000 characters of mostly-ASCII text keeps under it).
const YOUTUBE_DESCRIPTION_CHARS: usize = 5000;
/// "People & Blogs": YouTube requires a category and Outpost drafts carry none.
const YOUTUBE_DEFAULT_CATEGORY: &str = "22";

fn truncate_chars(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

/// Split draft text into a YouTube title and description: the first line is the
/// title (YouTube rejects `<` and `>` in titles, so they are dropped), and the
/// rest, after the blank line Video Studio writes, is the description.
fn youtube_title_and_description(text: &str) -> (String, String) {
    let text = text.trim();
    let (first, rest) = text.split_once('\n').unwrap_or((text, ""));
    let title: String = first.chars().filter(|c| !matches!(c, '<' | '>')).collect();
    let title = truncate_chars(title.trim(), YOUTUBE_TITLE_CHARS);
    let description = truncate_chars(rest.trim(), YOUTUBE_DESCRIPTION_CHARS);
    let title = if title.is_empty() {
        "Untitled".to_owned()
    } else {
        title
    };
    (title, description)
}

/// The upload tool's arguments for `platform`, with the staged `file`.
///
/// Both platforms publish publicly: a scheduled Outpost post is a public post on
/// every other platform too. TikTok refuses public posts from an app that has not
/// passed its audit; that refusal comes back as the publish error instead of
/// Outpost quietly posting privately.
fn upload_arguments(platform: Platform, text: &str, file: Value) -> Value {
    match platform {
        Platform::Youtube => {
            let (title, description) = youtube_title_and_description(text);
            json!({
                "title": title,
                "description": description,
                "tags": [],
                "categoryId": YOUTUBE_DEFAULT_CATEGORY,
                "privacyStatus": "public",
                "videoFilePath": file,
            })
        }
        _ => {
            let file_param = upload_tool(platform).map_or("file_to_upload", |tool| tool.file_param);
            let mut arguments = json!({
                "caption": text.trim(),
                "privacy_level": "PUBLIC_TO_EVERYONE",
                "publish": true,
            });
            arguments[file_param] = file;
            arguments
        }
    }
}

/// Find a string id in any of the shapes a tool result may nest it in.
///
/// Composio documents some toolkits' `data` as a string; when that string is
/// itself JSON, it is parsed and searched like an object.
fn find_id(data: &Value, keys: &[&str]) -> Option<String> {
    let decoded = |value: Option<&Value>| match value {
        Some(Value::String(text)) => serde_json::from_str::<Value>(text).ok(),
        Some(other) => Some(other.clone()),
        None => None,
    };
    let root = decoded(Some(data))?;
    let mut scopes = vec![root.clone()];
    for name in ["data", "response_data", "video"] {
        if let Some(scope) = decoded(root.get(name)) {
            for inner in ["data", "response_data", "video"] {
                if let Some(nested) = decoded(scope.get(inner)) {
                    scopes.push(nested);
                }
            }
            scopes.push(scope);
        }
    }
    scopes.iter().find_map(|scope| {
        keys.iter()
            .find_map(|key| scope.get(*key).and_then(Value::as_str))
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
    })
}

fn upload_result(platform: Platform, data: &Value) -> PublishResult {
    match platform {
        Platform::Youtube => match find_id(data, &["id", "videoId", "video_id"]) {
            Some(id) => PublishResult::Ok {
                remote_url: Some(format!("https://www.youtube.com/watch?v={id}")),
                remote_id: id,
            },
            // The tool reported success, so the video may be live; a retry
            // could upload it again.
            None => PublishResult::final_err(outcome_unknown(
                platform,
                "the upload call answered without a video id",
            )),
        },
        _ => match find_id(data, &["publish_id", "publishId", "id"]) {
            // TikTok processes the upload after this call returns; the publish id
            // is the handle for its status. There is no public URL yet.
            Some(id) => PublishResult::Ok {
                remote_id: id,
                remote_url: None,
            },
            None => PublishResult::final_err(outcome_unknown(
                platform,
                "the upload call answered without a publish id",
            )),
        },
    }
}

/// Read a count from any of several aliases, because platforms disagree about what a
/// share is called.
fn count_alias(data: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|k| data.get(*k).and_then(Value::as_u64))
}

/// Coerce a remote timestamp: a number is epoch millis (or seconds, scaled up); a
/// string is RFC-3339. Anything else falls back to `now`, because an inbox item with
/// no time sorts unpredictably and dropping it entirely would lose real engagement.
fn parse_remote_time(value: Option<&Value>, now: i64) -> i64 {
    match value {
        Some(Value::Number(n)) => {
            n.as_i64()
                .map_or(now, |v| if v < 100_000_000_000 { v * 1_000 } else { v })
        }
        Some(Value::String(s)) => chrono::DateTime::parse_from_rfc3339(s)
            .map(|d| d.timestamp_millis())
            .unwrap_or(now),
        _ => now,
    }
}

#[async_trait]
impl PlatformProvider for ComposioProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Composio
    }

    async fn connect(
        &self,
        account: &ProviderAccount,
        agent_grant: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        // The grant goes back to Core, which refuses an agent whose account
        // access is limited: this call names no connected account the agent
        // could have been allowed, so only an unrestricted agent may connect.
        let response = self
            .router
            .call_with_agent_grant(
                ManagedProviderCall {
                    provider: "composio".to_owned(),
                    tool_id: account.platform.composio_toolkit().to_owned(),
                    operation: Some("connect".to_owned()),
                    account_id: Some(account.id.clone()),
                    method: "POST".to_owned(),
                    query: Vec::new(),
                    body: None,
                    idempotency_key: None,
                    request_id: format!("social:composio:connect:{}", account.id),
                    fallback_cost_micro_usd: None,
                    task_label: Some("Outpost Composio account connect".to_owned()),
                    timeout_ms: None,
                    max_cost_micro_usd: None,
                },
                agent_grant,
            )
            .await
            .map_err(super::managed_call_error)?;
        // NOTE: the hosted OAuth flow is NOT completed here — the broker returns a
        // `redirectUrl` a human has to visit. We record the connected-account id so
        // subsequent calls can address it, and the account is only genuinely usable
        // once that flow finishes. Surfacing the URL to the UI needs a wider
        // `connect` return type than this trait has; until then the honest state is
        // "id recorded, authorization pending".
        Ok(response
            .body
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string))
    }

    async fn disconnect(&self, account: &ProviderAccount) -> anyhow::Result<()> {
        let Some(external_id) = account.external_id.as_deref().filter(|s| !s.is_empty()) else {
            // Nothing was ever connected: disconnecting is a no-op, not an error.
            return Ok(());
        };
        // A failed delete is not fatal — the local row is being dropped either way,
        // and leaving a stale broker link is better than refusing to disconnect.
        if let Err(error) = self
            .router
            .call(ManagedProviderCall {
                provider: "composio".to_owned(),
                tool_id: external_id.to_owned(),
                operation: Some("disconnect".to_owned()),
                account_id: None,
                method: "DELETE".to_owned(),
                query: Vec::new(),
                body: None,
                idempotency_key: None,
                request_id: format!("social:composio:disconnect:{external_id}"),
                fallback_cost_micro_usd: None,
                task_label: Some("Outpost Composio account disconnect".to_owned()),
                timeout_ms: None,
                max_cost_micro_usd: None,
            })
            .await
        {
            tracing::warn!(error = %error, "ryu-social: managed Composio disconnect failed; dropping the local link anyway");
        }
        Ok(())
    }

    /// A local video upload hashes, PUTs and then runs the upload tool, each
    /// with its own bound; the publish deadline covers all three. An elapsed
    /// deadline is not retried, because it may have cut off a running upload.
    fn publish_deadline(&self, request: &PublishRequest) -> PublishDeadline {
        let local = request.media.iter().find(|media| !media.is_remote());
        match (local, upload_tool(request.account.platform)) {
            (Some(media), Some(_)) => {
                let size = std::fs::metadata(local_media_path(media))
                    .map_or(MAX_STAGED_UPLOAD_BYTES, |m| m.len());
                // Hashing reads the file once before the PUT; allow 10 s per GiB.
                let hashing = std::time::Duration::from_secs(60 + size.div_ceil(1 << 30) * 10);
                PublishDeadline {
                    limit: hashing
                        + std::time::Duration::from_millis(crate::state::PROVIDER_CALL_TIMEOUT_MS)
                        + upload_timeout(size)
                        + upload_execute_timeout(size)
                        + std::time::Duration::from_millis(ManagedProviderCall::TIMEOUT_MARGIN_MS),
                    retry_after_timeout: false,
                }
            }
            _ => PublishDeadline::default(),
        }
    }

    async fn publish(&self, request: &PublishRequest) -> PublishResult {
        let platform = request.account.platform;

        let Some(connected_account_id) = request.account.external_id.as_deref() else {
            return PublishResult::err(format!(
                "{platform} account is not connected to Composio yet"
            ));
        };

        // A hosted broker cannot read this machine's filesystem, so a local file
        // has to be staged with the broker first. Outpost does that for the
        // platforms whose toolkit has a file-taking upload tool; everywhere else a
        // local path is refused up front rather than passed through, which would
        // have the broker publish a post with a silently missing attachment.
        if let Some(local) = request.media.iter().find(|m| !m.is_remote()) {
            return match upload_tool(platform) {
                Some(tool) => {
                    self.publish_uploaded_video(request, &tool, connected_account_id)
                        .await
                }
                None => PublishResult::err(format!(
                    "Composio cannot publish the local file \"{}\" to {platform}: Outpost stages local media only for YouTube and TikTok; use a public media URL for {platform}",
                    local.url
                )),
            };
        }

        // Threads/carousels are not modelled by this call: the broker takes one text
        // plus a media list, so a multi-segment post degrades to its first segment.
        // The pipeline has already validated the whole post against the platform's
        // limits, so this is a documented degrade, not silent truncation.
        let arguments = json!({
            "text": request.text,
            "media": request.media.iter().map(|m| m.url.clone()).collect::<Vec<_>>(),
            // Forwarded in the hope the broker honours it. It is not documented to,
            // which is precisely why the pipeline ALSO keeps a durable
            // already-published check rather than trusting this field.
            "idempotencyKey": request.idempotency_key,
        });

        match self
            .execute_tool(
                &tool_slug(platform, "CREATE_POST"),
                Some(connected_account_id),
                arguments,
                request.agent_grant.as_deref(),
            )
            .await
        {
            Ok(data) => {
                let Some(remote_id) = data.get("id").and_then(Value::as_str) else {
                    return PublishResult::err("Composio publish returned no post id");
                };
                PublishResult::Ok {
                    remote_id: remote_id.to_string(),
                    remote_url: data
                        .get("url")
                        .or_else(|| data.get("permalink"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                }
            }
            // Every expected failure — 4xx, 5xx, transport, a false `successful` —
            // becomes a value the retry loop can inspect. `publish` never errors.
            Err(e) => PublishResult::err(e.to_string()),
        }
    }

    async fn read_engagement(
        &self,
        post: &RemotePostRef,
        agent_grant: Option<&str>,
    ) -> anyhow::Result<EngagementCounts> {
        let data = self
            .execute_tool(
                &tool_slug(post.platform, "GET_POST"),
                post.connected_account_id.as_deref(),
                json!({ "postId": post.remote_id }),
                agent_grant,
            )
            .await?;
        Ok(EngagementCounts {
            likes: count_alias(&data, &["like_count", "likes"]),
            comments: count_alias(&data, &["comment_count", "reply_count"]),
            shares: count_alias(&data, &["share_count", "retweet_count"]),
            views: count_alias(&data, &["view_count", "impression_count"]),
            fetched_at: now_ms(),
        })
    }

    async fn capabilities(&self, platform: Platform) -> PlatformCapabilities {
        let response = self
            .router
            .call(ManagedProviderCall {
                provider: "composio".to_owned(),
                tool_id: platform.composio_toolkit().to_owned(),
                operation: Some("capabilities".to_owned()),
                account_id: None,
                method: "GET".to_owned(),
                query: Vec::new(),
                body: None,
                idempotency_key: None,
                request_id: format!("social:composio:capabilities:{platform}"),
                fallback_cost_micro_usd: None,
                task_label: Some("Outpost Composio capability discovery".to_owned()),
                timeout_ms: None,
                max_cost_micro_usd: None,
            })
            .await;
        let response = match response {
            Ok(response) if response.ok => response.body,
            _ => return PlatformCapabilities::empty(),
        };
        let slugs: Vec<String> = response
            .get("items")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|i| i.get("slug").and_then(Value::as_str))
                    .map(str::to_ascii_lowercase)
                    .collect()
            })
            .unwrap_or_default();

        let any = |needles: &[&str]| slugs.iter().any(|s| needles.iter().any(|n| s.contains(n)));
        // PER-SLUG conjunction, not whole-list. The upstream heuristic asked "does
        // ANY slug contain 'message' AND does ANY slug contain 'send'", which reports
        // `send_dm: true` for a toolkit exposing only `GET_USER` + `SEND_TWEET`.
        let any_slug_with_both = |a: &[&str], b: &[&str]| {
            slugs
                .iter()
                .any(|s| a.iter().any(|n| s.contains(n)) && b.iter().any(|n| s.contains(n)))
        };

        PlatformCapabilities {
            publish: any(&["post", "create", "tweet", "upload", "publish"]),
            read_comments: any(&["comment", "repl"]),
            read_dms: any_slug_with_both(&["message", "dm"], &["get", "list", "fetch"]),
            send_dm: any_slug_with_both(&["message", "dm"], &["send", "create"]),
            read_engagement: any(&["metric", "analytic", "insight"]),
            // ALWAYS false: scheduling is ours. Never delegated, for any provider.
            schedule: false,
        }
    }

    async fn read_inbox(
        &self,
        account: &ProviderAccount,
        agent_grant: Option<&str>,
    ) -> anyhow::Result<Vec<ProviderInboxItem>> {
        let platform = account.platform;
        let caps = self.capabilities(platform).await;
        let mut wanted: Vec<(String, InboxKind)> = Vec::new();
        if caps.read_comments {
            wanted.push((tool_slug(platform, "LIST_COMMENTS"), InboxKind::Comment));
            wanted.push((tool_slug(platform, "LIST_MENTIONS"), InboxKind::Mention));
        }
        if caps.read_dms {
            wanted.push((tool_slug(platform, "LIST_MESSAGES"), InboxKind::Dm));
        }

        let now = now_ms();
        let mut items = Vec::new();
        for (slug, kind) in wanted {
            // Each tool independently: one 404 must not lose the results of the
            // others, and a toolkit exposing comments but not mentions is normal.
            let data = match self
                .execute_tool(
                    &slug,
                    account.external_id.as_deref(),
                    json!({}),
                    agent_grant,
                )
                .await
            {
                Ok(data) => data,
                // Core refusing the agent is not a missing tool: report it.
                Err(e) if super::is_agent_access_refusal(&e) => return Err(e),
                Err(e) => {
                    tracing::debug!(tool = %slug, error = %e, "ryu-social: composio inbox tool unavailable");
                    continue;
                }
            };
            let Some(entries) = data.get("items").and_then(Value::as_array) else {
                continue;
            };
            for entry in entries {
                // No remote id means no dedupe key, and an inbox that duplicates on
                // every poll is worse than one that drops an unidentifiable item.
                let Some(external_id) = entry.get("id").and_then(Value::as_str) else {
                    continue;
                };
                items.push(ProviderInboxItem {
                    external_id: external_id.to_string(),
                    platform,
                    kind,
                    author: entry
                        .get("author")
                        .or_else(|| entry.get("username"))
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                    text: entry
                        .get("text")
                        .or_else(|| entry.get("body"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    permalink: entry
                        .get("url")
                        .or_else(|| entry.get("permalink"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    received_at: parse_remote_time(entry.get("created_at"), now),
                    connected_account_id: account.external_id.clone(),
                });
            }
        }
        Ok(items)
    }

    async fn reply_to_inbox_item(
        &self,
        item: &ProviderInboxItem,
        text: &str,
        agent_grant: Option<&str>,
    ) -> PublishResult {
        let slug = match item.kind {
            InboxKind::Dm => tool_slug(item.platform, "SEND_MESSAGE"),
            _ => tool_slug(item.platform, "REPLY_TO_COMMENT"),
        };
        match self
            .execute_tool(
                &slug,
                item.connected_account_id.as_deref(),
                json!({ "parentId": item.external_id, "text": text }),
                agent_grant,
            )
            .await
        {
            Ok(data) => PublishResult::Ok {
                remote_id: data
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                remote_url: data
                    .get("url")
                    .or_else(|| data.get("permalink"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            },
            Err(e) => PublishResult::err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> ComposioProvider {
        ComposioProvider::managed(ProviderRouter::for_test(
            crate::state::PLUGIN_ID,
            reqwest::Client::new(),
            "http://127.0.0.1:1/providers/call",
            "http://127.0.0.1:1/providers/status",
        ))
    }

    #[test]
    fn tool_slugs_use_the_toolkit_name_and_x_maps_to_twitter() {
        assert_eq!(tool_slug(Platform::X, "CREATE_POST"), "TWITTER_CREATE_POST");
        assert_eq!(
            tool_slug(Platform::Linkedin, "LIST_COMMENTS"),
            "LINKEDIN_LIST_COMMENTS"
        );
    }

    #[tokio::test]
    async fn a_local_media_path_is_rejected_before_any_network_call() {
        let p = provider();
        let request = PublishRequest {
            account: ProviderAccount {
                id: "acc_1".into(),
                platform: Platform::X,
                label: None,
                external_id: Some("ca_1".into()),
            },
            text: "hi".into(),
            media: vec![super::super::types::PublishMedia {
                url: "/Users/me/cat.png".into(),
                mime_type: "image/png".into(),
                alt_text: None,
            }],
            segments: None,
            idempotency_key: Some("sp_1:acc_1".into()),
            agent_grant: None,
        };
        let result = p.publish(&request).await;
        assert!(result
            .error()
            .unwrap()
            .contains("cannot publish the local file"));
    }

    #[tokio::test]
    async fn an_unreachable_broker_yields_an_empty_matrix_rather_than_an_error() {
        // Port 1 on loopback refuses instantly, so this is a real transport failure
        // with no network dependency.
        let caps = provider().capabilities(Platform::X).await;
        assert_eq!(caps, PlatformCapabilities::empty());
    }

    #[tokio::test]
    async fn an_unconnected_account_fails_the_publish_as_a_value_not_an_error() {
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
            agent_grant: None,
        };
        assert!(provider()
            .publish(&request)
            .await
            .error()
            .unwrap()
            .contains("not connected"));
    }

    #[test]
    fn only_youtube_and_tiktok_have_upload_tools() {
        assert_eq!(
            upload_tool(Platform::Youtube).map(|tool| tool.file_param),
            Some("videoFilePath")
        );
        assert_eq!(
            upload_tool(Platform::Tiktok).map(|tool| tool.slug),
            Some("TIKTOK_UPLOAD_VIDEO")
        );
        for platform in [Platform::X, Platform::Instagram, Platform::Linkedin] {
            assert!(upload_tool(platform).is_none());
        }
    }

    #[test]
    fn youtube_titles_come_from_the_first_line_within_limits() {
        assert_eq!(
            youtube_title_and_description("Launch <day>\n\nAll of it.\n0:00 Intro"),
            ("Launch day".to_owned(), "All of it.\n0:00 Intro".to_owned())
        );
        let (title, description) = youtube_title_and_description(&"t".repeat(300));
        assert_eq!(title.chars().count(), YOUTUBE_TITLE_CHARS);
        assert!(description.is_empty());
        assert_eq!(youtube_title_and_description("  ").0, "Untitled");
    }

    #[test]
    fn upload_results_need_an_id() {
        assert!(upload_result(Platform::Youtube, &json!({}))
            .error()
            .is_some());
        assert_eq!(
            upload_result(Platform::Youtube, &json!({ "id": "abc" })),
            PublishResult::Ok {
                remote_id: "abc".into(),
                remote_url: Some("https://www.youtube.com/watch?v=abc".into()),
            }
        );
        let missing = upload_result(Platform::Tiktok, &json!({ "data": {} }));
        assert!(missing.error().unwrap().contains("without a publish id"));
        // The tool reported success, so a missing id is never retried.
        assert!(!missing.is_retryable());
    }

    #[test]
    fn ids_are_found_inside_json_encoded_data_strings() {
        let encoded = json!({ "data": json!({ "id": "yt_9" }).to_string() });
        assert_eq!(find_id(&encoded, &["id"]).as_deref(), Some("yt_9"));
        let nested =
            json!({ "data": json!({ "response_data": { "videoId": "yt_10" } }).to_string() });
        assert_eq!(find_id(&nested, &["videoId"]).as_deref(), Some("yt_10"));
        assert_eq!(find_id(&json!({ "data": "not json" }), &["id"]), None);
    }

    #[test]
    fn router_refusals_before_dispatch_are_the_only_retryable_upload_failures() {
        assert!(was_refused_before_dispatch(&ProviderRouterError::NotHosted));
        for status in [400, 402, 429] {
            assert!(was_refused_before_dispatch(
                &ProviderRouterError::Rejected {
                    status,
                    body: String::new()
                }
            ));
        }
        for status in [500, 502, 503, 504] {
            assert!(!was_refused_before_dispatch(
                &ProviderRouterError::Rejected {
                    status,
                    body: String::new()
                }
            ));
        }
    }

    #[test]
    fn upload_executes_get_a_long_bounded_timeout() {
        assert_eq!(
            upload_execute_timeout(0),
            std::time::Duration::from_secs(600)
        );
        assert_eq!(
            upload_execute_timeout(u64::MAX / 2),
            std::time::Duration::from_millis(ManagedProviderCall::MAX_TIMEOUT_MS)
        );
    }

    #[test]
    fn remote_times_coerce_from_seconds_millis_and_rfc3339() {
        let now = 1_700_000_000_000;
        assert_eq!(
            parse_remote_time(Some(&json!(1_600_000_000)), now),
            1_600_000_000_000
        );
        assert_eq!(
            parse_remote_time(Some(&json!(1_600_000_000_000i64)), now),
            1_600_000_000_000
        );
        assert_eq!(
            parse_remote_time(Some(&json!("2023-01-01T00:00:00Z")), now),
            1_672_531_200_000
        );
        assert_eq!(parse_remote_time(None, now), now);
        assert_eq!(parse_remote_time(Some(&json!("nonsense")), now), now);
    }

    /// What the mock Core saw on one managed provider call.
    type SeenCall = (Option<String>, Option<String>, Option<String>);

    /// A mock Core: capability discovery answers with an inbox tool and a
    /// reply tool, and every call that carries an agent grant is refused for
    /// the agent's account access, unless it names `ca_allowed`.
    async fn limited_agent_core() -> (
        ComposioProvider,
        std::sync::Arc<std::sync::Mutex<Vec<SeenCall>>>,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
        type Seen = std::sync::Arc<std::sync::Mutex<Vec<SeenCall>>>;

        async fn call(
            State(seen): State<Seen>,
            headers: HeaderMap,
            Json(call): Json<ManagedProviderCall>,
        ) -> (axum::http::StatusCode, Json<Value>) {
            let grant = headers
                .get(ryu_app_events::HDR_AGENT_GRANT)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            seen.lock().expect("lock").push((
                call.operation.clone(),
                call.account_id.clone(),
                grant.clone(),
            ));
            if call.operation.as_deref() == Some("capabilities") {
                return (
                    axum::http::StatusCode::OK,
                    Json(json!({ "ok": true, "status": 200, "body": { "items": [
                        { "slug": "TWITTER_LIST_COMMENTS" },
                        { "slug": "TWITTER_REPLY_TO_COMMENT" }
                    ] } })),
                );
            }
            if grant.is_some() && call.account_id.as_deref() != Some("ca_allowed") {
                return (
                    axum::http::StatusCode::FORBIDDEN,
                    Json(json!({
                        "error": "this agent is not allowed to use that connected account",
                        "reason": "agent_account_access"
                    })),
                );
            }
            (
                axum::http::StatusCode::OK,
                Json(json!({ "ok": true, "status": 200, "body": { "id": "r_1", "items": [] } })),
            )
        }

        let seen: Seen = std::sync::Arc::default();
        let app = Router::new()
            .route("/providers/call", post(call))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("mock listener");
        let address = listener.local_addr().expect("mock address");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let provider = ComposioProvider::managed(ProviderRouter::for_test(
            crate::state::PLUGIN_ID,
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("proxy-free client"),
            format!("http://{address}/providers/call"),
            format!("http://{address}/providers/status"),
        ));
        (provider, seen, server)
    }

    fn x_account(external_id: &str) -> ProviderAccount {
        ProviderAccount {
            id: "acc_1".into(),
            platform: Platform::X,
            label: None,
            external_id: Some(external_id.into()),
        }
    }

    /// An agent's inbox refresh and account connect carry its grant, name the
    /// account, and Core's refusal comes back as an error rather than an empty
    /// inbox.
    #[tokio::test]
    async fn inbox_reads_and_connects_hand_the_agent_grant_back_and_fail_on_refusal() {
        let (provider, seen, server) = limited_agent_core().await;

        let inbox = provider
            .read_inbox(&x_account("ca_other"), Some("grant.sig"))
            .await;
        let connect = provider
            .connect(&x_account("ca_other"), Some("grant.sig"))
            .await;
        server.abort();

        let inbox_error = inbox
            .expect_err("a refused inbox read is reported")
            .to_string();
        assert!(inbox_error.contains("Ryu blocked this"), "{inbox_error}");
        assert!(
            connect.is_err(),
            "a limited agent may not start a connection"
        );
        let seen = seen.lock().expect("lock").clone();
        assert!(seen.contains(&(
            Some("execute".to_owned()),
            Some("ca_other".to_owned()),
            Some("grant.sig".to_owned())
        )));
        assert!(seen.contains(&(
            Some("connect".to_owned()),
            Some("acc_1".to_owned()),
            Some("grant.sig".to_owned())
        )));
    }

    /// A reply names the connected account it goes out from, so an agent
    /// allowed that account can reply.
    #[tokio::test]
    async fn a_reply_names_its_connected_account_for_a_limited_agent() {
        let (provider, seen, server) = limited_agent_core().await;
        let item = ProviderInboxItem {
            external_id: "c_1".into(),
            platform: Platform::X,
            kind: InboxKind::Comment,
            author: "@someone".into(),
            text: "hi".into(),
            permalink: None,
            received_at: 0,
            connected_account_id: Some("ca_allowed".into()),
        };

        let result = provider
            .reply_to_inbox_item(&item, "thanks", Some("grant.sig"))
            .await;
        server.abort();

        assert!(result.is_ok(), "{:?}", result.error());
        assert_eq!(
            seen.lock().expect("lock").clone(),
            vec![(
                Some("execute".to_owned()),
                Some("ca_allowed".to_owned()),
                Some("grant.sig".to_owned())
            )]
        );
    }
}
