//! Composio: the BYO-key broker that fronts every platform we have no first-party
//! adapter for.
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
//! The API key is read from the environment ([`super::registry`]), never from a
//! const, never from `SocialSettings` (that blob is serialized to the client on every
//! `GET /settings`), and never logged: it lives behind [`ApiKey`], whose `Debug` is
//! redacted, and every error string this module produces goes through
//! [`ComposioProvider::redact`] before it leaves — because a broker that echoes the
//! request back in an error body would otherwise put the key into a `post_history`
//! row and from there onto the user's screen.

use async_trait::async_trait;
use serde_json::{json, Value};

use super::types::{
    PlatformProvider, ProviderAccount, ProviderId, ProviderInboxItem, PublishRequest,
    PublishResult, RemotePostRef,
};
use crate::models::{now_ms, EngagementCounts, InboxKind, Platform, PlatformCapabilities};

const DEFAULT_BASE_URL: &str = "https://backend.composio.dev/api/v3";

/// A key that cannot be printed by accident.
#[derive(Clone)]
pub struct ApiKey(String);

impl ApiKey {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiKey(***)")
    }
}

#[derive(Debug)]
pub struct ComposioProvider {
    http: reqwest::Client,
    api_key: ApiKey,
    base_url: String,
}

impl ComposioProvider {
    /// Construction is side-effect free — no network, no validation call. The
    /// registry builds providers eagerly, and a constructor that dialled out would
    /// turn "render the capability matrix" into a multi-second operation.
    pub fn new(http: reqwest::Client, api_key: ApiKey, base_url: Option<String>) -> Self {
        Self {
            http,
            api_key,
            base_url: base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
        }
    }

    /// Strip the API key out of anything on its way to a caller. Cheap insurance: the
    /// broker's error bodies are opaque to us, and a `post_history.error` row is
    /// rendered verbatim in the UI.
    fn redact(&self, text: impl Into<String>) -> String {
        let text = text.into();
        let key = self.api_key.expose();
        if key.is_empty() {
            return text;
        }
        text.replace(key, "***")
    }

    async fn request(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> anyhow::Result<Value> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self
            .http
            .request(method, &url)
            .header("x-api-key", self.api_key.expose())
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(self.redact(e.to_string())))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            // The path, not the full URL: the base URL is not a secret but there is
            // no reason to widen what lands in a stored error string.
            anyhow::bail!(self.redact(format!(
                "Composio {path} failed: {} {}",
                status.as_u16(),
                text
            )));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!(self.redact(format!("Composio {path} returned unreadable JSON: {e}"))))
    }

    /// Run one broker tool and unwrap its envelope.
    ///
    /// `{ successful: false }` or a non-empty `error` is a FAILED tool call even
    /// though the HTTP status was 200 — collapsing the two is how a failed publish
    /// gets recorded as published.
    async fn execute_tool(&self, slug: &str, connected_account_id: Option<&str>, arguments: Value) -> anyhow::Result<Value> {
        let mut body = json!({ "toolSlug": slug, "arguments": arguments });
        if let Some(id) = connected_account_id {
            body["connectedAccountId"] = json!(id);
        }
        let response = self
            .request(reqwest::Method::POST, "/tools/execute", Some(body))
            .await?;
        if response.get("successful").and_then(Value::as_bool) == Some(false) {
            let message = response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Composio tool call failed");
            anyhow::bail!(self.redact(message.to_string()));
        }
        if let Some(error) = response.get("error").and_then(Value::as_str) {
            if !error.trim().is_empty() {
                anyhow::bail!(self.redact(error.to_string()));
            }
        }
        Ok(response.get("data").cloned().unwrap_or(Value::Null))
    }
}

/// `{TOOLKIT}_{VERB}` — the slug shape. See the module docs: the VERB half is a
/// guess for every verb below.
fn tool_slug(platform: Platform, verb: &str) -> String {
    format!("{}_{verb}", platform.composio_toolkit().to_ascii_uppercase())
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
        Some(Value::Number(n)) => n.as_i64().map_or(now, |v| if v < 100_000_000_000 { v * 1_000 } else { v }),
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

    async fn connect(&self, account: &ProviderAccount) -> anyhow::Result<Option<String>> {
        let response = self
            .request(
                reqwest::Method::POST,
                "/connected_accounts",
                Some(json!({
                    "toolkit": account.platform.composio_toolkit(),
                    "userId": account.id,
                })),
            )
            .await?;
        // NOTE: the hosted OAuth flow is NOT completed here — the broker returns a
        // `redirectUrl` a human has to visit. We record the connected-account id so
        // subsequent calls can address it, and the account is only genuinely usable
        // once that flow finishes. Surfacing the URL to the UI needs a wider
        // `connect` return type than this trait has; until then the honest state is
        // "id recorded, authorization pending".
        Ok(response
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string))
    }

    async fn disconnect(&self, account: &ProviderAccount) -> anyhow::Result<()> {
        let Some(external_id) = account.external_id.as_deref().filter(|s| !s.is_empty()) else {
            // Nothing was ever connected: disconnecting is a no-op, not an error.
            return Ok(());
        };
        let path = format!("/connected_accounts/{}", urlencode(external_id));
        // A failed delete is not fatal — the local row is being dropped either way,
        // and leaving a stale broker link is better than refusing to disconnect.
        if let Err(e) = self.request(reqwest::Method::DELETE, &path, None).await {
            tracing::warn!(error = %e, "ryu-social: composio disconnect failed; dropping the local link anyway");
        }
        Ok(())
    }

    async fn publish(&self, request: &PublishRequest) -> PublishResult {
        let platform = request.account.platform;

        // A hosted broker cannot read this machine's filesystem. Rejecting up front
        // is the honest failure: passing a local path through would have the broker
        // publish a post with a silently missing image.
        if let Some(local) = request.media.iter().find(|m| !m.is_remote()) {
            return PublishResult::err(format!(
                "Composio cannot publish the local file \"{}\" — media must be uploaded to a URL the broker can fetch",
                local.url
            ));
        }

        let Some(connected_account_id) = request.account.external_id.as_deref() else {
            return PublishResult::err(format!(
                "{platform} account is not connected to Composio yet"
            ));
        };

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
            .execute_tool(&tool_slug(platform, "CREATE_POST"), Some(connected_account_id), arguments)
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
            Err(e) => PublishResult::err(self.redact(e.to_string())),
        }
    }

    async fn read_engagement(&self, post: &RemotePostRef) -> anyhow::Result<EngagementCounts> {
        let data = self
            .execute_tool(
                &tool_slug(post.platform, "GET_POST"),
                None,
                json!({ "postId": post.remote_id }),
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
        let path = format!(
            "/tools?toolkit_slug={}&limit=200",
            urlencode(platform.composio_toolkit())
        );
        let Ok(response) = self.request(reqwest::Method::GET, &path, None).await else {
            // Unreachable broker → all-false, never an error. One bad platform must
            // not blank the whole matrix.
            return PlatformCapabilities::empty();
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

        let any = |needles: &[&str]| {
            slugs
                .iter()
                .any(|s| needles.iter().any(|n| s.contains(n)))
        };
        // PER-SLUG conjunction, not whole-list. The upstream heuristic asked "does
        // ANY slug contain 'message' AND does ANY slug contain 'send'", which reports
        // `send_dm: true` for a toolkit exposing only `GET_USER` + `SEND_TWEET`.
        let any_slug_with_both = |a: &[&str], b: &[&str]| {
            slugs.iter().any(|s| {
                a.iter().any(|n| s.contains(n)) && b.iter().any(|n| s.contains(n))
            })
        };

        PlatformCapabilities {
            publish: any(&["post", "create", "tweet"]),
            read_comments: any(&["comment", "repl"]),
            read_dms: any_slug_with_both(&["message", "dm"], &["get", "list", "fetch"]),
            send_dm: any_slug_with_both(&["message", "dm"], &["send", "create"]),
            read_engagement: any(&["metric", "analytic", "insight"]),
            // ALWAYS false: scheduling is ours. Never delegated, for any provider.
            schedule: false,
        }
    }

    async fn read_inbox(&self, account: &ProviderAccount) -> anyhow::Result<Vec<ProviderInboxItem>> {
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
                .execute_tool(&slug, account.external_id.as_deref(), json!({}))
                .await
            {
                Ok(data) => data,
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
                });
            }
        }
        Ok(items)
    }

    async fn reply_to_inbox_item(&self, item: &ProviderInboxItem, text: &str) -> PublishResult {
        let slug = match item.kind {
            InboxKind::Dm => tool_slug(item.platform, "SEND_MESSAGE"),
            _ => tool_slug(item.platform, "REPLY_TO_COMMENT"),
        };
        match self
            .execute_tool(
                &slug,
                None,
                json!({ "parentId": item.external_id, "text": text }),
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
            Err(e) => PublishResult::err(self.redact(e.to_string())),
        }
    }
}

/// Minimal percent-encoding for a path/query segment.
///
/// Hand-rolled rather than pulling in `urlencoding`: the crate list here is matched
/// to Core's to keep the shared lockfile still, and the only inputs are toolkit slugs
/// and broker ids.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> ComposioProvider {
        ComposioProvider::new(
            reqwest::Client::new(),
            ApiKey::new("super-secret-key"),
            Some("http://127.0.0.1:1/api/v3".to_string()),
        )
    }

    #[test]
    fn the_api_key_never_survives_a_debug_print_or_an_error_string() {
        let p = provider();
        assert_eq!(format!("{:?}", p.api_key), "ApiKey(***)");
        assert!(!format!("{p:?}").contains("super-secret-key"));
        assert_eq!(
            p.redact("boom: x-api-key=super-secret-key rejected"),
            "boom: x-api-key=*** rejected"
        );
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
        };
        let result = p.publish(&request).await;
        assert!(result.error().unwrap().contains("cannot publish the local file"));
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
        };
        assert!(provider()
            .publish(&request)
            .await
            .error()
            .unwrap()
            .contains("not connected"));
    }

    #[test]
    fn remote_times_coerce_from_seconds_millis_and_rfc3339() {
        let now = 1_700_000_000_000;
        assert_eq!(parse_remote_time(Some(&json!(1_600_000_000)), now), 1_600_000_000_000);
        assert_eq!(parse_remote_time(Some(&json!(1_600_000_000_000i64)), now), 1_600_000_000_000);
        assert_eq!(
            parse_remote_time(Some(&json!("2023-01-01T00:00:00Z")), now),
            1_672_531_200_000
        );
        assert_eq!(parse_remote_time(None, now), now);
        assert_eq!(parse_remote_time(Some(&json!("nonsense")), now), now);
    }

    #[test]
    fn urlencoding_escapes_everything_outside_the_unreserved_set() {
        assert_eq!(urlencode("abc-123_x.y~z"), "abc-123_x.y~z");
        assert_eq!(urlencode("a/b c"), "a%2Fb%20c");
    }
}
