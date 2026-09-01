//! Treg-backed social operations.
//!
//! Outpost does not call X directly. It discovers the operation in Treg's
//! public catalog, asks Ryu's managed provider bridge to execute the catalog
//! endpoint, and lets Gateway charge the bound organization wallet from the
//! provider response metadata. The app never receives the Treg credential.

use async_trait::async_trait;
use serde_json::{json, Value};

use super::types::{
    PlatformProvider, ProviderAccount, ProviderId, PublishRequest, PublishResult, RemotePostRef,
};
use crate::models::{EngagementCounts, Platform, PlatformCapabilities};

const DEFAULT_BASE_URL: &str = "https://treg.to";

#[derive(Debug, Clone)]
struct EndpointSpec {
    id: String,
    method: reqwest::Method,
    cost_micro_usd: u64,
}

#[derive(Debug)]
struct TregResponse {
    status: reqwest::StatusCode,
    body: Value,
    cost_micro_usd: Option<u64>,
    call_id: Option<String>,
}

pub struct TregProvider {
    http: reqwest::Client,
    base_url: String,
    router: ryu_app_events::ProviderRouter,
}

impl std::fmt::Debug for TregProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TregProvider")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl TregProvider {
    pub fn new(
        http: reqwest::Client,
        base_url: Option<String>,
        router: ryu_app_events::ProviderRouter,
    ) -> Self {
        Self {
            http,
            base_url: base_url
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_BASE_URL.to_owned())
                .trim_end_matches('/')
                .to_owned(),
            router,
        }
    }

    async fn endpoint_detail(&self, endpoint_id: &str) -> anyhow::Result<Value> {
        let url = format!(
            "{}/catalog/endpoints/{}",
            self.base_url,
            urlencode(endpoint_id)
        );
        let response = self
            .http
            .get(url)
            .header("X-Treg-Client", "ryu-social")
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("Treg catalog returned HTTP {status}");
        }
        response.json().await.map_err(Into::into)
    }

    async fn endpoint_for(
        &self,
        platform: Platform,
        capability: &str,
    ) -> anyhow::Result<Option<EndpointSpec>> {
        let Some(endpoint_id) = endpoint_id_for(platform, capability) else {
            return Ok(None);
        };
        let body = self.endpoint_detail(endpoint_id).await?;
        Ok(parse_endpoint(&body, endpoint_id))
    }

    async fn call(
        &self,
        endpoint: &EndpointSpec,
        body: Value,
        idempotency_key: Option<String>,
        request_id: String,
    ) -> anyhow::Result<TregResponse> {
        let response = self
            .router
            .call(ryu_app_events::ManagedProviderCall {
                provider: "treg".to_owned(),
                tool_id: endpoint.id.clone(),
                operation: Some("execute".to_owned()),
                account_id: None,
                method: endpoint.method.to_string(),
                query: Vec::new(),
                body: Some(body),
                idempotency_key,
                request_id,
                fallback_cost_micro_usd: Some(endpoint.cost_micro_usd),
                task_label: Some("Outpost social publish".to_owned()),
            })
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let status = reqwest::StatusCode::from_u16(response.status)
            .map_err(|error| anyhow::anyhow!("Gateway returned invalid Treg status: {error}"))?;
        Ok(TregResponse {
            status,
            body: response.body,
            cost_micro_usd: response.cost_micro_usd,
            call_id: response.call_id,
        })
    }

    fn remote_url(platform: Platform, id: &str) -> Option<String> {
        (platform == Platform::X).then(|| format!("https://x.com/i/web/status/{id}"))
    }
}

#[async_trait]
impl PlatformProvider for TregProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Treg
    }

    async fn connect(&self, account: &ProviderAccount) -> anyhow::Result<Option<String>> {
        if !self
            .router
            .status("treg")
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?
        {
            anyhow::bail!(
                "Ryu's managed Treg provider is not configured for the {} account",
                account.platform
            );
        }
        Ok(Some("treg".to_owned()))
    }

    async fn disconnect(&self, _account: &ProviderAccount) -> anyhow::Result<()> {
        Ok(())
    }

    async fn publish(&self, request: &PublishRequest) -> PublishResult {
        if request.account.platform != Platform::X {
            return PublishResult::err(format!(
                "Treg publishing is not configured for {}",
                request.account.platform
            ));
        }
        if let Some(media) = request.media.iter().find(|media| !media.is_remote()) {
            return PublishResult::err(format!(
                "Treg cannot publish the local file \"{}\"",
                media.url
            ));
        }
        if !request.media.is_empty() {
            return PublishResult::err(
                "Treg X publishing currently supports text-only posts and threads",
            );
        }

        let segments = request.effective_segments();
        let mut parent_id: Option<String> = None;
        let mut last_url = None;
        for (index, segment) in segments.iter().enumerate() {
            let capability = if index == 0 {
                "x.post.create"
            } else {
                "x.post.reply"
            };
            let endpoint = match self.endpoint_for(Platform::X, capability).await {
                Ok(Some(endpoint)) => endpoint,
                Ok(None) => {
                    return PublishResult::err(format!(
                        "Treg catalog has no {} endpoint for X",
                        capability
                    ));
                }
                Err(error) => return PublishResult::err(error.to_string()),
            };
            let body = if let Some(parent_id) = parent_id.as_deref() {
                json!({
                    "text": segment.text,
                    "reply": { "in_reply_to_tweet_id": parent_id }
                })
            } else {
                json!({ "text": segment.text })
            };
            let request_id = request
                .segment_key(index)
                .unwrap_or_else(|| format!("social:treg:{}:{index}", uuid::Uuid::new_v4()));
            let response = match self
                .call(&endpoint, body, request.segment_key(index), request_id)
                .await
            {
                Ok(response) => response,
                Err(error) => return PublishResult::err(error.to_string()),
            };
            if !response.status.is_success() {
                return PublishResult::err(format!(
                    "Treg endpoint {} returned HTTP {}",
                    endpoint.id, response.status
                ));
            }
            let Some(remote_id) = response
                .body
                .get("data")
                .and_then(|data| data.get("id"))
                .and_then(Value::as_str)
            else {
                return PublishResult::err(format!(
                    "Treg endpoint {} returned no post id",
                    endpoint.id
                ));
            };
            parent_id = Some(remote_id.to_owned());
            last_url = Self::remote_url(Platform::X, remote_id);
        }

        let Some(remote_id) = parent_id else {
            return PublishResult::err("Treg refused an empty X thread");
        };
        PublishResult::Ok {
            remote_id,
            remote_url: last_url,
        }
    }

    async fn read_engagement(&self, _post: &RemotePostRef) -> anyhow::Result<EngagementCounts> {
        anyhow::bail!("Treg engagement mapping is not enabled for Outpost yet")
    }

    async fn capabilities(&self, platform: Platform) -> PlatformCapabilities {
        if platform != Platform::X {
            return PlatformCapabilities::empty();
        }
        let Ok(Some(endpoint)) = self.endpoint_for(Platform::X, "x.post.create").await else {
            return PlatformCapabilities::empty();
        };
        PlatformCapabilities {
            publish: endpoint.id == "x.x.post.create"
                && self.router.status("treg").await.unwrap_or(false),
            // Treg's public Threads/X data endpoints are not wired into Outpost's
            // inbox/analytics shapes yet, so do not advertise them prematurely.
            read_comments: false,
            read_dms: false,
            send_dm: false,
            read_engagement: false,
            schedule: false,
        }
    }
}

fn endpoint_id_for(platform: Platform, capability: &str) -> Option<&'static str> {
    match (platform, capability) {
        (Platform::X, "x.post.create") => Some("x.x.post.create"),
        (Platform::X, "x.post.reply") => Some("x.x.post.reply"),
        _ => None,
    }
}

fn parse_endpoint(body: &Value, expected_id: &str) -> Option<EndpointSpec> {
    let endpoint = body.get("endpoint")?;
    let id = endpoint
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| *id == expected_id)?;
    let method = endpoint
        .get("method")
        .and_then(Value::as_str)
        .and_then(|method| method.parse().ok())?;
    let usd = endpoint
        .get("cost")
        .and_then(|cost| cost.get("usd"))
        .and_then(Value::as_f64)?;
    let cost_micro_usd = usd_to_micro_usd(usd)?;
    Some(EndpointSpec {
        id: id.to_owned(),
        method,
        cost_micro_usd,
    })
}

fn usd_to_micro_usd(usd: f64) -> Option<u64> {
    if !usd.is_finite() || usd <= 0.0 {
        return None;
    }
    Some((usd * 1_000_000.0).ceil() as u64)
}

fn urlencode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::types::ProviderOperation;
    use axum::{
        extract::{Path, State},
        routing::{get, post},
        Json, Router,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MockState {
        calls: Arc<Mutex<Vec<ryu_app_events::ManagedProviderCall>>>,
    }

    #[test]
    fn parses_x_thread_capabilities_and_prices() {
        let create = parse_endpoint(
            &json!({
                "endpoint": {
                    "id": "x.x.post.create",
                    "method": "POST",
                    "cost": { "usd": 0.015 }
                }
            }),
            "x.x.post.create",
        )
        .expect("create endpoint");
        let reply = parse_endpoint(
            &json!({
                "endpoint": {
                    "id": "x.x.post.reply",
                    "method": "POST",
                    "cost": { "usd": 0.015 }
                }
            }),
            "x.x.post.reply",
        )
        .expect("reply endpoint");
        assert_eq!(create.id, "x.x.post.create");
        assert_eq!(create.cost_micro_usd, 15_000);
        assert_eq!(reply.id, "x.x.post.reply");
        assert_eq!(
            ProviderOperation::Publish.is_supported(PlatformCapabilities {
                publish: true,
                ..PlatformCapabilities::empty()
            }),
            true
        );
    }

    #[test]
    fn urls_are_encoded() {
        assert_eq!(urlencode("x.x.post.create"), "x.x.post.create");
        assert_eq!(urlencode("x/post"), "x%2Fpost");
    }

    #[tokio::test]
    async fn publish_chains_text_segments_through_create_and_reply() {
        async fn endpoint(Path(endpoint): Path<String>) -> Json<Value> {
            Json(json!({
                "endpoint": {
                    "id": endpoint,
                    "method": "POST",
                    "cost": { "usd": 0.015 }
                }
            }))
        }

        async fn call(
            State(state): State<MockState>,
            Json(body): Json<ryu_app_events::ManagedProviderCall>,
        ) -> Json<Value> {
            let mut calls = state.calls.lock().expect("mock calls lock");
            let id = format!("post-{}", calls.len() + 1);
            calls.push(body);
            Json(json!({
                "ok": true,
                "status": 201,
                "body": { "data": { "id": id } },
                "costMicroUsd": 15000,
                "callId": format!("call-{}", calls.len())
            }))
        }

        let state = MockState::default();
        let app = Router::new()
            .route("/catalog/endpoints/:endpoint", get(endpoint))
            .route("/providers/call", post(call))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("mock listener");
        let address = listener.local_addr().expect("mock address");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let provider = TregProvider::new(
            reqwest::Client::new(),
            Some(format!("http://{address}")),
            ryu_app_events::ProviderRouter::for_test(
                crate::state::PLUGIN_ID,
                reqwest::Client::new(),
                format!("http://{address}/providers/call"),
                format!("http://{address}/providers/status"),
            ),
        );
        let request = PublishRequest {
            account: super::super::types::ProviderAccount {
                id: "acc_x".to_owned(),
                platform: Platform::X,
                label: None,
                external_id: None,
            },
            text: "first".to_owned(),
            media: Vec::new(),
            segments: Some(vec![
                super::super::types::PublishSegment {
                    text: "first".to_owned(),
                    media: Vec::new(),
                },
                super::super::types::PublishSegment {
                    text: "second".to_owned(),
                    media: Vec::new(),
                },
            ]),
            idempotency_key: Some("post:account".to_owned()),
        };

        let result = provider.publish(&request).await;
        server.abort();

        let calls = state.calls.lock().expect("mock calls lock");
        assert!(matches!(result, PublishResult::Ok { ref remote_id, .. } if remote_id == "post-2"));
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].tool_id, "x.x.post.create");
        assert_eq!(calls[0].body.as_ref().expect("body")["text"], "first");
        assert_eq!(calls[1].tool_id, "x.x.post.reply");
        assert_eq!(
            calls[1].body.as_ref().expect("body")["reply"]["in_reply_to_tweet_id"],
            "post-1"
        );
        assert!(calls[0]
            .idempotency_key
            .as_deref()
            .is_some_and(|value| !value.is_empty()));
        assert_ne!(calls[0].idempotency_key, calls[1].idempotency_key);
    }
}
