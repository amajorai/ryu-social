use super::media::*;
use super::*;

/// What the fake Gateway router and fake storage endpoint saw.
#[derive(Clone, Default)]
struct BrokerLog {
    calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    /// The agent grant header of each call, in the same order as `calls`.
    grants: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
    uploads: std::sync::Arc<std::sync::Mutex<Vec<(Vec<u8>, Option<String>, Option<String>)>>>,
    storage: std::sync::Arc<std::sync::Mutex<String>>,
    refuse_upload: bool,
    /// Answer the upload tool's `execute` with this HTTP status, as Core
    /// relays a Gateway timeout or upstream failure.
    execute_status: Option<u16>,
    /// Answer the upload tool with `data` as a JSON-encoded string.
    string_data: bool,
    /// `Content-MD5` headers the fake storage saw.
    content_md5: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

/// A fake of Core's `providers.call` bridge (and the Gateway behind it) and
/// of the presigned storage endpoint, on one loopback server.
async fn fake_broker(log: BrokerLog) -> (String, tokio::task::JoinHandle<()>) {
    use axum::routing::{post, put};
    async fn call(
        State(log): State<BrokerLog>,
        headers: axum::http::HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (axum::http::StatusCode, Json<serde_json::Value>) {
        log.grants.lock().unwrap().push(
            headers
                .get(ryu_app_events::HDR_AGENT_GRANT)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
        log.calls.lock().unwrap().push(body.clone());
        if body["operation"] == json!("execute") {
            if let Some(status) = log.execute_status {
                return (
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    Json(json!({ "error": "upstream timed out" })),
                );
            }
            if log.string_data {
                let id = json!({ "id": "yt_string_1" }).to_string();
                return (
                    axum::http::StatusCode::OK,
                    Json(json!({ "ok": true, "status": 200, "body": { "data": id } })),
                );
            }
        }
        let reply = match (body["operation"].as_str(), body["toolId"].as_str()) {
            (Some("capabilities"), Some(toolkit)) => json!({
                "items": [{ "slug": format!("{}_UPLOAD_VIDEO", toolkit.to_ascii_uppercase()) }]
            }),
            (Some("file_upload"), _) => json!({
                "key": "uploads/fixture/render.mp4",
                "uploadUrl": format!("{}/put?sig=fixture", log.storage.lock().unwrap()),
                "storageBackend": "s3",
            }),
            (Some("execute"), Some("YOUTUBE_UPLOAD_VIDEO")) => {
                json!({ "response_data": { "id": "yt_video_1" } })
            }
            (Some("execute"), Some("TIKTOK_UPLOAD_VIDEO")) => {
                json!({ "data": { "publish_id": "v_pub_1" } })
            }
            _ => json!({}),
        };
        (
            axum::http::StatusCode::OK,
            Json(json!({ "ok": true, "status": 200, "body": reply })),
        )
    }
    async fn upload(
        State(log): State<BrokerLog>,
        headers: axum::http::HeaderMap,
        body: axum::body::Bytes,
    ) -> axum::http::StatusCode {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        log.uploads.lock().unwrap().push((
            body.to_vec(),
            header("content-type"),
            header("content-length"),
        ));
        log.content_md5.lock().unwrap().push(header("content-md5"));
        // Like S3 and Azure: a body that does not match its declared
        // digest is refused.
        if let Some(declared) = header("content-md5") {
            use base64::Engine;
            use md5::Digest;
            let actual = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(&body));
            if actual != declared {
                return axum::http::StatusCode::BAD_REQUEST;
            }
        }
        if log.refuse_upload {
            axum::http::StatusCode::FORBIDDEN
        } else {
            axum::http::StatusCode::OK
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    *log.storage.lock().unwrap() = base.clone();
    let router = axum::Router::new()
        .route("/providers/call", post(call))
        .route("/put", put(upload))
        .with_state(log);
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (base, task)
}

fn composio_via(base: &str) -> crate::providers::ComposioProvider {
    crate::providers::ComposioProvider::managed(ryu_app_events::ProviderRouter::for_test(
        crate::state::PLUGIN_ID,
        reqwest::Client::new(),
        format!("{base}/providers/call"),
        format!("{base}/providers/status"),
    ))
    .with_loopback_uploads()
}

fn render_request(platform: Platform, path: &str) -> crate::providers::PublishRequest {
    crate::providers::PublishRequest {
        account: crate::providers::ProviderAccount {
            id: "acc_1".into(),
            platform,
            label: None,
            external_id: Some("ca_1".into()),
        },
        text: "Launch day\n\nThe whole story.\n\n0:00 Intro".into(),
        media: vec![crate::publish::to_publish_media(&MediaRef {
            path: path.to_owned(),
            mime_type: "video/mp4".into(),
            name: "Launch.mp4".into(),
        })],
        segments: None,
        idempotency_key: Some("sp_1:acc_1".into()),
        agent_grant: None,
    }
}

/// A Send to Outpost render published through the real Composio provider:
/// the file is staged through the Gateway router's `file_upload` operation,
/// streamed byte for byte to the presigned URL, and handed to the platform's
/// upload tool as `{ name, mimetype, s3key }`. The router and storage are
/// fakes; no live YouTube or TikTok publish is exercised here.
#[tokio::test]
async fn an_imported_render_is_staged_and_published_through_composio() {
    use crate::providers::PlatformProvider;
    let root = temp_media_root("composio-handoff");
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let state = state().with_media_root(root.clone());
    let render: Vec<u8> = (0..(crate::media_import::IMPORT_CHUNK_BYTES + 77))
        .map(|i| (i % 241) as u8)
        .collect();
    let path = import_render(&state, &render).await;
    let log = BrokerLog::default();
    let (base, server) = fake_broker(log.clone()).await;
    let composio = composio_via(&base);

    let youtube = composio
        .publish(&render_request(Platform::Youtube, &path))
        .await;
    assert_eq!(
        youtube,
        crate::providers::PublishResult::Ok {
            remote_id: "yt_video_1".into(),
            remote_url: Some("https://www.youtube.com/watch?v=yt_video_1".into()),
        }
    );
    let tiktok = composio
        .publish(&render_request(Platform::Tiktok, &path))
        .await;
    assert_eq!(
        tiktok,
        crate::providers::PublishResult::Ok {
            remote_id: "v_pub_1".into(),
            remote_url: None,
        }
    );

    let uploads = log.uploads.lock().unwrap().clone();
    assert_eq!(uploads.len(), 2);
    for (bytes, content_type, content_length) in &uploads {
        assert_eq!(bytes, &render);
        assert_eq!(content_type.as_deref(), Some("video/mp4"));
        assert_eq!(
            content_length.as_deref(),
            Some(render.len().to_string().as_str())
        );
    }
    let calls = log.calls.lock().unwrap().clone();
    let operations: Vec<(&str, &str)> = calls
        .iter()
        .map(|call| {
            (
                call["operation"].as_str().unwrap(),
                call["toolId"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        operations,
        [
            ("file_upload", "YOUTUBE_UPLOAD_VIDEO"),
            ("execute", "YOUTUBE_UPLOAD_VIDEO"),
            ("file_upload", "TIKTOK_UPLOAD_VIDEO"),
            ("execute", "TIKTOK_UPLOAD_VIDEO"),
        ]
    );
    let md5 = calls[0]["body"]["md5"].as_str().unwrap();
    assert_eq!(md5.len(), 32);
    assert_eq!(calls[0]["body"]["size"], json!(render.len()));
    // Every PUT declared the hashed digest, so storage verifies the bytes.
    let digests = log.content_md5.lock().unwrap().clone();
    assert_eq!(digests.len(), 2);
    assert!(digests.iter().all(Option::is_some));
    // The upload tool is given a long upstream timeout; staging is not.
    assert!(calls[0].get("timeoutMs").is_none());
    assert!(calls[1]["timeoutMs"].as_u64().unwrap() >= 600_000);
    assert_eq!(calls[0]["body"]["toolkitSlug"], json!("youtube"));
    assert!(calls[0]["body"]["filename"]
        .as_str()
        .unwrap()
        .ends_with(".mp4"));
    let staged = json!({
        "name": calls[0]["body"]["filename"],
        "mimetype": "video/mp4",
        "s3key": "uploads/fixture/render.mp4",
    });
    let youtube_args = &calls[1]["body"];
    assert_eq!(calls[1]["accountId"], json!("ca_1"));
    assert_eq!(youtube_args["videoFilePath"], staged);
    assert_eq!(youtube_args["title"], json!("Launch day"));
    assert_eq!(
        youtube_args["description"],
        json!("The whole story.\n\n0:00 Intro")
    );
    assert_eq!(youtube_args["privacyStatus"], json!("public"));
    let tiktok_args = &calls[3]["body"];
    assert_eq!(tiktok_args["file_to_upload"], staged);
    assert_eq!(tiktok_args["publish"], json!(true));
    assert_eq!(tiktok_args["privacy_level"], json!("PUBLIC_TO_EVERYONE"));
    // The broker credential never appears in anything this process sent.
    assert!(calls.iter().all(|call| call.get("apiKey").is_none()));
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}

/// A scheduling agent's grant covers the whole video publish: the staging
/// step names the connected account and carries the grant, like the upload
/// tool call that follows, so Core checks the agent's account access for
/// both and never sees a grant-less call for the agent's own post.
#[tokio::test]
async fn video_staging_names_the_account_and_carries_the_agents_grant() {
    use crate::providers::PlatformProvider;
    let root = temp_media_root("composio-grant");
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let state = state().with_media_root(root.clone());
    let path = import_render(&state, b"render bytes").await;
    let log = BrokerLog::default();
    let (base, server) = fake_broker(log.clone()).await;
    let composio = composio_via(&base);

    let mut request = render_request(Platform::Youtube, &path);
    request.agent_grant = Some("grant.sig".into());
    let outcome = composio.publish(&request).await;
    assert!(
        matches!(outcome, crate::providers::PublishResult::Ok { .. }),
        "{outcome:?}"
    );

    let calls = log.calls.lock().unwrap().clone();
    let grants = log.grants.lock().unwrap().clone();
    let steps: Vec<(&str, Option<&str>, Option<&str>)> = calls
        .iter()
        .zip(&grants)
        .filter(|(call, _)| {
            matches!(
                call["operation"].as_str(),
                Some("file_upload") | Some("execute")
            )
        })
        .map(|(call, grant)| {
            (
                call["operation"].as_str().unwrap(),
                call["accountId"].as_str(),
                grant.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        steps,
        [
            ("file_upload", Some("ca_1"), Some("grant.sig")),
            ("execute", Some("ca_1"), Some("grant.sig")),
        ]
    );
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}

/// A storage refusal fails the publish before the upload tool runs, so a
/// half-staged file is never published; platforms without an upload tool
/// still refuse local media before any network call.
#[tokio::test]
async fn a_refused_upload_or_an_unstageable_platform_fails_visibly() {
    use crate::providers::PlatformProvider;
    let root = temp_media_root("composio-refused");
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let state = state().with_media_root(root.clone());
    let path = import_render(&state, b"render bytes").await;
    let log = BrokerLog {
        refuse_upload: true,
        ..BrokerLog::default()
    };
    let (base, server) = fake_broker(log.clone()).await;
    let composio = composio_via(&base);

    let error = composio
        .publish(&render_request(Platform::Youtube, &path))
        .await
        .error()
        .unwrap_or_default()
        .to_owned();
    assert!(error.contains("HTTP 403"), "{error}");
    assert!(log
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|call| call["operation"] != json!("execute")));

    log.calls.lock().unwrap().clear();
    for platform in [Platform::Instagram, Platform::Linkedin, Platform::X] {
        let error = composio
            .publish(&render_request(platform, &path))
            .await
            .error()
            .unwrap_or_default()
            .to_owned();
        assert!(
            error.contains("Composio cannot publish the local file") && error.contains(&path),
            "{platform:?}: {error}"
        );
    }
    assert!(log.calls.lock().unwrap().is_empty());

    // A file outside the media root is never staged.
    let outside =
        std::env::temp_dir().join(format!("ryu-social-outside-{}.mp4", uuid::Uuid::new_v4()));
    std::fs::write(&outside, b"x").unwrap();
    let error = composio
        .publish(&render_request(Platform::Tiktok, outside.to_str().unwrap()))
        .await
        .error()
        .unwrap_or_default()
        .to_owned();
    assert!(error.contains("media root"), "{error}");
    assert!(log.calls.lock().unwrap().is_empty());
    std::fs::remove_file(outside).unwrap();
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}

/// The duplicate-post hazard: Composio runs the upload tool upstream even
/// when Gateway's answer is a timeout. A scheduled publish whose upload call
/// fails after dispatch is settled as failed after ONE call, with a reason
/// that says to check the platform, instead of being retried into a second
/// public video.
#[tokio::test]
async fn a_failed_upload_call_is_never_retried_into_a_duplicate() {
    use std::sync::Arc;
    let root = temp_media_root("composio-no-dup");
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let mut state = state().with_media_root(root.clone());
    let path = import_render(&state, b"render bytes for a large file").await;

    for (execute_status, string_data) in [(Some(504), false), (Some(502), false), (None, true)] {
        let log = BrokerLog {
            execute_status,
            string_data,
            ..BrokerLog::default()
        };
        let (base, server) = fake_broker(log.clone()).await;
        state.providers =
            crate::providers::ProviderRegistry::with_provider(Arc::new(composio_via(&base)));
        let post = due_post_of(&state, Platform::Youtube, &path).await;
        let outcome = crate::publish::run_post(&state, &post).await.unwrap();
        let executes = log
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call["operation"] == json!("execute"))
            .count();
        assert_eq!(
            executes, 1,
            "{execute_status:?}: the upload tool ran once; {outcome:?}"
        );
        if string_data {
            // `data` as a JSON string still yields the video id.
            assert_eq!(outcome.status, PostStatus::Published, "{outcome:?}");
            assert_eq!(outcome.targets[0].remote_id.as_deref(), Some("yt_string_1"));
        } else {
            assert_eq!(outcome.status, PostStatus::Failed, "{outcome:?}");
            let target = &outcome.targets[0];
            assert_eq!(target.attempts, 1);
            let error = target.error.as_deref().unwrap_or_default();
            assert!(
                error.contains("outcome is unknown") && error.contains("Check YouTube"),
                "{error}"
            );
        }
        server.abort();
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// A Gateway refusal before dispatch (here a 429 rate limit) published
/// nothing, so it stays an ordinary, retryable failure.
#[tokio::test]
async fn a_refusal_before_dispatch_stays_retryable() {
    use crate::providers::PlatformProvider;
    let root = temp_media_root("composio-refused-early");
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let state = state().with_media_root(root.clone());
    let path = import_render(&state, b"render").await;
    let log = BrokerLog {
        execute_status: Some(429),
        ..BrokerLog::default()
    };
    let (base, server) = fake_broker(log.clone()).await;
    let result = composio_via(&base)
        .publish(&render_request(Platform::Tiktok, &path))
        .await;
    assert!(result.is_retryable(), "{result:?}");
    assert!(result.error().unwrap().contains("refused"));
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}
