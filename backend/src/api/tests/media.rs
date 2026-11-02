use super::*;

/// Tests that point the process-wide `RYU_SOCIAL_MEDIA_ROOT` at their own
/// root run one at a time.
pub(super) static MEDIA_ROOT_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Import `render` through the chunked session handlers, as Video Studio's
/// Send to Outpost does, and return the library row's `path`.
pub(super) async fn import_render(state: &AppState, render: &[u8]) -> String {
    use base64::Engine;
    let started = start_media_import(
        State(state.clone()),
        Json(crate::media_import::StartImportBody {
            workspace_id: None,
            name: "Launch.mp4".into(),
            mime_type: "video/mp4".into(),
            size: render.len() as u64,
        }),
    )
    .await
    .unwrap()
    .0;
    let id = started["id"].as_str().unwrap().to_owned();
    let chunk_size = started["chunk_size"].as_u64().unwrap() as usize;
    for (index, chunk) in render.chunks(chunk_size).enumerate() {
        let _received = append_media_import(
            State(state.clone()),
            Path(id.clone()),
            Json(crate::media_import::ImportChunkBody {
                workspace_id: None,
                offset: (index * chunk_size) as u64,
                data: base64::engine::general_purpose::STANDARD.encode(chunk),
            }),
        )
        .await
        .unwrap();
    }
    let finished = finish_media_import(
        State(state.clone()),
        Path(id),
        Json(crate::media_import::FinishImportBody {
            workspace_id: None,
            sha256: None,
            metadata_text: Some("Title: Launch\n".into()),
        }),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(finished["media"]["kind"], json!("video"));
    assert!(finished["metadata_path"]
        .as_str()
        .unwrap()
        .ends_with("Launch-metadata.txt"));
    finished["media"]["path"].as_str().unwrap().to_owned()
}

pub(super) fn temp_media_root(label: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("ryu-social-{label}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    root
}

/// One scheduled post of `path` to a new `platform` account, made due now.
pub(super) async fn due_post_of(
    state: &AppState,
    platform: Platform,
    path: &str,
) -> crate::models::ScheduledPost {
    let account = state
        .store
        .create_account(DEFAULT_WORKSPACE_ID, platform, "@studio", Some("ext_1"))
        .await
        .unwrap();
    let mut body = DraftBody::empty();
    body.segments[0].text = "Launch day".into();
    body.segments[0].media = vec![MediaRef {
        path: path.to_owned(),
        mime_type: "video/mp4".into(),
        name: "Launch.mp4".into(),
    }];
    body.account_ids = vec![account.id.clone()];
    body.normalize();
    let draft = create_draft(
        State(state.clone()),
        Json(DraftBodyPayload {
            workspace_id: None,
            body,
        }),
    )
    .await
    .unwrap()
    .0;
    let post = create_post(
        State(state.clone()),
        HeaderMap::new(),
        Json(CreatePostBody {
            workspace_id: None,
            draft_id: Some(draft.id),
            body: None,
            scheduled_for: Some(1),
            account_ids: vec![account.id],
            variants: BTreeMap::new(),
        }),
    )
    .await
    .unwrap()
    .0;
    let now = chrono::Utc::now().timestamp_millis();
    assert!(state
        .store
        .mark_post_due_now(&post.id, now, None)
        .await
        .unwrap());
    state
        .store
        .get_scheduled_post(&post.id)
        .await
        .unwrap()
        .unwrap()
}

/// The Video Studio handoff end to end against the deterministic fake
/// provider: a render is imported in chunks into the media root, attached to
/// one draft per target platform, and published. The fake provider accepts
/// local files; the real Composio provider stages them first (see
/// `an_imported_render_is_staged_and_published_through_composio`).
#[tokio::test]
async fn an_imported_render_publishes_through_the_fake_provider() {
    use std::sync::Arc;
    let root = temp_media_root("handoff");
    // Publish re-checks local media against the process-wide root.
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let fake = Arc::new(crate::providers::FakeProvider::new());
    let mut state = state().with_media_root(root.clone());
    state.providers = crate::providers::ProviderRegistry::with_provider(fake.clone());

    let render: Vec<u8> = (0..(crate::media_import::IMPORT_CHUNK_BYTES + 10))
        .map(|i| (i % 251) as u8)
        .collect();
    let path = import_render(&state, &render).await;
    assert_eq!(std::fs::read(&path).unwrap(), render);
    assert_eq!(
        list_media(
            State(state.clone()),
            Query(ScopeQuery {
                workspace_id: None,
                limit: None
            })
        )
        .await
        .unwrap()
        .0["media"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    for platform in [Platform::Youtube, Platform::Tiktok] {
        let post = due_post_of(&state, platform, &path).await;
        let outcome = crate::publish::run_post(&state, &post).await.unwrap();
        assert_eq!(
            outcome.status,
            PostStatus::Published,
            "{platform:?}: {outcome:?}"
        );
    }
    let calls = fake.calls();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().all(|call| call.media_count == 1));
    std::fs::remove_dir_all(root).unwrap();
}

/// Deleting an imported file's library row deletes the file once nothing
/// refers to it; a draft that still attaches it keeps it on disk, and a
/// hand-picked file is never deleted.
#[tokio::test]
async fn deleting_imported_media_frees_its_file_once_unreferenced() {
    let root = temp_media_root("import-delete");
    let _env = MEDIA_ROOT_ENV.lock().await;
    std::env::set_var("RYU_SOCIAL_MEDIA_ROOT", &root);
    let state = state().with_media_root(root.clone());
    let media_id = |path: &str| {
        let state = state.clone();
        let path = path.to_owned();
        async move {
            state
                .store
                .list_media(DEFAULT_WORKSPACE_ID)
                .await
                .unwrap()
                .into_iter()
                .find(|media| media.path == path)
                .unwrap()
                .id
        }
    };

    // Attached to a draft: the row goes, the file stays.
    let kept = import_render(&state, b"kept").await;
    let _post = due_post_of(&state, Platform::Youtube, &kept).await;
    let removed = delete_media(State(state.clone()), Path(media_id(&kept).await))
        .await
        .unwrap()
        .0;
    assert_eq!(removed["file_removed"], json!(false));
    assert!(std::path::Path::new(&kept).exists());

    // Unreferenced: the file and its metadata text are deleted.
    let freed = import_render(&state, b"freed").await;
    let removed = delete_media(State(state.clone()), Path(media_id(&freed).await))
        .await
        .unwrap()
        .0;
    assert_eq!(removed["file_removed"], json!(true));
    assert!(!std::path::Path::new(&freed).parent().unwrap().exists());

    // A file the user picked is only ever a reference.
    let picked = root.join("picked.mp4");
    std::fs::write(&picked, b"mine").unwrap();
    let picked_path = picked.to_string_lossy().into_owned();
    state
        .store
        .upsert_media(
            DEFAULT_WORKSPACE_ID,
            &picked_path,
            "picked.mp4",
            Some("video/mp4"),
        )
        .await
        .unwrap();
    let removed = delete_media(State(state.clone()), Path(media_id(&picked_path).await))
        .await
        .unwrap()
        .0;
    assert_eq!(removed["file_removed"], json!(false));
    assert!(picked.exists());
    std::fs::remove_dir_all(root).unwrap();
}
