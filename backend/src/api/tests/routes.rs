use super::*;

#[test]
fn openapi_doc_covers_the_served_routes() {
    let doc = openapi();
    assert!(!doc.paths.paths.is_empty());
}

/// Every documented path must be ABSOLUTE, under this app's mount.
///
/// The failure this catches is silent and total: Core matches a derived operation
/// by stripping the mount off the spec's own path, so a relative `/posts` slipping
/// into an annotation makes that operation match nothing and vanish. Nothing logs
/// above debug, the app still serves the route, and the tool simply never exists —
/// which is why the emptiness check above is not enough on its own.
#[test]
fn every_documented_path_is_absolute_under_the_mount() {
    for path in openapi().paths.paths.keys() {
        // The mount root itself is allowed even though nothing serves it today,
        // so this asserts "under the mount" and not the narrower "has a subpath".
        assert!(
            path == "/api/social" || path.starts_with("/api/social/"),
            "`{path}` is not under the /api/social mount"
        );
    }
}

/// The doc must not describe a route the manifest does not declare, and must not
/// use axum's `:id` form.
///
/// Core intersects derived operations against `sidecars[].http.routes[]` and DROPS
/// the rest, so an operation outside that set is wasted work at best. The
/// placeholder half is the sharper edge: the matcher writes `{id}`, the router
/// writes `:id`, and mixing them up silently drops the operation.
#[test]
fn documented_paths_use_brace_placeholders_and_match_the_router() {
    for path in openapi().paths.paths.keys() {
        assert!(
            !path.contains(':'),
            "`{path}` uses axum's `:param` form; the spec must use `{{param}}`"
        );
    }
}

/// Agents draft, schedule and publish through the `ryu_ext` tools Core lowers
/// from this document. Core keeps an operation only when a manifest route
/// declares its path (`:param` matches one segment, a trailing `*rest` the
/// rest) for its method (a route with no method takes any), and drops the rest
/// with a debug line. A dropped compose operation would leave agents unable to
/// post at all, and nothing else would fail, so this pins both halves: every
/// documented operation is declared, and the operations agents publish with
/// are documented. Those calls reach the app through Core's ext-proxy, which
/// stamps the calling agent's grant, so Outpost's provider calls carry it.
#[test]
fn agent_publishing_operations_survive_the_declared_route_intersection() {
    let manifest: Value =
        serde_json::from_str(include_str!("../../../../manifest.json")).expect("manifest parses");
    let http = &manifest["sidecars"][0]["http"];
    let mount = http["mount"]
        .as_str()
        .expect("the sidecar declares a mount");
    let routes = http["routes"]
        .as_array()
        .expect("the sidecar declares routes");
    let matches = |pattern: &str, actual: &str| {
        let pattern: Vec<&str> = pattern.trim_start_matches('/').split('/').collect();
        let actual: Vec<&str> = actual.trim_start_matches('/').split('/').collect();
        for (index, segment) in pattern.iter().enumerate() {
            if segment.starts_with('*') {
                return true;
            }
            let Some(value) = actual.get(index) else {
                return false;
            };
            if segment.starts_with(':') {
                if value.is_empty() {
                    return false;
                }
            } else if segment != value {
                return false;
            }
        }
        pattern.len() == actual.len()
    };

    let doc = serde_json::to_value(openapi()).expect("the document serializes");
    let paths = doc["paths"].as_object().expect("the document has paths");
    let mut operations = Vec::new();
    for (path, item) in paths {
        for method in ["get", "post", "put", "patch", "delete"] {
            if let Some(operation) = item.get(method) {
                let id = operation["operationId"].as_str().unwrap_or_default();
                operations.push((id.to_owned(), method, path.clone()));
            }
        }
    }

    for (id, method, path) in &operations {
        let sub_path = path
            .strip_prefix(mount)
            .unwrap_or_else(|| panic!("`{path}` is not under `{mount}`"));
        let declared = routes.iter().any(|route| {
            matches(route["path"].as_str().unwrap_or_default(), sub_path)
                && route["method"]
                    .as_str()
                    .is_none_or(|declared| declared.eq_ignore_ascii_case(method))
        });
        assert!(
            declared,
            "`{id}` ({method} {path}) is documented but the manifest declares no route \
             for it, so Core drops it and agents never get the tool"
        );
    }

    for (id, method, path) in [
        ("create_draft", "post", "/api/social/drafts"),
        ("patch_draft", "patch", "/api/social/drafts/{id}"),
        ("validate_post", "post", "/api/social/posts/validate"),
        ("create_post", "post", "/api/social/posts"),
        ("schedule_post", "post", "/api/social/posts/{id}/schedule"),
        ("publish_now", "post", "/api/social/posts/{id}/publish-now"),
        ("retry_post", "post", "/api/social/posts/{id}/retry"),
        ("cancel_post", "post", "/api/social/posts/{id}/cancel"),
    ] {
        assert!(
            operations
                .iter()
                .any(|(seen, seen_method, seen_path)| seen == id
                    && *seen_method == method
                    && seen_path == path),
            "agents need `{id}` ({method} {path}) as a derived tool"
        );
    }
}

/// The audit exploit: a caller holding only `social.publish` plus access to one
/// workspace sent a nested `node` object to the workspace route and rewrote the
/// install-wide inbox policy and stored account credentials. The workspace route
/// must now refuse the node half and leave the node blob untouched.
#[tokio::test]
async fn workspace_settings_route_refuses_node_wide_changes() {
    let state = state();
    let before = crate::settings::load(&state.store).await.unwrap();
    assert!(before.inbox_polling_enabled);
    assert!(before.bluesky_app_password.is_none());

    let body: PatchSettingsBody = serde_json::from_value(json!({
        "workspace_id": DEFAULT_WORKSPACE_ID,
        "scheduler_enabled": false,
        "node": {
            "inbox_polling_enabled": false,
            "bluesky_handle": "@attacker",
            "bluesky_app_password": "attacker-app-password"
        }
    }))
    .unwrap();
    let refused = patch_settings(State(state.clone()), Json(body)).await;
    assert!(matches!(refused, Err(ApiError::BadRequest(_))));

    let after = crate::settings::load(&state.store).await.unwrap();
    assert!(after.inbox_polling_enabled);
    assert!(after.bluesky_handle.is_none());
    assert!(after.bluesky_app_password.is_none());
    // Refused as a whole: the workspace half of the same body is not applied.
    let workspace = state
        .store
        .get_settings(DEFAULT_WORKSPACE_ID)
        .await
        .unwrap();
    assert!(workspace.scheduler_enabled);
}

#[tokio::test]
async fn workspace_settings_route_still_patches_workspace_fields() {
    let state = state();
    let body: PatchSettingsBody = serde_json::from_value(json!({
        "scheduler_enabled": false,
        "node": null
    }))
    .unwrap();
    let saved = patch_settings(State(state.clone()), Json(body))
        .await
        .unwrap()
        .0;
    assert_eq!(saved["settings"]["scheduler_enabled"], json!(false));
    assert_eq!(saved["node"]["inbox_polling_enabled"], json!(true));
}

#[tokio::test]
async fn node_settings_route_applies_node_changes_and_redacts_credentials() {
    let state = state();
    let patch: crate::settings::NodeSettingsPatch = serde_json::from_value(json!({
        "inbox_polling_enabled": false,
        "bluesky_app_password": "owner-app-password"
    }))
    .unwrap();
    let response = patch_node_settings(State(state.clone()), Json(patch))
        .await
        .unwrap()
        .0;
    assert_eq!(response["node"]["inbox_polling_enabled"], json!(false));
    assert_eq!(response["node"]["bluesky_app_password_set"], json!(true));
    assert!(!response.to_string().contains("owner-app-password"));
    let stored = crate::settings::load(&state.store).await.unwrap();
    assert!(!stored.inbox_polling_enabled);
}

/// Core enforces the manifest's per-route permission before the sidecar sees a
/// request, so the route split only holds if the manifest keeps the node route on
/// its own level and `social.publish` never implies it.
#[test]
fn manifest_gates_node_settings_on_a_separate_permission() {
    let manifest: Value =
        serde_json::from_str(include_str!("../../../../manifest.json")).expect("manifest JSON");
    let routes = manifest["sidecars"][0]["http"]["routes"]
        .as_array()
        .expect("routes");
    let permission_for = |path: &str, method: &str| {
        routes
            .iter()
            .find(|route| route["path"] == path && route["method"] == method)
            .and_then(|route| route["permission"].as_str())
            .map(str::to_owned)
    };
    assert_eq!(
        permission_for("/settings", "PATCH").as_deref(),
        Some("social.publish")
    );
    assert_eq!(
        permission_for("/node-settings", "PATCH").as_deref(),
        Some("social.manage-node")
    );

    let levels = manifest["permission_levels"].as_array().expect("levels");
    let implies = |id: &str| -> Vec<String> {
        levels
            .iter()
            .find(|level| level["id"] == id)
            .and_then(|level| level["implies"].as_array())
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    };
    let mut reachable = vec!["social.publish".to_owned()];
    let mut index = 0;
    while index < reachable.len() {
        for next in implies(&reachable[index]) {
            if !reachable.contains(&next) {
                reachable.push(next);
            }
        }
        index += 1;
    }
    assert!(!reachable.iter().any(|level| level == "social.manage-node"));
    assert!(levels
        .iter()
        .any(|level| level["id"] == "social.manage-node"));
}

#[test]
fn preview_truncates_on_characters_not_bytes() {
    assert_eq!(preview("  hi  "), "hi");
    let long = "🎉".repeat(200);
    let out = preview(&long);
    assert_eq!(out.chars().count(), CALENDAR_TITLE_CHARS + 1);
}
