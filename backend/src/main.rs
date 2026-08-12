//! `ryu-social` — the standalone, out-of-process Outpost sidecar.
//!
//! A social-media scheduling and publishing command center that runs as a SEPARATE
//! PROCESS Core spawns, health-checks, and proxies to on loopback — the same shape
//! as `ryu-mail` / `ryu-teams`. Core does NOT contain this code and does not link
//! it: there is no `lib.rs`, every module below is bin-private, and the only route
//! into this process is the generic ext-proxy. So Outpost scales, fails, and ships
//! independently of the rest of the node.
//!
//! Contract surface — the paths Core forwards to, byte-identical whether they
//! arrive via the `public_mount` (`/api/social/*`) or the plugin proxy
//! (`/api/ext/@ryu/social/*`, rewritten onto the same prefix):
//!
//! ```text
//!   /health                       — un-gated loopback probe
//!   /api/social/*                 — the whole app surface (see `api::routes`)
//! ```
//!
//! SECURITY: this binary binds LOOPBACK ONLY (127.0.0.1) **and** guards every
//! `/api/social/*` route with a shared-secret bearer (`RYU_EXT_TOKEN`, injected by
//! Core into this child's spawn env). Core stays the auth front — it runs its own
//! `require_auth`, then re-stamps `Authorization: Bearer <RYU_EXT_TOKEN>` on the
//! loopback hop — so a request that did NOT come through Core (any other local
//! process on a shared host) is rejected with 401. The gate is FAIL-CLOSED: with no
//! token configured, every protected route rejects rather than falling open.
//!
//! `/health` is the ONE un-gated route. It has to be: Core probes it BEFORE it has
//! any reason to trust this process, and it returns no user data.
//!
//! Port: `RYU_SOCIAL_PORT` env, default `8005`. Data dir: resolved via the inlined
//! `paths::ryu_dir` (`RYU_DIR`-env-first, injected by Core at spawn), so it opens
//! the SAME `social.db` the node uses. This sidecar OWNS that database; nothing
//! else opens it.

// The domain contract and the persistence layer are written to be complete for the
// modules that land beside them, so plenty of their surface has no caller YET.
// Scoped to those two modules rather than a crate-wide blanket, so real dead code
// in the handlers still warns.
#[allow(dead_code)]
mod models;
#[allow(dead_code)]
mod store;

// Same reasoning for the three module-owned seams: the provider trait, the publish
// primitives and the analytics scalar are the contract the agents filling those
// modules build against, so their surface is intentionally wider than today's
// callers. `state` carries the declared hook-event ids and the shared HTTP client,
// which those same modules consume.
#[allow(dead_code)]
mod analytics;
#[allow(dead_code)]
mod providers;
#[allow(dead_code)]
mod publish;
#[allow(dead_code)]
mod state;

// `settings` exposes the whole node-settings surface — the credential accessors the
// provider layer consumes and the quiet-hours helpers the background loops read —
// ahead of every consumer landing; `templates` exposes the built-in table and its
// deterministic ids for the same reason. Same `allow` reasoning as the modules above.
#[allow(dead_code)]
mod settings;
#[allow(dead_code)]
mod templates;

mod api;
mod error;
mod inbox;
mod paths;
// The scheduler owns two seams that have no HTTP caller inside this crate: the
// timing recommender (a pure projection the companion UI reads through the queue
// view) and `queue`, the read model behind `GET /queue`. Both are deliberately
// wider than today's single call site, same reasoning as the modules above.
#[allow(dead_code)]
mod queue;
#[allow(dead_code)]
mod scheduler;

use std::net::{Ipv4Addr, SocketAddr};

use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;

use crate::state::{AppState, Config};
use crate::store::SocialStore;

/// Default loopback port for the social sidecar (overridable via `RYU_SOCIAL_PORT`,
/// which Core injects profile-shifted so concurrent dev/release nodes do not
/// collide on it).
///
/// Must stay equal to `sidecars[0].port` in `apps-store/social/manifest.json`: the
/// manifest value is what Core injects and what its health probe polls, and this
/// constant is only the standalone-run fallback — a drift between the two is a
/// sidecar that Core reports unhealthy while it happily serves on another port.
/// There is no port registry (see `SidecarSpec::port`), so avoiding a collision is
/// this file's job: `8004` was taken by `@ryu/ugc` while this app was being built.
const DEFAULT_PORT: u16 = 8005;

/// The external prefix. Must match the manifest's `sidecars[0].http.mount`, or Core
/// will forward `/api/social/posts` to a router that only knows `/posts`.
const MOUNT: &str = "/api/social";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("RYU_SOCIAL_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    // The shared secret Core injects via the generic ext-proxy loader: a per-plugin
    // minted token it stamps on every proxied hop and on the health probe.
    let token = std::env::var("RYU_EXT_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    if token.is_some() {
        tracing::info!("ryu-social: protected {MOUNT}/* routes require the injected shared-secret bearer");
    } else {
        tracing::warn!(
            "ryu-social: no RYU_EXT_TOKEN set; protected {MOUNT}/* routes are FAIL-CLOSED (reject all). Core injects this token when it spawns the sidecar."
        );
    }

    let store = SocialStore::open(paths::ryu_dir().join("social.db"))?;
    let mut state = AppState::new(store.clone(), Config::from_env(port));

    // Re-resolve the provider registry against the PERSISTED node settings before
    // anything can serve a request.
    //
    // `AppState::new` stays synchronous (every test builds a state through it), so
    // the registry it installs can only read the environment. That is the whole
    // credential story for an operator-configured deployment, but it is not the
    // normal one: a user who pastes a Composio key or a Bluesky app password into the
    // settings tab writes it to the node settings blob, which an env-only registry
    // never looks at. Without this pass those fields are inert — stored, echoed back
    // by `GET /settings` as `<set>`, and connected to nothing.
    //
    // Reading the blob needs the store and is async, hence here rather than in
    // `AppState::new`. Env still wins over the blob (see `from_node_settings`), and
    // the shared client is threaded through so the whole process keeps ONE
    // connection pool instead of the second one `ProviderRegistry::new()` allocates.
    //
    // Best-effort: an unreadable/corrupt settings blob leaves the env-resolved
    // registry in place rather than refusing to boot. A node whose settings row is
    // damaged should still publish whatever the environment configured, and `/health`
    // is what surfaces a database that is genuinely unreadable.
    match settings::load(&store).await {
        Ok(node) => {
            state.providers = providers::ProviderRegistry::from_credentials(
                state.http.clone(),
                providers::ProviderCredentials::from_node_settings(&node),
            );
        }
        Err(e) => tracing::warn!(
            error = %e,
            "ryu-social: could not read node settings; provider credentials fall back to the environment alone"
        ),
    }
    let state = state;

    // The scheduler tick owns its own clone of the state and runs for the process
    // lifetime. Spawned, not awaited: `main` must keep serving HTTP.
    let scheduler = scheduler::spawn(state.clone());

    // Two more background loops, both on their own much slower cadence and both
    // separate from the publish tick on purpose: each makes a burst of third-party
    // calls when it wakes, and folding either into the 30-second sweep would put every
    // scheduled post behind an inbox poll or an analytics refresh. Both re-read their
    // interval from the node settings every cycle, so a change in the settings tab
    // takes effect without a restart, and both no-op entirely when nothing is
    // configured to poll.
    let inbox_poller = inbox::spawn_poller(state.clone());
    let engagement_refresher = analytics::spawn_refresher(state.clone());

    // The app router, with the shared-secret gate layered over the WHOLE nest.
    // Outpost has no public route: there is no inbound webhook here, so every path
    // under the mount is protected without exception.
    let gated_token = token.clone();
    let app_routes = Router::new()
        .nest(MOUNT, api::routes(state))
        .layer(from_fn(move |req: Request, next: Next| {
            let expected = gated_token.clone();
            async move { require_social_token(req, next, expected.as_deref()).await }
        }));

    // `/health` sits OUTSIDE the gated nest so Core's loopback probe succeeds before
    // auth. It asserts the DB is READABLE (not merely that the process is alive) and
    // returns no user data — a health check that only proved liveness would report
    // green on a node whose database is missing.
    let health_store = store;
    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let store = health_store.clone();
                async move { health(store).await }
            }),
        )
        .merge(app_routes);

    // LOOPBACK ONLY (belt) + shared-secret bearer (suspenders).
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ryu-social sidecar listening on http://{addr}{MOUNT}");

    let result = axum::serve(listener, app).await;
    // Stop the background loops on shutdown so a supervised restart does not briefly
    // run two sweeps — or two inbox polls, which would double-emit `inbox.received`
    // for the same comment — against one database.
    scheduler.abort();
    inbox_poller.abort();
    engagement_refresher.abort();
    result?;
    Ok(())
}

/// Un-gated loopback health probe. Confirms DB readiness with a cheap read and
/// returns counts only — never content.
async fn health(store: SocialStore) -> Response {
    match store.list_workspaces().await {
        Ok(workspaces) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "workspaceCount": workspaces.len() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Shared-secret bearer gate for the proxied surface.
///
/// **Fail-closed:** `expected == None`/empty (no token configured) rejects every
/// request rather than falling open, so a bare-run or misconfigured sidecar never
/// serves a user's connected accounts unauthenticated.
async fn require_social_token(req: Request, next: Next, expected: Option<&str>) -> Response {
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if bearer_ok(provided, expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

/// Pure bearer check, factored out so the auth decision is unit-testable without an
/// axum `Request`/`Next`. Returns `true` only when `expected` is a non-empty token
/// AND `provided` equals it (constant-time compared).
fn bearer_ok(provided: Option<&str>, expected: Option<&str>) -> bool {
    let Some(expected) = expected.filter(|t| !t.is_empty()) else {
        return false;
    };
    ct_eq(provided.unwrap_or("").as_bytes(), expected.as_bytes())
}

/// Constant-time byte comparison — no early return on the first mismatched byte, so
/// the token check does not leak length/prefix via timing.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::bearer_ok;

    #[test]
    fn bearer_ok_matches_only_exact_nonempty_token() {
        assert!(bearer_ok(Some("secret"), Some("secret")));
        assert!(!bearer_ok(Some("secret"), Some("other")));
        assert!(!bearer_ok(Some("secre"), Some("secret")));
        assert!(!bearer_ok(None, Some("secret")));
    }

    #[test]
    fn bearer_ok_is_fail_closed_without_expected() {
        // No/empty configured token → reject everything, even a matching-looking hdr.
        assert!(!bearer_ok(Some("secret"), None));
        assert!(!bearer_ok(Some(""), Some("")));
        assert!(!bearer_ok(None, None));
    }
}
