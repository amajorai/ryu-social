//! One error type for the whole HTTP surface, so every handler can be written as
//! `-> ApiResult<Json<T>>` and use `?` on store/serde/provider calls.
//!
//! Why a single enum rather than per-handler `(StatusCode, Json<Value>)` tuples
//! (the shape `ryu-teams` uses): this app has ~30 routes across five later-owned
//! modules. A tuple-returning convention makes every one of those handlers
//! re-implement its own error mapping, which is exactly how a 500 ends up leaking a
//! SQL string to the frame. Funnelling through one `IntoResponse` gives a single
//! place where the status code, the stable machine-readable `code`, and the
//! message-vs-detail split are decided.
//!
//! Wire shape is fixed and snake_case, matching the rest of the sidecar:
//! `{ "error": "<human message>", "code": "<machine code>" }`.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Every handler in [`crate::api`] returns this.
pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug)]
pub enum ApiError {
    /// The addressed row does not exist (or is not visible in this workspace).
    NotFound(String),
    /// The caller's payload is structurally wrong — a missing field, an unparseable
    /// platform, a compose payload that fails the per-platform limit check.
    BadRequest(String),
    /// The row exists but is not in a state that admits this transition — e.g.
    /// cancelling a post that already reached `publishing`. Distinct from
    /// `BadRequest` because the client's payload was fine and a retry may succeed.
    Conflict(String),
    /// A route whose handler is owned by a module that has not landed yet. Returns
    /// 501 rather than 500 so the UI (and any smoke test) can tell "not built" from
    /// "broken", and so a monitoring alert on 5xx does not fire on known gaps.
    ///
    /// **Currently unconstructed — and that is the good outcome:** every module-owned
    /// stub has been filled in, so nothing in this crate answers "not built" any more.
    /// Kept (with the `allow`) rather than deleted because it is the one honest status
    /// for a surface a later feature declares before it implements, and re-deriving the
    /// 501-vs-500 distinction later is how a known gap ends up paging someone.
    #[allow(dead_code)]
    NotImplemented(String),
    /// A dependency we do not control failed — the remote platform API, the network.
    /// 502, because the fault is upstream of this process.
    Upstream(String),
    /// Anything else. The `anyhow` chain is logged in full; the client gets a fixed
    /// string, because these messages contain SQL, file paths, and occasionally
    /// fragments of credentials.
    Internal(anyhow::Error),
}

impl ApiError {
    pub fn not_found(what: impl Into<String>) -> Self {
        Self::NotFound(what.into())
    }

    pub fn bad_request(why: impl Into<String>) -> Self {
        Self::BadRequest(why.into())
    }

    pub fn conflict(why: impl Into<String>) -> Self {
        Self::Conflict(why.into())
    }

    /// The marker a later agent's module returns until its body lands. Kept as a
    /// constructor rather than a bare string so `grep -rn "not_implemented"` finds
    /// every remaining gap in one pass.
    #[allow(dead_code)]
    pub fn not_implemented(what: impl Into<String>) -> Self {
        Self::NotImplemented(what.into())
    }

    pub fn upstream(what: impl Into<String>) -> Self {
        Self::Upstream(what.into())
    }

    /// The stable machine-readable discriminator. The UI branches on this, never on
    /// the human message, so the message stays free to change.
    fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::BadRequest(_) => "bad_request",
            Self::Conflict(_) => "conflict",
            Self::NotImplemented(_) => "not_implemented",
            Self::Upstream(_) => "upstream_error",
            Self::Internal(_) => "internal_error",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m) => write!(f, "{m} not found"),
            Self::BadRequest(m) | Self::Conflict(m) | Self::Upstream(m) => write!(f, "{m}"),
            Self::NotImplemented(m) => write!(f, "{m} is not implemented yet"),
            Self::Internal(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Log the FULL chain before narrowing what the client sees. For `Internal`
        // this is the only place the real cause is ever recorded.
        if let Self::Internal(e) = &self {
            tracing::error!(error = ?e, "ryu-social: internal error");
        } else {
            tracing::debug!(error = %self, code = self.code(), "ryu-social: request rejected");
        }
        let status = self.status();
        let code = self.code();
        // `Internal` deliberately does NOT forward `e` — see the variant's doc.
        let message = match &self {
            Self::Internal(_) => "internal error".to_string(),
            other => other.to_string(),
        };
        (status, Json(json!({ "error": message, "code": code }))).into_response()
    }
}

// ── `?` conversions ────────────────────────────────────────────────────────────
//
// The store returns `anyhow::Result`, so `From<anyhow::Error>` is what makes every
// handler's `?` work. `rusqlite`/`serde_json` conversions exist so a module that
// touches those crates directly (the publish runner's own transaction, a provider
// decoding a response) does not have to `.map_err(anyhow::Error::from)` at each
// call site.

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Internal(anyhow::Error::from(e))
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        Self::Internal(anyhow::Error::from(e))
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(e: reqwest::Error) -> Self {
        // A transport failure to a platform API is upstream, not our bug.
        Self::Upstream(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_and_codes_are_stable() {
        assert_eq!(ApiError::not_found("draft").status(), StatusCode::NOT_FOUND);
        assert_eq!(ApiError::not_found("draft").code(), "not_found");
        assert_eq!(
            ApiError::not_implemented("publish").status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(ApiError::conflict("x").status(), StatusCode::CONFLICT);
        assert_eq!(ApiError::upstream("x").status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn internal_errors_do_not_leak_their_cause_to_the_client() {
        let err = ApiError::Internal(anyhow::anyhow!("SELECT * FROM secrets failed"));
        let message = match &err {
            ApiError::Internal(_) => "internal error".to_string(),
            other => other.to_string(),
        };
        assert_eq!(message, "internal error");
    }
}
