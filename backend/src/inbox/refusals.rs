//! Refusals for inbox operations, one whole sentence per inbox item kind.
//!
//! The item kind is never spliced into a sentence as an English word: the
//! Companion translates each refusal by matching the complete sentence, and a
//! spliced noun could not agree with the rest of it in another language.

use crate::error::ApiError;
use crate::models::InboxKind;

/// The configured provider cannot send a reply to an item of `kind` on `platform`.
pub fn reply_unsupported(kind: InboxKind, platform: &str) -> ApiError {
    match kind {
        InboxKind::Comment => ApiError::conflict(format!(
            "replying to a comment on {platform} is not supported by the configured provider"
        )),
        InboxKind::Reply => ApiError::conflict(format!(
            "replying to a reply on {platform} is not supported by the configured provider"
        )),
        InboxKind::Mention => ApiError::conflict(format!(
            "replying to a mention on {platform} is not supported by the configured provider"
        )),
        InboxKind::Dm => ApiError::conflict(format!(
            "replying to a direct message on {platform} is not supported by the configured provider"
        )),
    }
}
