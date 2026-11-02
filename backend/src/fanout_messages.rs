//! Inbox, toast and push copy as localizable messages.
//!
//! Core renders a notify hint's `i18n` text per recipient (the inbox row and
//! toast in the node's `ui-language`, each push in the locale its device
//! registered with); the English rendering still goes out as the plain
//! `title`/`body`, which an older Core delivers as is.
//!
//! Each `*_MESSAGE` is `format!` with one named argument per placeholder
//! (`name = arg("name")`) so the i18n extractor catalogs the ICU source;
//! `tools/generate-core-i18n.mjs` copies the translations into Core's catalog.
//! Every message is a whole sentence. Core's renderer has no plural selector,
//! so counts are written as `label: {count}`.
#![allow(clippy::useless_format)]

use ryu_app_events::{NotifyHint, NotifyMessage, NotifyTemplate as Template};

use crate::models::InboxKind;

pub(crate) const POST_PUBLISHED_MESSAGE: Template = |_| format!("Post published");
pub(crate) const POST_FAILED_MESSAGE: Template = |_| format!("Post failed to publish");
pub(crate) const NEW_COMMENT_MESSAGE: Template =
    |arg| format!("New comment from {author}", author = arg("author"));
pub(crate) const NEW_REPLY_MESSAGE: Template =
    |arg| format!("New reply from {author}", author = arg("author"));
pub(crate) const NEW_MENTION_MESSAGE: Template =
    |arg| format!("New mention from {author}", author = arg("author"));
pub(crate) const NEW_DIRECT_MESSAGE_MESSAGE: Template =
    |arg| format!("New direct message from {author}", author = arg("author"));

/// A scheduled post went out.
pub(crate) fn post_published() -> NotifyHint {
    NotifyHint::localized(NotifyMessage::new(POST_PUBLISHED_MESSAGE), Vec::new())
        .with_level("success")
}

/// A post failed (or only partly) published.
pub(crate) fn post_failed() -> NotifyHint {
    NotifyHint::localized(NotifyMessage::new(POST_FAILED_MESSAGE), Vec::new()).with_level("error")
}

/// A new inbound item; the body is the item's own text, shown as written.
pub(crate) fn inbox_received(kind: InboxKind, author: &str, text: &str) -> NotifyHint {
    let title = match kind {
        InboxKind::Comment => NEW_COMMENT_MESSAGE,
        InboxKind::Reply => NEW_REPLY_MESSAGE,
        InboxKind::Mention => NEW_MENTION_MESSAGE,
        InboxKind::Dm => NEW_DIRECT_MESSAGE_MESSAGE,
    };
    let body = (!text.is_empty()).then(|| NotifyMessage::verbatim(text));
    NotifyHint::localized(
        NotifyMessage::new(title).text("author", author),
        body.into_iter().collect(),
    )
}
