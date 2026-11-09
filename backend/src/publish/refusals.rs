//! Refusals for post transitions, one whole sentence per post status.
//!
//! The status is never spliced into a sentence as an English word: the
//! Companion translates each refusal by matching the complete sentence, and a
//! spliced status could not agree with the rest of it in another language.

use crate::error::ApiError;
use crate::models::PostStatus;

/// "Publish now" lost its guarded transition: the post is no longer `scheduled`.
pub(crate) fn cannot_publish_now(status: PostStatus) -> ApiError {
    match status {
        PostStatus::Scheduled => {
            ApiError::conflict("post is scheduled and can no longer be published now")
        }
        PostStatus::Due => ApiError::conflict("post is due and can no longer be published now"),
        PostStatus::Publishing => {
            ApiError::conflict("post is publishing and can no longer be published now")
        }
        PostStatus::Published => {
            ApiError::conflict("post is published and can no longer be published now")
        }
        PostStatus::Partial => {
            ApiError::conflict("post is partial and can no longer be published now")
        }
        PostStatus::Failed => {
            ApiError::conflict("post is failed and can no longer be published now")
        }
        PostStatus::Cancelled => {
            ApiError::conflict("post is cancelled and can no longer be published now")
        }
    }
}

/// A retry was asked of a post that is neither `partial` nor `failed`.
pub(crate) fn cannot_retry(status: PostStatus) -> ApiError {
    match status {
        PostStatus::Scheduled => ApiError::conflict(
            "only a partial or failed post can be retried; this one is scheduled",
        ),
        PostStatus::Due => {
            ApiError::conflict("only a partial or failed post can be retried; this one is due")
        }
        PostStatus::Publishing => ApiError::conflict(
            "only a partial or failed post can be retried; this one is publishing",
        ),
        PostStatus::Published => ApiError::conflict(
            "only a partial or failed post can be retried; this one is published",
        ),
        PostStatus::Partial => {
            ApiError::conflict("only a partial or failed post can be retried; this one is partial")
        }
        PostStatus::Failed => {
            ApiError::conflict("only a partial or failed post can be retried; this one is failed")
        }
        PostStatus::Cancelled => ApiError::conflict(
            "only a partial or failed post can be retried; this one is cancelled",
        ),
    }
}

/// A reschedule lost its guarded transition.
pub(crate) fn cannot_reschedule(status: PostStatus) -> ApiError {
    match status {
        PostStatus::Scheduled => {
            ApiError::conflict("a scheduled post can no longer be rescheduled")
        }
        PostStatus::Due => ApiError::conflict("a due post can no longer be rescheduled"),
        PostStatus::Publishing => {
            ApiError::conflict("a publishing post can no longer be rescheduled")
        }
        PostStatus::Published => {
            ApiError::conflict("a published post can no longer be rescheduled")
        }
        PostStatus::Partial => ApiError::conflict("a partial post can no longer be rescheduled"),
        PostStatus::Failed => ApiError::conflict("a failed post can no longer be rescheduled"),
        PostStatus::Cancelled => {
            ApiError::conflict("a cancelled post can no longer be rescheduled")
        }
    }
}

/// A cancel arrived after publishing began.
pub(crate) fn cannot_cancel(status: PostStatus) -> ApiError {
    match status {
        PostStatus::Scheduled => ApiError::conflict("a scheduled post can no longer be cancelled"),
        PostStatus::Due => ApiError::conflict("a due post can no longer be cancelled"),
        PostStatus::Publishing => {
            ApiError::conflict("a publishing post can no longer be cancelled")
        }
        PostStatus::Published => ApiError::conflict("a published post can no longer be cancelled"),
        PostStatus::Partial => ApiError::conflict("a partial post can no longer be cancelled"),
        PostStatus::Failed => ApiError::conflict("a failed post can no longer be cancelled"),
        PostStatus::Cancelled => ApiError::conflict("a cancelled post can no longer be cancelled"),
    }
}
