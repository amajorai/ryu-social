//! Starter templates: the seven shapes a creator reaches for often enough that an
//! empty template list is a worse first run than an opinionated one.
//!
//! CRUD lives in [`crate::store`] and is wired straight into the routes — a template
//! is inert content, so there is nothing for this module to do on read or write. What
//! IS here is the seed, and the seed has two requirements that make it more than a
//! loop of inserts.
//!
//! ## Requirement 1: seeding twice must not duplicate
//!
//! Every built-in gets a DETERMINISTIC id — `tpl_builtin_<slug>__<workspace_id>` —
//! instead of the random `tpl_<uuid>` a user-created template gets. The id is the
//! dedupe key, so the insert is `INSERT OR IGNORE` and a restart, a second tab, and
//! two concurrent `GET /templates` handlers all converge on exactly one row per
//! built-in. A name-based check would not: names are user-editable, and renaming
//! "Launch" would resurrect it on the next call.
//!
//! ## Requirement 2: deleting a built-in must be permanent
//!
//! `INSERT OR IGNORE` alone does not give this. If the seed runs on every
//! `GET /templates` — which it does, because that is the only way a workspace created
//! after boot ever gets seeded — then deleting "Quote" would bring it back on the
//! next page load, and the delete button would look broken.
//!
//! So the seed is guarded by a per-workspace MARKER, stored in the `settings` table
//! under the reserved key `__seed__:<workspace_id>`. The marker is written once, and
//! its presence is what makes the seed a genuine no-op afterwards. One indexed
//! primary-key lookup per `/templates` call is the whole cost.
//!
//! (The marker row is keyed by `__seed__:<id>`, so `delete_workspace`'s
//! `DELETE FROM settings WHERE workspace_id = ?1` cascade does not match it. That
//! leaves one small orphan row per deleted workspace — recorded here rather than
//! papered over. Re-creating a workspace mints a fresh id, so an orphan can never be
//! mistaken for the marker of a live workspace.)

use std::collections::BTreeMap;

use crate::models::{Platform, TemplateBody};
use crate::store::SocialStore;

/// Prefix on every built-in template's id. Deterministic, so the seed is idempotent
/// and so a client can tell a starter from something the user wrote.
pub const BUILTIN_ID_PREFIX: &str = "tpl_builtin_";

/// The `settings` key holding "this workspace has been seeded".
fn seed_marker_key(workspace_id: &str) -> String {
    format!("__seed__:{workspace_id}")
}

/// The stable id of one built-in inside one workspace.
pub fn builtin_id(slug: &str, workspace_id: &str) -> String {
    format!("{BUILTIN_ID_PREFIX}{slug}__{workspace_id}")
}

/// One starter template, as declared. `platform_defaults` entries are per-platform
/// starting text — the same body shape a user-authored template has, so a built-in is
/// editable and deletable exactly like anything else. There is no "protected" flag.
struct Builtin {
    slug: &'static str,
    name: &'static str,
    text: &'static str,
    /// `(platform, text)` pairs. Kept as a slice rather than a map so the table stays
    /// `const`-shaped and reviewable as one block.
    platform_defaults: &'static [(Platform, &'static str)],
}

/// The starter set.
///
/// Chosen as the seven recurring *jobs* rather than seven pretty examples: ship
/// something, explain something at length, report what changed, close the week, ask
/// the audience, quote a source, and walk through steps. Every one is a fill-in-the-
/// blanks skeleton with `[bracketed]` slots, because a template whose text is
/// finished prose gets read once and deleted.
///
/// The per-platform defaults are where the real value is: the X variant of a launch
/// is not the LinkedIn variant with fewer words, and having both pre-shaped is what
/// makes cross-posting take a minute instead of ten.
const BUILTINS: &[Builtin] = &[
    Builtin {
        slug: "launch",
        name: "Launch announcement",
        text: "\
[Product] is live.

[One sentence on the problem it removes.]

What's in it:
• [capability]
• [capability]
• [capability]

[link]",
        platform_defaults: &[
            (
                Platform::X,
                "[Product] is live.\n\n[The one-line pitch.]\n\n[link]",
            ),
            (
                Platform::Linkedin,
                "\
Today we're launching [Product].

[Two or three sentences on why this problem was worth solving, and who it was hurting.]

What it does:
• [capability]
• [capability]
• [capability]

[What we learned building it, in one honest sentence.]

Try it: [link]",
            ),
            (
                Platform::Bluesky,
                "[Product] is live — [the one-line pitch]. [link]",
            ),
        ],
    },
    Builtin {
        slug: "thread",
        name: "Thread / deep dive",
        text: "\
[The claim, stated flatly enough to be arguable.]

Here's what actually happened:

1. [beat]
2. [beat]
3. [beat]

[The takeaway someone could act on tomorrow.]",
        platform_defaults: &[
            (
                Platform::X,
                "\
[The claim, stated flatly enough to be arguable.]

A thread on what actually happened 🧵",
            ),
            (
                Platform::Linkedin,
                "\
[The claim, stated flatly enough to be arguable.]

[Beat one — the setup, and what it cost.]

[Beat two — the turn.]

[Beat three — what it looks like now.]

[The takeaway someone could act on tomorrow.]",
            ),
        ],
    },
    Builtin {
        slug: "changelog",
        name: "Changelog / what shipped",
        text: "\
Shipped in [version]:

✦ [change] — [why it matters in one clause]
✦ [change] — [why it matters in one clause]
✦ [fix]

Full notes: [link]",
        platform_defaults: &[
            (
                Platform::X,
                "[version] is out:\n\n✦ [change]\n✦ [change]\n✦ [fix]\n\n[link]",
            ),
            (
                Platform::Linkedin,
                "\
[Product] [version] is out.

The headline change: [change], because [the complaint it answers].

Also in this release:
• [change]
• [fix]

Full notes: [link]",
            ),
        ],
    },
    Builtin {
        slug: "weekly-recap",
        name: "Weekly recap",
        text: "\
Week of [date]:

→ Shipped: [thing]
→ Learned: [thing]
→ Struggling with: [thing]

Next week: [the one thing that matters].",
        platform_defaults: &[(
            Platform::Linkedin,
            "\
A week of [project], honestly.

Shipped: [thing].
Learned: [thing — including the part that was uncomfortable].
Still stuck on: [thing].

Next week the only thing that matters is [the one thing].

[Question back to the reader.]",
        )],
    },
    Builtin {
        slug: "question",
        name: "Audience question",
        text: "\
[The question, asked in one line and answerable in one line.]

Context: [why you're asking, in a sentence.]

[Your own answer, so it reads as a conversation and not a survey.]",
        platform_defaults: &[
            (
                Platform::X,
                "[The question, asked in one line.]\n\nMine: [your answer].",
            ),
            (
                Platform::Threads,
                "[The question, asked in one line.]\n\nMine: [your answer].",
            ),
        ],
    },
    Builtin {
        slug: "quote",
        name: "Quote / commentary",
        text: "\
\"[The quote.]\"
— [Who said it, and where]

[Why you disagree, or what it changed for you. One paragraph, not a summary of the quote.]

[link]",
        platform_defaults: &[(
            Platform::Linkedin,
            "\
\"[The quote.]\" — [Who said it]

[Why it landed, or why it's wrong. Say which.]

[The concrete thing you changed because of it.]

Source: [link]",
        )],
    },
    Builtin {
        slug: "carousel",
        name: "Carousel / step-by-step",
        text: "\
[The promise: what the reader can do by the last slide.]

Slide 1 — [the problem, in their words]
Slide 2 — [step]
Slide 3 — [step]
Slide 4 — [step]
Slide 5 — [the result, shown not claimed]

[Call to action.]",
        platform_defaults: &[
            (
                Platform::Instagram,
                "\
[The promise, in one line.]

Swipe for the [N] steps →

[3–5 hashtags that are actually about the topic.]",
            ),
            (
                Platform::Linkedin,
                "\
[The promise: what the reader can do by the last slide.]

[One sentence on who this is for and who it isn't.]

Steps in the carousel below ↓",
            ),
        ],
    },
];

/// Build the stored body for one built-in.
fn body_of(builtin: &Builtin) -> TemplateBody {
    let mut platform_defaults = BTreeMap::new();
    for (platform, text) in builtin.platform_defaults {
        platform_defaults.insert(platform.as_str().to_string(), (*text).to_string());
    }
    let mut body = TemplateBody {
        schema_version: TemplateBody::SCHEMA_VERSION,
        text: builtin.text.to_string(),
        platform_defaults,
    };
    // Drops any blank per-platform entry, so the table above cannot ship a default
    // that renders as an empty "has a LinkedIn variant" badge.
    body.normalize();
    body
}

/// Seed the built-ins into `workspace_id`, exactly once, ever.
///
/// Returns how many rows were newly inserted — `0` on every call after the first,
/// including the first call in a process that restarted. Never fails the caller's
/// request: a seed that could not run is a missing convenience, not a broken
/// template list, so errors are logged and swallowed by [`ensure_seeded`].
pub async fn seed_builtins(store: &SocialStore, workspace_id: &str) -> anyhow::Result<usize> {
    let marker = seed_marker_key(workspace_id);
    if store.get_settings_blob(&marker).await?.is_some() {
        return Ok(0);
    }

    let mut inserted = 0usize;
    for builtin in BUILTINS {
        let id = builtin_id(builtin.slug, workspace_id);
        if store
            .insert_seed_template(&id, workspace_id, builtin.name, &body_of(builtin))
            .await?
        {
            inserted += 1;
        }
    }

    // Written AFTER the inserts, so a crash mid-seed simply re-seeds — `INSERT OR
    // IGNORE` on a deterministic id makes the retry free. Writing the marker first
    // would leave a workspace permanently half-seeded.
    store
        .put_settings_blob(
            &marker,
            &serde_json::json!({ "seeded_at": crate::models::now_ms(), "count": BUILTINS.len() })
                .to_string(),
        )
        .await?;
    Ok(inserted)
}

/// The call site's wrapper: seed if needed, and never let a seeding failure turn a
/// successful `GET /templates` into a 500.
pub async fn ensure_seeded(store: &SocialStore, workspace_id: &str) {
    match seed_builtins(store, workspace_id).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            workspace = %workspace_id,
            count = n,
            "ryu-social: seeded starter templates"
        ),
        Err(e) => tracing::warn!(
            workspace = %workspace_id,
            error = %e,
            "ryu-social: could not seed starter templates"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_builtin_has_a_unique_slug_and_a_non_empty_body() {
        let mut slugs: Vec<&str> = BUILTINS.iter().map(|b| b.slug).collect();
        slugs.sort_unstable();
        let count = slugs.len();
        slugs.dedup();
        assert_eq!(slugs.len(), count, "duplicate built-in slug");
        assert_eq!(count, 7);

        for builtin in BUILTINS {
            assert!(!builtin.name.trim().is_empty(), "{}", builtin.slug);
            let body = body_of(builtin);
            assert!(!body.text.trim().is_empty(), "{}", builtin.slug);
            // A declared per-platform default must survive normalization — a blank
            // one would be silently dropped and the table would be lying.
            assert_eq!(
                body.platform_defaults.len(),
                builtin.platform_defaults.len(),
                "{} has a blank per-platform default",
                builtin.slug
            );
        }
    }

    #[tokio::test]
    async fn seeding_is_idempotent_across_restarts() {
        let store = SocialStore::open_in_memory().unwrap();

        let first = seed_builtins(&store, crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        assert_eq!(first, BUILTINS.len());

        // A second pass — the restart case — inserts nothing.
        let second = seed_builtins(&store, crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        assert_eq!(second, 0);

        let templates = store
            .list_templates(crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        assert_eq!(templates.len(), BUILTINS.len());
        assert!(templates
            .iter()
            .all(|t| t.id.starts_with(BUILTIN_ID_PREFIX)));
    }

    #[tokio::test]
    async fn a_deleted_builtin_stays_deleted() {
        let store = SocialStore::open_in_memory().unwrap();
        seed_builtins(&store, crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();

        let victim = builtin_id("quote", crate::models::DEFAULT_WORKSPACE_ID);
        assert!(store.delete_template(&victim).await.unwrap());

        // The marker, not the id, is what makes this hold: `INSERT OR IGNORE` alone
        // would happily re-create the row we just deleted.
        assert_eq!(
            seed_builtins(&store, crate::models::DEFAULT_WORKSPACE_ID)
                .await
                .unwrap(),
            0
        );
        assert!(store.get_template(&victim).await.unwrap().is_none());
        assert_eq!(
            store
                .list_templates(crate::models::DEFAULT_WORKSPACE_ID)
                .await
                .unwrap()
                .len(),
            BUILTINS.len() - 1
        );
    }

    #[tokio::test]
    async fn each_workspace_gets_its_own_copies() {
        let store = SocialStore::open_in_memory().unwrap();
        let other = store.create_workspace("Client").await.unwrap();

        seed_builtins(&store, crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        assert_eq!(
            seed_builtins(&store, &other.id).await.unwrap(),
            BUILTINS.len()
        );

        // Ids are namespaced per workspace, so the two sets are independent rows and
        // editing one workspace's "Launch" cannot change the other's.
        let mine = store
            .list_templates(crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        let theirs = store.list_templates(&other.id).await.unwrap();
        assert_eq!(mine.len(), BUILTINS.len());
        assert_eq!(theirs.len(), BUILTINS.len());
        assert!(mine.iter().all(|m| theirs.iter().all(|t| t.id != m.id)));
    }

    #[tokio::test]
    async fn a_seeded_template_carries_its_per_platform_defaults() {
        let store = SocialStore::open_in_memory().unwrap();
        seed_builtins(&store, crate::models::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap();
        let launch = store
            .get_template(&builtin_id("launch", crate::models::DEFAULT_WORKSPACE_ID))
            .await
            .unwrap()
            .expect("launch template");
        assert_eq!(launch.name, "Launch announcement");
        assert!(launch.body.platform_defaults.contains_key("x"));
        assert!(launch.body.platform_defaults.contains_key("linkedin"));
    }
}
