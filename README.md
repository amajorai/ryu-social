# ryu-social

Outpost for Ryu — a social-media command center: compose multi-segment posts against per-platform limits, schedule them onto a month/week/day calendar, and let a durable retrying publish queue push them to X, Bluesky, LinkedIn, Instagram, TikTok, YouTube, Reddit, Facebook and Threads — plus a reply inbox and engagement history.

> **The public home of `ryu-social`.** Source, builds, and releases live here —
> binaries for every platform are attached to each release.
>
> This tree is generated from the Ryu monorepo, so commits pushed here
> directly are replaced on the next sync. **Pull requests are welcome** —
> open them here and they are ported into the monorepo, then flow back out.
> Ryu as a whole: https://github.com/amajorai/ryu

## Install

- Binary: `ryu-social` from the [Ryu releases](https://github.com/amajorai/ryu/releases).
- Crate: `cargo install ryu-social`.

## License

Apache-2.0 — see [LICENSE](./LICENSE).

---

# Outpost (`@ryu/social`)

A publishing command center for every social account you run: compose once, tailor per
platform, queue it on a calendar, and let the node publish while you are away. Modeled on
[amajorai/outpost](https://github.com/amajorai/outpost), rebuilt as a Ryu app — the
scheduling brain is a Rust sidecar the node owns, so a post goes out on time whether or not
the desktop app is running.

What it does, concretely: connect accounts, write a post (with per-account variants and
threads), validate it against each platform's real character/media limits, put it on a
calendar, and let a durable queue publish it with retries. Afterwards it keeps the publish
record, refreshes engagement numbers, and collects replies and mentions into one inbox you
can answer from.

## Parts

- **`backend/` (`ryu-social`)** — the whole brain, and a **sidecar-only crate**: a single
  `[[bin]]`, no `lib.rs`, and **zero dependency on `apps/core`** (it links only
  `ryu-app-events`, whose entire job is an outbound HTTP POST to Core's `events.emit`
  capability). Owns `~/.ryu/social.db` (rusqlite, `user_version` migration ladder) and
  serves 33 JSON routes at `/api/social`. It also runs the **scheduler tick loop** —
  a catch-up sweep, a crash-lease reaper, and a CAS-guarded claim — which is why the app
  works with the UI closed.
- **`ui/` (`@ryu/social-app`)** — the companion surface: a React app built to one
  self-contained HTML file via `vite-plugin-singlefile`, consuming `@ryu/ui`. Full-page
  Companion (Path B, `ui_format: "html"`), eight panels behind a rail: Overview, Compose,
  Calendar, Queue, Inbox, Library, Activity, Settings.
- **`manifest.json`** — the registration. One `local` sidecar, one companion runnable, and
  the `contributes` surfaces (hook events, a settings tab, a sidebar section, a list-detail
  view, a Store tab).

## Architecture: three hops, and why

```
companion frame  ──window.ryu.social.request──▶  desktop host  ──Bearer <node token>──▶  Core
   (null-origin,                                (PluginHostPanel                        /api/social/*
    connect-src 'none')                          + lib/api/social.ts)                        │
                                                                                  ext-proxy route
                                                                                    allowlist
                                                                                             ▼
                                                                        ryu-social on 127.0.0.1:8005
                                                                            (Bearer RYU_EXT_TOKEN)
```

**The frame cannot reach the sidecar directly, by design.** A companion's CSP is
`connect-src 'none'`, and the manifest `csp` allowlist that can widen it accepts only
`https://` origins with a dotted host — so `http://127.0.0.1:8005` is rejected four
different ways. Outpost therefore declares **no** `csp` block at all and drives everything
over the capability-gated `window.ryu` bridge.

**The bridge is three verbs, not thirty-five.** `social.request({ method, path, body })` is
one generic forwarder the host re-issues against `/api/social<path>`; `social.open` and
`social.openList` are the two shell-navigation verbs a sandboxed frame genuinely cannot
perform itself. The forwarder grants no authority that did not already exist — `/api/social`
is the sidecar's `public_mount`, which Core already serves to any client holding the node
token, which is exactly what the host holds. Real enforcement stays where it belongs: the
`social:crud` grant gates the verb, and Core's ext-proxy route allowlist gates the paths (a
sub-path matching no declared `http.routes[]` entry is a hard 404, and a declared prefix
does **not** admit its subpaths). See `ui/src/ryu.d.ts` for the long form.

**The sidecar never calls back into the host**, so the manifest declares no
`sidecars[].host_api.grants`. Emitting app events is not an exception: `ryu-app-events`
authenticates with the injected `RYU_EXT_TOKEN` and Core authorizes the emit against the
manifest's own declared `hook_events`, exactly as `@ryu/mail` and `@ryu/teams` do with no
`host_api` block either.

## Building

Two halves, built independently.

```bash
# The sidecar
cargo check -p ryu-social
cargo test  -p ryu-social      # 135 tests, all crate-scoped (no live Core needed)
cargo build -p ryu-social --release

# The companion UI
bun install --cwd apps-store/social/ui
bun run --cwd apps-store/social/ui build     # → ui/dist/index.html (one file, no external refs)
```

### Refreshing the Core UI fixture

Core ships the built companion **compiled in**, because a built-in's package directory is
not on the user's machine. After any UI change:

```bash
bun run --cwd apps-store/social/ui build
cp apps-store/social/ui/dist/index.html \
   apps/core/src/plugin_manifest/fixtures/social.ui.html
# or, equivalently:
scripts/sync-app-fixtures.sh social
```

`SOCIAL_UI_HTML` in `apps/core/src/plugin_manifest/mod.rs` `include_str!`s that fixture, and
`apps/core/src/plugins/seed.rs` seeds it as the plugin's `ui_code`.

**Never create `apps/core/src/plugin_manifest/fixtures/social.manifest.json`.** The manifest
has exactly one home — this directory — and Core `include_str!`s it from here. A fixture copy
would win over the real file and silently swallow every edit;
`packaged_manifests_are_compiled_in_from_their_package_home` fails if one appears.

### Running it standalone

```bash
RYU_SOCIAL_PORT=8005 RYU_EXT_TOKEN=dev-secret cargo run -p ryu-social
curl localhost:8005/health
curl -H 'Authorization: Bearer dev-secret' localhost:8005/api/social/platforms
```

`/health` is the one un-gated route — Core probes it before it has any reason to trust the
process. Everything under `/api/social/*` is **fail-closed**: with no `RYU_EXT_TOKEN` set,
every protected route rejects rather than falling open.

## Routes

All 33 are relative to the `/api/social` mount and appear verbatim in the manifest's
`sidecars[0].http.routes[]`. Lists return `{"<plural>": [...]}`, single reads return the
entity at the top level, deletes return `{"ok": true}`; `workspace_id` is always an optional
query parameter, never a path segment.

| Path | Methods | What it is |
| --- | --- | --- |
| `/workspaces` | GET POST | Posting workspaces (one seeded `default`). |
| `/workspaces/:id` | GET PATCH DELETE | One workspace; the default refuses deletion. |
| `/accounts` | GET POST | Connected social accounts. |
| `/accounts/:id` | GET DELETE | One account. |
| `/accounts/:id/connect` | POST | Hand credentials to the account's provider. |
| `/accounts/:id/capabilities` | GET | What this account can actually post. |
| `/drafts` | GET POST | Saved drafts (text + media + threads as segments). |
| `/drafts/:id` | GET PATCH DELETE | One draft. |
| `/posts` | GET POST | Scheduled posts; POST schedules and fans out to targets. |
| `/posts/validate` | POST | Per-platform limit check without saving anything. |
| `/posts/:id` | GET PATCH DELETE | One scheduled post plus its fan-out legs. |
| `/posts/:id/schedule` | POST | Move a post to a new time. |
| `/posts/:id/cancel` | POST | Take it out of the queue. |
| `/posts/:id/publish-now` | POST | Skip the queue and run it immediately. |
| `/posts/:id/retry` | POST | Requeue the failed legs only — never a published one. |
| `/calendar` | GET | Month/week projection of what goes out when. |
| `/queue` | GET | The live queue: next attempt, attempts left, in-flight flag. |
| `/best-times` | GET | Ranked `(weekday, hour)` slots from this workspace's history. |
| `/history` | GET | Publish records. |
| `/history/:id` | GET | One publish record. |
| `/history/:id/refresh-engagement` | POST | Re-pull likes/reposts/replies. |
| `/inbox` | GET | Replies, mentions and comments across accounts. |
| `/inbox/refresh` | POST | Poll every account for new items. |
| `/inbox/:id/reply` | POST | Answer an item in place. |
| `/inbox/:id/read` | POST | Mark it read. |
| `/templates` | GET POST | Reusable post shapes (seven seeded per workspace). |
| `/templates/:id` | GET PATCH DELETE | One template. |
| `/templates/:id/use` | POST | Start a fresh draft from it — the Store tab's "Use". |
| `/media` | GET POST | The media library; rows are references, never copies. |
| `/media/:id` | DELETE | Drop a reference; the user's file is untouched. |
| `/activity` | GET | The per-account activity roll-up. |
| `/settings` | GET PATCH | Per-workspace settings (timezone, retries, backoff…). |
| `/platforms` | GET | The authoritative per-platform limits table. |

`/health` is deliberately **not** in this list: it lives outside the bearer gate in
`main.rs`, so Core's pre-auth probe succeeds. Do not move it into the nest.

## Events

Four, all emitted best-effort and after the fact — a hook subscriber can never prevent a
post from being scheduled or published:

- `@ryu/social#post.scheduled` — a post was queued, before any provider was contacted.
- `@ryu/social#post.published` — every leg settled and all of them succeeded (once per
  post, not once per account).
- `@ryu/social#post.failed` — settled with at least one leg unpublished; `status`
  separates `partial` from `failed`.
- `@ryu/social#inbox.received` — one new reply/mention/comment, per item, after it is
  durable.

Each is documented in the manifest with the cases where it does **not** fire, which is the
half a consumer actually needs.

## v1 scope

**In:** workspaces, accounts, drafts with per-account variants and threads, scheduling with
a durable retrying queue, publish-now, cancel, retry, the calendar and queue projections,
best-time recommendations, publish history with engagement refresh, a unified reply inbox,
templates (seven seeded), a media library, per-workspace settings, and the four hook events.

**Explicitly deferred:**

- **Danger-Zone deletion is declared but inert.** The manifest declares its `social` data
  category, but Core's `DataCategory` enum is a closed set that does not yet resolve it, so
  the Settings row is skipped. Wiring it would need a `social_client.rs` in Core, which
  `AGENTS.md` bans — the right fix is a generic "ask the owning sidecar to clear itself"
  seam, not another per-app Core module.
- **No `contributes.quotas`.** Nothing in `SocialSettings` is a plan-tier cap — they are
  poll intervals, retry budgets, a timezone and an enforcement toggle. A quota key also has
  to exist in `APP_QUOTAS` (`packages/auth/src/lib/plans.ts`) to mean anything, so declaring
  one here alone would be half a feature.
- **`/social/:id` deep links carry only the post id**, baked into the frame as
  `window.ryu.context.postId`. There is no per-post shell route beyond that.
- **Provider coverage** is whatever `backend/src/providers/` implements; an account whose
  provider is unconfigured reports itself so rather than failing at publish time.
- **No agent tools.** Outpost contributes no `runnables` of kind `tool`, so an agent cannot
  publish on the user's behalf yet. Posting publicly under someone's name is the wrong first
  thing to hand an agent unattended; the hook events are the supported automation seam.
