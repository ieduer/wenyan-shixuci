# 文言 · 诗学辞 operations

Last normalized: 2026-08-14 PDT
Owner: suen
Lifecycle: active
Data class: student_owned
Documentation status: generated from local source, Git/GitHub audit, project catalog, and live Cloudflare inventory; unresolved facts remain fail-closed.

## Quick start

- Canonical local path: `/Users/ylsuen/CF/wenyan-shixuci`
- Git authority: `ieduer/wenyan-shixuci`
- Canonical Git baseline read back for this candidate: `origin/main` / `d094f50879fa8a85bc68b745f641ae4e3ca1fab8` (`e2c9682` remains an ancestor and the accepted v3 script revision)
- Source-only candidate branch: `codex/wy-server-graded-practice-v1-20260814`; it is not deployed and is not connected to `src/index.ts`
- Runtime config: `wenyan-shixuci/wrangler.toml` (name `wenyan-shixuci`)
- Current state: [PROJECT_STATE.md](../PROJECT_STATE.md)
- Workspace resource routing: [project resource index](../../reports/operations/project_resource_index.md)
- Documentation standard: [project operations standard](../../runbooks/project_operations_documentation_standard.md)
- Production mutation is forbidden until exact owner, target, bindings, backup, verification, and rollback have fresh readback.

## Existing project documentation relationship

This `docs/OPERATIONS.md` is the single project-local operations entrypoint.
Existing detailed manuals remain authoritative annexes for their exact scope;
historical handovers and ledgers are evidence, not current state.

- No earlier operational handbook was detected.

## Project and runtime inventory

| Project ID | Runtime type | Resource | Domains |
| --- | --- | --- | --- |
| `wy-bdfz-net` | `worker` | `wenyan-shixuci` | wy.bdfz.net |

Live Cloudflare matching is metadata-only and does not prove application health:

| Resource | Live type | Readback | Detail |
| --- | --- | --- | --- |
| `wenyan-shixuci` | Worker | verified 2026-08-10 | compatibility `2026-04-21`; modified `2026-07-07T02:40:07.952145Z` |

## Authority and dependencies

- Project names: 文言 · 诗学辞
- Catalog owner: suen
- Data classes: student_owned
- Identity modes: central
- User Center required: true
- Pulse measurement: worker_analytics
- Runtime bindings: 17 names cataloged; names are intentionally omitted from this general handbook. Inspect the exact project config and live binding types under task-scoped authority.
- Shared User Center, APIS, nav, image, Pulse, App, clone-family, and VPS effects must be checked through workspace topic runbooks; this file does not weaken those gates.

## Resource location and restore

- Source authority: `/Users/ylsuen/CF/wenyan-shixuci`; Git/GitHub authority above.
- External/local build inputs, archived paths, receipts, retention, and hydrate commands not stated below are `review_required` and block deletion.

Catalog backup evidence:
- Cloudflare immutable Worker versions: current=7447c0b9-7cb0-4595-9e02-dbe7891cf89d, previous=76dc587d-beec-4d0d-8693-4359c9eaf0f9; REVIEW_REQUIRED: export stateful bindings before destructive changes

Catalog restore evidence:
- REVIEW_REQUIRED: define and test data restore for the listed stateful bindings

Before deleting any local resource, satisfy the workspace path-preserving archive, remote readback, isolated restore, receipt, handbook, and project-state gates.

## Preflight and AI ownership

1. Read `/Users/ylsuen/CF/AGENTS.md`, this file, `PROJECT_STATE.md`, and linked annexes.
2. Inspect `git -C "/Users/ylsuen/CF/wenyan-shixuci" status --short` when Git-backed.
3. Inspect recent `reports/agent_action_log.jsonl` ownership.
4. Resolve the exact source, Worker/Pages/VPS/App target, domains, bindings, data, and rollback live.
5. Append a scoped `start` row before the first mutation.
6. Preserve unrelated dirty work; never reset, clean, broad-checkout, or stash another task's changes.

## Build, test, and local verification entrypoints

Detected package entrypoints (presence is not proof they currently pass):

- `npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run build:data`
- `npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run build:v3`
- `npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run check`
- `npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run check:sources`
- `npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run deploy`
- `npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run dev`

Run only commands supported by the current project toolchain and verify expected outputs in the project before using them as release evidence.

## Source-only My scoring candidate (inactive)

The `wy-server-graded-practice-v1` candidate is an additive foundation for the
`server_graded_practice_v1` adapter class. It does not change the current
`POST /api/challenge/answer` path, install a public route, apply a migration,
add a binding, send an event, or activate My scoring.

Audited source facts at Git `d094f50879fa8a85bc68b745f641ae4e3ca1fab8`:

- the Worker validates a signed HMAC answer token bound to owner/run/item, loads
  the persisted challenge item and tracked answer key, and computes correctness
  server-side;
- authenticated ownership currently resolves to a mutable User Center slug,
  not an immutable positive User Center `userId`;
- the existing `sync_outbox` is durable for legacy progress synchronization but
  has neither the fixed evidence delivery identity nor payload-conflict
  isolation required for A–F evidence;
- `src/generated/answer_keys.json` contains 1,244/1,244 entries with a
  `correct_label`; its raw file digest is
  `sha256:a87511f0fbb008843bbdda583805d682b3d00203c9643f6b7de1c130b5fe6b19`,
  and its canonical grading-identity digest is
  `sha256:0542ad12cb1babb6d72e975f1167e71ff82cce187a532d7cf96b84884af515bc`.

Candidate authorities:

- machine-readable contract: `contracts/wy-server-graded-practice-v1.json`;
- source envelope, catalog verification, D1 outbox/idempotency and aggregate
  health foundation: `src/wy-server-graded-practice-v1.ts`;
- unapplied additive schema: `migrations/0100_wy_server_graded_practice_candidate_v1.sql`;
- hostile verification: `tests/wy-server-graded-practice-v1.test.mjs`;
- exact CI authorities: Node `22.21.1` and `24.18.0`, with `.nvmrc` fixed to
  `24.18.0`.

The delivery boundary accepts only `sourceAttemptId`. Identity and grade must be
loaded from an append-only source-owned attempt created after signed-token
binding, server answer-key grading, and immutable positive User Center user-id
resolution. The envelope excludes submitted/correct answers, token, cookie,
session, slug, names, class, grade, and weight. Same attempt plus same bytes is
idempotent; changed bytes are quarantined without overwriting the first record.

All candidate records remain `pending_mapping`, and candidate health always
reports `blocked`. Activation remains forbidden until all of the following have
current evidence: a matching User Center registry contract, immutable live
identity resolution, independently reviewed migration and deployment, and a
real-account WY → My A–F acceptance/readback/rollback loop.

Local verification from a clean checkout:

```bash
npm ci --ignore-scripts
./node_modules/.bin/tsc --noEmit
node --experimental-strip-types --test tests/wy-server-graded-practice-v1.test.mjs
```

Capability-fit receipt: `no-new-capability`. The candidate uses only the
project's existing Cloudflare Worker, Web Crypto, and D1 programming model. It
adds no binding, changes no compatibility date, and evaluates no preview or
beta capability. This is source and test evidence only, not Cloudflare adoption
or production authority.

## Health and business-path verification

Catalog health probes:
- curl -sS -o /dev/null -w '%{http_code}\n' https://wy.bdfz.net/ # expected 2xx/3xx

Catalog contract checks:
- jq '.projects[] | select(.project_id=="wy-bdfz-net")' platform/project_verification_evidence.json

Also verify authentication boundaries, data read/write behavior, browser/device path, monitoring, clone-family and shared-hub regressions as applicable. HTTP 200 or a build alone is insufficient.

## Preview, deployment, and rollback

Catalog deploy commands (not authorization; fresh preflight remains mandatory):
- npm --prefix "/Users/ylsuen/CF/wenyan-shixuci" run deploy

Rollback/failback authorities:
- npx wrangler versions deploy 7447c0b9-7cb0-4595-9e02-dbe7891cf89d@100 --name wenyan-shixuci --yes

For data-backed projects, immutable code rollback does not restore D1/KV/R2/DO/Queue state. Use backup/restore or backward-compatible forward-fix procedures verified for the exact resource.

Candidate rollback is source-only: close the draft PR or revert its eventual
merge commit. Because the candidate is not imported, deployed, or migrated,
there is no candidate D1/Queue/student-data rollback action and the production
Worker rollback anchor above is unchanged.

## Monitoring, privacy, cost, and incidents

- Monitoring coverage: required
- Measurement: worker_analytics
- Never record secret values, cookies, sessions, private keys, raw student content, or sensitive payloads.
- Verify current logs, errors, cost/usage, limits, owner, stop condition, and incident runbook before representing runtime health.

## Verification standard

1. Source of truth: local/Git/GitHub authority above, refreshed before mutation.
2. Health probe: catalog probes above plus expected response semantics.
3. Contract/business path: catalog checks plus auth/data/UI/device behavior.
4. Deploy and forbidden actions: catalog command above; no deploy from dirty, duplicate, reconstruction, archive, or unverified source.
5. Dependency regression: matrix fan-out, shared hubs, clone family, App/VPS as applicable.
6. Backup/restore: catalog evidence above; missing exact evidence is blocking for writes/deletion.
7. Rollback/failback: catalog authority above, refreshed live before release.
8. Last verified: live authority 2026-07-15T10:45:14.366Z; source-only candidate tests 2026-08-14 PDT. Candidate tests are not live verification.

## Synchronized documentation and handoff

Any change to source authority, architecture, dependencies, runtime resources,
deployment, data, backup/restore, verification, monitoring, incidents, rollback,
or ownership must update this manual in the same task. Accepted version,
objective, blockers, deployment state, rollback anchor, and next action must
update `PROJECT_STATE.md` in the same task.

Every AI closeout must record changed files, generated artifacts, tests, live
version/deployment, rollback, dirty-tree state, unresolved follow-ups, and the
manual/state updates in `reports/agent_action_log.jsonl`. Chat is not a durable handoff.
