# Project State

Last updated: 2026-08-14 PDT
Current version: production and accepted source remain unchanged; source-only scoring candidate is based on d094f50879fa8a85bc68b745f641ae4e3ca1fab8
Current objective: independently review the inactive wy-server-graded-practice-v1 candidate without changing the current answer path, production runtime, or student data
Completed work: the candidate contract, exact answer-key catalog digest, immutable server-attempt boundary, privacy-bounded v2 envelope, additive unapplied D1 schema, replacement-safe append-only guards, race-safe idempotent outbox/conflict isolation, aggregate blocked health, hostile tests, and exact Node 22.21.1/24.18.0 CI were prepared in an isolated checkout
Pending work: keep pending_mapping until User Center registers the exact contract, WY resolves an immutable positive live User Center userId at the server grading boundary, the migration/deployment receives independent review, and a real-account WY to My A–F acceptance/readback/rollback loop passes
Known problems: the current authenticated answer path still owns data by mutable User Center slug; the legacy sync_outbox is not an A–F evidence ledger; the canonical local worktree has unrelated intentional data/frontend/runtime/migration changes that this candidate did not touch
Next recommended task: review the draft source candidate; do not import it into src/index.ts, apply migration 0100, add a sink/binding, deploy, or mutate D1/Queue/student data until every listed blocker is closed with current evidence
Deployment status: candidate only, blocked, pending_mapping, runtimeScoringActive=false; no route/config/binding change, migration execution, D1/Queue/student-data mutation, Cloudflare deployment, or My score write performed
Rollback anchor: npx wrangler versions deploy 7447c0b9-7cb0-4595-9e02-dbe7891cf89d@100 --name wenyan-shixuci --yes
Operations authority: /Users/ylsuen/CF/wenyan-shixuci/docs/OPERATIONS.md
Ownership status: candidate source files belong to task wy-server-graded-practice-v1-candidate-20260814; no production mutation authority is implied; consult reports/agent_action_log.jsonl
