# Project State

Last updated: 2026-11-04 UTC
Current version: accepted production source 71053cc4c64ad27b3c889d9dbbe8acdcddf2868a (refs/heads/claude/wy-bank-fix-20261104), Worker version 39b3fb8c-206a-4c7c-99f5-886c2a827616, deployment 4eab69a2-df38-4035-a9ed-7435f80188bf (2026-11-04T02:51Z); registry project wy-bdfz-net is state accepted
Current objective: question-bank quality; 438 textbook questions fixed from the 2026-11-04 two-round review (data/manual/v3/bank_fixes_20261104.json, scripts/v3/apply_bank_fixes.py)
Completed work: live 7447c0b9 adopted as accepted baseline 4b86c2c (rebuilds the live bundle byte-for-byte); bank fixes released through release-worker-transaction.mjs; Status 20261104-wy-bank-review r1
Pending work: merge claude/wy-bank-fix-20261104 into main (main b1337f5 predates the v3 1,858-key bank); the inactive wy-server-graded-practice-v1 candidate still pins the 1,244-key main catalog and fails against the live bank; a full data rebuild needs the archived textbook_ai_migration tree
Known problems: runtime JSON (data/runtime, public/runtime) is gitignored — rebuild it by copying the accepted runtime and running npm run apply:fixes; the authenticated answer path still owns data by mutable User Center slug
Next recommended task: merge the release branch into main; realign the scoring candidate with the 1,858-key catalog before any activation
Deployment status: production = 39b3fb8c (accepted); no D1/binding/route/secret change
Rollback anchor: version 7447c0b9-7cb0-4595-9e02-dbe7891cf89d (deployment 3f8c5946, source 4b86c2c) via /Users/ylsuen/CF/_meta/reports/operations/bank-fix-wy-20261104/release/wy-release.mjs rollback <reason>
Operations authority: docs/OPERATIONS.md; release report /Users/ylsuen/CF/_meta/reports/operations/bank-fix-wy-20261104/REPORT.md
Ownership status: release bank-fix-wy-20261104 complete (claude-opus-5.5); consult reports/agent_action_log.jsonl
