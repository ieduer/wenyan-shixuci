# GitHub Actions workload-routing annex

Project: `ieduer/wenyan-shixuci` (public).

The default branch has a path-filtered source-candidate pull-request/manual
gate. Keep it as verification only; add an explicit 10-minute job timeout when
the exact current branch is next safely changed. Do not add deployment,
schedule, AI/OCR/PDF batch or artifact retention to this gate. Standard public
runners do not consume the private allowance, but source authority and bounded
runtime remain mandatory.

## Account boundary

- Workspace authority: `/Users/ylsuen/CF/runbooks/github_actions_usage_and_workload_routing.md`.
- Evidence: `/Users/ylsuen/CF/reports/github_actions_usage_audit_2026-08-22.md`.
- Current reset: **2026-09-01 00:00 UTC** = **2026-08-31 17:00 PDT** =
  **2026-09-01 08:00 CST**. Reset is not remediation and does not authorize
  mass replay.
- Account target is 1,400 private minutes, with day-7/day-14/day-21 checkpoints
  at 350/700/1,050 and freeze/escalation thresholds at 1,400/1,600/1,800/2,000.
- No schedule is authorized by this annex. Any future schedule must record owner,
  maximum runtime/monthly minutes, request and job timeouts, concurrency,
  disable path and last-duration readback.

## Synchronization status

This is a documentation-only, task-scoped annex. Existing operations files or
adjacent source were already dirty/untracked, so they were not rewritten. Use
GitHub's current default-branch tree as the workflow authority, preserve the
pre-existing work, and synchronize this annex into the canonical project
manual only after ownership and exact source authority are clean. No workflow,
artifact, repository setting, release or runtime changed in this task.
