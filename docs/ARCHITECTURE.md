# Architecture

## Runtime

- Cloudflare Worker API: `src/index.ts`
- Static frontend: `public/`
- Mutable state: D1
- Report objects: R2
- Immutable knowledge base: build output lands in `public/runtime/*.json`
- `data/runtime/` only stores local build mirrors during development and is git-ignored
- `question_templates/*.json` defines minimum evidence, distractor rules, grading, and review policy per question type

## Data flow

1. `scripts/build_runtime_data.py` reads local textbook, exam, and dictionary sources
2. Script generates:
   - `public/runtime/terms_function*.json`
   - `public/runtime/terms_content*.json`
   - `public/runtime/exam_questions*.json`
   - `public/runtime/textbook_examples*.json`
   - `public/runtime/dict_links*.json`
   - `public/runtime/manifest.json`
3. Runtime JSON is size-checked. Any asset above 25 MiB is split automatically by query dimension and shard index.
4. Frontend consumes Worker APIs only; it does not read raw runtime JSON directly
5. Every term must link back to exam evidence. Textbook, revised dictionary, and idiom dictionary links may be partial; if all three miss, the term stays available but is marked `needs_manual_review`

## Challenge model

- Kind values: `function_word`, `content_word`
- Question types:
  - `xuci_pair_compare`
  - `content_gloss`
  - `translation_keypoint`
  - `sentence_meaning`
  - `passage_meaning`
  - `analysis_short`
- `analysis_short` in V1 is multi-select key-point grading, not free-text scoring

## User center sync

- Frontend mounts `https://my.bdfz.net/site-auth.js`
- Worker never trusts direct `user_id` from the client
- Authenticated sync only happens after Worker verifies the shared `.bdfz.net` session by calling `https://my.bdfz.net/api/session` with the caller cookie
- Sync events are stored in `sync_outbox` first, then flushed to user center on the current or next authenticated request
- `session_reports` only stores report index and summary; report objects live in R2

## Abuse control

- Anonymous sessions use a signed session cookie
- Each challenge item issues a one-time signed `answerToken`
- Answer submit validates token, expiry, `run_id`, `item_id`, and session ownership
- High-frequency or invalid submissions are rate-limited and logged to `abuse_events`
- Leaderboard writes only happen for authenticated users
