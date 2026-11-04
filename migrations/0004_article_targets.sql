ALTER TABLE challenge_runs ADD COLUMN target_scope TEXT NOT NULL DEFAULT '';
ALTER TABLE challenge_runs ADD COLUMN target_key TEXT NOT NULL DEFAULT '';
ALTER TABLE challenge_runs ADD COLUMN target_label TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_challenge_runs_target
  ON challenge_runs(session_id, kind, mode, target_scope, target_key, status, updated_at DESC);
