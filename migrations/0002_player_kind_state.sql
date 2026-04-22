CREATE TABLE IF NOT EXISTS player_kind_state (
  session_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  tier_index INTEGER NOT NULL DEFAULT 1,
  highest_tier_index INTEGER NOT NULL DEFAULT 1,
  rounds_played INTEGER NOT NULL DEFAULT 0,
  perfect_rounds INTEGER NOT NULL DEFAULT 0,
  total_answered INTEGER NOT NULL DEFAULT 0,
  total_correct INTEGER NOT NULL DEFAULT 0,
  perfect_streak INTEGER NOT NULL DEFAULT 0,
  last_run_id TEXT DEFAULT NULL,
  last_round_answered INTEGER NOT NULL DEFAULT 0,
  last_round_correct INTEGER NOT NULL DEFAULT 0,
  last_result TEXT NOT NULL DEFAULT 'idle',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (session_id, kind),
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_player_kind_state_kind
  ON player_kind_state(kind, tier_index DESC, perfect_rounds DESC, total_correct DESC, updated_at DESC);
