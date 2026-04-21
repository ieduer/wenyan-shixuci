CREATE TABLE IF NOT EXISTS anon_sessions (
  id TEXT PRIMARY KEY,
  alias TEXT NOT NULL,
  display_name TEXT DEFAULT '',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS challenge_runs (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  mode TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT,
  score REAL NOT NULL DEFAULT 0,
  correct_count INTEGER NOT NULL DEFAULT 0,
  answered_count INTEGER NOT NULL DEFAULT 0,
  streak INTEGER NOT NULL DEFAULT 0,
  max_streak INTEGER NOT NULL DEFAULT 0,
  rating_delta INTEGER NOT NULL DEFAULT 0,
  report_id TEXT DEFAULT NULL,
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_challenge_runs_session ON challenge_runs(session_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_challenge_runs_mode ON challenge_runs(mode, updated_at DESC);

CREATE TABLE IF NOT EXISTS challenge_items (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  question_type TEXT NOT NULL,
  kind TEXT NOT NULL,
  term_id TEXT NOT NULL,
  term_ids_json TEXT NOT NULL DEFAULT '[]',
  sense_key TEXT NOT NULL,
  prompt_json TEXT NOT NULL,
  answer_json TEXT NOT NULL,
  submitted_answer_json TEXT DEFAULT NULL,
  score REAL NOT NULL DEFAULT 0,
  correct INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  answered_at TEXT DEFAULT NULL,
  FOREIGN KEY (run_id) REFERENCES challenge_runs(id),
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_challenge_items_run ON challenge_items(run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_challenge_items_term ON challenge_items(session_id, term_id, question_type);

CREATE TABLE IF NOT EXISTS user_term_mastery (
  session_id TEXT NOT NULL,
  term_id TEXT NOT NULL,
  sense_key TEXT NOT NULL,
  question_type TEXT NOT NULL,
  mastery_score REAL NOT NULL DEFAULT 0,
  stability_score REAL NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  correct_attempts INTEGER NOT NULL DEFAULT 0,
  best_streak INTEGER NOT NULL DEFAULT 0,
  consecutive_correct INTEGER NOT NULL DEFAULT 0,
  correct_after_delay INTEGER NOT NULL DEFAULT 0,
  decay_factor REAL NOT NULL DEFAULT 1,
  last_result TEXT NOT NULL DEFAULT 'unseen',
  last_seen_at TEXT DEFAULT NULL,
  next_review_at TEXT DEFAULT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (session_id, term_id, sense_key, question_type),
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_mastery_review ON user_term_mastery(session_id, next_review_at);

CREATE TABLE IF NOT EXISTS review_queue (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  term_id TEXT NOT NULL,
  sense_key TEXT NOT NULL,
  question_type TEXT NOT NULL,
  priority REAL NOT NULL DEFAULT 0,
  due_at TEXT NOT NULL,
  source_item_id TEXT DEFAULT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_review_queue_due ON review_queue(session_id, status, due_at, priority DESC);

CREATE TABLE IF NOT EXISTS session_reports (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  mode TEXT NOT NULL,
  score REAL NOT NULL DEFAULT 0,
  accuracy REAL NOT NULL DEFAULT 0,
  summary_markdown TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  report_markdown_key TEXT NOT NULL,
  report_json_key TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  download_count INTEGER NOT NULL DEFAULT 0,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (run_id) REFERENCES challenge_runs(id),
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_session_reports_session ON session_reports(session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS leaderboard_scores (
  session_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  score REAL NOT NULL DEFAULT 0,
  runs INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (session_id, scope, scope_key),
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_leaderboard_scope ON leaderboard_scores(scope, scope_key, score DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS badge_unlocks (
  session_id TEXT NOT NULL,
  badge_key TEXT NOT NULL,
  unlocked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  meta_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (session_id, badge_key),
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_badge_unlocks_session ON badge_unlocks(session_id, unlocked_at DESC);

CREATE TABLE IF NOT EXISTS sync_outbox (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  auth_subject TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT DEFAULT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_sync_outbox_status ON sync_outbox(auth_subject, status, created_at);

CREATE TABLE IF NOT EXISTS abuse_events (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  auth_subject TEXT NOT NULL DEFAULT '',
  ip_hash TEXT NOT NULL DEFAULT '',
  event_type TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id) REFERENCES anon_sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_abuse_events_session ON abuse_events(session_id, created_at DESC);
