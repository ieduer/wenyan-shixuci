-- Source-only, fail-closed candidate for wy-server-graded-practice-v1.
-- This migration has not been applied. It does not install a public route,
-- User Center sink, Queue producer, identity resolver, or production scorer.

CREATE TABLE IF NOT EXISTS wy_practice_candidate_releases (
  source_release_id TEXT PRIMARY KEY,
  contract_version TEXT NOT NULL,
  adapter_class TEXT NOT NULL,
  source_catalog_digest TEXT NOT NULL,
  answer_keys_file_digest TEXT NOT NULL,
  catalog_item_count INTEGER NOT NULL CHECK (catalog_item_count > 0),
  mapping_disposition TEXT NOT NULL CHECK (mapping_disposition = 'pending_mapping'),
  activation_allowed INTEGER NOT NULL CHECK (activation_allowed = 0),
  runtime_scoring_active INTEGER NOT NULL CHECK (runtime_scoring_active = 0),
  created_at TEXT NOT NULL
);

INSERT OR IGNORE INTO wy_practice_candidate_releases (
  source_release_id,
  contract_version,
  adapter_class,
  source_catalog_digest,
  answer_keys_file_digest,
  catalog_item_count,
  mapping_disposition,
  activation_allowed,
  runtime_scoring_active,
  created_at
) VALUES (
  'wy-practice-0542ad12cb1babb6',
  'wy-server-graded-practice-v1',
  'server_graded_practice_v1',
  'sha256:0542ad12cb1babb6d72e975f1167e71ff82cce187a532d7cf96b84884af515bc',
  'sha256:a87511f0fbb008843bbdda583805d682b3d00203c9643f6b7de1c130b5fe6b19',
  1244,
  'pending_mapping',
  0,
  0,
  '2026-08-14T00:00:00.000Z'
);

-- A future, independently reviewed answer-handler integration may append one
-- record only after authoritative server grading, signed token binding, and an
-- immutable positive User Center user id all succeed. No raw answer is stored.
CREATE TABLE IF NOT EXISTS wy_practice_server_attempts (
  source_attempt_id TEXT PRIMARY KEY,
  uc_user_id INTEGER NOT NULL CHECK (uc_user_id > 0),
  challenge_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('content_word', 'function_word')),
  question_type TEXT NOT NULL CHECK (
    question_type IN ('content_gloss', 'function_gloss', 'sentence_meaning', 'xuci_pair_compare')
  ),
  correct INTEGER NOT NULL CHECK (correct IN (0, 1)),
  attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
  answered_at TEXT NOT NULL,
  answer_token_binding_verified INTEGER NOT NULL CHECK (answer_token_binding_verified = 1),
  grading_authority TEXT NOT NULL CHECK (grading_authority = 'server_answer_key'),
  resource_version TEXT NOT NULL CHECK (
    resource_version GLOB 'sha256:*' AND length(resource_version) = 71
  ),
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wy_practice_attempts_user_time
  ON wy_practice_server_attempts(uc_user_id, answered_at);

CREATE TABLE IF NOT EXISTS wy_practice_evidence_outbox (
  delivery_key TEXT PRIMARY KEY CHECK (
    delivery_key GLOB 'wy_delivery_sha256_*' AND length(delivery_key) = 83
  ),
  source_event_id TEXT NOT NULL UNIQUE CHECK (
    source_event_id GLOB 'wy_event_sha256_*' AND length(source_event_id) = 80
  ),
  source_attempt_id TEXT NOT NULL UNIQUE REFERENCES wy_practice_server_attempts(source_attempt_id),
  source_release_id TEXT NOT NULL REFERENCES wy_practice_candidate_releases(source_release_id),
  contract_version TEXT NOT NULL CHECK (contract_version = 'wy-server-graded-practice-v1'),
  canonical_unit_id TEXT NOT NULL,
  resource_version TEXT NOT NULL CHECK (
    resource_version GLOB 'sha256:*' AND length(resource_version) = 71
  ),
  payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
  envelope_json TEXT NOT NULL,
  delivery_status TEXT NOT NULL DEFAULT 'pending_mapping' CHECK (
    delivery_status IN ('pending_mapping', 'blocked', 'accepted', 'quarantined')
  ),
  sink_receipt_id TEXT DEFAULT NULL,
  last_error_code TEXT DEFAULT NULL,
  delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
  occurred_at TEXT NOT NULL,
  academic_year TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wy_practice_outbox_status
  ON wy_practice_evidence_outbox(delivery_status, updated_at);

CREATE TABLE IF NOT EXISTS wy_practice_evidence_conflicts (
  conflict_id TEXT PRIMARY KEY CHECK (
    conflict_id GLOB 'wy_conflict_sha256_*' AND length(conflict_id) = 83
  ),
  delivery_key TEXT NOT NULL REFERENCES wy_practice_evidence_outbox(delivery_key),
  source_attempt_id TEXT NOT NULL REFERENCES wy_practice_server_attempts(source_attempt_id),
  existing_payload_hash TEXT NOT NULL CHECK (length(existing_payload_hash) = 64),
  rejected_payload_hash TEXT NOT NULL CHECK (length(rejected_payload_hash) = 64),
  rejection_code TEXT NOT NULL CHECK (rejection_code = 'immutable_attempt_payload_conflict'),
  observed_at TEXT NOT NULL
);

-- Source attempts and candidate release facts are append-only. Delivery state
-- may advance later, but its identity and envelope bytes cannot be rewritten.
CREATE TRIGGER IF NOT EXISTS deny_wy_practice_release_update
BEFORE UPDATE ON wy_practice_candidate_releases
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_candidate_release_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_release_delete
BEFORE DELETE ON wy_practice_candidate_releases
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_candidate_release_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_attempt_update
BEFORE UPDATE ON wy_practice_server_attempts
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_server_attempt_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_attempt_delete
BEFORE DELETE ON wy_practice_server_attempts
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_server_attempt_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_outbox_identity_update
BEFORE UPDATE OF
  delivery_key,
  source_event_id,
  source_attempt_id,
  source_release_id,
  contract_version,
  canonical_unit_id,
  resource_version,
  payload_hash,
  envelope_json,
  occurred_at,
  academic_year,
  created_at
ON wy_practice_evidence_outbox
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_identity_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_outbox_delete
BEFORE DELETE ON wy_practice_evidence_outbox
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_history_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_conflict_update
BEFORE UPDATE ON wy_practice_evidence_conflicts
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_conflict_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_conflict_delete
BEFORE DELETE ON wy_practice_evidence_conflicts
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_conflict_immutable');
END;
