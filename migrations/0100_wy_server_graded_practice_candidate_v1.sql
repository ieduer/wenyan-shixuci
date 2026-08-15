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

-- UPDATE/DELETE guards are not sufficient for immutable facts: SQLite
-- INSERT OR REPLACE can remove the conflicting row without firing DELETE
-- triggers when recursive_triggers is disabled. Reject any non-identical
-- release collision before the conflict algorithm can replace history, while
-- still allowing this migration's exact seed to be replayed idempotently.
CREATE TRIGGER IF NOT EXISTS deny_wy_practice_release_conflicting_insert
BEFORE INSERT ON wy_practice_candidate_releases
WHEN EXISTS (
  SELECT 1
    FROM wy_practice_candidate_releases
   WHERE source_release_id = NEW.source_release_id
     AND (
       contract_version IS NOT NEW.contract_version
       OR adapter_class IS NOT NEW.adapter_class
       OR source_catalog_digest IS NOT NEW.source_catalog_digest
       OR answer_keys_file_digest IS NOT NEW.answer_keys_file_digest
       OR catalog_item_count IS NOT NEW.catalog_item_count
       OR mapping_disposition IS NOT NEW.mapping_disposition
       OR activation_allowed IS NOT NEW.activation_allowed
       OR runtime_scoring_active IS NOT NEW.runtime_scoring_active
       OR created_at IS NOT NEW.created_at
     )
)
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_candidate_release_immutable');
END;

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

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_attempt_conflicting_insert
BEFORE INSERT ON wy_practice_server_attempts
WHEN EXISTS (
  SELECT 1
    FROM wy_practice_server_attempts
   WHERE source_attempt_id = NEW.source_attempt_id
)
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_server_attempt_immutable');
END;

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

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_outbox_conflicting_insert
BEFORE INSERT ON wy_practice_evidence_outbox
WHEN EXISTS (
  SELECT 1
    FROM wy_practice_evidence_outbox
   WHERE delivery_key = NEW.delivery_key
      OR source_event_id = NEW.source_event_id
      OR source_attempt_id = NEW.source_attempt_id
)
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_identity_immutable');
END;

-- Delivery status is a monotonic state machine. Keep its transitions in an
-- append-only audit table so a future sink can reconcile after a retry or
-- crash without silently moving an evidence record backwards.
CREATE TABLE IF NOT EXISTS wy_practice_evidence_status_history (
  history_id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_key TEXT NOT NULL REFERENCES wy_practice_evidence_outbox(delivery_key),
  from_status TEXT NOT NULL CHECK (
    from_status IN ('initial', 'pending_mapping', 'blocked', 'accepted', 'quarantined')
  ),
  to_status TEXT NOT NULL CHECK (
    to_status IN ('pending_mapping', 'blocked', 'accepted', 'quarantined')
  ),
  changed_at TEXT NOT NULL,
  UNIQUE(delivery_key, from_status, to_status)
);

-- Backfill an initial history row if this additive migration is applied after
-- a pre-existing candidate outbox. Re-running the migration is idempotent.
INSERT INTO wy_practice_evidence_status_history (
  delivery_key, from_status, to_status, changed_at
)
SELECT delivery_key, 'initial', delivery_status, created_at
  FROM wy_practice_evidence_outbox AS outbox
 WHERE NOT EXISTS (
   SELECT 1
     FROM wy_practice_evidence_status_history AS history
    WHERE history.delivery_key = outbox.delivery_key
      AND history.from_status = 'initial'
      AND history.to_status = outbox.delivery_status
 );

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_status_history_conflicting_insert
BEFORE INSERT ON wy_practice_evidence_status_history
WHEN EXISTS (
  SELECT 1
    FROM wy_practice_evidence_status_history
   WHERE delivery_key = NEW.delivery_key
     AND from_status = NEW.from_status
     AND to_status = NEW.to_status
)
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_status_history_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_status_history_update
BEFORE UPDATE ON wy_practice_evidence_status_history
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_status_history_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_status_history_delete
BEFORE DELETE ON wy_practice_evidence_status_history
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_status_history_immutable');
END;

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_outbox_status_regression
BEFORE UPDATE OF delivery_status ON wy_practice_evidence_outbox
WHEN NOT (
  OLD.delivery_status = NEW.delivery_status
  OR (
    OLD.delivery_status = 'pending_mapping'
    AND NEW.delivery_status IN ('blocked', 'accepted', 'quarantined')
  )
  OR (
    OLD.delivery_status = 'blocked'
    AND NEW.delivery_status IN ('accepted', 'quarantined')
  )
)
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_status_regression');
END;

CREATE TRIGGER IF NOT EXISTS record_wy_practice_outbox_insert_status
AFTER INSERT ON wy_practice_evidence_outbox
BEGIN
  INSERT OR IGNORE INTO wy_practice_evidence_status_history (
    delivery_key, from_status, to_status, changed_at
  ) VALUES (NEW.delivery_key, 'initial', NEW.delivery_status, NEW.created_at);
END;

CREATE TRIGGER IF NOT EXISTS record_wy_practice_outbox_status_transition
AFTER UPDATE OF delivery_status ON wy_practice_evidence_outbox
WHEN OLD.delivery_status <> NEW.delivery_status
BEGIN
  INSERT OR IGNORE INTO wy_practice_evidence_status_history (
    delivery_key, from_status, to_status, changed_at
  ) VALUES (NEW.delivery_key, OLD.delivery_status, NEW.delivery_status, NEW.updated_at);
END;

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

CREATE TRIGGER IF NOT EXISTS deny_wy_practice_conflict_conflicting_insert
BEFORE INSERT ON wy_practice_evidence_conflicts
WHEN EXISTS (
  SELECT 1
    FROM wy_practice_evidence_conflicts
   WHERE conflict_id = NEW.conflict_id
)
BEGIN
  SELECT RAISE(ABORT, 'wy_practice_evidence_conflict_immutable');
END;

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
