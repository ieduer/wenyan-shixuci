import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import {
  WY_PRACTICE_ADAPTER_CLASS,
  WY_PRACTICE_ANSWER_KEYS_FILE_DIGEST,
  WY_PRACTICE_CATALOG_DIGEST,
  WY_PRACTICE_CATALOG_ITEM_COUNT,
  WY_PRACTICE_CONTRACT_VERSION,
  WY_PRACTICE_SOURCE_RELEASE_ID,
  WyPracticeContractError,
  WyPracticeD1Store,
  WyServerGradedPracticeCandidate,
  academicYearForBeijing,
  parseWyPracticeDeliveryRequest,
  verifyWyAnswerKeyCatalog,
} from '../src/wy-server-graded-practice-v1.ts';

const NOW = Date.parse('2026-09-02T00:00:00.000Z');
const ANSWER_KEYS_URL = new URL('../src/generated/answer_keys.json', import.meta.url);
const MIGRATION_URL = new URL(
  '../migrations/0100_wy_server_graded_practice_candidate_v1.sql',
  import.meta.url,
);
const answerKeysBytes = await readFile(ANSWER_KEYS_URL);
const answerKeys = JSON.parse(answerKeysBytes.toString('utf8'));
const verifiedCatalog = await verifyWyAnswerKeyCatalog(answerKeys);
const migrationSql = await readFile(MIGRATION_URL, 'utf8');

function validAttempt(overrides = {}) {
  return {
    source_attempt_id: 'wy-attempt:test-00000001',
    uc_user_id: 42,
    challenge_id: 'xuci-beijing-2002-none-q9',
    kind: 'function_word',
    question_type: 'xuci_pair_compare',
    correct: 1,
    attempt_no: 1,
    answered_at: '2026-09-01T08:00:00.000Z',
    answer_token_binding_verified: 1,
    grading_authority: 'server_answer_key',
    resource_version: WY_PRACTICE_CATALOG_DIGEST,
    ...overrides,
  };
}

class SQLiteD1 {
  constructor(database) {
    this.database = database;
  }

  prepare(query) {
    const database = this.database;
    let values = [];
    return {
      bind(...nextValues) {
        values = nextValues;
        return this;
      },
      async first() {
        return database.prepare(query).get(...values) || null;
      },
      async all() {
        return { results: database.prepare(query).all(...values) };
      },
      async run() {
        const result = database.prepare(query).run(...values);
        return { meta: { changes: Number(result.changes || 0) } };
      },
    };
  }
}

function sqliteHarness(attempt = validAttempt()) {
  const database = new DatabaseSync(':memory:');
  database.exec('PRAGMA foreign_keys=ON');
  database.exec(migrationSql);
  database.prepare(
    `INSERT INTO wy_practice_server_attempts (
       source_attempt_id, uc_user_id, challenge_id, kind, question_type,
       correct, attempt_no, answered_at, answer_token_binding_verified,
       grading_authority, resource_version, created_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    attempt.source_attempt_id,
    attempt.uc_user_id,
    attempt.challenge_id,
    attempt.kind,
    attempt.question_type,
    attempt.correct,
    attempt.attempt_no,
    attempt.answered_at,
    attempt.answer_token_binding_verified,
    attempt.grading_authority,
    attempt.resource_version,
    '2026-09-01T08:00:00.000Z',
  );
  const store = new WyPracticeD1Store(new SQLiteD1(database));
  const service = new WyServerGradedPracticeCandidate({
    loader: store,
    outbox: store,
    catalog: verifiedCatalog,
    now: () => NOW,
  });
  return { database, store, service };
}

function memoryOutbox() {
  return {
    async commitCandidate() {
      throw new Error('unexpected candidate commit');
    },
    async readHealthCounts() {
      return {
        serverAttempts: 0,
        outboxRecords: 0,
        pendingMapping: 0,
        accepted: 0,
        blocked: 0,
        quarantined: 0,
        conflicts: 0,
      };
    },
  };
}

function candidateWithLoader(loader) {
  return new WyServerGradedPracticeCandidate({
    loader,
    outbox: memoryOutbox(),
    catalog: verifiedCatalog,
    now: () => NOW,
  });
}

function allKeys(value, result = new Set()) {
  if (Array.isArray(value)) {
    for (const item of value) allKeys(item, result);
  } else if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      result.add(key);
      allKeys(item, result);
    }
  }
  return result;
}

test('machine-readable contract stays candidate, blocked and pending_mapping', async () => {
  const contract = JSON.parse(await readFile(
    new URL('../contracts/wy-server-graded-practice-v1.json', import.meta.url),
    'utf8',
  ));
  assert.equal(contract.contractVersion, WY_PRACTICE_CONTRACT_VERSION);
  assert.equal(contract.adapterClass, WY_PRACTICE_ADAPTER_CLASS);
  assert.equal(contract.sourceReleaseId, WY_PRACTICE_SOURCE_RELEASE_ID);
  assert.equal(contract.activation.status, 'blocked');
  assert.equal(contract.activation.candidate, true);
  assert.equal(contract.activation.mappingDisposition, 'pending_mapping');
  assert.equal(contract.activation.runtimeScoringActive, false);
  assert.equal(contract.activation.productionDeploymentAuthorized, false);
  assert.equal(contract.producer.publicRoute, null);
  assert.deepEqual(contract.producer.acceptedDeliveryRequestFields, ['sourceAttemptId']);
  assert.equal(contract.delivery.migrationApplied, false);
  assert.equal(contract.delivery.sinkBinding, null);
  assert.deepEqual(contract.delivery.statusStateMachine.allowedTransitions, [
    'pending_mapping->pending_mapping',
    'pending_mapping->blocked',
    'pending_mapping->accepted',
    'pending_mapping->quarantined',
    'blocked->blocked',
    'blocked->accepted',
    'blocked->quarantined',
    'accepted->accepted',
    'quarantined->quarantined',
  ]);
  assert.deepEqual(contract.delivery.statusStateMachine.terminalStates, [
    'accepted',
    'quarantined',
  ]);
  assert.deepEqual(contract.delivery.futureConsumerActivationRequirements, [
    'CAS_claim_lease_before_delivery',
    'lease_expiry_recovery_after_worker_crash',
    'read_back_status_and_sink_receipt_before_acknowledgement',
    'crash_safe_replay_must_preserve_delivery_identity_and_first_envelope',
  ]);
});

test('candidate is isolated from the current Worker route and bindings', async () => {
  const [workerSource, wrangler] = await Promise.all([
    readFile(new URL('../src/index.ts', import.meta.url), 'utf8'),
    readFile(new URL('../wrangler.toml', import.meta.url), 'utf8'),
  ]);
  assert.equal(workerSource.includes('wy-server-graded-practice-v1'), false);
  assert.equal(workerSource.includes('WyServerGradedPracticeCandidate'), false);
  assert.equal(wrangler.includes('WY_PRACTICE'), false);
  assert.equal(wrangler.includes('LEARNING_EVIDENCE'), false);
});

test('CI declares only exact Node 22.21.1 and 24.18.0 authorities', async () => {
  const [workflow, nvmrc] = await Promise.all([
    readFile(new URL('../.github/workflows/wy-source-candidate.yml', import.meta.url), 'utf8'),
    readFile(new URL('../.nvmrc', import.meta.url), 'utf8'),
  ]);
  assert.equal(nvmrc.trim(), '24.18.0');
  const versions = [...workflow.matchAll(/^\s+- (\d+\.\d+\.\d+)$/gm)].map((match) => match[1]);
  assert.deepEqual(versions, ['22.21.1', '24.18.0']);
  assert.match(workflow, /actions\/checkout@[a-f0-9]{40}/);
  assert.match(workflow, /actions\/setup-node@[a-f0-9]{40}/);
  assert.match(workflow, /npm ci --ignore-scripts/);
});

test('tracked answer catalog has the exact audited bytes and semantic grading identity', async () => {
  const rawDigest = `sha256:${createHash('sha256').update(answerKeysBytes).digest('hex')}`;
  assert.equal(rawDigest, WY_PRACTICE_ANSWER_KEYS_FILE_DIGEST);
  assert.equal(verifiedCatalog.itemCount, WY_PRACTICE_CATALOG_ITEM_COUNT);
  assert.equal(verifiedCatalog.digest, WY_PRACTICE_CATALOG_DIGEST);
  assert.equal(verifiedCatalog.entries.size, 1244);
  const contract = JSON.parse(await readFile(
    new URL('../contracts/wy-server-graded-practice-v1.json', import.meta.url),
    'utf8',
  ));
  assert.equal(contract.resourceCatalog.itemCountWithCorrectLabel, 1244);
  assert.equal(contract.resourceCatalog.sourceRuleCoveragePercent, 100);
  assert.equal(contract.resourceCatalog.centralMappingDisposition, 'pending_mapping');
});

test('answer catalog drift fails closed instead of silently changing a release', async () => {
  const drifted = structuredClone(answerKeys);
  drifted['xuci-beijing-2002-none-q9'].correct_label = 'Z';
  await assert.rejects(
    () => verifyWyAnswerKeyCatalog(drifted),
    (error) => error instanceof WyPracticeContractError
      && error.code === 'answer_key_catalog_digest_mismatch',
  );
});

test('delivery request accepts only an opaque source attempt id', () => {
  assert.deepEqual(
    parseWyPracticeDeliveryRequest({ sourceAttemptId: 'wy-attempt:test-00000001' }),
    { sourceAttemptId: 'wy-attempt:test-00000001' },
  );
  for (const hostile of [
    { sourceAttemptId: 'wy-attempt:test-00000001', userId: 42 },
    { sourceAttemptId: 'wy-attempt:test-00000001', slug: 'student' },
    { sourceAttemptId: 'wy-attempt:test-00000001', correct: true },
    { sourceAttemptId: 'wy-attempt:test-00000001', score: 1 },
    { sourceAttemptId: 'wy-attempt:test-00000001', grade: 'A' },
    { sourceAttemptId: 'wy-attempt:test-00000001', weight: 99 },
    { sourceAttemptId: 'wy-attempt:test-00000001', answer: 'B' },
    { sourceAttemptId: 'wy-attempt:test-00000001', answerToken: 'hostile' },
  ]) {
    assert.throws(
      () => parseWyPracticeDeliveryRequest(hostile),
      (error) => error instanceof WyPracticeContractError
        && error.code === 'unsupported_delivery_request_fields',
    );
  }
});

test('candidate migration is additive, idempotent and append-only', () => {
  assert.doesNotMatch(migrationSql, /\bDROP\s+TABLE\b|\bALTER\s+TABLE\b|\bDELETE\s+FROM\b/i);
  const database = new DatabaseSync(':memory:');
  database.exec('PRAGMA foreign_keys=ON');
  database.exec(migrationSql);
  database.exec(migrationSql);
  assert.equal(
    database.prepare('SELECT COUNT(*) AS count FROM wy_practice_candidate_releases').get().count,
    1,
  );
  const attempt = validAttempt();
  database.prepare(
    `INSERT INTO wy_practice_server_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    attempt.source_attempt_id,
    attempt.uc_user_id,
    attempt.challenge_id,
    attempt.kind,
    attempt.question_type,
    attempt.correct,
    attempt.attempt_no,
    attempt.answered_at,
    attempt.answer_token_binding_verified,
    attempt.grading_authority,
    attempt.resource_version,
    '2026-09-01T08:00:00.000Z',
  );
  assert.throws(
    () => database.exec(
      "UPDATE wy_practice_server_attempts SET correct=0 WHERE source_attempt_id='wy-attempt:test-00000001'",
    ),
    /wy_practice_server_attempt_immutable/,
  );
  assert.throws(
    () => database.exec(
      "DELETE FROM wy_practice_server_attempts WHERE source_attempt_id='wy-attempt:test-00000001'",
    ),
    /wy_practice_server_attempt_immutable/,
  );
  database.close();
});

test('append-only facts reject REPLACE and UPSERT bypasses with recursive triggers disabled', async () => {
  const original = validAttempt();
  const { database, store, service } = sqliteHarness(original);
  database.exec('PRAGMA recursive_triggers=OFF');
  await service.commitCandidate({ sourceAttemptId: original.source_attempt_id });

  assert.throws(
    () => database.exec(
      `INSERT OR REPLACE INTO wy_practice_candidate_releases (
         source_release_id, contract_version, adapter_class, source_catalog_digest,
         answer_keys_file_digest, catalog_item_count, mapping_disposition,
         activation_allowed, runtime_scoring_active, created_at
       )
       SELECT source_release_id, contract_version, adapter_class,
              'sha256:hostile-release-replacement', answer_keys_file_digest,
              catalog_item_count, mapping_disposition, activation_allowed,
              runtime_scoring_active, created_at
         FROM wy_practice_candidate_releases
        WHERE source_release_id='wy-practice-0542ad12cb1babb6'`,
    ),
    /wy_practice_candidate_release_immutable/,
  );
  assert.throws(
    () => database.exec(
      `INSERT OR REPLACE INTO wy_practice_server_attempts
       SELECT source_attempt_id, uc_user_id, challenge_id, kind, question_type,
              0, attempt_no, answered_at, answer_token_binding_verified,
              grading_authority, resource_version, created_at
         FROM wy_practice_server_attempts
        WHERE source_attempt_id='wy-attempt:test-00000001'`,
    ),
    /wy_practice_server_attempt_immutable/,
  );
  assert.throws(
    () => database.exec(
      `INSERT INTO wy_practice_server_attempts
       SELECT * FROM wy_practice_server_attempts
        WHERE source_attempt_id='wy-attempt:test-00000001'
       ON CONFLICT(source_attempt_id) DO UPDATE SET correct=0`,
    ),
    /wy_practice_server_attempt_immutable/,
  );
  assert.throws(
    () => database.exec(
      `INSERT OR REPLACE INTO wy_practice_evidence_outbox
       SELECT delivery_key, source_event_id, source_attempt_id, source_release_id,
              contract_version, canonical_unit_id, resource_version,
              '${'b'.repeat(64)}', envelope_json, delivery_status, sink_receipt_id,
              last_error_code, delivery_attempts, occurred_at, academic_year,
              created_at, updated_at
         FROM wy_practice_evidence_outbox
        WHERE source_attempt_id='wy-attempt:test-00000001'`,
    ),
    /wy_practice_evidence_identity_immutable/,
  );

  const changedLoader = {
    async loadServerGradedAttempt() {
      return validAttempt({ correct: 0 });
    },
  };
  const changedService = new WyServerGradedPracticeCandidate({
    loader: changedLoader,
    outbox: store,
    catalog: verifiedCatalog,
    now: () => NOW,
  });
  const conflict = await changedService.commitCandidate({
    sourceAttemptId: original.source_attempt_id,
  });
  assert.equal(conflict.status, 'quarantined');
  const storedConflictId = database.prepare(
    'SELECT conflict_id FROM wy_practice_evidence_conflicts LIMIT 1',
  ).get().conflict_id;
  assert.match(storedConflictId, /^wy_conflict_sha256_[a-f0-9]{64}$/);
  assert.throws(
    () => database.prepare(
      `INSERT OR REPLACE INTO wy_practice_evidence_conflicts
       SELECT conflict_id, delivery_key, source_attempt_id, existing_payload_hash,
              rejected_payload_hash, rejection_code, '2026-09-03T00:00:00.000Z'
         FROM wy_practice_evidence_conflicts
        WHERE conflict_id=?`,
    ).run(storedConflictId),
    /wy_practice_evidence_conflict_immutable/,
  );
  database.close();
});

test('envelope uses only immutable UC identity and persisted server grade', async () => {
  const { database, service } = sqliteHarness();
  const { envelope, record } = await service.prepareCandidate({
    sourceAttemptId: 'wy-attempt:test-00000001',
  });
  assert.equal(envelope.schema, 'bdfz-learning-evidence-event-v2');
  assert.equal(envelope.userId, 42);
  assert.equal(envelope.dimensionKey, 'practice');
  assert.equal(envelope.eventType, 'server_graded_practice_answer');
  assert.equal(envelope.assessmentKind, 'performance');
  assert.equal(envelope.scoringRole, 'formative');
  assert.equal(envelope.verificationMethod, 'source_answer_key');
  assert.equal(envelope.rawValue, 1);
  assert.equal(envelope.maxValue, 1);
  assert.equal(envelope.normalizedValue, 1);
  assert.equal(envelope.correctness, 'correct');
  assert.equal(envelope.resourceVersion, WY_PRACTICE_CATALOG_DIGEST);
  assert.equal(envelope.canonicalUnitId, 'wy:practice:xuci-beijing-2002-none-q9');
  assert.match(envelope.sourceEventId, /^wy_event_sha256_[a-f0-9]{64}$/);
  assert.match(record.deliveryKey, /^wy_delivery_sha256_[a-f0-9]{64}$/);
  assert.match(record.payloadHash, /^[a-f0-9]{64}$/);
  const forbiddenKeys = [
    'slug',
    'studentName',
    'studentNumber',
    'className',
    'submittedAnswer',
    'correctAnswer',
    'answerToken',
    'cookie',
    'sessionId',
    'grade',
    'weight',
  ];
  const keys = allKeys(envelope);
  for (const key of forbiddenKeys) assert.equal(keys.has(key), false, key);
  database.close();
});

test('slug-only identity, missing token binding and non-server grading fail closed', async () => {
  for (const [overrides, code] of [
    [{ uc_user_id: 0 }, 'immutable_user_center_user_id_missing'],
    [{ uc_user_id: Number.NaN }, 'immutable_user_center_user_id_missing'],
    [{ answer_token_binding_verified: 0 }, 'signed_answer_token_binding_not_verified'],
    [{ grading_authority: 'request_body' }, 'server_answer_key_authority_missing'],
  ]) {
    const loader = { async loadServerGradedAttempt() { return validAttempt(overrides); } };
    await assert.rejects(
      () => candidateWithLoader(loader).prepareCandidate({
        sourceAttemptId: 'wy-attempt:test-00000001',
      }),
      (error) => error instanceof WyPracticeContractError && error.code === code,
    );
  }
});

test('attempt challenge, kind and question type must match the verified catalog', async () => {
  for (const overrides of [
    { challenge_id: 'not-in-catalog' },
    { kind: 'content_word' },
    { question_type: 'content_gloss' },
  ]) {
    const loader = { async loadServerGradedAttempt() { return validAttempt(overrides); } };
    await assert.rejects(
      () => candidateWithLoader(loader).prepareCandidate({
        sourceAttemptId: 'wy-attempt:test-00000001',
      }),
      WyPracticeContractError,
    );
  }
});

test('Beijing academic year changes exactly at September 1 midnight', () => {
  assert.equal(academicYearForBeijing('2026-08-31T15:59:59.999Z'), '2025-2026');
  assert.equal(academicYearForBeijing('2026-08-31T16:00:00.000Z'), '2026-2027');
});

test('same attempt and envelope are inserted once then replayed', async () => {
  const { database, service } = sqliteHarness();
  const first = await service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' });
  const second = await service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' });
  assert.equal(first.status, 'pending_mapping');
  assert.equal(first.inserted, true);
  assert.equal(second.status, 'pending_mapping');
  assert.equal(second.replayed, true);
  assert.equal(first.sourceEventId, second.sourceEventId);
  assert.equal(first.deliveryKey, second.deliveryKey);
  assert.equal(
    database.prepare('SELECT COUNT(*) AS count FROM wy_practice_evidence_outbox').get().count,
    1,
  );
  database.close();
});

test('concurrent same-payload replay cannot create a second outbox record', async () => {
  const { database, service } = sqliteHarness();
  const results = await Promise.all([
    service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' }),
    service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' }),
  ]);
  assert.equal(results.filter((result) => result.inserted).length, 1);
  assert.equal(results.filter((result) => result.replayed).length, 1);
  assert.equal(
    database.prepare('SELECT COUNT(*) AS count FROM wy_practice_evidence_outbox').get().count,
    1,
  );
  database.close();
});

test('outbox status history is monotonic and terminal states cannot regress', async () => {
  const { database, service } = sqliteHarness();
  await service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' });
  const deliveryKey = database.prepare(
    'SELECT delivery_key FROM wy_practice_evidence_outbox LIMIT 1',
  ).get().delivery_key;

  assert.equal(
    database.prepare(
      'SELECT COUNT(*) AS count FROM wy_practice_evidence_status_history',
    ).get().count,
    1,
  );
  database.exec('PRAGMA recursive_triggers=OFF');
  assert.throws(() => database.prepare(
    `INSERT OR REPLACE INTO wy_practice_evidence_status_history
       (delivery_key, from_status, to_status, changed_at)
     VALUES (?, 'initial', 'pending_mapping', '2026-09-02T00:00:01.000Z')`,
  ).run(deliveryKey), /wy_practice_evidence_status_history_immutable/);
  assert.throws(() => database.prepare(
    `UPDATE wy_practice_evidence_status_history
        SET to_status='blocked'
      WHERE delivery_key=?`,
  ).run(deliveryKey), /wy_practice_evidence_status_history_immutable/);
  assert.throws(() => database.prepare(
    'DELETE FROM wy_practice_evidence_status_history WHERE delivery_key=?',
  ).run(deliveryKey), /wy_practice_evidence_status_history_immutable/);
  database.exec(migrationSql);
  assert.equal(
    database.prepare(
      'SELECT COUNT(*) AS count FROM wy_practice_evidence_status_history',
    ).get().count,
    1,
  );

  assert.doesNotThrow(() => database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='pending_mapping', updated_at='2026-09-02T00:01:00.000Z'
      WHERE delivery_key=?`,
  ).run(deliveryKey));
  assert.doesNotThrow(() => database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='blocked', updated_at='2026-09-02T00:02:00.000Z'
      WHERE delivery_key=?`,
  ).run(deliveryKey));
  assert.throws(() => database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='pending_mapping', updated_at='2026-09-02T00:03:00.000Z'
      WHERE delivery_key=?`,
  ).run(deliveryKey), /wy_practice_evidence_status_regression/);
  assert.doesNotThrow(() => database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='accepted', updated_at='2026-09-02T00:04:00.000Z'
      WHERE delivery_key=?`,
  ).run(deliveryKey));
  assert.doesNotThrow(() => database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='accepted', updated_at='2026-09-02T00:05:00.000Z'
      WHERE delivery_key=?`,
  ).run(deliveryKey));
  assert.throws(() => database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='blocked', updated_at='2026-09-02T00:06:00.000Z'
      WHERE delivery_key=?`,
  ).run(deliveryKey), /wy_practice_evidence_status_regression/);
  assert.equal(
    database.prepare(
      'SELECT COUNT(*) AS count FROM wy_practice_evidence_status_history',
    ).get().count,
    3,
  );

  const quarantined = sqliteHarness();
  await quarantined.service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' });
  const quarantinedKey = quarantined.database.prepare(
    'SELECT delivery_key FROM wy_practice_evidence_outbox LIMIT 1',
  ).get().delivery_key;
  assert.doesNotThrow(() => quarantined.database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='quarantined', updated_at='2026-09-02T00:07:00.000Z'
      WHERE delivery_key=?`,
  ).run(quarantinedKey));
  assert.doesNotThrow(() => quarantined.database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='quarantined', updated_at='2026-09-02T00:08:00.000Z'
      WHERE delivery_key=?`,
  ).run(quarantinedKey));
  assert.throws(() => quarantined.database.prepare(
    `UPDATE wy_practice_evidence_outbox
        SET delivery_status='accepted', updated_at='2026-09-02T00:09:00.000Z'
      WHERE delivery_key=?`,
  ).run(quarantinedKey), /wy_practice_evidence_status_regression/);
  quarantined.database.close();
  database.close();
});

test('same immutable attempt with a changed grade is quarantined without overwrite', async () => {
  const original = validAttempt();
  const { database, store } = sqliteHarness(original);
  let loaded = original;
  const loader = { async loadServerGradedAttempt() { return loaded; } };
  const service = new WyServerGradedPracticeCandidate({
    loader,
    outbox: store,
    catalog: verifiedCatalog,
    now: () => NOW,
  });
  const first = await service.commitCandidate({ sourceAttemptId: original.source_attempt_id });
  assert.equal(first.inserted, true);
  const firstEnvelope = database.prepare(
    'SELECT envelope_json FROM wy_practice_evidence_outbox WHERE source_attempt_id = ?',
  ).get(original.source_attempt_id).envelope_json;
  loaded = validAttempt({ correct: 0 });
  const conflict = await service.commitCandidate({ sourceAttemptId: original.source_attempt_id });
  assert.equal(conflict.status, 'quarantined');
  assert.equal(conflict.errorCode, 'immutable_attempt_payload_conflict');
  assert.equal(
    database.prepare(
      'SELECT envelope_json FROM wy_practice_evidence_outbox WHERE source_attempt_id = ?',
    ).get(original.source_attempt_id).envelope_json,
    firstEnvelope,
  );
  assert.equal(
    database.prepare('SELECT COUNT(*) AS count FROM wy_practice_evidence_conflicts').get().count,
    1,
  );
  database.close();
});

test('unknown catalog version remains pending_mapping and creates no score envelope', async () => {
  const loader = {
    async loadServerGradedAttempt() {
      return validAttempt({ resource_version: `sha256:${'b'.repeat(64)}` });
    },
  };
  const outbox = memoryOutbox();
  const service = new WyServerGradedPracticeCandidate({
    loader,
    outbox,
    catalog: verifiedCatalog,
    now: () => NOW,
  });
  const result = await service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' });
  assert.deepEqual(result, {
    status: 'pending_mapping',
    inserted: false,
    replayed: false,
    sourceEventId: null,
    deliveryKey: null,
    errorCode: 'resource_version_pending_mapping',
  });
});

test('health is aggregate-only and cannot claim runtime or production activation', async () => {
  const { database, service } = sqliteHarness();
  await service.commitCandidate({ sourceAttemptId: 'wy-attempt:test-00000001' });
  const health = await service.health();
  assert.equal(health.status, 'blocked');
  assert.equal(health.candidate, true);
  assert.equal(health.mappingDisposition, 'pending_mapping');
  assert.equal(health.runtimeScoringActive, false);
  assert.equal(health.productionDeploymentAuthorized, false);
  assert.equal(health.counts.serverAttempts, 1);
  assert.equal(health.counts.outboxRecords, 1);
  assert.equal(health.counts.pendingMapping, 1);
  const serialized = JSON.stringify(health);
  assert.equal(serialized.includes('wy-attempt:test-00000001'), false);
  assert.equal(serialized.includes('"userId"'), false);
  database.close();
});
