export const WY_PRACTICE_CONTRACT_VERSION = 'wy-server-graded-practice-v1' as const;
export const WY_PRACTICE_ADAPTER_CLASS = 'server_graded_practice_v1' as const;
export const WY_PRACTICE_EVENT_SCHEMA = 'bdfz-learning-evidence-event-v2' as const;
export const WY_PRACTICE_SOURCE_RELEASE_ID = 'wy-practice-0542ad12cb1babb6' as const;
export const WY_PRACTICE_MAPPING_VERSION = 'wy-server-graded-practice-mapping-v1' as const;
export const WY_PRACTICE_REGISTRY_VERSION = 'wy-practice-answer-key-registry-2026-08-14-v1' as const;
export const WY_PRACTICE_CATALOG_DIGEST = 'sha256:0542ad12cb1babb6d72e975f1167e71ff82cce187a532d7cf96b84884af515bc' as const;
export const WY_PRACTICE_ANSWER_KEYS_FILE_DIGEST = 'sha256:a87511f0fbb008843bbdda583805d682b3d00203c9643f6b7de1c130b5fe6b19' as const;
export const WY_PRACTICE_CATALOG_ITEM_COUNT = 1244 as const;

export const WY_PRACTICE_BLOCKERS = Object.freeze([
  'user_center_contract_registry_entry_missing',
  'immutable_user_center_user_id_resolver_missing',
  'candidate_migration_not_applied',
  'candidate_worker_not_deployed',
  'real_account_my_a_to_f_acceptance_readback_rollback_missing',
]);

const SOURCE_SYSTEM = 'wenyan-shixuci' as const;
const SOURCE_SITE_KEY = 'wy' as const;
const SOURCE_ORIGIN = 'https://wy.bdfz.net/' as const;
const GRADING_AUTHORITY = 'server_answer_key' as const;
const SOURCE_ATTEMPT_ID_RE = /^[A-Za-z0-9._:-]{8,160}$/;
const DELIVERY_KEY_RE = /^wy_delivery_sha256_[a-f0-9]{64}$/;
const SOURCE_EVENT_ID_RE = /^wy_event_sha256_[a-f0-9]{64}$/;
const SHA256_RE = /^sha256:[a-f0-9]{64}$/;
const QUESTION_TYPES = new Set([
  'content_gloss',
  'function_gloss',
  'sentence_meaning',
  'xuci_pair_compare',
]);
const KINDS = new Set(['content_word', 'function_word']);
const VERIFIED_CATALOG = Symbol('verified-wy-answer-key-catalog');

export interface StoredWyServerGradedAttempt {
  source_attempt_id: string;
  uc_user_id: number;
  challenge_id: string;
  kind: string;
  question_type: string;
  correct: number;
  attempt_no: number;
  answered_at: string;
  answer_token_binding_verified: number;
  grading_authority: string;
  resource_version: string;
}

interface CatalogEntryIdentity {
  kind: string;
  questionType: string;
}

export interface VerifiedWyAnswerKeyCatalog {
  readonly digest: typeof WY_PRACTICE_CATALOG_DIGEST;
  readonly itemCount: typeof WY_PRACTICE_CATALOG_ITEM_COUNT;
  readonly entries: ReadonlyMap<string, CatalogEntryIdentity>;
  readonly [VERIFIED_CATALOG]: true;
}

export interface WyPracticeEvidenceEnvelope {
  schema: typeof WY_PRACTICE_EVENT_SCHEMA;
  schemaVersion: 2;
  sourceSystem: typeof SOURCE_SYSTEM;
  sourceSiteKey: typeof SOURCE_SITE_KEY;
  contractVersion: typeof WY_PRACTICE_CONTRACT_VERSION;
  sourceEventId: string;
  sourceAttemptId: string;
  sourceVersion: typeof WY_PRACTICE_SOURCE_RELEASE_ID;
  sourceReleaseId: typeof WY_PRACTICE_SOURCE_RELEASE_ID;
  canonicalUnitId: string;
  resourceVersion: typeof WY_PRACTICE_CATALOG_DIGEST;
  mappingVersion: typeof WY_PRACTICE_MAPPING_VERSION;
  registryVersion: typeof WY_PRACTICE_REGISTRY_VERSION;
  userId: number;
  academicYear: string;
  dimensionKey: 'practice';
  eventType: 'server_graded_practice_answer';
  interactionKey: string;
  assessmentKind: 'performance';
  scoringRole: 'formative';
  verificationMethod: 'source_answer_key';
  eligibilityStatus: 'eligible';
  resourceKey: string;
  attemptNo: number;
  rawValue: 0 | 1;
  maxValue: 1;
  normalizedValue: 0 | 1;
  correctness: 'correct' | 'incorrect';
  occurredAt: string;
  sourceUrl: typeof SOURCE_ORIGIN;
  sourcePayloadRef: string;
  facets: Array<{ key: string; value: string }>;
}

export interface WyPracticeCandidateRecord {
  deliveryKey: string;
  sourceEventId: string;
  sourceAttemptId: string;
  sourceReleaseId: typeof WY_PRACTICE_SOURCE_RELEASE_ID;
  contractVersion: typeof WY_PRACTICE_CONTRACT_VERSION;
  canonicalUnitId: string;
  resourceVersion: typeof WY_PRACTICE_CATALOG_DIGEST;
  payloadHash: string;
  envelopeJson: string;
  occurredAt: string;
  academicYear: string;
  createdAt: string;
}

export type WyPracticeCandidateCommit =
  | { kind: 'inserted'; record: WyPracticeCandidateRecord }
  | { kind: 'replay'; record: WyPracticeCandidateRecord }
  | {
    kind: 'conflict';
    deliveryKey: string;
    sourceAttemptId: string;
    existingPayloadHash: string;
    rejectedPayloadHash: string;
    conflictId: string;
  };

export interface WyServerGradedAttemptLoader {
  loadServerGradedAttempt(sourceAttemptId: string): Promise<StoredWyServerGradedAttempt | null>;
}

export interface WyPracticeCandidateOutbox {
  commitCandidate(record: WyPracticeCandidateRecord): Promise<WyPracticeCandidateCommit>;
  readHealthCounts(): Promise<WyPracticeHealthCounts>;
}

export interface WyPracticeHealthCounts {
  serverAttempts: number;
  outboxRecords: number;
  pendingMapping: number;
  accepted: number;
  blocked: number;
  quarantined: number;
  conflicts: number;
}

interface D1RunResultLike {
  meta?: { changes?: number };
}

interface D1PreparedStatementLike {
  bind(...values: unknown[]): D1PreparedStatementLike;
  first<T = Record<string, unknown>>(): Promise<T | null>;
  all<T = Record<string, unknown>>(): Promise<{ results?: T[] }>;
  run(): Promise<D1RunResultLike>;
}

export interface D1DatabaseLike {
  prepare(query: string): D1PreparedStatementLike;
}

interface StoredOutboxRow {
  delivery_key: string;
  source_event_id: string;
  source_attempt_id: string;
  source_release_id: string;
  contract_version: string;
  canonical_unit_id: string;
  resource_version: string;
  payload_hash: string;
  envelope_json: string;
  occurred_at: string;
  academic_year: string;
  created_at: string;
}

export class WyPracticeContractError extends Error {
  readonly code: string;

  constructor(code: string) {
    super(code);
    this.name = 'WyPracticeContractError';
    this.code = code;
  }
}

function hasExactKeys(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length
    && actual.every((key, index) => key === expected[index]);
}

function stableCanonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableCanonical).join(',')}]`;
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map((key) => (
      `${JSON.stringify(key)}:${stableCanonical(record[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value);
}

async function sha256Hex(value: unknown): Promise<string> {
  const serialized = typeof value === 'string' ? value : stableCanonical(value);
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(serialized),
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');
}

function compareCodePoints(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function catalogIdentityRows(value: unknown): {
  rows: Array<Record<string, unknown>>;
  entries: Map<string, CatalogEntryIdentity>;
} {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new WyPracticeContractError('answer_key_catalog_not_an_object');
  }
  const source = value as Record<string, unknown>;
  const rows: Array<Record<string, unknown>> = [];
  const entries = new Map<string, CatalogEntryIdentity>();
  for (const [key, rawEntry] of Object.entries(source).sort(([left], [right]) => (
    compareCodePoints(left, right)
  ))) {
    if (!rawEntry || typeof rawEntry !== 'object' || Array.isArray(rawEntry)) {
      throw new WyPracticeContractError('invalid_answer_key_entry');
    }
    const entry = rawEntry as Record<string, unknown>;
    const sourceRef = entry.source_ref;
    if (!sourceRef || typeof sourceRef !== 'object' || Array.isArray(sourceRef)) {
      throw new WyPracticeContractError('invalid_answer_key_source_ref');
    }
    const ref = sourceRef as Record<string, unknown>;
    if (
      entry.challenge_id !== key
      || !KINDS.has(String(entry.kind || ''))
      || !QUESTION_TYPES.has(String(entry.question_type || ''))
      || typeof entry.correct_label !== 'string'
      || entry.correct_label.length < 1
      || typeof entry.correct_text !== 'string'
      || !Array.isArray(entry.term_ids)
      || typeof entry.term_id !== 'string'
      || typeof entry.priority_level !== 'string'
      || typeof entry.source_type !== 'string'
      || !Number.isSafeInteger(Number(ref.year))
      || typeof ref.paper !== 'string'
      || typeof ref.paper_key !== 'string'
      || !Number.isSafeInteger(Number(ref.question_number))
    ) throw new WyPracticeContractError('invalid_answer_key_grading_identity');
    const kind = String(entry.kind);
    const questionType = String(entry.question_type);
    rows.push({
      challengeId: key,
      kind,
      questionType,
      termId: entry.term_id,
      termIds: entry.term_ids,
      priorityLevel: entry.priority_level,
      sourceType: entry.source_type,
      sourceRef: {
        year: ref.year,
        paper: ref.paper,
        paperKey: ref.paper_key,
        questionNumber: ref.question_number,
      },
      correctLabel: entry.correct_label,
      correctText: entry.correct_text,
    });
    entries.set(key, { kind, questionType });
  }
  return { rows, entries };
}

export async function verifyWyAnswerKeyCatalog(
  value: unknown,
): Promise<VerifiedWyAnswerKeyCatalog> {
  const { rows, entries } = catalogIdentityRows(value);
  if (rows.length !== WY_PRACTICE_CATALOG_ITEM_COUNT) {
    throw new WyPracticeContractError('answer_key_catalog_item_count_mismatch');
  }
  const digest = `sha256:${await sha256Hex(JSON.stringify(rows))}`;
  if (digest !== WY_PRACTICE_CATALOG_DIGEST) {
    throw new WyPracticeContractError('answer_key_catalog_digest_mismatch');
  }
  return Object.freeze({
    digest: WY_PRACTICE_CATALOG_DIGEST,
    itemCount: WY_PRACTICE_CATALOG_ITEM_COUNT,
    entries,
    [VERIFIED_CATALOG]: true as const,
  });
}

export function parseWyPracticeDeliveryRequest(value: unknown): { sourceAttemptId: string } {
  if (!hasExactKeys(value, ['sourceAttemptId'])) {
    throw new WyPracticeContractError('unsupported_delivery_request_fields');
  }
  const sourceAttemptId = String(value.sourceAttemptId || '');
  if (!SOURCE_ATTEMPT_ID_RE.test(sourceAttemptId)) {
    throw new WyPracticeContractError('invalid_source_attempt_id');
  }
  return { sourceAttemptId };
}

export function academicYearForBeijing(occurredAt: string): string {
  const occurredMs = Date.parse(occurredAt);
  if (!Number.isFinite(occurredMs)) {
    throw new WyPracticeContractError('invalid_answered_at');
  }
  const shifted = new Date(occurredMs + 8 * 60 * 60 * 1000);
  const year = shifted.getUTCFullYear();
  const month = shifted.getUTCMonth() + 1;
  const start = month >= 9 ? year : year - 1;
  return `${start}-${start + 1}`;
}

function assertStoredAttempt(
  row: StoredWyServerGradedAttempt,
  catalog: VerifiedWyAnswerKeyCatalog,
  nowMs: number,
): CatalogEntryIdentity {
  if (!row || typeof row !== 'object') {
    throw new WyPracticeContractError('server_attempt_not_found');
  }
  if (!SOURCE_ATTEMPT_ID_RE.test(String(row.source_attempt_id || ''))) {
    throw new WyPracticeContractError('invalid_source_attempt_id');
  }
  if (!Number.isSafeInteger(Number(row.uc_user_id)) || Number(row.uc_user_id) <= 0) {
    throw new WyPracticeContractError('immutable_user_center_user_id_missing');
  }
  if (row.resource_version !== WY_PRACTICE_CATALOG_DIGEST) {
    throw new WyPracticeContractError('resource_version_pending_mapping');
  }
  if (!SHA256_RE.test(row.resource_version)) {
    throw new WyPracticeContractError('invalid_resource_version');
  }
  const catalogEntry = catalog.entries.get(String(row.challenge_id || ''));
  if (!catalogEntry) throw new WyPracticeContractError('challenge_not_in_verified_catalog');
  if (row.kind !== catalogEntry.kind || row.question_type !== catalogEntry.questionType) {
    throw new WyPracticeContractError('server_attempt_catalog_identity_mismatch');
  }
  if (row.correct !== 0 && row.correct !== 1) {
    throw new WyPracticeContractError('invalid_server_correctness');
  }
  if (!Number.isSafeInteger(Number(row.attempt_no)) || Number(row.attempt_no) <= 0) {
    throw new WyPracticeContractError('invalid_attempt_number');
  }
  const answeredMs = Date.parse(String(row.answered_at || ''));
  if (!Number.isFinite(answeredMs) || answeredMs > nowMs + 5 * 60 * 1000) {
    throw new WyPracticeContractError('invalid_answered_at');
  }
  if (row.answer_token_binding_verified !== 1) {
    throw new WyPracticeContractError('signed_answer_token_binding_not_verified');
  }
  if (row.grading_authority !== GRADING_AUTHORITY) {
    throw new WyPracticeContractError('server_answer_key_authority_missing');
  }
  return catalogEntry;
}

function recordFromRow(row: StoredOutboxRow): WyPracticeCandidateRecord {
  if (
    !DELIVERY_KEY_RE.test(row.delivery_key)
    || !SOURCE_EVENT_ID_RE.test(row.source_event_id)
    || row.source_release_id !== WY_PRACTICE_SOURCE_RELEASE_ID
    || row.contract_version !== WY_PRACTICE_CONTRACT_VERSION
    || row.resource_version !== WY_PRACTICE_CATALOG_DIGEST
    || !/^[a-f0-9]{64}$/.test(row.payload_hash)
  ) throw new WyPracticeContractError('invalid_persisted_outbox_record');
  return {
    deliveryKey: row.delivery_key,
    sourceEventId: row.source_event_id,
    sourceAttemptId: row.source_attempt_id,
    sourceReleaseId: WY_PRACTICE_SOURCE_RELEASE_ID,
    contractVersion: WY_PRACTICE_CONTRACT_VERSION,
    canonicalUnitId: row.canonical_unit_id,
    resourceVersion: WY_PRACTICE_CATALOG_DIGEST,
    payloadHash: row.payload_hash,
    envelopeJson: row.envelope_json,
    occurredAt: row.occurred_at,
    academicYear: row.academic_year,
    createdAt: row.created_at,
  };
}

export class WyPracticeD1Store implements WyServerGradedAttemptLoader, WyPracticeCandidateOutbox {
  readonly db: D1DatabaseLike;

  constructor(db: D1DatabaseLike) {
    this.db = db;
  }

  async loadServerGradedAttempt(
    sourceAttemptId: string,
  ): Promise<StoredWyServerGradedAttempt | null> {
    return this.db.prepare(
      `SELECT source_attempt_id, uc_user_id, challenge_id, kind, question_type,
              correct, attempt_no, answered_at, answer_token_binding_verified,
              grading_authority, resource_version
         FROM wy_practice_server_attempts
        WHERE source_attempt_id = ?`,
    ).bind(sourceAttemptId).first<StoredWyServerGradedAttempt>();
  }

  async commitCandidate(record: WyPracticeCandidateRecord): Promise<WyPracticeCandidateCommit> {
    const insertion = await this.db.prepare(
      `INSERT OR IGNORE INTO wy_practice_evidence_outbox (
         delivery_key, source_event_id, source_attempt_id, source_release_id,
         contract_version, canonical_unit_id, resource_version, payload_hash,
         envelope_json, delivery_status, occurred_at, academic_year, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_mapping', ?, ?, ?, ?)`,
    ).bind(
      record.deliveryKey,
      record.sourceEventId,
      record.sourceAttemptId,
      record.sourceReleaseId,
      record.contractVersion,
      record.canonicalUnitId,
      record.resourceVersion,
      record.payloadHash,
      record.envelopeJson,
      record.occurredAt,
      record.academicYear,
      record.createdAt,
      record.createdAt,
    ).run();
    const row = await this.db.prepare(
      `SELECT delivery_key, source_event_id, source_attempt_id, source_release_id,
              contract_version, canonical_unit_id, resource_version, payload_hash,
              envelope_json, occurred_at, academic_year, created_at
         FROM wy_practice_evidence_outbox
        WHERE delivery_key = ? OR source_attempt_id = ?
        ORDER BY CASE WHEN delivery_key = ? THEN 0 ELSE 1 END
        LIMIT 1`,
    ).bind(record.deliveryKey, record.sourceAttemptId, record.deliveryKey).first<StoredOutboxRow>();
    if (!row) throw new WyPracticeContractError('outbox_commit_readback_missing');
    const stored = recordFromRow(row);
    const same = stored.deliveryKey === record.deliveryKey
      && stored.sourceEventId === record.sourceEventId
      && stored.sourceAttemptId === record.sourceAttemptId
      && stored.payloadHash === record.payloadHash
      && stored.envelopeJson === record.envelopeJson;
    if (same) {
      return Number(insertion.meta?.changes || 0) > 0
        ? { kind: 'inserted', record: stored }
        : { kind: 'replay', record: stored };
    }
    const conflictId = `wy_conflict_sha256_${await sha256Hex({
      deliveryKey: record.deliveryKey,
      sourceAttemptId: record.sourceAttemptId,
      existingDeliveryKey: stored.deliveryKey,
      existingPayloadHash: stored.payloadHash,
      rejectedPayloadHash: record.payloadHash,
    })}`;
    await this.db.prepare(
      `INSERT OR IGNORE INTO wy_practice_evidence_conflicts (
         conflict_id, delivery_key, source_attempt_id, existing_payload_hash,
         rejected_payload_hash, rejection_code, observed_at
       ) VALUES (?, ?, ?, ?, ?, 'immutable_attempt_payload_conflict', ?)`,
    ).bind(
      conflictId,
      stored.deliveryKey,
      record.sourceAttemptId,
      stored.payloadHash,
      record.payloadHash,
      record.createdAt,
    ).run();
    return {
      kind: 'conflict',
      deliveryKey: stored.deliveryKey,
      sourceAttemptId: record.sourceAttemptId,
      existingPayloadHash: stored.payloadHash,
      rejectedPayloadHash: record.payloadHash,
      conflictId,
    };
  }

  async readHealthCounts(): Promise<WyPracticeHealthCounts> {
    const [attempts, outbox, statuses, conflicts] = await Promise.all([
      this.db.prepare(
        'SELECT COUNT(*) AS count FROM wy_practice_server_attempts',
      ).first<{ count: number }>(),
      this.db.prepare(
        'SELECT COUNT(*) AS count FROM wy_practice_evidence_outbox',
      ).first<{ count: number }>(),
      this.db.prepare(
        `SELECT delivery_status AS status, COUNT(*) AS count
           FROM wy_practice_evidence_outbox
          GROUP BY delivery_status`,
      ).all<{ status: string; count: number }>(),
      this.db.prepare(
        'SELECT COUNT(*) AS count FROM wy_practice_evidence_conflicts',
      ).first<{ count: number }>(),
    ]);
    const counts: WyPracticeHealthCounts = {
      serverAttempts: Number(attempts?.count || 0),
      outboxRecords: Number(outbox?.count || 0),
      pendingMapping: 0,
      accepted: 0,
      blocked: 0,
      quarantined: 0,
      conflicts: Number(conflicts?.count || 0),
    };
    for (const row of statuses.results || []) {
      if (row.status === 'pending_mapping') counts.pendingMapping += Number(row.count || 0);
      else if (row.status === 'accepted') counts.accepted += Number(row.count || 0);
      else if (row.status === 'blocked') counts.blocked += Number(row.count || 0);
      else if (row.status === 'quarantined') counts.quarantined += Number(row.count || 0);
    }
    return counts;
  }
}

export class WyServerGradedPracticeCandidate {
  readonly loader: WyServerGradedAttemptLoader;
  readonly outbox: WyPracticeCandidateOutbox;
  readonly catalog: VerifiedWyAnswerKeyCatalog;
  readonly now: () => number;

  constructor(options: {
    loader: WyServerGradedAttemptLoader;
    outbox: WyPracticeCandidateOutbox;
    catalog: VerifiedWyAnswerKeyCatalog;
    now?: () => number;
  }) {
    if (options.catalog[VERIFIED_CATALOG] !== true) {
      throw new WyPracticeContractError('answer_key_catalog_not_verified');
    }
    this.loader = options.loader;
    this.outbox = options.outbox;
    this.catalog = options.catalog;
    this.now = options.now || Date.now;
  }

  async prepareCandidate(request: unknown): Promise<{
    envelope: WyPracticeEvidenceEnvelope;
    record: WyPracticeCandidateRecord;
  }> {
    const { sourceAttemptId } = parseWyPracticeDeliveryRequest(request);
    const row = await this.loader.loadServerGradedAttempt(sourceAttemptId);
    if (!row) throw new WyPracticeContractError('server_attempt_not_found');
    const catalogEntry = assertStoredAttempt(row, this.catalog, this.now());
    const canonicalUnitId = `wy:practice:${row.challenge_id}`;
    const deliveryIdentity = {
      sourceSiteKey: SOURCE_SITE_KEY,
      contractVersion: WY_PRACTICE_CONTRACT_VERSION,
      sourceReleaseId: WY_PRACTICE_SOURCE_RELEASE_ID,
      canonicalUnitId,
      resourceVersion: WY_PRACTICE_CATALOG_DIGEST,
      sourceAttemptId: row.source_attempt_id,
    };
    const identityHash = await sha256Hex(deliveryIdentity);
    const sourceEventId = `wy_event_sha256_${identityHash}`;
    const deliveryKey = `wy_delivery_sha256_${identityHash}`;
    const score = row.correct === 1 ? 1 : 0;
    const academicYear = academicYearForBeijing(row.answered_at);
    const envelope: WyPracticeEvidenceEnvelope = {
      schema: WY_PRACTICE_EVENT_SCHEMA,
      schemaVersion: 2,
      sourceSystem: SOURCE_SYSTEM,
      sourceSiteKey: SOURCE_SITE_KEY,
      contractVersion: WY_PRACTICE_CONTRACT_VERSION,
      sourceEventId,
      sourceAttemptId: row.source_attempt_id,
      sourceVersion: WY_PRACTICE_SOURCE_RELEASE_ID,
      sourceReleaseId: WY_PRACTICE_SOURCE_RELEASE_ID,
      canonicalUnitId,
      resourceVersion: WY_PRACTICE_CATALOG_DIGEST,
      mappingVersion: WY_PRACTICE_MAPPING_VERSION,
      registryVersion: WY_PRACTICE_REGISTRY_VERSION,
      userId: Number(row.uc_user_id),
      academicYear,
      dimensionKey: 'practice',
      eventType: 'server_graded_practice_answer',
      interactionKey: catalogEntry.questionType,
      assessmentKind: 'performance',
      scoringRole: 'formative',
      verificationMethod: 'source_answer_key',
      eligibilityStatus: 'eligible',
      resourceKey: canonicalUnitId,
      attemptNo: Number(row.attempt_no),
      rawValue: score,
      maxValue: 1,
      normalizedValue: score,
      correctness: score === 1 ? 'correct' : 'incorrect',
      occurredAt: row.answered_at,
      sourceUrl: SOURCE_ORIGIN,
      sourcePayloadRef: `wy_practice_server_attempts:${row.source_attempt_id}`,
      facets: [
        { key: 'kind', value: catalogEntry.kind },
        { key: 'question_type', value: catalogEntry.questionType },
        { key: 'answer_authority', value: GRADING_AUTHORITY },
      ],
    };
    const envelopeJson = stableCanonical(envelope);
    const createdAt = new Date(this.now()).toISOString();
    const record: WyPracticeCandidateRecord = {
      deliveryKey,
      sourceEventId,
      sourceAttemptId: row.source_attempt_id,
      sourceReleaseId: WY_PRACTICE_SOURCE_RELEASE_ID,
      contractVersion: WY_PRACTICE_CONTRACT_VERSION,
      canonicalUnitId,
      resourceVersion: WY_PRACTICE_CATALOG_DIGEST,
      payloadHash: await sha256Hex(envelopeJson),
      envelopeJson,
      occurredAt: row.answered_at,
      academicYear,
      createdAt,
    };
    return { envelope, record };
  }

  async commitCandidate(request: unknown): Promise<{
    status: 'pending_mapping' | 'quarantined';
    inserted: boolean;
    replayed: boolean;
    sourceEventId: string | null;
    deliveryKey: string | null;
    errorCode: string | null;
  }> {
    try {
      const prepared = await this.prepareCandidate(request);
      const committed = await this.outbox.commitCandidate(prepared.record);
      if (committed.kind === 'conflict') {
        return {
          status: 'quarantined',
          inserted: false,
          replayed: false,
          sourceEventId: prepared.record.sourceEventId,
          deliveryKey: committed.deliveryKey,
          errorCode: 'immutable_attempt_payload_conflict',
        };
      }
      return {
        status: 'pending_mapping',
        inserted: committed.kind === 'inserted',
        replayed: committed.kind === 'replay',
        sourceEventId: committed.record.sourceEventId,
        deliveryKey: committed.record.deliveryKey,
        errorCode: null,
      };
    } catch (error) {
      if (
        error instanceof WyPracticeContractError
        && error.code === 'resource_version_pending_mapping'
      ) {
        return {
          status: 'pending_mapping',
          inserted: false,
          replayed: false,
          sourceEventId: null,
          deliveryKey: null,
          errorCode: error.code,
        };
      }
      throw error;
    }
  }

  async health(): Promise<{
    status: 'blocked';
    candidate: true;
    adapterClass: typeof WY_PRACTICE_ADAPTER_CLASS;
    contractVersion: typeof WY_PRACTICE_CONTRACT_VERSION;
    sourceReleaseId: typeof WY_PRACTICE_SOURCE_RELEASE_ID;
    mappingDisposition: 'pending_mapping';
    runtimeScoringActive: false;
    productionDeploymentAuthorized: false;
    blockers: readonly string[];
    catalog: { itemCount: number; digest: string };
    counts: WyPracticeHealthCounts;
  }> {
    return {
      status: 'blocked',
      candidate: true,
      adapterClass: WY_PRACTICE_ADAPTER_CLASS,
      contractVersion: WY_PRACTICE_CONTRACT_VERSION,
      sourceReleaseId: WY_PRACTICE_SOURCE_RELEASE_ID,
      mappingDisposition: 'pending_mapping',
      runtimeScoringActive: false,
      productionDeploymentAuthorized: false,
      blockers: WY_PRACTICE_BLOCKERS,
      catalog: {
        itemCount: this.catalog.itemCount,
        digest: this.catalog.digest,
      },
      counts: await this.outbox.readHealthCounts(),
    };
  }
}
