type Kind = "function_word" | "content_word";
type Mode = "warmup" | "ranked" | "review";
type QuestionType =
  | "xuci_pair_compare"
  | "content_gloss"
  | "translation_keypoint"
  | "sentence_meaning"
  | "passage_meaning"
  | "analysis_short";

interface Env {
  DB: D1Database;
  REPORTS: R2Bucket;
  ASSETS: Fetcher;
  SITE_KEY: string;
  SITE_TITLE: string;
  PRIMARY_HOST: string;
  USER_CENTER_ORIGIN: string;
  USER_CENTER_SESSION_ENDPOINT: string;
  REPORT_SYNC_ENDPOINT: string;
  ASSET_MAX_BYTES: string;
  TURNSTILE_ENABLED: string;
  TURNSTILE_SITE_KEY: string;
  LOCAL_DEV?: string;
  SIGNING_SECRET?: string;
}

interface UserCenterUser {
  slug: string;
  displayName?: string;
}

interface UserCenterSession {
  authenticated: boolean;
  user: UserCenterUser | null;
}

interface UsageProfile {
  part_of_speech?: string;
  semantic_value?: string;
  syntactic_function?: string;
  relation?: string;
}

interface TextbookRef {
  ref_id: string;
  school_stage: string;
  book_key: string;
  title: string;
  kind: string;
  page_start: number | null;
  page_end: number | null;
  sentence: string;
  context_window: string[];
  note_block: string;
}

interface BasisRecord {
  basis_type: string;
  exam_year: number | null;
  question_number: number | null;
  evidence_sentence: string;
  answer_span: string;
  why_required: string;
  confidence: number;
  needs_manual_review: boolean;
}

interface DictLink {
  entry_id: string | number;
  headword: string;
  summary: string;
  source: string;
}

interface TermRecord {
  term_id: string;
  kind: Kind;
  headword: string;
  display_headword: string;
  must_master: boolean;
  must_master_basis: BasisRecord[];
  beijing_frequency: number;
  national_frequency: number;
  year_range: [number | null, number | null];
  question_type_counts: Record<string, number>;
  frequencies: {
    total: number;
    beijing: number;
    national: number;
  };
  usage_relations: UsageProfile[] | Array<Record<string, unknown>>;
  sample_glosses: string[];
  textbook_refs: TextbookRef[];
  dict_refs: DictLink[];
  idiom_refs: DictLink[];
  needs_manual_review: boolean;
}

interface BankOption {
  label?: string;
  text?: string;
  key?: string;
  term_id?: string;
  headword?: string;
  sentences?: string[];
  usage_profile?: UsageProfile[];
}

interface BankAnswer {
  label?: string;
  keys?: string[];
}

interface BankItem {
  challenge_id: string;
  question_type: QuestionType;
  kind: Kind;
  term_id: string;
  term_ids?: string[];
  year?: number;
  paper?: string;
  paper_key?: string;
  question_number?: number;
  stem: string;
  sentence?: string;
  passage?: string;
  options: BankOption[];
  answer: BankAnswer;
  explanation: string;
  response_mode?: "single_select" | "multi_select";
}

interface ExamQuestionsPayload {
  built_at: string;
  challenge_bank: Record<QuestionType, BankItem[]>;
}

interface RuntimeManifestShard {
  file_name: string;
  size_bytes: number;
  sha256: string;
}

interface RuntimeManifestAsset {
  kind: "list" | "object";
  shards: RuntimeManifestShard[];
}

interface RuntimeManifest {
  built_at: string;
  asset_max_bytes: number;
  assets: Record<string, RuntimeManifestAsset>;
  stats: Record<string, number>;
}

interface RuntimeData {
  manifest: RuntimeManifest;
  termsFunction: TermRecord[];
  termsContent: TermRecord[];
  examQuestions: ExamQuestionsPayload;
  textbookExamples: Record<string, TextbookRef[]>;
  dictLinks: Record<string, { revised_sense_links: DictLink[]; idiom_links: DictLink[] }>;
  termMap: Map<string, TermRecord>;
}

interface SessionState {
  id: string;
  alias: string;
  displayName: string;
  token: string;
  isNew: boolean;
}

interface ChallengePromptPayload {
  challengeId: string;
  questionType: QuestionType;
  kind: Kind;
  termId: string;
  termIds: string[];
  senseKey: string;
  stem: string;
  sentence?: string;
  passage?: string;
  options: BankOption[];
  explanation: string;
  responseMode: "single_select" | "multi_select";
}

interface ChallengeAnswerPayload {
  label?: string;
  keys?: string[];
}

interface AuthContext {
  session: UserCenterSession;
  cookieHeader: string;
}

const SESSION_COOKIE = "wy_session";
const DEV_SIGNING_SECRET = "local-dev-only-not-for-production";
const QUESTION_TYPE_ORDER_CONTENT: QuestionType[] = [
  "content_gloss",
  "translation_keypoint",
  "sentence_meaning",
  "passage_meaning",
  "analysis_short",
];
const BADGE_DEFS = [
  { key: "first-correct", title: "初鸣勋章", detail: "答对第一题" },
  { key: "streak-3", title: "三连破阵", detail: "连续答对 3 题" },
  { key: "review-tamer", title: "错题驯服者", detail: "清空当前错题追击" },
  { key: "perfect-run", title: "满分抄手", detail: "单次挑战 5 题全对" },
];

let runtimeCache: Promise<RuntimeData> | null = null;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      return routeApi(request, env, url);
    }
    return env.ASSETS.fetch(request);
  },
};

async function routeApi(request: Request, env: Env, url: URL): Promise<Response> {
  try {
    const path = url.pathname;
    const method = request.method.toUpperCase();
    if (method === "GET" && path === "/api/bootstrap") return handleBootstrap(request, env);
    if (method === "GET" && path === "/api/challenge/next") return handleChallengeNext(request, env, url);
    if (method === "POST" && path === "/api/challenge/answer") return handleChallengeAnswer(request, env);
    if (method === "GET" && path.startsWith("/api/term/")) return handleTerm(request, env, path.slice("/api/term/".length));
    if (method === "GET" && path === "/api/leaderboard") return handleLeaderboard(env, url);
    if (method === "POST" && path === "/api/report/finalize") return handleReportFinalize(request, env);
    if (method === "GET" && path.startsWith("/api/report/")) return handleReportGet(request, env, path.slice("/api/report/".length), url);
    return json({ error: "Not found" }, 404);
  } catch (error) {
    console.error("api error", error);
    return json(
      {
        error: "Internal error",
        detail: error instanceof Error ? error.message : String(error),
      },
      500
    );
  }
}

async function handleBootstrap(request: Request, env: Env): Promise<Response> {
  const runtime = await loadRuntime(env, request);
  const sessionState = await ensureAnonSession(request, env);
  const auth = await getAuthContext(request, env);
  if (auth) {
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }
  const leaderboard = await loadLeaderboard(env, "day");
  const response = json(
    {
      ok: true,
      site: {
        siteKey: env.SITE_KEY,
        title: env.SITE_TITLE,
        turnstileEnabled: env.TURNSTILE_ENABLED === "1",
        turnstileSiteKey: env.TURNSTILE_SITE_KEY || "",
      },
      session: {
        id: sessionState.id,
        alias: sessionState.alias,
        displayName: sessionState.displayName,
      },
      auth: auth
        ? {
            authenticated: true,
            user: auth.user,
          }
        : { authenticated: false, user: null },
      stats: {
        functionTerms: runtime.termsFunction.length,
        contentTerms: runtime.termsContent.length,
        functionChallenges: runtime.examQuestions.challenge_bank.xuci_pair_compare.length,
        contentChallenges:
          runtime.examQuestions.challenge_bank.content_gloss.length +
          runtime.examQuestions.challenge_bank.translation_keypoint.length,
      },
      leaderboard,
    },
    200
  );
  if (sessionState.isNew) {
    response.headers.append("Set-Cookie", buildSessionCookie(env, request, sessionState.token));
  }
  return response;
}

async function handleChallengeNext(request: Request, env: Env, url: URL): Promise<Response> {
  const runtime = await loadRuntime(env, request);
  const sessionState = await ensureAnonSession(request, env);
  const auth = await getAuthContext(request, env);
  if (auth) {
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }

  const kind = canonicalKind(url.searchParams.get("kind"));
  const mode = canonicalMode(url.searchParams.get("mode"));
  let runId = cleanString(url.searchParams.get("runId"), 80);
  let runRow = runId ? await getRun(env, runId, sessionState.id) : null;
  if (!runRow) {
    runId = crypto.randomUUID();
    await env.DB.prepare(
      `INSERT INTO challenge_runs (id, session_id, kind, mode, status, started_at, updated_at)
       VALUES (?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    )
      .bind(runId, sessionState.id, kind, mode)
      .run();
    runRow = await getRun(env, runId, sessionState.id);
  }
  if (!runRow) {
    return json({ error: "Unable to establish challenge run" }, 500);
  }

  const reviewItem = mode === "review" ? await claimReviewTarget(env, sessionState.id) : null;
  const bankItem = selectBankItem(runtime, sessionState.id, runRow, kind, mode, reviewItem);
  if (!bankItem) {
    return json({ error: "No challenge item available" }, 404);
  }

  const senseKey = buildSenseKey(bankItem);
  const prompt: ChallengePromptPayload = {
    challengeId: bankItem.challenge_id,
    questionType: bankItem.question_type,
    kind,
    termId: bankItem.term_id,
    termIds: bankItem.term_ids && bankItem.term_ids.length ? bankItem.term_ids : [bankItem.term_id],
    senseKey,
    stem: bankItem.stem,
    sentence: bankItem.sentence,
    passage: bankItem.passage,
    options: bankItem.options,
    explanation: bankItem.explanation,
    responseMode: bankItem.response_mode || "single_select",
  };
  const answer = bankItem.answer;
  const itemId = crypto.randomUUID();
  await env.DB.prepare(
    `INSERT INTO challenge_items (id, run_id, session_id, question_type, kind, term_id, term_ids_json, sense_key, prompt_json, answer_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      itemId,
      runId,
      sessionState.id,
      prompt.questionType,
      kind,
      prompt.termId,
      JSON.stringify(prompt.termIds),
      senseKey,
      JSON.stringify(prompt),
      JSON.stringify(answer)
    )
    .run();

  const answerToken = await signToken(
    env,
    request,
    {
      type: "answer",
      sid: sessionState.id,
      rid: runId,
      iid: itemId,
      exp: Math.floor(Date.now() / 1000) + 10 * 60,
    }
  );

  const response = json(
    {
      ok: true,
      run: summarizeRun(runRow),
      item: {
        id: itemId,
        prompt,
        answerToken,
      },
      reviewTarget: reviewItem,
    },
    200
  );
  if (sessionState.isNew) {
    response.headers.append("Set-Cookie", buildSessionCookie(env, request, sessionState.token));
  }
  return response;
}

async function handleChallengeAnswer(request: Request, env: Env): Promise<Response> {
  const sessionState = await ensureAnonSession(request, env);
  const auth = await getAuthContext(request, env);
  if (auth) {
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }
  const ipHash = await sha256Hex(request.headers.get("CF-Connecting-IP") || "");
  if (!(await enforceRateLimit(env, sessionState.id, auth?.user.slug || "", ipHash))) {
    return json({ error: "Too many submissions. Wait a moment and try again." }, 429);
  }

  const body = (await request.json().catch(() => null)) as Record<string, unknown> | null;
  if (!body) {
    return json({ error: "Invalid JSON body" }, 400);
  }
  const answerToken = cleanString(body.answerToken, 1200);
  const submitted = normalizeSubmittedAnswer(body.answer);
  if (!answerToken || !submitted) {
    return json({ error: "answerToken and answer are required" }, 400);
  }
  const tokenPayload = await verifyToken(env, request, answerToken);
  if (
    !tokenPayload ||
    tokenPayload.type !== "answer" ||
    tokenPayload.sid !== sessionState.id ||
    typeof tokenPayload.rid !== "string" ||
    typeof tokenPayload.iid !== "string"
  ) {
    await logAbuseEvent(env, sessionState.id, auth?.user.slug || "", ipHash, "invalid_answer_token", {
      answerTokenPresent: Boolean(answerToken),
    });
    return json({ error: "Invalid or expired answer token" }, 401);
  }

  const row = await env.DB.prepare(
    `SELECT id, run_id, question_type, kind, term_id, term_ids_json, sense_key, prompt_json, answer_json, answered_at
     FROM challenge_items WHERE id = ? AND run_id = ? AND session_id = ?`
  )
    .bind(tokenPayload.iid, tokenPayload.rid, sessionState.id)
    .first<Record<string, unknown>>();
  if (!row) {
    return json({ error: "Challenge item not found" }, 404);
  }
  if (row.answered_at) {
    return json({ error: "This item has already been answered" }, 409);
  }

  const prompt = safeParseObject(row.prompt_json) as unknown as ChallengePromptPayload;
  const answer = safeParseObject(row.answer_json) as unknown as BankAnswer;
  const correct = evaluateAnswer(prompt.responseMode, answer, submitted);
  const relevantTermIds = resolveRelevantTermIds(prompt, answer, submitted);
  const scoreDelta = scoreForAnswer(prompt.questionType, correct);

  await env.DB.prepare(
    `UPDATE challenge_items
     SET submitted_answer_json = ?, score = ?, correct = ?, answered_at = CURRENT_TIMESTAMP
     WHERE id = ?`
  )
    .bind(JSON.stringify(submitted), scoreDelta, correct ? 1 : 0, String(row.id))
    .run();

  await updateRunAfterAnswer(env, String(row.run_id), scoreDelta, correct);
  await updateMasteryRows(env, sessionState.id, relevantTermIds, String(row.sense_key), String(row.question_type), correct);
  await updateReviewQueue(env, sessionState.id, relevantTermIds, String(row.sense_key), prompt.questionType, correct, String(row.id));

  const run = await getRun(env, String(row.run_id), sessionState.id);
  const pendingReviewCount = await countPendingReview(env, sessionState.id);
  const newlyUnlocked = run ? await unlockBadges(env, sessionState.id, run, pendingReviewCount) : [];

  return json(
    {
      ok: true,
      result: {
        correct,
        scoreDelta,
        encouragingFeedback: correct
          ? pickEncouragement(true, prompt.questionType)
          : pickEncouragement(false, prompt.questionType),
        explanation: prompt.explanation,
        pendingReviewCount,
        relatedTerms: relevantTermIds,
      },
      run: run ? summarizeRun(run) : null,
      badges: newlyUnlocked,
    },
    200
  );
}

async function handleTerm(request: Request, env: Env, rawTermId: string): Promise<Response> {
  const runtime = await loadRuntime(env, request);
  const termId = decodeURIComponent(rawTermId);
  const term = runtime.termMap.get(termId);
  if (!term) {
    return json({ error: "Term not found" }, 404);
  }
  const relatedCounts = buildRelatedChallengeCounts(runtime, termId);
  return json(
    {
      ok: true,
      term,
      relatedCounts,
      textbookRefs: runtime.textbookExamples[termId] || term.textbook_refs,
      dictLinks: runtime.dictLinks[termId] || {
        revised_sense_links: term.dict_refs,
        idiom_links: term.idiom_refs,
      },
    },
    200
  );
}

async function handleLeaderboard(env: Env, url: URL): Promise<Response> {
  const scopeParam = cleanString(url.searchParams.get("scope"), 12);
  const scope = scopeParam === "week" ? "week" : scopeParam === "all" ? "all" : "day";
  return json(await loadLeaderboard(env, scope), 200);
}

async function handleReportFinalize(request: Request, env: Env): Promise<Response> {
  const sessionState = await ensureAnonSession(request, env);
  const auth = await getAuthContext(request, env);
  if (auth) {
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }
  const body = (await request.json().catch(() => null)) as Record<string, unknown> | null;
  const runId = cleanString(body?.runId, 80);
  if (!runId) {
    return json({ error: "runId required" }, 400);
  }
  const run = await getRun(env, runId, sessionState.id);
  if (!run) {
    return json({ error: "Run not found" }, 404);
  }
  const items = await listRunItems(env, runId, sessionState.id);
  if (!items.length) {
    return json({ error: "Run has no answered items yet" }, 400);
  }

  const reportId = crypto.randomUUID();
  const accuracy = Number(run.answered_count) > 0 ? Number(run.correct_count) / Number(run.answered_count) : 0;
  const pendingReviewCount = await countPendingReview(env, sessionState.id);
  const mistakeTerms = collectMistakeTerms(items);
  const newMasteredTerms = await listMasteredTerms(env, sessionState.id);
  const reportJsonPayload = {
    report_id: reportId,
    kind: String(run.kind),
    mode: String(run.mode),
    started_at: String(run.started_at),
    finished_at: new Date().toISOString(),
    score: Number(run.score),
    accuracy,
    mistake_terms: mistakeTerms,
    new_mastered_terms: newMasteredTerms.slice(0, 20),
    queue_after_run: pendingReviewCount,
    download_files: {
      markdown: `/api/report/${reportId}?format=markdown`,
      json: `/api/report/${reportId}?format=json`,
    },
  };
  const reportMarkdown = buildReportMarkdown(run, items, reportJsonPayload);
  const reportJson = JSON.stringify(reportJsonPayload, null, 2);
  const reportMarkdownKey = `reports/${reportId}/report.md`;
  const reportJsonKey = `reports/${reportId}/report.json`;

  await env.REPORTS.put(reportMarkdownKey, reportMarkdown, {
    httpMetadata: { contentType: "text/markdown; charset=utf-8" },
  });
  await env.REPORTS.put(reportJsonKey, reportJson, {
    httpMetadata: { contentType: "application/json; charset=utf-8" },
  });

  await env.DB.prepare(
    `INSERT INTO session_reports
     (id, run_id, session_id, kind, mode, score, accuracy, summary_markdown, summary_json, report_markdown_key, report_json_key, content_hash, started_at, finished_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      reportId,
      runId,
      sessionState.id,
      String(run.kind),
      String(run.mode),
      Number(run.score),
      accuracy,
      reportMarkdown.slice(0, 1200),
      JSON.stringify({ mistake_terms: mistakeTerms, queue_after_run: pendingReviewCount }),
      reportMarkdownKey,
      reportJsonKey,
      await sha256Hex(reportMarkdown + reportJson),
      String(run.started_at),
      reportJsonPayload.finished_at
    )
    .run();

  await env.DB.prepare(
    `UPDATE challenge_runs
     SET status = 'finished', finished_at = ?, updated_at = ?, report_id = ?
     WHERE id = ? AND session_id = ?`
  )
    .bind(reportJsonPayload.finished_at, reportJsonPayload.finished_at, reportId, runId, sessionState.id)
    .run();

  if (auth) {
    await queueSyncEvent(env, sessionState.id, auth.user.slug, "progress_sync", buildProgressSyncPayload(run, accuracy));
    await queueSyncEvent(
      env,
      sessionState.id,
      auth.user.slug,
      "mastery_report",
      buildMasterySyncPayload(run, accuracy, reportId)
    );
    await updateLeaderboard(env, sessionState.id, auth.user.slug, auth.user.displayName || auth.user.slug, Number(run.score));
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }

  return json(
    {
      ok: true,
      reportId,
      markdownUrl: `/api/report/${reportId}?format=markdown`,
      jsonUrl: `/api/report/${reportId}?format=json`,
      summary: reportJsonPayload,
    },
    200
  );
}

async function handleReportGet(request: Request, env: Env, reportId: string, url: URL): Promise<Response> {
  const sessionState = await ensureAnonSession(request, env);
  const auth = await getAuthContext(request, env);
  if (auth) {
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }
  const report = await env.DB.prepare(
    `SELECT id, session_id, report_markdown_key, report_json_key, download_count
     FROM session_reports WHERE id = ? AND session_id = ?`
  )
    .bind(reportId, sessionState.id)
    .first<Record<string, unknown>>();
  if (!report) {
    return json({ error: "Report not found" }, 404);
  }

  const format = cleanString(url.searchParams.get("format"), 12) === "markdown" ? "markdown" : "json";
  const key = format === "markdown" ? String(report.report_markdown_key) : String(report.report_json_key);
  const object = await env.REPORTS.get(key);
  if (!object) {
    return json({ error: "Report object missing from storage" }, 404);
  }

  await env.DB.prepare(
    `UPDATE session_reports SET download_count = download_count + 1 WHERE id = ?`
  )
    .bind(reportId)
    .run();

  if (auth) {
    await queueSyncEvent(
      env,
      sessionState.id,
      auth.user.slug,
      "download_record",
      buildDownloadSyncPayload(reportId, format)
    );
    await flushSyncOutbox(env, sessionState.id, auth.user.slug, auth.cookieHeader);
  }

  const headers = new Headers();
  headers.set("Content-Type", format === "markdown" ? "text/markdown; charset=utf-8" : "application/json; charset=utf-8");
  headers.set(
    "Content-Disposition",
    `attachment; filename="${format === "markdown" ? `${reportId}.md` : `${reportId}.json`}"`
  );
  return new Response(object.body, { headers });
}

async function loadRuntime(env: Env, request: Request): Promise<RuntimeData> {
  if (!runtimeCache) {
    runtimeCache = (async () => {
      const manifest = (await fetchRuntimeAsset<RuntimeManifest>(env, request, "manifest.json")) as RuntimeManifest;
      const termsFunction = await loadShardedAsset<TermRecord[]>(env, request, manifest, "terms_function");
      const termsContent = await loadShardedAsset<TermRecord[]>(env, request, manifest, "terms_content");
      const examQuestions = await loadShardedAsset<ExamQuestionsPayload>(env, request, manifest, "exam_questions");
      const textbookExamples = await loadShardedAsset<Record<string, TextbookRef[]>>(
        env,
        request,
        manifest,
        "textbook_examples"
      );
      const dictLinks = await loadShardedAsset<Record<string, { revised_sense_links: DictLink[]; idiom_links: DictLink[] }>>(
        env,
        request,
        manifest,
        "dict_links"
      );
      const termMap = new Map<string, TermRecord>();
      [...termsFunction, ...termsContent].forEach((term) => termMap.set(term.term_id, term));
      return {
        manifest,
        termsFunction,
        termsContent,
        examQuestions,
        textbookExamples,
        dictLinks,
        termMap,
      };
    })();
  }
  return runtimeCache;
}

async function fetchRuntimeAsset<T>(env: Env, request: Request, fileName: string): Promise<T> {
  const assetUrl = new URL(`/runtime/${fileName}`, request.url);
  const response = await env.ASSETS.fetch(new Request(assetUrl.toString(), { method: "GET" }));
  if (!response.ok) {
    throw new Error(`Runtime asset missing: ${fileName} (${response.status})`);
  }
  return (await response.json()) as T;
}

async function loadShardedAsset<T>(env: Env, request: Request, manifest: RuntimeManifest, assetKey: string): Promise<T> {
  const asset = manifest.assets[assetKey];
  if (!asset) {
    throw new Error(`Manifest missing asset ${assetKey}`);
  }
  const payloads: unknown[] = [];
  for (const shard of asset.shards) {
    payloads.push(await fetchRuntimeAsset<unknown>(env, request, shard.file_name));
  }
  if (asset.kind === "list") {
    return payloads.flat() as T;
  }
  return Object.assign({}, ...payloads) as T;
}

function selectBankItem(
  runtime: RuntimeData,
  sessionId: string,
  runRow: Record<string, unknown>,
  kind: Kind,
  mode: Mode,
  reviewItem: Record<string, unknown> | null
): BankItem | null {
  const bank = runtime.examQuestions.challenge_bank;
  const answeredCount = Number(runRow.answered_count || 0);
  if (reviewItem) {
    const reviewTermId = String(reviewItem.term_id || "");
    const reviewQuestionType = String(reviewItem.question_type || "") as QuestionType;
    const candidates = (bank[reviewQuestionType] || []).filter((item) =>
      (item.term_ids && item.term_ids.includes(reviewTermId)) || item.term_id === reviewTermId
    );
    if (candidates.length) {
      return candidates[answeredCount % candidates.length] || null;
    }
  }

  if (kind === "function_word") {
    const items = bank.xuci_pair_compare || [];
    if (!items.length) return null;
    const offset = stableNumber(`${sessionId}:${String(runRow.id)}`, items.length);
    return items[(offset + answeredCount) % items.length] || null;
  }

  const questionTypes: QuestionType[] =
    mode === "warmup" ? ["content_gloss", "translation_keypoint"] : QUESTION_TYPE_ORDER_CONTENT;
  const start = answeredCount % questionTypes.length;
  for (let step = 0; step < questionTypes.length; step += 1) {
    const qType = questionTypes[(start + step) % questionTypes.length];
    const sourceItems = bank[qType] || [];
    const items =
      mode === "warmup"
        ? sourceItems.filter((item) => String(item.paper || "").includes("北京"))
        : sourceItems;
    if (!items.length) continue;
    const offset = stableNumber(`${sessionId}:${String(runRow.id)}:${qType}`, items.length);
    return items[(offset + answeredCount) % items.length] || null;
  }
  return null;
}

function buildSenseKey(item: BankItem): string {
  if (item.question_type === "analysis_short") return `analysis:${item.challenge_id}`;
  if (item.answer.label) {
    const correct = item.options.find((option) => option.label === item.answer.label);
    return `${item.question_type}:${cleanString(correct?.text || correct?.headword || item.challenge_id, 80)}`;
  }
  return `${item.question_type}:${item.challenge_id}`;
}

function evaluateAnswer(
  responseMode: "single_select" | "multi_select",
  answer: BankAnswer,
  submitted: ChallengeAnswerPayload
): boolean {
  if (responseMode === "multi_select") {
    const expected = [...(answer.keys || [])].sort();
    const actual = [...(submitted.keys || [])].sort();
    return JSON.stringify(expected) === JSON.stringify(actual);
  }
  return Boolean(answer.label && submitted.label && answer.label === submitted.label);
}

function resolveRelevantTermIds(
  prompt: ChallengePromptPayload,
  answer: BankAnswer,
  submitted: ChallengeAnswerPayload
): string[] {
  if (prompt.questionType === "xuci_pair_compare") {
    const correctOption = prompt.options.find((option) => option.label === answer.label);
    const selectedOption = prompt.options.find((option) => option.label === submitted.label);
    return uniqueStrings([
      correctOption?.term_id || "",
      selectedOption?.term_id || "",
      ...prompt.termIds,
    ]);
  }
  return uniqueStrings(prompt.termIds);
}

function scoreForAnswer(questionType: QuestionType, correct: boolean): number {
  if (!correct) return -4;
  if (questionType === "analysis_short") return 16;
  if (questionType === "xuci_pair_compare") return 14;
  return 12;
}

async function updateRunAfterAnswer(env: Env, runId: string, scoreDelta: number, correct: boolean): Promise<void> {
  const run = await env.DB.prepare(
    `SELECT correct_count, answered_count, streak, max_streak, score FROM challenge_runs WHERE id = ?`
  )
    .bind(runId)
    .first<Record<string, unknown>>();
  if (!run) return;
  const answeredCount = Number(run.answered_count || 0) + 1;
  const correctCount = Number(run.correct_count || 0) + (correct ? 1 : 0);
  const streak = correct ? Number(run.streak || 0) + 1 : 0;
  const maxStreak = Math.max(Number(run.max_streak || 0), streak);
  const score = Number(run.score || 0) + scoreDelta;
  await env.DB.prepare(
    `UPDATE challenge_runs
     SET correct_count = ?, answered_count = ?, streak = ?, max_streak = ?, score = ?, updated_at = CURRENT_TIMESTAMP
     WHERE id = ?`
  )
    .bind(correctCount, answeredCount, streak, maxStreak, score, runId)
    .run();
}

async function updateMasteryRows(
  env: Env,
  sessionId: string,
  termIds: string[],
  senseKey: string,
  questionType: string,
  correct: boolean
): Promise<void> {
  const nowIso = new Date().toISOString();
  for (const termId of uniqueStrings(termIds)) {
    const existing = await env.DB.prepare(
      `SELECT mastery_score, stability_score, attempts, correct_attempts, best_streak, consecutive_correct, correct_after_delay, decay_factor, next_review_at
       FROM user_term_mastery WHERE session_id = ? AND term_id = ? AND sense_key = ? AND question_type = ?`
    )
      .bind(sessionId, termId, senseKey, questionType)
      .first<Record<string, unknown>>();
    const attempts = Number(existing?.attempts || 0) + 1;
    const previousConsecutive = Number(existing?.consecutive_correct || 0);
    const consecutiveCorrect = correct ? previousConsecutive + 1 : 0;
    const correctAttempts = Number(existing?.correct_attempts || 0) + (correct ? 1 : 0);
    const delayedCorrect =
      correct &&
      typeof existing?.next_review_at === "string" &&
      Date.parse(existing.next_review_at) < Date.now()
        ? 1
        : 0;
    const correctAfterDelay = Number(existing?.correct_after_delay || 0) + delayedCorrect;
    const baseMastery = Number(existing?.mastery_score || 0);
    const baseStability = Number(existing?.stability_score || 0);
    const decayFactor = correct
      ? Math.max(0.7, Number(existing?.decay_factor || 1) * 0.94)
      : Math.min(1.45, Number(existing?.decay_factor || 1) * 1.12);
    const masteryScore = clampNumber(baseMastery + (correct ? 0.2 : -0.12) + delayedCorrect * 0.1, 0, 1.4);
    const stabilityScore = clampNumber(baseStability + (correct ? 0.16 : -0.1) + delayedCorrect * 0.12, 0, 1.2);
    const nextReviewAt = new Date(Date.now() + computeReviewDelayMs(consecutiveCorrect, stabilityScore, correct)).toISOString();
    if (existing) {
      await env.DB.prepare(
        `UPDATE user_term_mastery
         SET mastery_score = ?, stability_score = ?, attempts = ?, correct_attempts = ?, best_streak = ?, consecutive_correct = ?, correct_after_delay = ?, decay_factor = ?, last_result = ?, last_seen_at = ?, next_review_at = ?, updated_at = CURRENT_TIMESTAMP
         WHERE session_id = ? AND term_id = ? AND sense_key = ? AND question_type = ?`
      )
        .bind(
          masteryScore,
          stabilityScore,
          attempts,
          correctAttempts,
          Math.max(Number(existing.best_streak || 0), consecutiveCorrect),
          consecutiveCorrect,
          correctAfterDelay,
          decayFactor,
          correct ? "correct" : "wrong",
          nowIso,
          nextReviewAt,
          sessionId,
          termId,
          senseKey,
          questionType
        )
        .run();
    } else {
      await env.DB.prepare(
        `INSERT INTO user_term_mastery
         (session_id, term_id, sense_key, question_type, mastery_score, stability_score, attempts, correct_attempts, best_streak, consecutive_correct, correct_after_delay, decay_factor, last_result, last_seen_at, next_review_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
      )
        .bind(
          sessionId,
          termId,
          senseKey,
          questionType,
          masteryScore,
          stabilityScore,
          attempts,
          correctAttempts,
          consecutiveCorrect,
          consecutiveCorrect,
          correctAfterDelay,
          decayFactor,
          correct ? "correct" : "wrong",
          nowIso,
          nextReviewAt
        )
        .run();
    }
  }
}

function computeReviewDelayMs(consecutiveCorrect: number, stabilityScore: number, correct: boolean): number {
  if (!correct) return 8 * 60 * 1000;
  if (consecutiveCorrect <= 1) return 20 * 60 * 1000;
  if (consecutiveCorrect === 2) return 6 * 60 * 60 * 1000;
  if (stabilityScore > 0.7) return 2 * 24 * 60 * 60 * 1000;
  return 24 * 60 * 60 * 1000;
}

async function updateReviewQueue(
  env: Env,
  sessionId: string,
  termIds: string[],
  senseKey: string,
  questionType: QuestionType,
  correct: boolean,
  sourceItemId: string
): Promise<void> {
  const uniqueTermIds = uniqueStrings(termIds);
  if (correct) {
    for (const termId of uniqueTermIds) {
      await env.DB.prepare(
        `UPDATE review_queue
         SET status = 'done', updated_at = CURRENT_TIMESTAMP
         WHERE session_id = ? AND term_id = ? AND sense_key = ? AND question_type = ? AND status = 'pending'`
      )
        .bind(sessionId, termId, senseKey, questionType)
        .run();
    }
    return;
  }

  for (const termId of uniqueTermIds) {
    await env.DB.prepare(
      `INSERT INTO review_queue
       (id, session_id, term_id, sense_key, question_type, priority, due_at, source_item_id, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
    )
      .bind(
        crypto.randomUUID(),
        sessionId,
        termId,
        senseKey,
        questionType,
        10,
        new Date(Date.now() + 2 * 60 * 1000).toISOString(),
        sourceItemId
      )
      .run();
  }
}

async function claimReviewTarget(env: Env, sessionId: string): Promise<Record<string, unknown> | null> {
  return (
    (await env.DB.prepare(
      `SELECT id, term_id, sense_key, question_type
       FROM review_queue
       WHERE session_id = ? AND status = 'pending' AND due_at <= ?
       ORDER BY priority DESC, created_at ASC
       LIMIT 1`
    )
      .bind(sessionId, new Date().toISOString())
      .first<Record<string, unknown>>()) || null
  );
}

async function countPendingReview(env: Env, sessionId: string): Promise<number> {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS c FROM review_queue WHERE session_id = ? AND status = 'pending'`
  )
    .bind(sessionId)
    .first<Record<string, unknown>>();
  return Number(row?.c || 0);
}

async function listMasteredTerms(env: Env, sessionId: string): Promise<string[]> {
  const { results } = await env.DB.prepare(
    `SELECT DISTINCT term_id FROM user_term_mastery
     WHERE session_id = ? AND mastery_score >= 1 AND stability_score >= 0.65 AND correct_after_delay >= 1 AND consecutive_correct >= 2
     ORDER BY updated_at DESC`
  )
    .bind(sessionId)
    .all<Record<string, unknown>>();
  return (results || []).map((row) => String(row.term_id || ""));
}

async function unlockBadges(
  env: Env,
  sessionId: string,
  run: Record<string, unknown>,
  pendingReviewCount: number
): Promise<Array<{ key: string; title: string; detail: string }>> {
  const conditions = new Set<string>();
  if (Number(run.correct_count || 0) >= 1) conditions.add("first-correct");
  if (Number(run.max_streak || 0) >= 3) conditions.add("streak-3");
  if (pendingReviewCount === 0 && Number(run.answered_count || 0) >= 4) conditions.add("review-tamer");
  if (
    Number(run.answered_count || 0) >= 5 &&
    Number(run.correct_count || 0) === Number(run.answered_count || 0)
  ) {
    conditions.add("perfect-run");
  }

  const unlocked: Array<{ key: string; title: string; detail: string }> = [];
  for (const badge of BADGE_DEFS) {
    if (!conditions.has(badge.key)) continue;
    const existing = await env.DB.prepare(
      `SELECT badge_key FROM badge_unlocks WHERE session_id = ? AND badge_key = ?`
    )
      .bind(sessionId, badge.key)
      .first<Record<string, unknown>>();
    if (existing) continue;
    await env.DB.prepare(
      `INSERT INTO badge_unlocks (session_id, badge_key, meta_json) VALUES (?, ?, ?)`
    )
      .bind(sessionId, badge.key, JSON.stringify(badge))
      .run();
    unlocked.push(badge);
  }
  return unlocked;
}

async function getRun(env: Env, runId: string, sessionId: string): Promise<Record<string, unknown> | null> {
  return (
    (await env.DB.prepare(
      `SELECT id, session_id, kind, mode, status, started_at, updated_at, finished_at, score, correct_count, answered_count, streak, max_streak, rating_delta, report_id
       FROM challenge_runs WHERE id = ? AND session_id = ?`
    )
      .bind(runId, sessionId)
      .first<Record<string, unknown>>()) || null
  );
}

async function listRunItems(env: Env, runId: string, sessionId: string): Promise<Array<Record<string, unknown>>> {
  const { results } = await env.DB.prepare(
    `SELECT question_type, term_id, prompt_json, submitted_answer_json, correct, score
     FROM challenge_items
     WHERE run_id = ? AND session_id = ? AND answered_at IS NOT NULL
     ORDER BY answered_at ASC`
  )
    .bind(runId, sessionId)
    .all<Record<string, unknown>>();
  return results || [];
}

function summarizeRun(run: Record<string, unknown>): Record<string, unknown> {
  return {
    id: run.id,
    kind: run.kind,
    mode: run.mode,
    status: run.status,
    score: Number(run.score || 0),
    correctCount: Number(run.correct_count || 0),
    answeredCount: Number(run.answered_count || 0),
    streak: Number(run.streak || 0),
    maxStreak: Number(run.max_streak || 0),
    reportId: run.report_id || null,
  };
}

function collectMistakeTerms(items: Array<Record<string, unknown>>): string[] {
  const mistakes = new Set<string>();
  items.forEach((item) => {
    if (Number(item.correct || 0) === 1) return;
    const prompt = safeParseObject(item.prompt_json) as unknown as ChallengePromptPayload;
    uniqueStrings(prompt.termIds || [prompt.termId]).forEach((termId) => mistakes.add(termId));
  });
  return [...mistakes];
}

function buildReportMarkdown(
  run: Record<string, unknown>,
  items: Array<Record<string, unknown>>,
  payload: Record<string, unknown>
): string {
  const lines = [
    "# 文言实虚词本次报告",
    "",
    `- 挑战类型：${String(run.kind) === "function_word" ? "虚词挑战" : "实词挑战"}`,
    `- 模式：${String(run.mode)}`,
    `- 得分：${Number(run.score || 0)}`,
    `- 正确率：${Math.round(Number(payload.accuracy || 0) * 100)}%`,
    `- 已答题数：${Number(run.answered_count || 0)}`,
    `- 最大连胜：${Number(run.max_streak || 0)}`,
    `- 错题追击队列：${Number(payload.queue_after_run || 0)}`,
    "",
    "## 错题词",
    ...(Array.isArray(payload.mistake_terms) && payload.mistake_terms.length
      ? (payload.mistake_terms as string[]).map((item) => `- ${item}`)
      : ["- 本次暂无错题"]),
    "",
    "## 新掌握词",
    ...(Array.isArray(payload.new_mastered_terms) && payload.new_mastered_terms.length
      ? (payload.new_mastered_terms as string[]).map((item) => `- ${item}`)
      : ["- 本次暂无新增彻底掌握词"]),
    "",
    "## 作答流水",
  ];
  items.forEach((item, index) => {
    const prompt = safeParseObject(item.prompt_json) as unknown as ChallengePromptPayload;
    lines.push(
      `- ${index + 1}. [${Number(item.correct || 0) === 1 ? "对" : "错"}] ${prompt.questionType} / ${prompt.termIds.join(", ")}`
    );
  });
  lines.push("", "## 下载入口", `- Markdown：${String(payload.download_files && (payload.download_files as Record<string, unknown>).markdown || "")}`);
  lines.push(`- JSON：${String(payload.download_files && (payload.download_files as Record<string, unknown>).json || "")}`);
  return lines.join("\n");
}

async function loadLeaderboard(env: Env, scope: "day" | "week" | "all"): Promise<Record<string, unknown>> {
  const scopeKey = leaderboardScopeKey(scope);
  const { results } = await env.DB.prepare(
    `SELECT display_name, score, runs, updated_at
     FROM leaderboard_scores
     WHERE scope = ? AND scope_key = ?
     ORDER BY score DESC, updated_at ASC
     LIMIT 20`
  )
    .bind(scope, scopeKey)
    .all<Record<string, unknown>>();
  return {
    scope,
    scopeKey,
    entries: (results || []).map((row, index) => ({
      rank: index + 1,
      displayName: row.display_name,
      score: Number(row.score || 0),
      runs: Number(row.runs || 0),
      updatedAt: row.updated_at,
    })),
  };
}

async function updateLeaderboard(
  env: Env,
  sessionId: string,
  authSubject: string,
  displayName: string,
  scoreDelta: number
): Promise<void> {
  const scopes: Array<"day" | "week" | "all"> = ["day", "week", "all"];
  for (const scope of scopes) {
    const scopeKey = leaderboardScopeKey(scope);
    await env.DB.prepare(
      `INSERT INTO leaderboard_scores (session_id, scope, scope_key, display_name, score, runs, updated_at)
       VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
       ON CONFLICT(session_id, scope, scope_key)
       DO UPDATE SET
         display_name = excluded.display_name,
         score = leaderboard_scores.score + excluded.score,
         runs = leaderboard_scores.runs + 1,
         updated_at = CURRENT_TIMESTAMP`
    )
      .bind(sessionId, scope, scopeKey, displayName || authSubject, scoreDelta)
      .run();
  }
}

function leaderboardScopeKey(scope: "day" | "week" | "all"): string {
  if (scope === "all") return "all-time";
  const now = new Date();
  if (scope === "day") return now.toISOString().slice(0, 10);
  const week = isoWeek(now);
  return `${week.year}-W${String(week.week).padStart(2, "0")}`;
}

function isoWeek(date: Date): { year: number; week: number } {
  const target = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNr = (target.getUTCDay() + 6) % 7;
  target.setUTCDate(target.getUTCDate() - dayNr + 3);
  const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
  const diff = target.getTime() - firstThursday.getTime();
  const week = 1 + Math.round(diff / 604800000);
  return { year: target.getUTCFullYear(), week };
}

async function getAuthContext(request: Request, env: Env): Promise<{ user: UserCenterUser; cookieHeader: string } | null> {
  const cookieHeader = request.headers.get("Cookie") || "";
  if (!cookieHeader.includes("bdfz_uc_session=")) return null;
  const response = await fetch(env.USER_CENTER_SESSION_ENDPOINT, {
    method: "GET",
    headers: { Cookie: cookieHeader },
  });
  if (!response.ok) return null;
  const payload = (await response.json().catch(() => null)) as UserCenterSession | null;
  if (!payload?.authenticated || !payload.user?.slug) return null;
  return {
    user: payload.user,
    cookieHeader,
  };
}

async function queueSyncEvent(
  env: Env,
  sessionId: string,
  authSubject: string,
  eventType: string,
  payload: Record<string, unknown>
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO sync_outbox (id, session_id, auth_subject, event_type, payload_json, status, retry_count, updated_at)
     VALUES (?, ?, ?, ?, ?, 'pending', 0, CURRENT_TIMESTAMP)`
  )
    .bind(crypto.randomUUID(), sessionId, authSubject, eventType, JSON.stringify(payload))
    .run();
}

async function flushSyncOutbox(env: Env, sessionId: string, authSubject: string, cookieHeader: string): Promise<void> {
  const { results } = await env.DB.prepare(
    `SELECT id, event_type, payload_json, retry_count
     FROM sync_outbox
     WHERE session_id = ? AND auth_subject = ? AND status = 'pending'
     ORDER BY created_at ASC
     LIMIT 30`
  )
    .bind(sessionId, authSubject)
    .all<Record<string, unknown>>();
  for (const row of results || []) {
    try {
      const eventType = String(row.event_type || "");
      const payload = safeParseObject(row.payload_json);
      let endpoint = "";
      let method = "POST";
      if (eventType === "progress_sync") {
        endpoint = `${env.USER_CENTER_ORIGIN}/api/progress`;
        method = "PUT";
      } else if (eventType === "download_record") {
        endpoint = `${env.USER_CENTER_ORIGIN}/api/data-records`;
        method = "PUT";
      } else if (eventType === "mastery_report") {
        endpoint = env.REPORT_SYNC_ENDPOINT;
        method = "POST";
      } else {
        throw new Error(`Unknown sync event ${eventType}`);
      }
      const response = await fetch(endpoint, {
        method,
        headers: {
          "Content-Type": "application/json",
          Cookie: cookieHeader,
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        throw new Error(`sync failed ${response.status}`);
      }
      await env.DB.prepare(
        `UPDATE sync_outbox SET status = 'done', updated_at = CURRENT_TIMESTAMP WHERE id = ?`
      )
        .bind(String(row.id))
        .run();
    } catch (error) {
      await env.DB.prepare(
        `UPDATE sync_outbox
         SET retry_count = retry_count + 1, last_error = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`
      )
        .bind(error instanceof Error ? error.message.slice(0, 500) : String(error), String(row.id))
        .run();
    }
  }
}

function buildProgressSyncPayload(run: Record<string, unknown>, accuracy: number): Record<string, unknown> {
  const kind = String(run.kind || "");
  const itemTitle = `${kind === "function_word" ? "虚词" : "实词"}挑战`;
  return {
    siteKey: "wy",
    itemKey: `run:${String(run.id)}`,
    itemTitle,
    itemGroup: kind,
    itemType: "challenge_run",
    state: "done",
    completed: true,
    score: Math.round(accuracy * 100),
    meta: {
      progressPercent: Math.round(accuracy * 100),
      answeredCount: Number(run.answered_count || 0),
      correctCount: Number(run.correct_count || 0),
      runMode: String(run.mode || ""),
    },
  };
}

function buildMasterySyncPayload(
  run: Record<string, unknown>,
  accuracy: number,
  reportId: string
): Record<string, unknown> {
  const kind = String(run.kind || "");
  return {
    siteKey: "wy",
    itemKey: `run:${String(run.id)}`,
    itemTitle: `${kind === "function_word" ? "虚词" : "实词"}挑战`,
    scorePercent: Math.round(accuracy * 100),
    isPerfect: accuracy === 1,
    challengeType: "gaokao",
    challengeIdentifier: reportId,
  };
}

function buildDownloadSyncPayload(reportId: string, format: "markdown" | "json"): Record<string, unknown> {
  return {
    siteKey: "wy",
    recordKind: "download",
    recordKey: `report:${reportId}:${format}`,
    sessionKey: `report:${reportId}`,
    title: `本次报告 ${format === "markdown" ? "Markdown" : "JSON"}`,
    summary: `report-${reportId}.${format === "markdown" ? "md" : "json"}`,
    itemGroup: "download",
    itemType: "report",
    contentFormat: "download-meta-v1",
    sourceUrl: `/api/report/${reportId}?format=${format}`,
    payload: {
      fileName: `report-${reportId}.${format === "markdown" ? "md" : "json"}`,
      fileUrl: `/api/report/${reportId}?format=${format}`,
      format,
      fileType: "report",
      sourceSessionKey: `report:${reportId}`,
    },
  };
}

async function ensureAnonSession(request: Request, env: Env): Promise<SessionState> {
  const cookies = parseCookies(request.headers.get("Cookie") || "");
  const host = new URL(request.url).hostname;
  const token = cookies[SESSION_COOKIE];
  const payload = token ? await verifyToken(env, request, token) : null;
  if (payload && payload.type === "session" && typeof payload.sid === "string" && typeof payload.alias === "string") {
    await env.DB.prepare(
      `INSERT INTO anon_sessions (id, alias, display_name, created_at, last_seen_at)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
       ON CONFLICT(id) DO UPDATE SET alias = excluded.alias, last_seen_at = CURRENT_TIMESTAMP`
    )
      .bind(payload.sid, payload.alias, payload.alias)
      .run();
    return {
      id: payload.sid,
      alias: payload.alias,
      displayName: payload.alias,
      token,
      isNew: false,
    };
  }

  const id = crypto.randomUUID();
  const alias = buildAlias(id);
  const newToken = await signToken(env, request, {
    type: "session",
    sid: id,
    alias,
    host,
    exp: Math.floor(Date.now() / 1000) + 180 * 24 * 60 * 60,
  });
  await env.DB.prepare(
    `INSERT INTO anon_sessions (id, alias, display_name, created_at, last_seen_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
  )
    .bind(id, alias, alias)
    .run();
  return {
    id,
    alias,
    displayName: alias,
    token: newToken,
    isNew: true,
  };
}

function buildAlias(seed: string): string {
  const left = ["青砚", "寒灯", "竹简", "云阶", "澄怀", "墨溪", "松风", "兰舟"];
  const right = ["行者", "应考生", "读书人", "挑灯客", "追义者", "临卷人", "破阵师", "守拙生"];
  return `${left[stableNumber(seed, left.length)]}${right[stableNumber(`${seed}:r`, right.length)]}`;
}

function buildSessionCookie(env: Env, request: Request, token: string): string {
  const hostname = new URL(request.url).hostname.toLowerCase();
  const hostHeader = (request.headers.get("Host") || "").split(":")[0].toLowerCase();
  const effectiveHost = hostHeader || hostname;
  const isLocalHost =
    env.LOCAL_DEV === "1" || effectiveHost === "127.0.0.1" || effectiveHost === "localhost";
  const segments = [
    `${SESSION_COOKIE}=${token}`,
    "Path=/",
    "HttpOnly",
    `Max-Age=${180 * 24 * 60 * 60}`,
  ];
  if (isLocalHost) {
    segments.splice(3, 0, "SameSite=Lax");
  } else {
    segments.splice(3, 0, "Secure", "SameSite=None");
    segments.splice(2, 0, `Domain=.${env.PRIMARY_HOST.split(".").slice(-2).join(".")}`);
  }
  return segments.join("; ");
}

async function signToken(env: Env, request: Request, payload: Record<string, unknown>): Promise<string> {
  const secret = getSigningSecret(env, request);
  const body = encoder.encode(JSON.stringify(payload));
  const key = await importHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, body);
  return `${toBase64Url(body)}.${toBase64Url(new Uint8Array(signature))}`;
}

async function verifyToken(
  env: Env,
  request: Request,
  token: string
): Promise<Record<string, unknown> | null> {
  const [bodyPart, sigPart] = token.split(".");
  if (!bodyPart || !sigPart) return null;
  const secret = getSigningSecret(env, request);
  const key = await importHmacKey(secret);
  const bodyBytes = fromBase64Url(bodyPart);
  const sigBytes = fromBase64Url(sigPart);
  const valid = await crypto.subtle.verify("HMAC", key, sigBytes, bodyBytes);
  if (!valid) return null;
  const payload = safeParseObject(decoder.decode(bodyBytes));
  const exp = Number(payload.exp || 0);
  if (exp && exp < Math.floor(Date.now() / 1000)) return null;
  return payload;
}

async function importHmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
}

function getSigningSecret(env: Env, request: Request): string {
  if (env.SIGNING_SECRET) return env.SIGNING_SECRET;
  const url = new URL(request.url);
  const hostname = url.hostname.toLowerCase();
  const hostHeader = (request.headers.get("Host") || "").split(":")[0].toLowerCase();
  const isLocalHost =
    hostname === "127.0.0.1" ||
    hostname === "localhost" ||
    hostHeader === "127.0.0.1" ||
    hostHeader === "localhost" ||
    url.protocol === "http:";
  if (isLocalHost || hostname.endsWith(".workers.dev")) {
    return DEV_SIGNING_SECRET;
  }
  throw new Error("SIGNING_SECRET is required in non-local environments");
}

async function enforceRateLimit(env: Env, sessionId: string, authSubject: string, ipHash: string): Promise<boolean> {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS c
     FROM challenge_items
     WHERE session_id = ? AND answered_at IS NOT NULL AND answered_at >= datetime('now', '-60 seconds')`
  )
    .bind(sessionId)
    .first<Record<string, unknown>>();
  const count = Number(row?.c || 0);
  if (count <= 40) return true;
  await logAbuseEvent(env, sessionId, authSubject, ipHash, "rate_limit", { count });
  return false;
}

async function logAbuseEvent(
  env: Env,
  sessionId: string,
  authSubject: string,
  ipHash: string,
  eventType: string,
  detail: Record<string, unknown>
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO abuse_events (id, session_id, auth_subject, ip_hash, event_type, detail_json)
     VALUES (?, ?, ?, ?, ?, ?)`
  )
    .bind(crypto.randomUUID(), sessionId, authSubject, ipHash, eventType, JSON.stringify(detail))
    .run();
}

function buildRelatedChallengeCounts(runtime: RuntimeData, termId: string): Record<string, number> {
  const counts: Record<string, number> = {};
  (Object.keys(runtime.examQuestions.challenge_bank) as QuestionType[]).forEach((questionType) => {
    counts[questionType] = (runtime.examQuestions.challenge_bank[questionType] || []).filter(
      (item) => item.term_id === termId || item.term_ids?.includes(termId)
    ).length;
  });
  return counts;
}

function pickEncouragement(correct: boolean, questionType: QuestionType): string {
  if (correct) {
    const options = {
      xuci_pair_compare: ["这一组关系抓得很稳。", "意义和用法都落点准确。", "这种北京卷对照题你已经上手了。"],
      default: ["这个义项拿住了。", "这一步判断是对的。", "语境义把得住，后面会更稳。"],
    };
    return stablePickText(
      questionType === "xuci_pair_compare" ? options.xuci_pair_compare : options.default,
      questionType
    );
  }
  const options = {
    xuci_pair_compare: ["这题先别急，下一轮会先追同词同考点。", "这组关系容易混，马上补一题相近干扰。", "先把这两个句子的连接关系分清。"],
    default: ["这题记住语境义，下一题会追同词复练。", "先把词义落稳，再进句意就顺了。", "错得有价值，后面会给你更贴近的追击题。"],
  };
  return stablePickText(
    questionType === "xuci_pair_compare" ? options.xuci_pair_compare : options.default,
    `${questionType}:wrong`
  );
}

function stablePickText(values: string[], seed: string): string {
  return values[stableNumber(seed, values.length)] || values[0] || "";
}

function normalizeSubmittedAnswer(value: unknown): ChallengeAnswerPayload | null {
  if (!value || typeof value !== "object") return null;
  const input = value as Record<string, unknown>;
  const label = cleanString(input.label, 2).toUpperCase();
  const keys = Array.isArray(input.keys)
    ? uniqueStrings(input.keys.map((item) => cleanString(item, 2).toUpperCase()))
    : [];
  if (label) return { label };
  if (keys.length) return { keys };
  return null;
}

function canonicalKind(value: unknown): Kind {
  const raw = cleanString(value, 40).toLowerCase();
  if (raw === "xuci" || raw === "function_word") return "function_word";
  return "content_word";
}

function canonicalMode(value: unknown): Mode {
  const raw = cleanString(value, 40).toLowerCase();
  if (raw === "review") return "review";
  if (raw === "ranked") return "ranked";
  return "warmup";
}

function cleanString(value: unknown, maxLength: number): string {
  return String(value || "").trim().slice(0, maxLength);
}

function parseCookies(cookieHeader: string): Record<string, string> {
  const cookies: Record<string, string> = {};
  cookieHeader.split(";").forEach((part) => {
    const index = part.indexOf("=");
    if (index <= 0) return;
    const key = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();
    if (key) cookies[key] = value;
  });
  return cookies;
}

function safeParseObject(input: unknown): Record<string, unknown> {
  if (!input) return {};
  if (typeof input === "object" && !Array.isArray(input)) return input as Record<string, unknown>;
  if (typeof input !== "string") return {};
  try {
    const parsed = JSON.parse(input) as unknown;
    return typeof parsed === "object" && parsed && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function stableNumber(seed: string, modulo: number): number {
  if (modulo <= 0) return 0;
  const digest = hashSync(seed);
  return parseInt(digest.slice(0, 8), 16) % modulo;
}

function hashSync(value: string): string {
  const bytes = encoder.encode(value);
  let hash = 2166136261;
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash >>> 0).toString(16).padStart(8, "0");
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function toBase64Url(input: Uint8Array | ArrayBuffer): string {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function fromBase64Url(value: string): Uint8Array {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "===".slice((normalized.length + 3) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
