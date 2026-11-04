const THEME_KEY = "wy-theme";
const AUDIT_DEVICE_KEY = "wy-audit-device-id-v1";
const THEMES = ["parchment", "forest", "ink"];
const ROUND_SIZE = 10;
const MIXED_KIND = "textbook_word";
const GAOKAO_KIND = "gaokao_mix";

const state = {
  bootstrap: null,
  theme: loadTheme(),
  kind: MIXED_KIND,
  articleId: "",
  activeArticle: null,
  mode: "ladder",
  sourceRunId: "",
  run: null,
  currentItem: null,
  answered: false,
  submitting: false,
  answerReveal: null,
  answerCard: null,
  roundMarks: [],
  roundComplete: null,
  player: null,
  leaderboard: null,
  leaderboardScope: "total",
  challengeRequestSeq: 0,
  feedbackSubmitting: false,
};

const els = {
  arenaGrid: document.querySelector("#arena-grid"),
  questionRoot: document.querySelector("#question-root"),
  roundTrack: document.querySelector("#round-track"),
  battleStage: document.querySelector(".battle-stage"),
  modePanel: document.querySelector("#mode-panel"),
  articlePanel: document.querySelector("#article-panel"),
  articleCatalog: document.querySelector("#article-catalog"),
  leaderboardRoot: document.querySelector("#leaderboard-root"),
  leaderboardMeta: document.querySelector("#leaderboard-meta"),
  accountSummary: document.querySelector("#account-summary"),
  rankBadge: document.querySelector("#rank-badge"),
  rankName: document.querySelector("#rank-name"),
  rankMode: document.querySelector("#rank-mode"),
  roundFraction: document.querySelector("#round-fraction"),
  roundGoal: document.querySelector("#round-goal"),
  themeSelect: document.querySelector("#theme-select"),
  feedbackModal: document.querySelector("#feedback-modal"),
  feedbackForm: document.querySelector("#feedback-form"),
  feedbackPromptMeta: document.querySelector("#feedback-prompt-meta"),
  feedbackAnonymous: document.querySelector("#feedback-anonymous"),
  feedbackName: document.querySelector("#feedback-name"),
  feedbackMessage: document.querySelector("#feedback-message"),
  feedbackScreenshot: document.querySelector("#feedback-screenshot"),
  feedbackStatus: document.querySelector("#feedback-status"),
  feedbackClose: document.querySelector("#feedback-close"),
  feedbackCancel: document.querySelector("#feedback-cancel"),
  feedbackSubmit: document.querySelector("#feedback-submit"),
  leaderboardTabs: Array.from(document.querySelectorAll(".leaderboard-tab")),
};

els.leaderboardTabs.forEach((button) => {
  button.addEventListener("click", () => {
    state.leaderboardScope = button.dataset.scope || "total";
    renderLeaderboard();
  });
});

els.themeSelect?.addEventListener("change", (event) => {
  const value = event.target instanceof HTMLSelectElement ? event.target.value : "parchment";
  setTheme(value);
});

applyTheme(state.theme);
applyInitialViewMode();

boot().catch((error) => {
  console.error(error);
  renderError(error.message || String(error));
});

async function boot() {
  if (window.BdfzIdentity?.ensureWidget) {
    window.BdfzIdentity.ensureWidget({
      siteKey: "wy",
      insetBottom: 20,
      mobileInsetBottom: 84,
      mobileInsetSide: 12,
    });
  }
  initFeedbackModal();
  await refreshBootstrap();
  const requested = initialRoute();
  if (requested.articleId) {
    await startChallenge(MIXED_KIND, requested.articleId, "ladder", "", { focus: "question", scroll: false });
    return;
  }
  await chooseTrack(MIXED_KIND, { scroll: false });
}

function initialRoute() {
  const params = new URLSearchParams(window.location.search);
  const kind = params.get("kind") || "";
  const articleId = params.get("articleId") || params.get("article_id") || "";
  if (kind === MIXED_KIND && articleId) {
    return { kind, articleId };
  }
  return { kind: "", articleId: "" };
}

function applyInitialViewMode() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("embed") === "1") {
    document.body.classList.add("embed-mode");
  }
}

async function refreshBootstrap() {
  state.bootstrap = await api("/api/bootstrap");
  state.leaderboard = state.bootstrap?.leaderboard || state.leaderboard;
  state.player = currentPlayerState();
  state.activeArticle = currentArticle();
}

function resetRoundState() {
  state.run = null;
  state.currentItem = null;
  state.answered = false;
  state.submitting = false;
  state.answerReveal = null;
  state.answerCard = null;
  state.roundMarks = [];
  state.roundComplete = null;
}

async function chooseTrack(kind, viewOptions = {}) {
  const requestSeq = ++state.challengeRequestSeq;
  state.kind = kind;
  state.articleId = "";
  state.activeArticle = null;
  state.mode = "ladder";
  state.sourceRunId = "";
  resetRoundState();
  state.player = currentPlayerState(kind);
  renderShell();
  revealInteractionTarget(kind === MIXED_KIND ? "articles" : "question", viewOptions);
  try {
    await loadLeaderboard(kind, requestSeq);
  } catch (error) {
    if (requestSeq !== state.challengeRequestSeq) return;
    console.error(error);
    renderError(error.message || String(error));
  }
}

async function startChallenge(kind = state.kind, articleId = "", mode = "ladder", sourceRunId = "", viewOptions = {}) {
  const requestSeq = ++state.challengeRequestSeq;
  state.kind = kind;
  state.articleId = kind === MIXED_KIND ? articleId : "";
  state.activeArticle = kind === MIXED_KIND ? currentArticle(articleId) : null;
  state.mode = mode;
  state.sourceRunId = sourceRunId;
  resetRoundState();
  state.player = currentPlayerState(kind);
  renderShell();
  try {
    await loadLeaderboard(kind, requestSeq);
    if (kind === MIXED_KIND && !state.articleId && mode !== "review") {
      revealInteractionTarget("articles", viewOptions);
      return;
    }
    await loadNextQuestion(requestSeq, {
      focus: viewOptions.focus || "question",
      scroll: viewOptions.scroll,
    });
  } catch (error) {
    if (requestSeq !== state.challengeRequestSeq) return;
    if (kind === MIXED_KIND && articleId !== state.articleId) return;
    console.error(error);
    renderError(error.message || String(error));
  }
}

async function loadLeaderboard(kind, requestSeq = state.challengeRequestSeq) {
  const payload = await api(`/api/leaderboard?kind=${encodeURIComponent(kind || state.kind || GAOKAO_KIND)}&limit=10`);
  if (requestSeq !== state.challengeRequestSeq) return;
  state.leaderboard = payload;
  renderLeaderboard();
}

async function loadNextQuestion(requestSeq = state.challengeRequestSeq, viewOptions = {}) {
  if (state.kind === MIXED_KIND && !state.articleId && state.mode !== "review") {
    renderShell();
    revealInteractionTarget("articles", viewOptions);
    return;
  }
  const requestedKind = state.kind || GAOKAO_KIND;
  const requestedArticleId = state.articleId;
  const search = new URLSearchParams({ kind: requestedKind });
  search.set("mode", state.mode || "ladder");
  if (requestedArticleId && state.mode !== "review") {
    search.set("articleId", requestedArticleId);
  }
  if (state.run?.id && !state.roundComplete) {
    search.set("runId", state.run.id);
  }
  if (!state.run?.id && state.sourceRunId) {
    search.set("sourceRunId", state.sourceRunId);
  }
  const payload = await api(`/api/challenge/next?${search.toString()}`);
  if (requestSeq !== state.challengeRequestSeq) return;
  if (requestedKind === MIXED_KIND && requestedArticleId !== state.articleId) return;
  state.run = payload.run || null;
  state.mode = payload.run?.mode || state.mode || "ladder";
  state.currentItem = payload.item || null;
  state.answered = false;
  state.submitting = false;
  state.answerReveal = null;
  state.answerCard = null;
  state.roundComplete = payload.roundCompleted ? payload.round || null : null;
  state.player = payload.player || currentPlayerState();
  state.activeArticle = currentArticle(requestedArticleId || payload.run?.targetKey || payload.item?.prompt?.articleId || "");
  if (state.roundComplete && !state.currentItem) {
    await refreshBootstrap();
    if (requestSeq !== state.challengeRequestSeq) return;
    if (requestedKind === MIXED_KIND && requestedArticleId !== state.articleId) return;
    state.player = currentPlayerState();
    await loadLeaderboard(requestedKind, requestSeq);
  }
  renderShell();
  revealInteractionTarget(viewOptions.focus || "question", viewOptions);
}

function renderShell() {
  renderStatus();
  renderModeSelector();
  renderArticleCatalog();
  renderQuestion();
  renderRoundTrack();
  renderLeaderboard();
}

function renderStatus() {
  const player = state.player || currentPlayerState();
  els.rankName.textContent = player?.tierName || "工蜂";
  els.rankMode.textContent = player?.modeName || "常規排位";
  const answeredCount = Number(state.run?.answeredCount || state.roundMarks.length || 0);
  const targetCount = Number(state.run?.targetCount || state.roundComplete?.targetCount || ROUND_SIZE);
  const correctCount = Number(state.run?.correctCount || countTrue(state.roundMarks) || 0);
  els.roundFraction.textContent = `${answeredCount} / ${targetCount}`;
  els.roundGoal.textContent = `${correctCount} / ${targetCount}`;
  els.rankBadge.dataset.kind = state.kind || player?.kind || "";
  const summary = buildAccountSummary(player);
  if (els.accountSummary) {
    els.accountSummary.textContent = summary;
    els.accountSummary.hidden = !summary;
  }
}

function renderModeSelector() {
  if (!els.modePanel) return;
  const gaokaoActive = state.kind === GAOKAO_KIND;
  const articleActive = state.kind === MIXED_KIND;
  els.modePanel.innerHTML = `
    <div class="mode-switch" role="tablist" aria-label="挑戰模式">
      <button class="mode-switch-btn ${gaokaoActive ? "active" : ""}" type="button" data-track="${GAOKAO_KIND}">
        高考混編
      </button>
      <button class="mode-switch-btn ${articleActive ? "active" : ""}" type="button" data-track="${MIXED_KIND}">
        選篇掌握
      </button>
    </div>
  `;
  els.modePanel.querySelector(`[data-track="${GAOKAO_KIND}"]`)?.addEventListener("click", () => {
    void startChallenge(GAOKAO_KIND, "", "ladder", "", { focus: "question" });
  });
  els.modePanel.querySelector(`[data-track="${MIXED_KIND}"]`)?.addEventListener("click", () => {
    void chooseTrack(MIXED_KIND, { focus: "articles" });
  });
}

function renderArticleCatalog() {
  if (els.articlePanel) {
    els.articlePanel.hidden = state.kind !== MIXED_KIND;
  }
  if (els.arenaGrid) {
    els.arenaGrid.classList.toggle("no-left", state.kind !== MIXED_KIND);
  }
  if (!els.articleCatalog) return;
  if (state.kind !== MIXED_KIND) {
    els.articleCatalog.innerHTML = "";
    return;
  }
  const articles = Array.isArray(state.bootstrap?.articles) ? state.bootstrap.articles : [];
  if (!articles.length) {
    els.articleCatalog.innerHTML = "";
    return;
  }
  const grouped = new Map();
  articles.forEach((article) => {
    const key = article.book_title || article.book_key || "教材";
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(article);
  });
  els.articleCatalog.innerHTML = [...grouped.entries()]
    .map(([groupTitle, items]) => {
      return `
        <section class="article-group">
          <h2 class="article-group-title">${escapeHtml(groupTitle)}</h2>
          <div class="article-grid">
            ${items
              .map((article) => {
                const active = article.article_id === state.articleId;
                return `
                  <button
                    class="article-entry ${active ? "active" : ""}"
                    type="button"
                    data-article-id="${escapeAttr(article.article_id || "")}"
                  >
                    <span class="article-entry-line">
                      <strong>${escapeHtml(article.title || "")}</strong>
                      <span class="article-entry-count">${Number(article.challenge_count || 0)}</span>
                    </span>
                  </button>
                `;
              })
              .join("")}
          </div>
        </section>
      `;
    })
    .join("");
  els.articleCatalog.querySelectorAll("[data-article-id]").forEach((button) => {
    button.addEventListener("click", () => {
      void startChallenge(MIXED_KIND, button.dataset.articleId || "", "ladder", "", { focus: "question" });
    });
  });
}

function renderQuestion() {
  if (state.kind === MIXED_KIND && !state.articleId && state.mode !== "review") {
    els.questionRoot.innerHTML = `<div class="empty-state empty-state-quiet"></div>`;
    return;
  }

  if (!state.currentItem?.prompt && !state.run && !state.roundComplete) {
    els.questionRoot.innerHTML = `<div class="empty-state empty-state-quiet"></div>`;
    return;
  }

  if (!state.currentItem?.prompt) {
    renderRoundCompletion();
    return;
  }

  const prompt = state.currentItem.prompt;
  const headword = termHeadword(prompt.termId);
  const visibleContext = buildVisibleContextForPrompt(prompt, headword);
  const focusTexts = collectFocusTexts(prompt);
  const parts = [
    `<article class="question-card">`,
    prompt.sourceLabel ? `<div class="question-source">${escapeHtml(prompt.sourceLabel)}</div>` : "",
    `<h2 class="question-stem">${escapeHtml(prompt.stem)}</h2>`,
  ];

  if (visibleContext.length) {
    parts.push(
      `<div class="question-context context-preview" title="${escapeAttr(contextTooltip(prompt.contextWindow || [], prompt.sentence || prompt.passage || "", headword))}">
        ${visibleContext
          .map((line) => {
            const focus = focusTexts.some((focusText) => isFocusContextLine(line, focusText, headword)) ||
              isFocusContextLine(line, prompt.sentence || prompt.passage || "", headword);
            const headwords = collectHighlightHeadwords(prompt, headword);
            return `<p class="context-line ${focus ? "is-focus" : ""}">${highlightTerms(line, headwords)}</p>`;
          })
          .join("")}
      </div>`
    );
  } else if (prompt.sentence) {
    parts.push(
      `<div class="question-context" title="${escapeAttr(contextTooltip(prompt.contextWindow || [], prompt.sentence || "", headword))}">${highlightTerm(prompt.sentence, headword)}</div>`
    );
  }
  if (prompt.passage) {
    parts.push(
      `<div class="question-passage" title="${escapeAttr(contextTooltip(prompt.contextWindow || [], prompt.passage || "", headword))}">${highlightTerm(prompt.passage, headword)}</div>`
    );
  }

  parts.push(`<div class="option-list">`);
  prompt.options.forEach((option) => {
    const key = option.label || option.key || "";
    const visual = optionState(prompt, option, key);
    const classes = [
      "option-card",
      visual.selected ? "selected" : "",
      visual.correct ? "correct" : "",
      visual.incorrect ? "incorrect" : "",
      visual.dimmed ? "dimmed" : "",
    ]
      .filter(Boolean)
      .join(" ");

    parts.push(`
      <button class="${classes}" type="button" data-option="${escapeAttr(key)}" title="${escapeAttr(optionTooltip(prompt, option, headword))}" ${state.answered ? "disabled" : ""}>
        <div class="option-line">
          <span class="option-tag">${escapeHtml(key)}</span>
          <span class="option-line-text">${renderOptionInlineText(prompt, option, headword)}</span>
        </div>
      </button>
    `);
  });
  parts.push(`</div>`);

  if (state.answered && state.answerCard) {
    parts.push(renderAnalysisCard());
  }

  if (state.roundComplete) {
    parts.push(renderCompletionCard());
  }

  parts.push(`</article>`);
  els.questionRoot.innerHTML = parts.join("");
  els.questionRoot.querySelectorAll("[data-option]").forEach((button) => {
    button.addEventListener("click", () => submitAnswer(button.dataset.option || ""));
  });
  els.questionRoot.querySelector("#continue-btn")?.addEventListener("click", () => {
    void loadNextQuestion(undefined, { focus: "question" });
  });
  els.questionRoot.querySelector("#feedback-btn")?.addEventListener("click", () => {
    openFeedbackModal();
  });
  els.questionRoot.querySelector("#redo-mistakes-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, state.articleId, "review", state.run?.id || "");
  });
  els.questionRoot.querySelector("#next-round-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, state.articleId || state.activeArticle?.article_id || "", "ladder");
  });
}

function renderAnalysisCard() {
  const answerKey = state.answerCard;
  if (!answerKey) return "";
  const summaryTone = state.roundMarks[state.roundMarks.length - 1] ? "good" : "bad";
  const analyses = Array.isArray(answerKey.option_analyses) ? answerKey.option_analyses : [];
  const supports = [
    ...(Array.isArray(answerKey.dict_support) ? answerKey.dict_support.slice(0, 1).map((item) => item.summary) : []),
    ...(Array.isArray(answerKey.textbook_support) ? answerKey.textbook_support.slice(0, 1).map((item) => item.note_block || item.sentence) : []),
  ]
    .map((text) => cleanInline(text))
    .filter(Boolean)
    .slice(0, 2);
  return `
    <section class="analysis-card ${summaryTone}">
      <header class="analysis-head">
        <strong>正解 ${escapeHtml(answerKey.correct_label || "")}</strong>
        <span>${escapeHtml(answerKey.correct_text || "")}</span>
      </header>
      <p class="analysis-summary">${escapeHtml(answerKey.explanation || "")}</p>
      <div class="analysis-list">
        ${analyses
          .map(
            (item) => `
              <div class="analysis-item ${item.is_correct ? "is-correct" : ""}">
                <span>${escapeHtml(item.label)}</span>
                <p>${escapeHtml(item.analysis)}</p>
              </div>
            `
          )
          .join("")}
      </div>
      ${
        supports.length
          ? `<div class="analysis-support">${supports
              .map((item) => `<span class="support-chip">${escapeHtml(item)}</span>`)
              .join("")}</div>`
          : ""
      }
      ${
        `<div class="analysis-actions">
          <button id="feedback-btn" class="ghost-btn" type="button">問題反饋</button>
          ${!state.roundComplete ? `<button id="continue-btn" class="next-round-btn" type="button">下一題</button>` : ""}
        </div>`
      }
    </section>
  `;
}

function renderCompletionCard() {
  if (!state.roundComplete) return "";
  const promoted = Boolean(state.roundComplete.promoted);
  const toTier = state.roundComplete.toTier || {};
  const fromTier = state.roundComplete.fromTier || {};
  const reviewAvailable = Boolean(state.roundComplete.reviewAvailable);
  const isReview = (state.roundComplete.mode || state.mode) === "review";
  const challengeDoneLabel = state.kind === MIXED_KIND ? "通篇完成" : "本輪完成";
  return `
    <section class="completion-card ${promoted ? "up" : "hold"}">
      <div class="completion-crest">${escapeHtml(toTier.tierName || fromTier.tierName || "工蜂")}</div>
      <div class="completion-copy">
        <strong>${
          isReview
            ? "錯題回看"
            : promoted && state.kind === MIXED_KIND
              ? "通篇完成"
              : challengeDoneLabel
        }</strong>
        <span>${state.roundComplete.correctCount} / ${state.roundComplete.targetCount || ROUND_SIZE}</span>
      </div>
      <div class="completion-actions">
        ${
          reviewAvailable
            ? `<button id="redo-mistakes-btn" class="ghost-btn" type="button">錯題再來一遍</button>`
            : ""
        }
        <button id="next-round-btn" class="next-round-btn" type="button">${isReview ? "返回常規題" : "再來一輪"}</button>
      </div>
    </section>
  `;
}

function renderRoundCompletion() {
  els.questionRoot.innerHTML = renderCompletionCard() || `<div class="empty-state empty-state-quiet"></div>`;
  els.questionRoot.querySelector("#redo-mistakes-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, state.articleId, "review", state.run?.id || "");
  });
  els.questionRoot.querySelector("#next-round-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, state.articleId || state.activeArticle?.article_id || "", "ladder");
  });
}

function renderRoundTrack() {
  const targetCount = Number(state.run?.targetCount || state.roundComplete?.targetCount || ROUND_SIZE);
  const marks = state.roundMarks.slice(0, targetCount);
  const currentIndex = state.currentItem?.prompt && !state.roundComplete ? Math.min(marks.length, targetCount - 1) : -1;
  const hasQuestion = Boolean(state.currentItem?.prompt || state.roundComplete);
  const hasAnswer = Boolean(state.answered || state.answerCard || state.roundComplete);
  placeRoundTrack(hasAnswer);
  els.roundTrack.classList.toggle("is-idle", !hasQuestion);
  els.roundTrack.classList.toggle("after-answer", hasAnswer);
  els.questionRoot.classList.toggle("has-answer", hasAnswer);
  els.questionRoot.classList.toggle("is-idle", !hasQuestion);
  els.roundTrack.style.gridTemplateColumns = `repeat(${targetCount}, minmax(0, 1fr))`;
  els.roundTrack.innerHTML = Array.from({ length: targetCount }, (_, index) => {
    const status =
      index < marks.length ? (marks[index] ? "hit" : "miss") : index === currentIndex ? "current" : "pending";
    return `<span class="track-slot ${status}" aria-label="slot-${index + 1}"></span>`;
  }).join("");
}

function placeRoundTrack(hasAnswer) {
  if (!els.roundTrack) return;
  if (hasAnswer) {
    if (els.roundTrack.parentElement !== els.questionRoot) {
      els.questionRoot.appendChild(els.roundTrack);
    }
    return;
  }
  if (els.battleStage && els.roundTrack.parentElement !== els.battleStage) {
    els.battleStage.appendChild(els.roundTrack);
  }
}

function renderLeaderboard() {
  els.leaderboardTabs.forEach((button) => {
    button.classList.toggle("active", button.dataset.scope === state.leaderboardScope);
  });
  if (els.leaderboardMeta) {
    els.leaderboardMeta.textContent = formatPeriodLabel(state.leaderboardScope, state.leaderboard?.period);
  }
  const entries = Array.isArray(state.leaderboard?.[state.leaderboardScope]) ? state.leaderboard[state.leaderboardScope] : [];
  const me = state.leaderboard?.me?.[state.leaderboardScope] || null;
  if (!entries.length && !me) {
    els.leaderboardRoot.innerHTML = `<div class="leaderboard-empty">暫無榜單資料。</div>`;
    return;
  }
  const topRows = entries.slice(0, state.leaderboardScope === "total" ? 10 : 8);
  const meShown = Boolean(me && topRows.some((entry) => entry.sessionId === me.sessionId));
  const parts = topRows.map((entry) => renderLeaderboardRow(entry, Boolean(me && entry.sessionId === me.sessionId)));
  if (me && !meShown) {
    parts.push(`<div class="leaderboard-divider">我的位置</div>`);
    parts.push(renderLeaderboardRow(me, true, true));
  }
  els.leaderboardRoot.innerHTML = parts.join("");
}

function optionState(prompt, option, key) {
  const selected = state.answerReveal ? state.answerReveal.submitted.has(key) : false;
  const correct = state.answerReveal ? state.answerReveal.correct.has(key) : false;
  const incorrect = state.answered && selected && !correct;
  const dimmed = state.answered && !selected && !correct;
  return { selected, correct, incorrect, dimmed };
}

async function submitAnswer(label) {
  if (!state.currentItem?.answerToken || state.submitting || state.answered) return;
  try {
    state.submitting = true;
    const payload = await api("/api/challenge/answer", {
      method: "POST",
      body: JSON.stringify({
        answerToken: state.currentItem.answerToken,
        answer: { label },
      }),
    });
    if (payload?.staleItem) {
      state.currentItem = null;
      state.answered = false;
      state.submitting = false;
      state.answerReveal = null;
      state.answerCard = null;
      renderShell();
      await loadNextQuestion(undefined, { focus: "question" });
      return;
    }
    const correct = Boolean(payload?.result?.correct);
    state.run = payload.run || state.run;
    state.answered = true;
    state.answerReveal = buildAnswerReveal(payload?.result?.correctAnswer, payload?.result?.submittedAnswer);
    state.answerCard = payload?.result?.answerKey || null;
    state.player = payload?.player || state.player || currentPlayerState();
    if (!payload?.alreadyAnswered) {
      state.roundMarks.push(correct);
    }
    state.roundComplete = payload?.roundCompleted ? payload.round || null : null;
    renderShell();
    revealInteractionTarget(state.roundComplete ? "completion" : "analysis", { focus: state.roundComplete ? "completion" : "analysis" });
    if (state.roundComplete) {
      await refreshBootstrap();
      state.player = currentPlayerState();
      await loadLeaderboard(state.kind);
      return;
    }
  } catch (error) {
    console.error(error);
    renderError(error.message || String(error));
  } finally {
    state.submitting = false;
  }
}

function buildAnswerReveal(correctAnswer, submittedAnswer) {
  const correct = new Set();
  const submitted = new Set();
  if (correctAnswer?.label) correct.add(correctAnswer.label);
  (correctAnswer?.keys || []).forEach((key) => correct.add(key));
  if (submittedAnswer?.label) submitted.add(submittedAnswer.label);
  (submittedAnswer?.keys || []).forEach((key) => submitted.add(key));
  return { correct, submitted };
}

function currentPlayerState(kind = state.kind) {
  if (!state.bootstrap?.player) return null;
  if (kind === GAOKAO_KIND) return state.bootstrap.player.gaokao || null;
  if (kind === MIXED_KIND) return state.bootstrap.player.mixed || null;
  return kind === "function_word" ? state.bootstrap.player.function : state.bootstrap.player.content;
}

function buildAccountSummary(player) {
  const auth = state.bootstrap?.auth;
  if (!auth?.authenticated || !auth.user || !player) return "";
  const displayName = cleanInline(auth.user.displayName || auth.user.slug || "");
  const tierName = cleanInline(player.tierName || "工蜂");
  const modeName = cleanInline(player.modeName || "常規排位");
  const totalScore = Number(player.totalScore || 0);
  const roundsPlayed = Number(player.roundsPlayed || 0);
  return `已登錄：${displayName}｜段位：${tierName}（${modeName}）｜總積分：${totalScore}｜參與：${roundsPlayed} 次`;
}

function currentArticle(articleId = state.articleId) {
  const articles = Array.isArray(state.bootstrap?.articles) ? state.bootstrap.articles : [];
  return articles.find((article) => article.article_id === articleId) || null;
}

function setTheme(theme) {
  const normalized = THEMES.includes(theme) ? theme : "parchment";
  state.theme = normalized;
  applyTheme(normalized);
  window.localStorage.setItem(THEME_KEY, normalized);
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  if (els.themeSelect) {
    els.themeSelect.value = theme;
  }
}

function loadTheme() {
  const stored = window.localStorage.getItem(THEME_KEY);
  return THEMES.includes(stored) ? stored : "parchment";
}

function kindLabel(kind) {
  if (kind === GAOKAO_KIND) return "高考混編";
  if (kind === MIXED_KIND) return "選篇掌握";
  return kind === "function_word" ? "虛詞" : "實詞";
}

function formatPeriodLabel(scope, periodInfo) {
  if (!periodInfo) return "統計範圍：全部歷史";
  if (scope === "daily") return `日期：${periodInfo.dailyKey || "-"}`;
  if (scope === "weekly") return `週次：${periodInfo.weeklyKey || "-"}`;
  return "統計範圍：全部歷史";
}

function leaderboardValue(entry, scope) {
  if (scope === "total") {
    return `${Number(entry.tier?.tierIndex || 1)}段`;
  }
  return `${Math.round(Number(entry.score || 0))}分`;
}

function leaderboardSubline(entry, scope) {
  if (scope === "total") {
    return `全對 ${Number(entry.perfectRounds || 0)} 輪｜作答 ${Number(entry.roundsPlayed || 0)} 輪`;
  }
  return `本期 ${Number(entry.runs || 0)} 輪｜段位 ${escapeHtml(entry.tier?.tierName || "工蜂")}`;
}

function renderLeaderboardRow(entry, isMe = false, pinned = false) {
  return `
    <div class="leaderboard-row ${isMe ? "me" : ""} ${pinned ? "pinned" : ""}">
      <span class="leaderboard-rank">#${Number(entry.rank || 0)}</span>
      <div class="leaderboard-main">
        <strong>${escapeHtml(entry.displayName || "")}</strong>
        <div class="leaderboard-tier">${escapeHtml(entry.tier?.tierName || "工蜂")} · ${escapeHtml(entry.tier?.modeName || "常規排位")}</div>
      </div>
      <div class="leaderboard-score">
        ${leaderboardValue(entry, state.leaderboardScope)}
        <small>${leaderboardSubline(entry, state.leaderboardScope)}</small>
      </div>
    </div>
  `;
}

function termHeadword(termId) {
  return String(termId || "").split("::").pop() || "";
}

function optionTooltip(prompt, option, headword) {
  if (prompt.questionType !== "xuci_pair_compare") {
    return contextTooltip(
      option.context_window || prompt.contextWindow || [],
      option.sentence || prompt.sentence || prompt.passage || "",
      option.headword || headword
    );
  }
  const optionHeadword = option.headword || headword;
  const sentences = normalizedOptionSentences(option);
  const contextGroups = Array.isArray(option.sentence_contexts) ? option.sentence_contexts : [];
  const sections = sentences
    .map((sentence, index) => {
      const tooltip = contextTooltip(contextGroups[index] || [], sentence, optionHeadword);
      return cleanInline(tooltip);
    })
    .filter(Boolean);
  return sections.join("\n\n");
}

function renderOptionInlineText(prompt, option, headword) {
  if (prompt.questionType === "xuci_pair_compare") {
    const optionHeadword = option.headword || headword || "";
    const sentences = normalizedOptionSentences(option);
    const sentenceHtml = sentences
      .map((sentence) => highlightTerm(sentence, optionHeadword))
      .filter(Boolean)
      .join(`<span class="option-inline-sep">；</span>`);
    const headwordHtml = optionHeadword
      ? `<strong class="option-inline-headword">${escapeHtml(optionHeadword)}</strong>`
      : "";
    return [headwordHtml, sentenceHtml].filter(Boolean).join(`<span class="option-inline-sep">｜</span>`);
  }

  if (prompt.sourceKind === "exam" && option.sentence) {
    const optionHeadword = option.headword || headword;
    const sentence = cleanInline(option.sentence || "");
    const text = cleanInline(option.text || "");
    if (!text || compactText(text) === compactText(sentence)) {
      return highlightTerm(option.sentence || "", optionHeadword);
    }
    if (compactText(text).includes(compactText(sentence))) {
      return highlightTerm(option.text || "", optionHeadword);
    }
    return `${highlightTerm(option.sentence || "", optionHeadword)}<span class="option-inline-sep">——</span><span class="option-inline-detail">${highlightTerm(option.text || "", optionHeadword)}</span>`;
  }

  return highlightTerm(option.text || "", option.headword || headword);
}

function normalizedOptionSentences(option) {
  if (Array.isArray(option.sentences) && option.sentences.length) {
    return option.sentences.map((item) => cleanInline(item)).filter(Boolean);
  }
  return String(option.text || "")
    .split(/◆|；|;/)
    .map((item) => cleanInline(item))
    .filter(Boolean);
}

function highlightTerm(text, headword) {
  const safe = escapeHtml(text || "");
  if (!headword) return safe;
  const escaped = escapeRegex(escapeHtml(headword));
  return safe.replace(new RegExp(escaped, "g"), `<span class="term-hit">${escapeHtml(headword)}</span>`);
}

function renderError(message) {
  els.questionRoot.innerHTML = `
    <div class="empty-state error-state">
      <p>${escapeHtml(message)}</p>
    </div>
  `;
  revealInteractionTarget("question", { focus: "question" });
}

function revealInteractionTarget(target, options = {}) {
  if (options.scroll === false || !isCompactLayout()) return;
  const element = targetElement(target);
  if (!element) return;
  const behavior = window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth";
  window.requestAnimationFrame(() => {
    element.scrollIntoView({ behavior, block: "start", inline: "nearest" });
  });
}

function targetElement(target) {
  if (target === "articles") {
    return els.articlePanel && !els.articlePanel.hidden ? els.articlePanel : els.modePanel;
  }
  if (target === "analysis") {
    return els.questionRoot?.querySelector(".analysis-card") || els.questionRoot;
  }
  if (target === "completion") {
    return els.questionRoot?.querySelector(".completion-card") || els.questionRoot;
  }
  return els.questionRoot?.querySelector(".question-card") || document.querySelector(".battle-stage") || els.questionRoot;
}

function isCompactLayout() {
  return window.matchMedia("(max-width: 980px)").matches;
}

function countTrue(values) {
  return values.filter(Boolean).length;
}

function cleanInline(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function contextTooltip(items, focusText = "", headword = "") {
  const lines = pickContextLines(items, focusText, headword);
  return lines.join("\n");
}

function pickContextLines(items, focusText = "", headword = "") {
  const lines = expandContextSentences(items);
  if (lines.length <= 3) return lines;
  const focus = cleanInline(focusText);
  const compactFocus = compactText(focus);
  const normalizedHeadword = cleanInline(headword);
  const compactHeadword = compactText(normalizedHeadword);
  let index = lines.findIndex((line) => {
    const compactLine = compactText(line);
    return (
      (focus && line.includes(focus)) ||
      (compactFocus && compactLine.includes(compactFocus)) ||
      (normalizedHeadword && line.includes(normalizedHeadword)) ||
      (compactHeadword && compactLine.includes(compactHeadword))
    );
  });
  if (index < 0) index = Math.min(1, lines.length - 1);
  let start = Math.max(0, index - 1);
  let end = Math.min(lines.length, start + 3);
  start = Math.max(0, end - 3);
  return lines.slice(start, end);
}

function buildVisibleContext(prompt, headword = "") {
  return pickContextLines(prompt.contextWindow || [], prompt.sentence || prompt.passage || "", headword);
}

function buildVisibleContextForPrompt(prompt, headword = "") {
  if (prompt.sourceKind === "exam") {
    return expandContextSentences(prompt.contextWindow || []);
  }
  if (prompt.sourceKind === "textbook") {
    return buildVisibleContext(prompt, headword);
  }
  return [];
}

function collectFocusTexts(prompt) {
  const texts = [];
  if (prompt.sentence) texts.push(prompt.sentence);
  if (prompt.passage) texts.push(prompt.passage);
  const options = Array.isArray(prompt.options) ? prompt.options : [];
  options.forEach((option) => {
    if (option?.sentence) texts.push(option.sentence);
    if (Array.isArray(option?.sentences)) {
      option.sentences.forEach((s) => {
        if (s) texts.push(s);
      });
    } else if (prompt.questionType === "xuci_pair_compare") {
      normalizedOptionSentences(option).forEach((s) => texts.push(s));
    }
  });
  return texts;
}

function collectHighlightHeadwords(prompt, fallback = "") {
  const words = new Set();
  if (fallback) words.add(fallback);
  const options = Array.isArray(prompt.options) ? prompt.options : [];
  options.forEach((option) => {
    if (option?.headword) words.add(option.headword);
  });
  return [...words].filter(Boolean);
}

function highlightTerms(text, headwords) {
  if (!headwords || !headwords.length) return escapeHtml(text || "");
  let safe = escapeHtml(text || "");
  const sorted = [...headwords].sort((a, b) => b.length - a.length);
  sorted.forEach((word) => {
    if (!word) return;
    const escaped = escapeRegex(escapeHtml(word));
    safe = safe.replace(new RegExp(escaped, "g"), `<span class="term-hit">${escapeHtml(word)}</span>`);
  });
  return safe;
}

function expandContextSentences(items) {
  return (Array.isArray(items) ? items : [])
    .flatMap((item) => splitContextSentence(String(item || "")))
    .map((item) => cleanInline(item))
    .filter(Boolean);
}

function splitContextSentence(text) {
  const value = cleanInline(text);
  if (!value) return [];
  const pieces = value.match(/[^。！？；]+(?:[。！？；]+|[”」』"])?/g) || [value];
  return pieces.map((piece) => cleanInline(piece)).filter(Boolean);
}

function isFocusContextLine(line, focusText, headword = "") {
  const focus = cleanInline(focusText);
  const token = cleanInline(headword);
  if (!focus && !token) return false;
  return (
    (focus && (cleanInline(line).includes(focus) || compactText(line).includes(compactText(focus)))) ||
    (token && (cleanInline(line).includes(token) || compactText(line).includes(compactText(token))))
  );
}

function compactText(value) {
  return cleanInline(value).replace(/\s+/g, "");
}

function initFeedbackModal() {
  els.feedbackClose?.addEventListener("click", closeFeedbackModal);
  els.feedbackCancel?.addEventListener("click", closeFeedbackModal);
  els.feedbackAnonymous?.addEventListener("change", syncFeedbackReporterState);
  els.feedbackForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    void submitFeedback();
  });
  els.feedbackModal?.addEventListener("click", (event) => {
    if (event.target === els.feedbackModal) {
      closeFeedbackModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !els.feedbackModal?.hidden) {
      closeFeedbackModal();
    }
  });
}

function openFeedbackModal() {
  if (!state.currentItem?.prompt || !state.answerCard || !els.feedbackModal) return;
  els.feedbackForm?.reset();
  const defaultName = defaultReporterName();
  if (els.feedbackName) {
    els.feedbackName.value = defaultName;
  }
  if (els.feedbackAnonymous) {
    els.feedbackAnonymous.checked = !defaultName;
  }
  const sourceLabel = state.currentItem.prompt.sourceLabel || kindLabel(state.kind);
  const stem = cleanInline(state.currentItem.prompt.stem || "");
  if (els.feedbackPromptMeta) {
    els.feedbackPromptMeta.textContent = `${sourceLabel} · ${stem}`;
  }
  setFeedbackStatus("");
  syncFeedbackReporterState();
  els.feedbackModal.hidden = false;
  document.body.classList.add("modal-open");
  els.feedbackMessage?.focus();
}

function closeFeedbackModal() {
  if (!els.feedbackModal) return;
  els.feedbackModal.hidden = true;
  document.body.classList.remove("modal-open");
  state.feedbackSubmitting = false;
  if (els.feedbackSubmit) {
    els.feedbackSubmit.disabled = false;
  }
}

function syncFeedbackReporterState() {
  const anonymous = Boolean(els.feedbackAnonymous?.checked);
  if (els.feedbackName) {
    els.feedbackName.disabled = anonymous;
    if (anonymous) {
      els.feedbackName.value = "";
    } else if (!els.feedbackName.value) {
      els.feedbackName.value = defaultReporterName();
    }
  }
}

function defaultReporterName() {
  const user = state.bootstrap?.auth?.user;
  return cleanInline(user?.displayName || user?.slug || "");
}

function currentFeedbackPayload() {
  const prompt = state.currentItem?.prompt;
  const answerCard = state.answerCard;
  if (!prompt || !answerCard) return null;
  return {
    challengeId: prompt.challengeId || "",
    kind: prompt.kind || state.kind || "",
    questionType: prompt.questionType || "",
    termId: prompt.termId || "",
    sourceKind: prompt.sourceKind || "",
    sourceLabel: prompt.sourceLabel || "",
    sourceTitle: prompt.sourceTitle || "",
    stem: prompt.stem || "",
    sentence: prompt.sentence || "",
    passage: prompt.passage || "",
    contextWindow: JSON.stringify(prompt.contextWindow || []),
    correctLabel: answerCard.correct_label || "",
    correctText: answerCard.correct_text || "",
    explanation: answerCard.explanation || "",
    optionAnalyses: JSON.stringify(answerCard.option_analyses || []),
    pageUrl: window.location.href,
  };
}

async function submitFeedback() {
  if (state.feedbackSubmitting) return;
  const payload = currentFeedbackPayload();
  if (!payload) {
    setFeedbackStatus("當前題目資料不足，暫時無法提交。", "error");
    return;
  }
  const message = cleanInline(els.feedbackMessage?.value || "");
  if (!message) {
    setFeedbackStatus("請先寫明這道題的問題。", "error");
    els.feedbackMessage?.focus();
    return;
  }
  const file = els.feedbackScreenshot?.files?.[0] || null;
  if (file && (!file.type.startsWith("image/") || file.size > 8 * 1024 * 1024)) {
    setFeedbackStatus("截圖需為圖片，且不能超過 8 MB。", "error");
    return;
  }

  const formData = new FormData();
  Object.entries(payload).forEach(([key, value]) => {
    formData.append(key, value);
  });
  const anonymous = Boolean(els.feedbackAnonymous?.checked);
  formData.append("anonymous", anonymous ? "1" : "0");
  formData.append("reporterName", cleanInline(els.feedbackName?.value || ""));
  formData.append("message", els.feedbackMessage?.value || "");
  if (file) {
    formData.append("screenshot", file, file.name || "feedback-image");
  }

  try {
    state.feedbackSubmitting = true;
    if (els.feedbackSubmit) {
      els.feedbackSubmit.disabled = true;
    }
    setFeedbackStatus("正在提交到 GitHub…");
    const result = await api("/api/feedback", {
      method: "POST",
      body: formData,
    });
    const issue = result?.issue;
    const issueLabel = issue?.number ? `#${issue.number}` : "issue";
    const issueUrl = issue?.html_url || "";
    setFeedbackStatus(
      issueUrl ? `已提交到 GitHub ${issueLabel}` : "已提交到 GitHub。",
      "success",
      issueUrl
    );
    if (els.feedbackMessage) {
      els.feedbackMessage.value = "";
    }
    if (els.feedbackScreenshot) {
      els.feedbackScreenshot.value = "";
    }
  } catch (error) {
    console.error(error);
    setFeedbackStatus(error.message || String(error), "error");
  } finally {
    state.feedbackSubmitting = false;
    if (els.feedbackSubmit) {
      els.feedbackSubmit.disabled = false;
    }
  }
}

function setFeedbackStatus(message, tone = "", link = "") {
  if (!els.feedbackStatus) return;
  els.feedbackStatus.className = `feedback-status ${tone}`.trim();
  if (!message) {
    els.feedbackStatus.textContent = "";
    return;
  }
  els.feedbackStatus.innerHTML = link
    ? `${escapeHtml(message)} <a href="${escapeAttr(link)}" target="_blank" rel="noopener noreferrer">打開 Issue</a>`
    : escapeHtml(message);
}

async function api(url, init = {}) {
  const headers = new Headers(init.headers || {});
  if (!(init.body instanceof FormData) && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  headers.set("X-WY-Device-Id", getAuditDeviceId());
  const response = await fetch(url, {
    ...init,
    credentials: "include",
    headers,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail || payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

function getAuditDeviceId() {
  try {
    const existing = window.localStorage.getItem(AUDIT_DEVICE_KEY);
    if (existing && /^[a-z0-9_-]{12,128}$/i.test(existing)) return existing;
    const bytes = new Uint8Array(18);
    crypto.getRandomValues(bytes);
    const generated = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
    window.localStorage.setItem(AUDIT_DEVICE_KEY, generated);
    return generated;
  } catch (error) {
    return `ephemeral-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  }
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
