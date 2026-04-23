const THEME_KEY = "wy-theme";
const THEMES = ["parchment", "forest", "ink"];
const ROUND_SIZE = 10;

const state = {
  bootstrap: null,
  theme: loadTheme(),
  kind: "",
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
  feedbackSubmitting: false,
};

const els = {
  questionRoot: document.querySelector("#question-root"),
  roundTrack: document.querySelector("#round-track"),
  leaderboardRoot: document.querySelector("#leaderboard-root"),
  leaderboardKind: document.querySelector("#leaderboard-kind"),
  rankBadge: document.querySelector("#rank-badge"),
  rankKicker: document.querySelector("#rank-kicker"),
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
};

document.querySelectorAll("[data-kind]").forEach((button) => {
  button.addEventListener("click", () => startChallenge(button.dataset.kind || ""));
});

els.themeSelect?.addEventListener("change", (event) => {
  const value = event.target instanceof HTMLSelectElement ? event.target.value : "parchment";
  setTheme(value);
});

applyTheme(state.theme);

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
  renderShell();
}

async function refreshBootstrap() {
  state.bootstrap = await api("/api/bootstrap");
  if (state.kind) {
    state.player = currentPlayerState();
  }
  syncEntryButtons();
}

async function startChallenge(kind, mode = "ladder", sourceRunId = "") {
  state.kind = kind;
  state.mode = mode;
  state.sourceRunId = sourceRunId;
  state.run = null;
  state.currentItem = null;
  state.answered = false;
  state.submitting = false;
  state.answerReveal = null;
  state.answerCard = null;
  state.roundMarks = [];
  state.roundComplete = null;
  state.player = currentPlayerState();
  syncEntryButtons();
  await loadLeaderboard(kind);
  await loadNextQuestion();
}

async function loadLeaderboard(kind) {
  const payload = await api(`/api/leaderboard?kind=${encodeURIComponent(kind || "content_word")}`);
  state.leaderboard = payload;
  renderLeaderboard();
}

async function loadNextQuestion() {
  if (!state.kind) {
    renderShell();
    return;
  }
  const search = new URLSearchParams({ kind: state.kind });
  search.set("mode", state.mode || "ladder");
  if (state.run?.id && !state.roundComplete) {
    search.set("runId", state.run.id);
  }
  if (!state.run?.id && state.sourceRunId) {
    search.set("sourceRunId", state.sourceRunId);
  }
  const payload = await api(`/api/challenge/next?${search.toString()}`);
  state.run = payload.run || null;
  state.mode = payload.run?.mode || state.mode || "ladder";
  state.currentItem = payload.item || null;
  state.answered = false;
  state.submitting = false;
  state.answerReveal = null;
  state.answerCard = null;
  state.roundComplete = payload.roundCompleted ? payload.round || null : null;
  state.player = payload.player || currentPlayerState();
  if (state.roundComplete && !state.currentItem) {
    await refreshBootstrap();
    state.player = currentPlayerState();
    await loadLeaderboard(state.kind);
  }
  renderShell();
}

function renderShell() {
  renderStatus();
  renderQuestion();
  renderRoundTrack();
  renderLeaderboard();
}

function renderStatus() {
  const player = state.player || currentPlayerState();
  const kindLabelText = kindLabel(state.kind || player?.kind || "content_word");
  els.rankKicker.textContent = kindLabelText;
  els.rankName.textContent = player?.tierName || "工蜂";
  els.rankMode.textContent = state.mode === "review" ? "錯題回看" : player?.modeName || "常規排位";
  const answeredCount = Number(state.run?.answeredCount || state.roundMarks.length || 0);
  const targetCount = Number(state.run?.targetCount || state.roundComplete?.targetCount || ROUND_SIZE);
  const correctCount = Number(state.run?.correctCount || countTrue(state.roundMarks) || 0);
  els.roundFraction.textContent = `${answeredCount} / ${targetCount}`;
  els.roundGoal.textContent = `${correctCount} / ${targetCount}`;
  els.rankBadge.dataset.kind = state.kind || player?.kind || "";
}

function renderQuestion() {
  if (!state.kind) {
    els.questionRoot.innerHTML = `
      <div class="empty-state">
        <p>選擇實詞或虛詞後直接進題。</p>
      </div>
    `;
    return;
  }

  if (!state.currentItem?.prompt) {
    renderRoundCompletion();
    return;
  }

  const prompt = state.currentItem.prompt;
  const headword = termHeadword(prompt.termId);
  const visibleContext = prompt.sourceKind === "textbook" ? buildVisibleContext(prompt, headword) : [];
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
            const focus = isFocusContextLine(line, prompt.sentence || prompt.passage || "", headword);
            return `<p class="context-line ${focus ? "is-focus" : ""}">${highlightTerm(line, headword)}</p>`;
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

    if (prompt.questionType === "xuci_pair_compare") {
      const sentences = Array.isArray(option.sentences)
        ? option.sentences
            .map((sentence, index) => {
              const contexts = Array.isArray(option.sentence_contexts) ? option.sentence_contexts[index] || [] : [];
              return `<div class="pair-sentence" title="${escapeAttr(contextTooltip(contexts, sentence, option.headword || headword))}">${highlightTerm(sentence, option.headword || headword)}</div>`;
            })
            .join("")
        : "";
      parts.push(`
        <button class="${classes}" type="button" data-option="${escapeAttr(key)}" ${state.answered ? "disabled" : ""}>
          <div class="option-head">
            <span class="option-tag">${escapeHtml(key)}</span>
            <strong>${escapeHtml(option.headword || "")}</strong>
          </div>
          <div class="pair-sentences">${sentences}</div>
        </button>
      `);
      return;
    }

    parts.push(`
      <button class="${classes}" type="button" data-option="${escapeAttr(key)}" title="${escapeAttr(contextTooltip(option.context_window || [], option.sentence || "", option.headword || headword))}" ${state.answered ? "disabled" : ""}>
        <div class="option-head">
          <span class="option-tag">${escapeHtml(key)}</span>
        </div>
        ${
          prompt.sourceKind === "exam" && option.sentence
            ? `<div class="option-sentence">${highlightTerm(option.sentence || "", option.headword || headword)}</div>
               <p class="option-gloss">${highlightTerm(option.text || "", option.headword || headword)}</p>`
            : `<p class="option-gloss">${highlightTerm(option.text || "", headword)}</p>`
        }
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
    void loadNextQuestion();
  });
  els.questionRoot.querySelector("#feedback-btn")?.addEventListener("click", () => {
    openFeedbackModal();
  });
  els.questionRoot.querySelector("#redo-mistakes-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, "review", state.run?.id || "");
  });
  els.questionRoot.querySelector("#next-round-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, "ladder");
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
  const reviewCount = Number(state.roundComplete.reviewCount || 0);
  const isReview = (state.roundComplete.mode || state.mode) === "review";
  const authNote = state.bootstrap?.auth?.authenticated
    ? `<p class="completion-note">已登入，本輪結果已自動寫入用戶系統。</p>`
    : "";
  return `
    <section class="completion-card ${promoted ? "up" : "hold"}">
      <div class="completion-crest">${escapeHtml(toTier.tierName || fromTier.tierName || "工蜂")}</div>
      <div class="completion-copy">
        <strong>${
          isReview
            ? "這輪錯題回看已完成"
            : promoted
              ? "本輪全對，段位晉升"
              : "本輪未全對，段位保持"
        }</strong>
        <span>${state.roundComplete.correctCount} / ${state.roundComplete.targetCount || ROUND_SIZE}</span>
        ${
          reviewAvailable
            ? `<p class="completion-note">要不要把做錯的題再來一遍？目前還有 ${reviewCount} 題待回看。</p>`
            : ""
        }
        ${authNote}
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
  els.questionRoot.innerHTML = renderCompletionCard() || `
    <div class="empty-state">
      <p>本輪已結束，點擊詞類開始下一輪。</p>
    </div>
  `;
  els.questionRoot.querySelector("#redo-mistakes-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, "review", state.run?.id || "");
  });
  els.questionRoot.querySelector("#next-round-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind, "ladder");
  });
}

function renderRoundTrack() {
  const targetCount = Number(state.run?.targetCount || state.roundComplete?.targetCount || ROUND_SIZE);
  const marks = state.roundMarks.slice(0, targetCount);
  const currentIndex = state.currentItem?.prompt && !state.roundComplete ? Math.min(marks.length, targetCount - 1) : -1;
  els.roundTrack.style.gridTemplateColumns = `repeat(${targetCount}, minmax(0, 1fr))`;
  els.roundTrack.innerHTML = Array.from({ length: targetCount }, (_, index) => {
    const status =
      index < marks.length ? (marks[index] ? "hit" : "miss") : index === currentIndex ? "current" : "pending";
    return `<span class="track-slot ${status}" aria-label="slot-${index + 1}"></span>`;
  }).join("");
}

function renderLeaderboard() {
  els.leaderboardKind.textContent = kindLabel(state.kind || state.leaderboard?.kind || "content_word");
  const entries = Array.isArray(state.leaderboard?.entries) ? state.leaderboard.entries : [];
  if (!entries.length) {
    els.leaderboardRoot.innerHTML = `<div class="leaderboard-empty">暫無榜單資料。</div>`;
    return;
  }
  const maxTier = Math.max(...entries.map((entry) => Number(entry.tier?.tierIndex || 1)), 1);
  els.leaderboardRoot.innerHTML = entries
    .slice(0, 8)
    .map((entry) => {
      const fill = Math.max(18, Math.round((Number(entry.tier?.tierIndex || 1) / maxTier) * 100));
      return `
        <div class="leaderboard-row">
          <span class="leaderboard-rank">#${entry.rank}</span>
          <div class="leaderboard-main">
            <strong>${escapeHtml(entry.displayName || "")}</strong>
            <div class="leaderboard-tier">${escapeHtml(entry.tier?.tierName || "工蜂")} · ${Number(entry.perfectRounds || 0)} 胜</div>
            <div class="leaderboard-bar"><i style="width:${fill}%"></i></div>
          </div>
        </div>
      `;
    })
    .join("");
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
      await loadNextQuestion();
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

function currentPlayerState() {
  if (!state.bootstrap?.player) return null;
  return state.kind === "function_word" ? state.bootstrap.player.function : state.bootstrap.player.content;
}

function syncEntryButtons() {
  document.querySelectorAll("[data-kind]").forEach((button) => {
    button.classList.toggle("active", button.dataset.kind === state.kind);
    button.disabled = !hasAvailableChallenges(button.dataset.kind || "");
  });
}

function hasAvailableChallenges(kind) {
  const stats = state.bootstrap?.stats || {};
  if (kind === "function_word") return Number(stats.functionChallenges || 0) > 0;
  if (kind === "content_word") return Number(stats.contentChallenges || 0) > 0;
  return false;
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
  return kind === "function_word" ? "虛詞" : "實詞";
}

function termHeadword(termId) {
  return String(termId || "").split("::").pop() || "";
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
  const response = await fetch(url, {
    credentials: "include",
    headers,
    ...init,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail || payload.error || `HTTP ${response.status}`);
  }
  return payload;
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
