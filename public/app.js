const THEME_KEY = "wy-theme";
const THEMES = ["parchment", "forest", "ink"];
const ROUND_SIZE = 10;

const state = {
  bootstrap: null,
  theme: loadTheme(),
  kind: "",
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
  autoAdvanceTimer: 0,
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
};

document.querySelectorAll("[data-kind]").forEach((button) => {
  button.addEventListener("click", () => startChallenge(button.dataset.kind || ""));
});

els.themeSelect?.addEventListener("change", (event) => {
  const value = event.target instanceof HTMLSelectElement ? event.target.value : "parchment";
  setTheme(value);
});

window.addEventListener("pagehide", clearAdvanceTimer);

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

async function startChallenge(kind) {
  clearAdvanceTimer();
  state.kind = kind;
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
  if (state.run?.id && !state.roundComplete) {
    search.set("runId", state.run.id);
  }
  const payload = await api(`/api/challenge/next?${search.toString()}`);
  state.run = payload.run || null;
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
  els.rankMode.textContent = player?.modeName || "常規排位";
  const answeredCount = Number(state.run?.answeredCount || state.roundMarks.length || 0);
  const correctCount = Number(state.run?.correctCount || countTrue(state.roundMarks) || 0);
  els.roundFraction.textContent = `${answeredCount} / ${ROUND_SIZE}`;
  els.roundGoal.textContent = `${correctCount} / ${ROUND_SIZE}`;
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
  const parts = [
    `<article class="question-card">`,
    `<h2 class="question-stem">${escapeHtml(prompt.stem)}</h2>`,
  ];

  if (prompt.sentence) {
    parts.push(`<div class="question-context">${highlightTerm(prompt.sentence, headword)}</div>`);
  }
  if (prompt.passage) {
    parts.push(`<div class="question-passage">${highlightTerm(prompt.passage, headword)}</div>`);
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
        ? option.sentences.map((sentence) => `<div class="pair-sentence">${highlightTerm(sentence, option.headword || headword)}</div>`).join("")
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
      <button class="${classes}" type="button" data-option="${escapeAttr(key)}" ${state.answered ? "disabled" : ""}>
        <div class="option-head">
          <span class="option-tag">${escapeHtml(key)}</span>
        </div>
        <p>${highlightTerm(option.text || "", headword)}</p>
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
  els.questionRoot.querySelector("#next-round-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind);
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
    </section>
  `;
}

function renderCompletionCard() {
  if (!state.roundComplete) return "";
  const promoted = Boolean(state.roundComplete.promoted);
  const toTier = state.roundComplete.toTier || {};
  const fromTier = state.roundComplete.fromTier || {};
  return `
    <section class="completion-card ${promoted ? "up" : "hold"}">
      <div class="completion-crest">${escapeHtml(toTier.tierName || fromTier.tierName || "工蜂")}</div>
      <div class="completion-copy">
        <strong>${promoted ? "本輪全對，段位晉升" : "本輪未全對，段位保持"}</strong>
        <span>${state.roundComplete.correctCount} / ${ROUND_SIZE}</span>
      </div>
      <button id="next-round-btn" class="next-round-btn" type="button">再來一輪</button>
    </section>
  `;
}

function renderRoundCompletion() {
  els.questionRoot.innerHTML = renderCompletionCard() || `
    <div class="empty-state">
      <p>本輪已結束，點擊詞類開始下一輪。</p>
    </div>
  `;
  els.questionRoot.querySelector("#next-round-btn")?.addEventListener("click", () => {
    void startChallenge(state.kind);
  });
}

function renderRoundTrack() {
  const marks = state.roundMarks.slice(0, ROUND_SIZE);
  const currentIndex = state.currentItem?.prompt && !state.roundComplete ? Math.min(marks.length, ROUND_SIZE - 1) : -1;
  els.roundTrack.innerHTML = Array.from({ length: ROUND_SIZE }, (_, index) => {
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
    const correct = Boolean(payload?.result?.correct);
    state.run = payload.run || state.run;
    state.answered = true;
    state.answerReveal = buildAnswerReveal(payload?.result?.correctAnswer, payload?.result?.submittedAnswer);
    state.answerCard = payload?.result?.answerKey || null;
    state.player = payload?.player || state.player || currentPlayerState();
    state.roundMarks.push(correct);
    state.roundComplete = payload?.roundCompleted ? payload.round || null : null;
    renderShell();
    if (state.roundComplete) {
      await refreshBootstrap();
      state.player = currentPlayerState();
      await loadLeaderboard(state.kind);
      return;
    }
    clearAdvanceTimer();
    state.autoAdvanceTimer = window.setTimeout(() => {
      state.autoAdvanceTimer = 0;
      void loadNextQuestion();
    }, correct ? 2800 : 3400);
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

function clearAdvanceTimer() {
  if (state.autoAdvanceTimer) {
    window.clearTimeout(state.autoAdvanceTimer);
    state.autoAdvanceTimer = 0;
  }
}

function cleanInline(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

async function api(url, init = {}) {
  const response = await fetch(url, {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
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
